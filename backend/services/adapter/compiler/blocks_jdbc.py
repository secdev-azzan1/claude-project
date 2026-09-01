"""`jdbc` adapter compilation - compiler-spec.md §3.2.

FULL scope: all three modes (read/write/lookup). `jdbc` is never `terminal`
per `compile_flow.py`'s dispatch (only `kafka`/`kafka_kc` set `terminal =
True`) and `compile_entry()` here DOES receive `builder` (unlike
`blocks_kafka.compile_entry`), so every mode below adds its processors
directly and returns a real tail, exactly like `blocks_http.compile_read`.

`read` can be a flow root (`compute_root_menu()` lists "jdbc · read");
`write`/`lookup` never are (`compute_add_menu()`-only entries) - each mode
guards `is_root` accordingly. The mid-chain "jdbc read" menu item is no
longer legal.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict

from models.adapter import AppService, FlowBlock

from .ir import CompileError, ControllerServiceSpec, ProcessorSpec, ensure_json_record_services
from .jdbc_bookmarks import add_incremental_source
from ..jdbc import trino_jdbc_url, trino_table_parts
from .transforms import Tail, cron_or_period

if TYPE_CHECKING:  # pragma: no cover
    from models.adapter import Flow
    from .ir import BlockBuilder, CompileContext


_DIALECT_DRIVERS = {
    "postgresql": "org.postgresql.Driver",
    "mysql": "com.mysql.cj.jdbc.Driver",
    "trino": "io.trino.jdbc.TrinoDriver",
}


def compile_entry(
    builder: "BlockBuilder", *, flow: "Flow", block: FlowBlock, ctx: "CompileContext", flow_token: str,
    is_root: bool, add_param,
) -> Tail:
    if block.mode == "read":
        return _compile_read(builder, flow=flow, block=block, ctx=ctx, flow_token=flow_token, is_root=is_root,
                             add_param=add_param)
    if block.mode == "write":
        return _compile_write(builder, flow=flow, block=block, ctx=ctx, flow_token=flow_token, is_root=is_root,
                              add_param=add_param)
    if block.mode == "lookup":
        return _compile_lookup(builder, flow=flow, block=block, ctx=ctx, flow_token=flow_token, is_root=is_root,
                               add_param=add_param)
    raise CompileError(f"Unknown jdbc mode {block.mode!r} on block {block.id}")


def _service_for(block: FlowBlock, ctx: "CompileContext") -> AppService:
    if not block.serviceId:
        raise CompileError(f"jdbc block {block.id!r} has no service selected")
    svc = ctx.services.get(block.serviceId)
    if svc is None:
        raise CompileError(f"jdbc block {block.id!r} references unknown service {block.serviceId!r}")
    return svc


def _configured_table(block: FlowBlock) -> str:
    table = str(block.config.get("table") or block.entity or "").strip()
    if not table:
        raise CompileError(f"jdbc block {block.id!r} has no table configured")
    return table


def _table_name(block: FlowBlock, *, service: AppService | None = None) -> str:
    table = _configured_table(block)
    if service and str(service.config.get("dialect") or "postgresql").lower() == "trino":
        try:
            _, _, leaf_table = trino_table_parts(table)
        except ValueError as exc:
            raise CompileError(f"jdbc block {block.id!r}: {exc}") from exc
        return leaf_table
    return table


def _ensure_db_pool(builder: "BlockBuilder", *, service: AppService, table: str | None, add_param) -> str:
    """Add (once per group) the shared `DBCPConnectionPool` CS built from the
    block's database service — dialect -> JDBC URL scheme + driver class
    (postgresql/mysql/trino per `JdbcDialect` in models/adapter/service.py),
    host/port/database from service config, user/password (sensitive) as
    parameters. `DBCPConnectionPool`'s own property names ("Database
    Connection URL", "Database Driver Class Name", "Database User",
    "Password") are long-standing, stable NiFi controller-service properties
    — high confidence, unlike the QueryDatabaseTableRecord/ConsumeKafka
    property names flagged elsewhere in this task. Both are additionally
    CONFIRMED byte-exact against `Publish3.json`'s live `TrinoJDBC`
    DBCPConnectionPool (docs/orchestration/analysis/user-reference-flows-2.md
    §5.2/§7).

    URL shape is dialect-conditional: `postgresql`/`mysql` keep the standard
    trailing `/{database}` path segment. Trino uses the selected block's
    `catalog.schema.table` reference to build
    `jdbc:trino://host:port/catalog/schema`; HTTPS endpoints add `SSL=true`.
    The processor receives the leaf table while the connection selects the
    correct Trino catalog and schema.

    `Database Driver Locations` (a JAR path list) is required on a real NiFi
    instance for drivers it doesn't bundle, like Trino's — the reference sets
    it to a single NAR-extension JAR path. That path is deployment-specific
    (dependent on where the operator placed the jar on the NiFi host), so we
    don't invent one; we only set the property when the service config
    supplies an explicit `driverLocations` string (comma-separated paths, set
    verbatim). If absent, the property is left unset entirely — NiFi may
    still resolve the driver via a globally-installed jar — rather than
    guessing a path that would likely be wrong.
    """
    cs_key = "cs_db_pool"
    sid = service.id
    dialect = str(service.config.get("dialect", "postgresql"))
    driver = _DIALECT_DRIVERS.get(dialect)
    if driver is None:
        raise CompileError(f"Unknown jdbc dialect {dialect!r} on service {sid!r}")
    host = service.config.get("host", "")
    port = service.config.get("port", "")
    database = service.config.get("database", "")
    if dialect == "trino":
        if not table:
            raise CompileError(f"Trino service {sid!r} needs a fully-qualified table")
        try:
            url = trino_jdbc_url(service.config, table)
        except ValueError as exc:
            raise CompileError(f"Trino service {sid!r}: {exc}") from exc
    else:
        url = f"jdbc:{dialect}://{host}:{port}/{database}"

    add_param(f"svc_{sid}_db_url", url, False)
    add_param(f"svc_{sid}_db_user", str(service.config.get("username", "")), False)
    password = service.config.get("password")
    if password:
        add_param(f"svc_{sid}_db_password", password, True)

    if not builder.has_cs(cs_key):
        properties: Dict[str, Any] = {
            "Database Connection URL": f"#{{svc_{sid}_db_url}}",
            "Database Driver Class Name": driver,
            "Database User": f"#{{svc_{sid}_db_user}}",
        }
        if password:
            properties["Password"] = f"#{{svc_{sid}_db_password}}"
        driver_locations = str(service.config.get("driverLocations") or "").strip()
        if driver_locations:
            properties["Database Driver Locations"] = driver_locations
        builder.add_cs(
            ControllerServiceSpec(
                key=cs_key, name="db_pool", type="org.apache.nifi.dbcp.DBCPConnectionPool",
                properties=properties,
            )
        )
    return cs_key


def _compile_read(
    builder: "BlockBuilder", *, flow: "Flow", block: FlowBlock, ctx: "CompileContext", flow_token: str,
    is_root: bool, add_param,
) -> Tail:
    """Compile a JDBC read.

    Ordinary reads keep the existing ``QueryDatabaseTableRecord`` path.  An
    incremental read is different: NiFi's processor-local state is not the
    platform bookmark, so it uses the explicit Redis-backed source in
    ``jdbc_bookmarks.py``. That source drains the rows available after the
    bookmark in one ordered batch, captures the final watermark, and lets the
    terminal publisher commit the cursor only after the batch succeeds.
    """
    service = _service_for(block, ctx)
    table_ref = _configured_table(block)
    table = _table_name(block, service=service)
    cs_pool = _ensure_db_pool(builder, service=service, table=table_ref, add_param=add_param)
    _, writer_key = ensure_json_record_services(builder)

    columns = [c for c in (block.config.get("columns") or []) if isinstance(c, str) and c.strip()]
    incremental = block.config.get("incremental") is True

    if not is_root:
        raise CompileError(f"jdbc read block {block.id!r} cannot be placed mid-chain")

    period, strategy = cron_or_period(flow.cron)
    if incremental:
        return add_incremental_source(
            builder,
            flow=flow,
            block=block,
            ctx=ctx,
            add_param=add_param,
            db_pool=cs_pool,
            table=table,
            cron=(period, strategy),
        )

    props: Dict[str, Any] = {
        "Database Connection Pooling Service": cs_pool,
        "Table Name": table,
        "Record Writer": writer_key,
    }
    if columns:
        props["Columns to Return"] = ", ".join(columns)
    builder.add_processor(
        ProcessorSpec(key="query", name="query", type="org.apache.nifi.processors.standard.QueryDatabaseTableRecord",
                      properties=props, schedulingPeriod=period, schedulingStrategy=strategy, runOnPrimary=True)
    )
    # NO DLQ edge here (review C3): QueryDatabaseTableRecord is a source
    # processor with exactly one relationship, `success` — there is no
    # `failure` to wire. A query failure is a RUN failure (no record exists
    # yet, MVP §7.14): NiFi yields/penalizes the processor and the failure
    # surfaces as a bulletin. The DLQ path starts at the downstream split.

    reader_key, split_writer_key = ensure_json_record_services(builder)
    builder.add_processor(
        ProcessorSpec(key="split", name="split", type="org.apache.nifi.processors.standard.SplitRecord",
                      properties={"Record Reader": reader_key, "Record Writer": split_writer_key,
                                  "Records Per Split": "1"},
                      autoTerminate=["original"])
    )
    builder.link("query", "split", ["success"])
    builder.to_dlq("split", "failure")
    return "split", "splits"


def _compile_write(
    builder: "BlockBuilder", *, flow: "Flow", block: FlowBlock, ctx: "CompileContext", flow_token: str,
    is_root: bool, add_param,
) -> Tail:
    """compiler-spec §3.2: `PutDatabaseRecord` (Record Reader JsonTreeReader,
    Statement Type = INSERT default, Database Connection Pooling Service,
    Table Name from config/entity).

    change_type note (documented per the task brief, not implemented): the
    frontend's own copy for jdbc write ("History-driven writes: the record's
    `change_type` maps to INSERT / UPDATE / DELETE" — BlockForm.tsx) implies
    a PER-RECORD dynamic statement type. NiFi's `PutDatabaseRecord` does
    support a RecordPath-driven "Statement Type" mode for exactly this, but
    the precise NiFi 2.9 property/enum spelling for that mode isn't
    confirmed here (no reference flow uses PutDatabaseRecord) -- this
    compiles a STATIC `Statement Type` (default `INSERT`, overridable via
    `config.statementType`) instead. Flagged for live E2E.
    """
    if is_root:
        raise CompileError(f"jdbc write block {block.id!r} cannot be a flow root")
    service = _service_for(block, ctx)
    table_ref = _configured_table(block)
    table = _table_name(block, service=service)
    cs_pool = _ensure_db_pool(builder, service=service, table=table_ref, add_param=add_param)
    reader_key, _ = ensure_json_record_services(builder)

    statement_type = str(block.config.get("statementType") or "INSERT").upper()
    builder.add_processor(
        ProcessorSpec(
            key="write", name="write", type="org.apache.nifi.processors.standard.PutDatabaseRecord",
            properties={
                "Record Reader": reader_key,
                "Database Connection Pooling Service": cs_pool,
                "Statement Type": statement_type,
                "Table Name": table,
            },
            # M6: every PutDatabaseRecord relationship needs a disposition —
            # `retry` is auto-terminated (transient errors resurface via the
            # `failure` -> DLQ path on the next attempt rather than looping),
            # `failure` goes to the DLQ below, `success` is the tail.
            autoTerminate=["retry"],
        )
    )
    builder.link("inputPort", "write", [])
    builder.to_dlq("write", "failure")
    return "write", "success"


def _compile_lookup(
    builder: "BlockBuilder", *, flow: "Flow", block: FlowBlock, ctx: "CompileContext", flow_token: str,
    is_root: bool, add_param,
) -> Tail:
    """compiler-spec §3.2: `LookupRecord` + `DatabaseRecordLookupService`
    (Lookup Service wiring, Result RecordPath `/<joinField>_lookup`, Routing
    Strategy route to success) — kept simple and documented, per the task
    brief's own instruction.

    UNCONFIRMED property names (no reference flow uses either processor):
    `LookupRecord`'s exact class (`org.apache.nifi.processors.standard.
    LookupRecord` — standard NiFi bundle, high-moderate confidence) and its
    "Result RecordPath"/"Routing Strategy" property spellings; the dynamic
    per-record lookup-coordinate property (named `<joinField>`, valued
    `/<joinField>` as a RecordPath into the incoming record) mirrors how
    `LookupRecord`'s coordinate properties are generally documented to work,
    but is not verified against this specific service.
    `DatabaseRecordLookupService`'s class name
    (`org.apache.nifi.lookup.db.DatabaseRecordLookupService`) and its
    "Lookup Key Column" property are moderate-confidence best guesses.
    Flagged for live E2E.

    NOTE: `config.lookupJoinField` is optional. The frontend exposes it for
    JDBC lookup blocks, and the compiler falls back to `"id"` when it is
    omitted so existing flows keep working.
    """
    if is_root:
        raise CompileError(f"jdbc lookup block {block.id!r} cannot be a flow root")
    service = _service_for(block, ctx)
    table_ref = _configured_table(block)
    table = _table_name(block, service=service)
    cs_pool = _ensure_db_pool(builder, service=service, table=table_ref, add_param=add_param)
    join_field = str(block.config.get("lookupJoinField") or "id")

    cs_lookup_key = "cs_db_lookup"
    if not builder.has_cs(cs_lookup_key):
        builder.add_cs(
            ControllerServiceSpec(
                key=cs_lookup_key, name="db_lookup", type="org.apache.nifi.lookup.db.DatabaseRecordLookupService",
                properties={
                    "Database Connection Pooling Service": cs_pool,
                    "Table Name": table,
                    "Lookup Key Column": join_field,
                },
            )
        )

    reader_key, writer_key = ensure_json_record_services(builder)
    builder.add_processor(
        ProcessorSpec(
            key="lookup", name="lookup", type="org.apache.nifi.processors.standard.LookupRecord",
            properties={
                "Record Reader": reader_key,
                "Record Writer": writer_key,
                "Lookup Service": cs_lookup_key,
                "Result RecordPath": f"/{join_field}_lookup",
                "Routing Strategy": "Route to Success",
                join_field: f"/{join_field}",
            },
        )
    )
    builder.link("inputPort", "lookup", [])
    builder.to_dlq("lookup", "failure")
    return "lookup", "success"
