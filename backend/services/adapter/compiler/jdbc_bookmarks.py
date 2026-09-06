"""Shared compiler helpers for Redis-backed JDBC incremental reads.

The incremental JDBC contract is deliberately explicit instead of relying on
NiFi processor state:

* Redis is read before every query.
* A query returns the currently available ordered rows as one record-set
  FlowFile.  The last row carries the greatest watermark (and optional
  tie-breaker) for the candidate cursor.
* The bookmark is written only after the downstream terminal publisher has
  reported success for the whole batch.

Batching is intentional.  A scheduled run drains all rows after the current
cursor instead of publishing only one row and waiting for the next timer tick.
The commit remains at-least-once: if publishing fails, the cursor is not
advanced, so a retry can replay already-published rows but cannot silently
skip them.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Optional

from .ir import CompileError, ControllerServiceSpec, ProcessorSpec

if TYPE_CHECKING:  # pragma: no cover
    from models.adapter import FlowBlock
    from .ir import BlockBuilder, CompileContext


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


# Groovy body for the watermark type probe (see `attach_watermark_type_probe`).
#
# Deliberately free of any `${...}` sequence: NiFi does not evaluate Expression
# Language in `Script Body`, but keeping the text EL-free means the script
# survives being round-tripped through anything that does.
#
# Every input arrives as a dynamic property rather than being interpolated into
# the source, so the same body compiles once and is reused by every incremental
# flow, and the values stay visible/editable in the NiFi UI.
#
# `getColumns` is tried three ways because catalog/schema mean different things
# per driver: Trino reports catalog=`gold`, schema=`cmdb`; MySQL puts the
# database in *catalog* and leaves schema null; PostgreSQL uses schema only.
# Narrowing first and widening on a miss gets the right answer everywhere
# without the compiler having to know which dialect it is talking to.
_WATERMARK_TYPE_PROBE_GROOVY = """\
import org.apache.nifi.dbcp.DBCPService

def ff = session.get()
if (!ff) return

def prop = { n -> def p = context.getProperty(n); p == null ? null : p.getValue() }
def blank = { s -> s == null || s.trim().isEmpty() }

// java.sql.Types codes NiFi's own `sql.args.N.type` handling knows how to
// bind. Anything outside this set reaches the driver as a raw string literal,
// which is how a perfectly correct type code can still break the query.
def BINDABLE = [-16, -15, -9, -7, -6, -5, -4, -3, -2, -1, 1, 2, 3, 4, 5, 6, 7, 8, 12, 16, 91, 92, 93, 2005, 2011] as Set
// Types a driver may legitimately report that NiFi cannot bind, mapped to the
// closest one it can. Trino reports `timestamp(6) with time zone` as 2014
// (TIMESTAMP_WITH_TIMEZONE); NiFi has no branch for it, so it passes the epoch
// through verbatim and Trino rejects "'1787480137525' is not a valid TIMESTAMP
// literal". Narrowing to 93 makes NiFi parse the epoch into a real
// java.sql.Timestamp and bind it properly.
def ALIAS = [2014: 93, 2013: 92, 70: 12, 1111: 12]
def bindable = { code ->
    if (code == null) return null
    def n = code as Integer
    n = BINDABLE.contains(n) ? n : ALIAS[n]
    return n == null ? null : String.valueOf(n)
}

def fallback = prop('dmp.watermark.fallback.type')
def column = prop('dmp.watermark.column')
def table = prop('dmp.watermark.table')
def schema = prop('dmp.watermark.schema')
def catalog = prop('dmp.watermark.catalog')
def resolved = fallback

try {
    def dbcp = context.controllerServiceLookup.getControllerService(prop('dmp.dbcp.service.id')) as DBCPService
    def conn = dbcp.getConnection()
    try {
        def find = { cat, sch ->
            def rs = conn.getMetaData().getColumns(blank(cat) ? null : cat, blank(sch) ? null : sch, table, column)
            try {
                return rs.next() ? bindable(rs.getInt('DATA_TYPE')) : null
            } finally {
                rs.close()
            }
        }
        resolved = find(catalog, schema) ?: find(null, schema) ?: find(null, null) ?: fallback
    } finally {
        conn.close()
    }
} catch (Exception e) {
    log.warn('dmp watermark type probe failed for ' + table + '.' + column + '; falling back to JDBC type ' + fallback, e)
    resolved = fallback
}

session.transfer(session.putAttribute(ff, 'jdbc.bookmark.type', resolved), REL_SUCCESS)
"""


@dataclass(frozen=True)
class BookmarkSource:
    """The source cursor metadata propagated to a terminal block."""

    block_id: str
    watermark_column: str
    watermark_type: int
    tie_breaker: str = ""
    tie_breaker_type: int = 12


_JDBC_TYPES = {
    "longnvarchar": -16,
    "bit": -7,
    "boolean": 16,
    "tinyint": -6,
    "bigint": -5,
    "longvarbinary": -4,
    "varbinary": -3,
    "binary": -2,
    "longvarchar": -1,
    "char": 1,
    "numeric": 2,
    "decimal": 3,
    "integer": 4,
    "int": 4,
    "smallint": 5,
    "float": 6,
    "real": 7,
    "double": 8,
    "varchar": 12,
    "string": 12,
    "date": 91,
    "time": 92,
    "timestamp": 93,
    "clob": 2005,
    "nclob": 2011,
}


def _identifier(value: str, *, label: str) -> str:
    """Accept a simple SQL identifier or a dotted qualified identifier."""
    raw = str(value or "").strip()
    parts = raw.split(".") if raw else []
    if not parts or any(not _IDENTIFIER_RE.fullmatch(part) for part in parts):
        raise CompileError(
            f"{label} {raw!r} must contain only simple SQL identifiers "
            "(letters, numbers, underscore, or $)."
        )
    return raw


def _jdbc_type(value: Any, *, default: int = 93) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value if value in set(_JDBC_TYPES.values()) else default
    text = str(value or "").strip().lower()
    if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
        number = int(text)
        return number if number in set(_JDBC_TYPES.values()) else default
    return _JDBC_TYPES.get(text, default)


def bookmark_source_for_block(block: "FlowBlock") -> Optional[BookmarkSource]:
    """Return validated cursor metadata for an incremental JDBC read."""
    if block.adapter != "jdbc" or block.mode != "read" or (block.config or {}).get("incremental") is not True:
        return None
    config = block.config or {}
    watermark = _identifier(str(config.get("watermarkColumn") or ""), label="Watermark column")
    tie = str(config.get("bookmarkTieBreaker") or "").strip()
    if tie:
        tie = _identifier(tie, label="Bookmark tie-breaker")
    return BookmarkSource(
        block_id=block.id,
        watermark_column=watermark,
        watermark_type=_jdbc_type(config.get("watermarkType"), default=93),
        tie_breaker=tie,
        tie_breaker_type=_jdbc_type(config.get("bookmarkTieBreakerType"), default=12),
    )


def source_for_block(flow: Any, block: "FlowBlock") -> Optional[BookmarkSource]:
    """Find the nearest incremental JDBC ancestor, if one exists."""
    by_id = {b.id: b for b in flow.blocks}
    current = block
    seen: set[str] = set()
    while current and current.id not in seen:
        seen.add(current.id)
        source = bookmark_source_for_block(current)
        if source:
            return source
        current = by_id.get(current.parentId) if current.parentId else None
    return None


def bookmark_key(flow_id: str, block_id: str) -> str:
    return f"dmp:jdbc:bookmark:{flow_id}:{block_id}"


def bookmark_key_param(block_id: str) -> str:
    """The NiFi parameter reference holding this block's Redis cache key.

    Both the fetch and the commit must address the *same* entry, so both go
    through this one helper. They previously disagreed in mechanism -- the
    fetch read the `jdbc.bookmark.key` FlowFile attribute while the commit
    used the parameter -- which resolved to the same string in a clean deploy
    but let the two drift apart the moment either side was edited.
    """
    return f"#{{jdbc_bookmark_key_{block_id}}}"


def _bookmark_cache(builder: "BlockBuilder", ctx: "CompileContext", *, add_param, flow_id: str, source: BookmarkSource) -> str:
    if "redis" not in ctx.connections:
        raise CompileError(
            f"Incremental JDBC block {source.block_id!r} requires an active Redis connection for its bookmark."
        )
    redis_cfg = ctx.connection_config("redis")
    host = redis_cfg.get("host", "redis")
    port = redis_cfg.get("port", 6379)
    db_index = redis_cfg.get("bookmarksDb", 1)
    password = redis_cfg.get("password")

    add_param("redis_connection_string", f"{host}:{port}", False)
    add_param("redis_password", password, True)
    key = "cs_jdbc_bookmark_cache"
    pool_key = "cs_jdbc_bookmark_pool"
    if not builder.has_cs(pool_key):
        builder.add_cs(
            ControllerServiceSpec(
                key=pool_key,
                name="jdbc_bookmark_pool",
                type="org.apache.nifi.redis.service.RedisConnectionPoolService",
                properties={
                    "Connection String": "#{redis_connection_string}",
                    "Redis Mode": "Standalone",
                    "Database Index": str(db_index),
                    "Password": "#{redis_password}",
                },
            )
        )
    if not builder.has_cs(key):
        builder.add_cs(
            ControllerServiceSpec(
                key=key,
                name="jdbc_bookmark_cache",
                type="org.apache.nifi.redis.service.RedisDistributedMapCacheClientService",
                properties={"Redis Connection Pool": pool_key, "TTL": "0 secs"},
            )
        )
    add_param(f"jdbc_bookmark_key_{source.block_id}", bookmark_key(flow_id, source.block_id), False)
    return key


def _query_projection(block: "FlowBlock", source: BookmarkSource) -> str:
    columns = [str(c).strip() for c in ((block.config or {}).get("columns") or []) if str(c).strip()]
    if not columns:
        return "*"
    safe_columns = [_identifier(c, label="JDBC column") for c in columns]
    if source.watermark_column not in safe_columns:
        safe_columns.append(source.watermark_column)
    if source.tie_breaker and source.tie_breaker not in safe_columns:
        safe_columns.append(source.tie_breaker)
    return ", ".join(safe_columns)


def incremental_query_sql(block: "FlowBlock", source: BookmarkSource, *, with_cursor: bool) -> str:
    table = _identifier(str((block.config or {}).get("table") or block.entity or ""), label="JDBC table")
    projection = _query_projection(block, source)
    wm = source.watermark_column
    order = f"{wm} ASC"
    if source.tie_breaker:
        order += f", {source.tie_breaker} ASC"
    if not with_cursor:
        where = f"{wm} IS NOT NULL"
        if source.tie_breaker:
            where += f" AND {source.tie_breaker} IS NOT NULL"
        return f"SELECT {projection} FROM {table} WHERE {where} ORDER BY {order}"
    if not source.tie_breaker:
        return f"SELECT {projection} FROM {table} WHERE {wm} > ? ORDER BY {order}"
    tie = source.tie_breaker
    return (
        f"SELECT {projection} FROM {table} WHERE ({wm} > ?) OR ({wm} = ? AND {tie} > ?) "
        f"ORDER BY {order}"
    )


def initial_cursor_sql(block: "FlowBlock", source: BookmarkSource) -> str:
    table = _identifier(str((block.config or {}).get("table") or block.entity or ""), label="JDBC table")
    if source.tie_breaker:
        return (
            f"SELECT {source.watermark_column} AS __dmp_watermark, "
            f"{source.tie_breaker} AS __dmp_tie FROM {table} "
            f"WHERE {source.watermark_column} IS NOT NULL AND {source.tie_breaker} IS NOT NULL "
            f"ORDER BY {source.watermark_column} DESC, {source.tie_breaker} DESC LIMIT 1"
        )
    return (
        f"SELECT {source.watermark_column} AS __dmp_watermark FROM {table} "
        f"WHERE {source.watermark_column} IS NOT NULL "
        f"ORDER BY {source.watermark_column} DESC LIMIT 1"
    )


def _update_attributes(builder: "BlockBuilder", key: str, props: Dict[str, Any], *, tail: tuple[str, str]) -> tuple[str, str]:
    builder.add_processor(
        ProcessorSpec(
            key=key,
            name=key,
            type="org.apache.nifi.processors.attributes.UpdateAttribute",
            properties=props,
        )
    )
    builder.link(tail[0], key, [tail[1]] if tail[1] else [])
    return key, "success"


def _ensure_incremental_json_writer(builder: "BlockBuilder") -> str:
    """Return the writer used to keep one incremental batch as an array.

    The ordinary application writer is intentionally ``output-oneline``
    because most flow paths work one record per FlowFile. Incremental JDBC is
    different: the query must drain all rows available in the scheduled run,
    while the downstream Record Reader still turns the array into individual
    records for Kafka publication.
    """
    key = "cs_incremental_json_writer"
    if not builder.has_cs(key):
        builder.add_cs(
            ControllerServiceSpec(
                key=key,
                name="incremental_json_writer",
                type="org.apache.nifi.json.JsonRecordSetWriter",
                properties={
                    "Schema Access Strategy": "inherit-record-schema",
                    "Schema Write Strategy": "no-schema",
                    "Output Grouping": "output-array",
                },
            )
        )
    return key


def attach_bookmark_commit(
    builder: "BlockBuilder",
    *,
    ctx: "CompileContext",
    flow_id: str,
    source: BookmarkSource,
    add_param,
    tail: tuple[str, str],
    key_prefix: str,
) -> tuple[str, str]:
    """Commit the candidate cursor after a terminal processor succeeds."""
    cache_key = _bookmark_cache(builder, ctx, add_param=add_param, flow_id=flow_id, source=source)
    guard_key = f"{key_prefix}__bookmark_guard"
    payload_key = f"{key_prefix}__bookmark_payload"
    put_key = f"{key_prefix}__bookmark_commit"
    replacement_value = '{"watermark":"${jdbc.bookmark.candidate:escapeJson()}'
    if source.tie_breaker:
        replacement_value += '","tie":"${jdbc.bookmark.tie:escapeJson()}'
    replacement_value += '"}'
    # A run that returns no rows is the normal steady state once the flow has
    # caught up, and `bookmark_capture` uses "Path Not Found Behavior: skip",
    # so `jdbc.bookmark.candidate` is simply absent on those runs. Without this
    # guard the payload writer below -- "Always Replace", so it fires
    # regardless -- would emit `{"watermark":""}` and the `replace` cache
    # strategy would overwrite a perfectly good cursor with an empty one. The
    # next run then binds '' as the watermark and dies in ExecuteSQLRecord
    # before it can ever write a better bookmark, wedging the flow permanently.
    # Dropping the FlowFile here leaves the stored cursor untouched, which is
    # the at-least-once contract this module's docstring describes.
    builder.add_processor(
        ProcessorSpec(
            key=guard_key,
            name=guard_key,
            type="org.apache.nifi.processors.standard.RouteOnAttribute",
            properties={
                "Routing Strategy": "Route to Property name",
                "commit": "${jdbc.bookmark.candidate:isEmpty():not()}",
            },
            autoTerminate=["unmatched"],
        )
    )
    builder.link(tail[0], guard_key, [tail[1]] if tail[1] else [])
    builder.to_dlq(guard_key, "failure")
    builder.add_processor(
        ProcessorSpec(
            key=payload_key,
            name=payload_key,
            type="org.apache.nifi.processors.standard.ReplaceText",
            properties={
                "Replacement Strategy": "Always Replace",
                "Replacement Value": replacement_value,
                "Evaluation Mode": "Entire text",
                "Character Set": "UTF-8",
            },
        )
    )
    builder.link(guard_key, payload_key, ["commit"])
    builder.to_dlq(payload_key, "failure")
    builder.add_processor(
        ProcessorSpec(
            key=put_key,
            name=put_key,
            type="org.apache.nifi.processors.standard.PutDistributedMapCache",
            properties={
                "Cache Entry Identifier": bookmark_key_param(source.block_id),
                "Cache Update Strategy": "replace",
                "Distributed Cache Service": cache_key,
            },
            autoTerminate=["success"],
        )
    )
    builder.link(payload_key, put_key, ["success"])
    builder.to_dlq(put_key, "failure")
    return put_key, "success"


def _table_parts(raw: str) -> tuple[str, str, str]:
    """Split a configured table reference into (catalog, schema, table).

    Read right-to-left so every supported shape works without knowing the
    dialect: `asset_groups`, `cmdb.asset_groups`, `gold.cmdb.asset_groups`.
    Missing levels come back empty and the probe simply widens its search.
    """
    parts = [p for p in str(raw or "").split(".") if p]
    table = parts[-1] if parts else ""
    schema = parts[-2] if len(parts) >= 2 else ""
    catalog = parts[-3] if len(parts) >= 3 else ""
    return catalog, schema, table


def attach_watermark_type_probe(
    builder: "BlockBuilder", *, block: "FlowBlock", source: BookmarkSource, db_pool: str, tail: tuple[str, str]
) -> tuple[str, str]:
    """Ask the JDBC driver for the watermark column's real type.

    `sql.args.1.type` has to name the JDBC type NiFi should bind the stored
    cursor as. The only other source for it is `config.watermarkType`, which
    nothing in the product ever writes -- not the block form, not validation,
    not any stored flow -- so it always fell back to 93/TIMESTAMP and every
    non-timestamp watermark (an integer id, a date, a text column) was bound
    as a timestamp and failed.

    `DatabaseMetaData.getColumns().DATA_TYPE` is the driver's own answer, so
    it is correct for Trino, PostgreSQL and MySQL alike with no dialect
    specific SQL and no type-name mapping to maintain. Running it inside NiFi
    -- which already holds the pooled connection built in `blocks_jdbc.py` --
    keeps this backend free of database drivers and of any network path to
    the customer's database.

    One metadata round trip per scheduled run: stateless, nothing to
    invalidate, and still right if the column is later re-typed. On any
    failure the script logs and falls back to `source.watermark_type`, so a
    driver that refuses metadata degrades to today's behaviour instead of
    stalling the flow.
    """
    catalog, schema, table = _table_parts(str((block.config or {}).get("table") or block.entity or ""))
    key = "bookmark_type_probe"
    builder.add_processor(
        ProcessorSpec(
            key=key,
            name=key,
            type="org.apache.nifi.processors.script.ExecuteScript",
            properties={
                "Script Engine": "Groovy",
                "Script Body": _WATERMARK_TYPE_PROBE_GROOVY,
                # Exactly a controller-service plan key, so `_resolve_component_refs`
                # in deployer/nifi_apply.py swaps it for the pool's real NiFi id.
                "dmp.dbcp.service.id": db_pool,
                "dmp.watermark.catalog": catalog,
                "dmp.watermark.schema": schema,
                "dmp.watermark.table": table,
                "dmp.watermark.column": source.watermark_column.split(".")[-1],
                "dmp.watermark.fallback.type": str(source.watermark_type),
            },
        )
    )
    builder.link(tail[0], key, [tail[1]] if tail[1] else [])
    builder.to_dlq(key, "failure")
    return key, "success"


def add_incremental_source(
    builder: "BlockBuilder",
    *,
    flow: Any,
    block: "FlowBlock",
    ctx: "CompileContext",
    add_param,
    db_pool: str,
    table: str,
    cron: tuple[str, str],
) -> tuple[str, str]:
    """Build the Redis fetch -> batch query -> cursor capture source."""
    source = bookmark_source_for_block(block)
    if source is None:  # pragma: no cover - caller guards this
        raise CompileError(f"JDBC block {block.id!r} is not incremental")
    cache_key = _bookmark_cache(builder, ctx, add_param=add_param, flow_id=flow.id, source=source)
    batch_writer_key = _ensure_incremental_json_writer(builder)
    period, strategy = cron
    key_param = bookmark_key_param(source.block_id)

    builder.add_processor(
        ProcessorSpec(
            key="trigger", name="trigger", type="org.apache.nifi.processors.standard.GenerateFlowFile",
            properties={"Batch Size": "1", "Unique FlowFiles": "false"},
            schedulingPeriod=period, schedulingStrategy=strategy, runOnPrimary=True,
        )
    )
    source_tail = _update_attributes(
        builder, "bookmark_key", {"jdbc.bookmark.key": key_param}, tail=("trigger", "success")
    )
    builder.add_processor(
        ProcessorSpec(
            key="bookmark_fetch", name="bookmark_fetch",
            type="org.apache.nifi.processors.standard.FetchDistributedMapCache",
            properties={
                # Same parameter the commit writes to -- see `bookmark_key_param`.
                "Cache Entry Identifier": key_param,
                "Distributed Cache Service": cache_key,
                "Put Cache Value In Attribute": "jdbc.bookmark.raw",
                "Max Length To Put In Attribute": "4096",
                "Character Set": "UTF-8",
            },
        )
    )
    builder.link(source_tail[0], "bookmark_fetch", [source_tail[1]])
    builder.to_dlq("bookmark_fetch", "failure")

    query_props: Dict[str, Any] = {
        "Database Connection Pooling Service": db_pool,
        "Record Writer": batch_writer_key,
        "SQL Query": "${jdbc.query}",
        "Max Rows Per FlowFile": "0",
    }
    builder.add_processor(
        ProcessorSpec(
            key="query", name="query", type="org.apache.nifi.processors.standard.ExecuteSQLRecord",
            properties=query_props, autoTerminate=[],
        )
    )
    builder.to_dlq("query", "failure")

    initial_is_new = str((block.config or {}).get("initialPosition") or "oldest").strip().lower() == "new"
    if initial_is_new:
        new_props = {"jdbc.query": initial_cursor_sql(block, source)}
        new_tail = _update_attributes(builder, "bookmark_initial_seed", new_props, tail=("bookmark_fetch", "not-found"))
        # The initial snapshot establishes a boundary and intentionally emits
        # no data. An empty table (or a table with only null cursor values)
        # returns no row and simply ends this run without creating a key.
        builder.add_processor(
            ProcessorSpec(
                key="bookmark_initial_query", name="bookmark_initial_query",
                type="org.apache.nifi.processors.standard.ExecuteSQLRecord",
                properties={"Database Connection Pooling Service": db_pool, "Record Writer": batch_writer_key, "SQL Query": "${jdbc.query}", "Max Rows Per FlowFile": "0"},
            )
        )
        builder.to_dlq("bookmark_initial_query", "failure")
        builder.add_processor(
            ProcessorSpec(
                key="bookmark_initial_extract", name="bookmark_initial_extract",
                type="org.apache.nifi.processors.standard.EvaluateJsonPath",
                properties={"Destination": "flowfile-attribute", "Return Type": "scalar", "Path Not Found Behavior": "ignore", "jdbc.bookmark.candidate": "$[-1].__dmp_watermark"},
                autoTerminate=["unmatched"],
            )
        )
        builder.link("bookmark_initial_query", "bookmark_initial_extract", ["success"])
        builder.to_dlq("bookmark_initial_extract", "failure")
        if source.tie_breaker:
            builder.add_processor(
                ProcessorSpec(
                    key="bookmark_initial_tie", name="bookmark_initial_tie",
                    type="org.apache.nifi.processors.standard.EvaluateJsonPath",
                    properties={"Destination": "flowfile-attribute", "Return Type": "scalar", "Path Not Found Behavior": "ignore", "jdbc.bookmark.tie": "$[-1].__dmp_tie"},
                    autoTerminate=["unmatched"],
                )
            )
            builder.link("bookmark_initial_extract", "bookmark_initial_tie", ["matched"])
            initial_capture_tail = ("bookmark_initial_tie", "matched")
        else:
            initial_capture_tail = ("bookmark_initial_extract", "matched")
        initial_commit = attach_bookmark_commit(
            builder, ctx=ctx, flow_id=flow.id, source=source, add_param=add_param,
            tail=initial_capture_tail, key_prefix="bookmark_initial",
        )
        # The initial boundary must not become a data tail.
        builder.auto_terminate_tail(initial_commit[0], initial_commit[1])
    else:
        oldest_props = {"jdbc.query": incremental_query_sql(block, source, with_cursor=False)}
        oldest_tail = _update_attributes(builder, "bookmark_oldest", oldest_props, tail=("bookmark_fetch", "not-found"))

    builder.link("bookmark_fetch", "bookmark_decode", ["success"])

    # For an existing bookmark, decode the cached JSON value into attributes.
    builder.add_processor(
        ProcessorSpec(
            key="bookmark_decode", name="bookmark_decode", type="org.apache.nifi.processors.standard.ReplaceText",
            properties={"Replacement Strategy": "Always Replace", "Replacement Value": "${jdbc.bookmark.raw}", "Evaluation Mode": "Entire text", "Character Set": "UTF-8"},
        )
    )
    builder.to_dlq("bookmark_decode", "failure")
    builder.add_processor(
        ProcessorSpec(
            key="bookmark_extract", name="bookmark_extract", type="org.apache.nifi.processors.standard.EvaluateJsonPath",
            properties={"Destination": "flowfile-attribute", "Return Type": "scalar", "Path Not Found Behavior": "skip", "jdbc.bookmark.value": "$.watermark"},
            autoTerminate=["unmatched"],
        )
    )
    builder.link("bookmark_decode", "bookmark_extract", ["success"])
    builder.to_dlq("bookmark_extract", "failure")
    if source.tie_breaker:
        builder.add_processor(
            ProcessorSpec(
                key="bookmark_extract_tie", name="bookmark_extract_tie",
                type="org.apache.nifi.processors.standard.EvaluateJsonPath",
                properties={"Destination": "flowfile-attribute", "Return Type": "scalar", "Path Not Found Behavior": "skip", "jdbc.bookmark.tie": "$.tie"},
                autoTerminate=["unmatched"],
            )
        )
        builder.link("bookmark_extract", "bookmark_extract_tie", ["matched"])
        query_seed_tail = ("bookmark_extract_tie", "matched")
    else:
        query_seed_tail = ("bookmark_extract", "matched")

    # A cached entry can exist and still carry no usable cursor: an earlier
    # build committed `{"watermark":""}` on every zero-row run, and
    # `bookmark_extract` skips a missing path rather than failing. Feeding that
    # into the parameterised query binds '' and throws inside ExecuteSQLRecord,
    # and because the run dies before reaching the commit the flow can never
    # repair itself. Treat an empty cursor as "no bookmark" and fall through to
    # the seed branch built above, so a corrupt entry costs one replay instead
    # of permanent downtime.
    seed_key = "bookmark_initial_seed" if initial_is_new else "bookmark_oldest"
    builder.add_processor(
        ProcessorSpec(
            key="bookmark_resume",
            name="bookmark_resume",
            type="org.apache.nifi.processors.standard.RouteOnAttribute",
            properties={
                "Routing Strategy": "Route to Property name",
                "resume": "${jdbc.bookmark.value:isEmpty():not()}",
            },
        )
    )
    builder.link(query_seed_tail[0], "bookmark_resume", [query_seed_tail[1]])
    builder.link("bookmark_resume", seed_key, ["unmatched"])
    builder.to_dlq("bookmark_resume", "failure")

    probe_tail = attach_watermark_type_probe(
        builder, block=block, source=source, db_pool=db_pool, tail=("bookmark_resume", "resume")
    )
    # `sql.args.N.type` is resolved per run by the probe above rather than
    # frozen at compile time; `source.watermark_type` survives as the script's
    # fallback, so a flow that does set `watermarkType` still gets it.
    watermark_type_el = "${jdbc.bookmark.type}"
    existing_query_tail = _update_attributes(
        builder,
        "bookmark_existing_query",
        {
            "jdbc.query": incremental_query_sql(block, source, with_cursor=True),
            "sql.args.1.type": watermark_type_el,
            "sql.args.1.value": "${jdbc.bookmark.value}",
            **({
                "sql.args.2.type": watermark_type_el,
                "sql.args.2.value": "${jdbc.bookmark.value}",
                "sql.args.3.type": str(source.tie_breaker_type),
                "sql.args.3.value": "${jdbc.bookmark.tie}",
            } if source.tie_breaker else {}),
        },
        tail=probe_tail,
    )
    builder.link(existing_query_tail[0], "query", [existing_query_tail[1]])
    if initial_is_new:
        builder.link(new_tail[0], "bookmark_initial_query", [new_tail[1]])
    else:
        builder.link(oldest_tail[0], "query", [oldest_tail[1]])

    # The query output is an ordered JSON array. The final element is the
    # greatest cursor in this batch, so one capture/commit advances the
    # bookmark only after the publisher has accepted every record.
    candidate_path = "$[-1]." + source.watermark_column.split(".")[-1]
    builder.add_processor(
        ProcessorSpec(
            key="bookmark_capture", name="bookmark_capture", type="org.apache.nifi.processors.standard.EvaluateJsonPath",
            properties={"Destination": "flowfile-attribute", "Return Type": "scalar", "Path Not Found Behavior": "skip", "jdbc.bookmark.candidate": candidate_path},
            autoTerminate=["unmatched"],
        )
    )
    builder.link("query", "bookmark_capture", ["success"])
    builder.to_dlq("bookmark_capture", "failure")
    if source.tie_breaker:
        tie_path = "$[-1]." + source.tie_breaker.split(".")[-1]
        builder.add_processor(
            ProcessorSpec(
                key="bookmark_capture_tie", name="bookmark_capture_tie",
                type="org.apache.nifi.processors.standard.EvaluateJsonPath",
                properties={"Destination": "flowfile-attribute", "Return Type": "scalar", "Path Not Found Behavior": "skip", "jdbc.bookmark.tie": tie_path},
                autoTerminate=["unmatched"],
            )
        )
        builder.link("bookmark_capture", "bookmark_capture_tie", ["matched"])
        return "bookmark_capture_tie", "matched"
    return "bookmark_capture", "matched"
