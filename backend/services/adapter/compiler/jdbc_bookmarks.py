"""Shared compiler helpers for Redis-backed JDBC incremental reads.

The incremental JDBC contract is deliberately explicit instead of relying on
NiFi processor state:

* Redis is read before every query.
* A query returns one ordered row at a time.  The row carries its watermark
  (and optional tie-breaker) as FlowFile attributes.
* The bookmark is written only after the downstream terminal publisher has
  reported success.

One-row scheduling is intentional.  It makes the commit boundary unambiguous
and prevents a partially published result set from advancing the cursor past
rows that were not delivered.  A later batching optimisation can preserve the
same contract with an explicit acknowledgement/fan-in stage.
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
        return f"SELECT {projection} FROM {table} WHERE {where} ORDER BY {order} LIMIT 1"
    if not source.tie_breaker:
        return f"SELECT {projection} FROM {table} WHERE {wm} > ? ORDER BY {order} LIMIT 1"
    tie = source.tie_breaker
    return (
        f"SELECT {projection} FROM {table} WHERE ({wm} > ?) OR ({wm} = ? AND {tie} > ?) "
        f"ORDER BY {order} LIMIT 1"
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
    payload_key = f"{key_prefix}__bookmark_payload"
    put_key = f"{key_prefix}__bookmark_commit"
    replacement_value = '{"watermark":"${jdbc.bookmark.candidate:escapeJson()}'
    if source.tie_breaker:
        replacement_value += '","tie":"${jdbc.bookmark.tie:escapeJson()}'
    replacement_value += '"}'
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
    builder.link(tail[0], payload_key, [tail[1]] if tail[1] else [])
    builder.to_dlq(payload_key, "failure")
    builder.add_processor(
        ProcessorSpec(
            key=put_key,
            name=put_key,
            type="org.apache.nifi.processors.standard.PutDistributedMapCache",
            properties={
                "Cache Entry Identifier": f"#{{jdbc_bookmark_key_{source.block_id}}}",
                "Cache Update Strategy": "Replace if present",
                "Distributed Cache Service": cache_key,
            },
            autoTerminate=["success"],
        )
    )
    builder.link(payload_key, put_key, ["success"])
    builder.to_dlq(put_key, "failure")
    return put_key, "success"


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
    """Build the Redis fetch -> one-row query -> cursor capture source."""
    source = bookmark_source_for_block(block)
    if source is None:  # pragma: no cover - caller guards this
        raise CompileError(f"JDBC block {block.id!r} is not incremental")
    cache_key = _bookmark_cache(builder, ctx, add_param=add_param, flow_id=flow.id, source=source)
    period, strategy = cron
    key_param = f"#{{jdbc_bookmark_key_{source.block_id}}}"

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
                "Cache Entry Identifier": "${jdbc.bookmark.key}",
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
        "Record Writer": "cs_json_writer",
        "SQL select query": "${jdbc.query}",
        "Max Rows Per FlowFile": "1",
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
                properties={"Database Connection Pooling Service": db_pool, "Record Writer": "cs_json_writer", "SQL select query": "${jdbc.query}", "Max Rows Per FlowFile": "1"},
            )
        )
        builder.to_dlq("bookmark_initial_query", "failure")
        builder.add_processor(
            ProcessorSpec(
                key="bookmark_initial_extract", name="bookmark_initial_extract",
                type="org.apache.nifi.processors.standard.EvaluateJsonPath",
                properties={"Destination": "flowfile-attribute", "Return Type": "scalar", "Path Not Found Behavior": "ignore", "jdbc.bookmark.candidate": "$. __dmp_watermark".replace(" ", "")},
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
                    properties={"Destination": "flowfile-attribute", "Return Type": "scalar", "Path Not Found Behavior": "ignore", "jdbc.bookmark.tie": "$. __dmp_tie".replace(" ", "")},
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
    builder.add_processor(
        ProcessorSpec(
            key="bookmark_extract", name="bookmark_extract", type="org.apache.nifi.processors.standard.EvaluateJsonPath",
            properties={"Destination": "flowfile-attribute", "Return Type": "scalar", "Path Not Found Behavior": "fail", "jdbc.bookmark.value": "$.watermark"},
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
                properties={"Destination": "flowfile-attribute", "Return Type": "scalar", "Path Not Found Behavior": "fail", "jdbc.bookmark.tie": "$.tie"},
                autoTerminate=["unmatched"],
            )
        )
        builder.link("bookmark_extract", "bookmark_extract_tie", ["matched"])
        query_seed_tail = ("bookmark_extract_tie", "matched")
    else:
        query_seed_tail = ("bookmark_extract", "matched")
    existing_query_tail = _update_attributes(
        builder,
        "bookmark_existing_query",
        {
            "jdbc.query": incremental_query_sql(block, source, with_cursor=True),
            "sql.args.1.type": str(source.watermark_type),
            "sql.args.1.value": "${jdbc.bookmark.value}",
            **({
                "sql.args.2.type": str(source.watermark_type),
                "sql.args.2.value": "${jdbc.bookmark.value}",
                "sql.args.3.type": str(source.tie_breaker_type),
                "sql.args.3.value": "${jdbc.bookmark.tie}",
            } if source.tie_breaker else {}),
        },
        tail=query_seed_tail,
    )
    builder.link(existing_query_tail[0], "query", [existing_query_tail[1]])
    if initial_is_new:
        builder.link(new_tail[0], "bookmark_initial_query", [new_tail[1]])
    else:
        builder.link(oldest_tail[0], "query", [oldest_tail[1]])

    reader_key, split_writer_key = ("cs_json_reader", "cs_json_writer")
    builder.add_processor(
        ProcessorSpec(
            key="split", name="split", type="org.apache.nifi.processors.standard.SplitRecord",
            properties={"Record Reader": reader_key, "Record Writer": split_writer_key, "Records Per Split": "1"},
            autoTerminate=["original"],
        )
    )
    builder.link("query", "split", ["success"])
    builder.to_dlq("split", "failure")
    candidate_path = "$." + source.watermark_column.split(".")[-1]
    builder.add_processor(
        ProcessorSpec(
            key="bookmark_capture", name="bookmark_capture", type="org.apache.nifi.processors.standard.EvaluateJsonPath",
            properties={"Destination": "flowfile-attribute", "Return Type": "scalar", "Path Not Found Behavior": "fail", "jdbc.bookmark.candidate": candidate_path},
            autoTerminate=["unmatched"],
        )
    )
    builder.link("split", "bookmark_capture", ["splits"])
    builder.to_dlq("bookmark_capture", "failure")
    if source.tie_breaker:
        tie_path = "$." + source.tie_breaker.split(".")[-1]
        builder.add_processor(
            ProcessorSpec(
                key="bookmark_capture_tie", name="bookmark_capture_tie",
                type="org.apache.nifi.processors.standard.EvaluateJsonPath",
                properties={"Destination": "flowfile-attribute", "Return Type": "scalar", "Path Not Found Behavior": "fail", "jdbc.bookmark.tie": tie_path},
                autoTerminate=["unmatched"],
            )
        )
        builder.link("bookmark_capture", "bookmark_capture_tie", ["matched"])
        return "bookmark_capture_tie", "matched"
    return "bookmark_capture", "matched"
