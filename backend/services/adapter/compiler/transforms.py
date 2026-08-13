"""Per-record transform chain + dedup — compiler-spec.md §4.

`build_chain()` compiles a block's `transforms: TransformRule[]` list, in
order, into processors appended to the given `BlockBuilder`. Each rule gets
its own processor (or, for `rename`, a copy+drop pair — see the docstring on
`_compile_rename` for why one `UpdateRecord` alone cannot do it) keyed
`t<idx>__<kind>`, so the scope map can attribute failures back to the exact
rule that produced them. `dedup` is handled last (validation guarantees it is
already the last rule in the list) via `_compile_dedup`, adapted from
`docs/orchestration/analysis/dedup-reference-flow.md`'s reference Groovy
script — extended per the T7.1 task brief:
  - identity values come from the rule's configured `identityFields` list
    (not a single hardcoded field);
  - a record missing any configured identity field is routed to REL_FAILURE
    with `dlq.reason=dedup_identity_missing` instead of hashing a `null`
    identity into the cache key.

Every processor's `failure` (or `unmatched`, for detect) relationship that
represents a record-level failure is wired to the block's DLQ path via
`builder.to_dlq(...)` — see `dlq.py`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Tuple

from models.adapter import FlowBlock, TransformRule

from .ir import (
    CompileError,
    ControllerServiceSpec,
    ProcessorSpec,
    dedupe_preserve_order,
    ensure_json_record_services,
    format_duration_hours,
)

if TYPE_CHECKING:  # pragma: no cover
    from models.adapter import Flow
    from .ir import BlockBuilder, CompileContext

Tail = Tuple[str, str]  # (processor key, forward relationship name)

DEDUP_PLATFORM_EXCLUDES = ["ingest_id", "ingest_ts", "op"]


def cron_or_period(cron: Optional[str]) -> Tuple[str, str]:
    """5-field UTC cron -> NiFi cron (`sec min hour dom mon dow`), per
    compiler-spec §3.1 item 1 / §3.2 ("trigger = cron scheduling on the
    processor itself when root"): `0 <min> <hour> <dom> <mon> <dow>`.

    Shared by http's `GenerateFlowFile` trigger (root http read/write) and
    jdbc's `QueryDatabaseTableRecord` root scheduling — both need the exact
    same UTC-cron -> NiFi-cron mapping, so it lives here (the one place the
    task brief sanctions a cross-`blocks_*` shared helper) rather than being
    duplicated. Originally a private `_cron_or_period` in `blocks_http.py`;
    moved here verbatim when jdbc needed the same mapping.
    """
    if not cron:
        return "1 hour", "TIMER_DRIVEN"
    fields = cron.strip().split()
    if len(fields) != 5:
        raise CompileError(f"Cron must be a 5-field expression (UTC), got {cron!r}")
    minute, hour, dom, mon, dow = fields
    return f"0 {minute} {hour} {dom} {mon} {dow}", "CRON_DRIVEN"

# Adapted from docs/orchestration/analysis/dedup-reference-flow.md's reference
# script (SRC/EXCLUDES dynamic properties, canonical-JSON-minus-excludes SHA-256
# hash). Extended for T7.1: IDENTITY_FIELDS is a new dynamic property (the
# reference hardcoded `object_id`); a record missing ANY configured identity
# field routes to REL_FAILURE with dlq.reason=dedup_identity_missing instead of
# hashing a `null` identity into the cache key.
GROOVY_HASH_SCRIPT = """import groovy.json.*
import java.security.MessageDigest
import org.apache.nifi.processor.io.InputStreamCallback

def flowFile = session.get()
if (!flowFile) return
try {
    def exclStr = (binding.hasVariable('EXCLUDES') ? (EXCLUDES.value ?: '') : '')
    def excl = exclStr.split(',').collect { it.trim() }.findAll { it }
    def src = (binding.hasVariable('SRC') ? (SRC.value ?: 'src') : 'src')
    def identStr = (binding.hasVariable('IDENTITY_FIELDS') ? (IDENTITY_FIELDS.value ?: '') : '')
    def identityFields = identStr.split(',').collect { it.trim() }.findAll { it }

    def holder = [t: '']
    session.read(flowFile, { inp -> holder.t = inp.getText('UTF-8') } as InputStreamCallback)
    def parsed = new JsonSlurper().parseText(holder.t)
    def rec = (parsed instanceof List) ? parsed[0] : parsed

    def missing = identityFields.findAll { f ->
        !(rec instanceof Map) || !rec.containsKey(f) || rec.get(f) == null || rec.get(f).toString().trim().isEmpty()
    }
    if (identityFields.isEmpty() || !missing.isEmpty()) {
        flowFile = session.putAttribute(flowFile, 'dlq.reason', 'dedup_identity_missing')
        session.transfer(flowFile, REL_FAILURE)
        return
    }
    def identityValue = identityFields.collect { f -> rec.get(f).toString() }.join(',')

    def m = new LinkedHashMap(rec)
    excl.each { m.remove(it) }
    def cj = JsonOutput.toJson(m)
    def h = MessageDigest.getInstance('SHA-256').digest(cj.getBytes('UTF-8')).encodeHex().toString()
    flowFile = session.putAttribute(flowFile, 'dedupe.key', (src + ':' + identityValue + ':' + h).toString())
    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    log.error('dedupe hash failed: ' + e.message, e)
    flowFile = session.putAttribute(flowFile, 'dlq.reason', 'dedup_script_error')
    session.transfer(flowFile, REL_FAILURE)
}
"""


def _to_record_path(field_name: str) -> str:
    """"field" -> "/field", "a.b.c" -> "/a/b/c", "/a/b" stays as-is."""
    raw = (field_name or "").strip()
    if not raw:
        return "/"
    if raw.startswith("/"):
        return raw
    parts = [p.strip() for p in raw.split(".") if p.strip()]
    return "/" + "/".join(parts) if parts else "/"


def build_chain(
    builder: "BlockBuilder",
    *,
    flow: "Flow",
    block: FlowBlock,
    ctx: "CompileContext",
    flow_token: str,
    tail: Tail,
    add_param,
) -> Tail:
    """Compile `block.transforms` onto `builder`, chaining from `tail`.
    Returns the new tail (key, relationship) after the last rule."""
    rules = list(block.transforms)
    for idx, rule in enumerate(rules):
        if rule.kind == "dedup":
            tail = _compile_dedup(builder, flow=flow, block=block, ctx=ctx, flow_token=flow_token, rule=rule,
                                  tail=tail, add_param=add_param)
        elif rule.kind == "extract":
            tail = _compile_extract(builder, idx=idx, rule=rule, tail=tail)
        elif rule.kind == "add_field":
            tail = _compile_update_record(builder, idx=idx, kind="add_field", tail=tail, field_props={
                _to_record_path(str(rule.config.get("field", ""))): str(rule.config.get("value", "")),
            })
        elif rule.kind == "set_from_attribute":
            attribute = str(rule.config.get("attribute", ""))
            tail = _compile_update_record(builder, idx=idx, kind="set_from_attribute", tail=tail, field_props={
                _to_record_path(str(rule.config.get("field", ""))): "${" + attribute + "}",
            })
        elif rule.kind == "rename":
            tail = _compile_rename(builder, idx=idx, rule=rule, tail=tail)
        elif rule.kind == "coerce":
            tail = _compile_coerce(builder, idx=idx, rule=rule, tail=tail)
        elif rule.kind == "remove_field":
            tail = _compile_remove_field(builder, idx=idx, rule=rule, tail=tail)
        else:  # pragma: no cover - validation.py already restricts TransformKind
            raise CompileError(f"Unknown transform kind {rule.kind!r} on block {block.id}")
    return tail


def _compile_extract(builder: "BlockBuilder", *, idx: int, rule: TransformRule, tail: Tail) -> Tail:
    key = f"t{idx}__extract"
    attribute = str(rule.config.get("attribute", "")) or f"extract_{idx}"
    path = str(rule.config.get("path", "$"))
    default = rule.config.get("default")
    props = {
        "Destination": "flowfile-attribute",
        "Return Type": "scalar",
        "Path Not Found Behavior": "ignore",
        "Null Value Representation": "empty string",
        attribute: path,
    }
    if default not in (None, ""):
        # `default` is not materialized by a second processor (spec: "each
        # rule = its own processor") — recorded here for a downstream EL
        # consumer to apply via `${attr:isEmpty():ifElse('<default>', ${attr})}`.
        props["Default Value (informational)"] = str(default)
    tail_key, tail_rel = tail
    from_key, from_rel = key, "matched"
    builder.add_processor(
        ProcessorSpec(key=key, name=key, type="org.apache.nifi.processors.standard.EvaluateJsonPath",
                       properties=props, autoTerminate=["unmatched"])
    )
    builder.link(tail_key, key, [tail_rel])
    builder.to_dlq(key, "failure")
    return from_key, from_rel


def _compile_update_record(builder: "BlockBuilder", *, idx: int, kind: str, tail: Tail, field_props: dict) -> Tail:
    key = f"t{idx}__{kind}"
    reader_key, writer_key = ensure_json_record_services(builder)
    props = {"Record Reader": reader_key, "Record Writer": writer_key,
             "Replacement Value Strategy": "literal-value", **field_props}
    tail_key, tail_rel = tail
    builder.add_processor(
        ProcessorSpec(key=key, name=key, type="org.apache.nifi.processors.standard.UpdateRecord", properties=props)
    )
    builder.link(tail_key, key, [tail_rel])
    builder.to_dlq(key, "failure")
    return key, "success"


def _compile_rename(builder: "BlockBuilder", *, idx: int, rule: TransformRule, tail: Tail) -> Tail:
    """`rename` = copy value to the new field name, then drop the old one.

    NiFi's `UpdateRecord` alone cannot rename a field (it can only set field
    values, and only ONE `Replacement Value Strategy` applies per processor
    instance, so a single instance can't both copy-by-record-path AND clear
    the source field). Two processors are used instead: an `UpdateRecord`
    (record-path-value strategy) copying `/<from>` -> `/<to>`, then a
    `RemoveRecordField` dropping `/<from>`. Both are keyed under the same
    rule index (`t<idx>__rename__copy` / `__drop`) so scope-map attribution
    still reads as "one rule, one unit."
    """
    from_field = _to_record_path(str(rule.config.get("from", "")))
    to_field = _to_record_path(str(rule.config.get("to", "")))
    copy_key = f"t{idx}__rename__copy"
    drop_key = f"t{idx}__rename__drop"
    tail_key, tail_rel = tail
    reader_key, writer_key = ensure_json_record_services(builder)

    builder.add_processor(
        ProcessorSpec(
            key=copy_key, name=copy_key, type="org.apache.nifi.processors.standard.UpdateRecord",
            properties={"Record Reader": reader_key, "Record Writer": writer_key,
                        "Replacement Value Strategy": "record-path-value", to_field: from_field},
        )
    )
    builder.link(tail_key, copy_key, [tail_rel])
    builder.to_dlq(copy_key, "failure")

    builder.add_processor(
        ProcessorSpec(
            key=drop_key, name=drop_key, type="org.apache.nifi.processors.standard.RemoveRecordField",
            properties={"Record Reader": reader_key, "Record Writer": writer_key, "field_to_remove_1": from_field},
        )
    )
    builder.link(copy_key, drop_key, ["success"])
    builder.to_dlq(drop_key, "failure")
    return drop_key, "success"


def _compile_coerce(builder: "BlockBuilder", *, idx: int, rule: TransformRule, tail: Tail) -> Tail:
    """`coerce` compiles to a pass-through `UpdateRecord` (record-path-value,
    identity path) carrying the target type as an informational property.
    Real type coercion is enforced by the downstream RecordSetWriter's
    schema (Avro on kafka_kc, inferred JSON otherwise) on serialize, which is
    the standard NiFi pattern for loosely-typed JSON records — there is no
    single-processor "CAST" primitive on `UpdateRecord` itself.
    """
    key = f"t{idx}__coerce"
    field_path = _to_record_path(str(rule.config.get("field", "")))
    target_type = str(rule.config.get("type", "string"))
    tail_key, tail_rel = tail
    reader_key, writer_key = ensure_json_record_services(builder)
    builder.add_processor(
        ProcessorSpec(
            key=key, name=key, type="org.apache.nifi.processors.standard.UpdateRecord",
            properties={
                "Record Reader": reader_key, "Record Writer": writer_key,
                "Replacement Value Strategy": "record-path-value",
                field_path: field_path,
                "Coerce Target Type (informational)": target_type,
            },
        )
    )
    builder.link(tail_key, key, [tail_rel])
    builder.to_dlq(key, "failure")
    return key, "success"


def _compile_remove_field(builder: "BlockBuilder", *, idx: int, rule: TransformRule, tail: Tail) -> Tail:
    key = f"t{idx}__remove_field"
    field_path = _to_record_path(str(rule.config.get("field", "")))
    tail_key, tail_rel = tail
    reader_key, writer_key = ensure_json_record_services(builder)
    builder.add_processor(
        ProcessorSpec(
            key=key, name=key, type="org.apache.nifi.processors.standard.RemoveRecordField",
            properties={"Record Reader": reader_key, "Record Writer": writer_key, "field_to_remove_1": field_path},
        )
    )
    builder.link(tail_key, key, [tail_rel])
    builder.to_dlq(key, "failure")
    return key, "success"


def _compile_dedup(
    builder: "BlockBuilder",
    *,
    flow: "Flow",
    block: FlowBlock,
    ctx: "CompileContext",
    flow_token: str,
    rule: TransformRule,
    tail: Tail,
    add_param,
) -> Tail:
    identity_fields = [f for f in (rule.config.get("identityFields") or []) if isinstance(f, str) and f.strip()]
    user_excludes = [f for f in (rule.config.get("excludedFields") or []) if isinstance(f, str) and f.strip()]
    window_hours = rule.config.get("windowHours", 24)
    if not isinstance(window_hours, (int, float)) or isinstance(window_hours, bool):
        window_hours = 24

    excludes = dedupe_preserve_order(user_excludes + DEDUP_PLATFORM_EXCLUDES)
    # `dedupEpoch` (T7.2+): a server-set, redeploy-only counter on this rule's
    # config, bumped by lifecycle.clear_dedup_cache() every time an operator
    # asks to clear the dedup cache for this block. Since Redis isn't
    # reachable from the app host (module docstring on
    # services/adapter/deployer/lifecycle.py), a real FLUSH of the block's
    # cache namespace isn't possible from here -- rewriting the cache-key
    # namespace on next redeploy is the honest substitute: every hash key
    # this Groovy script produces is prefixed with SRC, so bumping the epoch
    # makes every post-redeploy key miss the old namespace outright, which is
    # operationally identical to a flush for THIS block's keys. Epoch 0 (the
    # default -- never cleared) omits the suffix entirely so already-compiled
    # plans/fixtures for existing flows are byte-identical to before this
    # field existed; only a block that has actually had its cache cleared at
    # least once gets the `__e<epoch>` suffix.
    epoch_raw = rule.config.get("dedupEpoch")
    try:
        epoch = int(epoch_raw) if epoch_raw is not None else 0
    except (TypeError, ValueError):
        epoch = 0
    src = f"{flow_token}__{block.id}" + (f"__e{epoch}" if epoch else "")
    ttl = format_duration_hours(float(window_hours))

    hash_key = "dedupe__hash"
    detect_key = "dedupe__detect"
    redis_pool_key = "cs_redis_pool"
    redis_cache_key = "cs_redis_cache"

    redis_cfg = ctx.connection_config("redis")
    host = redis_cfg.get("host", "redis")
    port = redis_cfg.get("port", 6379)
    db_index = redis_cfg.get("dedupDb", 0)
    redis_password = redis_cfg.get("password")

    add_param("redis_connection_string", f"{host}:{port}", False)
    add_param("redis_password", redis_password, True)

    builder.add_cs(
        ControllerServiceSpec(
            key=redis_pool_key, name="redis_pool", type="org.apache.nifi.redis.service.RedisConnectionPoolService",
            properties={
                "Connection String": "#{redis_connection_string}",
                "Redis Mode": "Standalone",
                "Database Index": str(db_index),
                "Password": "#{redis_password}",
            },
        )
    )
    builder.add_cs(
        ControllerServiceSpec(
            key=redis_cache_key, name="dedupe_redis_cache",
            type="org.apache.nifi.redis.service.RedisDistributedMapCacheClientService",
            properties={"Redis Connection Pool": redis_pool_key, "TTL": ttl},
        )
    )

    tail_key, tail_rel = tail
    builder.add_processor(
        ProcessorSpec(
            key=hash_key, name=hash_key, type="org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
            properties={
                "Script Body": GROOVY_HASH_SCRIPT,
                "Failure Strategy": "rollback",
                "SRC": src,
                "EXCLUDES": ",".join(excludes),
                "IDENTITY_FIELDS": ",".join(identity_fields),
            },
        )
    )
    builder.link(tail_key, hash_key, [tail_rel])
    builder.to_dlq(hash_key, "failure")

    builder.add_processor(
        ProcessorSpec(
            key=detect_key, name=detect_key, type="org.apache.nifi.processors.standard.DetectDuplicate",
            properties={
                "Cache Entry Identifier": "${dedupe.key}",
                "Cache The Entry Identifier": "true",
                "Age Off Duration": ttl,
                "Distributed Cache Service": redis_cache_key,
            },
            autoTerminate=["duplicate"],
        )
    )
    builder.link(hash_key, detect_key, ["success"])
    builder.to_dlq(detect_key, "failure")
    return detect_key, "non-duplicate"
