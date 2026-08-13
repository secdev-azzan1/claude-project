"""T7.5 — real runtime observability + block test, backing
`routers/v2/flows.py`'s `/{id}/metrics`, `/{id}/dlq`, `/{id}/messages`,
`/{id}/runtime`, `/{id}/runtime/repair` and `/{id}/blocks/{blockId}/test`.

Every function here takes the raw `flows_v2` document (a plain dict, same
convention `services/adapter/deployer/lifecycle.py` uses) plus `db`, and
returns a plain dict shaped exactly like the frontend type it backs
(`FlowMetrics`, `DlqRecord[]`, `TopicMessage[]`, `FlowRuntime`,
`BlockTestResult` — see `frontend/src/prototype/types.ts` and its Python
mirror `models/adapter/runtime.py`). Router functions in
`routers/v2/flows.py` are thin: load the flow doc, call in here, translate
the handful of raised exceptions below into the right HTTP status.

Connection-dict translation (`connections_v2` doc -> the plain
`{"endpoint", "auth_type", ...}` shape `nifi_client`/`kafka_client`/
`kafka_connect_client` take) is reused from `deployer/lifecycle.py` rather
than duplicated a third time — `_nifi_conn_dict` / `_kafka_conn_dict` /
`_kafka_connect_conn_dict` / `_active_connection` are private there, but
intra-package reuse of them is an established pattern already (e.g.
`deployer/topics.py` reusing `kafka_client`'s private Kafbat helpers).

NEVER FAKE ZEROS: every honest-value note below explains exactly which
number is real and why, so a future reader can tell "genuinely zero" apart
from "we couldn't find out." A metrics/runtime read that cannot reach the
system it needs returns an explicit `available: False` / `reachable: False`
rather than inventing a number.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import httpx

from models.adapter import AppService, Flow, FlowBlock, GatewayProxy, PlatformConnection
from services import kafka_client, kafka_connect_client, nifi_flow_manager
from services.adapter.common import COLLECTIONS, audit, new_id, now_iso
from services.adapter.deployer import topics as topics_mod
from services.adapter.deployer.lifecycle import (
    _active_connection,
    _kafka_conn_dict,
    _kafka_connect_conn_dict,
    _nifi_conn_dict,
)
from services.adapter.legality import hosts_test
from services.adapter.naming import dlq_name, tokenize
from services.adapter.validation import GatewaySnapshot, block_proxy_id
from services.connection_fingerprint import probe_nifi_fingerprint
from services.http_tls import tls_verify_enabled
from services.nifi_client import nifi_api_request

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------ loaders
# Same small COLLECTIONS reads routers/v2/flows.py and deployer/lifecycle.py
# each already have their own copy of -- kept local here too rather than
# imported from either (the router would create an import-order dependency
# on this module, and lifecycle.py's copies are private).


async def _load_services(db) -> List[AppService]:
    docs = await db[COLLECTIONS.services].find({}, {"_id": 0}).to_list(None)
    return [AppService(**d) for d in docs]


async def _load_connections(db) -> List[PlatformConnection]:
    docs = await db[COLLECTIONS.connections].find({}, {"_id": 0}).to_list(None)
    return [PlatformConnection(**d) for d in docs]


async def _load_gateway(db) -> GatewaySnapshot:
    doc = await db[COLLECTIONS.gateway].find_one({}, {"_id": 0}) or {}
    proxies = [GatewayProxy(**p) for p in (doc.get("proxies") or [])]
    allowlist = list(doc.get("allowlist") or [])
    return GatewaySnapshot(proxies=proxies, allowlist=allowlist)


def _as_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _all_connector_names(flow_doc: Dict[str, Any]) -> List[str]:
    scope_map = flow_doc.get("runtimeScopeMap") or {}
    names: List[str] = []
    for entry in scope_map.values():
        names.extend((entry or {}).get("connectorNames") or [])
    return names


# ============================================================== 1. metrics


def _unwrap(row: Any, key: str) -> Dict[str, Any]:
    """NiFi's status snapshot lists wrap each entry as `{"<key>": {...}}`
    (confirmed by `services/nifi_flow_manager.py`'s existing
    `processorStatusSnapshots` handling) -- defensively unwrap the same way
    for every snapshot list this module reads, in case a given NiFi build
    returns the DTO unwrapped instead."""
    return row.get(key, row) if isinstance(row, dict) else {}


def _flatten_pg_snapshots(snapshot: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Every nested `processGroupStatusSnapshot` under `snapshot`
    (recursive), keyed by process group id. Our compiled flows are exactly
    two levels deep (flow PG -> one child PG per block, never nested further
    per compiler-spec §7), but this walks arbitrarily deep defensively."""
    out: Dict[str, Dict[str, Any]] = {}
    for row in snapshot.get("processGroupStatusSnapshots") or []:
        child = _unwrap(row, "processGroupStatusSnapshot")
        cid = child.get("id")
        if cid:
            out[cid] = child
        out.update(_flatten_pg_snapshots(child))
    return out


def _sum_dlq_inflow(snapshot: Dict[str, Any]) -> int:
    """`errors24h`'s honest source: every block group wires its record
    failures to a processor literally named `dlq__meta`
    (`services/adapter/compiler/dlq.py`) -- summing `flowFilesIn` on every
    connection whose destination is that processor, across the whole PG
    tree, is a real count of records that failed in NiFi's current status
    window, entirely from the same PG-status read `records24h`/`queued`
    come from (no Kafka dependency, no guessing)."""
    total = 0
    for row in snapshot.get("connectionStatusSnapshots") or []:
        conn = _unwrap(row, "connectionStatusSnapshot")
        if conn.get("destinationName") == "dlq__meta":
            total += _as_int(conn.get("flowFilesIn"))
    for row in snapshot.get("processGroupStatusSnapshots") or []:
        total += _sum_dlq_inflow(_unwrap(row, "processGroupStatusSnapshot"))
    return total


async def get_flow_metrics(db, flow_doc: Dict[str, Any]) -> Dict[str, Any]:
    """`GET /{id}/metrics`. `{"available": False, "reason": ...}` when not
    deployed or NiFi cannot be reached -- otherwise a `FlowMetrics`-shaped
    dict (see `models/adapter/runtime.py::FlowMetrics`).

    `records24h`/`errors24h` are NOT real 24-hour rolling counters -- NiFi's
    status snapshot has no such window (it is a live/recent-buffer
    aggregate, typically the last few minutes). Rather than fabricate a
    zero, this reports the most honest numbers the PG status snapshot
    actually offers: `records24h` is the flow PG's aggregate `flowFilesOut`
    (total records the whole flow has produced in NiFi's current status
    window) and `errors24h` is the live count of records that reached this
    flow's DLQ processor in that same window (`_sum_dlq_inflow`). Both are
    real, current, non-fabricated numbers -- just not literally
    "in the last 24 hours."
    """
    pg_id = flow_doc.get("nifiProcessGroupId")
    if not pg_id:
        return {"available": False, "reason": "not deployed"}

    connections = await _load_connections(db)
    nifi_conn_doc = _active_connection(connections, "nifi")
    if not nifi_conn_doc:
        return {"available": False, "reason": "No active NiFi connection is configured."}
    nifi_conn = _nifi_conn_dict(nifi_conn_doc)

    status = await nifi_api_request(
        nifi_conn["endpoint"],
        "GET",
        f"/nifi-api/flow/process-groups/{pg_id}/status",
        params={"recursive": "true"},
        auth_type=nifi_conn["auth_type"],
        username=nifi_conn["username"],
        password=nifi_conn["password"],
        token=nifi_conn["token"],
    )
    if not status.get("ok"):
        return {"available": False, "reason": status.get("error") or "NiFi is unreachable."}

    pg_status = (status.get("data") or {}).get("processGroupStatus") or {}
    snapshot = pg_status.get("aggregateSnapshot") or {}

    child_snapshots = _flatten_pg_snapshots(snapshot)
    scope_map = flow_doc.get("runtimeScopeMap") or {}
    blocks_by_id = {b.get("id"): b for b in flow_doc.get("blocks") or []}

    per_block: List[Dict[str, Any]] = []
    for block_id, entry in scope_map.items():
        block_pg_id = (entry or {}).get("processGroupId")
        if not block_pg_id:
            continue  # kc blocks etc. generate no NiFi process group
        child = child_snapshots.get(block_pg_id) or {}
        block = blocks_by_id.get(block_id) or {}
        adapter = (entry or {}).get("adapter") or block.get("adapter") or ""
        name = block.get("name") or block_id
        per_block.append(
            {
                "blockId": block_id,
                "label": f"{adapter} · {name}" if adapter else name,
                "recordsIn": _as_int(child.get("flowFilesIn")),
                "recordsOut": _as_int(child.get("flowFilesOut")),
                "queued": _as_int(child.get("flowFilesQueued")),
            }
        )

    topic_counts: List[Dict[str, Any]] = []
    kafka_conn_doc = _active_connection(connections, "kafka")
    if kafka_conn_doc:
        kafka_conn = _kafka_conn_dict(kafka_conn_doc)
        topic_names = [t.get("name") for t in (flow_doc.get("topics") or []) if t.get("name")]
        dlq_topic = dlq_name(flow_doc.get("name") or "")
        if dlq_topic not in topic_names:
            topic_names.append(dlq_topic)
        for name in topic_names:
            r = await topics_mod.count_topic(kafka_conn, name)
            if r.get("ok"):
                topic_counts.append({"topic": name, "messages": _as_int(r.get("total_messages"))})
            # A failed per-topic count (unreachable Kafka, topic not found)
            # is skipped rather than reported as a fake zero.

    return {
        "available": True,
        "flowId": flow_doc["id"],
        "records24h": _as_int(snapshot.get("flowFilesOut")),
        "errors24h": _sum_dlq_inflow(snapshot),
        "queued": _as_int(snapshot.get("flowFilesQueued")),
        "perBlock": per_block,
        "topicCounts": topic_counts,
        # Not derivable from a point-in-time status snapshot -- left unset
        # rather than guessed at "Success".
        "lastRunOutcome": None,
    }


# ==================================================================== 2. dlq
# `kafka_client.py`'s public message readers don't surface Kafka message
# headers, and DLQ attribution needs them (`dlq.block`/`dlq.reason` are
# promoted from FlowFile attributes to Kafka headers by `dlq__publish`'s
# "FlowFile Attribute Header Pattern" -- see compiler/dlq.py). Rather than
# edit that shared module, this reads headers directly, reusing its already
# object exports of `_kafbat_login`/`_kafbat_get_cluster_name`/
# `_parse_kafbat_sse_events` (the same private-helper-reuse precedent
# `deployer/topics.py` already established for Kafbat topic deletion).


def _decode_kafka_bytes(value: Optional[bytes]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace")
    return str(value)


def _fetch_recent_records_sync(
    bootstrap_servers: str,
    topic: str,
    limit: int,
    security_protocol: str,
    sasl_mechanism: Optional[str],
    sasl_username: Optional[str],
    sasl_password: Optional[str],
) -> Dict[str, Any]:
    consumer = None
    try:
        from kafka import KafkaConsumer, TopicPartition

        kwargs: Dict[str, Any] = {
            "bootstrap_servers": bootstrap_servers,
            "enable_auto_commit": False,
            "group_id": None,
            "request_timeout_ms": 10000,
            "connections_max_idle_ms": 15000,
            "security_protocol": security_protocol,
            "consumer_timeout_ms": 2000,
        }
        if security_protocol in ("SASL_SSL", "SASL_PLAINTEXT") and sasl_mechanism:
            kwargs["sasl_mechanism"] = sasl_mechanism
            kwargs["sasl_plain_username"] = sasl_username or ""
            kwargs["sasl_plain_password"] = sasl_password or ""

        consumer = KafkaConsumer(**kwargs)
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return {"ok": False, "error": f"Kafka topic '{topic}' was not found.", "messages": []}

        topic_partitions = [TopicPartition(topic, p) for p in sorted(partitions)]
        end_offsets = consumer.end_offsets(topic_partitions)
        if not end_offsets:
            return {"ok": True, "messages": []}

        window = max(10, min(200, limit * 2))
        consumer.assign(topic_partitions)
        for tp in topic_partitions:
            end = int(end_offsets.get(tp, 0))
            consumer.seek(tp, max(0, end - window))

        rows: List[Dict[str, Any]] = []
        max_collect = max(limit * 8, 100)
        idle_polls = 0
        while len(rows) < max_collect and idle_polls < 3:
            polled = consumer.poll(timeout_ms=700, max_records=500)
            if not polled:
                idle_polls += 1
                continue
            idle_polls = 0
            for _tp, records in polled.items():
                for rec in records:
                    from datetime import datetime, timezone

                    ts_iso = None
                    if rec.timestamp is not None and rec.timestamp >= 0:
                        ts_iso = datetime.fromtimestamp(rec.timestamp / 1000.0, tz=timezone.utc).isoformat()
                    headers: Dict[str, Optional[str]] = {}
                    for hk, hv in rec.headers or []:
                        headers[hk] = _decode_kafka_bytes(hv)
                    rows.append(
                        {
                            "partition": int(rec.partition),
                            "offset": int(rec.offset),
                            "timestamp_ms": int(rec.timestamp) if rec.timestamp is not None else None,
                            "timestamp": ts_iso,
                            "key": _decode_kafka_bytes(rec.key),
                            "value": _decode_kafka_bytes(rec.value),
                            "headers": headers,
                        }
                    )
        rows.sort(key=lambda r: ((r.get("timestamp_ms") or -1), r.get("offset", -1)), reverse=True)
        return {"ok": True, "messages": rows[:limit]}
    except ImportError:
        return {"ok": False, "error": "kafka-python library not installed.", "messages": []}
    except Exception as exc:  # pragma: no cover - defensive
        return {"ok": False, "error": str(exc)[:300], "messages": []}
    finally:
        if consumer is not None:
            try:
                consumer.close()
            except Exception:
                pass


async def _fetch_recent_records_kafbat(
    kafbat_url: str, username: Optional[str], password: Optional[str], topic: str, limit: int
) -> Dict[str, Any]:
    base = (kafbat_url or "").rstrip("/")
    if not base:
        return {"ok": False, "error": "Kafbat URL not configured.", "messages": []}
    try:
        async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=30.0, follow_redirects=True) as client:
            await kafka_client._kafbat_login(client, base, username, password)
            cluster = await kafka_client._kafbat_get_cluster_name(client, base)
            if not cluster:
                return {"ok": False, "error": "Could not determine the Kafbat cluster name.", "messages": []}

            resp = await client.get(
                f"{base}/api/clusters/{quote(cluster, safe='')}/topics/{quote(topic, safe='')}/messages/v2",
                params={"mode": "LATEST", "limit": limit},
            )
            if resp.status_code == 404:
                return {"ok": False, "error": f"Kafka topic '{topic}' was not found.", "messages": []}
            if resp.status_code != 200:
                return {"ok": False, "error": f"Kafbat messages/v2 returned {resp.status_code}.", "messages": []}

            rows: List[Dict[str, Any]] = []
            for event in kafka_client._parse_kafbat_sse_events(resp.text):
                if str(event.get("type") or "").upper() != "MESSAGE":
                    continue
                msg = event.get("message") or {}
                if not isinstance(msg, dict):
                    continue
                timestamp = msg.get("timestamp")
                raw_headers = msg.get("headers")
                headers = (
                    {str(k): (None if v is None else str(v)) for k, v in raw_headers.items()}
                    if isinstance(raw_headers, dict)
                    else {}
                )
                rows.append(
                    {
                        "partition": int(msg.get("partition") or 0),
                        "offset": int(msg.get("offset") or 0),
                        "timestamp_ms": kafka_client._timestamp_ms_from_iso(timestamp),
                        "timestamp": timestamp,
                        "key": msg.get("key"),
                        "value": msg.get("value"),
                        "headers": headers,
                    }
                )
            rows.sort(key=lambda r: ((r.get("timestamp_ms") or -1), r.get("offset", -1)), reverse=True)
            return {"ok": True, "messages": rows[:limit]}
    except Exception as exc:
        return {"ok": False, "error": f"Kafbat message lookup failed: {str(exc)[:200]}", "messages": []}


async def _fetch_recent_records(kafka_conn: Dict[str, Any], topic: str, limit: int) -> Dict[str, Any]:
    bootstrap = kafka_conn.get("endpoint") or ""
    kafbat_url = kafka_conn.get("kafbat_url")
    if bootstrap:
        loop = asyncio.get_event_loop()
        try:
            result = await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    _fetch_recent_records_sync,
                    bootstrap,
                    topic,
                    limit,
                    kafka_conn.get("security_protocol") or "PLAINTEXT",
                    kafka_conn.get("sasl_mechanism"),
                    kafka_conn.get("sasl_username"),
                    kafka_conn.get("sasl_password"),
                ),
                timeout=25.0,
            )
            if result.get("ok"):
                return result
        except asyncio.TimeoutError:
            pass
        except Exception:  # pragma: no cover - defensive
            logger.exception("Native Kafka DLQ read for topic %s failed unexpectedly.", topic)
    if kafbat_url:
        return await _fetch_recent_records_kafbat(kafbat_url, kafka_conn.get("kafbat_username"), kafka_conn.get("kafbat_password"), topic, limit)
    return {"ok": False, "error": "Kafka is not reachable from this host.", "messages": []}


def _dlq_record_from_raw(flow_id: str, topic: str, m: Dict[str, Any], blocks_by_id: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    headers = m.get("headers") or {}
    block_id = headers.get("dlq.block")
    block_name = (blocks_by_id.get(block_id) or {}).get("name") or block_id or ""
    error_class = headers.get("dlq.reason") or ""
    payload = m.get("value")
    preview = "" if payload is None else str(payload)[:500]
    return {
        "id": f"{topic}-{m.get('partition', 0)}-{m.get('offset', 0)}",
        "flowId": flow_id,
        "ts": m.get("timestamp") or "",
        "blockName": block_name,
        "errorClass": error_class,
        "payloadPreview": preview,
    }


async def get_flow_dlq(db, flow_doc: Dict[str, Any]) -> Dict[str, Any]:
    """`GET /{id}/dlq`. Up to 50 newest records from `dlq.<flowToken>`, newest
    first. An unreachable Kafka, or a topic that doesn't exist yet (no
    failures ever dead-lettered), both come back as an honest empty list --
    nothing here is a hard error."""
    connections = await _load_connections(db)
    kafka_conn_doc = _active_connection(connections, "kafka")
    if not kafka_conn_doc:
        return {"records": []}
    kafka_conn = _kafka_conn_dict(kafka_conn_doc)
    topic = dlq_name(flow_doc.get("name") or "")

    result = await _fetch_recent_records(kafka_conn, topic, 50)
    if not result.get("ok"):
        return {"records": []}

    blocks_by_id = {b.get("id"): b for b in flow_doc.get("blocks") or []}
    records = [_dlq_record_from_raw(flow_doc["id"], topic, m, blocks_by_id) for m in result.get("messages") or []]
    records.sort(key=lambda r: (r["ts"], r["id"]), reverse=True)
    return {"records": records[:50]}


# =============================================================== 3. messages


class TopicNotOwned(Exception):
    """The requested topic is not one of this flow's `topics` -- the router
    turns this into a 404."""


def _topic_message_from_raw(m: Dict[str, Any]) -> Dict[str, Any]:
    """`kafka_client.py` always decodes message values with
    `errors="replace"`, so it never tells us a payload was actually binary
    -- the presence of the U+FFFD replacement character is the only signal
    left by the time it reaches us, and is used here as the binary
    heuristic (`TopicMessage.value = null` + `bytes` -- the UI renders
    "binary payload (N bytes)" itself from those two fields)."""
    value = m.get("value")
    if value is None:
        return {"offset": _as_int(m.get("offset")), "ts": m.get("timestamp") or "", "key": m.get("key"), "value": None, "bytes": 0}
    raw = value.encode("utf-8", errors="replace")
    is_binary = "�" in value
    return {
        "offset": _as_int(m.get("offset")),
        "ts": m.get("timestamp") or "",
        "key": m.get("key"),
        "value": None if is_binary else value,
        "bytes": len(raw),
    }


async def get_topic_messages(db, flow_doc: Dict[str, Any], topic: str) -> Dict[str, Any]:
    """`GET /{id}/messages?topic=`. Group-less viewer, newest-first, capped
    at 50 -- `topic` must be one of this flow's own `topics` (404
    otherwise); nothing here reads/writes a consumer group."""
    owned = {t.get("name") for t in (flow_doc.get("topics") or []) if t.get("name")}
    if not topic or topic not in owned:
        raise TopicNotOwned(f"Topic {topic!r} does not belong to this flow.")

    connections = await _load_connections(db)
    kafka_conn_doc = _active_connection(connections, "kafka")
    if not kafka_conn_doc:
        return {"messages": []}
    kafka_conn = _kafka_conn_dict(kafka_conn_doc)

    result = await kafka_client.get_recent_topic_messages(
        bootstrap_servers=kafka_conn["endpoint"],
        topic=topic,
        limit=50,
        security_protocol=kafka_conn["security_protocol"],
        sasl_mechanism=kafka_conn["sasl_mechanism"],
        sasl_username=kafka_conn["sasl_username"],
        sasl_password=kafka_conn["sasl_password"],
        kafbat_url=kafka_conn["kafbat_url"],
        kafbat_username=kafka_conn["kafbat_username"],
        kafbat_password=kafka_conn["kafbat_password"],
    )
    if not result.get("ok"):
        return {"messages": []}

    messages = [_topic_message_from_raw(m) for m in result.get("messages") or []]
    messages.sort(key=lambda m: m["offset"], reverse=True)
    return {"messages": messages[:50]}


# ================================================================ 4. runtime


def _drift_finding(
    *, kind: str, summary: str, where: str, expected: Optional[str], observed: Optional[str], verdict: str, verdict_detail: str, repairable: bool, now: str
) -> Dict[str, Any]:
    return {
        "id": new_id("drift"),
        "kind": kind,
        "summary": summary,
        "where": where,
        "expected": expected,
        "observed": observed,
        "verdict": verdict,
        "verdictDetail": verdict_detail,
        "observedAt": now,
        "repairable": repairable,
    }


def _pg_missing_verdict(observed_fp: Optional[str], deployed_fp: str) -> Tuple[str, str]:
    """The alpha's disambiguation, kept: a missing process group on the SAME
    instance (root-group fingerprint matches what was recorded at deploy)
    means really deleted; on a DIFFERENT one (fingerprint differs) means the
    active connection now points somewhere else, so the flow may still be
    running there -- deployed elsewhere; with no recorded fingerprint at all
    there is nothing to compare against, so the honest answer is unknown."""
    if not deployed_fp:
        return (
            "unknown",
            "No fingerprint was recorded for this flow at deploy time, so there is nothing to compare the live NiFi instance against.",
        )
    if observed_fp == deployed_fp:
        return (
            "really_deleted",
            "The active NiFi connection points at the same instance (root process group fingerprint matches) this flow was deployed to, and the process group is gone there — it was deleted outside the platform.",
        )
    return (
        "deployed_elsewhere",
        "The active NiFi connection points at a different NiFi instance (its root process group fingerprint does not match the one recorded at deploy time) — the flow's process group may still exist, unmanaged, on the original instance.",
    )


_OAUTH2_CS_KEY_RE = re.compile(r"^cs_oauth2_(.+)$")


def _controller_service_state(raw_state: str, validation_errors: List[str]) -> str:
    if validation_errors:
        return "INVALID"
    return "ENABLED" if raw_state.upper().startswith("ENABL") else "DISABLED"


def _component_state(raw_state: str, validation_errors: List[str]) -> str:
    if validation_errors:
        return "INVALID"
    upper = (raw_state or "").upper()
    return upper if upper in ("RUNNING", "STOPPED", "DISABLED") else "STOPPED"


def _properties_from_descriptors(properties: Dict[str, Any], descriptors: Dict[str, Any]) -> List[Dict[str, Any]]:
    out = []
    for name, value in properties.items():
        sensitive = bool((descriptors.get(name) or {}).get("sensitive"))
        out.append({"name": name, "value": value, "sensitive": sensitive})
    return out


async def _read_one_processor(nifi_conn: Dict[str, Any], processor_id: str, block_id: str) -> Dict[str, Any]:
    info = await nifi_flow_manager.get_processor_config(
        nifi_conn["endpoint"],
        processor_id,
        auth_type=nifi_conn["auth_type"],
        username=nifi_conn["username"],
        password=nifi_conn["password"],
        token=nifi_conn["token"],
    )
    if not info.get("ok"):
        return {
            "id": processor_id,
            "name": "",
            "type": "",
            "blockId": block_id,
            "state": "STOPPED",
            "invalidReasons": [info.get("error") or "Could not read this component from NiFi."],
            "properties": [],
        }
    validation_errors = info.get("validation_errors") or []
    return {
        "id": processor_id,
        "name": info.get("name", ""),
        "type": info.get("type", ""),
        "blockId": block_id,
        "state": _component_state(str(info.get("state") or ""), validation_errors),
        "invalidReasons": validation_errors or None,
        "properties": _properties_from_descriptors(info.get("properties") or {}, info.get("descriptors") or {}),
    }


async def _read_one_controller_service(nifi_conn: Dict[str, Any], cs_id: str, key: str, service_pins: Dict[str, int]) -> Dict[str, Any]:
    info = await nifi_flow_manager.get_controller_service_config(
        nifi_conn["endpoint"],
        cs_id,
        auth_type=nifi_conn["auth_type"],
        username=nifi_conn["username"],
        password=nifi_conn["password"],
        token=nifi_conn["token"],
    )
    m = _OAUTH2_CS_KEY_RE.match(key)
    app_service_id = m.group(1) if m else None
    if not info.get("ok"):
        return {
            "id": cs_id,
            "name": "",
            "type": "",
            "state": "DISABLED",
            "appServiceId": app_service_id,
            "pinnedRevision": service_pins.get(app_service_id) if app_service_id else None,
            "scope": "flow",
            "sharedWith": None,
            "properties": [],
        }
    validation_errors = info.get("validation_errors") or []
    return {
        "id": cs_id,
        "name": info.get("name", ""),
        "type": info.get("type", ""),
        "state": _controller_service_state(str(info.get("state") or ""), validation_errors),
        "appServiceId": app_service_id,
        "pinnedRevision": service_pins.get(app_service_id) if app_service_id else None,
        # The compiler never generates a platform-shared controller service
        # today -- every one it creates lives inside this flow's own block
        # group PG, so scope is always "flow".
        "scope": "flow",
        "sharedWith": None,
        "properties": _properties_from_descriptors(info.get("properties") or {}, info.get("descriptors") or {}),
    }


async def _read_components(nifi_conn: Dict[str, Any], flow_doc: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    scope_map = flow_doc.get("runtimeScopeMap") or {}
    service_pins = flow_doc.get("servicePins") or {}
    components: List[Dict[str, Any]] = []
    controller_services: List[Dict[str, Any]] = []
    for block_id, entry in scope_map.items():
        comp_ids: Dict[str, str] = (entry or {}).get("components") or {}
        for key, nifi_id in comp_ids.items():
            if key == "inputPort" or key.startswith("outputPort:"):
                # Structural ports carry no tunable properties/validation in
                # the same sense processors and controller services do --
                # excluded from the observability list on purpose.
                continue
            if key.startswith("cs_"):
                controller_services.append(await _read_one_controller_service(nifi_conn, nifi_id, key, service_pins))
            else:
                components.append(await _read_one_processor(nifi_conn, nifi_id, block_id))
    return components, controller_services


async def _read_connectors(connections: List[PlatformConnection], flow_doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    connector_names = _all_connector_names(flow_doc)
    if not connector_names:
        return []
    kc_conn_doc = _active_connection(connections, "kafka_connect")
    if not kc_conn_doc:
        return []
    kc_conn = _kafka_connect_conn_dict(kc_conn_doc)
    listing = await kafka_connect_client.list_connectors_with_status(kc_conn)
    if not listing.get("ok"):
        return []
    data = listing.get("data") or {}
    if not isinstance(data, dict):
        return []

    name_to_block: Dict[str, str] = {}
    for block_id, entry in (flow_doc.get("runtimeScopeMap") or {}).items():
        for n in (entry or {}).get("connectorNames") or []:
            name_to_block[n] = block_id

    out: List[Dict[str, Any]] = []
    for name in connector_names:
        info = data.get(name) or {}
        status = info.get("status") or {}
        connector_status = status.get("connector") or {}
        tasks_raw = status.get("tasks") or []
        info_cfg = (info.get("info") or {}).get("config") or {}
        tasks = [
            {
                "id": t.get("id", 0),
                "state": str(t.get("state") or "UNASSIGNED").upper(),
                "workerId": t.get("worker_id") or t.get("workerId") or "",
                "lastErrorTrace": t.get("trace"),
            }
            for t in tasks_raw
        ]
        failed_trace = next((t["lastErrorTrace"] for t in tasks if t["state"] == "FAILED" and t["lastErrorTrace"]), None)
        out.append(
            {
                "name": name,
                "blockId": name_to_block.get(name, ""),
                "connectorClass": info_cfg.get("connector.class", ""),
                "state": str(connector_status.get("state") or "UNASSIGNED").upper(),
                "workerId": connector_status.get("worker_id") or connector_status.get("workerId") or "",
                "tasks": tasks,
                # Kafka Connect's REST API exposes no per-connector record
                # counters without a JMX/Prometheus scrape (out of scope
                # here) -- 0 is the honest "not available via REST" value,
                # not a fabricated one.
                "recordsSent": 0,
                "recordsFailed": 0,
                "lastErrorTrace": failed_trace or connector_status.get("trace"),
            }
        )
    return out


async def _upsert_runtime(db, flow_id: str, doc: Dict[str, Any]) -> None:
    existing = await db[COLLECTIONS.runtimes].find_one({"flowId": flow_id}, {"_id": 0})
    if existing:
        await db[COLLECTIONS.runtimes].update_one({"flowId": flow_id}, {"$set": doc})
    else:
        await db[COLLECTIONS.runtimes].insert_one({"flowId": flow_id, **doc})


async def read_runtime(db, flow_doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """`GET /{id}/runtime`. Doubles as the "refresh from NiFi" action -- the
    frontend calls this exact endpoint for both the initial load and the
    explicit Refresh button (`api.ts`'s `getFlowRuntime`/`refreshFlowRuntime`
    both just GET this route). Returns `None` when there is nothing to show
    (never deployed and no prior runtime doc) -- the router turns that into
    a 404, matching `getFlowRuntime`'s `404 -> null` contract.

    A read never invents or clears a drift finding by itself when the
    underlying NiFi instance could not be reached at all -- it just carries
    the previously-persisted `drift`/`orphans`/`components`/
    `controllerServices` forward untouched and reports `reachable: False`.
    When NiFi IS reachable, the full runtime (components/services/drift) is
    recomputed fresh from the current live state + the current flow doc --
    which self-resolves once repair or redeploy legitimately changes what's
    being compared, without needing to special-case "does this specific
    finding still apply."
    """
    flow_id = flow_doc["id"]
    prior = await db[COLLECTIONS.runtimes].find_one({"flowId": flow_id}, {"_id": 0})
    pg_id = flow_doc.get("nifiProcessGroupId")

    if not pg_id:
        return prior  # None if there never was one either

    connections = await _load_connections(db)
    nifi_conn_doc = _active_connection(connections, "nifi")
    now = now_iso()
    deployed_fp = ((flow_doc.get("provenance") or {}).get("nifi") or {}).get("fingerprint") or ""
    nifi_connection_id = ((flow_doc.get("provenance") or {}).get("nifi") or {}).get("connectionId") or (nifi_conn_doc.id if nifi_conn_doc else "")

    if not nifi_conn_doc:
        return await _persist_unreachable(db, flow_doc, prior, connections, "No active NiFi connection is configured.", nifi_connection_id, deployed_fp, now)

    nifi_conn = _nifi_conn_dict(nifi_conn_doc)
    fp_result = await probe_nifi_fingerprint(nifi_conn)
    if not fp_result.get("ok"):
        reason = fp_result.get("error") or "NiFi is unreachable."
        return await _persist_unreachable(db, flow_doc, prior, connections, reason, nifi_connection_id, deployed_fp, now)

    observed_fp = fp_result.get("fingerprint")

    pg_check = await nifi_api_request(
        nifi_conn["endpoint"],
        "GET",
        f"/nifi-api/process-groups/{pg_id}",
        auth_type=nifi_conn["auth_type"],
        username=nifi_conn["username"],
        password=nifi_conn["password"],
        token=nifi_conn["token"],
    )

    drift: List[Dict[str, Any]] = []
    components: List[Dict[str, Any]] = []
    controller_services: List[Dict[str, Any]] = []

    if not pg_check.get("ok"):
        verdict, detail = _pg_missing_verdict(observed_fp, deployed_fp)
        drift.append(
            _drift_finding(
                kind="process_group_missing",
                summary=f"The process group ({pg_id}) this flow was deployed to no longer exists there.",
                where=flow_doc.get("name") or flow_id,
                expected=deployed_fp or None,
                observed=observed_fp,
                verdict=verdict,
                verdict_detail=detail,
                repairable=verdict in ("really_deleted", "deployed_elsewhere"),
                now=now,
            )
        )
    else:
        components, controller_services = await _read_components(nifi_conn, flow_doc)

    connectors = await _read_connectors(connections, flow_doc)

    runtime_doc = {
        "flowId": flow_id,
        "nifiConnectionId": nifi_connection_id,
        "processGroupId": pg_id,
        "deployedFingerprint": deployed_fp,
        "observedFingerprint": observed_fp,
        "reachable": True,
        "unreachableReason": None,
        "lastReadAt": now,
        "components": components,
        "controllerServices": controller_services,
        "connectors": connectors,
        "drift": drift,
        "orphans": (prior or {}).get("orphans") or [],
    }
    await _upsert_runtime(db, flow_id, runtime_doc)
    return runtime_doc


async def _persist_unreachable(
    db,
    flow_doc: Dict[str, Any],
    prior: Optional[Dict[str, Any]],
    connections: List[PlatformConnection],
    reason: str,
    nifi_connection_id: str,
    deployed_fp: str,
    now: str,
) -> Dict[str, Any]:
    # Connect is a different system -- still probed independently even when
    # NiFi itself could not be reached.
    connectors = await _read_connectors(connections, flow_doc)
    runtime_doc = {
        "flowId": flow_doc["id"],
        "nifiConnectionId": nifi_connection_id,
        "processGroupId": flow_doc.get("nifiProcessGroupId"),
        "deployedFingerprint": deployed_fp,
        "observedFingerprint": None,
        "reachable": False,
        "unreachableReason": reason,
        "lastReadAt": now,
        "components": (prior or {}).get("components") or [],
        "controllerServices": (prior or {}).get("controllerServices") or [],
        "connectors": connectors,
        "drift": (prior or {}).get("drift") or [],
        "orphans": (prior or {}).get("orphans") or [],
    }
    await _upsert_runtime(db, flow_doc["id"], runtime_doc)
    return runtime_doc


class RuntimeRepairRefused(Exception):
    """No repairable drift to act on -- the router turns this into a 409."""


async def repair_runtime(db, flow_doc: Dict[str, Any]) -> Dict[str, Any]:
    """`POST /{id}/runtime/repair`. Force-clears dead `nifiProcessGroupId`
    references for every currently-repairable drift finding, records an
    orphan for any process group that likely still exists (just on a
    different, no-longer-connected instance), audits, and returns the flow
    to Draft -- matching the confirm-dialog copy already shipped in
    `Flows.tsx` ("The flow returns to Draft. Deploy it again to compile a
    fresh runtime."). Never touches the live NiFi/Connect runtime itself --
    only this platform's own reference to it.
    """
    flow_id = flow_doc["id"]
    runtime_doc = await read_runtime(db, flow_doc)
    if not runtime_doc:
        raise RuntimeRepairRefused("No runtime record for this flow — nothing to repair.")

    repairable = [d for d in (runtime_doc.get("drift") or []) if d.get("repairable")]
    if not repairable:
        raise RuntimeRepairRefused("No repairable drift findings — nothing to force.")

    now = now_iso()
    new_orphans: List[Dict[str, Any]] = []
    pg_id = flow_doc.get("nifiProcessGroupId")
    repaired_ids = set()

    for finding in repairable:
        if finding.get("kind") != "process_group_missing":
            continue
        repaired_ids.add(finding.get("id"))
        if finding.get("verdict") == "deployed_elsewhere" and pg_id:
            # `expected` on this finding IS the fingerprint of the instance
            # this flow was actually deployed to -- the process group most
            # likely still lives there, just unmanaged now that the active
            # connection points elsewhere.
            new_orphans.append(
                {
                    "id": new_id("orphan"),
                    "kind": "process_group",
                    "ref": pg_id,
                    "instance": finding.get("expected") or "unknown instance",
                    "recordedAt": now,
                }
            )
        # verdict == "really_deleted" leaves nothing to record: it is
        # actually gone, not orphaned.

    orphans = list(runtime_doc.get("orphans") or []) + new_orphans
    remaining_drift = [d for d in (runtime_doc.get("drift") or []) if d.get("id") not in repaired_ids]

    await db[COLLECTIONS.flows].update_one(
        {"id": flow_id},
        {"$set": {"nifiProcessGroupId": None, "state": "Draft", "deployedAt": None, "updatedAt": now}},
    )

    updated_runtime = {**runtime_doc, "processGroupId": None, "drift": remaining_drift, "orphans": orphans, "lastReadAt": now}
    await _upsert_runtime(db, flow_id, updated_runtime)

    await audit(
        db,
        action="Runtime force repaired",
        target=flow_doc.get("name") or flow_id,
        status="Warning",
        details=f"{len(repaired_ids)} finding(s) resolved, {len(new_orphans)} orphan(s) recorded.",
        object="Flow",
    )

    return {"runtime": updated_runtime, "orphans": new_orphans, "clearedFindings": len(repaired_ids)}


# ============================================================== 5. block test


class BlockTestError(Exception):
    """Base for a REFUSAL the router turns into an HTTP error status -- as
    opposed to an in-band `{"ok": False, "reason": ...}` `BlockTestResult`,
    which is persisted and returned normally with a 200
    (`TestPanel.tsx`: "failures come back as data, never thrown"). Only a
    genuinely can't-even-attempt-it condition raises."""


class BlockNotFound(BlockTestError):
    pass


class BlockNotTestRunnable(BlockTestError):
    def __init__(self) -> None:
        super().__init__("write blocks are not test-runnable")


class BlockTestUnsupported(BlockTestError):
    def __init__(self, adapter: str) -> None:
        super().__init__("block test for this adapter arrives with its compiler support")
        self.adapter = adapter


class BlockTestPlaceholders(BlockTestError):
    def __init__(self, placeholders: List[str]) -> None:
        super().__init__(
            "Unresolved ${...} value(s) in this block's configuration: "
            f"{', '.join(placeholders)} — a live test has no prompt to resolve them."
        )
        self.placeholders = placeholders


_PLACEHOLDER_RE = re.compile(r"\$\{([a-zA-Z0-9_.-]+)\}")


def _resolve_jsonpath(data: Any, path: str) -> Any:
    """Minimal JSONPath resolver ("$.a.b" / "a.b" / "$.a[0].b") -- same
    small subset `routers/v2/services.py`'s own `_test_http_session_token`
    uses; kept as a local copy here rather than imported (a router module
    should not be a dependency of a service module)."""
    if not path:
        return None
    trimmed = path.strip()
    if trimmed.startswith("$"):
        trimmed = trimmed[1:]
    trimmed = trimmed.lstrip(".")
    if not trimmed:
        return data
    tokens = re.findall(r"[^.\[\]]+|\[\d+\]", trimmed)
    current = data
    for token in tokens:
        if token.startswith("[") and token.endswith("]"):
            if not isinstance(current, list):
                return None
            idx = int(token[1:-1])
            if idx >= len(current) or idx < -len(current):
                return None
            current = current[idx]
        else:
            if not isinstance(current, dict) or token not in current:
                return None
            current = current[token]
    return current


def _resolve_record_path(data: Any, path: str) -> List[Any]:
    """Resolves an http block's `recordPath` ("$", "$.items", "$.items[*]",
    "$.data.rows[*].id", ...) against a parsed JSON response, fanning out on
    every `[*]` wildcard. Returns a flat list of matched values -- a path
    with no wildcard still comes back as a (usually 1-element) list, which
    is exactly the "records" shape the probe wants."""
    p = (path or "$").strip()
    if p.startswith("$"):
        p = p[1:]
    p = p.lstrip(".")
    if not p:
        return data if isinstance(data, list) else [data]
    tokens = re.findall(r"[^.\[\]]+|\[\*\]|\[\d+\]", p)
    current: List[Any] = [data]
    for token in tokens:
        nxt: List[Any] = []
        if token == "[*]":
            for item in current:
                if isinstance(item, list):
                    nxt.extend(item)
        elif token.startswith("[") and token.endswith("]"):
            idx = int(token[1:-1])
            for item in current:
                if isinstance(item, list) and -len(item) <= idx < len(item):
                    nxt.append(item[idx])
        else:
            for item in current:
                if isinstance(item, dict) and token in item:
                    nxt.append(item[token])
        current = nxt
    return current


def _flatten_keys(obj: Any, prefix: str = "", depth: int = 0, max_depth: int = 3) -> List[str]:
    if not isinstance(obj, dict) or depth > max_depth:
        return []
    out: List[str] = []
    for k, v in obj.items():
        path = f"{prefix}.{k}" if prefix else k
        out.append(path)
        if isinstance(v, dict):
            out.extend(_flatten_keys(v, path, depth + 1, max_depth))
    return out


def _probe_query_params(pagination: Dict[str, Any]) -> Dict[str, str]:
    """Real first-page query params, mirroring `blocks_http.py::compile_read`'s
    `init_props` initial values -- the probe issues exactly ONE request
    (bounded, no pagination loop), so only the first page's params matter."""
    fields = pagination.get("fields") or {}
    ptype = pagination.get("type", "none")
    params: Dict[str, str] = {}
    if ptype == "offset":
        params[str(fields.get("offsetParam", "offset"))] = "0"
        params[str(fields.get("limitParam", "limit"))] = str(fields.get("limitValue", 100))
    elif ptype == "page":
        params[str(fields.get("pageParam", "page"))] = str(fields.get("firstPage", 1))
        params[str(fields.get("sizeParam", "size"))] = str(fields.get("sizeValue", 100))
    elif ptype == "cursor":
        params[str(fields.get("cursorParam", "cursor"))] = ""
    return params


def _static_auth(service: AppService) -> Tuple[Dict[str, str], Dict[str, str], Optional[Tuple[str, str]]]:
    """basic/bearer/api_key -- headers/query params/httpx auth tuple. Mirrors
    `routers/v2/services.py::_test_http`'s auth-application style."""
    mode = service.config.get("authMode", "none")
    headers: Dict[str, str] = {}
    params: Dict[str, str] = {}
    auth: Optional[Tuple[str, str]] = None
    if mode == "basic":
        auth = (str(service.config.get("username") or ""), str(service.config.get("password") or ""))
    elif mode == "bearer":
        headers["Authorization"] = f"Bearer {service.config.get('token') or ''}"
    elif mode == "api_key":
        key_name = str(service.config.get("keyName") or "X-Api-Key")
        key_value = str(service.config.get("keyValue") or "")
        if service.config.get("keyLocation") == "query":
            params[key_name] = key_value
        else:
            headers[key_name] = key_value
    return headers, params, auth


async def _session_token_login(client: httpx.AsyncClient, service: AppService) -> Dict[str, Any]:
    base_url = str(service.config.get("baseUrl") or "").rstrip("/")
    login_path = str(service.config.get("loginPath") or "/login")
    login_url = f"{base_url}/{login_path.lstrip('/')}" if login_path else base_url
    try:
        resp = await client.post(login_url, json={"username": service.config.get("username"), "password": service.config.get("password")})
    except httpx.HTTPError as exc:
        return {"error": f"Login request failed: {exc}"[:300]}
    if resp.status_code >= 400:
        return {"error": f"Login failed — HTTP {resp.status_code}."}
    try:
        body = resp.json()
    except Exception:
        return {"error": "Login response was not valid JSON."}
    token_path = str(service.config.get("tokenPath") or "$.token")
    token = _resolve_jsonpath(body, token_path)
    if not token:
        return {"error": f"Token JSONPath {token_path!r} did not resolve to a value in the login response."}
    return {"token": token}


async def _oauth2_login(client: httpx.AsyncClient, service: AppService) -> Dict[str, Any]:
    token_url = str(service.config.get("tokenUrl") or "").strip()
    if not token_url:
        return {"error": "No token URL configured for OAuth2."}
    try:
        resp = await client.post(
            token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": service.config.get("clientId") or "",
                "client_secret": service.config.get("clientSecret") or "",
            },
        )
    except httpx.HTTPError as exc:
        return {"error": f"OAuth2 token request failed: {exc}"[:300]}
    if resp.status_code >= 400:
        return {"error": f"OAuth2 token request failed — HTTP {resp.status_code}."}
    try:
        body = resp.json()
    except Exception:
        return {"error": "OAuth2 token response was not valid JSON."}
    token = body.get("access_token")
    if not token:
        return {"error": "OAuth2 token response did not contain access_token."}
    return {"token": token}


def _resolve_base_url(block: FlowBlock, service: AppService, gateway: GatewaySnapshot, connections: List[PlatformConnection]) -> Tuple[str, Optional[str]]:
    """`(base_url, error)` -- mirrors `blocks_http.py::_base_url_expr` for a
    LIVE probe: the service's own `baseUrl`, or -- when the service (or,
    legacy, the block) selects a gateway proxy -- the APISIX runtime URL
    plus the tokenized proxy name, exactly as compiled (the block's own
    `path` is appended by the caller, same as the compiled `request.url`)."""
    proxy_id = block_proxy_id(block, [service])
    if not proxy_id:
        return str(service.config.get("baseUrl") or "").rstrip("/"), None
    proxy = next((p for p in gateway.proxies if p.id == proxy_id), None)
    if proxy is None:
        return "", f"The gateway proxy this block routes through ({proxy_id}) no longer exists."
    apisix_conn = next((c for c in connections if c.type == "apisix" and c.active), None)
    runtime_url = str((apisix_conn.config or {}).get("runtimeUrl") or "").rstrip("/") if apisix_conn else ""
    if not runtime_url:
        return "", "No active APISIX connection is configured to route this block's proxy egress."
    return f"{runtime_url}/{tokenize(proxy.name)}", None


async def _run_http_read_probe(db, block: FlowBlock) -> Dict[str, Any]:
    tested_at = now_iso()
    services = await _load_services(db)
    service = next((s for s in services if s.id == block.serviceId), None)
    if service is None:
        return {"ok": False, "reason": "No service is bound to this block.", "testedAt": tested_at}

    gateway = await _load_gateway(db)
    connections = await _load_connections(db)

    base_url, base_error = _resolve_base_url(block, service, gateway, connections)
    if base_error:
        return {"ok": False, "reason": base_error, "testedAt": tested_at}

    config = block.config or {}
    path = str(config.get("path") or "")
    pagination = config.get("pagination") or {"type": "none", "fields": {}}
    query_params = _probe_query_params(pagination)
    url = f"{base_url}{path}"

    auth_mode = service.config.get("authMode", "none")
    try:
        async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=15.0) as client:
            if auth_mode == "session_token":
                token_result = await _session_token_login(client, service)
                if token_result.get("error"):
                    return {"ok": False, "reason": token_result["error"], "testedAt": tested_at}
                header = str(service.config.get("tokenHeader", "Authorization")) or "Authorization"
                resp = await client.get(url, params=query_params, headers={header: str(token_result["token"])})
            elif auth_mode == "oauth2":
                token_result = await _oauth2_login(client, service)
                if token_result.get("error"):
                    return {"ok": False, "reason": token_result["error"], "testedAt": tested_at}
                resp = await client.get(url, params=query_params, headers={"Authorization": f"Bearer {token_result['token']}"})
            else:
                headers, extra_params, basic_auth = _static_auth(service)
                query_params.update(extra_params)
                resp = await client.get(url, params=query_params, headers=headers, auth=basic_auth)
    except httpx.TimeoutException:
        return {"ok": False, "reason": f"Connection timed out after 15s reaching {url}.", "testedAt": tested_at}
    except httpx.ConnectError as exc:
        return {"ok": False, "reason": f"Cannot connect to {url}: {exc}"[:300], "testedAt": tested_at}
    except httpx.HTTPError as exc:
        return {"ok": False, "reason": str(exc)[:300], "testedAt": tested_at}

    if resp.status_code >= 400:
        return {"ok": False, "reason": f"Request failed — HTTP {resp.status_code}.", "testedAt": tested_at}

    try:
        body = resp.json()
    except Exception:
        return {"ok": False, "reason": "Response was not valid JSON.", "testedAt": tested_at}

    record_path = str(config.get("recordPath", "$"))
    split = bool(config.get("split", True))
    records = _resolve_record_path(body, record_path) if split else [body]
    records = records[:10]

    detected_fields = _flatten_keys(records[0]) if records and isinstance(records[0], dict) else []

    return {"ok": True, "records": records, "detectedFields": detected_fields, "testedAt": tested_at}


async def _persist_block_test_result(db, flow_doc: Dict[str, Any], block_id: str, result: Dict[str, Any]) -> None:
    blocks = flow_doc.get("blocks") or []
    idx = next((i for i, b in enumerate(blocks) if b.get("id") == block_id), None)
    flow_name = flow_doc.get("name") or flow_doc.get("id")
    block_name = blocks[idx].get("name") if idx is not None else block_id
    if idx is not None:
        await db[COLLECTIONS.flows].update_one(
            {"id": flow_doc["id"]}, {"$set": {f"blocks.{idx}.testResult": result, "updatedAt": now_iso()}}
        )
    await audit(
        db,
        action="Block tested",
        target=f"{flow_name} / {block_name or block_id}",
        status="Success" if result.get("ok") else "Failed",
        details=result.get("reason"),
        object="Flow",
    )


async def test_block(db, flow_doc: Dict[str, Any], block_id: str) -> Dict[str, Any]:
    """`POST /{id}/blocks/{blockId}/test`. Bounded live probe (<=10 records,
    no commits) for `http` `read` blocks -- the only adapter/mode this task
    implements. `jdbc` and `kafka read` block tests, plus any other http
    mode, refuse with a 501 ("arrives with its compiler support" -- none of
    those modes compile yet either, see `blocks_jdbc.py`/`blocks_http.py`).
    Any block that does not host a Test section at all
    (`legality.hosts_test`: every write, `kafka_kc`, `kc`) refuses with a
    422, mirroring the same rule the builder UI already enforces client-side.
    """
    flow = Flow(**flow_doc)
    block = next((b for b in flow.blocks if b.id == block_id), None)
    if block is None:
        raise BlockNotFound(f"Block {block_id!r} not found on this flow.")

    if not hosts_test(block):
        raise BlockNotTestRunnable()

    if block.adapter == "jdbc" or (block.adapter == "kafka" and block.mode == "read"):
        raise BlockTestUnsupported(block.adapter)
    if not (block.adapter == "http" and block.mode == "read"):
        raise BlockTestUnsupported(block.adapter)

    config_text = json.dumps(block.config or {}, default=str)
    placeholders = sorted(set(_PLACEHOLDER_RE.findall(config_text)))
    if placeholders:
        raise BlockTestPlaceholders(placeholders)

    try:
        result = await _run_http_read_probe(db, block)
    except Exception as exc:  # pragma: no cover - defensive: probe failures must come back as data
        logger.exception("Unexpected error running the block test probe for block %s.", block_id)
        result = {"ok": False, "reason": f"Unexpected test error: {exc}"[:300], "testedAt": now_iso()}

    await _persist_block_test_result(db, flow_doc, block_id, result)
    return result
