"""DeploymentPlan.topics -> live Kafka topics (T7.2).

Thin orchestration over `services/kafka_client.py`. As that module's own
docstring says, the broker is not reachable by TCP from this host in this
environment — every `kafka_conn` dict passed in here therefore carries a
Kafbat management-API fallback (`kafka_connection_mode`, `kafbat_url`,
`kafbat_username`, `kafbat_password`), and `ensure_topics`/`count_topic`
already fall back to it transparently via `kafka_client.py`'s own native
Kafka-then-Kafbat logic.

`delete_topic` is the one operation `kafka_client.py` does not expose at
all (it has topic *creation* and *message clearing* — via
`.../topics/{topic}/messages` — but no `.../topics/{topic}` DELETE): this
module adds it directly against the Kafbat REST API, reusing that module's
private `_kafbat_login`/`_kafbat_get_cluster_name` helpers (login + cluster
name resolution) rather than duplicating them, since `services/kafka_client.py`
itself is out of scope for this task's edits.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List
from urllib.parse import quote

import httpx

from services import kafka_client
from services.adapter.compiler.ir import TopicSpec
from services.http_tls import tls_verify_enabled

logger = logging.getLogger(__name__)


async def ensure_topics(kafka_conn: Dict[str, Any], topics: List[TopicSpec]) -> List[Dict[str, Any]]:
    """Create-or-verify every topic in `topics` (data topics + the flow's
    single DLQ topic — the caller passes both kinds through the same list,
    `TopicSpec.kind` is metadata for the caller, not a branch here)."""
    results = []
    for spec in topics:
        r = await kafka_client.ensure_topic_exists(kafka_conn, spec.name)
        results.append({"name": spec.name, "kind": spec.kind, "ok": bool(r.get("ok")), "error": r.get("error")})
    return results


async def empty_topic(kafka_conn: Dict[str, Any], topic: str) -> Dict[str, Any]:
    """Clear all retained records in `topic` (used by undeploy — data
    topics are emptied, never deleted, so an adopted downstream consumer
    never loses the topic itself)."""
    return await kafka_client.clear_topic_messages(
        bootstrap_servers=kafka_conn.get("endpoint") or "",
        topic=topic,
        security_protocol=kafka_conn.get("security_protocol") or "PLAINTEXT",
        sasl_mechanism=kafka_conn.get("sasl_mechanism"),
        sasl_username=kafka_conn.get("sasl_username"),
        sasl_password=kafka_conn.get("sasl_password"),
        kafbat_url=kafka_conn.get("kafbat_url"),
        kafbat_username=kafka_conn.get("kafbat_username"),
        kafbat_password=kafka_conn.get("kafbat_password"),
    )


async def count_topic(kafka_conn: Dict[str, Any], topic: str) -> Dict[str, Any]:
    return await kafka_client.get_topic_message_count(
        bootstrap_servers=kafka_conn.get("endpoint") or "",
        topic=topic,
        security_protocol=kafka_conn.get("security_protocol") or "PLAINTEXT",
        sasl_mechanism=kafka_conn.get("sasl_mechanism"),
        sasl_username=kafka_conn.get("sasl_username"),
        sasl_password=kafka_conn.get("sasl_password"),
        kafbat_url=kafka_conn.get("kafbat_url"),
        kafbat_username=kafka_conn.get("kafbat_username"),
        kafbat_password=kafka_conn.get("kafbat_password"),
    )


async def delete_topic(kafka_conn: Dict[str, Any], topic: str) -> Dict[str, Any]:
    """Delete `topic` entirely (used by `lifecycle.delete` — DLQ + owned
    data topics — never by `undeploy`, which only empties them)."""
    kafbat_url = (kafka_conn.get("kafbat_url") or "").rstrip("/")
    if not kafbat_url:
        return {"ok": False, "error": "No Kafbat URL configured — topic deletion needs the Kafbat management API.", "error_code": "KAFBAT_NOT_CONFIGURED"}

    try:
        async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=30.0, follow_redirects=True) as client:
            await kafka_client._kafbat_login(client, kafbat_url, kafka_conn.get("kafbat_username"), kafka_conn.get("kafbat_password"))
            cluster = await kafka_client._kafbat_get_cluster_name(client, kafbat_url)
            if not cluster:
                return {"ok": False, "error": "Could not determine the Kafbat cluster name.", "error_code": "KAFBAT_CLUSTER_NOT_FOUND"}

            resp = await client.delete(f"{kafbat_url}/api/clusters/{quote(cluster, safe='')}/topics/{quote(topic, safe='')}")
            if resp.status_code == 404:
                # Already gone — deletion is idempotent, same convention as
                # kafka_connect_client.delete_connector.
                return {"ok": True, "topic": topic, "message": "Topic already deleted (not found)."}
            if resp.status_code not in (200, 202, 204):
                return {"ok": False, "error": f"Kafbat topic delete returned {resp.status_code}: {resp.text[:200]}", "error_code": "KAFBAT_DELETE_FAILED"}
            return {"ok": True, "topic": topic}
    except Exception as exc:
        logger.exception("delete_topic(%s) failed", topic)
        return {"ok": False, "error": f"Kafbat topic delete failed: {str(exc)[:200]}", "error_code": "KAFBAT_DELETE_FAILED"}
