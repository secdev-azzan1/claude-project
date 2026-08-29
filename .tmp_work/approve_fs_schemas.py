"""Build + approve an Avro schema per FortiSIEM entity.

Fields are inferred from records already sitting in each entity's raw topic,
then the 11 meta keys are appended as ["null","string"] — matching the types an
already-approved SentinelOne schema uses for exactly those keys.

`agent_status` has no source data at all (the endpoint returns
`<Statuses></Statuses>` for every device), so it gets a meta-only schema: enough
to satisfy the ceremony and let the flow deploy, without inventing data fields.
"""
import asyncio
import json
import os
import sys
import urllib.request

sys.path.insert(0, "backend")
for line in open("backend/.env", encoding="utf-8"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())

from services.kafka_client import _kafbat_recent_topic_messages  # noqa: E402

BASE = "http://localhost:8010"

META = ["source_platform", "customer_tenant_organization", "source_object_type",
        "source_object_id", "object_id", "cursor_window",
        "api_endpoint_export_query_identity", "ingest_ts", "extraction_timestamp",
        "ingestion_run_batch_identity", "source_event_update_timestamp"]

# entity -> (flowId, avro blockId, topic)
TARGETS = {}
for e in ["case", "report", "monitor", "task", "rule", "user", "watchlist", "lookup_table"]:
    TARGETS[e] = ("flow-fs-cmdb", f"b-{e}-k-avro", f"raw.fortisiem_cmdb_catalog.{e}")
TARGETS["incident"] = ("flow-fs-incident", "b-inc-k-avro", "raw.fortisiem_incident.incident")
TARGETS["agent_status"] = ("flow-fs-agent-status", "b-agent-k-avro",
                           "raw.fortisiem_agent_status.agent_status")


def avro_type(v):
    if isinstance(v, bool):
        return "boolean"
    if isinstance(v, int):
        return "long"
    if isinstance(v, float):
        return "double"
    return "string"          # strings, nulls, and anything nested -> string


def build_avro(entity, topic, samples):
    types = {}
    for rec in samples:
        if not isinstance(rec, dict):
            continue
        for k, v in rec.items():
            if v is None:
                types.setdefault(k, "string")
                continue
            t = avro_type(v)
            prev = types.get(k)
            # widen on conflict; string wins
            types[k] = "string" if prev and prev != t else t
    for m in META:
        types[m] = "string"
    fields = [{"name": k, "type": ["null", types[k]], "default": None} for k in sorted(types)]
    return {"type": "record", "name": entity, "namespace": topic.rsplit(".", 1)[0], "fields": fields}


def approve(entity, flow_id, block_id, topic, avro):
    body = {"flowId": flow_id, "blockId": block_id, "entity": entity, "topic": topic,
            "subject": f"{topic}-value", "provenance": "sample_run", "avro": avro}
    req = urllib.request.Request(f"{BASE}/api/v2/schemas/approve", data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            out = json.loads(r.read())
        return True, out.get("id") or out.get("subject")
    except urllib.error.HTTPError as e:
        return False, e.read().decode()[:300]


async def main():
    base = os.environ.get("KAFBAT_URL", "")
    u = os.environ.get("KAFBAT_USERNAME") or None
    p = os.environ.get("KAFBAT_PASSWORD") or None

    for entity, (flow_id, block_id, topic) in TARGETS.items():
        samples = []
        if entity != "agent_status":
            r = await _kafbat_recent_topic_messages(base, u, p, topic, limit=40)
            for m in (r.get("messages") or []):
                v = m.get("value")
                if isinstance(v, str):
                    try:
                        v = json.loads(v)
                    except Exception:
                        continue
                if isinstance(v, dict):
                    samples.append(v)
        avro = build_avro(entity, topic, samples)
        ok, info = approve(entity, flow_id, block_id, topic, avro)
        print("%-14s samples=%-4s fields=%-4s %s %s" % (
            entity, len(samples), len(avro["fields"]), "OK  " if ok else "FAIL", info))


asyncio.run(main())
