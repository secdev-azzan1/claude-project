"""Approve schemas for entities outside flow-fs-cmdb.

Same builder as approve_fs_schemas.py: infer field types from records already in
the entity's topic, then append the 11 meta keys as ["null","string"].
Entities whose topic does not exist yet get a meta-only schema and are reported,
because a schema built with no sample would otherwise silently drop every real
data field from the Avro branch.
"""
import asyncio
import json
import os
import sys
import urllib.error
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

# (entity, flowId, avroBlockId, topic)
TARGETS = [
    ("event_pulling", "flow-9d7ask", "b-flz7ij-avro", "raw.fortisiem_pagination_test.event_pulling"),
]
VIP = ["organization", "device", "interface", "processor", "storage", "installed_software",
       "software_service", "software_patch", "device_custom_property",
       "device_business_service_membership"]
BLOCK = {"organization": "b-org-write", "device": "b-device-write", "interface": "b-interface-write",
         "processor": "b-processor-write", "storage": "b-storage-write",
         "installed_software": "b-installed-software-write",
         "software_service": "b-software-service-write", "software_patch": "b-software-patch-write",
         "device_custom_property": "b-device-custom-property-write",
         "device_business_service_membership": "b-device-business-service-membership-write"}
for e in VIP:
    TARGETS.append((e, "flow-vipjvz", f"{BLOCK[e]}-avro", f"raw.fortisiem_device_inventory.{e}"))


def avro_type(v):
    if isinstance(v, bool):
        return "boolean"
    if isinstance(v, int):
        return "long"
    if isinstance(v, float):
        return "double"
    return "string"


def build(entity, topic, samples):
    types = {}
    for rec in samples:
        for k, v in rec.items():
            if v is None:
                types.setdefault(k, "string")
                continue
            t = avro_type(v)
            prev = types.get(k)
            types[k] = "string" if prev and prev != t else t
    for m in META:
        types[m] = "string"
    return {"type": "record", "name": entity, "namespace": topic.rsplit(".", 1)[0],
            "fields": [{"name": k, "type": ["null", types[k]], "default": None} for k in sorted(types)]}


def approve(entity, flow_id, block_id, topic, avro):
    body = {"flowId": flow_id, "blockId": block_id, "entity": entity, "topic": topic,
            "subject": f"{topic}-value", "provenance": "sample_run", "avro": avro}
    req = urllib.request.Request(f"{BASE}/api/v2/schemas/approve", data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return True, json.loads(r.read()).get("id")
    except urllib.error.HTTPError as e:
        return False, e.read().decode()[:200]


async def main():
    base = os.environ.get("KAFBAT_URL", "")
    u = os.environ.get("KAFBAT_USERNAME") or None
    p = os.environ.get("KAFBAT_PASSWORD") or None
    meta_only = []
    for entity, flow_id, block_id, topic in TARGETS:
        samples = []
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
        avro = build(entity, topic, samples)
        if not samples:
            meta_only.append(entity)
        ok, info = approve(entity, flow_id, block_id, topic, avro)
        print("%-38s samples=%-4s fields=%-4s %s %s" % (
            entity, len(samples), len(avro["fields"]), "OK  " if ok else "FAIL", info))
    if meta_only:
        print("\nMETA-ONLY (no sample data available, Avro would carry only the 11 meta keys):")
        for e in meta_only:
            print("   ", e)


asyncio.run(main())
