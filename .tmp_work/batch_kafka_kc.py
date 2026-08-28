import asyncio
import copy
import json
import sys
import time

sys.path.insert(0, "backend")
import os
os.environ.setdefault("MONGO_URL", "mongodb://localhost:27018")
os.environ.setdefault("DB_NAME", "dmp_platform")

import httpx
import db as dbmod
from services.adapter.deployer import nifi_apply
from services.adapter.deployer.lifecycle import _load_connections, _active_connection, _nifi_conn_dict
from services.nifi_client import nifi_api_request
from services.adapter.naming import tokenize, base_topic_name

APP_BASE = "http://127.0.0.1:8000/api/v2/flows"
SCHEMA_BASE = "http://127.0.0.1:8000/api/v2/schemas"
CATALOG_SERVICE_ID = "svc-dmya5u"


def log(msg):
    print(msg, flush=True)


def block_id_for(entity):
    return "b-" + entity.replace("_", "-") + "-avro-write"


def display_name_for(entity):
    return "Publish " + " ".join(w.capitalize() for w in entity.split("_")) + " Avro"


async def find_trigger_processor(url, auth, pg_id, _label=None):
    r = await nifi_api_request(url, "GET", f"/nifi-api/flow/process-groups/{pg_id}", **auth)
    if not r.get("ok"):
        if _label:
            log(f"{_label} process-group fetch failed: {r}")
        return None
    flow = r["data"]["processGroupFlow"]["flow"]
    for p in flow.get("processors", []):
        c = p["component"]
        if c["name"] == "trigger" and "GenerateFlowFile" in c["type"]:
            return c["id"]
    for g in flow.get("processGroups", []):
        found = await find_trigger_processor(url, auth, g["component"]["id"])
        if found:
            return found
    return None


async def get_metrics_patient(client, flow_id, attempts=3, timeout=150):
    for i in range(attempts):
        try:
            r = await client.get(f"{APP_BASE}/{flow_id}/metrics", timeout=timeout)
            return r.json()
        except (httpx.ReadTimeout, httpx.ConnectTimeout):
            log(f"[{flow_id}]   metrics timed out (attempt {i+1}/{attempts}), retrying")
    return {}


async def prepare_entity_block(client, flow_id, flow_name, kafka_block):
    entity = kafka_block["entity"]
    bronze_topic = kafka_block.get("topicOverride")
    parent_id = kafka_block["parentId"]
    branch = kafka_block.get("branch")
    transforms = kafka_block.get("transforms") or []

    if not bronze_topic:
        return {"entity": entity, "ok": False, "error": "raw write block has no topicOverride"}

    r = await client.get(f"{APP_BASE}/{flow_id}/messages", params={"topic": bronze_topic, "limit": 15})
    msgs = r.json().get("messages", [])
    records = []
    for m in msgs:
        try:
            records.append(json.loads(m["value"]))
        except Exception:
            pass
    if not records:
        return {"entity": entity, "ok": False, "error": f"no sample records on {bronze_topic}"}

    ndjson = "\n".join(json.dumps(rec) for rec in records)
    namespace = f"raw.{tokenize(flow_name)}"
    files = {"files": (f"{entity}.ndjson", ndjson, "application/x-ndjson")}
    data = {"name": entity, "namespace": namespace}
    r = await client.post(f"{SCHEMA_BASE}/infer", files=files, data=data)
    if r.status_code != 200:
        return {"entity": entity, "ok": False, "error": f"infer failed {r.status_code}: {r.text[:300]}"}
    avro = r.json()["avro"]

    field_names = {f["name"] for f in (avro.get("fields") or [])}
    if "ingest_id" not in field_names:
        avro.setdefault("fields", []).append({"name": "ingest_id", "type": ["null", "string"], "default": None})

    new_block_id = block_id_for(entity)
    topic = base_topic_name(flow_name, entity)
    subject = f"{topic}-value"
    approve_body = {
        "flowId": flow_id,
        "blockId": new_block_id,
        "entity": entity,
        "topic": topic,
        "subject": subject,
        "provenance": "sample_run",
        "evidence": f"Inferred from {len(records)} sample records already landed on {bronze_topic}",
        "avro": avro,
    }
    r = await client.post(f"{SCHEMA_BASE}/approve", json=approve_body)
    if r.status_code != 200:
        return {"entity": entity, "ok": False, "error": f"approve failed {r.status_code}: {r.text[:300]}"}
    approve_result = r.json()

    new_transforms = []
    for t in transforms:
        nt = copy.deepcopy(t)
        nt["id"] = f"{t['id']}-avro"
        new_transforms.append(nt)

    new_branch = None
    if branch:
        new_branch = copy.deepcopy(branch)
        if new_branch.get("name"):
            new_branch["name"] = f"{new_branch['name']}-avro"

    new_block = {
        "id": new_block_id,
        "adapter": "kafka_kc",
        "mode": None,
        "name": display_name_for(entity),
        "parentId": parent_id,
        "branch": new_branch,
        "serviceId": CATALOG_SERVICE_ID,
        "entity": entity,
        "config": {},
        "transforms": new_transforms,
        "topicOverride": None,
        "testResult": None,
    }
    return {
        "entity": entity,
        "ok": True,
        "new_block": new_block,
        "topic": topic,
        "registryGlobalId": approve_result.get("registryGlobalId"),
        "transform_count": len(new_transforms),
    }


async def process_flow(client, url, auth, flow_id, allowed_entities=None):
    log(f"\n===== {flow_id} =====")
    fr = await client.get(f"{APP_BASE}/{flow_id}")
    if fr.status_code != 200:
        log(f"[{flow_id}] could not fetch flow doc: {fr.status_code}")
        return {"flow_id": flow_id, "ok": False, "error": "fetch failed"}
    flow_doc = fr.json()
    flow_name = flow_doc["name"]
    pg_id = flow_doc.get("nifiProcessGroupId")

    existing_kc_entities = {b["entity"] for b in flow_doc["blocks"] if b.get("adapter") == "kafka_kc"}
    kafka_writes = [
        b for b in flow_doc["blocks"]
        if b.get("adapter") == "kafka" and b.get("mode") == "write" and b.get("entity") not in existing_kc_entities
    ]
    if allowed_entities is not None:
        skipped = [b["entity"] for b in kafka_writes if b["entity"] not in allowed_entities]
        if skipped:
            log(f"[{flow_id}] skipping out-of-scope entities: {skipped}")
        kafka_writes = [b for b in kafka_writes if b["entity"] in allowed_entities]
    if not kafka_writes:
        log(f"[{flow_id}] nothing to do (all entities already have kafka_kc)")
        return {"flow_id": flow_id, "ok": True, "skipped": True}

    results = []
    for kb in kafka_writes:
        log(f"[{flow_id}] preparing entity={kb['entity']}")
        res = await prepare_entity_block(client, flow_id, flow_name, kb)
        results.append(res)
        if res["ok"]:
            log(f"[{flow_id}]   OK topic={res['topic']} globalId={res['registryGlobalId']} transforms={res['transform_count']}")
        else:
            log(f"[{flow_id}]   FAILED: {res['error']}")

    good = [r for r in results if r["ok"]]
    bad = [r for r in results if not r["ok"]]
    if not good:
        log(f"[{flow_id}] no entities succeeded, skipping deploy")
        return {"flow_id": flow_id, "ok": False, "results": results}

    flow_doc["blocks"] = flow_doc["blocks"] + [r["new_block"] for r in good]
    r = await client.post(f"{APP_BASE}/", json=flow_doc)
    if r.status_code != 200:
        log(f"[{flow_id}] SAVE FAILED {r.status_code}: {r.text[:500]}")
        return {"flow_id": flow_id, "ok": False, "error": "save failed", "detail": r.text[:500], "results": results}
    log(f"[{flow_id}] blocks inserted ({len(good)} new)")

    r = await client.post(f"{APP_BASE}/{flow_id}/validate")
    issues = r.json()
    if issues:
        log(f"[{flow_id}] VALIDATE FAILED: {json.dumps(issues)[:800]}")
        return {"flow_id": flow_id, "ok": False, "error": "validate failed", "issues": issues, "results": results}
    log(f"[{flow_id}] validate clean")

    r = await client.post(f"{APP_BASE}/{flow_id}/verbs/redeploy", timeout=60)
    if r.status_code != 200:
        log(f"[{flow_id}] REDEPLOY FAILED {r.status_code}: {r.text[:500]}")
        return {"flow_id": flow_id, "ok": False, "error": "redeploy failed", "results": results}
    redeployed_doc = r.json()
    pg_id = redeployed_doc.get("nifiProcessGroupId") or pg_id
    log(f"[{flow_id}] redeployed -> {redeployed_doc.get('state')} pg_id={pg_id}")

    trigger_id = None
    for attempt in range(4):
        trigger_id = await find_trigger_processor(url, auth, pg_id, _label=f"[{flow_id}] attempt {attempt+1}")
        if trigger_id:
            break
        await asyncio.sleep(4)
    if not trigger_id:
        log(f"[{flow_id}] COULD NOT FIND TRIGGER after redeploy -- skipping verify")
        return {"flow_id": flow_id, "ok": True, "results": results, "verified": False}

    await client.post(f"{APP_BASE}/{flow_id}/enabled", json={"enabled": True})
    await client.post(f"{APP_BASE}/{flow_id}/verbs/start", timeout=60)
    await client.post(f"{APP_BASE}/{flow_id}/verbs/pause", timeout=60)
    result = await nifi_apply._set_processors_state(conn_dict_global, [trigger_id], "RUN_ONCE")
    log(f"[{flow_id}] RUN_ONCE: {json.dumps(result)}")

    governed_topics = [r["topic"] for r in good]
    seen = {t: 0 for t in governed_topics}
    start_t = time.time()
    max_wait_s = 90
    while time.time() - start_t < max_wait_s:
        m = await get_metrics_patient(client, flow_id)
        tc = {t["topic"]: t["messages"] for t in (m.get("topicCounts") or [])}
        for t in governed_topics:
            seen[t] = tc.get(t, 0)
        log(f"[{flow_id}]   t+{int(time.time()-start_t)}s " + " ".join(f"{t}={seen[t]}" for t in governed_topics))
        if all(v > 0 for v in seen.values()):
            break
        await asyncio.sleep(6)

    await client.post(f"{APP_BASE}/{flow_id}/verbs/stop", timeout=60)
    log(f"[{flow_id}] stopped")

    pg_ids = set(await nifi_apply._collect_pg_ids_recursive(url, auth, pg_id))
    bb = await nifi_api_request(url, "GET", "/nifi-api/flow/bulletin-board", params={"limit": "100"}, **auth)
    bulletins = ((bb.get("data") or {}).get("bulletinBoard") or {}).get("bulletins") or []
    relevant = [b for b in bulletins if (b.get("bulletin") or {}).get("groupId") in pg_ids]
    if relevant:
        log(f"[{flow_id}] BULLETINS ({len(relevant)}):")
        for b in relevant:
            bl = b.get("bulletin") or {}
            log(f"    [{bl.get('level')}] {bl.get('sourceName')}: {bl.get('message')}")
    else:
        log(f"[{flow_id}] no bulletins")

    all_verified = all(v > 0 for v in seen.values())
    if not all_verified:
        log(f"[{flow_id}] WARNING: not all governed topics show messages: {seen}")

    return {"flow_id": flow_id, "ok": True, "results": results, "verified": all_verified, "topic_counts": seen}


conn_dict_global = None


async def main():
    global conn_dict_global
    flow_ids = sys.argv[1:]
    await dbmod.init_db()
    db = dbmod.get_db()
    connections = await _load_connections(db)
    nifi_conn_doc = _active_connection(connections, "nifi")
    conn_dict = _nifi_conn_dict(nifi_conn_doc)
    conn_dict_global = conn_dict
    url = conn_dict["endpoint"]
    auth = {"auth_type": conn_dict["auth_type"], "username": conn_dict["username"],
            "password": conn_dict["password"], "token": conn_dict["token"]}

    summary = []
    async with httpx.AsyncClient(timeout=60) as client:
        for flow_id in flow_ids:
            try:
                res = await process_flow(client, url, auth, flow_id)
            except Exception as exc:
                log(f"[{flow_id}] EXCEPTION: {exc!r}")
                res = {"flow_id": flow_id, "ok": False, "error": repr(exc)}
            summary.append(res)

    log("\n===== BATCH SUMMARY =====")
    for s in summary:
        log(json.dumps({k: v for k, v in s.items() if k != "results"}))


if __name__ == "__main__":
    asyncio.run(main())
