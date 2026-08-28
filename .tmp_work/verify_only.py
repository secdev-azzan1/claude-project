import asyncio
import json
import sys
import time

sys.path.insert(0, "backend")
sys.path.insert(0, ".tmp_work")
import os
os.environ.setdefault("MONGO_URL", "mongodb://localhost:27018")
os.environ.setdefault("DB_NAME", "dmp_platform")

import httpx
import db as dbmod
import batch_kafka_kc as bk
from services.adapter.deployer import nifi_apply
from services.adapter.deployer.lifecycle import _load_connections, _active_connection, _nifi_conn_dict
from services.nifi_client import nifi_api_request
from services.adapter.naming import tokenize, base_topic_name

APP_BASE = bk.APP_BASE


async def verify_flow(client, url, auth, flow_id):
    fr = await client.get(f"{APP_BASE}/{flow_id}")
    flow_doc = fr.json()
    pg_id = flow_doc.get("nifiProcessGroupId")
    flow_name = flow_doc["name"]
    kc_entities = [b["entity"] for b in flow_doc["blocks"] if b.get("adapter") == "kafka_kc"]
    if not kc_entities:
        bk.log(f"[{flow_id}] no kafka_kc blocks -- nothing to verify")
        return {"flow_id": flow_id, "ok": False, "error": "no kc blocks"}
    governed_topics = [base_topic_name(flow_name, e) for e in kc_entities]

    trigger_id = None
    for attempt in range(4):
        trigger_id = await bk.find_trigger_processor(url, auth, pg_id, _label=f"[{flow_id}] attempt {attempt+1}")
        if trigger_id:
            break
        await asyncio.sleep(4)
    if not trigger_id:
        bk.log(f"[{flow_id}] COULD NOT FIND TRIGGER -- aborting verify")
        return {"flow_id": flow_id, "ok": False, "error": "no trigger"}

    await client.post(f"{APP_BASE}/{flow_id}/enabled", json={"enabled": True})
    await client.post(f"{APP_BASE}/{flow_id}/verbs/start", timeout=60)
    await client.post(f"{APP_BASE}/{flow_id}/verbs/pause", timeout=60)
    result = await nifi_apply._set_processors_state(bk.conn_dict_global, [trigger_id], "RUN_ONCE")
    bk.log(f"[{flow_id}] RUN_ONCE: {json.dumps(result)}")

    seen = {t: 0 for t in governed_topics}
    start_t = time.time()
    while time.time() - start_t < 90:
        m = await bk.get_metrics_patient(client, flow_id)
        tc = {t["topic"]: t["messages"] for t in (m.get("topicCounts") or [])}
        for t in governed_topics:
            seen[t] = tc.get(t, 0)
        bk.log(f"[{flow_id}]   t+{int(time.time()-start_t)}s " + " ".join(f"{t}={seen[t]}" for t in governed_topics))
        if all(v > 0 for v in seen.values()):
            break
        await asyncio.sleep(6)

    await client.post(f"{APP_BASE}/{flow_id}/verbs/stop", timeout=60)
    bk.log(f"[{flow_id}] stopped")

    pg_ids = set(await nifi_apply._collect_pg_ids_recursive(url, auth, pg_id))
    bb = await nifi_api_request(url, "GET", "/nifi-api/flow/bulletin-board", params={"limit": "100"}, **auth)
    bulletins = ((bb.get("data") or {}).get("bulletinBoard") or {}).get("bulletins") or []
    relevant = [b for b in bulletins if (b.get("bulletin") or {}).get("groupId") in pg_ids]
    if relevant:
        bk.log(f"[{flow_id}] BULLETINS ({len(relevant)}):")
        for b in relevant:
            bl = b.get("bulletin") or {}
            bk.log(f"    [{bl.get('level')}] {bl.get('sourceName')}: {bl.get('message')}")
    else:
        bk.log(f"[{flow_id}] no bulletins")

    ok = all(v > 0 for v in seen.values())
    if not ok:
        bk.log(f"[{flow_id}] WARNING: not all governed topics show messages: {seen}")
    return {"flow_id": flow_id, "ok": ok, "topic_counts": seen}


async def main():
    flow_ids = sys.argv[1:]
    await dbmod.init_db()
    db = dbmod.get_db()
    connections = await _load_connections(db)
    nifi_conn_doc = _active_connection(connections, "nifi")
    conn_dict = _nifi_conn_dict(nifi_conn_doc)
    bk.conn_dict_global = conn_dict
    url = conn_dict["endpoint"]
    auth = {"auth_type": conn_dict["auth_type"], "username": conn_dict["username"],
            "password": conn_dict["password"], "token": conn_dict["token"]}

    summary = []
    async with httpx.AsyncClient(timeout=60) as client:
        for flow_id in flow_ids:
            try:
                res = await verify_flow(client, url, auth, flow_id)
            except Exception as exc:
                bk.log(f"[{flow_id}] EXCEPTION: {exc!r}")
                res = {"flow_id": flow_id, "ok": False, "error": repr(exc)}
            summary.append(res)

    bk.log("\n===== VERIFY SUMMARY =====")
    for s in summary:
        bk.log(json.dumps(s))


asyncio.run(main())
