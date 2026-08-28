import asyncio
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

APP_BASE = "http://127.0.0.1:8000/api/v2/flows"


async def find_trigger_processor(url, auth, pg_id):
    r = await nifi_api_request(url, "GET", f"/nifi-api/flow/process-groups/{pg_id}", **auth)
    if not r.get("ok"):
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


async def main():
    flow_id = sys.argv[1]
    raw_topic = sys.argv[2]
    governed_topic = sys.argv[3]
    max_wait_s = int(sys.argv[4]) if len(sys.argv) > 4 else 90

    await dbmod.init_db()
    db = dbmod.get_db()
    connections = await _load_connections(db)
    nifi_conn_doc = _active_connection(connections, "nifi")
    conn_dict = _nifi_conn_dict(nifi_conn_doc)
    url = conn_dict["endpoint"]
    auth = {"auth_type": conn_dict["auth_type"], "username": conn_dict["username"],
            "password": conn_dict["password"], "token": conn_dict["token"]}

    async with httpx.AsyncClient(timeout=90) as client:
        fr = await client.get(f"{APP_BASE}/{flow_id}")
        pg_id = fr.json().get("nifiProcessGroupId")

        trigger_id = await find_trigger_processor(url, auth, pg_id)
        if not trigger_id:
            print(f"[{flow_id}] COULD NOT FIND TRIGGER -- aborting")
            return

        print(f"[{flow_id}] enabling + start")
        await client.post(f"{APP_BASE}/{flow_id}/enabled", json={"enabled": True})
        r = await client.post(f"{APP_BASE}/{flow_id}/verbs/start")
        print(f"[{flow_id}] start -> {r.status_code} {r.json().get('state')}")

        await client.post(f"{APP_BASE}/{flow_id}/verbs/pause")

        print(f"[{flow_id}] RUN_ONCE {trigger_id}")
        result = await nifi_apply._set_processors_state(conn_dict, [trigger_id], "RUN_ONCE")
        print(f"[{flow_id}] RUN_ONCE result:", json.dumps(result))

        print(f"[{flow_id}] polling (max {max_wait_s}s) for {raw_topic} and {governed_topic}")
        start_t = time.time()
        got_raw = got_gov = False
        while time.time() - start_t < max_wait_s:
            m = (await client.get(f"{APP_BASE}/{flow_id}/metrics")).json()
            tc = {t["topic"]: t["messages"] for t in (m.get("topicCounts") or [])}
            raw_c = tc.get(raw_topic, 0)
            gov_c = tc.get(governed_topic, 0)
            print(f"[{flow_id}]   t+{int(time.time()-start_t)}s queued={m.get('queued')} raw={raw_c} governed={gov_c}")
            if raw_c > 0:
                got_raw = True
            if gov_c > 0:
                got_gov = True
            if got_raw and got_gov:
                break
            await asyncio.sleep(5)

        print(f"[{flow_id}] verb stop")
        r = await client.post(f"{APP_BASE}/{flow_id}/verbs/stop")
        print(f"[{flow_id}] stop -> {r.status_code} {r.json().get('state')}")

        pg_ids = set(await nifi_apply._collect_pg_ids_recursive(url, auth, pg_id))
        bb = await nifi_api_request(url, "GET", "/nifi-api/flow/bulletin-board", params={"limit": "100"}, **auth)
        bulletins = ((bb.get("data") or {}).get("bulletinBoard") or {}).get("bulletins") or []
        relevant = [b for b in bulletins if (b.get("bulletin") or {}).get("groupId") in pg_ids]
        if relevant:
            print(f"[{flow_id}] BULLETINS ({len(relevant)}):")
            for b in relevant:
                bl = b.get("bulletin") or {}
                print(f"    [{bl.get('level')}] {bl.get('sourceName')}: {bl.get('message')}")
        else:
            print(f"[{flow_id}] no bulletins")

        if got_gov:
            mr = await client.get(f"{APP_BASE}/{flow_id}/messages", params={"topic": governed_topic, "limit": 2})
            print(f"[{flow_id}] sample governed messages:")
            print(json.dumps(mr.json(), indent=2)[:2500])

        m = (await client.get(f"{APP_BASE}/{flow_id}/metrics")).json()
        print(f"[{flow_id}] FINAL topicCounts:", m.get("topicCounts"))


asyncio.run(main())
