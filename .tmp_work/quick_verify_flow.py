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

# entity -> primary bronze topic name (matches build script naming)
TENANT = "sentinelone_securado"


async def find_trigger_processor(url, auth, pg_id):
    """Recursively search a process group tree for the GenerateFlowFile 'trigger' processor."""
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
    entity = sys.argv[2]
    topic = f"bronze.{TENANT}.{entity}"
    max_wait_s = int(sys.argv[3]) if len(sys.argv) > 3 else 90

    await dbmod.init_db()
    db = dbmod.get_db()
    connections = await _load_connections(db)
    nifi_conn_doc = _active_connection(connections, "nifi")
    conn_dict = _nifi_conn_dict(nifi_conn_doc)
    url = conn_dict["endpoint"]
    auth = {"auth_type": conn_dict["auth_type"], "username": conn_dict["username"],
            "password": conn_dict["password"], "token": conn_dict["token"]}

    async with httpx.AsyncClient(timeout=60) as client:
        fr = await client.get(f"{APP_BASE}/{flow_id}")
        pg_id = fr.json().get("nifiProcessGroupId")

        trigger_id = await find_trigger_processor(url, auth, pg_id)
        if not trigger_id:
            print(f"[{flow_id}] COULD NOT FIND TRIGGER PROCESSOR -- aborting")
            return

        print(f"[{flow_id}] enabling flow")
        r = await client.post(f"{APP_BASE}/{flow_id}/enabled", json={"enabled": True})
        print(f"[{flow_id}] enable -> HTTP {r.status_code}")

        print(f"[{flow_id}] verb start")
        r = await client.post(f"{APP_BASE}/{flow_id}/verbs/start")
        print(f"[{flow_id}] start -> HTTP {r.status_code}, state={r.json().get('state')}")

        print(f"[{flow_id}] verb pause (stops periodic trigger only)")
        r = await client.post(f"{APP_BASE}/{flow_id}/verbs/pause")
        print(f"[{flow_id}] pause -> HTTP {r.status_code}, state={r.json().get('state')}")

        print(f"[{flow_id}] RUN_ONCE trigger {trigger_id}")
        result = await nifi_apply._set_processors_state(conn_dict, [trigger_id], "RUN_ONCE")
        print(f"[{flow_id}] RUN_ONCE result:", json.dumps(result))

        print(f"[{flow_id}] polling for first records (max {max_wait_s}s, NOT waiting for full drain)")
        start_t = time.time()
        got_records = False
        while time.time() - start_t < max_wait_s:
            m = (await client.get(f"{APP_BASE}/{flow_id}/metrics")).json()
            tc = {t["topic"]: t["messages"] for t in (m.get("topicCounts") or [])}
            main_count = tc.get(topic, 0)
            print(f"[{flow_id}]   t+{int(time.time()-start_t)}s queued={m.get('queued')} {topic}={main_count}")
            if main_count > 0:
                got_records = True
                break
            await asyncio.sleep(5)

        if not got_records:
            print(f"[{flow_id}] WARNING: no records observed within {max_wait_s}s")

        print(f"[{flow_id}] verb stop")
        r = await client.post(f"{APP_BASE}/{flow_id}/verbs/stop")
        print(f"[{flow_id}] stop -> HTTP {r.status_code}, state={r.json().get('state')}")

        print(f"[{flow_id}] checking bulletin board")
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

        if got_records:
            mr = await client.get(f"{APP_BASE}/{flow_id}/messages", params={"topic": topic, "limit": 2})
            print(f"[{flow_id}] sample messages:")
            print(json.dumps(mr.json(), indent=2)[:3000])

        m = await (await client.get(f"{APP_BASE}/{flow_id}/metrics")).json() if False else (await client.get(f"{APP_BASE}/{flow_id}/metrics")).json()
        print(f"[{flow_id}] FINAL: queued={m.get('queued')} topicCounts={m.get('topicCounts')}")


asyncio.run(main())
