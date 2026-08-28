import asyncio
import json
import os
import sys
import time

sys.path.insert(0, "backend")

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27018")
os.environ.setdefault("DB_NAME", "dmp_platform")

import httpx
import db as dbmod
from services.adapter.deployer import nifi_apply
from services.adapter.deployer.lifecycle import _load_connections, _active_connection, _nifi_conn_dict
from services.nifi_client import nifi_api_request

APP_BASE = "http://127.0.0.1:8000/api/v2/flows"


async def app_verb(client, flow_id, verb):
    r = await client.post(f"{APP_BASE}/{flow_id}/verbs/{verb}")
    return r.status_code, r.json()


async def app_metrics(client, flow_id):
    r = await client.get(f"{APP_BASE}/{flow_id}/metrics")
    return r.json()


async def main():
    flow_id = sys.argv[1]
    trigger_id = sys.argv[2]
    max_wait_s = int(sys.argv[3]) if len(sys.argv) > 3 else 600
    skip_run_once = "--skip-run-once" in sys.argv

    await dbmod.init_db()
    db = dbmod.get_db()
    connections = await _load_connections(db)
    nifi_conn_doc = _active_connection(connections, "nifi")
    conn_dict = _nifi_conn_dict(nifi_conn_doc)
    url = conn_dict["endpoint"]
    auth = {"auth_type": conn_dict["auth_type"], "username": conn_dict["username"],
            "password": conn_dict["password"], "token": conn_dict["token"]}

    async with httpx.AsyncClient(timeout=60) as client:
        print(f"[{flow_id}] enabling flow")
        r = await client.post(f"{APP_BASE}/{flow_id}/enabled", json={"enabled": True})
        print(f"[{flow_id}] enable -> HTTP {r.status_code} {r.json()}")

        print(f"[{flow_id}] verb start")
        code, body = await app_verb(client, flow_id, "start")
        print(f"[{flow_id}] start -> HTTP {code}, state={body.get('state')}")

        print(f"[{flow_id}] verb pause (stops trigger only)")
        code, body = await app_verb(client, flow_id, "pause")
        print(f"[{flow_id}] pause -> HTTP {code}, state={body.get('state')}")

        if skip_run_once:
            print(f"[{flow_id}] skipping RUN_ONCE (already fired earlier; draining that queued FlowFile instead)")
        else:
            print(f"[{flow_id}] RUN_ONCE trigger {trigger_id}")
            result = await nifi_apply._set_processors_state(conn_dict, [trigger_id], "RUN_ONCE")
            print(f"[{flow_id}] RUN_ONCE result:", json.dumps(result))

        print(f"[{flow_id}] polling queue drain (max {max_wait_s}s)")
        stable_zero = 0
        start_t = time.time()
        last_queued = None
        while time.time() - start_t < max_wait_s:
            m = await app_metrics(client, flow_id)
            queued = m.get("queued")
            per_block = m.get("perBlock") or []
            last_queued = queued
            print(f"[{flow_id}]   t+{int(time.time()-start_t)}s queued={queued} perBlock={[(b['label'], b['queued']) for b in per_block]}")
            if queued == 0:
                stable_zero += 1
                if stable_zero >= 2:
                    print(f"[{flow_id}] drained (stable at 0 for 2 polls)")
                    break
            else:
                stable_zero = 0
            await asyncio.sleep(5)
        else:
            print(f"[{flow_id}] WARNING: did not confirm drain within {max_wait_s}s, last queued={last_queued}")

        pg_id = None
        fr = await client.get(f"{APP_BASE}/{flow_id}")
        pg_id = fr.json().get("nifiProcessGroupId")

        print(f"[{flow_id}] checking bulletin board")
        pg_ids = set(await nifi_apply._collect_pg_ids_recursive(url, auth, pg_id))
        bb = await nifi_api_request(url, "GET", "/nifi-api/flow/bulletin-board", params={"limit": "200"}, **auth)
        bulletins = ((bb.get("data") or {}).get("bulletinBoard") or {}).get("bulletins") or []
        relevant = [b for b in bulletins if (b.get("bulletin") or {}).get("groupId") in pg_ids]
        if relevant:
            print(f"[{flow_id}] BULLETINS ({len(relevant)}):")
            for b in relevant:
                bl = b.get("bulletin") or {}
                print(f"    [{bl.get('level')}] {bl.get('sourceName')}: {bl.get('message')}")
        else:
            print(f"[{flow_id}] no bulletins for this flow's process groups")

        print(f"[{flow_id}] verb stop")
        code, body = await app_verb(client, flow_id, "stop")
        print(f"[{flow_id}] stop -> HTTP {code}, state={body.get('state')}")

        m = await app_metrics(client, flow_id)
        print(f"[{flow_id}] final metrics: queued={m.get('queued')} topicCounts={m.get('topicCounts')}")


asyncio.run(main())
