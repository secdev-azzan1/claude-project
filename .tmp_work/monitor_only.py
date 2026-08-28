import asyncio
import sys
import json

sys.path.insert(0, "backend")

import db as dbmod
from services.nifi_client import nifi_api_request
from services.adapter.deployer.lifecycle import _load_connections, _active_connection, _nifi_conn_dict


def _to_int(v):
    if v is None:
        return 0
    return int(str(v).replace(",", "") or 0)


def total_queued_and_active(snap):
    total_q = _to_int(snap.get("queuedCount"))
    total_a = _to_int(snap.get("activeThreadCount"))
    for child in snap.get("processGroupStatusSnapshots", []) or []:
        cq, ca = total_queued_and_active(child["processGroupStatusSnapshot"])
        total_q += cq
        total_a += ca
    return total_q, total_a


async def get_status(url, auth, pg_id):
    return await nifi_api_request(url, "GET", f"/nifi-api/flow/process-groups/{pg_id}/status?recursive=true", **auth)


async def main():
    pg_id = sys.argv[1]
    max_wait = int(sys.argv[2]) if len(sys.argv) > 2 else 600

    await dbmod.init_db()
    db = dbmod.get_db()
    connections = await _load_connections(db)
    nifi_conn_doc = _active_connection(connections, "nifi")
    conn_dict = _nifi_conn_dict(nifi_conn_doc)
    url = conn_dict["endpoint"]
    auth = {"auth_type": conn_dict["auth_type"], "username": conn_dict["username"],
            "password": conn_dict["password"], "token": conn_dict["token"]}

    elapsed = 0
    interval = 10
    stable_count = 0
    prev_q = -1
    while elapsed < max_wait:
        await asyncio.sleep(interval)
        elapsed += interval
        status = await get_status(url, auth, pg_id)
        if not status.get("ok"):
            print("status error:", status)
            continue
        top = status["data"]["processGroupStatus"]["aggregateSnapshot"]
        q, a = total_queued_and_active(top)
        print(f"t={elapsed}s queued={q} activeThreads={a}")
        if q == 0 and a == 0 and prev_q == 0:
            stable_count += 1
            if stable_count >= 2:
                print("DRAINED")
                break
        else:
            stable_count = 0
        prev_q = q

    bulletins = await nifi_api_request(url, "GET", "/nifi-api/flow/bulletin-board?limit=200", **auth)
    if bulletins.get("ok"):
        blist = ((bulletins["data"] or {}).get("bulletinBoard") or {}).get("bulletins", [])
        print(f"bulletins total: {len(blist)}")
        for b in blist[:30]:
            bb = b.get("bulletin", {})
            print(" -", bb.get("level"), bb.get("sourceName"), bb.get("message"))


asyncio.run(main())
