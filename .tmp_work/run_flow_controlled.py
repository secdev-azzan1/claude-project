import asyncio
import sys
import json

sys.path.insert(0, "backend")

import db as dbmod
from services.nifi_client import nifi_api_request
from services.adapter.deployer.lifecycle import _load_connections, _active_connection, _nifi_conn_dict


async def set_state(url, auth, proc_id, state):
    current = await nifi_api_request(url, "GET", f"/nifi-api/processors/{proc_id}", **auth)
    if not current.get("ok"):
        return current
    rev = ((current.get("data") or {}).get("revision") or {}).get("version", 0)
    return await nifi_api_request(
        url, "PUT", f"/nifi-api/processors/{proc_id}/run-status",
        json_body={"revision": {"version": rev}, "state": state, "disconnectedNodeAcknowledged": False}, **auth,
    )


async def get_status(url, auth, pg_id):
    return await nifi_api_request(url, "GET", f"/nifi-api/flow/process-groups/{pg_id}/status?recursive=true", **auth)


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


async def main():
    trigger_id = sys.argv[1]
    pg_id = sys.argv[2]
    max_wait = int(sys.argv[3]) if len(sys.argv) > 3 else 90

    await dbmod.init_db()
    db = dbmod.get_db()
    connections = await _load_connections(db)
    nifi_conn_doc = _active_connection(connections, "nifi")
    conn_dict = _nifi_conn_dict(nifi_conn_doc)
    url = conn_dict["endpoint"]
    auth = {"auth_type": conn_dict["auth_type"], "username": conn_dict["username"],
            "password": conn_dict["password"], "token": conn_dict["token"]}

    r1 = await set_state(url, auth, trigger_id, "STOPPED")
    print("stop trigger ok:", r1.get("ok"))
    r2 = await set_state(url, auth, trigger_id, "RUN_ONCE")
    print("run_once ok:", r2.get("ok"), r2.get("error") if not r2.get("ok") else "")

    elapsed = 0
    interval = 5
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

    bulletins = await nifi_api_request(url, "GET", "/nifi-api/flow/bulletin-board?limit=100", **auth)
    if bulletins.get("ok"):
        blist = ((bulletins["data"] or {}).get("bulletinBoard") or {}).get("bulletins", [])
        errs = [b for b in blist if pg_id in json.dumps(b) or True]
        print(f"bulletins total: {len(blist)}")
        for b in blist[:20]:
            bb = b.get("bulletin", {})
            print(" -", bb.get("level"), bb.get("sourceName"), bb.get("message"))


asyncio.run(main())
