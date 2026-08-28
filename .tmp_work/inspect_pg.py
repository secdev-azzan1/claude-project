import asyncio
import sys
import json

sys.path.insert(0, "backend")

import db as dbmod
from services.nifi_client import nifi_api_request
from services.adapter.deployer.lifecycle import _load_connections, _active_connection, _nifi_conn_dict


async def main():
    pg_id = sys.argv[1]

    await dbmod.init_db()
    db = dbmod.get_db()
    connections = await _load_connections(db)
    nifi_conn_doc = _active_connection(connections, "nifi")
    conn_dict = _nifi_conn_dict(nifi_conn_doc)
    url = conn_dict["endpoint"]
    auth = {"auth_type": conn_dict["auth_type"], "username": conn_dict["username"],
            "password": conn_dict["password"], "token": conn_dict["token"]}

    status = await nifi_api_request(url, "GET", f"/nifi-api/flow/process-groups/{pg_id}/status?recursive=true", **auth)
    if not status.get("ok"):
        print("STATUS ERROR:", status)
        return
    top = status["data"]["processGroupStatus"]["aggregateSnapshot"]

    def walk(snap, indent=0):
        print("  " * indent + f"{snap.get('name')}: flowFilesIn={snap.get('flowFilesIn')} "
              f"flowFilesOut={snap.get('flowFilesOut')} queuedCount={snap.get('queuedCount')} "
              f"bytesRead={snap.get('bytesRead')} bytesWritten={snap.get('bytesWritten')} "
              f"activeThreadCount={snap.get('activeThreadCount')} terminatedThreadCount={snap.get('terminatedThreadCount')}")
        for conn in snap.get("connectionStatusSnapshots", []) or []:
            c = conn["connectionStatusSnapshot"]
            if int(c.get("flowFilesQueued") or 0) > 0:
                print("  " * (indent + 1) + f"[QUEUED] {c.get('sourceName')} -> {c.get('destinationName')} ({c.get('name')}): {c.get('queued')}")
        for pconn in snap.get("processorStatusSnapshots", []) or []:
            p = pconn["processorStatusSnapshot"]
            if p.get("runStatus") not in ("Stopped", None) or int(p.get("flowFilesIn") or 0) or int(p.get("flowFilesOut") or 0):
                print("  " * (indent + 1) + f"(proc) {p.get('name')}: in={p.get('flowFilesIn')} out={p.get('flowFilesOut')} "
                      f"status={p.get('runStatus')} tasks={p.get('tasks')} activeThreads={p.get('activeThreadCount')}")
        for child in snap.get("processGroupStatusSnapshots", []) or []:
            walk(child["processGroupStatusSnapshot"], indent + 1)

    walk(top)

    bulletins = await nifi_api_request(url, "GET", "/nifi-api/flow/bulletin-board?limit=100", **auth)
    if bulletins.get("ok"):
        blist = ((bulletins["data"] or {}).get("bulletinBoard") or {}).get("bulletins", [])
        print(f"\nbulletins: {len(blist)}")
        for b in blist[:30]:
            bb = b.get("bulletin", {})
            print(" -", bb.get("level"), bb.get("groupId"), bb.get("sourceName"), bb.get("message"))
    else:
        print("BULLETINS ERROR:", bulletins)


asyncio.run(main())
