import asyncio
import json
import sys
import os

sys.path.insert(0, "backend")
os.environ.setdefault("MONGO_URL", "mongodb://localhost:27018")
os.environ.setdefault("DB_NAME", "dmp_platform")

import httpx
import db as dbmod
from services.adapter.deployer.lifecycle import _load_connections, _active_connection, _nifi_conn_dict
from services.nifi_client import nifi_api_request

APP_BASE = "http://127.0.0.1:8000/api/v2/flows"


async def dump_states(url, auth, pg_id, indent=0):
    r = await nifi_api_request(url, "GET", f"/nifi-api/flow/process-groups/{pg_id}", **auth)
    if not r.get("ok"):
        print(" " * indent, "FAILED to fetch", pg_id, r)
        return
    flow = r["data"]["processGroupFlow"]["flow"]
    for p in flow.get("processors", []):
        c = p["component"]
        status = p.get("status", {})
        run_status = status.get("aggregateSnapshot", {}).get("runStatus") or c.get("state")
        active_threads = status.get("aggregateSnapshot", {}).get("activeThreadCount")
        print(" " * indent, f"{c['name']:30s} state={c.get('state')!s:10s} runStatus={run_status!s:10s} activeThreads={active_threads}")
    for g in flow.get("processGroups", []):
        print(" " * indent, f"[group] {g['component']['name']}")
        await dump_states(url, auth, g["component"]["id"], indent + 2)


async def main():
    flow_id = sys.argv[1]
    await dbmod.init_db()
    db = dbmod.get_db()
    connections = await _load_connections(db)
    nifi_conn_doc = _active_connection(connections, "nifi")
    conn_dict = _nifi_conn_dict(nifi_conn_doc)
    url = conn_dict["endpoint"]
    auth = {"auth_type": conn_dict["auth_type"], "username": conn_dict["username"],
            "password": conn_dict["password"], "token": conn_dict["token"]}
    async with httpx.AsyncClient(timeout=30) as client:
        fr = await client.get(f"{APP_BASE}/{flow_id}")
        pg_id = fr.json().get("nifiProcessGroupId")
    await dump_states(url, auth, pg_id)


asyncio.run(main())
