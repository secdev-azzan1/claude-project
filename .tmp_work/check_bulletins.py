import asyncio
import sys
import os

sys.path.insert(0, "backend")
os.environ.setdefault("MONGO_URL", "mongodb://localhost:27018")
os.environ.setdefault("DB_NAME", "dmp_platform")

import httpx
import db as dbmod
from services.adapter.deployer import nifi_apply
from services.adapter.deployer.lifecycle import _load_connections, _active_connection, _nifi_conn_dict
from services.nifi_client import nifi_api_request

APP_BASE = "http://127.0.0.1:8000/api/v2/flows"


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
    pg_ids = set(await nifi_apply._collect_pg_ids_recursive(url, auth, pg_id))
    bb = await nifi_api_request(url, "GET", "/nifi-api/flow/bulletin-board", params={"limit": "100"}, **auth)
    bulletins = ((bb.get("data") or {}).get("bulletinBoard") or {}).get("bulletins") or []
    relevant = [b for b in bulletins if (b.get("bulletin") or {}).get("groupId") in pg_ids]
    if relevant:
        print(f"BULLETINS ({len(relevant)}):")
        for b in relevant:
            bl = b.get("bulletin") or {}
            print(f"  [{bl.get('level')}] {bl.get('sourceName')}: {bl.get('message')}")
    else:
        print("no bulletins")


asyncio.run(main())
