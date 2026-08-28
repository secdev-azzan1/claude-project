import asyncio
import sys
import json

sys.path.insert(0, "backend")

import db as dbmod
from services.nifi_client import nifi_api_request
from services.adapter.deployer.lifecycle import _load_connections, _active_connection, _nifi_conn_dict

INGEST_ID = "0a00e822-01a0-1000-68b7-f28e69779c95"


async def main():
    await dbmod.init_db()
    db = dbmod.get_db()
    connections = await _load_connections(db)
    nifi_conn_doc = _active_connection(connections, "nifi")
    conn_dict = _nifi_conn_dict(nifi_conn_doc)
    url = conn_dict["endpoint"]
    auth = {"auth_type": conn_dict["auth_type"], "username": conn_dict["username"],
            "password": conn_dict["password"], "token": conn_dict["token"]}

    flow = await nifi_api_request(url, "GET", f"/nifi-api/flow/process-groups/{INGEST_ID}", **auth)
    if not flow.get("ok"):
        print("FLOW ERROR:", flow)
        return

    pgs = flow["data"]["processGroupFlow"]["flow"]["processGroups"]
    print(f"Children of 'Ingest(3) (1)' ({len(pgs)}):")
    for pg in pgs:
        comp = pg["component"]
        print(f"  - name={comp['name']!r} id={comp['id']}")

    def name_match(name, keywords):
        n = name.lower()
        return all(k in n for k in keywords)

    s1_candidates = [pg for pg in pgs if name_match(pg["component"]["name"], ["sentinelone"]) or name_match(pg["component"]["name"], ["sentinel"])]
    print(f"\nSentinelOne-ish candidates: {[c['component']['name'] for c in s1_candidates]}")

    maxuseful_candidates = [pg for pg in pgs if name_match(pg["component"]["name"], ["maximum"]) or name_match(pg["component"]["name"], ["max"])]
    print(f"Maximum-useful-ish candidates: {[c['component']['name'] for c in maxuseful_candidates]}")


asyncio.run(main())
