import asyncio
import sys
import json

sys.path.insert(0, "backend")

import db as dbmod
from services.nifi_client import nifi_api_request
from services.adapter.deployer.lifecycle import _load_connections, _active_connection, _nifi_conn_dict

S1_ID = "14ab82fd-01a0-1000-47d6-db7896347cfc"


async def main():
    await dbmod.init_db()
    db = dbmod.get_db()
    connections = await _load_connections(db)
    nifi_conn_doc = _active_connection(connections, "nifi")
    conn_dict = _nifi_conn_dict(nifi_conn_doc)
    url = conn_dict["endpoint"]
    auth = {"auth_type": conn_dict["auth_type"], "username": conn_dict["username"],
            "password": conn_dict["password"], "token": conn_dict["token"]}

    pg = await nifi_api_request(url, "GET", f"/nifi-api/process-groups/{S1_ID}", **auth)
    if not pg.get("ok"):
        print("PG ERROR:", pg)
        return
    pc_ref = pg["data"]["component"].get("parameterContext")
    print("Parameter context ref:", pc_ref)
    if not pc_ref:
        print("No parameter context set on this PG.")
        return
    pc_id = pc_ref["id"]
    pc = await nifi_api_request(url, "GET", f"/nifi-api/parameter-contexts/{pc_id}", **auth)
    if not pc.get("ok"):
        print("PC ERROR:", pc)
        return
    params = pc["data"]["component"].get("parameters", [])
    for p in params:
        pv = p["parameter"]
        name = pv.get("name")
        sensitive = pv.get("sensitive")
        value = pv.get("value")
        if sensitive:
            print(f"  {name} = <SENSITIVE, not shown>")
        else:
            print(f"  {name} = {value!r}")


asyncio.run(main())
