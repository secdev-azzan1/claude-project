import asyncio
import sys
import json

sys.path.insert(0, "backend")

import db as dbmod
from services.nifi_client import nifi_api_request
from services.adapter.deployer.lifecycle import _load_connections, _active_connection, _nifi_conn_dict


async def main():
    trigger_id = sys.argv[1]
    target_state = sys.argv[2] if len(sys.argv) > 2 else "RUN_ONCE"

    await dbmod.init_db()
    db = dbmod.get_db()
    connections = await _load_connections(db)
    nifi_conn_doc = _active_connection(connections, "nifi")
    conn_dict = _nifi_conn_dict(nifi_conn_doc)
    url = conn_dict["endpoint"]
    auth = {"auth_type": conn_dict["auth_type"], "username": conn_dict["username"],
            "password": conn_dict["password"], "token": conn_dict["token"]}

    current = await nifi_api_request(url, "GET", f"/nifi-api/processors/{trigger_id}", **auth)
    print("GET ok:", current.get("ok"))
    if not current.get("ok"):
        print("GET error:", current)
        return
    data = current.get("data") or {}
    rev = (data.get("revision") or {}).get("version", 0)
    run_status = (data.get("status") or {}).get("runStatus")
    comp_state = (data.get("component") or {}).get("state")
    print("revision:", rev, "runStatus:", run_status, "component.state:", comp_state)

    r = await nifi_api_request(
        url, "PUT", f"/nifi-api/processors/{trigger_id}/run-status",
        json_body={"revision": {"version": rev}, "state": target_state, "disconnectedNodeAcknowledged": False},
        **auth,
    )
    print("PUT ok:", r.get("ok"))
    print("PUT full:", json.dumps(r, default=str)[:2000])


asyncio.run(main())
