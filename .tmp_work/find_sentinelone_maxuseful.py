import asyncio
import sys
import json

sys.path.insert(0, "backend")

import db as dbmod
from services.nifi_client import nifi_api_request
from services.adapter.deployer.lifecycle import _load_connections, _active_connection, _nifi_conn_dict


async def main():
    await dbmod.init_db()
    db = dbmod.get_db()
    connections = await _load_connections(db)
    nifi_conn_doc = _active_connection(connections, "nifi")
    conn_dict = _nifi_conn_dict(nifi_conn_doc)
    url = conn_dict["endpoint"]
    auth = {"auth_type": conn_dict["auth_type"], "username": conn_dict["username"],
            "password": conn_dict["password"], "token": conn_dict["token"]}

    # Get root process group
    root = await nifi_api_request(url, "GET", "/nifi-api/process-groups/root", **auth)
    if not root.get("ok"):
        print("ROOT ERROR:", root)
        return
    root_id = root["data"]["id"]
    print(f"ROOT ID: {root_id}")

    # Get flow of root (includes top-level children)
    flow = await nifi_api_request(url, "GET", f"/nifi-api/flow/process-groups/{root_id}", **auth)
    if not flow.get("ok"):
        print("FLOW ERROR:", flow)
        return

    top_pgs = flow["data"]["processGroupFlow"]["flow"]["processGroups"]
    print(f"\nTop-level process groups ({len(top_pgs)}):")
    for pg in top_pgs:
        comp = pg["component"]
        print(f"  - name={comp['name']!r} id={comp['id']}")

    # Search recursively for "ingest" match at top level and 1 level down
    def name_match(name, keywords):
        n = name.lower()
        return all(k in n for k in keywords)

    ingest_candidates = [pg for pg in top_pgs if name_match(pg["component"]["name"], ["ingest"])]
    print(f"\nCandidates matching 'ingest' at top level: {[c['component']['name'] for c in ingest_candidates]}")

    # If none at top level, go one level down into each top pg
    found_ingest = None
    for pg in top_pgs:
        if name_match(pg["component"]["name"], ["ingest"]):
            found_ingest = pg
            break

    if not found_ingest:
        print("\nNo top-level 'ingest' match; searching one level down...")
        for pg in top_pgs:
            pgid = pg["component"]["id"]
            sub_flow = await nifi_api_request(url, "GET", f"/nifi-api/flow/process-groups/{pgid}", **auth)
            if not sub_flow.get("ok"):
                continue
            sub_pgs = sub_flow["data"]["processGroupFlow"]["flow"]["processGroups"]
            for spg in sub_pgs:
                print(f"    under {pg['component']['name']!r}: {spg['component']['name']!r} id={spg['component']['id']}")
                if name_match(spg["component"]["name"], ["ingest"]):
                    found_ingest = spg

    if found_ingest:
        print(f"\nFOUND ingest-like group: name={found_ingest['component']['name']!r} id={found_ingest['component']['id']}")
    else:
        print("\nNo 'ingest' match found at top level or one level down.")

    with open(".tmp_work/toplevel_pgs.json", "w") as f:
        json.dump({"root_id": root_id, "top_pgs": [{"name": p["component"]["name"], "id": p["component"]["id"]} for p in top_pgs]}, f, indent=2)


asyncio.run(main())
