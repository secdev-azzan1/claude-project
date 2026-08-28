import asyncio
import sys
import json

sys.path.insert(0, "backend")

import db as dbmod
from services.nifi_client import nifi_api_request
from services.adapter.deployer.lifecycle import _load_connections, _active_connection, _nifi_conn_dict

S1_ID = "14ab82fd-01a0-1000-47d6-db7896347cfc"

URL_PROP_KEYS = [
    "Remote URL", "HTTP URL", "URL", "Base URL", "Endpoint",
    "hostname", "host", "http-method", "HTTP Method",
]


async def fetch_pg_recursive(url, auth, pg_id, pg_name, path, results):
    """Fetch a process group's flow (processors, connections, sub-groups) and recurse."""
    flow = await nifi_api_request(url, "GET", f"/nifi-api/flow/process-groups/{pg_id}", **auth)
    if not flow.get("ok"):
        results["errors"].append({"pg_id": pg_id, "pg_name": pg_name, "path": path, "error": flow})
        return

    pg_flow = flow["data"]["processGroupFlow"]["flow"]

    node = {
        "pg_id": pg_id,
        "pg_name": pg_name,
        "path": path,
        "processors": [],
        "connections": [],
        "sub_groups": [],
        "input_ports": [],
        "output_ports": [],
        "funnels": [],
    }

    for proc in pg_flow.get("processors", []):
        comp = proc["component"]
        status = proc.get("status", {})
        props = comp.get("config", {}).get("properties", {}) or {}
        # capture any property whose key suggests URL/endpoint
        url_props = {}
        for k, v in props.items():
            kl = k.lower()
            if any(u.lower() in kl for u in URL_PROP_KEYS) or "url" in kl or "endpoint" in kl or "path" in kl:
                url_props[k] = v
        node["processors"].append({
            "id": comp["id"],
            "name": comp["name"],
            "type": comp["type"],
            "state": comp.get("state"),
            "validationStatus": comp.get("validationStatus"),
            "validationErrors": comp.get("validationErrors", []),
            "url_props": url_props,
            "all_props": props,
            "runStatus": status.get("runStatus"),
        })

    for conn in pg_flow.get("connections", []):
        comp = conn["component"]
        node["connections"].append({
            "id": comp["id"],
            "name": comp.get("name"),
            "source": {"id": comp["source"]["id"], "name": comp["source"].get("name"), "type": comp["source"].get("type"), "groupId": comp["source"].get("groupId")},
            "destination": {"id": comp["destination"]["id"], "name": comp["destination"].get("name"), "type": comp["destination"].get("type"), "groupId": comp["destination"].get("groupId")},
            "selectedRelationships": comp.get("selectedRelationships", []),
        })

    for ip in pg_flow.get("inputPorts", []):
        comp = ip["component"]
        node["input_ports"].append({"id": comp["id"], "name": comp["name"]})

    for op in pg_flow.get("outputPorts", []):
        comp = op["component"]
        node["output_ports"].append({"id": comp["id"], "name": comp["name"]})

    for fn in pg_flow.get("funnels", []):
        comp = fn["component"]
        node["funnels"].append({"id": comp["id"]})

    sub_pgs = pg_flow.get("processGroups", [])
    results["tree"].append(node)

    for spg in sub_pgs:
        scomp = spg["component"]
        node["sub_groups"].append({"id": scomp["id"], "name": scomp["name"]})
        await fetch_pg_recursive(url, auth, scomp["id"], scomp["name"], path + [scomp["name"]], results)


async def main():
    await dbmod.init_db()
    db = dbmod.get_db()
    connections = await _load_connections(db)
    nifi_conn_doc = _active_connection(connections, "nifi")
    conn_dict = _nifi_conn_dict(nifi_conn_doc)
    url = conn_dict["endpoint"]
    auth = {"auth_type": conn_dict["auth_type"], "username": conn_dict["username"],
            "password": conn_dict["password"], "token": conn_dict["token"]}

    results = {"tree": [], "errors": []}
    await fetch_pg_recursive(url, auth, S1_ID, "sentinelone.maximum_useful", ["sentinelone.maximum_useful"], results)

    out_path = "C:/Users/kaifm/Desktop/claude-project/.tmp_work/s1_maxuseful_dump.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)

    total_procs = sum(len(n["processors"]) for n in results["tree"])
    total_pgs = len(results["tree"])
    total_conns = sum(len(n["connections"]) for n in results["tree"])
    print(f"Total process groups (including root S1 group): {total_pgs}")
    print(f"Total processors: {total_procs}")
    print(f"Total connections: {total_conns}")
    print(f"Errors: {len(results['errors'])}")
    for e in results["errors"]:
        print("  ERROR:", e["path"], e["error"])
    print(f"\nDumped full detail to {out_path}")


asyncio.run(main())
