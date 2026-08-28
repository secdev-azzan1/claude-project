import asyncio
import json
import os
import sys
import time

sys.path.insert(0, "backend")

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27018")
os.environ.setdefault("DB_NAME", "dmp_platform")

import httpx
import db as dbmod
from services.adapter.deployer import nifi_apply
from services.adapter.deployer.lifecycle import _load_connections, _active_connection, _nifi_conn_dict
from services.nifi_client import nifi_api_request

APP_BASE = "http://127.0.0.1:8000/api/v2/flows"

FLOW_IDS = [
    # Tier A: near-empty (~0-1 rows)
    "flow-s1-service-user",
    "flow-s1-location",
    "flow-s1-tenant-policy",
    "flow-s1-system-info",
    "flow-s1-ioc",
    # Tier B: small/mid catalogs
    "flow-s1-activity-type",
    "flow-s1-role",
    "flow-s1-cloud-detection-rule",
    "flow-s1-agent-tag",
    "flow-s1-xdr-asset-tag",
    "flow-s1-agent-package",
    "flow-s1-config-override",
    "flow-s1-exclusion",
    "flow-s1-user",
    "flow-s1-application-cve",
    "flow-s1-alert",
    # Tier C: high-volume, last
    "flow-s1-xdr-asset",
    "flow-s1-activity",
    "flow-s1-agent",
    "flow-s1-restriction",
    "flow-s1-installed-application",
]


async def app_verb(client, flow_id, verb):
    r = await client.post(f"{APP_BASE}/{flow_id}/verbs/{verb}")
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, {"raw": r.text}


async def app_metrics(client, flow_id):
    r = await client.get(f"{APP_BASE}/{flow_id}/metrics")
    return r.json()


async def process_flow(client, url, auth, flow_id, max_wait_s):
    print(f"\n===== {flow_id} =====")

    r = await client.get(f"{APP_BASE}/{flow_id}")
    doc = r.json()
    root_block_id = next(b["id"] for b in doc["blocks"] if b.get("parentId") is None)
    entity = next((b.get("entity") for b in doc["blocks"] if b.get("entity")), "?")

    if doc.get("state") == "Draft" or not doc.get("nifiProcessGroupId"):
        print(f"[{flow_id}] deploying (entity={entity})")
        code, body = await app_verb(client, flow_id, "deploy")
        print(f"[{flow_id}] deploy -> HTTP {code}, state={body.get('state')}")
        if code >= 400:
            print(f"[{flow_id}] DEPLOY FAILED, skipping: {body}")
            return {"flow_id": flow_id, "entity": entity, "error": f"deploy failed: {body}"}
        r = await client.get(f"{APP_BASE}/{flow_id}")
        doc = r.json()

    scope = (doc.get("runtimeScopeMap") or {}).get(root_block_id) or {}
    trigger_id = (scope.get("components") or {}).get("trigger")
    pg_id = doc.get("nifiProcessGroupId")
    if not trigger_id:
        print(f"[{flow_id}] NO TRIGGER COMPONENT FOUND, skipping run. components={scope.get('components')}")
        return {"flow_id": flow_id, "entity": entity, "error": "no trigger id"}

    print(f"[{flow_id}] enabling flow")
    r = await client.post(f"{APP_BASE}/{flow_id}/enabled", json={"enabled": True})
    print(f"[{flow_id}] enable -> HTTP {r.status_code} {r.json()}")

    print(f"[{flow_id}] verb start")
    code, body = await app_verb(client, flow_id, "start")
    print(f"[{flow_id}] start -> HTTP {code}, state={body.get('state')}")

    print(f"[{flow_id}] verb pause (stops trigger only)")
    code, body = await app_verb(client, flow_id, "pause")
    print(f"[{flow_id}] pause -> HTTP {code}, state={body.get('state')}")

    print(f"[{flow_id}] RUN_ONCE trigger {trigger_id}")
    result = await nifi_apply._set_processors_state(auth_conn_dict(auth, url), [trigger_id], "RUN_ONCE")
    print(f"[{flow_id}] RUN_ONCE result:", json.dumps(result))

    print(f"[{flow_id}] polling queue drain (max {max_wait_s}s)")
    stable_zero = 0
    start_t = time.time()
    last_queued = None
    while time.time() - start_t < max_wait_s:
        m = await app_metrics(client, flow_id)
        queued = m.get("queued")
        last_queued = m
        print(f"[{flow_id}]   t+{int(time.time()-start_t)}s queued={queued}")
        if queued == 0:
            stable_zero += 1
            if stable_zero >= 2:
                print(f"[{flow_id}] drained (stable at 0 for 2 polls)")
                break
        else:
            stable_zero = 0
        await asyncio.sleep(5)
    else:
        print(f"[{flow_id}] WARNING: did not confirm drain within {max_wait_s}s")

    print(f"[{flow_id}] checking bulletin board")
    pg_ids = set(await nifi_apply._collect_pg_ids_recursive(url, auth, pg_id))
    bb = await nifi_api_request(url, "GET", "/nifi-api/flow/bulletin-board", params={"limit": "200"}, **auth)
    bulletins = ((bb.get("data") or {}).get("bulletinBoard") or {}).get("bulletins") or []
    relevant = [b for b in bulletins if (b.get("bulletin") or {}).get("groupId") in pg_ids]
    if relevant:
        print(f"[{flow_id}] BULLETINS ({len(relevant)}):")
        for b in relevant:
            bl = b.get("bulletin") or {}
            print(f"    [{bl.get('level')}] {bl.get('sourceName')}: {bl.get('message')}")
    else:
        print(f"[{flow_id}] no bulletins")

    print(f"[{flow_id}] verb stop")
    code, body = await app_verb(client, flow_id, "stop")
    print(f"[{flow_id}] stop -> HTTP {code}, state={body.get('state')}")

    m = await app_metrics(client, flow_id)
    print(f"[{flow_id}] FINAL metrics: queued={m.get('queued')} topicCounts={m.get('topicCounts')}")

    return {
        "flow_id": flow_id,
        "entity": entity,
        "topicCounts": m.get("topicCounts"),
        "queued": m.get("queued"),
        "bulletins": len(relevant),
    }


def auth_conn_dict(auth, url):
    return {
        "endpoint": url,
        "auth_type": auth["auth_type"],
        "username": auth["username"],
        "password": auth["password"],
        "token": auth["token"],
    }


async def main():
    max_wait_s = int(sys.argv[1]) if len(sys.argv) > 1 else 300
    only = sys.argv[2:] if len(sys.argv) > 2 else None
    flow_ids = only if only else FLOW_IDS

    await dbmod.init_db()
    db = dbmod.get_db()
    connections = await _load_connections(db)
    nifi_conn_doc = _active_connection(connections, "nifi")
    conn_dict = _nifi_conn_dict(nifi_conn_doc)
    url = conn_dict["endpoint"]
    auth = {"auth_type": conn_dict["auth_type"], "username": conn_dict["username"],
            "password": conn_dict["password"], "token": conn_dict["token"]}

    results = []
    async with httpx.AsyncClient(timeout=90) as client:
        for flow_id in flow_ids:
            try:
                res = await process_flow(client, url, auth, flow_id, max_wait_s)
            except Exception as exc:
                print(f"[{flow_id}] EXCEPTION: {exc}")
                res = {"flow_id": flow_id, "error": str(exc)}
            results.append(res)
            await asyncio.sleep(2)

    print("\n\n===== SUMMARY =====")
    for r in results:
        print(json.dumps(r))


asyncio.run(main())
