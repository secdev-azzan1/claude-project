import asyncio
import json
import sys

sys.path.insert(0, "backend")
sys.path.insert(0, ".tmp_work")
import os
os.environ.setdefault("MONGO_URL", "mongodb://localhost:27018")
os.environ.setdefault("DB_NAME", "dmp_platform")

import httpx
import db as dbmod
import batch_kafka_kc as bk
from services.adapter.deployer.lifecycle import _load_connections, _active_connection, _nifi_conn_dict

APP_BASE = bk.APP_BASE

PLAN = [
    ("flow-9pey8p", {"asset_service", "asset_software", "asset_vulnerability", "asset_vulnerability_solution", "site_organization"}),
    ("flow-tags", None),
    ("flow-vuln-reference", None),
    ("flow-vuln-category", None),
    ("flow-exploit", None),
    ("flow-malware-kit", None),
]


async def main():
    await dbmod.init_db()
    db = dbmod.get_db()
    connections = await _load_connections(db)
    nifi_conn_doc = _active_connection(connections, "nifi")
    conn_dict = _nifi_conn_dict(nifi_conn_doc)
    bk.conn_dict_global = conn_dict
    url = conn_dict["endpoint"]
    auth = {"auth_type": conn_dict["auth_type"], "username": conn_dict["username"],
            "password": conn_dict["password"], "token": conn_dict["token"]}

    summary = []
    async with httpx.AsyncClient(timeout=60) as client:
        for flow_id, allowed in PLAN:
            try:
                res = await bk.process_flow(client, url, auth, flow_id, allowed_entities=allowed)
            except Exception as exc:
                bk.log(f"[{flow_id}] EXCEPTION: {exc!r}")
                res = {"flow_id": flow_id, "ok": False, "error": repr(exc)}
            summary.append(res)

    bk.log("\n===== R7 BATCH SUMMARY =====")
    for s in summary:
        bk.log(json.dumps({k: v for k, v in s.items() if k != "results"}))


asyncio.run(main())
