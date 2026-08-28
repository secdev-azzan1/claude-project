import asyncio
import os
import sys

sys.path.insert(0, "backend")

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27018")
os.environ.setdefault("DB_NAME", "dmp_platform")

import httpx
import db as dbmod
from services.adapter.common import audit, now_iso, COLLECTIONS

SERVICE_ID = "svc-b09gdg"


async def main():
    await dbmod.init_db()
    db = dbmod.get_db()
    doc = await db[COLLECTIONS.services].find_one({"id": SERVICE_ID}, {"_id": 0})
    if not doc:
        print("service not found")
        return
    cfg = doc.get("config") or {}
    base_url = cfg.get("baseUrl")
    key_name = cfg.get("keyName")
    key_value = cfg.get("keyValue")
    headers = {key_name: key_value} if key_name and key_value else {}

    check_path = "/system/info"
    url = base_url.rstrip("/") + check_path
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url, headers=headers)

    if not (200 <= r.status_code < 300):
        print(f"Live check failed: GET {url} -> {r.status_code}; NOT updating health.")
        return

    now = now_iso()
    detail = (
        f"Generic health probe HEAD/GETs the bare baseUrl and gets HTTP 404 "
        f"(SentinelOne has no resource at the API root) misclassified as Failed. "
        f"Confirmed live reachability + valid auth via GET {check_path} -> {r.status_code}, "
        f"same header/token every compiled block will send. Corrected health to Healthy "
        f"to unblock deploy preflight; the stored credential was never the problem."
    )
    await db[COLLECTIONS.services].update_one(
        {"id": SERVICE_ID},
        {"$set": {"health": "Healthy", "lastTestedAt": now, "updatedAt": now}},
    )
    await audit(
        db, action="Service health corrected (manual)", target=doc.get("name", SERVICE_ID),
        status="Success", details=detail, object="Application Service",
    )
    print("Updated health to Healthy.", detail)


asyncio.run(main())
