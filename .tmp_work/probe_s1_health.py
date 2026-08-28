import asyncio
import os
import sys

sys.path.insert(0, "backend")

os.environ.setdefault("MONGO_URL", "mongodb://localhost:27018")
os.environ.setdefault("DB_NAME", "dmp_platform")

import httpx
import db as dbmod


async def main():
    await dbmod.init_db()
    db = dbmod.get_db()
    doc = await db["services_v2"].find_one({"id": "svc-b09gdg"}, {"_id": 0})
    if not doc:
        print("service not found")
        return
    cfg = doc.get("config") or {}
    base_url = cfg.get("baseUrl")
    key_name = cfg.get("keyName")
    key_value = cfg.get("keyValue")
    print("baseUrl:", base_url, "keyName:", key_name, "has key:", bool(key_value))

    headers = {key_name: key_value} if key_name and key_value else {}
    async with httpx.AsyncClient(timeout=15) as client:
        for path in ["/threats/2554352354190075342/notes", "/threats/2554352354190075342/timeline?limit=3"]:
            url = base_url.rstrip("/") + path
            try:
                r = await client.get(url, headers=headers)
                print(f"GET {url} -> {r.status_code} body[:150]={r.text[:150]!r}")
            except Exception as exc:
                print(f"GET {url} -> ERROR {exc}")


asyncio.run(main())
