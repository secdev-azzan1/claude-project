import asyncio
import sys

sys.path.insert(0, ".")

import db as dbmod
import httpx


async def main():
    await dbmod.init_db()
    db = dbmod.get_db()
    svc = await db["services_v2"].find_one({"id": "svc-nsdqhv"})
    cfg = svc.get("config", {}) if svc else {}
    base_url = cfg.get("baseUrl")
    username = cfg.get("username")
    password = cfg.get("password")

    async with httpx.AsyncClient(verify=False, timeout=30.0) as client:
        for tid in [1, 22, 7]:
            url = f"{base_url}/tags/{tid}/assets"
            resp = await client.get(url, auth=(username, password))
            print(f"--- tag {tid} assets status={resp.status_code} ---")
            print(resp.text[:400])
        for tid in [1]:
            url = f"{base_url}/tags/{tid}/sites"
            resp = await client.get(url, auth=(username, password))
            print(f"--- tag {tid} sites status={resp.status_code} ---")
            print(resp.text[:400])


asyncio.run(main())
