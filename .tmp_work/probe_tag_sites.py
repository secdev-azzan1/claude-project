import asyncio
import sys
import json

sys.path.insert(0, ".")

import db as dbmod
import httpx


async def main():
    await dbmod.init_db()
    db = dbmod.get_db()
    svc = await db["services_v2"].find_one({"id": "svc-nsdqhv"})
    cfg = svc.get("config", {}) if svc else {}
    base_url = cfg.get("baseUrl") or cfg.get("base_url")
    print("service base url:", base_url)
    print("service auth keys:", [k for k in cfg.keys()])

    auth_header = cfg.get("authHeader") or "Authorization"
    username = cfg.get("username")
    password = cfg.get("password")
    api_key = cfg.get("apiKey")

    tag_ids = [22, 7, 1]
    async with httpx.AsyncClient(verify=False, timeout=30.0) as client:
        for tid in tag_ids:
            url = f"{base_url}/tags/{tid}/sites"
            auth = (username, password) if username else None
            headers = {}
            if api_key:
                headers[auth_header] = api_key
            resp = await client.get(url, auth=auth, headers=headers)
            print(f"--- tag {tid} status={resp.status_code} ---")
            print(resp.text[:500])


asyncio.run(main())
