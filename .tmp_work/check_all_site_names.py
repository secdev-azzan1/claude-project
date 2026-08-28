import asyncio
import httpx
import json


async def main():
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.get("http://127.0.0.1:8000/api/v2/flows/flow-9pey8p/messages",
                                 params={"topic": "raw.rapid7_securado_site_assets.sites"})
        d = resp.json()
    msgs = d.get("messages", [])
    print(f"got {len(msgs)} messages")
    names = []
    for m in msgs:
        rec = json.loads(m["value"])
        names.append((rec.get("id"), repr(rec.get("name"))))
    for row in sorted(names):
        print(row)


asyncio.run(main())
