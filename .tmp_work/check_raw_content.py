import asyncio
import httpx

async def main():
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.get("http://127.0.0.1:8000/api/v2/flows/flow-9pey8p/messages",
                                 params={"topic": "raw.rapid7_securado_site_assets.sites"})
        d = resp.json()
    msgs = d.get("messages", [])[:1]
    for m in msgs:
        raw = m["value"]
        # find the site_id and id substrings raw
        import re
        for key in ("id", "site_id"):
            match = re.search(rf'"{key}"\s*:\s*[^,}}]+', raw)
            print(key, "->", match.group(0) if match else "NOT FOUND")

asyncio.run(main())
