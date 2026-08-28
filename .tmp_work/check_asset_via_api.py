import asyncio, httpx, json

async def check(topic):
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.get("http://127.0.0.1:8000/api/v2/flows/flow-9pey8p/messages", params={"topic": topic})
        d = resp.json()
    msgs = d.get("messages", [])
    print(f"{topic}: got {len(msgs)} messages")
    site_ids = set()
    hits = []
    for m in msgs:
        try:
            rec = json.loads(m["value"])
        except Exception:
            continue
        sid = rec.get("site_id")
        if sid is not None:
            site_ids.add(sid)
        blob = json.dumps(rec)
        if sid == "40" or "CCED Windows QUARTER" in blob or '"site:40"' in blob or ':40_' in blob.replace('"','') :
            hits.append(rec)
    print("  site_ids in sample:", sorted(site_ids))
    print("  hits for site 40:", len(hits))
    for h in hits[:3]:
        print(json.dumps(h, indent=2)[:800])

async def main():
    await check("bronze.rapid7_securado.asset")
    print()
    await check("raw.rapid7_securado_site_assets.asset")

asyncio.run(main())
