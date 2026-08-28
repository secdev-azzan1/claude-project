import asyncio, os, sys, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / "backend" / ".env")
import httpx

async def main():
    base = "https://apisix.datapasc.com/rapid7_securado/api/3"
    auth = ("apiuser", os.environ.get("RAPID7_PASSWORD") or "")
    # fall back: read password from app service config via backend API instead of env
    async with httpx.AsyncClient(timeout=60.0) as client:
        r = await client.get("http://127.0.0.1:8000/api/v2/flows/flow-9pey8p/messages",
                              params={"topic": "raw.rapid7_securado_site_assets.sites"})
        d = r.json()
    msgs = d.get("messages", [])
    for m in msgs:
        rec = json.loads(m["value"])
        if rec.get("id") == 55:
            print("site 55 full record:")
            print(json.dumps(rec, indent=2))
            break

asyncio.run(main())
