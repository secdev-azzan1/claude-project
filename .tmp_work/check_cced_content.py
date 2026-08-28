import asyncio, os, sys, json
sys.path.insert(0, ".")
from dotenv import load_dotenv
load_dotenv(".env")
from services.nifi_client import nifi_api_request
import httpx

async def main():
    base_url = os.environ["NIFI_URL"]
    username = os.environ["NIFI_USERNAME"]
    password = os.environ["NIFI_PASSWORD"]
    event_id = 86677851

    async with httpx.AsyncClient(timeout=30.0, verify=True) as client:
        for direction in ("input", "output"):
            url = f"{base_url}/nifi-api/provenance-events/{event_id}/content/{direction}"
            resp = await client.get(url, auth=(username, password))
            print(f"--- {direction} ({resp.status_code}) ---")
            print(resp.text[:600])
            print()

asyncio.run(main())
