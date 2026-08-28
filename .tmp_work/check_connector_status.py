import asyncio, os, sys, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / "backend" / ".env")
import httpx

async def main():
    url = os.environ["KAFKA_CONNECT_URL"].rstrip("/")
    name = "rapid7_securado_site_assets.b-site-avro-write.kafka_kc"
    async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
        r = await client.get(f"{url}/connectors/{name}/status")
        print(json.dumps(r.json(), indent=2))

asyncio.run(main())
