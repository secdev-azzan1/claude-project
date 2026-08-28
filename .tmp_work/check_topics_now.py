import asyncio, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))
import httpx

async def main():
    async with httpx.AsyncClient(timeout=120.0) as client:
        resp = await client.get("http://127.0.0.1:8000/api/v2/flows/flow-9pey8p/metrics")
        d = resp.json()
    print("queued:", d.get("queued"))
    for t in d.get("topicCounts", []):
        print(t)

asyncio.run(main())
