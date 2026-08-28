import asyncio
import os
import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / "backend" / ".env")

from services.nifi_client import nifi_api_request  # noqa: E402


async def main():
    base_url = os.environ["NIFI_URL"]
    username = os.environ["NIFI_USERNAME"]
    password = os.environ["NIFI_PASSWORD"]

    submit_body = {"provenance": {"request": {"maxResults": 10000, "searchTerms": {}}}}
    r = await nifi_api_request(base_url, "POST", "/nifi-api/provenance", auth_type="BASIC",
                                username=username, password=password, json_body=submit_body)
    query_id = r["data"]["provenance"]["id"]
    for _ in range(10):
        await asyncio.sleep(1)
        r2 = await nifi_api_request(base_url, "GET", f"/nifi-api/provenance/{query_id}",
                                     auth_type="BASIC", username=username, password=password)
        prov = r2["data"]["provenance"]
        if prov["finished"]:
            break

    events = prov["results"]["provenanceEvents"]
    fetch_events = [e for e in events if e.get("componentName") == "fetch" and e.get("eventType") == "DROP"]
    print(f"found {len(fetch_events)} fetch DROP events")
    if not fetch_events:
        return
    ev = fetch_events[0]
    ev_id = ev["eventId"]
    group_id = ev["groupId"]
    print("event id:", ev_id, "group:", group_id)

    r3 = await nifi_api_request(base_url, "GET", f"/nifi-api/provenance-events/{ev_id}",
                                 auth_type="BASIC", username=username, password=password)
    detail = r3["data"]["provenanceEvent"]
    attrs = detail.get("attributes", [])
    for a in attrs:
        name = a.get("name", "")
        if "invokehttp" in name.lower() or "status" in name.lower() or "url" in name.lower() or name in ("site_id", "id"):
            print(" ", name, "=", (a.get("value") or "")[:200])


asyncio.run(main())
