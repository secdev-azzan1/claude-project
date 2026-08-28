import asyncio, os, sys, json
from pathlib import Path
sys.path.insert(0, ".")
from dotenv import load_dotenv
load_dotenv(".env")
from services.nifi_client import nifi_api_request

async def main():
    base_url = os.environ["NIFI_URL"]
    username = os.environ["NIFI_USERNAME"]
    password = os.environ["NIFI_PASSWORD"]

    submit_body = {"provenance": {"request": {"maxResults": 10000, "searchTerms": {}}}}
    r = await nifi_api_request(base_url, "POST", "/nifi-api/provenance", auth_type="BASIC",
                                username=username, password=password, json_body=submit_body)
    query_id = r["data"]["provenance"]["id"]
    prov = None
    for _ in range(15):
        await asyncio.sleep(1)
        r2 = await nifi_api_request(base_url, "GET", f"/nifi-api/provenance/{query_id}",
                                     auth_type="BASIC", username=username, password=password)
        prov = r2["data"]["provenance"]
        if prov["finished"]:
            break

    events = prov["results"]["provenanceEvents"]
    print(f"total events in window: {len(events)}")
    # narrow to our routing components
    target_names = {
        "route_fields__check_0", "route_fields", "route_fields__merge_0",
        "route__exclude_cced_windows_quarter_sitepublish__rule_0",
        "t13__extract", "t12__add_field",
    }
    hits = [e for e in events if e.get("componentName") in target_names]
    print(f"events on target components: {len(hits)}")
    for e in hits[:40]:
        print(e.get("eventTime"), "|", e.get("componentName"), "|", e.get("eventType"), "| ff:", e.get("flowFileUuid"))

asyncio.run(main())
