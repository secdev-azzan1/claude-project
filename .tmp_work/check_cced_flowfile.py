import asyncio, os, sys, json
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
    route_events = [e for e in events if e.get("componentName") == "route__exclude_cced_windows_quarter_sitepublish__rule_0" and e.get("eventType") == "ROUTE"]
    print(f"{len(route_events)} ROUTE events on the sitepublish gate")

    for e in route_events:
        ev_id = e["eventId"]
        r3 = await nifi_api_request(base_url, "GET", f"/nifi-api/provenance-events/{ev_id}",
                                     auth_type="BASIC", username=username, password=password)
        detail = r3["data"]["provenanceEvent"]
        attrs = {a["name"]: a.get("value") for a in detail.get("attributes", [])}
        name_val = attrs.get("name")
        site_id_val = attrs.get("site_id")
        if site_id_val == "40" or name_val == "CCED Windows QUARTER":
            print("FOUND CCED flowfile!")
            print("  eventId:", ev_id, "ff:", e.get("flowFileUuid"))
            print("  relationship:", detail.get("relationship"))
            print("  name attr:", repr(name_val))
            print("  site_id attr:", repr(site_id_val))
            print("  full attrs:", json.dumps(attrs, indent=2))
            return
    print("Did not find a CCED flowfile among ROUTE events in this window (may be outside provenance window).")

asyncio.run(main())
