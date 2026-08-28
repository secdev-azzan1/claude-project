import asyncio, os, sys, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / "backend" / ".env")
from services.nifi_client import nifi_api_request

async def main():
    base_url = os.environ["NIFI_URL"]
    username = os.environ["NIFI_USERNAME"]
    password = os.environ["NIFI_PASSWORD"]
    root_pg = "3e2d213a-01a0-1000-6298-39c21c8fc8ab"

    r = await nifi_api_request(base_url, "GET", f"/nifi-api/process-groups/{root_pg}/process-groups",
                                auth_type="BASIC", username=username, password=password)
    groups = r["data"]["processGroups"]
    site_group = None
    for g in groups:
        comp = g["component"]
        print("PG:", comp["id"], comp["name"])
        if comp["name"] == "b-site" or "site" in comp["name"].lower():
            if comp["name"] == "b-site":
                site_group = comp["id"]
    if not site_group:
        print("b-site group not found by exact name, listing all names above")
        return

    print("\nb-site group id:", site_group)
    r2 = await nifi_api_request(base_url, "GET", f"/nifi-api/process-groups/{site_group}/connections",
                                 auth_type="BASIC", username=username, password=password)
    conns = r2["data"]["connections"]
    print(f"\n{len(conns)} connections in b-site group:")
    for c in conns:
        comp = c["component"]
        src = comp["source"]
        dst = comp["destination"]
        print(f"  {src.get('name')} [{src.get('type')}] --{comp.get('selectedRelationships')}--> {dst.get('name')} [{dst.get('type')}]")

asyncio.run(main())
