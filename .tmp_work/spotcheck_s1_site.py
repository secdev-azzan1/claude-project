import asyncio
import os
import sys

BACKEND_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "backend")
sys.path.insert(0, BACKEND_DIR)

from dotenv import load_dotenv
load_dotenv(os.path.join(BACKEND_DIR, ".env"))

from services.nifi_client import nifi_api_request
from services.nifi_flow_manager import get_processor_config

NIFI_URL = os.environ["NIFI_URL"]
NIFI_USERNAME = os.environ["NIFI_USERNAME"]
NIFI_PASSWORD = os.environ["NIFI_PASSWORD"]
PG_ID = "42ee2947-01a0-1000-cfde-feb98280ae93"  # flow-s1-site nifiProcessGroupId (post-redeploy)


async def main():
    r = await nifi_api_request(
        NIFI_URL, "GET", f"/nifi-api/process-groups/{PG_ID}/processors",
        auth_type="BASIC", username=NIFI_USERNAME, password=NIFI_PASSWORD,
    )
    if not r.get("ok"):
        print("LIST FAILED:", r)
        return
    procs = r["data"].get("processors", [])
    for p in procs:
        comp = p["component"]
        print(comp["id"], comp["name"], comp["type"])

    target = next((p for p in procs if p["component"]["name"] == "List Sites"), None)
    if not target:
        print("List Sites processor not found")
        return
    cfg = await get_processor_config(
        NIFI_URL, target["component"]["id"],
        auth_type="BASIC", username=NIFI_USERNAME, password=NIFI_PASSWORD,
    )
    print("\n--- List Sites processor properties ---")
    for k, v in cfg["properties"].items():
        print(f"{k} = {v}")


asyncio.run(main())
