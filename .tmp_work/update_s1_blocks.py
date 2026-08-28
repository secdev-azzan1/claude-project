import json
import sys

import httpx

APP_BASE = "http://127.0.0.1:8000/api/v2/flows"

FLOW_IDS = [
    "flow-s1-site", "flow-s1-group", "flow-s1-threat",
    "flow-s1-service-user", "flow-s1-location", "flow-s1-ioc",
    "flow-s1-activity-type", "flow-s1-role", "flow-s1-cloud-detection-rule",
    "flow-s1-agent-tag", "flow-s1-xdr-asset-tag", "flow-s1-agent-package",
    "flow-s1-config-override", "flow-s1-exclusion", "flow-s1-user",
    "flow-s1-application-cve", "flow-s1-alert", "flow-s1-xdr-asset",
    "flow-s1-activity", "flow-s1-agent", "flow-s1-restriction",
    "flow-s1-installed-application", "flow-s1-tenant-policy", "flow-s1-system-info",
]


def main():
    only = sys.argv[1:] or None
    flow_ids = only if only else FLOW_IDS
    results = []
    with httpx.Client(timeout=60) as client:
        for flow_id in flow_ids:
            live = client.get(f"{APP_BASE}/{flow_id}").json()
            with open(f".tmp_work/{flow_id}.json") as fh:
                built = json.load(fh)

            live_block_ids = {b["id"] for b in live["blocks"]}
            built_block_ids = {b["id"] for b in built["blocks"]}
            if live_block_ids != built_block_ids:
                print(f"[{flow_id}] BLOCK ID MISMATCH live={live_block_ids} built={built_block_ids} -- skipping")
                results.append({"flow_id": flow_id, "error": "block id mismatch"})
                continue

            live["blocks"] = built["blocks"]

            r = client.post(f"{APP_BASE}/", json=live)
            if r.status_code >= 400:
                print(f"[{flow_id}] SAVE FAILED {r.status_code}: {r.text[:500]}")
                results.append({"flow_id": flow_id, "error": f"save failed {r.status_code}: {r.text[:300]}"})
                continue
            print(f"[{flow_id}] blocks updated ({len(built['blocks'])} blocks)")
            results.append({"flow_id": flow_id, "ok": True})

    print("\n===== SUMMARY =====")
    for r in results:
        print(json.dumps(r))


main()
