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
    with httpx.Client(timeout=120) as client:
        for flow_id in flow_ids:
            r = client.post(f"{APP_BASE}/{flow_id}/verbs/redeploy")
            if r.status_code >= 400:
                print(f"[{flow_id}] REDEPLOY FAILED {r.status_code}: {r.text[:500]}")
                results.append({"flow_id": flow_id, "error": f"{r.status_code}: {r.text[:300]}"})
                continue
            body = r.json()
            print(f"[{flow_id}] redeploy -> HTTP {r.status_code}, state={body.get('state')}")
            results.append({"flow_id": flow_id, "ok": True, "state": body.get("state")})

    print("\n===== SUMMARY =====")
    for r in results:
        print(json.dumps(r))


main()
