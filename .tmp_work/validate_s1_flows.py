import json

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

results = []
with httpx.Client(timeout=60) as client:
    for flow_id in FLOW_IDS:
        r = client.post(f"{APP_BASE}/{flow_id}/validate")
        body = r.json() if r.headers.get("content-type", "").startswith("application/json") else r.text
        issues = body.get("issues", body) if isinstance(body, dict) else body
        ok = r.status_code < 400 and (not isinstance(body, dict) or not body.get("issues"))
        print(f"[{flow_id}] HTTP {r.status_code} ok={ok} issues={issues if not ok else 'none'}")
        results.append({"flow_id": flow_id, "status": r.status_code, "ok": ok})

print("\n===== SUMMARY =====")
bad = [r for r in results if not r["ok"]]
print(f"{len(results) - len(bad)}/{len(results)} clean")
for r in bad:
    print("FAILED:", json.dumps(r))
