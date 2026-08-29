"""Create a dedicated FortiSIEM AppService for the paginated-POST demo.

Base URL mirrors the production NiFi parameter context exactly
(`SOURCE_API_BASE = http://apisix:9080/fortisiem/phoenix/rest`) so NiFi resolves
it in-cluster. Nothing existing is modified.
"""
import json
import urllib.request

BASE = "http://localhost:8000"

payload = {
    "type": "http",
    "name": "FortiSIEM CMDB (paginated POST demo)",
    "config": {
        "baseUrl": "http://apisix:9080/fortisiem/phoenix/rest",
        "authMode": "basic",
        "username": "super/CMDBAPI",
        "password": "F0rti$iem@2024!!",
        "proxyId": None,
    },
}

req = urllib.request.Request(
    f"{BASE}/api/v2/services/",
    data=json.dumps(payload).encode(),
    headers={"Content-Type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(req, timeout=30) as r:
    out = json.loads(r.read())

print("service id :", out["id"])
print("baseUrl    :", out["config"]["baseUrl"])
print("authMode   :", out["config"]["authMode"])
print("username   :", out["config"]["username"])
print("hasPassword:", out.get("hasPassword"))
