"""Stop the flow-vipjvz process group directly in NiFi.

Normally the application drives this. Mongo is down (Docker engine returning
500), so the app's stop verb 500s — and the flow is live against a production
SIEM. Stopping it at the NiFi level is the safe action; nothing else is touched.
"""
import json
import subprocess
import sys
import urllib.parse

NIFI = "https://nifi.datapasc.com"
TARGET_NAME = "fortisiem_device_inventory"


def curl(a, d=None):
    return subprocess.run(["curl.exe", "--http1.1", "-k", "-sS"] + a, input=d, text=True,
                          capture_output=True, timeout=120).stdout


TOKEN = curl(["-H", "Content-Type: application/x-www-form-urlencoded", "--data-binary", "@-",
              f"{NIFI}/nifi-api/access/token"],
             urllib.parse.urlencode({"username": "admin", "password": "Nifiadmin@123"})).strip()
H = ["-H", f"Authorization: Bearer {TOKEN}"]


def nget(p):
    return json.loads(curl(H + [f"{NIFI}{p}"]))


root = nget("/nifi-api/flow/process-groups/root")["processGroupFlow"]["flow"]
match = [g for g in root.get("processGroups", [])
         if TARGET_NAME in (g["component"]["name"] or "")]
if not match:
    print("no PG named", TARGET_NAME)
    sys.exit(1)

for g in match:
    pg_id = g["id"]
    name = g["component"]["name"]
    st = g.get("status", {}).get("aggregateSnapshot", {})
    print("found %s (%s) running=%s" % (name, pg_id, st.get("runningCount")))
    body = json.dumps({"id": pg_id, "state": "STOPPED", "disconnectedNodeAcknowledged": False})
    out = curl(H + ["-X", "PUT", "-H", "Content-Type: application/json", "--data-binary", "@-",
                    f"{NIFI}/nifi-api/flow/process-groups/{pg_id}"], body)
    try:
        print("   ->", json.loads(out).get("state"))
    except Exception:
        print("   ->", out[:200])

for g in match:
    after = nget(f"/nifi-api/process-groups/{g['id']}")
    s = after.get("status", {}).get("aggregateSnapshot", {})
    print("after: running=%s stopped=%s" % (s.get("runningCount"), s.get("stoppedCount")))
