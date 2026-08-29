import sys
sys.path.insert(0, "tools")
import probe_fortisiem_entities as pf

TRIGGER_ID = "4811b763-01a0-1000-785f-54d06a1c4f6d"

ent = pf.nifi("GET", f"/nifi-api/processors/{TRIGGER_ID}")
version = ent["revision"]["version"]
resp = pf.nifi("PUT", f"/nifi-api/processors/{TRIGGER_ID}/run-status",
                {"revision": {"clientId": pf.CLIENT_ID, "version": version}, "state": "RUN_ONCE"})
print("run-once state:", resp["component"].get("state"))
