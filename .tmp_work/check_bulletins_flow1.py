import sys
sys.path.insert(0, "tools")
import probe_fortisiem_entities as pf
import json

FLOW_PG = "481076f8-01a0-1000-ff43-bd4125ecf164"

status, resp = pf.nifi("GET", f"/nifi-api/flow/process-groups/{FLOW_PG}/status?recursive=true") if False else (None, None)
# bulletin board scoped to this PG
resp = pf.nifi("GET", f"/nifi-api/flow/bulletin-board?groupId={FLOW_PG}")
bulletins = resp.get("bulletinBoard", {}).get("bulletins", [])
print(f"bulletin count: {len(bulletins)}")
for b in bulletins[:40]:
    bb = b.get("bulletin", b)
    print(bb.get("level"), "|", bb.get("sourceName"), "|", bb.get("message"))
