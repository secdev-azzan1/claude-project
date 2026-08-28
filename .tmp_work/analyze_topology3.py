import json
from collections import defaultdict

with open("C:/Users/kaifm/Desktop/claude-project/.tmp_work/s1_maxuseful_dump.json") as f:
    data = json.load(f)

node = data["tree"][0]
procs = node["processors"]
conns = node["connections"]

proc_by_id = {p["id"]: p for p in procs}
proc_by_name = {p["name"]: p for p in procs}

bwd = defaultdict(list)
for c in conns:
    src = c["source"]["id"]
    dst = c["destination"]["id"]
    bwd[dst].append((src, c["selectedRelationships"], c.get("name")))

# Inspect all predecessors of threat_note__list__next_cursor
target = proc_by_name["sentinelone.threat_note__list__next_cursor"]
print("Predecessors of sentinelone.threat_note__list__next_cursor:")
for src_id, rels, cname in bwd.get(target["id"], []):
    p = proc_by_id.get(src_id)
    print(f"  - {p['name'] if p else src_id} [{p['type'].split('.')[-1] if p else '?'}] rel={rels} connName={cname}")

print()
target2 = proc_by_name["sentinelone.threat_note__list__fetch"]
print("Predecessors of sentinelone.threat_note__list__fetch (the InvokeHTTP itself):")
for src_id, rels, cname in bwd.get(target2["id"], []):
    p = proc_by_id.get(src_id)
    print(f"  - {p['name'] if p else src_id} [{p['type'].split('.')[-1] if p else '?'}] rel={rels} connName={cname}")
