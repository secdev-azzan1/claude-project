import json
from collections import defaultdict

with open("C:/Users/kaifm/Desktop/claude-project/.tmp_work/s1_maxuseful_dump.json") as f:
    data = json.load(f)

node = data["tree"][0]
procs = node["processors"]
conns = node["connections"]

proc_by_id = {p["id"]: p for p in procs}
proc_by_name = {p["name"]: p for p in procs}

fwd = defaultdict(list)
bwd = defaultdict(list)
for c in conns:
    src = c["source"]["id"]
    dst = c["destination"]["id"]
    fwd[src].append((dst, c["selectedRelationships"]))
    bwd[dst].append((src, c["selectedRelationships"]))

# The 4 child InvokeHTTP that require a parent-supplied path parameter:
child_fetchers = [
    "sentinelone.group_policy__fetch",
    "sentinelone.site_policy__fetch",
    "sentinelone.threat_note__list__fetch",
    "sentinelone.threat_timeline__list__fetch",
]

def trace_full_path_upstream(start_name, max_hops=25):
    """Walk backward printing every processor name until we hit a *__list__fetch InvokeHTTP."""
    start = proc_by_name[start_name]
    path = [f"{start['name']} [{start['type'].split('.')[-1]}]"]
    cur = start["id"]
    visited = set()
    for _ in range(max_hops):
        preds = bwd.get(cur, [])
        if not preds:
            path.append("<no predecessor / source>")
            break
        # take first predecessor (there may be multiple incoming - note if so)
        if len(preds) > 1:
            path.append(f"<<{len(preds)} predecessors, following first>>")
        src_id = preds[0][0]
        if src_id in visited:
            path.append("<cycle detected, stop>")
            break
        visited.add(src_id)
        p = proc_by_id.get(src_id)
        if not p:
            path.append(f"<non-processor node: {src_id}>")
            break
        path.append(f"{p['name']} [{p['type'].split('.')[-1]}]")
        if "InvokeHTTP" in p["type"] and "__list__fetch" in p["name"]:
            break
        cur = src_id
    return path

for cf in child_fetchers:
    print(f"\n=== Upstream chain for {cf} ===")
    chain = trace_full_path_upstream(cf)
    for i, step in enumerate(reversed(chain)):
        print("  " * i + "-> " + step)
