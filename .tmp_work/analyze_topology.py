import json
import re
from collections import defaultdict

with open("C:/Users/kaifm/Desktop/claude-project/.tmp_work/s1_maxuseful_dump.json") as f:
    data = json.load(f)

node = data["tree"][0]
procs = node["processors"]
conns = node["connections"]

proc_by_id = {p["id"]: p for p in procs}

# Build forward adjacency: source_id -> list of (dest_id, relationship)
fwd = defaultdict(list)
bwd = defaultdict(list)
for c in conns:
    src = c["source"]["id"]
    dst = c["destination"]["id"]
    fwd[src].append((dst, c["selectedRelationships"]))
    bwd[dst].append((src, c["selectedRelationships"]))

# Find UpdateAttribute processors that SET the path-parameter attributes used by
# child InvokeHTTP URLs: s1_object_id, s1_site_id, s1_threat_id, etc.
param_names = set()
for p in procs:
    if "InvokeHTTP" in p["type"]:
        url = p["all_props"].get("HTTP URL", "") or ""
        for m in re.findall(r"\$\{([a-zA-Z0-9_]+)\}", url):
            if m not in ("cursor",) and not m.startswith("window_"):
                param_names.add(m)

print("Path-parameter attribute names found in InvokeHTTP URLs (excluding cursor/window):")
print(" ", sorted(param_names))

# Find which processors SET these attributes (UpdateAttribute or EvaluateJsonPath with matching prop value)
setters = defaultdict(list)
for p in procs:
    props = p["all_props"]
    for k, v in props.items():
        if k in param_names or (isinstance(v, str) and any(pn == k for pn in param_names)):
            pass
    for pn in param_names:
        if pn in props:
            setters[pn].append((p["name"], p["type"].split(".")[-1], props[pn]))

print("\nProcessors that set/reference each path-parameter:")
for pn, lst in setters.items():
    print(f"\n  ${{{pn}}}:")
    for name, typ, val in lst:
        print(f"    - [{typ}] {name}: {pn} = {val!r}")

# Now trace backward from each of those setter processors through the connection graph
# to find which list__fetch InvokeHTTP is the ultimate upstream ancestor (the "parent entity").
def trace_upstream_to_list_fetch(start_id, max_hops=15):
    """BFS backward from start_id, return list of InvokeHTTP 'list__fetch' processor names encountered."""
    visited = set()
    queue = [(start_id, 0)]
    found = []
    while queue:
        cur, hops = queue.pop(0)
        if cur in visited or hops > max_hops:
            continue
        visited.add(cur)
        p = proc_by_id.get(cur)
        if p and "InvokeHTTP" in p["type"] and "__list__fetch" in p["name"]:
            found.append(p["name"])
            continue  # stop this branch once we hit a list fetch
        for (src, rels) in bwd.get(cur, []):
            queue.append((src, hops + 1))
    return found

print("\n=== BACKWARD TRACE: for each param-setting processor, which list__fetch feeds it? ===")
for pn, lst in setters.items():
    for name, typ, val in lst:
        # find proc id
        pid = next((p["id"] for p in procs if p["name"] == name and p["type"].split(".")[-1] == typ), None)
        if pid:
            ancestors = trace_upstream_to_list_fetch(pid)
            print(f"  {name} (sets ${{{pn}}}) <- upstream list__fetch(es): {ancestors}")
