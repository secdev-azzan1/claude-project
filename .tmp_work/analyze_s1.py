import json
from collections import defaultdict

with open("C:/Users/kaifm/Desktop/claude-project/.tmp_work/s1_maxuseful_dump.json") as f:
    data = json.load(f)

node = data["tree"][0]
procs = node["processors"]
conns = node["connections"]

proc_by_id = {p["id"]: p for p in procs}

print(f"=== TOTAL PROCESSORS: {len(procs)} ===")
print(f"=== TOTAL CONNECTIONS: {len(conns)} ===\n")

# Processor type breakdown
type_counts = defaultdict(int)
for p in procs:
    short_type = p["type"].split(".")[-1]
    type_counts[short_type] += 1
print("=== PROCESSOR TYPE COUNTS ===")
for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
    print(f"  {t}: {c}")

# Invalid processors
invalid = [p for p in procs if p["validationStatus"] != "VALID"]
print(f"\n=== INVALID PROCESSORS: {len(invalid)} ===")
for p in invalid:
    print(f"  - {p['name']!r} ({p['type'].split('.')[-1]}) state={p['state']} runStatus={p['runStatus']}")
    for e in p["validationErrors"]:
        print(f"      ERROR: {e}")

# Run state breakdown
state_counts = defaultdict(int)
for p in procs:
    state_counts[p["state"]] += 1
print(f"\n=== RUN STATE COUNTS ===")
for s, c in sorted(state_counts.items(), key=lambda x: -x[1]):
    print(f"  {s}: {c}")

# InvokeHTTP processors and their URL properties
invoke_http = [p for p in procs if "InvokeHTTP" in p["type"]]
print(f"\n=== InvokeHTTP PROCESSORS: {len(invoke_http)} ===")
for p in invoke_http:
    print(f"\n  [{p['name']}] state={p['state']} valid={p['validationStatus']}")
    # print all props containing url-ish keys, plus HTTP Method
    all_props = p["all_props"]
    for k, v in all_props.items():
        kl = k.lower()
        if "url" in kl or "method" in kl:
            print(f"      {k} = {v}")

with open("C:/Users/kaifm/Desktop/claude-project/.tmp_work/s1_analysis_out.json", "w") as f:
    json.dump({
        "type_counts": type_counts,
        "invalid_count": len(invalid),
        "invalid_names": [p["name"] for p in invalid],
    }, f, indent=2)
