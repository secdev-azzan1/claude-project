"""What FortiSIEM coverage already exists in the application?

Lists every flow whose service is a FortiSIEM one (or whose name/topics say
fortisiem), and for each block: adapter/mode, entity, path, parent, pagination.
Read-only.
"""
import json
import urllib.request

BASE = "http://localhost:8010"


def get(p):
    return json.loads(urllib.request.urlopen(f"{BASE}{p}", timeout=60).read())


services = {s["id"]: s for s in get("/api/v2/services/")}
fs_svc = {sid for sid, s in services.items()
          if "fortisiem" in (s.get("name", "") + str(s.get("config", {}).get("baseUrl", ""))).lower()}

print("FortiSIEM services:")
for sid in sorted(fs_svc):
    s = services[sid]
    print("   %-12s %-42s retired=%-5s %s" % (sid, s["name"], s.get("retired"), s["config"].get("baseUrl")))

flows = get("/api/v2/flows/")
print("\nFlows touching FortiSIEM:")
covered = {}
for f in flows:
    blocks = f.get("blocks", [])
    hits = any(b.get("serviceId") in fs_svc for b in blocks)
    if not hits and "fortisiem" not in json.dumps(f).lower():
        continue
    print("\n=== %s | %s | state=%s | enabled=%s" % (f["id"], f["name"], f.get("state"), f.get("enabled")))
    for b in blocks:
        cfg = b.get("config") or {}
        pag = (cfg.get("pagination") or {}).get("type", "-")
        ent = b.get("entity") or "-"
        line = "    %-9s %-6s parent=%-10s entity=%-26s" % (
            b["adapter"], b.get("mode", "-"), b.get("parentId") or "ROOT", ent)
        if b["adapter"] == "http":
            line += " %-5s %-52s pag=%s" % (cfg.get("method", "-"), cfg.get("path", "-"), pag)
        print(line)
        if b["adapter"] == "kafka" and ent != "-":
            covered.setdefault(ent, []).append(f["id"])
    for t in f.get("topics", []):
        print("      topic: %s" % t.get("name"))

print("\n\n=== ENTITIES ALREADY COVERED BY A KAFKA WRITE ===")
for e in sorted(covered):
    print("   %-28s %s" % (e, ", ".join(covered[e])))
print("\ntotal distinct entities covered:", len(covered))
