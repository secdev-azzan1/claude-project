import json
import sys
import time

sys.path.insert(0, __file__.rsplit("\\", 1)[0])
import probe_fortisiem_entities as P

# Probes the "worth pursuing" standalone endpoints found in the FortiSIEM 7.5.1 combined
# API spec: the generic /query/cmdb report engine (one probe per target table), plus
# /watchlist/all, /pub/lookupTable, /organization/list, and /agentStatus/v3/all as
# documented-replacement sanity checks. Every call is single-shot, read-only, size=1
# where the API supports it. Reuses the exact scratch-processor pattern already proven
# in probe_fortisiem_entities.py (trigger -> InvokeHTTP -> LogAttribute(bulletin)),
# inside the live fortisiem PG so #{HTTP_USERNAME}/#{HTTP_PASSWORD} resolve from the
# real parameter context without the password ever passing through this script.

PREFIX = "fortisiem.maximum__admin_probe3"

BASE = "#{SOURCE_API_BASE}"

CMDB_TARGETS = ["USER", "AUDIT", "RULE", "REPORT", "TASK", "MONITOR", "IDENTITY", "EVENT_PULLING", "CASE"]

PROBES = []
PROBES.append(("organization_list_v2", "GET", f"{BASE}/organization/list", None))
PROBES.append(("agent_status_v3", "POST", f"{BASE}/agentStatus/v3/all?start=0&size=1", "[]"))
PROBES.append(("query_cmdb_schema_USER", "GET", f"{BASE}/query/cmdb/schema?target=USER", None))
for t in CMDB_TARGETS:
    PROBES.append((f"query_cmdb_{t}", "POST", f"{BASE}/query/cmdb?size=1&start=0", json.dumps({"target": t})))
PROBES.append(("watchlist_all", "GET", f"{BASE}/watchlist/all", None))
PROBES.append(("lookup_table", "GET", f"{BASE}/pub/lookupTable?size=1", None))


def run_one(key, method, url, body):
    trigger_name = f"{PREFIX}__{key}__trigger"
    fetch_name = f"{PREFIX}__{key}__fetch"
    log_name = f"{PREFIX}__{key}__log"

    trigger_props = {"Custom Text": body or f"probe3-{key}", "Batch Size": "1", "Unique FlowFiles": "false", "Data Format": "Text"}
    trigger = P.create_processor(trigger_name, "org.apache.nifi.processors.standard.GenerateFlowFile", 200, 3900, trigger_props, [])

    fetch_props = {
        "HTTP Method": method,
        "HTTP URL": url,
        "HTTP/2 Disabled": "True",
        "Request Username": "#{HTTP_USERNAME}",
        "Request Password": "#{HTTP_PASSWORD}",
        "Connection Timeout": "10 secs",
        "Socket Read Timeout": "30 secs",
    }
    if method == "POST":
        fetch_props["Content-Type"] = "application/json"
    fetch = P.create_processor(fetch_name, "org.apache.nifi.processors.standard.InvokeHTTP", 520, 3900, fetch_props, ["Retry", "No Retry", "Original", "Failure"])
    log = P.create_processor(log_name, "org.apache.nifi.processors.standard.LogAttribute", 840, 3900, {"Log Level": "warn", "Log Payload": "true"}, ["success", "failure"])

    P.create_connection(trigger, trigger_name, fetch, fetch_name, ["success"])
    P.create_connection(fetch, fetch_name, log, log_name, ["Response"])

    P.set_processor_state(log, "RUNNING")
    P.set_processor_state(fetch, "RUNNING")
    P.set_processor_state(trigger, "RUNNING")
    time.sleep(2)
    P.stop_processor(trigger)

    deadline = time.time() + 60
    conn_id = None
    for c in P.connections():
        comp = c["component"]
        if comp["source"].get("name") == fetch_name and comp["destination"].get("name") == log_name:
            conn_id = c["id"]
            break
    while conn_id and time.time() < deadline:
        ent = P.nifi("GET", f"/nifi-api/connections/{conn_id}")
        q = ent.get("status", {}).get("aggregateSnapshot", {}).get("flowFilesQueued")
        if not q or str(q) == "0":
            break
        time.sleep(2)

    # Let the bulletin actually propagate to the board before we stop/read (async in NiFi).
    time.sleep(4)
    P.stop_processor(fetch)
    P.stop_processor(log)
    time.sleep(1)

    log_ent = P.nifi("GET", f"/nifi-api/processors/{log}")
    log_bulletins = []
    for b in log_ent.get("bulletins") or []:
        bb = b.get("bulletin") or b
        if bb:
            log_bulletins.append({k: bb.get(k) for k in ["level", "message", "timestamp"]})

    return {"key": key, "method": method, "url": url, "log_bulletins": log_bulletins,
            "_names": [trigger_name, fetch_name, log_name]}


def cleanup():
    procs = P.processors_by_name()
    names = [n for n in procs if n.startswith(PREFIX)]
    P.delete_connections_involving(names)
    procs = P.processors_by_name()
    for name in names:
        if name in procs:
            P.delete_processor(procs[name]["id"])


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run-all"
    if cmd == "run-all":
        results = []
        for key, method, url, body in PROBES:
            try:
                results.append(run_one(key, method, url, body))
            except Exception as exc:
                results.append({"key": key, "error": str(exc)})
        print(json.dumps(results, indent=2))
    elif cmd == "cleanup":
        cleanup()
        print(json.dumps({"cleaned": True}))
    else:
        raise SystemExit(f"unknown command: {cmd}")
