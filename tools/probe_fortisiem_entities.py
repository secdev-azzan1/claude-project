import json
import os
import subprocess
import sys
import time
import urllib.parse


NIFI_BASE = os.environ.get("NIFI_BASE", "https://nifi.datapasc.com").rstrip("/")
NIFI_USER = os.environ.get("NIFI_USER")
NIFI_PASSWORD = os.environ.get("NIFI_PASSWORD")
NIFI_TOKEN = os.environ.get("NIFI_TOKEN")
CLIENT_ID = "codex-fortisiem-probe"

PG_ID = "11a8ce0c-01a0-1000-66c2-2931dd000cbb"  # fortisiem.maximum_useful -- inherits its parameter context

DEVICE_IP = "172.16.232.55"
ORG_NAME = "Super"

# One real GET per probe -- URL only, no request body needed for any of these.
PROBES = {
    "interfaces": "#{SOURCE_API_BASE}/cmdbDeviceInfo/device?ip=" + DEVICE_IP + "&loadDepend=true&fields=interfaces&organization=" + ORG_NAME,
    "processors": "#{SOURCE_API_BASE}/cmdbDeviceInfo/device?ip=" + DEVICE_IP + "&loadDepend=true&fields=processors&organization=" + ORG_NAME,
    "storages": "#{SOURCE_API_BASE}/cmdbDeviceInfo/device?ip=" + DEVICE_IP + "&loadDepend=true&fields=storages&organization=" + ORG_NAME,
    "applications": "#{SOURCE_API_BASE}/cmdbDeviceInfo/device?ip=" + DEVICE_IP + "&loadDepend=true&fields=applications&organization=" + ORG_NAME,
    "properties": "#{SOURCE_API_BASE}/cmdbDeviceInfo/properties?organization=" + ORG_NAME + "&orgId=1&ip=" + DEVICE_IP,
    "full_device": "#{SOURCE_API_BASE}/cmdbDeviceInfo/device?ip=" + DEVICE_IP + "&loadDepend=true&organization=" + ORG_NAME,
    "agent_status": "#{SOURCE_API_BASE}/agentStatus/all?request=1," + DEVICE_IP,
    "short_device_list": "#{SOURCE_API_BASE}/cmdbDeviceInfo/devices?organization=" + ORG_NAME,
}


def run_curl(args, input_text=None, timeout=90, attempts=3):
    last = None
    for i in range(attempts):
        proc = subprocess.run(["curl.exe", "--http1.1", "-k", "-sS"] + args, input=input_text, text=True, capture_output=True, timeout=timeout)
        if proc.returncode == 0:
            return proc.stdout
        last = f"curl exit {proc.returncode}: {proc.stderr[:400]} {proc.stdout[:400]}"
        time.sleep(1 + i)
    raise RuntimeError(last)


def login():
    if NIFI_TOKEN:
        return NIFI_TOKEN
    if not NIFI_USER or not NIFI_PASSWORD:
        raise RuntimeError("Set NIFI_TOKEN or NIFI_USER/NIFI_PASSWORD")
    body = urllib.parse.urlencode({"username": NIFI_USER, "password": NIFI_PASSWORD})
    return run_curl(["-H", "Content-Type: application/x-www-form-urlencoded", "--data-binary", "@-", f"{NIFI_BASE}/nifi-api/access/token"], body).strip()


TOKEN = None


def nifi(method, path, body=None, timeout=90):
    global TOKEN
    if TOKEN is None:
        TOKEN = login()
    args = ["-X", method, "-H", f"Authorization: Bearer {TOKEN}", "-w", "\nHTTP_STATUS:%{http_code}"]
    input_text = None
    if body is not None:
        args += ["-H", "Content-Type: application/json", "--data-binary", "@-"]
        input_text = json.dumps(body)
    args.append(f"{NIFI_BASE}{path}")
    out = run_curl(args, input_text, timeout=timeout)
    raw, status_txt = out.rsplit("\nHTTP_STATUS:", 1)
    status = int(status_txt.strip()[:3])
    if status < 200 or status > 299:
        raise RuntimeError(f"{method} {path} HTTP {status}: {raw[:2000]}")
    raw = raw.strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def flow():
    return nifi("GET", f"/nifi-api/flow/process-groups/{PG_ID}")["processGroupFlow"]["flow"]


def processors_by_name():
    return {p["component"]["name"]: p for p in flow().get("processors", [])}


def connections():
    return flow().get("connections", [])


def stop_processor(proc_id):
    ent = nifi("GET", f"/nifi-api/processors/{proc_id}")
    if ent["component"].get("state") in ("STOPPED", "DISABLED"):
        return
    nifi("PUT", f"/nifi-api/processors/{proc_id}/run-status", {"revision": {"clientId": CLIENT_ID, "version": ent["revision"]["version"]}, "state": "STOPPED"})


def set_processor_state(proc_id, state):
    ent = nifi("GET", f"/nifi-api/processors/{proc_id}")
    if ent["component"].get("state") == state:
        return ent
    return nifi("PUT", f"/nifi-api/processors/{proc_id}/run-status", {"revision": {"clientId": CLIENT_ID, "version": ent["revision"]["version"]}, "state": state})


def update_processor(proc_id, properties=None, auto_terms=None):
    ent = nifi("GET", f"/nifi-api/processors/{proc_id}")
    comp = ent["component"]
    cfg = comp.get("config", {})
    props = {k: v for k, v in (cfg.get("properties") or {}).items() if v != "********"}
    if properties:
        props.update(properties)
    new_cfg = {"properties": props}
    if auto_terms is not None:
        new_cfg["autoTerminatedRelationships"] = auto_terms
    payload = {"revision": {"clientId": CLIENT_ID, "version": ent["revision"]["version"]}, "component": {"id": proc_id, "name": comp["name"], "config": new_cfg}}
    for attempt in range(5):
        try:
            return nifi("PUT", f"/nifi-api/processors/{proc_id}", payload)
        except RuntimeError as exc:
            if "while the Processor is running" in str(exc) and attempt < 4:
                time.sleep(3)
                ent = nifi("GET", f"/nifi-api/processors/{proc_id}")
                payload["revision"]["version"] = ent["revision"]["version"]
                continue
            raise


def create_processor(name, proc_type, x, y, properties=None, auto_terms=None, scheduling_period="1 hour"):
    existing = processors_by_name().get(name)
    if existing:
        update_processor(existing["id"], properties or {}, auto_terms)
        return existing["id"]
    payload = {
        "revision": {"clientId": CLIENT_ID, "version": 0},
        "component": {
            "name": name,
            "type": proc_type,
            "position": {"x": float(x), "y": float(y)},
            "config": {
                "schedulingStrategy": "TIMER_DRIVEN",
                "schedulingPeriod": scheduling_period,
                "executionNode": "ALL",
                "penaltyDuration": "30 sec",
                "yieldDuration": "1 sec",
                "bulletinLevel": "WARN",
                "runDurationMillis": 0,
                "concurrentlySchedulableTaskCount": 1,
                "autoTerminatedRelationships": auto_terms or [],
                "properties": properties or {},
            },
        },
    }
    return nifi("POST", f"/nifi-api/process-groups/{PG_ID}/processors", payload)["id"]


def create_connection(source_id, source_name, dest_id, dest_name, relationships):
    relationships = sorted(relationships)
    for c in connections():
        comp = c["component"]
        if comp["source"]["id"] == source_id and comp["destination"]["id"] == dest_id and sorted(comp.get("selectedRelationships", [])) == relationships:
            return c["id"]
    payload = {
        "revision": {"clientId": CLIENT_ID, "version": 0},
        "component": {
            "parentGroupId": PG_ID,
            "source": {"id": source_id, "type": "PROCESSOR", "groupId": PG_ID, "name": source_name},
            "destination": {"id": dest_id, "type": "PROCESSOR", "groupId": PG_ID, "name": dest_name},
            "selectedRelationships": relationships,
            "flowFileExpiration": "0 sec",
            "backPressureObjectThreshold": 10000,
            "backPressureDataSizeThreshold": "1 GB",
        },
    }
    return nifi("POST", f"/nifi-api/process-groups/{PG_ID}/connections", payload)["id"]


def delete_connections_involving(names):
    names = set(names)
    for c in list(connections()):
        comp = c["component"]
        if comp["source"].get("name") in names or comp["destination"].get("name") in names:
            ent = nifi("GET", f"/nifi-api/connections/{c['id']}")
            queued = ent.get("status", {}).get("aggregateSnapshot", {}).get("flowFilesQueued")
            if queued and str(queued) != "0":
                req = nifi("POST", f"/nifi-api/flowfile-queues/{c['id']}/drop-requests", {"revision": {"clientId": CLIENT_ID, "version": 0}})
                drop_id = req["dropRequest"]["id"]
                for _ in range(30):
                    cur = nifi("GET", f"/nifi-api/flowfile-queues/{c['id']}/drop-requests/{drop_id}")
                    if cur["dropRequest"].get("finished"):
                        break
                    time.sleep(1)
                ent = nifi("GET", f"/nifi-api/connections/{c['id']}")
            version = ent["revision"]["version"]
            nifi("DELETE", f"/nifi-api/connections/{c['id']}?version={version}&clientId={urllib.parse.quote(CLIENT_ID)}")


def delete_processor(proc_id):
    ent = nifi("GET", f"/nifi-api/processors/{proc_id}")
    if ent["component"].get("state") == "RUNNING":
        stop_processor(proc_id)
    version = ent["revision"]["version"]
    nifi("DELETE", f"/nifi-api/processors/{proc_id}?version={version}&clientId={urllib.parse.quote(CLIENT_ID)}")


TRIGGER_NAME = "fortisiem.maximum__admin_probe__trigger"
FETCH_NAME = "fortisiem.maximum__admin_probe__fetch"
LOG_NAME = "fortisiem.maximum__admin_probe__log"


def run_one_probe(entity_key, url):
    trigger = create_processor(TRIGGER_NAME, "org.apache.nifi.processors.standard.GenerateFlowFile", 200, 3600, {"Custom Text": f"probe-{entity_key}", "Batch Size": "1", "Unique FlowFiles": "false"}, [])
    fetch = create_processor(FETCH_NAME, "org.apache.nifi.processors.standard.InvokeHTTP", 520, 3600, {
        "HTTP Method": "GET",
        "HTTP URL": url,
        "HTTP/2 Disabled": "True",
        "Request Username": "#{HTTP_USERNAME}",
        "Request Password": "#{HTTP_PASSWORD}",
        "Connection Timeout": "10 secs",
        "Socket Read Timeout": "30 secs",
    }, ["Retry", "No Retry", "Original", "Failure"])
    log = create_processor(LOG_NAME, "org.apache.nifi.processors.standard.LogAttribute", 840, 3600, {"Log Level": "warn", "Log Payload": "true"}, ["success", "failure"])

    create_connection(trigger, TRIGGER_NAME, fetch, FETCH_NAME, ["success"])
    create_connection(fetch, FETCH_NAME, log, LOG_NAME, ["Response"])

    set_processor_state(log, "RUNNING")
    set_processor_state(fetch, "RUNNING")
    set_processor_state(trigger, "RUNNING")
    time.sleep(2)
    stop_processor(trigger)

    deadline = time.time() + 60
    conn_id = None
    for c in connections():
        comp = c["component"]
        if comp["source"].get("name") == FETCH_NAME and comp["destination"].get("name") == LOG_NAME:
            conn_id = c["id"]
            break
    while conn_id and time.time() < deadline:
        ent = nifi("GET", f"/nifi-api/connections/{conn_id}")
        q = ent.get("status", {}).get("aggregateSnapshot", {}).get("flowFilesQueued")
        if not q or str(q) == "0":
            break
        time.sleep(2)

    stop_processor(fetch)
    stop_processor(log)

    ent = nifi("GET", f"/nifi-api/processors/{fetch}")
    bulletins = []
    for b in ent.get("bulletins") or []:
        bb = b.get("bulletin") or b
        if bb:
            bulletins.append({k: bb.get(k) for k in ["level", "message", "timestamp"]})

    log_ent = nifi("GET", f"/nifi-api/processors/{log}")
    log_bulletins = []
    for b in log_ent.get("bulletins") or []:
        bb = b.get("bulletin") or b
        if bb:
            log_bulletins.append({k: bb.get(k) for k in ["level", "message", "timestamp"]})

    return {"entity": entity_key, "fetch_bulletins": bulletins, "log_bulletins": log_bulletins}


def cleanup():
    delete_connections_involving([TRIGGER_NAME, FETCH_NAME, LOG_NAME])
    procs = processors_by_name()
    for name in (TRIGGER_NAME, FETCH_NAME, LOG_NAME):
        if name in procs:
            delete_processor(procs[name]["id"])


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "help"
    if cmd == "run-one":
        key = sys.argv[2]
        print(json.dumps(run_one_probe(key, PROBES[key]), indent=2))
    elif cmd == "run-all":
        results = []
        for key, url in PROBES.items():
            try:
                results.append(run_one_probe(key, url))
            except Exception as exc:
                results.append({"entity": key, "error": str(exc)})
        print(json.dumps(results, indent=2))
    elif cmd == "cleanup":
        cleanup()
        print(json.dumps({"cleaned": True}, indent=2))
    else:
        raise SystemExit(f"unknown command: {cmd}")
