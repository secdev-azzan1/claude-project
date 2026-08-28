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

PG_ID = os.environ.get("INGEST_PARENT_PG_ID", "0a00e822-01a0-1000-68b7-f28e69779c95")
CLIENT_ID = "codex-global-redis-clear"

REDIS_POOL_ID = os.environ.get("GLOBAL_REDIS_POOL_ID", "b90bcbdb-d69c-3725-51d1-444dd57b9336")


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
        return
    nifi("PUT", f"/nifi-api/processors/{proc_id}/run-status", {"revision": {"clientId": CLIENT_ID, "version": ent["revision"]["version"]}, "state": state})


def create_processor(name, proc_type, x, y, properties=None, auto_terms=None, scheduling_period="0 sec"):
    existing = processors_by_name().get(name)
    if existing:
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


def drop_queue(conn_id):
    req = nifi("POST", f"/nifi-api/flowfile-queues/{conn_id}/drop-requests", {"revision": {"clientId": CLIENT_ID, "version": 0}})
    drop_id = req["dropRequest"]["id"]
    for _ in range(30):
        cur = nifi("GET", f"/nifi-api/flowfile-queues/{conn_id}/drop-requests/{drop_id}")
        if cur["dropRequest"].get("finished"):
            break
        time.sleep(1)
    nifi("DELETE", f"/nifi-api/flowfile-queues/{conn_id}/drop-requests/{drop_id}")


def delete_connections_involving(names, allow_drop=True):
    names = set(names)
    for c in list(connections()):
        comp = c["component"]
        if comp["source"].get("name") in names or comp["destination"].get("name") in names:
            ent = nifi("GET", f"/nifi-api/connections/{c['id']}")
            queued = ent.get("status", {}).get("aggregateSnapshot", {}).get("flowFilesQueued")
            if queued and str(queued) != "0":
                if not allow_drop:
                    raise RuntimeError(f"Connection {c['id']} has queued FlowFiles; refusing to delete")
                # These are only ever trivial admin/trigger flowfiles (no production data flows
                # through this temp scaffolding), so dropping the queue here is safe.
                drop_queue(c["id"])
                ent = nifi("GET", f"/nifi-api/connections/{c['id']}")
            version = ent["revision"]["version"]
            nifi("DELETE", f"/nifi-api/connections/{c['id']}?version={version}&clientId={urllib.parse.quote(CLIENT_ID)}")


def delete_processor(proc_id):
    ent = nifi("GET", f"/nifi-api/processors/{proc_id}")
    if ent["component"].get("state") == "RUNNING":
        stop_processor(proc_id)
    version = ent["revision"]["version"]
    nifi("DELETE", f"/nifi-api/processors/{proc_id}?version={version}&clientId={urllib.parse.quote(CLIENT_ID)}")


REDIS_CLEAR_SCRIPT = r'''
import groovy.json.JsonOutput

def flowFile = session.get()
if (!flowFile) return

def prop = { name -> context.getProperty(name).evaluateAttributeExpressions(flowFile).getValue() }

def poolId = prop('REDIS_POOL_ID')
def pattern = prop('KEY_PATTERN') ?: '*'
def maxDeletes = (prop('MAX_DELETES') ?: '10000000') as int
def pool = context.controllerServiceLookup.getControllerService(poolId)
if (pool == null) {
    flowFile = session.putAttribute(flowFile, 'redis.clear.error', "Redis pool not found: ${poolId}")
    session.transfer(flowFile, REL_FAILURE)
    return
}

def matched = 0
def deleted = 0
def truncated = false
try {
    def conn = pool.getConnection()
    try {
        def keys = conn.keys(pattern.getBytes('UTF-8')) ?: []
        matched = keys.size()
        for (k in keys) {
            if (deleted >= maxDeletes) {
                truncated = true
                break
            }
            conn.del([k] as byte[][])
            deleted++
        }
    } finally {
        conn.close()
    }
    def summary = [pattern: pattern, matched: matched, deleted: deleted, truncated: truncated]
    flowFile = session.putAttribute(flowFile, 'redis.clear.deleted', deleted.toString())
    flowFile = session.putAttribute(flowFile, 'redis.clear.matched', matched.toString())
    log.warn('GLOBAL_REDIS_CLEAR ' + JsonOutput.toJson(summary))
    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    flowFile = session.putAttribute(flowFile, 'redis.clear.error', e.message ?: e.toString())
    log.error('global redis clear failed: ' + e.message, e)
    session.transfer(flowFile, REL_FAILURE)
}
'''


def clear_all(pattern="*", max_deletes=10000000):
    trigger_name = "global__admin__clear_all_redis_trigger"
    clear_name = "global__admin__clear_all_redis_run"
    log_name = "global__admin__clear_all_redis_log"

    delete_connections_involving([trigger_name, clear_name, log_name])
    procs = processors_by_name()
    for name in (trigger_name, clear_name, log_name):
        if name in procs:
            delete_processor(procs[name]["id"])

    # A "0 sec" period means "run continuously as fast as possible" for a TIMER_DRIVEN
    # GenerateFlowFile -- that floods the queue with thousands of flowfiles in seconds. A long
    # period fires exactly once immediately on start, then not again until well after we stop it.
    trigger = create_processor(
        trigger_name, "org.apache.nifi.processors.standard.GenerateFlowFile",
        200, 3400,
        {"Custom Text": "clear-all-redis", "Batch Size": "1", "Unique FlowFiles": "false"},
        [], scheduling_period="1 hour",
    )
    clear_proc = create_processor(
        clear_name, "org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
        520, 3400,
        {
            "Script Body": REDIS_CLEAR_SCRIPT,
            "REDIS_POOL_ID": REDIS_POOL_ID,
            "KEY_PATTERN": pattern,
            "MAX_DELETES": str(max_deletes),
        },
        [],
    )
    log_proc = create_processor(
        log_name, "org.apache.nifi.processors.standard.LogAttribute",
        840, 3400,
        {
            "Log Level": "warn",
            "Log Payload": "false",
            "Attributes to Log": "redis.clear.deleted,redis.clear.matched,redis.clear.error",
        },
        ["success"],
    )
    create_connection(trigger, trigger_name, clear_proc, clear_name, ["success"])
    create_connection(clear_proc, clear_name, log_proc, log_name, ["success", "failure"])

    set_processor_state(log_proc, "RUNNING")
    set_processor_state(clear_proc, "RUNNING")
    set_processor_state(trigger, "RUNNING")
    time.sleep(2)
    stop_processor(trigger)

    # Deleting many keys one-by-one can take a while -- poll until clear_proc's own input
    # queue (from the trigger) is empty rather than assuming a fixed sleep is long enough.
    conn = None
    for c in connections():
        comp = c["component"]
        if comp["source"].get("name") == trigger_name and comp["destination"].get("name") == clear_name:
            conn = c["id"]
            break
    deadline = time.time() + 120
    while conn and time.time() < deadline:
        ent = nifi("GET", f"/nifi-api/connections/{conn}")
        queued = ent.get("status", {}).get("aggregateSnapshot", {}).get("flowFilesQueued")
        if not queued or str(queued) == "0":
            break
        time.sleep(2)

    stop_processor(clear_proc)
    time.sleep(1)
    stop_processor(log_proc)

    ent = nifi("GET", f"/nifi-api/processors/{clear_proc}")
    bulletins = []
    for b in ent.get("bulletins") or []:
        bb = b.get("bulletin") or b
        if bb:
            msg = bb.get("message")
            if msg:
                bulletins.append({k: bb.get(k) for k in ["level", "message", "timestamp"]})

    delete_connections_involving([trigger_name, clear_name, log_name])
    delete_processor(trigger)
    delete_processor(clear_proc)
    delete_processor(log_proc)

    return {"bulletins": bulletins}


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "clear"
    if cmd == "clear":
        pattern = sys.argv[2] if len(sys.argv) > 2 else "*"
        print(json.dumps(clear_all(pattern), indent=2))
    else:
        raise SystemExit(f"unknown command: {cmd}")
