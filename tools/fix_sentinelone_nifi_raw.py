import json
import os
import subprocess
import time
import urllib.parse


NIFI_BASE = os.environ.get("NIFI_BASE", "https://nifi.datapasc.com").rstrip("/")
NIFI_USER = os.environ.get("NIFI_USER")
NIFI_PASSWORD = os.environ.get("NIFI_PASSWORD")
NIFI_TOKEN = os.environ.get("NIFI_TOKEN")

PG_ID = os.environ.get("SENTINELONE_PG_ID", "1c122170-d74a-3b60-2776-28717d5cf049")
CLIENT_ID = "codex-sentinelone-raw"

KAFKA_SERVICE_ID = os.environ.get("KAFKA_SERVICE_ID", "40675f79-8eaa-3193-8f8d-026c8c1ee947")
DEDUP_CACHE_SERVICE_ID = os.environ.get("SENTINELONE_DEDUP_CACHE_SERVICE_ID", "9b44f93e-227c-331a-e9f6-0e6af3155c1a")

AGENT_TOPIC = os.environ.get("SENTINELONE_AGENT_TOPIC", "bronze.sentinelone.agent__raw")
SITE_TOPIC = os.environ.get("SENTINELONE_SITE_TOPIC", "bronze.sentinelone.site__raw")
SENTINELONE_AUTHORIZATION = os.environ.get("SENTINELONE_AUTHORIZATION")

STANDARD_HEADER_PATTERN = (
    r"^(source_platform|customer_tenant_organization|source_object_type|"
    r"source_object_id|extraction_timestamp|source_event_update_timestamp|"
    r"api_endpoint_export_query_identity|cursor_window|payload_hash_fingerprint|"
    r"ingestion_run_batch_identity)$"
)


def run_curl(args, input_text=None, timeout=60):
    cmd = ["curl.exe", "--http1.1", "-k", "-sS"] + args
    proc = subprocess.run(cmd, input=input_text, text=True, capture_output=True, timeout=timeout)
    if proc.returncode != 0:
        raise RuntimeError(f"curl exit {proc.returncode}: {proc.stderr[:1000]} {proc.stdout[:1000]}")
    return proc.stdout


def login():
    if NIFI_TOKEN:
        return NIFI_TOKEN
    if not NIFI_USER or not NIFI_PASSWORD:
        raise RuntimeError("Set NIFI_TOKEN or NIFI_USER/NIFI_PASSWORD")
    body = urllib.parse.urlencode({"username": NIFI_USER, "password": NIFI_PASSWORD})
    return run_curl(
        [
            "-H",
            "Content-Type: application/x-www-form-urlencoded",
            "--data-binary",
            "@-",
            f"{NIFI_BASE}/nifi-api/access/token",
        ],
        body,
    ).strip()


TOKEN = None


def http(method, path, body=None, timeout=60):
    global TOKEN
    if TOKEN is None:
        TOKEN = login()
    args = ["-X", method, "-H", f"Authorization: Bearer {TOKEN}", "-w", "\nHTTP_STATUS:%{http_code}"]
    if body is not None:
        args += ["-H", "Content-Type: application/json", "--data-binary", "@-"]
        input_text = json.dumps(body)
    else:
        input_text = None
    args.append(f"{NIFI_BASE}{path}")
    out = run_curl(args, input_text, timeout)
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
    return http("GET", f"/nifi-api/flow/process-groups/{PG_ID}")["processGroupFlow"]["flow"]


def processors_by_name():
    return {p["component"]["name"]: p for p in flow().get("processors", [])}


def connections():
    return flow().get("connections", [])


def stop_processor(proc_id):
    ent = http("GET", f"/nifi-api/processors/{proc_id}")
    if ent["component"].get("state") in ("STOPPED", "DISABLED"):
        return
    payload = {"revision": {"clientId": CLIENT_ID, "version": ent["revision"]["version"]}, "state": "STOPPED"}
    http("PUT", f"/nifi-api/processors/{proc_id}/run-status", payload)


def set_processor_state(proc_id, state):
    ent = http("GET", f"/nifi-api/processors/{proc_id}")
    if ent["component"].get("state") == state:
        return
    payload = {"revision": {"clientId": CLIENT_ID, "version": ent["revision"]["version"]}, "state": state}
    http("PUT", f"/nifi-api/processors/{proc_id}/run-status", payload)


def start_processor(proc_id):
    set_processor_state(proc_id, "RUNNING")


def set_output_port_state(port_id, state):
    ent = http("GET", f"/nifi-api/output-ports/{port_id}")
    if ent["component"].get("state") == state:
        return
    payload = {"revision": {"clientId": CLIENT_ID, "version": ent["revision"]["version"]}, "state": state}
    http("PUT", f"/nifi-api/output-ports/{port_id}/run-status", payload)


def update_processor(proc_id, properties=None, auto_terms=None, scheduling_period=None, scheduling_strategy=None):
    ent = http("GET", f"/nifi-api/processors/{proc_id}")
    comp = ent["component"]
    cfg = comp.get("config", {})
    props = dict(cfg.get("properties") or {})
    if properties:
        props.update(properties)
    new_cfg = {"properties": props}
    if auto_terms is not None:
        new_cfg["autoTerminatedRelationships"] = auto_terms
    if scheduling_period is not None:
        new_cfg["schedulingPeriod"] = scheduling_period
    if scheduling_strategy is not None:
        new_cfg["schedulingStrategy"] = scheduling_strategy
    payload = {
        "revision": {"clientId": CLIENT_ID, "version": ent["revision"]["version"]},
        "component": {"id": proc_id, "name": comp["name"], "config": new_cfg},
    }
    return http("PUT", f"/nifi-api/processors/{proc_id}", payload)


def create_processor(name, proc_type, x, y, properties=None, auto_terms=None, scheduling_period="0 sec"):
    existing = processors_by_name().get(name)
    if existing:
        update_processor(existing["id"], properties or {}, auto_terms, scheduling_period)
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
    return http("POST", f"/nifi-api/process-groups/{PG_ID}/processors", payload)["id"]


def create_connection(source_id, source_name, dest_id, dest_name, relationships):
    relationships = sorted(relationships)
    for c in connections():
        comp = c["component"]
        if (
            comp["source"]["id"] == source_id
            and comp["destination"]["id"] == dest_id
            and sorted(comp["selectedRelationships"]) == relationships
        ):
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
    return http("POST", f"/nifi-api/process-groups/{PG_ID}/connections", payload)["id"]


def create_connection_to_destination(source_id, source_name, destination, relationships):
    relationships = sorted(relationships)
    dest_id = destination["id"]
    for c in connections():
        comp = c["component"]
        if (
            comp["source"]["id"] == source_id
            and comp["destination"]["id"] == dest_id
            and sorted(comp["selectedRelationships"]) == relationships
        ):
            return c["id"]
    payload = {
        "revision": {"clientId": CLIENT_ID, "version": 0},
        "component": {
            "parentGroupId": PG_ID,
            "source": {"id": source_id, "type": "PROCESSOR", "groupId": PG_ID, "name": source_name},
            "destination": {
                "id": dest_id,
                "type": destination["type"],
                "groupId": destination.get("groupId", PG_ID),
                "name": destination["name"],
            },
            "selectedRelationships": relationships,
            "flowFileExpiration": "0 sec",
            "backPressureObjectThreshold": 10000,
            "backPressureDataSizeThreshold": "1 GB",
        },
    }
    return http("POST", f"/nifi-api/process-groups/{PG_ID}/connections", payload)["id"]


def existing_error_destination():
    for c in connections():
        dest = c["component"]["destination"]
        if dest.get("name") == "sentinelone.agent__error_out":
            return dest
    raise RuntimeError("Could not locate sentinelone.agent__error_out destination")


RAW_JSON_HASH_SCRIPT = r'''
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import java.security.MessageDigest
import java.time.Instant
import org.apache.nifi.processor.io.InputStreamCallback

def flowFile = session.get()
if (!flowFile) return

try {
    def prop = { name ->
        context.getProperty(name).evaluateAttributeExpressions(flowFile).getValue()
    }

    def textHolder = [value: '']
    session.read(flowFile, { inputStream ->
        textHolder.value = inputStream.getText('UTF-8')
    } as InputStreamCallback)

    def parsed = new JsonSlurper().parseText(textHolder.value)
    def rec = (parsed instanceof List) ? parsed[0] : parsed
    if (!(rec instanceof Map)) {
        throw new IllegalArgumentException('Expected JSON object after split')
    }

    def sourcePlatform = prop('SOURCE_PLATFORM') ?: 'sentinelone'
    def entity = prop('SOURCE_OBJECT_TYPE')
    def objectId = prop('OBJECT_ID')
    if (!objectId || objectId.trim().length() == 0 || objectId.contains('${')) {
        objectId = flowFile.getAttribute('uuid')
    }
    objectId = objectId.replaceAll('[^A-Za-z0-9_.:-]', '_')

    def canonicalJson = JsonOutput.toJson(rec)
    def hash = MessageDigest.getInstance('SHA-256').digest(canonicalJson.getBytes('UTF-8')).encodeHex().toString()
    def dedupeKey = "${sourcePlatform}:${entity}:${objectId}:${hash}"

    def customerOrg = prop('CUSTOMER_TENANT_ORGANIZATION')
    if (!customerOrg || customerOrg.contains('${')) {
        customerOrg = rec.get('accountName') ?: rec.get('siteName') ?: flowFile.getAttribute('s1_site_name') ?: ''
    }
    def sourceUpdateTs = prop('SOURCE_EVENT_UPDATE_TIMESTAMP')
    if (!sourceUpdateTs || sourceUpdateTs.contains('${')) {
        sourceUpdateTs = ''
    }
    def apiQuery = prop('API_ENDPOINT_EXPORT_QUERY_IDENTITY') ?: ''
    def cursorWindow = prop('CURSOR_WINDOW') ?: ''
    def runId = flowFile.getAttribute('ingestion_run_batch_identity')
    if (!runId || runId.trim().length() == 0) {
        runId = flowFile.getAttribute('uuid')
    }

    def attrPairs = [
        'source_platform': sourcePlatform,
        'customer_tenant_organization': customerOrg,
        'source_object_type': entity,
        'source_object_id': objectId,
        'extraction_timestamp': Instant.now().toString(),
        'source_event_update_timestamp': sourceUpdateTs,
        'api_endpoint_export_query_identity': apiQuery,
        'cursor_window': cursorWindow,
        'payload_hash_fingerprint': hash,
        'ingestion_run_batch_identity': runId,
        'dedupe.key': dedupeKey
    ]
    attrPairs.each { k, v -> flowFile = session.putAttribute(flowFile, k, v.toString()) }

    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    flowFile = session.putAttribute(flowFile, 'dedupe.error', e.message ?: e.toString())
    log.error('sentinelone raw hash/enrich failed: ' + e.message, e)
    session.transfer(flowFile, REL_FAILURE)
}
'''


CONNECTIVITY_SCRIPT = r'''
import groovy.json.JsonOutput
import org.apache.nifi.processor.io.OutputStreamCallback

def flowFile = session.get()
if (!flowFile) return

def prop = { name -> context.getProperty(name).evaluateAttributeExpressions(flowFile).getValue() }
def status = -1
def bytes = 0
try {
    def url = new URL(prop('API_URL'))
    def conn = (HttpURLConnection) url.openConnection()
    conn.setRequestMethod('GET')
    conn.setConnectTimeout(15000)
    conn.setReadTimeout(15000)
    conn.setRequestProperty('Authorization', prop('AUTHORIZATION'))
    conn.setRequestProperty('Accept', 'application/json')
    status = conn.getResponseCode()
    def stream = status >= 400 ? conn.getErrorStream() : conn.getInputStream()
    if (stream != null) {
        byte[] buffer = new byte[8192]
        int len
        while ((len = stream.read(buffer)) > -1) {
            bytes += len
            if (bytes > 65536) break
        }
        stream.close()
    }
    def result = [
        reachable: true,
        authenticated: status >= 200 && status < 300,
        status: status,
        bytes: bytes,
        endpoint: '/web/api/v2.1/agents?limit=1'
    ]
    flowFile = session.write(flowFile, { out ->
        out.write(JsonOutput.toJson(result).getBytes('UTF-8'))
    } as OutputStreamCallback)
    log.warn('SENTINELONE_NIFI_CONNECTIVITY ' + JsonOutput.toJson(result))
    session.transfer(flowFile, status >= 200 && status < 300 ? REL_SUCCESS : REL_FAILURE)
} catch (Exception e) {
    def result = [
        reachable: false,
        authenticated: false,
        status: status,
        error: e.message,
        endpoint: '/web/api/v2.1/agents?limit=1'
    ]
    flowFile = session.write(flowFile, { out ->
        out.write(JsonOutput.toJson(result).getBytes('UTF-8'))
    } as OutputStreamCallback)
    log.warn('SENTINELONE_NIFI_CONNECTIVITY ' + JsonOutput.toJson(result))
    session.transfer(flowFile, REL_FAILURE)
}
'''


REDIS_CLEAR_SCRIPT = r'''
import groovy.json.JsonOutput

def flowFile = session.get()
if (!flowFile) return

def prop = { name -> context.getProperty(name).evaluateAttributeExpressions(flowFile).getValue() }

def poolId = prop('REDIS_POOL_ID')
def pattern = prop('KEY_PATTERN') ?: 'sentinelone:*'
def maxDeletes = (prop('MAX_DELETES') ?: '10000') as int
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
    log.warn('SENTINELONE_REDIS_CLEAR ' + JsonOutput.toJson(summary))
    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    flowFile = session.putAttribute(flowFile, 'redis.clear.error', e.message ?: e.toString())
    log.error('sentinelone redis clear failed: ' + e.message, e)
    session.transfer(flowFile, REL_FAILURE)
}
'''


def drop_queue(conn_id):
    req = http("POST", f"/nifi-api/flowfile-queues/{conn_id}/drop-requests", {"revision": {"clientId": CLIENT_ID, "version": 0}})
    drop_id = req["dropRequest"]["id"]
    for _ in range(30):
        cur = http("GET", f"/nifi-api/flowfile-queues/{conn_id}/drop-requests/{drop_id}")
        if cur["dropRequest"].get("finished"):
            break
        time.sleep(1)
    http("DELETE", f"/nifi-api/flowfile-queues/{conn_id}/drop-requests/{drop_id}")


def delete_connection(conn_id, allow_drop=False):
    ent = http("GET", f"/nifi-api/connections/{conn_id}")
    queued = ent.get("status", {}).get("aggregateSnapshot", {}).get("flowFilesQueued")
    if queued and queued != "0":
        if not allow_drop:
            # This script is for structural changes. Do not silently drop queued user data.
            raise RuntimeError(f"Connection {conn_id} has queued FlowFiles; refusing to delete")
        drop_queue(conn_id)
        ent = http("GET", f"/nifi-api/connections/{conn_id}")
    version = ent["revision"]["version"]
    http("DELETE", f"/nifi-api/connections/{conn_id}?version={version}&clientId={urllib.parse.quote(CLIENT_ID)}")


def delete_processor(proc_id):
    ent = http("GET", f"/nifi-api/processors/{proc_id}")
    if ent["component"].get("state") == "RUNNING":
        stop_processor(proc_id)
    version = ent["revision"]["version"]
    http("DELETE", f"/nifi-api/processors/{proc_id}?version={version}&clientId={urllib.parse.quote(CLIENT_ID)}")


def delete_connections_involving(processor_names, allow_drop=False):
    deleted = []
    names = set(processor_names)
    for c in list(connections()):
        comp = c["component"]
        if comp["source"]["name"] in names or comp["destination"]["name"] in names:
            delete_connection(c["id"], allow_drop=allow_drop)
            deleted.append(
                {
                    "id": c["id"],
                    "source": comp["source"]["name"],
                    "dest": comp["destination"]["name"],
                    "relationships": comp.get("selectedRelationships"),
                }
            )
    return deleted


def matching_connection(source_name, dest_name=None, rel=None):
    found = []
    for c in connections():
        comp = c["component"]
        if comp["source"]["name"] != source_name:
            continue
        if dest_name is not None and comp["destination"]["name"] != dest_name:
            continue
        if rel is not None and rel not in comp.get("selectedRelationships", []):
            continue
        found.append(c)
    return found


def disable_stale_raw_publish_if_present():
    procs = processors_by_name()
    p = procs.get("sentinelone.agent__raw__publish")
    if not p:
        return None
    stop_processor(p["id"])
    ent = http("GET", f"/nifi-api/processors/{p['id']}")
    if ent["component"].get("state") != "DISABLED":
        payload = {"revision": {"clientId": CLIENT_ID, "version": ent["revision"]["version"]}, "state": "DISABLED"}
        http("PUT", f"/nifi-api/processors/{p['id']}/run-status", payload)
    return p["id"]


def cleanup_stale():
    stale = ["sentinelone.agent__raw__publish", "sentinelone.agent__enrich__set_metadata"]
    deleted_connections = delete_connections_involving(stale)
    deleted_processors = []
    procs = processors_by_name()
    for name in stale:
        p = procs.get(name)
        if p:
            delete_processor(p["id"])
            deleted_processors.append({"name": name, "id": p["id"]})
    return {"deleted_connections": deleted_connections, "deleted_processors": deleted_processors, "inspect": inspect()}


def bypass_schema_update_record():
    procs = processors_by_name()
    for p in procs.values():
        if p["component"]["name"].startswith("sentinelone."):
            stop_processor(p["id"])

    # Remove schema-registry-dependent UpdateRecord from the active path.
    deleted_connections = delete_connections_involving(["sentinelone.agent__enrich__set_metadata"])
    procs = processors_by_name()
    extract_key = procs["sentinelone.agent__enrich__extract_key"]
    dedupe_hash = procs["sentinelone.agent__dedupe__hash"]
    create_connection(
        extract_key["id"],
        extract_key["component"]["name"],
        dedupe_hash["id"],
        dedupe_hash["component"]["name"],
        ["matched"],
    )
    procs = processors_by_name()
    deleted_processors = []
    stale = procs.get("sentinelone.agent__enrich__set_metadata")
    if stale:
        delete_processor(stale["id"])
        deleted_processors.append({"name": stale["component"]["name"], "id": stale["id"]})
    return {
        "deleted_connections": deleted_connections,
        "created_connection": "sentinelone.agent__enrich__extract_key -> sentinelone.agent__dedupe__hash",
        "deleted_processors": deleted_processors,
        "inspect": inspect(),
    }


def connectivity_probe():
    if not SENTINELONE_AUTHORIZATION:
        raise RuntimeError("Set SENTINELONE_AUTHORIZATION for the NiFi-side probe")
    trigger_name = "sentinelone.__admin__connectivity_trigger"
    check_name = "sentinelone.__admin__connectivity_check"
    delete_connections_involving([trigger_name, check_name], allow_drop=True)
    procs = processors_by_name()
    for name in (trigger_name, check_name):
        if name in procs:
            delete_processor(procs[name]["id"])

    trigger = create_processor(
        trigger_name,
        "org.apache.nifi.processors.standard.GenerateFlowFile",
        300,
        1050,
        {"Custom Text": "sentinelone-connectivity-probe", "Batch Size": "1", "Unique FlowFiles": "false"},
        [],
        scheduling_period="1 hour",
    )
    check = create_processor(
        check_name,
        "org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
        620,
        1050,
        {
            "Script Body": CONNECTIVITY_SCRIPT,
            "API_URL": "https://euce1-120-mssp.sentinelone.net/web/api/v2.1/agents?limit=1",
            "AUTHORIZATION": SENTINELONE_AUTHORIZATION,
        },
        ["success", "failure"],
    )
    create_connection(trigger, trigger_name, check, check_name, ["success"])
    start_processor(check)
    start_processor(trigger)
    time.sleep(3)
    stop_processor(trigger)
    time.sleep(2)
    stop_processor(check)
    ent = http("GET", f"/nifi-api/processors/{check}")
    bulletins = []
    for b in ent.get("bulletins") or []:
        bb = b.get("bulletin") or b
        if bb:
            msg = bb.get("message")
            if msg and "SENTINELONE_NIFI_CONNECTIVITY" in msg:
                bulletins.append({k: bb.get(k) for k in ["level", "message", "timestamp"]})
    delete_connections_involving([trigger_name, check_name], allow_drop=True)
    delete_processor(trigger)
    delete_processor(check)
    return {"bulletins": bulletins, "inspect": inspect()}


def clear_sentinelone_redis():
    trigger_name = "sentinelone.__admin__clear_redis_trigger"
    clear_name = "sentinelone.__admin__clear_redis_run"
    log_name = "sentinelone.__admin__clear_redis_log"
    procs = processors_by_name()
    for name in (trigger_name, clear_name, log_name):
        if name in procs:
            stop_processor(procs[name]["id"])
    delete_connections_involving([trigger_name, clear_name, log_name], allow_drop=True)
    procs = processors_by_name()
    for name in (trigger_name, clear_name, log_name):
        if name in procs:
            delete_processor(procs[name]["id"])

    trigger = create_processor(
        trigger_name,
        "org.apache.nifi.processors.standard.GenerateFlowFile",
        300,
        1160,
        {"Custom Text": "clear-sentinelone-redis", "Batch Size": "1", "Unique FlowFiles": "false"},
        [],
        scheduling_period="0 sec",
    )
    clear_proc = create_processor(
        clear_name,
        "org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
        620,
        1160,
        {
            "Script Body": REDIS_CLEAR_SCRIPT,
            "REDIS_POOL_ID": "b90bcbdb-d69c-3725-51d1-444dd57b9336",
            "KEY_PATTERN": "sentinelone:*",
            "MAX_DELETES": "10000",
        },
        [],
    )
    log_proc = create_processor(
        log_name,
        "org.apache.nifi.processors.standard.LogAttribute",
        940,
        1160,
        {
            "Log Level": "warn",
            "Log Payload": "false",
            "Attributes to Log": "redis.clear.deleted,redis.clear.matched,redis.clear.error",
        },
        ["success"],
    )
    create_connection(trigger, trigger_name, clear_proc, clear_name, ["success"])
    create_connection(clear_proc, clear_name, log_proc, log_name, ["success", "failure"])
    start_processor(log_proc)
    start_processor(clear_proc)
    start_processor(trigger)
    time.sleep(3)
    stop_processor(trigger)
    time.sleep(2)
    stop_processor(clear_proc)
    time.sleep(1)
    stop_processor(log_proc)
    ent = http("GET", f"/nifi-api/processors/{log_proc}")
    bulletins = []
    for b in ent.get("bulletins") or []:
        bb = b.get("bulletin") or b
        if bb:
            msg = bb.get("message")
            if msg:
                bulletins.append({k: bb.get(k) for k in ["level", "message", "timestamp"]})
    delete_connections_involving([trigger_name, clear_name, log_name], allow_drop=True)
    delete_processor(trigger)
    delete_processor(clear_proc)
    delete_processor(log_proc)
    return {"bulletins": bulletins, "inspect": inspect()}


def inspect():
    out = {"processors": [], "connections": []}
    for p in flow().get("processors", []):
        comp = p["component"]
        out["processors"].append(
            {
                "name": comp["name"],
                "id": comp["id"],
                "state": comp.get("state"),
                "validation": comp.get("validationStatus"),
                "type": comp["type"].split(".")[-1],
            }
        )
    for c in connections():
        comp = c["component"]
        out["connections"].append(
            {
                "source": comp["source"]["name"],
                "dest": comp["destination"]["name"],
                "relationships": comp.get("selectedRelationships"),
                "queued": c.get("status", {}).get("aggregateSnapshot", {}).get("flowFilesQueued"),
            }
        )
    return out


def status_summary():
    snap = inspect()
    return {
        "active_invalid": [
            p for p in snap["processors"] if p["state"] != "DISABLED" and p["validation"] != "VALID"
        ],
        "running": [p["name"] for p in snap["processors"] if p["state"] == "RUNNING"],
        "queued": [c for c in snap["connections"] if str(c.get("queued")) not in ("0", "None", "")],
    }


def run_once(max_wait_seconds=180):
    error_dest = existing_error_destination()
    if error_dest["type"] == "OUTPUT_PORT":
        set_output_port_state(error_dest["id"], "RUNNING")

    procs = processors_by_name()
    trigger = procs["sentinelone.agent__trigger"]
    for p in procs.values():
        name = p["component"]["name"]
        if name.startswith("sentinelone.") and name != "sentinelone.agent__trigger":
            start_processor(p["id"])
    start_processor(trigger["id"])
    time.sleep(5)
    stop_processor(trigger["id"])

    deadline = time.time() + max_wait_seconds
    while time.time() < deadline:
        current = status_summary()
        non_error_queues = [
            c for c in current["queued"] if c["dest"] != "sentinelone.agent__error_out"
        ]
        if not non_error_queues:
            break
        time.sleep(5)

    procs = processors_by_name()
    for p in procs.values():
        if p["component"]["name"].startswith("sentinelone."):
            stop_processor(p["id"])
    if error_dest["type"] == "OUTPUT_PORT":
        set_output_port_state(error_dest["id"], "STOPPED")

    return {"status": status_summary(), "inspect": inspect()}


def apply():
    procs = processors_by_name()
    error_out = existing_error_destination()

    for p in procs.values():
        if p["component"]["name"].startswith("sentinelone."):
            stop_processor(p["id"])

    # Anti-spam: keep the flow stopped and make the trigger slow.
    update_processor(
        procs["sentinelone.agent__trigger"]["id"],
        {"ingestion_run_batch_identity": "${uuid}"},
        scheduling_period="2 hours",
        scheduling_strategy="TIMER_DRIVEN",
    )

    # Preserve existing/rooted site extraction attributes and add site publish branch.
    if SENTINELONE_AUTHORIZATION:
        update_processor(procs["sentinelone.agent__list_sites__fetch"]["id"], {"Authorization": SENTINELONE_AUTHORIZATION})
        update_processor(procs["sentinelone.agent__list_agents__fetch"]["id"], {"Authorization": SENTINELONE_AUTHORIZATION})

    run_metadata = create_processor(
        "sentinelone.agent__run_metadata",
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        520,
        420,
        {"ingestion_run_batch_identity": "${now():toNumber()}_${uuid}"},
        [],
    )

    update_processor(
        procs["sentinelone.agent__list_sites__extract"]["id"],
        {
            "s1_site_id": "$.id",
            "s1_site_name": "$.name",
        },
    )

    # Extend existing agent key extraction using keys already present in the dummy flow plus useful stable IDs.
    update_processor(
        procs["sentinelone.agent__enrich__extract_key"]["id"],
        {
            "agent_id": "$.uuid",
            "s1_agent_id": "$.id",
            "account_id": "$.accountId",
            "account_name": "$.accountName",
            "site_id": "$.siteId",
            "site_name": "$.siteName",
            "group_id": "$.groupId",
            "group_name": "$.groupName",
            "computer_name": "$.computerName",
            "external_id": "$.externalId",
        },
    )

    site_hash = create_processor(
        "sentinelone.site__raw__dedupe_hash",
        "org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
        1480,
        700,
        {
            "Script Body": RAW_JSON_HASH_SCRIPT,
            "SOURCE_PLATFORM": "sentinelone",
            "SOURCE_OBJECT_TYPE": "site",
            "OBJECT_ID": "${s1_site_id}",
            "CUSTOMER_TENANT_ORGANIZATION": "${literal('')}",
            "SOURCE_EVENT_UPDATE_TIMESTAMP": "${literal('')}",
            "API_ENDPOINT_EXPORT_QUERY_IDENTITY": "GET /web/api/v2.1/sites?limit={limit}&cursor={cursor}",
            "CURSOR_WINDOW": "${literal('')}",
        },
        [],
    )
    site_detect = create_processor(
        "sentinelone.site__raw__dedupe_detect",
        "org.apache.nifi.processors.standard.DetectDuplicate",
        1780,
        700,
        {
            "Cache Entry Identifier": "${dedupe.key}",
            "Age Off Duration": "24 hours",
            "Distributed Cache Service": DEDUP_CACHE_SERVICE_ID,
            "Cache The Entry Identifier": "true",
        },
        ["duplicate"],
    )
    site_publish = create_processor(
        "sentinelone.site__raw__publish",
        "org.apache.nifi.kafka.processors.PublishKafka",
        2080,
        700,
        {
            "Kafka Connection Service": KAFKA_SERVICE_ID,
            "Topic Name": SITE_TOPIC,
            "Failure Strategy": "Route to Failure",
            "acks": "all",
            "compression.type": "none",
            "max.request.size": "500 MB",
            "Transactions Enabled": "false",
            "Publish Strategy": "USE_VALUE",
            "Record Reader": None,
            "Record Writer": None,
            "Message Key Field": None,
            "Kafka Key": "${source_object_id}",
            "Kafka Key Attribute Encoding": "utf-8",
            "FlowFile Attribute Header Pattern": STANDARD_HEADER_PATTERN,
            "Header Encoding": "UTF-8",
            "Record Metadata Strategy": "FROM_PROPERTIES",
        },
        ["success"],
    )

    # Replace agent metadata/dedupe hash with standard raw JSON enrichment/hash.
    update_processor(
        procs["sentinelone.agent__dedupe__hash"]["id"],
        {
            "Script Body": RAW_JSON_HASH_SCRIPT,
            "SOURCE_PLATFORM": "sentinelone",
            "SOURCE_OBJECT_TYPE": "agent",
            "OBJECT_ID": "${agent_id}",
            "CUSTOMER_TENANT_ORGANIZATION": "${literal('')}",
            "SOURCE_EVENT_UPDATE_TIMESTAMP": "${literal('')}",
            "API_ENDPOINT_EXPORT_QUERY_IDENTITY": "GET /web/api/v2.1/agents?siteIds={site_id}&limit={limit}&cursor={cursor}",
            "CURSOR_WINDOW": "${literal('')}",
        },
        [],
    )
    update_processor(
        procs["sentinelone.agent__dedupe__detect"]["id"],
        {
            "Cache Entry Identifier": "${dedupe.key}",
            "Age Off Duration": "24 hours",
            "Distributed Cache Service": DEDUP_CACHE_SERVICE_ID,
            "Cache The Entry Identifier": "true",
        },
        ["duplicate"],
    )
    update_processor(
        procs["sentinelone.agent__load__publish"]["id"],
        {
            "Kafka Connection Service": KAFKA_SERVICE_ID,
            "Topic Name": AGENT_TOPIC,
            "Failure Strategy": "Route to Failure",
            "acks": "all",
            "compression.type": "none",
            "max.request.size": "500 MB",
            "Transactions Enabled": "false",
            "Publish Strategy": "USE_VALUE",
            "Record Reader": None,
            "Record Writer": None,
            "Message Key Field": None,
            "Kafka Key": "${source_object_id}",
            "Kafka Key Attribute Encoding": "utf-8",
            "FlowFile Attribute Header Pattern": STANDARD_HEADER_PATTERN,
            "Header Encoding": "UTF-8",
            "Record Metadata Strategy": "FROM_PROPERTIES",
        },
        ["success"],
    )

    # Keep agent extraction independent from site dedupe; add site publish as a side branch.
    for c in matching_connection("sentinelone.agent__trigger", "sentinelone.agent__list_sites__init_cursor", "success"):
        delete_connection(c["id"])
    create_connection(
        procs["sentinelone.agent__trigger"]["id"],
        "sentinelone.agent__trigger",
        run_metadata,
        "sentinelone.agent__run_metadata",
        ["success"],
    )
    create_connection(
        run_metadata,
        "sentinelone.agent__run_metadata",
        procs["sentinelone.agent__list_sites__init_cursor"]["id"],
        "sentinelone.agent__list_sites__init_cursor",
        ["success"],
    )

    site_extract = procs["sentinelone.agent__list_sites__extract"]
    create_connection(site_extract["id"], site_extract["component"]["name"], site_hash, "sentinelone.site__raw__dedupe_hash", ["matched"])
    create_connection(site_hash, "sentinelone.site__raw__dedupe_hash", site_detect, "sentinelone.site__raw__dedupe_detect", ["success"])
    create_connection_to_destination(site_hash, "sentinelone.site__raw__dedupe_hash", error_out, ["failure"])
    create_connection(site_detect, "sentinelone.site__raw__dedupe_detect", site_publish, "sentinelone.site__raw__publish", ["non-duplicate"])
    create_connection_to_destination(site_detect, "sentinelone.site__raw__dedupe_detect", error_out, ["failure"])
    create_connection_to_destination(
        procs["sentinelone.agent__dedupe__hash"]["id"],
        "sentinelone.agent__dedupe__hash",
        error_out,
        ["failure"],
    )
    create_connection_to_destination(site_publish, "sentinelone.site__raw__publish", error_out, ["failure"])

    disabled_stale = disable_stale_raw_publish_if_present()
    return {"disabled_stale_raw_publish": disabled_stale, "inspect": inspect()}


if __name__ == "__main__":
    cmd = os.environ.get("CMD") or (os.sys.argv[1] if len(os.sys.argv) > 1 else "inspect")
    if cmd == "apply":
        print(json.dumps(apply(), indent=2))
    elif cmd == "inspect":
        print(json.dumps(inspect(), indent=2))
    elif cmd == "cleanup-stale":
        print(json.dumps(cleanup_stale(), indent=2))
    elif cmd == "connectivity-probe":
        print(json.dumps(connectivity_probe(), indent=2))
    elif cmd == "bypass-schema-update-record":
        print(json.dumps(bypass_schema_update_record(), indent=2))
    elif cmd == "run-once":
        print(json.dumps(run_once(), indent=2))
    elif cmd == "clear-sentinelone-redis":
        print(json.dumps(clear_sentinelone_redis(), indent=2))
    else:
        raise SystemExit(f"unknown command: {cmd}")
