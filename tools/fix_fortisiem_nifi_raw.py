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

PG_ID = os.environ.get("FORTISIEM_PG_ID", "77227d7a-2d8a-323d-02b2-5f7aec5ea246")
CLIENT_ID = "codex-fortisiem-raw"

ORG_TOPIC = os.environ.get("FORTISIEM_ORG_TOPIC", "bronze.fortisiem.organization__raw")
DEVICE_TOPIC = os.environ.get("FORTISIEM_DEVICE_TOPIC", "bronze.fortisiem.device__raw")
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


def update_processor(proc_id, properties=None, auto_terms=None, scheduling_period=None, scheduling_strategy=None, run_duration_millis=None):
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
    if run_duration_millis is not None:
        new_cfg["runDurationMillis"] = run_duration_millis
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
        if comp["source"]["id"] == source_id and comp["destination"]["id"] == dest_id and sorted(comp["selectedRelationships"]) == relationships:
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
        if comp["source"]["id"] == source_id and comp["destination"]["id"] == dest_id and sorted(comp["selectedRelationships"]) == relationships:
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
        if dest.get("name") == "fortisiem.device__error_out":
            return dest
    raise RuntimeError("Could not locate fortisiem.device__error_out destination")


def drop_queue(conn_id):
    req = http("POST", f"/nifi-api/flowfile-queues/{conn_id}/drop-requests", {"revision": {"clientId": CLIENT_ID, "version": 0}})
    drop_id = req["dropRequest"]["id"]
    for _ in range(30):
        cur = http("GET", f"/nifi-api/flowfile-queues/{conn_id}/drop-requests/{drop_id}")
        if cur["dropRequest"].get("finished"):
            break
        time.sleep(1)
    http("DELETE", f"/nifi-api/flowfile-queues/{conn_id}/drop-requests/{drop_id}")


def delete_connection(conn_id):
    ent = http("GET", f"/nifi-api/connections/{conn_id}")
    queued = ent.get("status", {}).get("aggregateSnapshot", {}).get("flowFilesQueued")
    if queued and queued != "0":
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


def delete_connections_involving(processor_names):
    processor_names = set(processor_names)
    deleted = []
    for c in list(connections()):
        comp = c["component"]
        if comp["source"]["name"] in processor_names or comp["destination"]["name"] in processor_names:
            delete_connection(c["id"])
            deleted.append(
                {
                    "id": c["id"],
                    "source": comp["source"]["name"],
                    "dest": comp["destination"]["name"],
                    "relationships": comp.get("selectedRelationships"),
                }
            )
    return deleted


RAW_HASH_SCRIPT = r'''
import java.security.MessageDigest
import java.nio.charset.StandardCharsets
import java.time.Instant
import org.apache.nifi.distributed.cache.client.Serializer
import org.apache.nifi.processor.io.InputStreamCallback

class StringSerializer implements Serializer<String> {
    void serialize(String value, OutputStream out) throws IOException {
        out.write(value.getBytes(StandardCharsets.UTF_8))
    }
}

def flowFile = session.get()
if (!flowFile) return

def prop = { name ->
    context.getProperty(name).evaluateAttributeExpressions(flowFile).getValue()
}

def bytesOut = new ByteArrayOutputStream()
session.read(flowFile, { inputStream ->
    byte[] buffer = new byte[8192]
    int len
    while ((len = inputStream.read(buffer)) > -1) {
        bytesOut.write(buffer, 0, len)
    }
} as InputStreamCallback)

def bytes = bytesOut.toByteArray()
def hash = MessageDigest.getInstance('SHA-256').digest(bytes).encodeHex().toString()
def sourcePlatform = prop('SOURCE_PLATFORM') ?: 'fortisiem'
def entity = prop('SOURCE_OBJECT_TYPE')
def objectId = prop('OBJECT_ID')
if (!objectId || objectId.trim().length() == 0 || objectId.contains('${')) {
    objectId = flowFile.getAttribute('uuid')
}
objectId = objectId.replaceAll('[^A-Za-z0-9_.:-]', '_')

def customerOrg = prop('CUSTOMER_TENANT_ORGANIZATION')
if (!customerOrg || customerOrg.contains('${')) {
    customerOrg = ''
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

flowFile = session.putAttribute(flowFile, 'source_platform', sourcePlatform)
flowFile = session.putAttribute(flowFile, 'customer_tenant_organization', customerOrg)
flowFile = session.putAttribute(flowFile, 'source_object_type', entity)
flowFile = session.putAttribute(flowFile, 'source_object_id', objectId)
flowFile = session.putAttribute(flowFile, 'extraction_timestamp', Instant.now().toString())
flowFile = session.putAttribute(flowFile, 'source_event_update_timestamp', sourceUpdateTs)
flowFile = session.putAttribute(flowFile, 'api_endpoint_export_query_identity', apiQuery)
flowFile = session.putAttribute(flowFile, 'cursor_window', cursorWindow)
flowFile = session.putAttribute(flowFile, 'payload_hash_fingerprint', hash)
flowFile = session.putAttribute(flowFile, 'ingestion_run_batch_identity', runId)

def dedupeKey = "${sourcePlatform}:${entity}:${objectId}:${hash}"
flowFile = session.putAttribute(flowFile, 'dedupe.key', dedupeKey)

def cacheId = prop('DMC_SERVICE_ID')
if (cacheId && cacheId.trim().length() > 0) {
    def cache = context.controllerServiceLookup.getControllerService(cacheId)
    if (cache == null) {
        flowFile = session.putAttribute(flowFile, 'dedupe.error', "DistributedMapCache service not found: ${cacheId}")
        session.transfer(flowFile, REL_FAILURE)
        return
    }
    def ser = new StringSerializer()
    def inserted = cache.putIfAbsent(flowFile.getAttribute('dedupe.key'), '1', ser, ser)
    if (!inserted) {
        session.remove(flowFile)
        return
    }
}
session.transfer(flowFile, REL_SUCCESS)
'''


REDIS_CLEAR_SCRIPT = r'''
import groovy.json.JsonOutput
import org.apache.nifi.processor.io.OutputStreamCallback

def flowFile = session.get()
if (!flowFile) return

def prop = { name ->
    context.getProperty(name).evaluateAttributeExpressions(flowFile).getValue()
}

def poolId = prop('REDIS_POOL_ID')
def pattern = prop('KEY_PATTERN') ?: 'fortisiem:*'
def maxDeletes = (prop('MAX_DELETES') ?: '10000') as int
def pool = context.controllerServiceLookup.getControllerService(poolId)
if (pool == null) {
    flowFile = session.putAttribute(flowFile, 'redis.clear.error', "Redis pool not found: ${poolId}")
    session.transfer(flowFile, REL_FAILURE)
    return
}

def deleted = 0
def matched = 0
def conn = null
try {
    conn = pool.getConnection()
    def nativeConn = conn.getNativeConnection()
    def keys = nativeConn.keys(pattern)
    matched = keys == null ? 0 : keys.size()
    if (keys != null) {
        keys.take(maxDeletes).each { k ->
            nativeConn.del(k)
            deleted++
        }
    }
    def result = [
        pattern: pattern,
        matched: matched,
        deleted: deleted,
        truncated: matched > maxDeletes
    ]
    log.warn('FORTISIEM_REDIS_CLEAR ' + JsonOutput.toJson(result))
    flowFile = session.write(flowFile, { os ->
        os.write(JsonOutput.toJson(result).getBytes('UTF-8'))
    } as OutputStreamCallback)
    flowFile = session.putAttribute(flowFile, 'redis.clear.deleted', deleted.toString())
    flowFile = session.putAttribute(flowFile, 'redis.clear.matched', matched.toString())
    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    log.error('Redis clear failed: ' + e.message, e)
    flowFile = session.putAttribute(flowFile, 'redis.clear.error', e.message)
    session.transfer(flowFile, REL_FAILURE)
} finally {
    try { conn?.close() } catch (ignored) {}
}
'''


def matching_connection(source_name, dest_name=None, rel=None):
    found = []
    for c in connections():
        comp = c["component"]
        if comp["source"]["name"] != source_name:
            continue
        if dest_name and comp["destination"]["name"] != dest_name:
            continue
        if rel and rel not in comp["selectedRelationships"]:
            continue
        found.append(c)
    return found


def apply():
    procs = processors_by_name()

    # Keep all FortiSIEM fetch/publish processors stopped while rewiring.
    for name, p in list(procs.items()):
        if name.startswith("fortisiem."):
            stop_processor(p["id"])

    procs = processors_by_name()

    trigger = procs["fortisiem.device__trigger"]
    update_processor(
        trigger["id"],
        {"Batch Size": "1", "File Size": "0B", "ingestion_run_batch_identity": "${uuid}"},
        scheduling_period="2 hours",
        scheduling_strategy="TIMER_DRIVEN",
    )

    list_org_fetch = procs["fortisiem.device__list_organizations__fetch"]
    org_extract = procs["fortisiem.device__list_organizations__extract"]
    device_extract_key = procs["fortisiem.device__enrich__extract_key"]
    old_device_detect = procs["fortisiem.device__dedupe__detect"]
    old_org_detect = procs["fortisiem.organization__dedupe__detect"]
    dedupe_cache = (
        old_org_detect["component"]["config"].get("properties", {}).get("Distributed Cache Service")
        or old_device_detect["component"]["config"].get("properties", {}).get("Distributed Cache Service")
    )

    org_hash = create_processor(
        "fortisiem.organization__raw__dedupe_hash",
        "org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
        1700,
        0,
        {
            "Script Body": RAW_HASH_SCRIPT,
            "SOURCE_PLATFORM": "fortisiem",
            "SOURCE_OBJECT_TYPE": "organization",
            "OBJECT_ID": "${org_name}",
            "CUSTOMER_TENANT_ORGANIZATION": "${org_name}",
            "SOURCE_EVENT_UPDATE_TIMESTAMP": "${literal('')}",
            "API_ENDPOINT_EXPORT_QUERY_IDENTITY": "GET /phoenix/rest/organization/list",
            "CURSOR_WINDOW": "${literal('')}",
            "DMC_SERVICE_ID": dedupe_cache,
        },
        [],
    )
    device_hash = create_processor(
        "fortisiem.device__raw__dedupe_hash",
        "org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
        2400,
        320,
        {
            "Script Body": RAW_HASH_SCRIPT,
            "SOURCE_PLATFORM": "fortisiem",
            "SOURCE_OBJECT_TYPE": "device",
            "OBJECT_ID": "${naturalId}_${organization__attr_id}_${accessip}",
            "CUSTOMER_TENANT_ORGANIZATION": "${org_name}",
            "SOURCE_EVENT_UPDATE_TIMESTAMP": "${literal('')}",
            "API_ENDPOINT_EXPORT_QUERY_IDENTITY": "GET /phoenix/rest/device/list",
            "CURSOR_WINDOW": "${literal('')}",
            "DMC_SERVICE_ID": dedupe_cache,
        },
        [],
    )
    run_metadata = create_processor(
        "fortisiem.device__run_metadata",
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        700,
        -260,
        {"ingestion_run_batch_identity": "${now():toNumber()}_${uuid}"},
        [],
    )
    org_detect = create_processor(
        "fortisiem.organization__raw__dedupe_detect",
        "org.apache.nifi.processors.standard.DetectDuplicate",
        2050,
        0,
        {
            "Cache Entry Identifier": "${dedupe.key}",
            "Cache The Entry Identifier": "true",
            "Age Off Duration": "24 hours",
            "Distributed Cache Service": dedupe_cache,
        },
        ["duplicate", "failure"],
    )
    device_detect = create_processor(
        "fortisiem.device__raw__dedupe_detect",
        "org.apache.nifi.processors.standard.DetectDuplicate",
        2750,
        320,
        {
            "Cache Entry Identifier": "${dedupe.key}",
            "Cache The Entry Identifier": "true",
            "Age Off Duration": "24 hours",
            "Distributed Cache Service": dedupe_cache,
        },
        ["duplicate", "failure"],
    )

    procs = processors_by_name()
    org_publish = procs["fortisiem.organization__raw__publish"]
    device_raw_publish = procs["fortisiem.device__raw__publish"]
    device_load_publish = procs.get("fortisiem.device__load__publish")
    error_out = existing_error_destination()

    update_processor(
        org_publish["id"],
        {
            "Topic Name": ORG_TOPIC,
            "Kafka Key": "${source_object_id}",
            "FlowFile Attribute Header Pattern": STANDARD_HEADER_PATTERN,
            "Publish Strategy": "USE_VALUE",
            "Record Reader": None,
            "Record Writer": None,
        },
        ["success"],
    )
    update_processor(
        device_raw_publish["id"],
        {
            "Topic Name": DEVICE_TOPIC,
            "Kafka Key": "${source_object_id}",
            "FlowFile Attribute Header Pattern": STANDARD_HEADER_PATTERN,
            "Publish Strategy": "USE_VALUE",
            "Record Reader": None,
            "Record Writer": None,
        },
        ["success"],
    )
    if device_load_publish:
        set_processor_state(device_load_publish["id"], "DISABLED")

    # Remove schema-registry dependent branch links; keep processors in place but orphaned.
    for c in matching_connection("fortisiem.device__list_organizations__extract", "fortisiem.organization__enrich__set_key", "matched"):
        delete_connection(c["id"])
    for c in matching_connection("fortisiem.device__enrich__extract_key", "asset__enrich__set_key", "matched"):
        delete_connection(c["id"])
    for c in matching_connection("asset__enrich__set_key", "fortisiem.device__dedupe__hash", "success"):
        delete_connection(c["id"])
    for c in matching_connection("fortisiem.device__dedupe__hash", "fortisiem.device__dedupe__detect", "success"):
        delete_connection(c["id"])
    for c in matching_connection("fortisiem.device__dedupe__detect", "fortisiem.device__load__publish", "non-duplicate"):
        delete_connection(c["id"])
    for c in matching_connection("fortisiem.organization__raw__dedupe_hash", "fortisiem.organization__dedupe__detect", "success"):
        delete_connection(c["id"])
    for c in matching_connection("fortisiem.organization__raw__dedupe_hash", "fortisiem.organization__raw__dedupe_detect", "success"):
        delete_connection(c["id"])
    for c in matching_connection("fortisiem.organization__dedupe__detect", "fortisiem.organization__raw__publish", "non-duplicate"):
        delete_connection(c["id"])
    for c in matching_connection("fortisiem.device__raw__dedupe_hash", "fortisiem.device__dedupe__detect", "success"):
        delete_connection(c["id"])
    for c in matching_connection("fortisiem.device__raw__dedupe_hash", "fortisiem.device__raw__dedupe_detect", "success"):
        delete_connection(c["id"])
    for c in matching_connection("fortisiem.device__dedupe__detect", "fortisiem.device__raw__publish", "non-duplicate"):
        delete_connection(c["id"])
    for c in matching_connection("fortisiem.device__trigger", "fortisiem.device__list_organizations__fetch", "success"):
        delete_connection(c["id"])
    for c in matching_connection("fortisiem.organization__raw__dedupe_detect", "fortisiem.device__error_out", "failure"):
        delete_connection(c["id"])
    for c in matching_connection("fortisiem.device__raw__dedupe_detect", "fortisiem.device__error_out", "failure"):
        delete_connection(c["id"])

    create_connection(trigger["id"], trigger["component"]["name"], run_metadata, "fortisiem.device__run_metadata", ["success"])
    create_connection(run_metadata, "fortisiem.device__run_metadata", list_org_fetch["id"], list_org_fetch["component"]["name"], ["success"])
    create_connection(org_extract["id"], org_extract["component"]["name"], org_hash, "fortisiem.organization__raw__dedupe_hash", ["matched"])
    create_connection(org_hash, "fortisiem.organization__raw__dedupe_hash", org_publish["id"], org_publish["component"]["name"], ["success"])
    create_connection(device_extract_key["id"], device_extract_key["component"]["name"], device_hash, "fortisiem.device__raw__dedupe_hash", ["matched"])
    create_connection(device_hash, "fortisiem.device__raw__dedupe_hash", device_raw_publish["id"], device_raw_publish["component"]["name"], ["success"])
    create_connection_to_destination(org_hash, "fortisiem.organization__raw__dedupe_hash", error_out, ["failure"])
    create_connection_to_destination(device_hash, "fortisiem.device__raw__dedupe_hash", error_out, ["failure"])

    for old_name in [
        "fortisiem.organization__enrich__set_key",
        "fortisiem.organization__dedupe__hash",
        "fortisiem.organization__dedupe__detect",
        "fortisiem.organization__raw__dedupe_detect",
        "asset__enrich__set_key",
        "fortisiem.device__dedupe__hash",
        "fortisiem.device__dedupe__detect",
        "fortisiem.device__raw__dedupe_detect",
    ]:
        old = processors_by_name().get(old_name)
        if old:
            set_processor_state(old["id"], "DISABLED")

    return inspect()


def inspect():
    fl = flow()
    interesting = []
    for p in fl.get("processors", []):
        n = p["component"]["name"]
        if n.startswith("fortisiem.organization") or n.startswith("fortisiem.device__") or n.startswith("asset__"):
            interesting.append(
                {
                    "name": n,
                    "id": p["id"],
                    "state": p["component"].get("state"),
                    "validation": p["component"].get("validationStatus"),
                    "type": p["component"]["type"].split(".")[-1],
                }
            )
    links = []
    for c in fl.get("connections", []):
        comp = c["component"]
        sn = comp["source"]["name"]
        dn = comp["destination"]["name"]
        if "fortisiem.organization" in sn + dn or "fortisiem.device" in sn + dn or sn.startswith("asset__") or dn.startswith("asset__"):
            links.append(
                {
                    "source": sn,
                    "dest": dn,
                    "relationships": comp.get("selectedRelationships"),
                    "queued": c.get("status", {}).get("aggregateSnapshot", {}).get("flowFilesQueued"),
                }
            )
    return {"processors": interesting, "connections": links}


def status():
    data = http("GET", f"/nifi-api/flow/process-groups/{PG_ID}/status?recursive=true")
    root = data["processGroupStatus"]["aggregateSnapshot"]
    procs = []
    for p in root.get("processorStatusSnapshots", []):
        snap = p["processorStatusSnapshot"]
        if snap["name"].startswith("fortisiem."):
            procs.append(
                {
                    "name": snap["name"],
                    "runStatus": snap.get("runStatus"),
                    "flowFilesIn": snap.get("flowFilesIn"),
                    "flowFilesOut": snap.get("flowFilesOut"),
                    "tasks": snap.get("tasks"),
                    "bytesWritten": snap.get("bytesWritten"),
                }
            )
    conns = []
    for c in root.get("connectionStatusSnapshots", []):
        snap = c["connectionStatusSnapshot"]
        if "fortisiem." in (snap.get("sourceName", "") + snap.get("destinationName", "")):
            conns.append(
                {
                    "source": snap.get("sourceName"),
                    "dest": snap.get("destinationName"),
                    "queued": snap.get("flowFilesQueued"),
                    "flowFilesIn": snap.get("flowFilesIn"),
                    "flowFilesOut": snap.get("flowFilesOut"),
                }
            )
    return {
        "queued": root.get("flowFilesQueued"),
        "bytesQueued": root.get("bytesQueued"),
        "processors": procs,
        "connections": conns,
    }


def drop_all_queued(only_destinations=None):
    only_destinations = set(only_destinations or [])
    dropped = []
    for c in connections():
        queued = c.get("status", {}).get("aggregateSnapshot", {}).get("flowFilesQueued")
        if queued and queued != 0 and queued != "0":
            if only_destinations and c["component"]["destination"]["name"] not in only_destinations:
                continue
            drop_queue(c["id"])
            comp = c["component"]
            dropped.append({"source": comp["source"]["name"], "dest": comp["destination"]["name"], "queued": queued})
    return dropped


def org_probe():
    procs = processors_by_name()
    # Ensure no accidental device expansion.
    stop_processor(procs["fortisiem.device__list_devices__fetch"]["id"])
    selected = [
        "fortisiem.organization__raw__publish",
        "fortisiem.organization__raw__dedupe_hash",
        "fortisiem.device__list_organizations__extract",
        "fortisiem.device__list_organizations__split",
        "fortisiem.device__list_organizations__fetch",
    ]
    for name in selected:
        start_processor(procs[name]["id"])
    start_processor(procs["fortisiem.device__trigger"]["id"])
    time.sleep(1)
    stop_processor(procs["fortisiem.device__trigger"]["id"])
    # Let the single generated request drain through org publish.
    time.sleep(20)
    last_status = None
    for _ in range(75):
        st = status()
        last_status = st
        blocking_queues = [
            c for c in st["connections"]
            if c["queued"] not in (0, "0", None)
            and c["dest"] not in ("fortisiem.device__list_devices__fetch",)
        ]
        # Queue to stopped list_devices_fetch is expected and is dropped after org verification.
        if not blocking_queues:
            break
        time.sleep(1)
    time.sleep(5)
    before_cleanup = status()
    for name in selected:
        stop_processor(procs[name]["id"])
    dropped = drop_all_queued(["fortisiem.device__list_devices__fetch"])
    remaining = [
        c for c in connections()
        if c.get("status", {}).get("aggregateSnapshot", {}).get("flowFilesQueued") not in (0, "0", None)
    ]
    return {"status_before_cleanup": before_cleanup, "status": status(), "dropped_queues": dropped, "remaining_queues": [
        {
            "source": c["component"]["source"]["name"],
            "dest": c["component"]["destination"]["name"],
            "queued": c.get("status", {}).get("aggregateSnapshot", {}).get("flowFilesQueued"),
        }
        for c in remaining
    ]}


def rewire_org_before_devices():
    procs = processors_by_name()
    for name, p in list(procs.items()):
        if name.startswith("fortisiem."):
            stop_processor(p["id"])
    procs = processors_by_name()
    trigger = procs["fortisiem.device__trigger"]
    update_processor(
        trigger["id"],
        {"Batch Size": "1", "File Size": "0B"},
        scheduling_period="2 hours",
        scheduling_strategy="TIMER_DRIVEN",
    )
    org_extract = procs["fortisiem.device__list_organizations__extract"]
    org_hash = procs["fortisiem.organization__raw__dedupe_hash"]
    org_publish = procs["fortisiem.organization__raw__publish"]
    list_devices = procs["fortisiem.device__list_devices__fetch"]
    # Remove the pre-dedupe path that let duplicate organizations still fetch devices.
    removed = []
    for c in matching_connection("fortisiem.device__list_organizations__extract", "fortisiem.device__list_devices__fetch", "matched"):
        delete_connection(c["id"])
        removed.append(c["id"])
    # Keep org publish after dedupe, and add device fetch after the same dedupe gate.
    create_connection(org_extract["id"], org_extract["component"]["name"], org_hash["id"], org_hash["component"]["name"], ["matched"])
    create_connection(org_hash["id"], org_hash["component"]["name"], org_publish["id"], org_publish["component"]["name"], ["success"])
    create_connection(org_hash["id"], org_hash["component"]["name"], list_devices["id"], list_devices["component"]["name"], ["success"])
    return {"removed_pre_dedupe_device_connections": removed, "inspect": inspect()}


STALE_PROCESSORS = [
    "fortisiem.organization__enrich__set_key",
    "fortisiem.organization__dedupe__hash",
    "fortisiem.organization__dedupe__detect",
    "fortisiem.organization__raw__dedupe_detect",
    "asset__enrich__set_key",
    "fortisiem.device__dedupe__hash",
    "fortisiem.device__dedupe__detect",
    "fortisiem.device__raw__dedupe_detect",
    "fortisiem.device__load__publish",
]


def cleanup_stale_processors():
    for p in processors_by_name().values():
        if p["component"]["name"].startswith("fortisiem."):
            stop_processor(p["id"])
    deleted_connections = delete_connections_involving(STALE_PROCESSORS)
    deleted_processors = []
    procs = processors_by_name()
    for name in STALE_PROCESSORS:
        p = procs.get(name)
        if not p:
            continue
        delete_processor(p["id"])
        deleted_processors.append({"name": name, "id": p["id"]})
    return {
        "deleted_connections": deleted_connections,
        "deleted_processors": deleted_processors,
        "inspect": inspect(),
    }


def clear_fortisiem_redis():
    procs = processors_by_name()
    for p in procs.values():
        if p["component"]["name"].startswith("fortisiem."):
            stop_processor(p["id"])
    trigger = create_processor(
        "fortisiem.__admin__clear_redis__trigger",
        "org.apache.nifi.processors.standard.GenerateFlowFile",
        -800,
        -500,
        {
            "File Size": "0B",
            "Batch Size": "1",
            "Data Format": "Text",
            "Unique FlowFiles": "false",
            "Custom Text": "clear-fortisiem-redis",
        },
        [],
        "2 hours",
    )
    clear_proc = create_processor(
        "fortisiem.__admin__clear_redis__run",
        "org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
        -400,
        -500,
        {
            "Script Body": REDIS_CLEAR_SCRIPT,
            "REDIS_POOL_ID": "b90bcbdb-d69c-3725-51d1-444dd57b9336",
            "KEY_PATTERN": "fortisiem:*",
            "MAX_DELETES": "10000",
        },
        ["success", "failure"],
        "0 sec",
    )
    create_connection(trigger, "fortisiem.__admin__clear_redis__trigger", clear_proc, "fortisiem.__admin__clear_redis__run", ["success"])
    start_processor(clear_proc)
    start_processor(trigger)
    time.sleep(3)
    stop_processor(trigger)
    time.sleep(2)
    stop_processor(clear_proc)
    # Capture recent bulletins from the clear processor before deleting it.
    clear_entity = http("GET", f"/nifi-api/processors/{clear_proc}")
    bulletins = []
    for b in clear_entity.get("bulletins") or []:
        bb = b.get("bulletin") or b
        if bb:
            bulletins.append({k: bb.get(k) for k in ["level", "message", "timestamp"]})
    delete_connections_involving(["fortisiem.__admin__clear_redis__trigger", "fortisiem.__admin__clear_redis__run"])
    delete_processor(trigger)
    delete_processor(clear_proc)
    return {"bulletins": bulletins, "inspect": inspect()}


def apply_standard_raw_metadata():
    procs = processors_by_name()
    changed = {}
    if "fortisiem.device__trigger" in procs:
        stop_processor(procs["fortisiem.device__trigger"]["id"])
        update_processor(
            procs["fortisiem.device__trigger"]["id"],
            {"ingestion_run_batch_identity": "${uuid}", "Batch Size": "1", "File Size": "0B"},
            scheduling_period="2 hours",
            scheduling_strategy="TIMER_DRIVEN",
        )
    for name in ("fortisiem.organization__raw__dedupe_hash", "fortisiem.device__raw__dedupe_hash"):
        if name in procs:
            stop_processor(procs[name]["id"])
            entity = "organization" if ".organization__" in name else "device"
            object_id = "${org_name}" if entity == "organization" else "${naturalId}_${organization__attr_id}_${accessip}"
            customer_org = "${org_name}"
            api_query = "GET /phoenix/rest/organization/list" if entity == "organization" else "GET /phoenix/rest/device/list"
            update_processor(
                procs[name]["id"],
                {
                    "Script Body": RAW_HASH_SCRIPT,
                    "SOURCE_PLATFORM": "fortisiem",
                    "SOURCE_OBJECT_TYPE": entity,
                    "OBJECT_ID": object_id,
                    "CUSTOMER_TENANT_ORGANIZATION": customer_org,
                    "SOURCE_EVENT_UPDATE_TIMESTAMP": "${literal('')}",
                    "API_ENDPOINT_EXPORT_QUERY_IDENTITY": api_query,
                    "CURSOR_WINDOW": "${literal('')}",
                },
            )
            changed[name] = {
                "metadata_attributes": [
                    "source_platform",
                    "customer_tenant_organization",
                    "source_object_type",
                    "source_object_id",
                    "extraction_timestamp",
                    "source_event_update_timestamp",
                    "api_endpoint_export_query_identity",
                    "cursor_window",
                    "payload_hash_fingerprint",
                    "ingestion_run_batch_identity",
                ],
                "source_object_id": object_id,
            }

    for name in ("fortisiem.organization__raw__publish", "fortisiem.device__raw__publish"):
        if name in procs:
            stop_processor(procs[name]["id"])
            update_processor(
                procs[name]["id"],
                {
                    "FlowFile Attribute Header Pattern": STANDARD_HEADER_PATTERN,
                    "Kafka Key": "${source_object_id}",
                },
            )
            changed[name] = {
                "kafka_key": "${source_object_id}",
                "header_pattern": STANDARD_HEADER_PATTERN,
            }
    return {"changed": changed, "inspect": inspect()}


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "inspect"
    if cmd == "apply":
        print(json.dumps(apply(), indent=2))
    elif cmd == "inspect":
        print(json.dumps(inspect(), indent=2))
    elif cmd == "status":
        print(json.dumps(status(), indent=2))
    elif cmd == "org-probe":
        print(json.dumps(org_probe(), indent=2))
    elif cmd == "rewire-org-before-devices":
        print(json.dumps(rewire_org_before_devices(), indent=2))
    elif cmd == "cleanup-stale":
        print(json.dumps(cleanup_stale_processors(), indent=2))
    elif cmd == "clear-fortisiem-redis":
        print(json.dumps(clear_fortisiem_redis(), indent=2))
    elif cmd == "apply-standard-raw-metadata":
        print(json.dumps(apply_standard_raw_metadata(), indent=2))
    else:
        raise SystemExit(f"unknown command: {cmd}")
