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

PG_ID = os.environ.get("SENTINELONE_MAX_PG_ID", "14ab82fd-01a0-1000-47d6-db7896347cfc")
CLIENT_ID = "codex-sentinelone-object-id-patch"

APICURIO_CCOMPAT = os.environ.get("APICURIO_CCOMPAT", "https://apicurio.datapasc.com/apis/ccompat/v7").rstrip("/")

ENTITIES = [
    "activity", "activity_type", "agent", "agent_package", "agent_tag", "alert",
    "application_cve", "cloud_detection_rule", "config_override", "exclusion", "group",
    "group_policy", "installed_application", "ioc", "location", "restriction", "role",
    "service_user", "site", "site_policy", "system_info", "tenant_policy", "threat",
    "threat_note", "threat_timeline", "user", "xdr_asset", "xdr_asset_tag",
]
NO_AVRO_ENTITIES = {"ioc", "threat_note"}
AVRO_ENTITIES = [e for e in ENTITIES if e not in NO_AVRO_ENTITIES]


# ---------------------------------------------------------------------------
# NiFi REST helpers (mirrors tools/fix_sentinelone_nifi_raw.py conventions)
# ---------------------------------------------------------------------------

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


def update_processor(proc_id, properties=None, auto_terms=None, scheduling_period=None, scheduling_strategy=None):
    ent = nifi("GET", f"/nifi-api/processors/{proc_id}")
    comp = ent["component"]
    cfg = comp.get("config", {})
    # NiFi masks sensitive properties as "********" on read; writing that back destroys the
    # real secret/param-ref while the processor still looks VALID. Drop masked entries unless
    # this call explicitly supplies a new value for that key.
    props = {k: v for k, v in (cfg.get("properties") or {}).items() if v != "********"}
    if properties:
        props.update(properties)
    new_cfg = {"properties": props}
    if auto_terms is not None:
        new_cfg["autoTerminatedRelationships"] = auto_terms
    if scheduling_period is not None:
        new_cfg["schedulingPeriod"] = scheduling_period
    if scheduling_strategy is not None:
        new_cfg["schedulingStrategy"] = scheduling_strategy
    payload = {"revision": {"clientId": CLIENT_ID, "version": ent["revision"]["version"]}, "component": {"id": proc_id, "name": comp["name"], "config": new_cfg}}
    return nifi("PUT", f"/nifi-api/processors/{proc_id}", payload)


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
    return nifi("POST", f"/nifi-api/process-groups/{PG_ID}/processors", payload)["id"]


def find_connection(source_id, dest_id, rel=None):
    for c in connections():
        comp = c["component"]
        if comp["source"]["id"] != source_id or comp["destination"]["id"] != dest_id:
            continue
        if rel is not None and rel not in comp.get("selectedRelationships", []):
            continue
        return c
    return None


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


def delete_connection(conn_id, allow_drop=False):
    ent = nifi("GET", f"/nifi-api/connections/{conn_id}")
    queued = ent.get("status", {}).get("aggregateSnapshot", {}).get("flowFilesQueued")
    if queued and str(queued) != "0":
        if not allow_drop:
            raise RuntimeError(f"Connection {conn_id} has queued FlowFiles; refusing to delete")
    version = ent["revision"]["version"]
    nifi("DELETE", f"/nifi-api/connections/{conn_id}?version={version}&clientId={urllib.parse.quote(CLIENT_ID)}")


# ---------------------------------------------------------------------------
# Apicurio ccompat v7 helpers (no auth required, confirmed reachable directly)
# ---------------------------------------------------------------------------

def apicurio_get(path, timeout=30):
    out = run_curl(["-w", "\nHTTP_STATUS:%{http_code}", f"{APICURIO_CCOMPAT}{path}"], timeout=timeout)
    raw, status_txt = out.rsplit("\nHTTP_STATUS:", 1)
    status = int(status_txt.strip()[:3])
    if status < 200 or status > 299:
        raise RuntimeError(f"GET {path} HTTP {status}: {raw[:1000]}")
    return json.loads(raw.strip())


def apicurio_post(path, body, timeout=30):
    args = ["-X", "POST", "-H", "Content-Type: application/vnd.schemaregistry.v1+json", "--data-binary", "@-", "-w", "\nHTTP_STATUS:%{http_code}", f"{APICURIO_CCOMPAT}{path}"]
    out = run_curl(args, json.dumps(body), timeout=timeout)
    raw, status_txt = out.rsplit("\nHTTP_STATUS:", 1)
    status = int(status_txt.strip()[:3])
    if status < 200 or status > 299:
        raise RuntimeError(f"POST {path} HTTP {status}: {raw[:1000]}")
    return json.loads(raw.strip())


# ---------------------------------------------------------------------------
# Canonical script bodies
# ---------------------------------------------------------------------------

RAW_DEDUPE_HASH_SCRIPT = r'''
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import java.security.MessageDigest
import java.time.Instant
import org.apache.nifi.processor.io.InputStreamCallback

def flowFile = session.get()
if (!flowFile) return

try {
    def prop = { name ->
        def p = context.getProperty(name)
        return p == null ? null : p.evaluateAttributeExpressions(flowFile).getValue()
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

    // Content hash excludes ingestion-generated fields (they are attributes, not payload)
    // plus any explicitly volatile source fields named in EXCLUDE_FIELDS.
    def excludeRaw = prop('EXCLUDE_FIELDS') ?: ''
    def excludes = excludeRaw.split(',').collect { it.trim() }.findAll { it }
    def hashable = rec
    if (excludes) {
        hashable = new LinkedHashMap()
        rec.each { k, v -> if (!excludes.contains(k.toString())) hashable.put(k.toString(), v) }
    }
    def canonical
    canonical = { obj ->
        if (obj instanceof Map) {
            def out = new TreeMap()
            obj.each { k, v -> out[k.toString()] = canonical(v) }
            return out
        }
        if (obj instanceof List) return obj.collect { canonical(it) }
        return obj
    }
    def canonicalJson = JsonOutput.toJson(canonical(hashable))
    def hash = MessageDigest.getInstance('SHA-256').digest(canonicalJson.getBytes('UTF-8')).encodeHex().toString()

    def customerOrg = prop('CUSTOMER_TENANT_ORGANIZATION')
    if (!customerOrg || customerOrg.trim().length() == 0 || customerOrg.contains('${')) {
        customerOrg = rec.get('siteName') ?: rec.get('accountName') ?: flowFile.getAttribute('s1_site_name')
    }
    // No DEFAULT_CUSTOMER parameter fallback here on purpose: when neither the per-entity
    // expression nor the record itself carries a tenant, the field is genuinely unresolvable,
    // so it defaults straight to the literal 'NA' rather than a guessed account name.
    if (!customerOrg || customerOrg.toString().trim().length() == 0) {
        customerOrg = 'NA'
    }

    // raw.md section 5B: Source + Tenant + Object Type + Source Object ID + Content Hash.
    // Tenant must be resolved before the key is built.
    def tenantKey = (customerOrg ?: '').toString().trim() ?: '_'
    def dedupeKey = "${sourcePlatform}:${tenantKey}:${entity}:${objectId}:${hash}"

    def sourceUpdateTs = prop('SOURCE_EVENT_UPDATE_TIMESTAMP')
    if (!sourceUpdateTs || sourceUpdateTs.contains('${')) {
        sourceUpdateTs = rec.get('updatedAt') ?: ''
    }

    def runId = flowFile.getAttribute('ingestion_run_batch_identity')
    if (!runId || runId.trim().length() == 0) runId = flowFile.getAttribute('uuid')

    def cursorWindow = prop('CURSOR_WINDOW') ?: ''

    def attrPairs = [
        'source_platform': sourcePlatform,
        'customer_tenant_organization': customerOrg,
        'source_object_type': entity,
        'source_object_id': objectId,
        'source_event_update_timestamp': sourceUpdateTs,
        'api_endpoint_export_query_identity': prop('API_ENDPOINT_EXPORT_QUERY_IDENTITY') ?: '',
        'cursor_window': cursorWindow,
        'payload_hash_fingerprint': hash,
        'ingestion_run_batch_identity': runId,
        // Per-record stamp, epoch millis, matching the fileshare flow's /ingest_ts.
        // Set AFTER the hash above, so it can never enter the content fingerprint.
        'ingest_ts': System.currentTimeMillis().toString(),
        'dedupe.key': dedupeKey,
        // Same composite key as dedupe.key, surfaced under a stable public attribute name so it
        // can be sent downstream as a header/value field (dedupe.key stays internal-only).
        'object_id': dedupeKey,
        'kafka_topic': "bronze.sentinelone.${entity}__raw",
        'avro_topic': "bronze.sentinelone.${entity}__raw.avro",
        'avro_subject': "bronze.sentinelone.${entity}__raw.avro-value"
    ]
    attrPairs.each { k, v -> flowFile = session.putAttribute(flowFile, k, v == null ? '' : v.toString()) }

    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    flowFile = session.putAttribute(flowFile, 'dedupe.error', e.message ?: e.toString())
    log.error('sentinelone raw hash/enrich failed: ' + e.message, e)
    session.transfer(flowFile, REL_FAILURE)
}
'''

RAW_BAKE_METADATA_SCRIPT = r'''
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.io.OutputStreamCallback

def flowFile = session.get()
if (!flowFile) return

try {
    def textHolder = [value: '']
    session.read(flowFile, { inputStream -> textHolder.value = inputStream.getText('UTF-8') } as InputStreamCallback)
    def parsed = new JsonSlurper().parseText(textHolder.value)

    def out = new LinkedHashMap()
    [
        'source_platform',
        'customer_tenant_organization',
        'source_object_type',
        'source_object_id',
        'object_id',
        'source_event_update_timestamp',
        'api_endpoint_export_query_identity',
        'cursor_window',
        'payload_hash_fingerprint',
        'ingestion_run_batch_identity'
    ].each { k -> out.put(k, flowFile.getAttribute(k) ?: '') }

    // ingest_ts is epoch millis and must land as a JSON number, not a quoted string, to match
    // the ["null","long"] Avro schema field.
    def ingestTsAttr = flowFile.getAttribute('ingest_ts')
    out.put('ingest_ts', (ingestTsAttr && ingestTsAttr.trim().length() > 0) ? Long.parseLong(ingestTsAttr) : null)

    if (parsed instanceof Map) {
        parsed.each { k, v -> if (!out.containsKey(k.toString())) out.put(k.toString(), v) }
    } else {
        out.put('raw_payload', parsed)
    }

    flowFile = session.write(flowFile, { outputStream -> outputStream.write(JsonOutput.toJson(out).getBytes('UTF-8')) } as OutputStreamCallback)
    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    flowFile = session.putAttribute(flowFile, 'raw.bake.error', e.message ?: e.toString())
    log.error('sentinelone raw metadata bake failed: ' + e.message, e)
    session.transfer(flowFile, REL_FAILURE)
}
'''

AVRO_NORMALIZE_JSON_SCRIPT = r'''
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.io.OutputStreamCallback
def flowFile = session.get()
if (!flowFile) return
def maxDepth = 5
def safeName = { String name ->
    def n = (name ?: '').replaceAll(/[^A-Za-z0-9_]/, '_')
    if (!n || !(n[0] ==~ /[A-Za-z_]/)) n = 'f_' + n
    return n
}
def normalize
normalize = { value, int depth ->
    if (value == null || value instanceof String || value instanceof Number || value instanceof Boolean) return value
    if (depth >= maxDepth) return JsonOutput.toJson(value)
    if (value instanceof List) return value.collect { normalize(it, depth + 1) }
    if (value instanceof Map) {
        def out = new LinkedHashMap()
        def used = new LinkedHashSet()
        value.each { k, v ->
            def fk = safeName(k.toString())
            def base = fk
            def idx = 2
            while (used.contains(fk)) { fk = base + '_' + idx; idx++ }
            used.add(fk)
            out.put(fk, normalize(v, depth + 1))
        }
        return out
    }
    return value.toString()
}
def withStandardMetadata = { normalized ->
    def out = new LinkedHashMap()
    [
        'source_platform',
        'customer_tenant_organization',
        'source_object_type',
        'source_object_id',
        'object_id',
        'source_event_update_timestamp',
        'api_endpoint_export_query_identity',
        'cursor_window',
        'payload_hash_fingerprint',
        'ingestion_run_batch_identity'
    ].each { k ->
        out.put(k, flowFile.getAttribute(k) ?: '')
    }
    // ingest_ts is epoch millis and must land as a JSON number, not a quoted string, to match
    // the ["null","long"] Avro schema field.
    def ingestTsAttr = flowFile.getAttribute('ingest_ts')
    out.put('ingest_ts', (ingestTsAttr && ingestTsAttr.trim().length() > 0) ? Long.parseLong(ingestTsAttr) : null)
    if (normalized instanceof Map) {
        normalized.each { k, v ->
            if (!out.containsKey(k.toString())) out.put(k.toString(), v)
        }
    } else {
        out.put('raw_payload', normalized == null ? null : normalized.toString())
    }
    return out
}
try {
    def textHolder = [value: '']
    session.read(flowFile, { inputStream -> textHolder.value = inputStream.getText('UTF-8') } as InputStreamCallback)
    def parsed = new JsonSlurper().parseText(textHolder.value)
    def normalized = withStandardMetadata(normalize(parsed, 0))
    flowFile = session.write(flowFile, { outputStream -> outputStream.write(JsonOutput.toJson(normalized).getBytes('UTF-8')) } as OutputStreamCallback)
    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    flowFile = session.putAttribute(flowFile, 'avro.normalize.error', e.message ?: e.toString())
    log.error('JSON Avro normalization failed: ' + e.message, e)
    session.transfer(flowFile, REL_FAILURE)
}
'''

HEADER_PATTERN_FIELDS = [
    "source_platform", "customer_tenant_organization", "source_object_type",
    "source_object_id", "object_id", "source_event_update_timestamp",
    "api_endpoint_export_query_identity", "cursor_window", "payload_hash_fingerprint",
    "ingestion_run_batch_identity", "ingest_ts",
]
HEADER_PATTERN = "^(" + "|".join(HEADER_PATTERN_FIELDS) + ")$"


# ---------------------------------------------------------------------------
# Per-entity apply
# ---------------------------------------------------------------------------

def apply_entity(entity):
    procs = processors_by_name()
    prefix = f"sentinelone.{entity}"

    dedupe_hash = procs[f"{prefix}__raw__dedupe_hash"]
    dedupe_detect = procs[f"{prefix}__raw__dedupe_detect"]
    raw_publish = procs[f"{prefix}__raw__publish"]

    # 1. object_id in dedupe_hash attrPairs.
    update_processor(dedupe_hash["id"], {"Script Body": RAW_DEDUPE_HASH_SCRIPT})

    # 2. New raw__bake_metadata processor between dedupe_detect and raw_publish.
    dd_pos = dedupe_detect.get("position", {"x": 0, "y": 0})
    rp_pos = raw_publish.get("position", {"x": dd_pos.get("x", 0) + 300, "y": dd_pos.get("y", 0)})
    mid_x = (dd_pos.get("x", 0) + rp_pos.get("x", dd_pos.get("x", 0) + 300)) / 2
    mid_y = dd_pos.get("y", 0)

    bake_id = create_processor(
        f"{prefix}__raw__bake_metadata",
        "org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
        mid_x,
        mid_y,
        {"Script Body": RAW_BAKE_METADATA_SCRIPT},
        ["failure"],
    )

    old_edge = find_connection(dedupe_detect["id"], raw_publish["id"], rel="non-duplicate")
    if old_edge:
        delete_connection(old_edge["id"])
    create_connection(dedupe_detect["id"], dedupe_detect["component"]["name"], bake_id, f"{prefix}__raw__bake_metadata", ["non-duplicate"])
    create_connection(bake_id, f"{prefix}__raw__bake_metadata", raw_publish["id"], raw_publish["component"]["name"], ["success"])

    # 3. object_id in raw__publish header pattern.
    update_processor(raw_publish["id"], {"FlowFile Attribute Header Pattern": HEADER_PATTERN})

    if entity in NO_AVRO_ENTITIES:
        return {"entity": entity, "avro": False}

    avro_normalize = procs[f"{prefix}__avro__normalize_json"]
    avro_publish = procs[f"{prefix}__avro__publish"]

    # 4. object_id + numeric ingest_ts in avro__normalize_json.
    update_processor(avro_normalize["id"], {"Script Body": AVRO_NORMALIZE_JSON_SCRIPT})

    # 5. object_id in avro__publish header pattern.
    update_processor(avro_publish["id"], {"FlowFile Attribute Header Pattern": HEADER_PATTERN})

    return {"entity": entity, "avro": True}


# ---------------------------------------------------------------------------
# Apicurio schema fix
# ---------------------------------------------------------------------------

def fix_schema(entity):
    subject = f"bronze.sentinelone.{entity}__raw.avro-value"
    latest = apicurio_get(f"/subjects/{urllib.parse.quote(subject, safe='')}/versions/latest")
    schema = json.loads(latest["schema"])
    fields = schema["fields"]
    names = [f["name"] for f in fields]
    changed = False

    for f in fields:
        if f["name"] == "ingest_ts" and f.get("type") != ["null", "long"]:
            f["type"] = ["null", "long"]
            changed = True

    if "object_id" not in names:
        insert_at = names.index("source_object_id") + 1 if "source_object_id" in names else 0
        fields.insert(insert_at, {"name": "object_id", "type": ["null", "string"], "default": None})
        changed = True

    if not changed:
        return {"subject": subject, "changed": False, "version": latest["version"]}

    result = apicurio_post(f"/subjects/{urllib.parse.quote(subject, safe='')}/versions", {"schema": json.dumps(schema)})
    return {"subject": subject, "changed": True, "new_id": result.get("id")}


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

def verify():
    snap = flow()
    procs = snap["processors"]
    invalid = [p["component"]["name"] for p in procs if p["component"]["name"].startswith("sentinelone.") and p["component"].get("validationStatus") not in ("VALID", None)]
    not_stopped = [p["component"]["name"] for p in procs if p["component"]["name"].startswith("sentinelone.") and p.get("status", {}).get("aggregateSnapshot", {}).get("runStatus") not in ("Stopped", "Disabled")]

    by_name = {p["component"]["name"]: p["component"] for p in procs}
    script_mismatches = []
    header_mismatches = []
    for entity in ENTITIES:
        prefix = f"sentinelone.{entity}"
        dh = by_name.get(f"{prefix}__raw__dedupe_hash")
        if dh and dh["config"]["properties"].get("Script Body") != RAW_DEDUPE_HASH_SCRIPT:
            script_mismatches.append(f"{prefix}__raw__dedupe_hash")
        bm = by_name.get(f"{prefix}__raw__bake_metadata")
        if not bm:
            script_mismatches.append(f"{prefix}__raw__bake_metadata MISSING")
        elif bm["config"]["properties"].get("Script Body") != RAW_BAKE_METADATA_SCRIPT:
            script_mismatches.append(f"{prefix}__raw__bake_metadata")
        rp = by_name.get(f"{prefix}__raw__publish")
        if rp and rp["config"]["properties"].get("FlowFile Attribute Header Pattern") != HEADER_PATTERN:
            header_mismatches.append(f"{prefix}__raw__publish")
        if entity in NO_AVRO_ENTITIES:
            continue
        an = by_name.get(f"{prefix}__avro__normalize_json")
        if an and an["config"]["properties"].get("Script Body") != AVRO_NORMALIZE_JSON_SCRIPT:
            script_mismatches.append(f"{prefix}__avro__normalize_json")
        ap = by_name.get(f"{prefix}__avro__publish")
        if ap and ap["config"]["properties"].get("FlowFile Attribute Header Pattern") != HEADER_PATTERN:
            header_mismatches.append(f"{prefix}__avro__publish")

    schema_status = []
    for entity in AVRO_ENTITIES:
        subject = f"bronze.sentinelone.{entity}__raw.avro-value"
        latest = apicurio_get(f"/subjects/{urllib.parse.quote(subject, safe='')}/versions/latest")
        schema = json.loads(latest["schema"])
        by_field = {f["name"]: f for f in schema["fields"]}
        ok = by_field.get("ingest_ts", {}).get("type") == ["null", "long"] and "object_id" in by_field
        if not ok:
            schema_status.append({"subject": subject, "ingest_ts_type": by_field.get("ingest_ts", {}).get("type"), "has_object_id": "object_id" in by_field})

    return {
        "invalid_processors": invalid,
        "not_stopped": not_stopped,
        "script_mismatches": script_mismatches,
        "header_mismatches": header_mismatches,
        "bad_schemas": schema_status,
    }


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "help"
    if cmd == "apply-entity":
        print(json.dumps(apply_entity(sys.argv[2]), indent=2))
    elif cmd == "apply-all":
        results = []
        for e in ENTITIES:
            try:
                results.append(apply_entity(e))
            except Exception as exc:
                results.append({"entity": e, "error": str(exc)})
        print(json.dumps(results, indent=2))
    elif cmd == "fix-schema":
        print(json.dumps(fix_schema(sys.argv[2]), indent=2))
    elif cmd == "fix-schemas-all":
        results = []
        for e in AVRO_ENTITIES:
            try:
                results.append(fix_schema(e))
            except Exception as exc:
                results.append({"entity": e, "error": str(exc)})
        print(json.dumps(results, indent=2))
    elif cmd == "verify":
        print(json.dumps(verify(), indent=2))
    else:
        raise SystemExit(f"unknown command: {cmd}")
