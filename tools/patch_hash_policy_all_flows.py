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
CLIENT_ID = "codex-hash-policy-patch"

SENTINELONE_PG_ID = "14ab82fd-01a0-1000-47d6-db7896347cfc"
RAPID7_ASYAD_PG_ID = "14db305d-01a0-1000-11f0-c68b900bbdb5"
RAPID7_SECURADO_PG_ID = "1508dfff-01a0-1000-861c-4cbb8f1c946c"

SENTINELONE_ENTITIES = [
    "activity", "activity_type", "agent", "agent_package", "agent_tag", "alert",
    "application_cve", "cloud_detection_rule", "config_override", "exclusion", "group",
    "group_policy", "installed_application", "ioc", "location", "restriction", "role",
    "service_user", "site", "site_policy", "system_info", "tenant_policy", "threat",
    "threat_note", "threat_timeline", "user", "xdr_asset", "xdr_asset_tag",
]
RAPID7_ENTITIES = ["site", "asset", "asset_service", "asset_software", "asset_vulnerability"]


# ---------------------------------------------------------------------------
# NiFi REST helpers
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


def flow(pg_id):
    return nifi("GET", f"/nifi-api/flow/process-groups/{pg_id}")["processGroupFlow"]["flow"]


def processors_by_name(pg_id):
    return {p["component"]["name"]: p for p in flow(pg_id).get("processors", [])}


def connections(pg_id):
    return flow(pg_id).get("connections", [])


def update_processor(proc_id, properties=None):
    ent = nifi("GET", f"/nifi-api/processors/{proc_id}")
    comp = ent["component"]
    cfg = comp.get("config", {})
    props = {k: v for k, v in (cfg.get("properties") or {}).items() if v != "********"}
    if properties:
        props.update(properties)
    payload = {"revision": {"clientId": CLIENT_ID, "version": ent["revision"]["version"]}, "component": {"id": proc_id, "name": comp["name"], "config": {"properties": props}}}
    return nifi("PUT", f"/nifi-api/processors/{proc_id}", payload)


def create_processor(pg_id, name, proc_type, x, y, properties=None, auto_terms=None, scheduling_period="0 sec"):
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
    return nifi("POST", f"/nifi-api/process-groups/{pg_id}/processors", payload)["id"]


def create_connection(pg_id, source_id, source_name, dest_id, dest_name, relationships):
    relationships = sorted(relationships)
    payload = {
        "revision": {"clientId": CLIENT_ID, "version": 0},
        "component": {
            "parentGroupId": pg_id,
            "source": {"id": source_id, "type": "PROCESSOR", "groupId": pg_id, "name": source_name},
            "destination": {"id": dest_id, "type": "PROCESSOR", "groupId": pg_id, "name": dest_name},
            "selectedRelationships": relationships,
            "flowFileExpiration": "0 sec",
            "backPressureObjectThreshold": 10000,
            "backPressureDataSizeThreshold": "1 GB",
        },
    }
    return nifi("POST", f"/nifi-api/process-groups/{pg_id}/connections", payload)["id"]


def find_connection(pg_id, source_id, dest_id, rel=None):
    for c in connections(pg_id):
        comp = c["component"]
        if comp["source"]["id"] != source_id or comp["destination"]["id"] != dest_id:
            continue
        if rel is not None and rel not in comp.get("selectedRelationships", []):
            continue
        return c
    return None


def delete_connection(pg_id, conn_id, allow_drop=False):
    ent = nifi("GET", f"/nifi-api/connections/{conn_id}")
    queued = ent.get("status", {}).get("aggregateSnapshot", {}).get("flowFilesQueued")
    if queued and str(queued) != "0":
        if not allow_drop:
            raise RuntimeError(f"Connection {conn_id} has queued FlowFiles; refusing to delete")
    version = ent["revision"]["version"]
    nifi("DELETE", f"/nifi-api/connections/{conn_id}?version={version}&clientId={urllib.parse.quote(CLIENT_ID)}")


def delete_processor(proc_id):
    ent = nifi("GET", f"/nifi-api/processors/{proc_id}")
    version = ent["revision"]["version"]
    nifi("DELETE", f"/nifi-api/processors/{proc_id}?version={version}&clientId={urllib.parse.quote(CLIENT_ID)}")


# ---------------------------------------------------------------------------
# SentinelOne: same script, drop canonicalization + native-id sanitization.
# ---------------------------------------------------------------------------

SENTINELONE_HASH_SCRIPT = r'''
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
    // Deliberately no character sanitization here -- native ids are used exactly as returned
    // by the source API.

    // Content hash excludes ingestion-generated fields (they are attributes, not payload)
    // plus any explicitly volatile source fields named in EXCLUDE_FIELDS. Key order is left
    // exactly as parsed (no canonicalization/sorting) -- only exclusion is applied.
    def excludeRaw = prop('EXCLUDE_FIELDS') ?: ''
    def excludes = excludeRaw.split(',').collect { it.trim() }.findAll { it }
    def hashable = rec
    if (excludes) {
        hashable = new LinkedHashMap()
        rec.each { k, v -> if (!excludes.contains(k.toString())) hashable.put(k.toString(), v) }
    }
    def hashableJson = JsonOutput.toJson(hashable)
    def hash = MessageDigest.getInstance('SHA-256').digest(hashableJson.getBytes('UTF-8')).encodeHex().toString()

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


def apply_sentinelone(entity):
    procs = processors_by_name(SENTINELONE_PG_ID)
    dh = procs[f"sentinelone.{entity}__raw__dedupe_hash"]
    update_processor(dh["id"], {"Script Body": SENTINELONE_HASH_SCRIPT})
    return {"entity": entity}


def apply_sentinelone_all():
    return [apply_sentinelone(e) for e in SENTINELONE_ENTITIES]


def verify_sentinelone():
    procs = processors_by_name(SENTINELONE_PG_ID)
    mismatches = []
    for e in SENTINELONE_ENTITIES:
        c = procs.get(f"sentinelone.{e}__raw__dedupe_hash")
        if not c:
            mismatches.append(f"{e} MISSING")
            continue
        if c["component"]["config"]["properties"].get("Script Body") != SENTINELONE_HASH_SCRIPT:
            mismatches.append(e)
        if c["component"].get("validationStatus") != "VALID":
            mismatches.append(f"{e} INVALID")
    return {"mismatches": mismatches}


# ---------------------------------------------------------------------------
# Rapid7 (asyad + securado): swap CryptographicHashContent -> Groovy hash with
# field-exclusion support, no canonicalization, no id sanitization elsewhere.
# The output attribute is still named content_SHA-256 so nothing downstream changes.
# ---------------------------------------------------------------------------

RAPID7_HASH_SCRIPT = r'''
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import java.security.MessageDigest
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

    def hash
    if (rec instanceof Map) {
        // Key order is left exactly as parsed (no canonicalization/sorting) -- only the
        // explicitly excluded fields (if any) are stripped before hashing.
        def excludeRaw = prop('EXCLUDE_FIELDS') ?: ''
        def excludes = excludeRaw.split(',').collect { it.trim() }.findAll { it }
        def hashable = rec
        if (excludes) {
            hashable = new LinkedHashMap()
            rec.each { k, v -> if (!excludes.contains(k.toString())) hashable.put(k.toString(), v) }
        }
        def hashableJson = JsonOutput.toJson(hashable)
        hash = MessageDigest.getInstance('SHA-256').digest(hashableJson.getBytes('UTF-8')).encodeHex().toString()
    } else {
        // Non-object content (shouldn't happen for these entities) -- hash the raw bytes.
        hash = MessageDigest.getInstance('SHA-256').digest(textHolder.value.getBytes('UTF-8')).encodeHex().toString()
    }

    flowFile = session.putAttribute(flowFile, 'content_SHA-256', hash)
    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    flowFile = session.putAttribute(flowFile, 'hash.error', e.message ?: e.toString())
    log.error('rapid7 hash failed: ' + e.message, e)
    session.transfer(flowFile, REL_FAILURE)
}
'''


def swap_rapid7_hash(pg_id, source_instance, entity):
    procs = processors_by_name(pg_id)
    prefix = f"{source_instance}.{entity}"
    old_hash = procs[f"{prefix}__hash"]

    if old_hash["component"]["type"] == "org.apache.nifi.processors.groovyx.ExecuteGroovyScript":
        # Already swapped (idempotent re-run) -- just make sure the script/property are current.
        update_processor(old_hash["id"], {"Script Body": RAPID7_HASH_SCRIPT, "EXCLUDE_FIELDS": "${literal('')}"})
        return {"entity": entity, "status": "already_swapped"}

    set_ids = procs[f"{prefix}__set_ids"]

    # Upstream of __hash varies by entity (detail_fetch for entities needing a per-item detail
    # call, extract directly for entities whose data comes embedded in the parent response) --
    # discover it live rather than assuming a fixed name.
    id_to_name = {p["id"]: name for name, p in procs.items()}
    in_edge = None
    for c in connections(pg_id):
        comp = c["component"]
        if comp["destination"]["id"] == old_hash["id"]:
            in_edge = c
            break
    if in_edge is None:
        raise RuntimeError(f"No inbound connection found for {prefix}__hash")
    in_rel = in_edge["component"]["selectedRelationships"]
    upstream_id = in_edge["component"]["source"]["id"]
    upstream_name = id_to_name.get(upstream_id, in_edge["component"]["source"].get("name"))

    out_edge = find_connection(pg_id, old_hash["id"], set_ids["id"], rel="success")

    pos = old_hash.get("position", {"x": 0, "y": 0})

    delete_connection(pg_id, in_edge["id"])
    if out_edge:
        delete_connection(pg_id, out_edge["id"])
    delete_processor(old_hash["id"])

    new_hash_id = create_processor(
        pg_id, f"{prefix}__hash", "org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
        pos.get("x", 0), pos.get("y", 0),
        {"Script Body": RAPID7_HASH_SCRIPT, "EXCLUDE_FIELDS": "${literal('')}"},
        ["failure"],
    )
    create_connection(pg_id, upstream_id, upstream_name, new_hash_id, f"{prefix}__hash", in_rel)
    create_connection(pg_id, new_hash_id, f"{prefix}__hash", set_ids["id"], set_ids["component"]["name"], ["success"])

    return {"entity": entity, "status": "swapped", "new_id": new_hash_id}


def swap_rapid7_all(pg_id, source_instance):
    return [swap_rapid7_hash(pg_id, source_instance, e) for e in RAPID7_ENTITIES]


def verify_rapid7(pg_id, source_instance):
    procs = processors_by_name(pg_id)
    mismatches = []
    for e in RAPID7_ENTITIES:
        c = procs.get(f"{source_instance}.{e}__hash")
        if not c:
            mismatches.append(f"{e} MISSING")
            continue
        if c["component"]["type"] != "org.apache.nifi.processors.groovyx.ExecuteGroovyScript":
            mismatches.append(f"{e} WRONG_TYPE")
            continue
        if c["component"]["config"]["properties"].get("Script Body") != RAPID7_HASH_SCRIPT:
            mismatches.append(e)
        if c["component"].get("validationStatus") != "VALID":
            mismatches.append(f"{e} INVALID")
    return {"mismatches": mismatches}


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "help"
    if cmd == "sentinelone-entity":
        print(json.dumps(apply_sentinelone(sys.argv[2]), indent=2))
    elif cmd == "sentinelone-all":
        print(json.dumps(apply_sentinelone_all(), indent=2))
    elif cmd == "sentinelone-verify":
        print(json.dumps(verify_sentinelone(), indent=2))
    elif cmd == "asyad-entity":
        print(json.dumps(swap_rapid7_hash(RAPID7_ASYAD_PG_ID, "rapid7_asyad", sys.argv[2]), indent=2))
    elif cmd == "asyad-all":
        print(json.dumps(swap_rapid7_all(RAPID7_ASYAD_PG_ID, "rapid7_asyad"), indent=2))
    elif cmd == "asyad-verify":
        print(json.dumps(verify_rapid7(RAPID7_ASYAD_PG_ID, "rapid7_asyad"), indent=2))
    elif cmd == "securado-entity":
        print(json.dumps(swap_rapid7_hash(RAPID7_SECURADO_PG_ID, "rapid7_securado", sys.argv[2]), indent=2))
    elif cmd == "securado-all":
        print(json.dumps(swap_rapid7_all(RAPID7_SECURADO_PG_ID, "rapid7_securado"), indent=2))
    elif cmd == "securado-verify":
        print(json.dumps(verify_rapid7(RAPID7_SECURADO_PG_ID, "rapid7_securado"), indent=2))
    else:
        raise SystemExit(f"unknown command: {cmd}")
