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

PG_ID = os.environ.get("RAPID7_ASYAD_MAX_PG_ID", "14db305d-01a0-1000-11f0-c68b900bbdb5")
CLIENT_ID = "codex-rapid7-asyad-object-id-patch"

APICURIO_CCOMPAT = os.environ.get("APICURIO_CCOMPAT", "https://apicurio.datapasc.com/apis/ccompat/v7").rstrip("/")
KAFKA_CONNECT_BASE = os.environ.get("KAFKA_CONNECT_BASE", "https://kafkaconnect.datapasc.com").rstrip("/")

ENTITIES = ["site", "asset", "asset_service", "asset_software", "asset_vulnerability"]

ORPHAN_ENTITIES = [
    "asset_vulnerability_solution", "exploit", "malware_kit", "operating_system",
    "site_organization", "software", "solution", "tag", "tag_asset",
    "vulnerability", "vulnerability_category", "vulnerability_reference",
]

HEADER_PATTERN_FIELDS = [
    "source_platform", "customer_tenant_organization", "source_object_type",
    "source_object_id", "extraction_timestamp", "source_event_update_timestamp",
    "api_endpoint_export_query_identity", "cursor_window", "payload_hash_fingerprint",
    "ingestion_run_batch_identity", "object_id", "ingest_ts",
]
HEADER_PATTERN = "^(" + "|".join(HEADER_PATTERN_FIELDS) + ")$"


# ---------------------------------------------------------------------------
# NiFi REST helpers (same shape as tools/patch_sentinelone_object_id_and_raw_body.py)
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


def set_processor_state(proc_id, state):
    ent = nifi("GET", f"/nifi-api/processors/{proc_id}")
    if ent["component"].get("state") == state:
        return
    nifi("PUT", f"/nifi-api/processors/{proc_id}/run-status", {"revision": {"clientId": CLIENT_ID, "version": ent["revision"]["version"]}, "state": state})


def stop_all():
    for p in processors_by_name().values():
        stop_processor(p["id"])


def run_once(wait_seconds=90):
    # replay__consume must never run alongside a live fetch -- it republishes from the raw
    # topic straight to __avro__publish, which would double-produce every record fetched live.
    procs = processors_by_name()
    for name, p in procs.items():
        if name == "rapid7_asyad.maximum__trigger" or "__replay__consume" in name:
            continue
        ent = nifi("GET", f"/nifi-api/processors/{p['id']}")
        if ent["component"].get("validationStatus") == "VALID":
            set_processor_state(p["id"], "RUNNING")

    trigger = procs["rapid7_asyad.maximum__trigger"]
    set_processor_state(trigger["id"], "RUNNING")
    time.sleep(4)
    stop_processor(trigger["id"])

    deadline = time.time() + wait_seconds
    last = []
    while time.time() < deadline:
        last = [c for c in connections() if str(c.get("status", {}).get("aggregateSnapshot", {}).get("flowFilesQueued", "0")) not in ("0", "None", "")]
        if not last:
            break
        time.sleep(5)

    stop_all()
    return {"queued_remaining": [{"source": c["component"]["source"].get("name"), "destination": c["component"]["destination"].get("name"), "queued": c["status"]["aggregateSnapshot"].get("flowFilesQueued")} for c in last]}


def update_processor(proc_id, properties=None, auto_terms=None):
    ent = nifi("GET", f"/nifi-api/processors/{proc_id}")
    comp = ent["component"]
    cfg = comp.get("config", {})
    # Masked "********" sensitive properties must never be written back verbatim.
    props = {k: v for k, v in (cfg.get("properties") or {}).items() if v != "********"}
    if properties:
        props.update(properties)
    new_cfg = {"properties": props}
    if auto_terms is not None:
        new_cfg["autoTerminatedRelationships"] = auto_terms
    payload = {"revision": {"clientId": CLIENT_ID, "version": ent["revision"]["version"]}, "component": {"id": proc_id, "name": comp["name"], "config": new_cfg}}
    return nifi("PUT", f"/nifi-api/processors/{proc_id}", payload)


def create_processor(name, proc_type, x, y, properties=None, auto_terms=None, scheduling_period="0 sec"):
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
    if queued and str(queued) != "0" and not allow_drop:
        raise RuntimeError(f"Connection {conn_id} has queued FlowFiles; refusing to delete")
    version = ent["revision"]["version"]
    nifi("DELETE", f"/nifi-api/connections/{conn_id}?version={version}&clientId={urllib.parse.quote(CLIENT_ID)}")


# ---------------------------------------------------------------------------
# Apicurio ccompat v7 helpers
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


def apicurio_delete(path, timeout=30):
    args = ["-X", "DELETE", "-w", "\nHTTP_STATUS:%{http_code}", f"{APICURIO_CCOMPAT}{path}"]
    out = run_curl(args, timeout=timeout)
    raw, status_txt = out.rsplit("\nHTTP_STATUS:", 1)
    status = int(status_txt.strip()[:3])
    return status, raw.strip()


def delete_schema_subject(subject):
    enc = urllib.parse.quote(subject, safe="")
    soft_status, soft_body = apicurio_delete(f"/subjects/{enc}")
    hard_status, hard_body = apicurio_delete(f"/subjects/{enc}?permanent=true")
    return {"subject": subject, "soft_status": soft_status, "hard_status": hard_status, "soft_body": soft_body[:200], "hard_body": hard_body[:200]}


# ---------------------------------------------------------------------------
# Kafka Connect helpers
# ---------------------------------------------------------------------------

def connect_get(path, timeout=30):
    out = run_curl(["-w", "\nHTTP_STATUS:%{http_code}", f"{KAFKA_CONNECT_BASE}{path}"], timeout=timeout)
    raw, status_txt = out.rsplit("\nHTTP_STATUS:", 1)
    status = int(status_txt.strip()[:3])
    if status < 200 or status > 299:
        raise RuntimeError(f"GET {path} HTTP {status}: {raw[:1000]}")
    return json.loads(raw.strip())


def connect_delete_connector(name):
    enc = urllib.parse.quote(name, safe="")
    args = ["-X", "DELETE", "-w", "\nHTTP_STATUS:%{http_code}", f"{KAFKA_CONNECT_BASE}/connectors/{enc}"]
    out = run_curl(args, timeout=60)
    raw, status_txt = out.rsplit("\nHTTP_STATUS:", 1)
    status = int(status_txt.strip()[:3])
    return {"name": name, "status": status, "body": raw.strip()[:300]}


# ---------------------------------------------------------------------------
# Step 1: remove orphans
# ---------------------------------------------------------------------------

def remove_orphans():
    schema_results = []
    connector_results = []
    for e in ORPHAN_ENTITIES:
        subject = f"bronze.rapid7_asyad.{e}__raw.avro-value"
        schema_results.append(delete_schema_subject(subject))
        connector_name = f"bronze.rapid7_asyad.{e}__raw.avro__iceberg"
        connector_results.append(connect_delete_connector(connector_name))
    return {"schemas": schema_results, "connectors": connector_results}


RAW_INGEST_TS_CAST_SCRIPT = r'''
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
    if (parsed instanceof Map && parsed.containsKey('ingest_ts')) {
        def v = parsed['ingest_ts']
        // UpdateRecord's literal-value strategy always types a brand-new field as String, so this
        // is the one field on the raw (schema-less) topic that needs an explicit numeric cast to
        // match SentinelOne's convention. Only touches ingest_ts; every other field is untouched.
        if (v instanceof String && v ==~ /^-?\d+$/) {
            parsed['ingest_ts'] = v.toLong()
        }
    }
    flowFile = session.write(flowFile, { outputStream -> outputStream.write(JsonOutput.toJson(parsed).getBytes('UTF-8')) } as OutputStreamCallback)
    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    flowFile = session.putAttribute(flowFile, 'raw.cast.error', e.message ?: e.toString())
    log.error('rapid7_asyad raw ingest_ts cast failed: ' + e.message, e)
    session.transfer(flowFile, REL_FAILURE)
}
'''


def apply_raw_ingest_ts_cast(entity):
    procs = processors_by_name()
    prefix = f"rapid7_asyad.{entity}"

    dedupe = procs[f"{prefix}__dedupe"]
    raw_publish = procs[f"{prefix}__raw__publish"]

    d_pos = dedupe.get("position", {"x": 0, "y": 0})
    rp_pos = raw_publish.get("position", {"x": d_pos.get("x", 0) + 300, "y": d_pos.get("y", 0)})
    mid_x = (d_pos.get("x", 0) + rp_pos.get("x", d_pos.get("x", 0) + 300)) / 2
    mid_y = rp_pos.get("y", 0)

    cast_id = create_processor(
        f"{prefix}__raw__cast_ingest_ts",
        "org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
        mid_x, mid_y,
        {"Script Body": RAW_INGEST_TS_CAST_SCRIPT},
        ["failure"],
    )

    old_edge = find_connection(dedupe["id"], raw_publish["id"], rel="non-duplicate")
    if old_edge:
        delete_connection(old_edge["id"])
    create_connection(dedupe["id"], dedupe["component"]["name"], cast_id, f"{prefix}__raw__cast_ingest_ts", ["non-duplicate"])
    create_connection(cast_id, f"{prefix}__raw__cast_ingest_ts", raw_publish["id"], raw_publish["component"]["name"], ["success"])

    return {"entity": entity, "cast_processor": cast_id}


# ---------------------------------------------------------------------------
# Step 2: per-entity NiFi changes
# ---------------------------------------------------------------------------

def apply_entity(entity):
    procs = processors_by_name()
    prefix = f"rapid7_asyad.{entity}"

    set_ids = procs[f"{prefix}__set_ids"]
    set_metadata = procs[f"{prefix}__set_metadata"]
    dedupe_key = procs[f"{prefix}__dedupe_key"]
    raw_publish = procs[f"{prefix}__raw__publish"]
    avro_publish = procs.get(f"{prefix}__avro__publish")

    # 1. New set_public_headers processor between set_ids and set_metadata.
    si_pos = set_ids.get("position", {"x": 0, "y": 0})
    sm_pos = set_metadata.get("position", {"x": si_pos.get("x", 0) + 300, "y": si_pos.get("y", 0)})
    mid_x = (si_pos.get("x", 0) + sm_pos.get("x", si_pos.get("x", 0) + 300)) / 2
    mid_y = si_pos.get("y", 0)

    headers_id = create_processor(
        f"{prefix}__set_public_headers",
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        mid_x, mid_y,
        {
            "source_platform": "rapid7",
            "customer_tenant_organization": "rapid7_asyad",
            "source_object_type": "${entity}",
            "source_object_id": "${object_id}",
            "source_event_update_timestamp": "",
            "api_endpoint_export_query_identity": "${api_path}",
            "payload_hash_fingerprint": "${'content_SHA-256'}",
            "ingest_ts": "${now():toNumber()}",
            "object_id_composite": "rapid7:rapid7_asyad:${entity}:${object_id}:${'content_SHA-256'}",
        },
        [],
    )

    old_edge = find_connection(set_ids["id"], set_metadata["id"], rel="success")
    if old_edge:
        delete_connection(old_edge["id"])
    create_connection(set_ids["id"], set_ids["component"]["name"], headers_id, f"{prefix}__set_public_headers", ["success"])
    create_connection(headers_id, f"{prefix}__set_public_headers", set_metadata["id"], set_metadata["component"]["name"], ["success"])

    # 2. Extend set_metadata with object_id + ingest_ts record-path mappings.
    update_processor(set_metadata["id"], {
        "/object_id": "${object_id_composite}",
        "/ingest_ts": "${ingest_ts}",
    })

    # 3. Extend dedupe_key: promote the staged composite to the public object_id attribute.
    update_processor(dedupe_key["id"], {
        "object_id": "${object_id_composite}",
    })

    # 4. Fix Kafka Key mismatch on raw__publish.
    update_processor(raw_publish["id"], {"Kafka Key": "${source_object_id}"})

    # 5. Extend header pattern on both publish processors.
    update_processor(raw_publish["id"], {"FlowFile Attribute Header Pattern": HEADER_PATTERN})
    if avro_publish:
        update_processor(avro_publish["id"], {"FlowFile Attribute Header Pattern": HEADER_PATTERN})

    return {"entity": entity, "avro": bool(avro_publish)}


# ---------------------------------------------------------------------------
# Step 3: Apicurio schema updates for live entities
# ---------------------------------------------------------------------------

def fix_schema(entity):
    subject = f"bronze.rapid7_asyad.{entity}__raw.avro-value"
    latest = apicurio_get(f"/subjects/{urllib.parse.quote(subject, safe='')}/versions/latest")
    schema = json.loads(latest["schema"])
    fields = schema["fields"]
    names = [f["name"] for f in fields]
    changed = False

    if "object_id" not in names:
        insert_at = names.index("source_object_id") + 1 if "source_object_id" in names else 0
        fields.insert(insert_at, {"name": "object_id", "type": ["null", "string"], "default": None})
        changed = True

    names = [f["name"] for f in fields]
    if "ingest_ts" not in names:
        insert_at = names.index("ingestion_run_batch_identity") + 1 if "ingestion_run_batch_identity" in names else len(fields)
        fields.insert(insert_at, {"name": "ingest_ts", "type": ["null", "long"], "default": None})
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
    invalid = [p["component"]["name"] for p in procs if p["component"]["name"].startswith("rapid7_asyad.") and p["component"].get("validationStatus") not in ("VALID", None)]
    not_stopped = [p["component"]["name"] for p in procs if p["component"]["name"].startswith("rapid7_asyad.") and p.get("status", {}).get("aggregateSnapshot", {}).get("runStatus") not in ("Stopped", "Disabled")]

    by_name = {p["component"]["name"]: p["component"] for p in procs}
    mismatches = []
    for entity in ENTITIES:
        prefix = f"rapid7_asyad.{entity}"
        hp = by_name.get(f"{prefix}__set_public_headers")
        if not hp:
            mismatches.append(f"{prefix}__set_public_headers MISSING")
        sm = by_name.get(f"{prefix}__set_metadata")
        if sm and sm["config"]["properties"].get("/object_id") != "${object_id_composite}":
            mismatches.append(f"{prefix}__set_metadata missing /object_id mapping")
        dk = by_name.get(f"{prefix}__dedupe_key")
        if dk and dk["config"]["properties"].get("object_id") != "${object_id_composite}":
            mismatches.append(f"{prefix}__dedupe_key missing object_id promotion")
        rp = by_name.get(f"{prefix}__raw__publish")
        if rp:
            if rp["config"]["properties"].get("Kafka Key") != "${source_object_id}":
                mismatches.append(f"{prefix}__raw__publish Kafka Key not fixed")
            if rp["config"]["properties"].get("FlowFile Attribute Header Pattern") != HEADER_PATTERN:
                mismatches.append(f"{prefix}__raw__publish header pattern mismatch")
        ap = by_name.get(f"{prefix}__avro__publish")
        if ap and ap["config"]["properties"].get("FlowFile Attribute Header Pattern") != HEADER_PATTERN:
            mismatches.append(f"{prefix}__avro__publish header pattern mismatch")

    schema_status = []
    for entity in ENTITIES:
        subject = f"bronze.rapid7_asyad.{entity}__raw.avro-value"
        latest = apicurio_get(f"/subjects/{urllib.parse.quote(subject, safe='')}/versions/latest")
        schema = json.loads(latest["schema"])
        by_field = {f["name"]: f for f in schema["fields"]}
        ok = by_field.get("ingest_ts", {}).get("type") == ["null", "long"] and "object_id" in by_field
        if not ok:
            schema_status.append({"subject": subject, "ingest_ts_type": by_field.get("ingest_ts", {}).get("type"), "has_object_id": "object_id" in by_field})

    orphans_remaining = []
    for e in ORPHAN_ENTITIES:
        subject = f"bronze.rapid7_asyad.{e}__raw.avro-value"
        try:
            apicurio_get(f"/subjects/{urllib.parse.quote(subject, safe='')}/versions/latest")
            orphans_remaining.append(subject)
        except RuntimeError:
            pass

    connectors = connect_get("/connectors")
    orphan_connectors_remaining = [c for c in connectors if any(f"bronze.rapid7_asyad.{e}__raw.avro__iceberg" == c for e in ORPHAN_ENTITIES)]

    return {
        "invalid_processors": invalid,
        "not_stopped": not_stopped,
        "mismatches": mismatches,
        "bad_schemas": schema_status,
        "orphan_schemas_remaining": orphans_remaining,
        "orphan_connectors_remaining": orphan_connectors_remaining,
    }


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "help"
    if cmd == "remove-orphans":
        print(json.dumps(remove_orphans(), indent=2))
    elif cmd == "run-once":
        print(json.dumps(run_once(int(os.environ.get("WAIT_SECONDS", "90"))), indent=2))
    elif cmd == "cast-ingest-ts-entity":
        print(json.dumps(apply_raw_ingest_ts_cast(sys.argv[2]), indent=2))
    elif cmd == "cast-ingest-ts-all":
        results = []
        for e in ENTITIES:
            try:
                results.append(apply_raw_ingest_ts_cast(e))
            except Exception as exc:
                results.append({"entity": e, "error": str(exc)})
        print(json.dumps(results, indent=2))
    elif cmd == "apply-entity":
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
        for e in ENTITIES:
            try:
                results.append(fix_schema(e))
            except Exception as exc:
                results.append({"entity": e, "error": str(exc)})
        print(json.dumps(results, indent=2))
    elif cmd == "verify":
        print(json.dumps(verify(), indent=2))
    else:
        raise SystemExit(f"unknown command: {cmd}")
