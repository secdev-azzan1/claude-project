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
CLIENT_ID = "codex-fortisiem-org-device-object-id"

PG_ID = "11a8ce0c-01a0-1000-66c2-2931dd000cbb"  # fortisiem.maximum_useful

APICURIO_CCOMPAT = os.environ.get("APICURIO_CCOMPAT", "https://apicurio.datapasc.com/apis/ccompat/v7").rstrip("/")

ENTITIES = ["organization", "device"]

HEADER_PATTERN_FIELDS = [
    "source_platform", "customer_tenant_organization", "source_object_type",
    "source_object_id", "extraction_timestamp", "source_event_update_timestamp",
    "api_endpoint_export_query_identity", "cursor_window", "payload_hash_fingerprint",
    "ingestion_run_batch_identity", "object_id", "ingest_ts",
]
HEADER_PATTERN = "^(" + "|".join(HEADER_PATTERN_FIELDS) + ")$"


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


def update_processor(proc_id, properties=None):
    ent = nifi("GET", f"/nifi-api/processors/{proc_id}")
    comp = ent["component"]
    cfg = comp.get("config", {})
    props = {k: v for k, v in (cfg.get("properties") or {}).items() if v != "********"}
    if properties:
        props.update(properties)
    payload = {"revision": {"clientId": CLIENT_ID, "version": ent["revision"]["version"]}, "component": {"id": proc_id, "name": comp["name"], "config": {"properties": props}}}
    return nifi("PUT", f"/nifi-api/processors/{proc_id}", payload)


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


def fix_schema(entity):
    subject = f"bronze.fortisiem.{entity}__raw.avro-value"
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
# NiFi: add object_id + ingest_ts to the existing dedupe_hash / avro_normalize
# Groovy scripts, without touching the embedded dedup-cache-lookup logic
# (that restructure is separate, out of scope here).
# ---------------------------------------------------------------------------

DEDUPE_HASH_INJECT_OLD = "flowFile = session.putAttribute(flowFile, 'dedupe.key', dedupeKey)\n"
DEDUPE_HASH_INJECT_NEW = (
    "flowFile = session.putAttribute(flowFile, 'dedupe.key', dedupeKey)\n"
    "// Same 5-segment composite convention as sentinelone/rapid7: source:tenant:entity:objectId:hash.\n"
    "// Deliberately not reusing dedupeKey above -- that uses DEDUPE_NAMESPACE, not tenant.\n"
    "def objectIdComposite = \"${sourcePlatform}:${customerOrg}:${entity}:${objectId}:${hash}\"\n"
    "flowFile = session.putAttribute(flowFile, 'object_id', objectIdComposite)\n"
    "// Per-record stamp, epoch millis, matching sentinelone/rapid7's ingest_ts. Set after the\n"
    "// hash above, so it can never enter the content fingerprint.\n"
    "flowFile = session.putAttribute(flowFile, 'ingest_ts', System.currentTimeMillis().toString())\n"
)

AVRO_NORMALIZE_OLD = (
    "    'payload_hash_fingerprint',\n"
    "        'ingestion_run_batch_identity'\n"
    "    ].each { k ->\n"
    "        out.put(k, flowFile.getAttribute(k) ?: '')\n"
    "    }\n"
)
AVRO_NORMALIZE_NEW = (
    "    'payload_hash_fingerprint',\n"
    "        'ingestion_run_batch_identity',\n"
    "        'object_id'\n"
    "    ].each { k ->\n"
    "        out.put(k, flowFile.getAttribute(k) ?: '')\n"
    "    }\n"
    "    // ingest_ts is epoch millis and must land as a JSON number, not a quoted string, to match\n"
    "    // the [\"null\",\"long\"] Avro schema field.\n"
    "    def ingestTsAttr = flowFile.getAttribute('ingest_ts')\n"
    "    out.put('ingest_ts', (ingestTsAttr && ingestTsAttr.trim().length() > 0) ? Long.parseLong(ingestTsAttr) : null)\n"
)


def apply_entity(entity):
    procs = processors_by_name()
    prefix = f"fortisiem.{entity}"

    dh = procs[f"{prefix}__raw__dedupe_hash"]
    dh_script = dh["component"]["config"]["properties"]["Script Body"]
    if DEDUPE_HASH_INJECT_OLD not in dh_script:
        raise RuntimeError(f"{prefix}__raw__dedupe_hash: expected anchor text not found, refusing to patch blind")
    if "objectIdComposite" not in dh_script:
        dh_script = dh_script.replace(DEDUPE_HASH_INJECT_OLD, DEDUPE_HASH_INJECT_NEW, 1)
        update_processor(dh["id"], {"Script Body": dh_script})

    an = procs[f"{prefix}__avro__normalize_json"]
    an_script = an["component"]["config"]["properties"]["Script Body"]
    if AVRO_NORMALIZE_OLD not in an_script:
        raise RuntimeError(f"{prefix}__avro__normalize_json: expected anchor text not found, refusing to patch blind")
    if "ingestTsAttr" not in an_script:
        an_script = an_script.replace(AVRO_NORMALIZE_OLD, AVRO_NORMALIZE_NEW, 1)
        update_processor(an["id"], {"Script Body": an_script})

    rp = procs[f"{prefix}__raw__publish"]
    update_processor(rp["id"], {"FlowFile Attribute Header Pattern": HEADER_PATTERN})

    ap = procs[f"{prefix}__avro__publish"]
    update_processor(ap["id"], {"FlowFile Attribute Header Pattern": HEADER_PATTERN})

    return {"entity": entity}


def verify():
    procs = {p["component"]["name"]: p["component"] for p in flow()["processors"]}
    mismatches = []
    for entity in ENTITIES:
        prefix = f"fortisiem.{entity}"
        dh = procs.get(f"{prefix}__raw__dedupe_hash")
        if not dh or "objectIdComposite" not in dh["config"]["properties"].get("Script Body", ""):
            mismatches.append(f"{prefix}__raw__dedupe_hash missing object_id")
        if dh and dh.get("validationStatus") != "VALID":
            mismatches.append(f"{prefix}__raw__dedupe_hash INVALID: {dh.get('validationErrors')}")
        an = procs.get(f"{prefix}__avro__normalize_json")
        if not an or "ingestTsAttr" not in an["config"]["properties"].get("Script Body", ""):
            mismatches.append(f"{prefix}__avro__normalize_json missing object_id")
        if an and an.get("validationStatus") != "VALID":
            mismatches.append(f"{prefix}__avro__normalize_json INVALID: {an.get('validationErrors')}")
        for suffix in ("raw__publish", "avro__publish"):
            p = procs.get(f"{prefix}__{suffix}")
            if not p or p["config"]["properties"].get("FlowFile Attribute Header Pattern") != HEADER_PATTERN:
                mismatches.append(f"{prefix}__{suffix} header pattern mismatch")
            if p and p.get("validationStatus") != "VALID":
                mismatches.append(f"{prefix}__{suffix} INVALID")

    schema_status = []
    for entity in ENTITIES:
        subject = f"bronze.fortisiem.{entity}__raw.avro-value"
        latest = apicurio_get(f"/subjects/{urllib.parse.quote(subject, safe='')}/versions/latest")
        schema = json.loads(latest["schema"])
        by_field = {f["name"]: f for f in schema["fields"]}
        ok = by_field.get("ingest_ts", {}).get("type") == ["null", "long"] and "object_id" in by_field
        if not ok:
            schema_status.append({"subject": subject, "ingest_ts_type": by_field.get("ingest_ts", {}).get("type"), "has_object_id": "object_id" in by_field})

    not_stopped = [name for name, p in procs.items() if name.startswith("fortisiem.") and p.get("state") not in ("STOPPED", "DISABLED")]

    return {"mismatches": mismatches, "bad_schemas": schema_status, "not_stopped": not_stopped}


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "help"
    if cmd == "apply-all":
        results = []
        for e in ENTITIES:
            try:
                results.append(apply_entity(e))
            except Exception as exc:
                results.append({"entity": e, "error": str(exc)})
        print(json.dumps(results, indent=2))
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
