"""
Shared helpers for the FortiSIEM native rebuild (organization/device/interface/processor/storage/
installed_software/device_custom_property/agent_status/incident). Matches the exact rapid7_asyad
processor pattern (set_ids -> hash -> set_public_headers -> set_metadata -> dedupe_key -> dedupe ->
raw__publish, plus replay__consume -> avro__publish), adapted for FortiSIEM's XML source via native
XMLReader/XMLRecordSetWriter instead of JSON.
"""
import json
import os
import subprocess
import time
import urllib.parse

NIFI_BASE = os.environ.get("NIFI_BASE", "https://nifi.datapasc.com").rstrip("/")
NIFI_USER = os.environ.get("NIFI_USER", "admin")
NIFI_PASSWORD = os.environ.get("NIFI_PASSWORD")

PG_ID = "11a8ce0c-01a0-1000-66c2-2931dd000cbb"
SCHEMA_REGISTRY_ID = "db86aea0-2bee-3687-9187-5679904d69b0"       # global__schema_registry
KAFKA_CONNECTION_ID = "40675f79-8eaa-3193-8f8d-026c8c1ee947"      # global__kafka_connection
DMC_SERVICE_ID = "11adb129-01a0-1000-60b4-f4a04de9e3ac"           # fortisiem.maximum__dedupe__cache
SCHEMA_REF_WRITER_ID = "2c59d8ad-103a-3e0e-fb8f-54726496f8b9"     # global__schema_ref_writer

STANDARD_HEADER_REGEX = ("^(source_platform|customer_tenant_organization|source_object_type|"
                          "source_object_id|extraction_timestamp|source_event_update_timestamp|"
                          "api_endpoint_export_query_identity|cursor_window|payload_hash_fingerprint|"
                          "ingestion_run_batch_identity|object_id|ingest_ts)$")

HASH_SCRIPT = '''import java.security.MessageDigest
import org.apache.nifi.processor.io.InputStreamCallback

def flowFile = session.get()
if (!flowFile) return

try {
    def prop = { name ->
        def p = context.getProperty(name)
        return p == null ? null : p.evaluateAttributeExpressions(flowFile).getValue()
    }

    def bytesOut = new ByteArrayOutputStream()
    session.read(flowFile, { inputStream ->
        byte[] buffer = new byte[8192]
        int len
        while ((len = inputStream.read(buffer)) > -1) bytesOut.write(buffer, 0, len)
    } as InputStreamCallback)
    def text = new String(bytesOut.toByteArray(), 'UTF-8')

    // Fields to exclude before hashing, if configured. XML preserves document order naturally
    // (no canonicalization needed) -- excluded top-level elements are simply stripped by tag name
    // before hashing, same "no canonicalization, honor EXCLUDE_FIELDS" policy as sentinelone/rapid7.
    def excludeRaw = prop('EXCLUDE_FIELDS') ?: ''
    def excludes = excludeRaw.split(',').collect { it.trim() }.findAll { it }
    def hashable = text
    excludes.each { tag ->
        hashable = hashable.replaceAll(/(?s)<${tag}(?:\\s[^>]*)?>.*?<\\/${tag}>\\s*/, '')
    }

    def hash = MessageDigest.getInstance('SHA-256').digest(hashable.getBytes('UTF-8')).encodeHex().toString()
    flowFile = session.putAttribute(flowFile, 'content_SHA-256', hash)
    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    flowFile = session.putAttribute(flowFile, 'hash.error', e.message ?: e.toString())
    log.error('fortisiem hash failed: ' + e.message, e)
    session.transfer(flowFile, REL_FAILURE)
}
'''


def run_curl(args, input_text=None, timeout=30, attempts=3):
    last = None
    for i in range(attempts):
        proc = subprocess.run(["curl.exe", "--http1.1", "-k", "-sS"] + args, input=input_text, text=True, capture_output=True, timeout=timeout)
        if proc.returncode == 0:
            return proc.stdout
        last = f"curl exit {proc.returncode}: {proc.stderr[:400]} {proc.stdout[:400]}"
        time.sleep(1 + i)
    raise RuntimeError(last)


def login():
    if not NIFI_PASSWORD:
        raise RuntimeError("Set NIFI_PASSWORD")
    body = urllib.parse.urlencode({"username": NIFI_USER, "password": NIFI_PASSWORD})
    return run_curl(["-H", "Content-Type: application/x-www-form-urlencoded", "--data-binary", "@-", f"{NIFI_BASE}/nifi-api/access/token"], body).strip()


def nifi(method, path, token, body=None, timeout=60):
    args = ["-X", method, "-H", f"Authorization: Bearer {token}"]
    input_text = None
    if body is not None:
        args += ["-H", "Content-Type: application/json", "--data-binary", "@-"]
        input_text = json.dumps(body)
    args += ["-w", "\nHTTP_STATUS:%{http_code}", f"{NIFI_BASE}{path}"]
    out = run_curl(args, input_text=input_text, timeout=timeout)
    raw, status_txt = out.rsplit("\nHTTP_STATUS:", 1)
    status = int(status_txt.strip()[:3])
    respbody = {}
    if raw.strip():
        try:
            respbody = json.loads(raw)
        except json.JSONDecodeError:
            respbody = {"raw_text": raw[:800]}
    return status, respbody


def nifi_ok(method, path, token, body=None, timeout=60, context=""):
    status, resp = nifi(method, path, token, body, timeout)
    if status not in (200, 201):
        raise RuntimeError(f"{context or path} failed HTTP {status}: {json.dumps(resp)[:500]}")
    return resp


def create_controller_service(token, service_type, name, properties, bundle):
    payload = {
        "revision": {"version": 0},
        "component": {"type": service_type, "bundle": bundle, "name": name, "properties": properties},
    }
    resp = nifi_ok("POST", f"/nifi-api/process-groups/{PG_ID}/controller-services", token, payload, context=f"create CS {name}")
    return resp["component"]["id"]


def enable_controller_service(token, cs_id):
    status, cs = nifi("GET", f"/nifi-api/controller-services/{cs_id}", token)
    version = cs["revision"]["version"]
    dstatus, dresp = nifi("PUT", f"/nifi-api/controller-services/{cs_id}/run-status", token, {"revision": {"version": version}, "state": "ENABLED"})
    return dstatus, dresp


def create_processor(token, proc_type, name, properties=None, bundle=None, position=None, auto_terminate=None, scheduling=None):
    component = {"type": proc_type, "name": name}
    if bundle:
        component["bundle"] = bundle
    config = {}
    if properties:
        config["properties"] = properties
    if auto_terminate:
        config["autoTerminatedRelationships"] = auto_terminate
    if scheduling:
        config.update(scheduling)
    if config:
        component["config"] = config
    if position:
        component["position"] = position
    payload = {"revision": {"version": 0}, "component": component}
    resp = nifi_ok("POST", f"/nifi-api/process-groups/{PG_ID}/processors", token, payload, context=f"create processor {name}")
    return resp["component"]["id"]


def update_processor(token, proc_id, properties=None, auto_terminate=None, name=None, attempts=6):
    last_err = None
    for i in range(attempts):
        status, current = nifi("GET", f"/nifi-api/processors/{proc_id}", token)
        if status != 200:
            raise RuntimeError(f"GET processor {proc_id} failed HTTP {status}")
        comp = current["component"]
        merged_props = dict(comp["config"].get("properties") or {})
        if properties:
            for k, v in properties.items():
                if v == "********":
                    continue
                merged_props[k] = v
        payload_component = {"id": proc_id, "config": dict(comp["config"])}
        payload_component["config"]["properties"] = merged_props
        if auto_terminate is not None:
            payload_component["config"]["autoTerminatedRelationships"] = auto_terminate
        if name:
            payload_component["name"] = name
        payload = {"revision": current["revision"], "component": payload_component}
        status, resp = nifi("PUT", f"/nifi-api/processors/{proc_id}", token, payload)
        if status == 200:
            return resp
        last_err = f"HTTP {status}: {json.dumps(resp)[:400]}"
        if "while the Processor is running" in json.dumps(resp) or status == 409:
            time.sleep(2 + i * 2)
            continue
        raise RuntimeError(f"update_processor {proc_id} failed: {last_err}")
    raise RuntimeError(f"update_processor {proc_id} failed after retries: {last_err}")


def create_connection(token, source_id, source_name, source_type, dest_id, dest_name, dest_type, relationships):
    payload = {
        "revision": {"version": 0},
        "component": {
            "parentGroupId": PG_ID,
            "source": {"id": source_id, "type": source_type, "groupId": PG_ID, "name": source_name},
            "destination": {"id": dest_id, "type": dest_type, "groupId": PG_ID, "name": dest_name},
            "selectedRelationships": relationships,
        },
    }
    resp = nifi_ok("POST", f"/nifi-api/process-groups/{PG_ID}/connections", token, payload, context=f"connect {source_name}->{dest_name}")
    return resp["id"]


def delete_processor(token, proc_id, name=""):
    status, current = nifi("GET", f"/nifi-api/processors/{proc_id}", token)
    if status != 200:
        return False, f"GET failed {status}"
    version = current["revision"]["version"]
    dstatus, dresp = nifi("DELETE", f"/nifi-api/processors/{proc_id}?version={version}", token)
    return dstatus == 200, dresp


def delete_connection(token, conn_id):
    status, current = nifi("GET", f"/nifi-api/connections/{conn_id}", token)
    if status != 200:
        return False, f"GET failed {status}"
    version = current["revision"]["version"]
    dstatus, dresp = nifi("DELETE", f"/nifi-api/connections/{conn_id}?version={version}", token)
    return dstatus == 200, dresp


def get_flow(token):
    return nifi_ok("GET", f"/nifi-api/flow/process-groups/{PG_ID}", token, context="get flow")


def xml_reader_bundle():
    return {"group": "org.apache.nifi", "artifact": "nifi-record-serialization-services-nar", "version": "2.9.0"}


def make_xml_reader_props(schema_name, attribute_prefix="attr_"):
    return {
        "Schema Access Strategy": "schema-name",
        "Schema Registry": SCHEMA_REGISTRY_ID,
        "Schema Name": schema_name,
        "Parse XML Attributes": "true",
        "Attribute Prefix": attribute_prefix,
        "Expect Records as Array": "false",
    }


def make_xml_writer_props(schema_name, root_tag):
    return {
        "Schema Write Strategy": "no-schema",
        "Schema Access Strategy": "schema-name",
        "Schema Registry": SCHEMA_REGISTRY_ID,
        "Schema Name": schema_name,
        "Name of Root Tag": root_tag,
        "Wrap Elements of Arrays": "no-wrapping",
        "Pretty Print XML": "false",
        "Suppress Null Values": "never-suppress",
    }


def make_avro_writer_props(schema_name):
    return {
        "Schema Write Strategy": "schema-reference-writer",
        "Schema Access Strategy": "schema-name",
        "Schema Registry": SCHEMA_REGISTRY_ID,
        "Schema Name": schema_name,
        "Schema Reference Writer": SCHEMA_REF_WRITER_ID,
    }
