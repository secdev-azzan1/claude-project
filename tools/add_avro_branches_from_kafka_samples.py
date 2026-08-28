import json
import os
import re
import subprocess
import time
import urllib.parse
import xml.etree.ElementTree as ET
from collections import OrderedDict

import requests


NIFI_BASE = os.environ.get("NIFI_BASE", "https://nifi.datapasc.com").rstrip("/")
NIFI_USER = os.environ.get("NIFI_USER")
NIFI_PASSWORD = os.environ.get("NIFI_PASSWORD")
NIFI_TOKEN = os.environ.get("NIFI_TOKEN")

KAFBAT_BASE = os.environ.get("KAFBAT_BASE", "https://kafbat.datapasc.com").rstrip("/")
KAFBAT_USER = os.environ.get("KAFBAT_USER")
KAFBAT_PASSWORD = os.environ.get("KAFBAT_PASSWORD")

APICURIO_CCOMPAT = os.environ.get("APICURIO_CCOMPAT", "https://apicurio.datapasc.com/apis/ccompat/v7").rstrip("/")

FORTISIEM_PG_ID = os.environ.get("FORTISIEM_PG_ID", "77227d7a-2d8a-323d-02b2-5f7aec5ea246")
SENTINELONE_PG_ID = os.environ.get("SENTINELONE_PG_ID", "1c122170-d74a-3b60-2776-28717d5cf049")

KAFKA_SERVICE_ID = os.environ.get("KAFKA_SERVICE_ID", "40675f79-8eaa-3193-8f8d-026c8c1ee947")
SCHEMA_REGISTRY_SERVICE_ID = os.environ.get("SCHEMA_REGISTRY_SERVICE_ID", "db86aea0-2bee-3687-9187-5679904d69b0")
SCHEMA_REF_WRITER_SERVICE_ID = os.environ.get("SCHEMA_REF_WRITER_SERVICE_ID", "2c59d8ad-103a-3e0e-fb8f-54726496f8b9")

CLIENT_ID = "codex-avro-branches"
MAX_DEPTH = int(os.environ.get("SCHEMA_MAX_DEPTH", "5"))
SAMPLE_LIMIT = int(os.environ.get("SCHEMA_SAMPLE_LIMIT", "100"))

STANDARD_HEADER_PATTERN = (
    r"^(source_platform|customer_tenant_organization|source_object_type|"
    r"source_object_id|extraction_timestamp|source_event_update_timestamp|"
    r"api_endpoint_export_query_identity|cursor_window|payload_hash_fingerprint|"
    r"ingestion_run_batch_identity)$"
)

ENTITIES = [
    {
        "key": "fortisiem_organization",
        "pg": "forti",
        "source_format": "xml",
        "raw_topic": "bronze.fortisiem.organization__raw",
        "avro_topic": "bronze.fortisiem.organization__raw.avro",
        "record_name": "fortisiem_organization_raw_avro",
        "namespace": "bronze.fortisiem",
        "branch_source": "fortisiem.organization__raw__dedupe_hash",
        "branch_rel": "success",
        "normalizer": "fortisiem.organization__avro__normalize_json",
        "publisher": "fortisiem.organization__avro__publish",
        "reader": "fortisiem.organization__avro_json_reader",
        "writer": "fortisiem.organization__avro_writer",
        "x": 2200,
        "y": -340,
    },
    {
        "key": "fortisiem_device",
        "pg": "forti",
        "source_format": "xml",
        "raw_topic": "bronze.fortisiem.device__raw",
        "avro_topic": "bronze.fortisiem.device__raw.avro",
        "record_name": "fortisiem_device_raw_avro",
        "namespace": "bronze.fortisiem",
        "branch_source": "fortisiem.device__raw__dedupe_hash",
        "branch_rel": "success",
        "normalizer": "fortisiem.device__avro__normalize_json",
        "publisher": "fortisiem.device__avro__publish",
        "reader": "fortisiem.device__avro_json_reader",
        "writer": "fortisiem.device__avro_writer_v2",
        "x": 2920,
        "y": 620,
    },
    {
        "key": "sentinelone_site",
        "pg": "s1",
        "source_format": "json",
        "raw_topic": "bronze.sentinelone.site__raw",
        "avro_topic": "bronze.sentinelone.site__raw.avro",
        "record_name": "sentinelone_site_raw_avro",
        "namespace": "bronze.sentinelone",
        "branch_source": "sentinelone.site__raw__dedupe_detect",
        "branch_rel": "non-duplicate",
        "normalizer": "sentinelone.site__avro__normalize_json",
        "publisher": "sentinelone.site__avro__publish",
        "reader": "sentinelone.site__avro_json_reader",
        "writer": "sentinelone.site__avro_writer",
        "x": 2100,
        "y": 920,
    },
    {
        "key": "sentinelone_agent",
        "pg": "s1",
        "source_format": "json",
        "raw_topic": "bronze.sentinelone.agent__raw",
        "avro_topic": "bronze.sentinelone.agent__raw.avro",
        "record_name": "sentinelone_agent_raw_avro",
        "namespace": "bronze.sentinelone",
        "branch_source": "sentinelone.agent__dedupe__detect",
        "branch_rel": "non-duplicate",
        "normalizer": "sentinelone.agent__avro__normalize_json",
        "publisher": "sentinelone.agent__avro__publish",
        "reader": "sentinelone.agent__avro_json_reader",
        "writer": "sentinelone.agent__avro_writer_v2",
        "x": 2600,
        "y": 30,
    },
]


def run_curl(args, input_text=None, timeout=60):
    proc = subprocess.run(["curl.exe", "--http1.1", "-k", "-sS"] + args, input=input_text, text=True, capture_output=True, timeout=timeout)
    if proc.returncode != 0:
        raise RuntimeError(f"curl exit {proc.returncode}: {proc.stderr[:1000]} {proc.stdout[:1000]}")
    return proc.stdout


def nifi_login():
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


NIFI_BEARER = None


def nifi(method, path, body=None, timeout=60):
    global NIFI_BEARER
    if NIFI_BEARER is None:
        NIFI_BEARER = nifi_login()
    args = ["-X", method, "-H", f"Authorization: Bearer {NIFI_BEARER}", "-w", "\nHTTP_STATUS:%{http_code}"]
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


def pg_id(kind):
    return FORTISIEM_PG_ID if kind == "forti" else SENTINELONE_PG_ID


def flow(kind):
    return nifi("GET", f"/nifi-api/flow/process-groups/{pg_id(kind)}")["processGroupFlow"]["flow"]


def processors_by_name(kind):
    return {p["component"]["name"]: p for p in flow(kind).get("processors", [])}


def controller_services_by_name(kind):
    data = nifi("GET", f"/nifi-api/flow/process-groups/{pg_id(kind)}/controller-services")
    return {s["component"]["name"]: s for s in data.get("controllerServices", [])}


def connections(kind):
    return flow(kind).get("connections", [])


def stop_processor(proc_id):
    ent = nifi("GET", f"/nifi-api/processors/{proc_id}")
    if ent["component"].get("state") in ("STOPPED", "DISABLED"):
        return
    payload = {"revision": {"clientId": CLIENT_ID, "version": ent["revision"]["version"]}, "state": "STOPPED"}
    nifi("PUT", f"/nifi-api/processors/{proc_id}/run-status", payload)


def update_processor(proc_id, properties=None, auto_terms=None, scheduling_period=None):
    ent = nifi("GET", f"/nifi-api/processors/{proc_id}")
    comp = ent["component"]
    cfg = comp.get("config", {})
    props = dict(cfg.get("properties") or {})
    if properties:
        props.update(properties)
    new_cfg = {"properties": props}
    if auto_terms is not None:
        new_cfg["autoTerminatedRelationships"] = auto_terms
    if scheduling_period:
        new_cfg["schedulingPeriod"] = scheduling_period
    payload = {
        "revision": {"clientId": CLIENT_ID, "version": ent["revision"]["version"]},
        "component": {"id": proc_id, "name": comp["name"], "config": new_cfg},
    }
    return nifi("PUT", f"/nifi-api/processors/{proc_id}", payload)


def create_processor(kind, name, proc_type, x, y, properties=None, auto_terms=None, scheduling_period="0 sec"):
    existing = processors_by_name(kind).get(name)
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
    return nifi("POST", f"/nifi-api/process-groups/{pg_id(kind)}/processors", payload)["id"]


def create_connection(kind, source_id, source_name, dest_id, dest_name, relationships):
    relationships = sorted(relationships)
    for c in connections(kind):
        comp = c["component"]
        if comp["source"]["id"] == source_id and comp["destination"]["id"] == dest_id and sorted(comp.get("selectedRelationships", [])) == relationships:
            return c["id"]
    payload = {
        "revision": {"clientId": CLIENT_ID, "version": 0},
        "component": {
            "parentGroupId": pg_id(kind),
            "source": {"id": source_id, "type": "PROCESSOR", "groupId": pg_id(kind), "name": source_name},
            "destination": {"id": dest_id, "type": "PROCESSOR", "groupId": pg_id(kind), "name": dest_name},
            "selectedRelationships": relationships,
            "flowFileExpiration": "0 sec",
            "backPressureObjectThreshold": 10000,
            "backPressureDataSizeThreshold": "1 GB",
        },
    }
    return nifi("POST", f"/nifi-api/process-groups/{pg_id(kind)}/connections", payload)["id"]


def existing_error_destination(kind):
    suffix = "__error_out"
    for c in connections(kind):
        dest = c["component"]["destination"]
        if dest["name"].endswith(suffix):
            return dest
    raise RuntimeError(f"No error output destination found for {kind}")


def create_connection_to_destination(kind, source_id, source_name, destination, relationships):
    relationships = sorted(relationships)
    for c in connections(kind):
        comp = c["component"]
        if comp["source"]["id"] == source_id and comp["destination"]["id"] == destination["id"] and sorted(comp.get("selectedRelationships", [])) == relationships:
            return c["id"]
    payload = {
        "revision": {"clientId": CLIENT_ID, "version": 0},
        "component": {
            "parentGroupId": pg_id(kind),
            "source": {"id": source_id, "type": "PROCESSOR", "groupId": pg_id(kind), "name": source_name},
            "destination": {
                "id": destination["id"],
                "type": destination.get("type", "OUTPUT_PORT"),
                "groupId": pg_id(kind),
                "name": destination["name"],
            },
            "selectedRelationships": relationships,
            "flowFileExpiration": "0 sec",
            "backPressureObjectThreshold": 10000,
            "backPressureDataSizeThreshold": "1 GB",
        },
    }
    return nifi("POST", f"/nifi-api/process-groups/{pg_id(kind)}/connections", payload)["id"]


def update_controller_service(service_id, properties):
    ent = nifi("GET", f"/nifi-api/controller-services/{service_id}")
    comp = ent["component"]
    props = dict(comp.get("properties") or {})
    props.update(properties)
    payload = {
        "revision": {"clientId": CLIENT_ID, "version": ent["revision"]["version"]},
        "component": {"id": service_id, "name": comp["name"], "properties": props},
    }
    return nifi("PUT", f"/nifi-api/controller-services/{service_id}", payload)


def enable_controller_service(service_id):
    ent = nifi("GET", f"/nifi-api/controller-services/{service_id}")
    if ent["component"].get("state") == "ENABLED":
        return
    payload = {"revision": {"clientId": CLIENT_ID, "version": ent["revision"]["version"]}, "state": "ENABLED"}
    nifi("PUT", f"/nifi-api/controller-services/{service_id}/run-status", payload)


def create_controller_service(kind, name, service_type, properties):
    existing = controller_services_by_name(kind).get(name)
    if existing:
        sid = existing["id"]
        state = existing["component"].get("state")
        if state == "ENABLED":
            ent = nifi("GET", f"/nifi-api/controller-services/{sid}")
            payload = {"revision": {"clientId": CLIENT_ID, "version": ent["revision"]["version"]}, "state": "DISABLED"}
            nifi("PUT", f"/nifi-api/controller-services/{sid}/run-status", payload)
            time.sleep(1)
        update_controller_service(sid, properties)
        enable_controller_service(sid)
        return sid
    payload = {
        "revision": {"clientId": CLIENT_ID, "version": 0},
        "component": {
            "name": name,
            "type": service_type,
            "properties": properties,
        },
    }
    sid = nifi("POST", f"/nifi-api/process-groups/{pg_id(kind)}/controller-services", payload)["id"]
    enable_controller_service(sid)
    return sid


def kafbat_session():
    if not KAFBAT_USER or not KAFBAT_PASSWORD:
        raise RuntimeError("Set KAFBAT_USER/KAFBAT_PASSWORD")
    s = requests.Session()
    s.verify = False
    s.post(f"{KAFBAT_BASE}/login", data={"username": KAFBAT_USER, "password": KAFBAT_PASSWORD}, timeout=20, allow_redirects=False)
    return s


def parse_sse(text):
    events = []
    for block in text.strip().split("\n\n"):
        payload = []
        for line in block.splitlines():
            if line.startswith("data:"):
                payload.append(line[5:].strip())
        if payload:
            try:
                events.append(json.loads("\n".join(payload)))
            except json.JSONDecodeError:
                pass
    return events


def fetch_topic_values(topic, limit=SAMPLE_LIMIT):
    s = kafbat_session()
    # This environment has one cluster named "local"; keep discovery simple and explicit.
    url = f"{KAFBAT_BASE}/api/clusters/local/topics/{urllib.parse.quote(topic, safe='')}/messages/v2"
    r = s.get(url, params={"mode": "LATEST", "limit": str(limit)}, timeout=60)
    r.raise_for_status()
    values = []
    for ev in parse_sse(r.text):
        if isinstance(ev, dict) and ev.get("type") == "MESSAGE" and isinstance(ev.get("message"), dict):
            val = ev["message"].get("value")
            if val:
                values.append(val)
    return values


def safe_field_name(name):
    name = str(name)
    name = re.sub(r"[^A-Za-z0-9_]", "_", name)
    if not name or not re.match(r"[A-Za-z_]", name[0]):
        name = f"f_{name}"
    return name


def json_string(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def normalize_json(value, depth=0):
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if depth >= MAX_DEPTH:
        return json_string(value)
    if isinstance(value, list):
        return [normalize_json(v, depth + 1) for v in value]
    if isinstance(value, dict):
        out = OrderedDict()
        used = set()
        for k, v in value.items():
            fk = safe_field_name(k)
            base = fk
            idx = 2
            while fk in used:
                fk = f"{base}_{idx}"
                idx += 1
            used.add(fk)
            out[fk] = normalize_json(v, depth + 1)
        return out
    return str(value)


def xml_to_obj(elem, depth=0):
    if depth >= MAX_DEPTH:
        return ET.tostring(elem, encoding="unicode")
    obj = OrderedDict()
    used = set()

    for k, v in elem.attrib.items():
        fk = safe_field_name(f"attr_{k}")
        used.add(fk)
        obj[fk] = v

    children_by_name = OrderedDict()
    for child in list(elem):
        ck = safe_field_name(child.tag)
        children_by_name.setdefault(ck, []).append(xml_to_obj(child, depth + 1))

    for ck, vals in children_by_name.items():
        fk = ck
        base = fk
        idx = 2
        while fk in used:
            fk = f"{base}_{idx}"
            idx += 1
        used.add(fk)
        obj[fk] = vals if len(vals) > 1 else vals[0]

    text = (elem.text or "").strip()
    if text:
        if obj:
            obj["text"] = text
        else:
            return text
    return obj


def normalize_xml(text):
    return xml_to_obj(ET.fromstring(text), 0)


class TypeNode:
    def __init__(self):
        self.null = False
        self.bool = False
        self.int = False
        self.long = False
        self.double = False
        self.string = False
        self.records = OrderedDict()
        self.array = None

    def add(self, value):
        if value is None:
            self.null = True
        elif isinstance(value, bool):
            self.bool = True
        elif isinstance(value, int) and not isinstance(value, bool):
            if -(2**31) <= value <= 2**31 - 1:
                self.int = True
            else:
                self.long = True
        elif isinstance(value, float):
            self.double = True
        elif isinstance(value, str):
            self.string = True
        elif isinstance(value, list):
            if self.array is None:
                self.array = TypeNode()
            if not value:
                self.array.string = True
            for item in value:
                self.array.add(item)
        elif isinstance(value, dict):
            for k, v in value.items():
                self.records.setdefault(k, TypeNode()).add(v)
        else:
            self.string = True


def record_schema_from_samples(samples, name, namespace):
    root = TypeNode()
    for s in samples:
        root.add(s)
    if not root.records:
        raise RuntimeError(f"No object fields inferred for {name}")
    return {
        "type": "record",
        "name": safe_field_name(name),
        "namespace": namespace,
        "fields": fields_from_record(root.records, safe_field_name(name), namespace, 1),
    }


def type_from_node(node, parent_name, namespace, depth):
    scalar_types = []
    if node.bool:
        scalar_types.append("boolean")
    if node.double:
        scalar_types.append("double")
    elif node.long:
        scalar_types.append("long")
    elif node.int:
        scalar_types.append("int")
    if node.string:
        scalar_types.append("string")

    complex_count = int(bool(node.records)) + int(bool(node.array))
    if len(scalar_types) + complex_count > 1:
        return "string"
    if node.array:
        return {"type": "array", "items": type_from_node(node.array, f"{parent_name}_item", namespace, depth + 1)}
    if node.records:
        if depth >= MAX_DEPTH:
            return "string"
        return {
            "type": "record",
            "name": safe_field_name(parent_name),
            "namespace": namespace,
            "fields": fields_from_record(node.records, parent_name, namespace, depth + 1),
        }
    if scalar_types:
        if len(set(scalar_types)) > 1:
            if any(t == "string" for t in scalar_types):
                return "string"
            if any(t == "double" for t in scalar_types):
                return "double"
            if any(t == "long" for t in scalar_types):
                return "long"
        return scalar_types[0]
    return "string"


def fields_from_record(record_nodes, parent_name, namespace, depth):
    fields = []
    for k, node in record_nodes.items():
        avro_type = type_from_node(node, f"{parent_name}_{k}", namespace, depth)
        fields.append({"name": safe_field_name(k), "type": ["null", avro_type], "default": None})
    return fields


def register_schema(subject, schema):
    payload = {"schema": json.dumps(schema, separators=(",", ":")), "schemaType": "AVRO"}
    url = f"{APICURIO_CCOMPAT}/subjects/{urllib.parse.quote(subject, safe='')}/versions"
    r = requests.post(url, json=payload, verify=False, timeout=30)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Register schema failed for {subject}: HTTP {r.status_code}: {r.text[:1000]}")
    check = requests.get(f"{APICURIO_CCOMPAT}/subjects/{urllib.parse.quote(subject, safe='')}/versions/latest", verify=False, timeout=30)
    if check.status_code != 200:
        raise RuntimeError(f"Registered schema not fetchable for {subject}: HTTP {check.status_code}: {check.text[:500]}")
    return check.json()


XML_TO_JSON_SCRIPT = r'''
import groovy.json.JsonOutput
import groovy.xml.XmlParser
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
def toJsonString = { value -> JsonOutput.toJson(value) }

def normalize
normalize = { node, int depth ->
    if (depth >= maxDepth) {
        return groovy.xml.XmlUtil.serialize(node)
    }
    def out = new LinkedHashMap()
    def used = new LinkedHashSet()
    node.attributes().each { k, v ->
        def fk = safeName('attr_' + k.toString())
        used.add(fk)
        out.put(fk, v == null ? null : v.toString())
    }
    def grouped = new LinkedHashMap()
    node.children().findAll { it instanceof Node }.each { child ->
        def ck = safeName(child.name().toString())
        if (!grouped.containsKey(ck)) grouped.put(ck, [])
        grouped.get(ck) << normalize(child, depth + 1)
    }
    grouped.each { k, vals ->
        def fk = k
        def base = fk
        def idx = 2
        while (used.contains(fk)) {
            fk = base + '_' + idx
            idx++
        }
        used.add(fk)
        out.put(fk, vals.size() == 1 ? vals[0] : vals)
    }
    def text = node.text()?.trim()
    if (text) {
        if (out.isEmpty()) return text
        out.put('text', text)
    }
    return out
}

try {
    def textHolder = [value: '']
    session.read(flowFile, { inputStream ->
        textHolder.value = inputStream.getText('UTF-8')
    } as InputStreamCallback)
    def parser = new XmlParser(false, false)
    def root = parser.parseText(textHolder.value)
    def normalized = normalize(root, 0)
    flowFile = session.write(flowFile, { outputStream ->
        outputStream.write(JsonOutput.toJson(normalized).getBytes('UTF-8'))
    } as OutputStreamCallback)
    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    flowFile = session.putAttribute(flowFile, 'avro.normalize.error', e.message ?: e.toString())
    log.error('XML to Avro JSON normalization failed: ' + e.message, e)
    session.transfer(flowFile, REL_FAILURE)
}
'''


JSON_NORMALIZE_SCRIPT = r'''
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
            while (used.contains(fk)) {
                fk = base + '_' + idx
                idx++
            }
            used.add(fk)
            out.put(fk, normalize(v, depth + 1))
        }
        return out
    }
    return value.toString()
}

try {
    def textHolder = [value: '']
    session.read(flowFile, { inputStream ->
        textHolder.value = inputStream.getText('UTF-8')
    } as InputStreamCallback)
    def parsed = new JsonSlurper().parseText(textHolder.value)
    def normalized = normalize(parsed, 0)
    flowFile = session.write(flowFile, { outputStream ->
        outputStream.write(JsonOutput.toJson(normalized).getBytes('UTF-8'))
    } as OutputStreamCallback)
    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    flowFile = session.putAttribute(flowFile, 'avro.normalize.error', e.message ?: e.toString())
    log.error('JSON Avro normalization failed: ' + e.message, e)
    session.transfer(flowFile, REL_FAILURE)
}
'''


def infer_and_register_all():
    os.makedirs("generated_schemas", exist_ok=True)
    result = {}
    for ent in ENTITIES:
        values = fetch_topic_values(ent["raw_topic"])
        if not values:
            raise RuntimeError(f"No Kafka samples found for {ent['raw_topic']}")
        normalized = []
        for val in values:
            if ent["source_format"] == "xml":
                normalized.append(normalize_xml(val))
            else:
                normalized.append(normalize_json(json.loads(val)))
        schema = record_schema_from_samples(normalized, ent["record_name"], ent["namespace"])
        subject = f"{ent['avro_topic']}-value"
        path = os.path.join("generated_schemas", f"{subject}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(schema, f, indent=2, ensure_ascii=False)
        reg = register_schema(subject, schema)
        result[ent["key"]] = {
            "raw_topic": ent["raw_topic"],
            "avro_topic": ent["avro_topic"],
            "subject": subject,
            "samples": len(values),
            "schema_file": path,
            "schema_id": reg.get("id"),
            "schema_version": reg.get("version"),
            "field_count": len(schema.get("fields", [])),
        }
    return result


def add_nifi_branches():
    results = {}
    for kind in ["forti", "s1"]:
        for p in processors_by_name(kind).values():
            name = p["component"]["name"]
            if name.startswith("fortisiem.") or name.startswith("sentinelone."):
                stop_processor(p["id"])

    for ent in ENTITIES:
        kind = ent["pg"]
        subject = f"{ent['avro_topic']}-value"
        reader_id = create_controller_service(
            kind,
            ent["reader"],
            "org.apache.nifi.json.JsonTreeReader",
            {
                "Schema Access Strategy": "schema-name",
                "Schema Registry": SCHEMA_REGISTRY_SERVICE_ID,
                "Schema Name": subject,
                "Schema Version": None,
                "Schema Branch": None,
                "Schema Text": "${avro.schema}",
                "Schema Reference Reader": None,
                "Schema Inference Cache": None,
                "Starting Field Strategy": "ROOT_NODE",
                "Starting Field Name": None,
                "Schema Application Strategy": "SELECTED_PART",
            },
        )
        writer_id = create_controller_service(
            kind,
            ent["writer"],
            "org.apache.nifi.avro.AvroRecordSetWriter",
            {
                "Schema Write Strategy": "schema-reference-writer",
                "Schema Reference Writer": SCHEMA_REF_WRITER_SERVICE_ID,
                "Schema Access Strategy": "schema-name",
                "Schema Registry": SCHEMA_REGISTRY_SERVICE_ID,
                "Schema Name": subject,
                "Schema Version": None,
                "Schema Branch": None,
                "Schema Text": "${avro.schema}",
                "Schema Reference Reader": None,
            },
        )
        script = XML_TO_JSON_SCRIPT if ent["source_format"] == "xml" else JSON_NORMALIZE_SCRIPT
        normalizer_id = create_processor(
            kind,
            ent["normalizer"],
            "org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
            ent["x"],
            ent["y"],
            {"Script Body": script},
            [],
        )
        publisher_id = create_processor(
            kind,
            ent["publisher"],
            "org.apache.nifi.kafka.processors.PublishKafka",
            ent["x"] + 330,
            ent["y"],
            {
                "Kafka Connection Service": KAFKA_SERVICE_ID,
                "Topic Name": ent["avro_topic"],
                "Failure Strategy": "Route to Failure",
                "acks": "all",
                "compression.type": "gzip",
                "max.request.size": "500 MB",
                "Transactions Enabled": "false",
                "Publish Strategy": "USE_VALUE",
                "Record Reader": reader_id,
                "Record Writer": writer_id,
                "Message Key Field": None,
                "Kafka Key": "${source_object_id}",
                "Kafka Key Attribute Encoding": "utf-8",
                "FlowFile Attribute Header Pattern": STANDARD_HEADER_PATTERN,
                "Header Encoding": "UTF-8",
                "Record Metadata Strategy": "FROM_PROPERTIES",
            },
            ["success"],
        )
        procs = processors_by_name(kind)
        branch = procs[ent["branch_source"]]
        error_dest = existing_error_destination(kind)
        create_connection(kind, branch["id"], ent["branch_source"], normalizer_id, ent["normalizer"], [ent["branch_rel"]])
        create_connection(kind, normalizer_id, ent["normalizer"], publisher_id, ent["publisher"], ["success"])
        create_connection_to_destination(kind, normalizer_id, ent["normalizer"], error_dest, ["failure"])
        create_connection_to_destination(kind, publisher_id, ent["publisher"], error_dest, ["failure"])
        results[ent["key"]] = {
            "reader": reader_id,
            "writer": writer_id,
            "normalizer": normalizer_id,
            "publisher": publisher_id,
            "avro_topic": ent["avro_topic"],
            "subject": subject,
        }
    return results


def inspect_nifi():
    out = {}
    for ent in ENTITIES:
        kind = ent["pg"]
        procs = processors_by_name(kind)
        p = procs.get(ent["publisher"])
        n = procs.get(ent["normalizer"])
        item = {}
        for label, proc in [("normalizer", n), ("publisher", p)]:
            if proc:
                full = nifi("GET", f"/nifi-api/processors/{proc['id']}")
                item[label] = {
                    "id": proc["id"],
                    "state": full["component"].get("state"),
                    "validation": full["component"].get("validationStatus"),
                    "validation_errors": full["component"].get("validationErrors"),
                }
        out[ent["key"]] = item
    return out


def main():
    cmd = os.environ.get("CMD") or (os.sys.argv[1] if len(os.sys.argv) > 1 else "plan")
    if cmd == "infer-register":
        print(json.dumps(infer_and_register_all(), indent=2))
    elif cmd == "apply":
        schemas = infer_and_register_all()
        branches = add_nifi_branches()
        time.sleep(2)
        print(json.dumps({"schemas": schemas, "branches": branches, "inspect": inspect_nifi()}, indent=2))
    elif cmd == "inspect":
        print(json.dumps(inspect_nifi(), indent=2))
    else:
        print(json.dumps({"entities": ENTITIES, "max_depth": MAX_DEPTH, "sample_limit": SAMPLE_LIMIT}, indent=2))


if __name__ == "__main__":
    main()
