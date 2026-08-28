import json
import os
import re
import subprocess
import sys
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

PARENT_PG_ID = os.environ.get("FORTISIEM_PARENT_PG_ID", "0a00e822-01a0-1000-68b7-f28e69779c95")
REFERENCE_FORTI_PG_ID = os.environ.get("FORTISIEM_REFERENCE_PG_ID", "77227d7a-2d8a-323d-02b2-5f7aec5ea246")
REFERENCE_PARAM_CONTEXT_ID = os.environ.get("FORTISIEM_PARAM_CONTEXT_ID", "8ba8b844-0d8d-352f-95b1-74269b3a9f3d")
PG_NAME = os.environ.get("FORTISIEM_MAX_PG_NAME", "fortisiem.maximum_useful")

KAFKA_SERVICE_ID = os.environ.get("KAFKA_SERVICE_ID", "40675f79-8eaa-3193-8f8d-026c8c1ee947")
SCHEMA_REGISTRY_SERVICE_ID = os.environ.get("SCHEMA_REGISTRY_SERVICE_ID", "db86aea0-2bee-3687-9187-5679904d69b0")
SCHEMA_REF_WRITER_SERVICE_ID = os.environ.get("SCHEMA_REF_WRITER_SERVICE_ID", "2c59d8ad-103a-3e0e-fb8f-54726496f8b9")
DMC_SERVICE_ID = os.environ.get("FORTISIEM_DMC_SERVICE_ID", "8e5e7ac4-db45-3f55-3d1c-e39206498c35")

CLIENT_ID = "codex-fortisiem-maximum"
MAX_DEPTH = int(os.environ.get("SCHEMA_MAX_DEPTH", "5"))
SAMPLE_LIMIT = int(os.environ.get("SCHEMA_SAMPLE_LIMIT", "100"))

STANDARD_HEADER_PATTERN = (
    r"^(source_platform|customer_tenant_organization|source_object_type|"
    r"source_object_id|extraction_timestamp|source_event_update_timestamp|"
    r"api_endpoint_export_query_identity|cursor_window|payload_hash_fingerprint|"
    r"ingestion_run_batch_identity|ingest_ts)$"
)

STANDARD_VALUE_FIELDS = [
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
]

CORE_ENTITIES = [
    {"entity": "organization", "topic": "bronze.fortisiem.organization__raw", "format": "xml", "record": "fortisiem_organization_raw_avro", "source": "fortisiem.organization__raw__dedupe_hash"},
    {"entity": "device", "topic": "bronze.fortisiem.device__raw", "format": "xml", "record": "fortisiem_device_raw_avro", "source": "fortisiem.device__raw__dedupe_hash"},
    {"entity": "interface", "topic": "bronze.fortisiem.interface__raw", "format": "json", "record": "fortisiem_interface_raw_avro", "source": "fortisiem.interface__raw__dedupe_hash"},
    {"entity": "processor", "topic": "bronze.fortisiem.processor__raw", "format": "json", "record": "fortisiem_processor_raw_avro", "source": "fortisiem.processor__raw__dedupe_hash"},
    {"entity": "storage", "topic": "bronze.fortisiem.storage__raw", "format": "json", "record": "fortisiem_storage_raw_avro", "source": "fortisiem.storage__raw__dedupe_hash"},
    {"entity": "installed_software", "topic": "bronze.fortisiem.installed_software__raw", "format": "json", "record": "fortisiem_installed_software_raw_avro", "source": "fortisiem.installed_software__raw__dedupe_hash"},
    {"entity": "software_service", "topic": "bronze.fortisiem.software_service__raw", "format": "json", "record": "fortisiem_software_service_raw_avro", "source": "fortisiem.software_service__raw__dedupe_hash"},
    {"entity": "software_patch", "topic": "bronze.fortisiem.software_patch__raw", "format": "json", "record": "fortisiem_software_patch_raw_avro", "source": "fortisiem.software_patch__raw__dedupe_hash"},
    {"entity": "device_custom_property", "topic": "bronze.fortisiem.device_custom_property__raw", "format": "json", "record": "fortisiem_device_custom_property_raw_avro", "source": "fortisiem.device_custom_property__raw__dedupe_hash"},
    {"entity": "device_business_service_membership", "topic": "bronze.fortisiem.device_business_service_membership__raw", "format": "json", "record": "fortisiem_device_business_service_membership_raw_avro", "source": "fortisiem.device_business_service_membership__raw__dedupe_hash"},
]

CHILD_EXTRACTS = {
    "interface": {"section": "interfaces", "child": "networkinterface", "api": "device detail /interfaces"},
    "processor": {"section": "processors", "child": "processor", "api": "device detail /processors"},
    "storage": {"section": "storages", "child": "storage", "api": "device detail /storages"},
    "installed_software": {"section": "applications", "child": "application", "api": "device detail /applications"},
    "software_service": {"section": "softwareServices", "child": "softwareservice", "api": "device detail /softwareServices"},
    "software_patch": {"section": "softwarePatches", "child": "softwarepatch", "api": "device detail /softwarePatches"},
    "device_custom_property": {"section": "properties", "child": "__SELF__", "api": "device detail /properties"},
    "device_business_service_membership": {"section": "appGroupName", "child": "__SELF__", "api": "device detail /appGroupName"},
}

SCAFFOLD_ENTITIES = [
    ("monitored_device", "GET /deviceInfo/monitoredDevices"),
    ("agent_status", "GET /agentStatus/all?request=<orgId>,<hostName>"),
    ("discovery_task", "POST/GET /deviceMon/discover [gated; not auto-run]"),
    ("discovery_result", "GET /deviceMon/status?taskId=<taskId>"),
    ("device_monitoring_config", "GET/update device monitoring attributes [gated]"),
    ("device_maintenance", "POST /deviceMaint/update or delete [gated]"),
    ("event_query", "POST /query/eventQuery [bounded-query required]"),
    ("event_query_progress", "GET /query/progress/<requestId>,<expireTime>"),
    ("event", "GET /query/events/<queryId>/<begin>/<end> [bounded-window required]"),
    ("incident", "GET /pub/incident"),
    ("incident_triggering_event", "GET /pub/incident/triggeringEvents"),
    ("incident_attribute", "incident detail attributes"),
    ("lookup_table", "GET /pub/lookupTable"),
    ("lookup_table_item", "GET /pub/lookupTable/{id}/data"),
    ("watchlist", "GET /watchlist/all"),
    ("watchlist_entry", "GET /watchlist/{id}"),
    ("ip_watchlist", "watchlist typed IP entries"),
    ("domain_watchlist", "watchlist typed domain entries"),
    ("hash_watchlist", "watchlist typed hash entries"),
    ("event_worker", "GET /system/eventworker"),
    ("query_worker", "GET /system/queryworker"),
    ("system_health_summary", "GET /system/health/summary"),
    ("system_health_instance", "GET /system/health/instance?instanceId=<id>"),
    ("system_health_detail", "GET /system/health"),
    ("availability_event", "query-derived availability events"),
    ("system_performance_event", "query-derived system performance events"),
    ("process_performance_event", "query-derived process performance events"),
    ("interface_performance_event", "query-derived interface performance events"),
    ("network_traffic_event", "query-derived network traffic events"),
    ("case", "conditional/gated"),
    ("kubernetes_resource", "conditional/gated"),
    ("container_resource", "conditional/gated"),
    ("certificate", "conditional/gated"),
    ("authentication_event", "conditional/gated"),
    ("malware_indicator", "conditional/gated"),
]


def run_curl(args, input_text=None, timeout=90, attempts=3):
    # curl.exe intermittently dies with 0xC0000005 on Windows under sustained load;
    # the request itself is fine, so retry transport-level failures before giving up.
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


def get_flow(pg_id):
    return nifi("GET", f"/nifi-api/flow/process-groups/{pg_id}")["processGroupFlow"]["flow"]


def find_pg(parent_id, name):
    for pg in get_flow(parent_id).get("processGroups", []):
        if pg["component"]["name"] == name:
            return pg
    return None


def ensure_pg():
    existing = find_pg(PARENT_PG_ID, PG_NAME)
    if existing:
        return existing["id"]
    payload = {
        "revision": {"clientId": CLIENT_ID, "version": 0},
        "component": {
            "name": PG_NAME,
            "position": {"x": 2700.0, "y": 900.0},
            "parameterContext": {"id": REFERENCE_PARAM_CONTEXT_ID},
        },
    }
    created = nifi("POST", f"/nifi-api/process-groups/{PARENT_PG_ID}/process-groups", payload)
    return created["id"]


PG_ID = None


def pg_id():
    global PG_ID
    if PG_ID is None:
        PG_ID = ensure_pg()
    return PG_ID


def processors_by_name():
    return {p["component"]["name"]: p for p in get_flow(pg_id()).get("processors", [])}


def process_groups_by_name():
    return {p["component"]["name"]: p for p in get_flow(pg_id()).get("processGroups", [])}


def labels_by_text():
    return {l["component"]["label"]: l for l in get_flow(pg_id()).get("labels", [])}


def connections():
    return get_flow(pg_id()).get("connections", [])


def stop_processor(proc_id):
    ent = nifi("GET", f"/nifi-api/processors/{proc_id}")
    state = ent["component"].get("state")
    if state in ("STOPPED", "DISABLED"):
        return
    nifi("PUT", f"/nifi-api/processors/{proc_id}/run-status", {"revision": {"clientId": CLIENT_ID, "version": ent["revision"]["version"]}, "state": "STOPPED"})


def set_processor_state(proc_id, state):
    ent = nifi("GET", f"/nifi-api/processors/{proc_id}")
    if ent["component"].get("state") == state:
        return
    nifi("PUT", f"/nifi-api/processors/{proc_id}/run-status", {"revision": {"clientId": CLIENT_ID, "version": ent["revision"]["version"]}, "state": state})


def update_processor(proc_id, properties=None, auto_terms=None, scheduling_period=None, scheduling_strategy=None, run_duration_millis=None):
    ent = nifi("GET", f"/nifi-api/processors/{proc_id}")
    comp = ent["component"]
    cfg = comp.get("config", {})
    # NiFi returns sensitive properties masked as "********". Writing that back would
    # replace the real secret (or its #{PARAM} reference) with the literal mask, so drop
    # masked entries unless the caller explicitly supplies a new value. Properties omitted
    # from the PUT are left untouched by NiFi.
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
    if run_duration_millis is not None:
        new_cfg["runDurationMillis"] = run_duration_millis
    payload = {"revision": {"clientId": CLIENT_ID, "version": ent["revision"]["version"]}, "component": {"id": proc_id, "name": comp["name"], "config": new_cfg}}
    return nifi("PUT", f"/nifi-api/processors/{proc_id}", payload)


def create_processor(name, proc_type, x, y, properties=None, auto_terms=None, scheduling_period="0 sec", scheduling_strategy="TIMER_DRIVEN", run_duration_millis=0):
    existing = processors_by_name().get(name)
    if existing:
        update_processor(existing["id"], properties or {}, auto_terms, scheduling_period, scheduling_strategy, run_duration_millis)
        return existing["id"]
    payload = {
        "revision": {"clientId": CLIENT_ID, "version": 0},
        "component": {
            "name": name,
            "type": proc_type,
            "position": {"x": float(x), "y": float(y)},
            "config": {
                "schedulingStrategy": scheduling_strategy,
                "schedulingPeriod": scheduling_period,
                "executionNode": "ALL",
                "penaltyDuration": "30 sec",
                "yieldDuration": "1 sec",
                "bulletinLevel": "WARN",
                "runDurationMillis": run_duration_millis,
                "concurrentlySchedulableTaskCount": 1,
                "autoTerminatedRelationships": auto_terms or [],
                "properties": properties or {},
            },
        },
    }
    return nifi("POST", f"/nifi-api/process-groups/{pg_id()}/processors", payload)["id"]


def create_connection(source_id, source_name, dest_id, dest_name, relationships):
    relationships = sorted(relationships)
    for c in connections():
        comp = c["component"]
        if comp["source"]["id"] == source_id and comp["destination"]["id"] == dest_id and sorted(comp.get("selectedRelationships", [])) == relationships:
            return c["id"]
    payload = {
        "revision": {"clientId": CLIENT_ID, "version": 0},
        "component": {
            "parentGroupId": pg_id(),
            "source": {"id": source_id, "type": "PROCESSOR", "groupId": pg_id(), "name": source_name},
            "destination": {"id": dest_id, "type": "PROCESSOR", "groupId": pg_id(), "name": dest_name},
            "selectedRelationships": relationships,
            "flowFileExpiration": "0 sec",
            "backPressureObjectThreshold": 10000,
            "backPressureDataSizeThreshold": "1 GB",
        },
    }
    return nifi("POST", f"/nifi-api/process-groups/{pg_id()}/connections", payload)["id"]


def create_label(label, x, y, w=480, h=90):
    if label in labels_by_text():
        return labels_by_text()[label]["id"]
    payload = {"revision": {"clientId": CLIENT_ID, "version": 0}, "component": {"label": label, "position": {"x": float(x), "y": float(y)}, "width": float(w), "height": float(h), "style": {"font-size": "12px"}}}
    return nifi("POST", f"/nifi-api/process-groups/{pg_id()}/labels", payload)["id"]


def create_controller_service(name, service_type, properties):
    data = nifi("GET", f"/nifi-api/flow/process-groups/{pg_id()}/controller-services")
    for svc in data.get("controllerServices", []):
        if svc["component"]["name"] == name:
            sid = svc["id"]
            ent = nifi("GET", f"/nifi-api/controller-services/{sid}")
            if ent["component"].get("state") == "ENABLED":
                nifi("PUT", f"/nifi-api/controller-services/{sid}/run-status", {"revision": {"clientId": CLIENT_ID, "version": ent["revision"]["version"]}, "state": "DISABLED"})
                time.sleep(1)
                ent = nifi("GET", f"/nifi-api/controller-services/{sid}")
            nifi("PUT", f"/nifi-api/controller-services/{sid}", {"revision": {"clientId": CLIENT_ID, "version": ent["revision"]["version"]}, "component": {"id": sid, "name": name, "properties": properties}})
            enable_controller_service(sid)
            return sid
    payload = {"revision": {"clientId": CLIENT_ID, "version": 0}, "component": {"name": name, "type": service_type, "properties": properties}}
    sid = nifi("POST", f"/nifi-api/process-groups/{pg_id()}/controller-services", payload)["id"]
    enable_controller_service(sid)
    return sid


def enable_controller_service(sid):
    ent = nifi("GET", f"/nifi-api/controller-services/{sid}")
    if ent["component"].get("state") == "ENABLED":
        return
    nifi("PUT", f"/nifi-api/controller-services/{sid}/run-status", {"revision": {"clientId": CLIENT_ID, "version": ent["revision"]["version"]}, "state": "ENABLED"})


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

def prop = { name -> context.getProperty(name).evaluateAttributeExpressions(flowFile).getValue() }

def bytesOut = new ByteArrayOutputStream()
session.read(flowFile, { inputStream ->
    byte[] buffer = new byte[8192]
    int len
    while ((len = inputStream.read(buffer)) > -1) bytesOut.write(buffer, 0, len)
} as InputStreamCallback)
def bytes = bytesOut.toByteArray()
def hash = MessageDigest.getInstance('SHA-256').digest(bytes).encodeHex().toString()

def sourcePlatform = prop('SOURCE_PLATFORM') ?: 'fortisiem'
def entity = prop('SOURCE_OBJECT_TYPE')
def objectId = prop('OBJECT_ID')
if (!objectId || objectId.trim().length() == 0 || objectId.contains('${')) objectId = flowFile.getAttribute('uuid')
objectId = objectId.replaceAll('[^A-Za-z0-9_.:-]', '_')

def extractionTs = flowFile.getAttribute('extraction_timestamp')
if (!extractionTs || extractionTs.trim().length() == 0) extractionTs = Instant.now().toString()
def runId = flowFile.getAttribute('ingestion_run_batch_identity')
if (!runId || runId.trim().length() == 0) runId = extractionTs + '_' + flowFile.getAttribute('uuid')

def customerOrg = prop('CUSTOMER_TENANT_ORGANIZATION')
if (!customerOrg || customerOrg.contains('${')) customerOrg = flowFile.getAttribute('organization__attr_name') ?: flowFile.getAttribute('org_name') ?: ''
def sourceUpdateTs = prop('SOURCE_EVENT_UPDATE_TIMESTAMP')
if (!sourceUpdateTs || sourceUpdateTs.contains('${')) sourceUpdateTs = ''
def apiQuery = prop('API_ENDPOINT_EXPORT_QUERY_IDENTITY') ?: ''
def cursorWindow = prop('CURSOR_WINDOW') ?: ''

flowFile = session.putAttribute(flowFile, 'source_platform', sourcePlatform)
flowFile = session.putAttribute(flowFile, 'customer_tenant_organization', customerOrg)
flowFile = session.putAttribute(flowFile, 'source_object_type', entity)
flowFile = session.putAttribute(flowFile, 'source_object_id', objectId)
flowFile = session.putAttribute(flowFile, 'extraction_timestamp', extractionTs)
flowFile = session.putAttribute(flowFile, 'source_event_update_timestamp', sourceUpdateTs)
flowFile = session.putAttribute(flowFile, 'api_endpoint_export_query_identity', apiQuery)
flowFile = session.putAttribute(flowFile, 'cursor_window', cursorWindow)
flowFile = session.putAttribute(flowFile, 'payload_hash_fingerprint', hash)
flowFile = session.putAttribute(flowFile, 'ingestion_run_batch_identity', runId)

def namespace = prop('DEDUPE_NAMESPACE') ?: 'fortisiem_max'
def dedupeKey = "${namespace}:${sourcePlatform}:${entity}:${objectId}:${hash}"
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


CHILD_EXTRACT_SCRIPT = r'''
import groovy.json.JsonOutput
import java.security.MessageDigest
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.io.OutputStreamCallback

def flowFile = session.get()
if (!flowFile) return

def prop = { name -> context.getProperty(name).evaluateAttributeExpressions(flowFile).getValue() }
def sectionName = prop('SECTION')
def childName = prop('CHILD_TAG')
if (childName == '__SELF__') childName = ''
def entityName = prop('ENTITY_NAME')
def apiIdentity = prop('API_IDENTITY')

def textHolder = [value: '']
try {
    session.read(flowFile, { inputStream -> textHolder.value = inputStream.getText('UTF-8') } as InputStreamCallback)
    def sourceText = textHolder.value
    def candidates = []
    if (childName && childName.trim().length() > 0) {
        def sectionMatcher = (sourceText =~ /(?s)<${sectionName}(?:\s[^>]*)?>(.*?)<\/${sectionName}>/)
        while (sectionMatcher.find()) {
            def sectionBody = sectionMatcher.group(1)
            def childMatcher = (sectionBody =~ /(?s)<${childName}(?:\s[^>]*)?>.*?<\/${childName}>/)
            while (childMatcher.find()) {
                candidates << childMatcher.group(0)
            }
        }
    } else {
        def selfMatcher = (sourceText =~ /(?s)<${sectionName}(?:\s[^>]*)?>.*?<\/${sectionName}>/)
        while (selfMatcher.find()) {
            candidates << selfMatcher.group(0)
        }
    }

    def fieldMap = { String xml ->
        def out = new LinkedHashMap()
        def attrMatcher = (xml =~ /^<[^>\s]+([^>]*)>/)
        if (attrMatcher.find()) {
            def attrs = attrMatcher.group(1) ?: ''
            def pairMatcher = (attrs =~ /([A-Za-z0-9_:-]+)\s*=\s*["']([^"']*)["']/)
            pairMatcher.each { m -> out.put('attr_' + m[1].toString(), m[2].toString()) }
        }
        def inner = xml.replaceFirst(/(?s)^<[A-Za-z0-9_:-]+(?:\s[^>]*)?>/, '').replaceFirst(/(?s)<\/[A-Za-z0-9_:-]+>\s*$/, '')
        def tagMatcher = (inner =~ /(?s)<([A-Za-z0-9_:-]+)(?:\s[^>]*)?>(.*?)<\/\1>/)
        tagMatcher.each { m ->
            def k = m[1].toString()
            def val = m[2].toString()
            if (!(val =~ /(?s)<[A-Za-z0-9_:-]+(?:\s[^>]*)?>/)) {
                out.put(k, val)
            }
        }
        if (out.isEmpty()) {
            def textOnly = inner.replaceAll(/(?s)<[^>]+>/, '').trim()
            if (textOnly) out.put('text', textOnly)
        }
        return out
    }

    def parent = [
        org_name: flowFile.getAttribute('org_name') ?: flowFile.getAttribute('organization__attr_name') ?: '',
        organization_id: flowFile.getAttribute('organization__attr_id') ?: '',
        natural_id: flowFile.getAttribute('naturalId') ?: '',
        access_ip: flowFile.getAttribute('access_ip') ?: '',
        host_name: flowFile.getAttribute('host_name') ?: ''
    ]
    int idx = 0
    candidates.each { String rawXml ->
        idx++
        def fields = fieldMap(rawXml)
        def keyParts = []
        ['name','displayName','description','type','macAddr','ipv4Addr','path','installTime','count','text'].each { k ->
            if (fields[k] != null && fields[k].toString().trim()) keyParts << fields[k].toString().trim()
        }
        def keySeed = (keyParts ? keyParts.join('|') : rawXml)
        def digest = MessageDigest.getInstance('SHA-256').digest(keySeed.getBytes('UTF-8')).encodeHex().toString().take(16)
        def parentId = parent.natural_id ?: parent.access_ip ?: parent.host_name ?: flowFile.getAttribute('uuid')
        def childId = (parent.org_name + ':' + parentId + ':' + entityName + ':' + digest).replaceAll('[^A-Za-z0-9_.:-]', '_')
        def body = [
            source_parent: parent,
            source_child_tag: childName ?: sectionName,
            source_section: sectionName,
            api_endpoint_export_query_identity: apiIdentity,
            fields: fields,
            raw_xml: rawXml
        ]
        def outFile = session.create(flowFile)
        outFile = session.write(outFile, { os -> os.write(JsonOutput.toJson(body).getBytes('UTF-8')) } as OutputStreamCallback)
        outFile = session.putAttribute(outFile, 'child_object_id', childId)
        outFile = session.putAttribute(outFile, 'child_index', idx.toString())
        outFile = session.putAttribute(outFile, 'child_section', sectionName)
        outFile = session.putAttribute(outFile, 'child_tag', childName ?: sectionName)
        session.transfer(outFile, REL_SUCCESS)
    }
    session.remove(flowFile)
} catch (Exception e) {
    flowFile = session.putAttribute(flowFile, 'child.extract.error', e.message ?: e.toString())
    log.error('FortiSIEM child extraction failed: ' + e.message, e)
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
        'extraction_timestamp',
        'source_event_update_timestamp',
        'api_endpoint_export_query_identity',
        'cursor_window',
        'payload_hash_fingerprint',
        'ingestion_run_batch_identity'
    ].each { k ->
        out.put(k, flowFile.getAttribute(k) ?: '')
    }
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


XML_TO_JSON_SCRIPT = r'''
import groovy.json.JsonOutput
import groovy.xml.XmlParser
import groovy.xml.XmlUtil
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
normalize = { node, int depth ->
    if (depth >= maxDepth) return XmlUtil.serialize(node)
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
        while (used.contains(fk)) { fk = base + '_' + idx; idx++ }
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
def withStandardMetadata = { normalized ->
    def out = new LinkedHashMap()
    [
        'source_platform',
        'customer_tenant_organization',
        'source_object_type',
        'source_object_id',
        'extraction_timestamp',
        'source_event_update_timestamp',
        'api_endpoint_export_query_identity',
        'cursor_window',
        'payload_hash_fingerprint',
        'ingestion_run_batch_identity'
    ].each { k ->
        out.put(k, flowFile.getAttribute(k) ?: '')
    }
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
    def root = new XmlParser(false, false).parseText(textHolder.value)
    def normalized = withStandardMetadata(normalize(root, 0))
    flowFile = session.write(flowFile, { outputStream -> outputStream.write(JsonOutput.toJson(normalized).getBytes('UTF-8')) } as OutputStreamCallback)
    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    flowFile = session.putAttribute(flowFile, 'avro.normalize.error', e.message ?: e.toString())
    log.error('XML to Avro JSON normalization failed: ' + e.message, e)
    session.transfer(flowFile, REL_FAILURE)
}
'''


def invoke_props(url):
    return {
        "HTTP Method": "GET",
        "HTTP URL": url,
        "HTTP/2 Disabled": "True",
        "SSL Context Service": None,
        "Connection Timeout": "5 secs",
        "Socket Read Timeout": "30 secs",
        "Socket Write Timeout": "15 secs",
        "Socket Idle Timeout": "5 mins",
        "Socket Idle Connections": "5",
        "Proxy Configuration Service": None,
        "Request Username": "#{HTTP_USERNAME}",
        "Request Password": "#{HTTP_PASSWORD}",
        "Request Digest Authentication Enabled": "false",
        "Request Failure Penalization Enabled": "true",
        "Request Body Enabled": "false",
        "Request Content-Encoding": "DISABLED",
        "Request Content-Type": "${mime.type}",
        "Request Date Header Enabled": "True",
        "Response Body Ignored": "false",
        "Response Cache Enabled": "false",
        "Response Cookie Strategy": "DISABLED",
        "Response Generation Required": "false",
        "Response FlowFile Naming Strategy": "RANDOM",
        "Response Header Request Attributes Enabled": "false",
        "Response Redirects Enabled": "True",
    }


def publish_props(topic, avro=False, reader_id=None, writer_id=None):
    props = {
        "Kafka Connection Service": KAFKA_SERVICE_ID,
        "Topic Name": topic,
        "Failure Strategy": "Route to Failure",
        "acks": "all",
        "compression.type": "gzip" if avro else "none",
        "max.request.size": "500 MB",
        "Transactions Enabled": "false",
        "Publish Strategy": "USE_VALUE",
        "Message Key Field": None,
        "Kafka Key": "${source_object_id}",
        "Kafka Key Attribute Encoding": "utf-8",
        "FlowFile Attribute Header Pattern": STANDARD_HEADER_PATTERN,
        "Header Encoding": "UTF-8",
        "Record Metadata Strategy": "FROM_PROPERTIES",
    }
    if avro:
        props["Record Reader"] = reader_id
        props["Record Writer"] = writer_id
    else:
        props["Record Reader"] = None
        props["Record Writer"] = None
    return props


def raw_hash(name, x, y, entity, object_id_expr, api_identity, customer_expr="${org_name:ifEmpty(${organization__attr_name})}"):
    return create_processor(
        name,
        "org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
        x,
        y,
        {
            "Script Body": RAW_HASH_SCRIPT,
            "SOURCE_PLATFORM": "fortisiem",
            "SOURCE_OBJECT_TYPE": entity,
            "OBJECT_ID": object_id_expr,
            "CUSTOMER_TENANT_ORGANIZATION": customer_expr,
            "SOURCE_EVENT_UPDATE_TIMESTAMP": "${literal('')}",
            "API_ENDPOINT_EXPORT_QUERY_IDENTITY": api_identity,
            "CURSOR_WINDOW": "${literal('')}",
            "DMC_SERVICE_ID": DMC_SERVICE_ID,
            "DEDUPE_NAMESPACE": "fortisiem_max_v9",
        },
        ["failure"],
    )


def build_raw():
    global DMC_SERVICE_ID
    pg_id()
    for p in processors_by_name().values():
        stop_processor(p["id"])
    DMC_SERVICE_ID = create_controller_service(
        "fortisiem.maximum__dedupe__cache",
        "org.apache.nifi.redis.service.RedisDistributedMapCacheClientService",
        {"Redis Connection Pool": "b90bcbdb-d69c-3725-51d1-444dd57b9336", "TTL": "24 hours"},
    )

    create_label("FortiSIEM maximum-useful ingestion. Enabled source branch: organization -> device list -> device detail -> detailed child entities. Higher-risk/query entities are scaffolded as notes only until bounded parameters are provided.", -450, -300, 900, 120)

    trigger = create_processor(
        "fortisiem.maximum__trigger",
        "org.apache.nifi.processors.standard.GenerateFlowFile",
        -380,
        0,
        {"File Size": "0B", "Batch Size": "1", "Data Format": "Text", "Unique FlowFiles": "false", "Custom Text": "fortisiem-maximum-trigger"},
        [],
        scheduling_period="6 hours",
    )
    run_meta = create_processor(
        "fortisiem.maximum__run_metadata",
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        -80,
        0,
        {
            "Delete Attributes Expression": None,
            "Store State": "Do not store state",
            "extraction_timestamp": "${now():format(\"yyyy-MM-dd'T'HH:mm:ss.SSSXXX\")}",
            "ingestion_run_batch_identity": "fortisiem-max-${now():toNumber()}-${uuid}",
        },
        [],
    )
    org_fetch = create_processor("fortisiem.maximum__list_organizations__fetch", "org.apache.nifi.processors.standard.InvokeHTTP", 220, 0, invoke_props("#{SOURCE_API_BASE}/config/Domain"), ["Original", "Failure", "Retry", "No Retry"])
    org_split = create_processor("fortisiem.maximum__list_organizations__split", "org.apache.nifi.processors.standard.SplitXml", 520, 0, {"Split Depth": "3"}, ["original", "failure"])
    org_extract = create_processor(
        "fortisiem.maximum__list_organizations__extract",
        "org.apache.nifi.processors.standard.EvaluateXPath",
        820,
        0,
        {"Destination": "flowfile-attribute", "Return Type": "auto-detect", "Allow DTD": "false", "org_name": "/domain/name/text()", "org_id": "/domain/@id", "Path Not Found Behavior": "ignore"},
        ["failure", "unmatched"],
    )
    org_hash = raw_hash("fortisiem.organization__raw__dedupe_hash", 1120, -160, "organization", "${org_name}", "GET #{SOURCE_API_BASE}/config/Domain", "${org_name}")
    org_pub = create_processor("fortisiem.organization__raw__publish", "org.apache.nifi.kafka.processors.PublishKafka", 1450, -160, publish_props("bronze.fortisiem.organization__raw"), ["success", "failure"])

    list_devices = create_processor("fortisiem.maximum__list_devices__fetch", "org.apache.nifi.processors.standard.InvokeHTTP", 1450, 120, invoke_props("#{SOURCE_API_BASE}/cmdbDeviceInfo/devices?organization=${org_name}"), ["Original", "Failure", "Retry", "No Retry"])
    device_split = create_processor("fortisiem.maximum__list_devices__split", "org.apache.nifi.processors.standard.SplitXml", 1750, 120, {"Split Depth": "1"}, ["original", "failure"])
    device_extract = create_processor(
        "fortisiem.maximum__list_devices__extract_key",
        "org.apache.nifi.processors.standard.EvaluateXPath",
        2050,
        120,
        {
            "Destination": "flowfile-attribute",
            "Return Type": "auto-detect",
            "Allow DTD": "false",
            "access_ip": "/device/accessIp/text()",
            "naturalId": "/device/naturalId/text()",
            "host_name": "/device/name/text()",
            "organization__attr_id": "/device/organization/@id",
            "organization__attr_name": "/device/organization/@name",
            "Path Not Found Behavior": "ignore",
        },
        ["failure", "unmatched"],
    )
    rate = create_processor(
        "fortisiem.maximum__device_detail__rate_limit",
        "org.apache.nifi.processors.standard.ControlRate",
        2350,
        120,
        {"Rate Control Criteria": "flowfile count", "Maximum Rate": "3", "Time Duration": "1 sec", "Grouping Attribute Name": None},
        ["failure"],
    )
    detail_fetch = create_processor("fortisiem.maximum__get_device_detail__fetch", "org.apache.nifi.processors.standard.InvokeHTTP", 2650, 120, invoke_props("#{SOURCE_API_BASE}/cmdbDeviceInfo/device?organization=${org_name}&ip=${access_ip}&loadDepend=true"), ["Original", "Failure", "Retry", "No Retry"])
    detail_extract = create_processor(
        "fortisiem.maximum__device_detail__extract_key",
        "org.apache.nifi.processors.standard.EvaluateXPath",
        2950,
        120,
        {
            "Destination": "flowfile-attribute",
            "Return Type": "auto-detect",
            "Allow DTD": "false",
            "access_ip": "/device/accessIp/text()",
            "naturalId": "/device/naturalId/text()",
            "host_name": "/device/name/text()",
            "organization__attr_id": "/device/organization/@id",
            "organization__attr_name": "/device/organization/@name",
            "Path Not Found Behavior": "ignore",
        },
        ["failure", "unmatched"],
    )
    device_hash = raw_hash("fortisiem.device__raw__dedupe_hash", 3250, 120, "device", "${naturalId}_${organization__attr_id}_${access_ip}", "GET #{SOURCE_API_BASE}/cmdbDeviceInfo/device?organization=${org_name}&ip=${access_ip}&loadDepend=true", "${org_name}")
    device_pub = create_processor("fortisiem.device__raw__publish", "org.apache.nifi.kafka.processors.PublishKafka", 3580, 120, publish_props("bronze.fortisiem.device__raw"), ["success", "failure"])

    create_connection(trigger, "fortisiem.maximum__trigger", run_meta, "fortisiem.maximum__run_metadata", ["success"])
    create_connection(run_meta, "fortisiem.maximum__run_metadata", org_fetch, "fortisiem.maximum__list_organizations__fetch", ["success"])
    create_connection(org_fetch, "fortisiem.maximum__list_organizations__fetch", org_split, "fortisiem.maximum__list_organizations__split", ["Response"])
    create_connection(org_split, "fortisiem.maximum__list_organizations__split", org_extract, "fortisiem.maximum__list_organizations__extract", ["split"])
    create_connection(org_extract, "fortisiem.maximum__list_organizations__extract", org_hash, "fortisiem.organization__raw__dedupe_hash", ["matched"])
    create_connection(org_hash, "fortisiem.organization__raw__dedupe_hash", org_pub, "fortisiem.organization__raw__publish", ["success"])
    create_connection(org_hash, "fortisiem.organization__raw__dedupe_hash", list_devices, "fortisiem.maximum__list_devices__fetch", ["success"])
    create_connection(list_devices, "fortisiem.maximum__list_devices__fetch", device_split, "fortisiem.maximum__list_devices__split", ["Response"])
    create_connection(device_split, "fortisiem.maximum__list_devices__split", device_extract, "fortisiem.maximum__list_devices__extract_key", ["split"])
    create_connection(device_extract, "fortisiem.maximum__list_devices__extract_key", rate, "fortisiem.maximum__device_detail__rate_limit", ["matched"])
    create_connection(rate, "fortisiem.maximum__device_detail__rate_limit", detail_fetch, "fortisiem.maximum__get_device_detail__fetch", ["success"])
    create_connection(detail_fetch, "fortisiem.maximum__get_device_detail__fetch", detail_extract, "fortisiem.maximum__device_detail__extract_key", ["Response"])
    create_connection(detail_extract, "fortisiem.maximum__device_detail__extract_key", device_hash, "fortisiem.device__raw__dedupe_hash", ["matched"])
    create_connection(device_hash, "fortisiem.device__raw__dedupe_hash", device_pub, "fortisiem.device__raw__publish", ["success"])

    x0 = 3250
    y0 = 420
    for idx, (entity, cfg) in enumerate(CHILD_EXTRACTS.items()):
        y = y0 + idx * 220
        extractor = create_processor(
            f"fortisiem.{entity}__extract_from_device_detail",
            "org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
            x0,
            y,
            {
                "Script Body": CHILD_EXTRACT_SCRIPT,
                "SECTION": cfg["section"],
                "CHILD_TAG": cfg["child"] or "",
                "ENTITY_NAME": entity,
                "API_IDENTITY": cfg["api"],
            },
            ["failure"],
        )
        h = raw_hash(f"fortisiem.{entity}__raw__dedupe_hash", x0 + 350, y, entity, "${child_object_id}", cfg["api"], "${org_name}")
        pub = create_processor(f"fortisiem.{entity}__raw__publish", "org.apache.nifi.kafka.processors.PublishKafka", x0 + 700, y, publish_props(f"bronze.fortisiem.{entity}__raw"), ["success", "failure"])
        create_connection(device_hash, "fortisiem.device__raw__dedupe_hash", extractor, f"fortisiem.{entity}__extract_from_device_detail", ["success"])
        create_connection(extractor, f"fortisiem.{entity}__extract_from_device_detail", h, f"fortisiem.{entity}__raw__dedupe_hash", ["success"])
        create_connection(h, f"fortisiem.{entity}__raw__dedupe_hash", pub, f"fortisiem.{entity}__raw__publish", ["success"])

    for idx, (entity, desc) in enumerate(SCAFFOLD_ENTITIES):
        create_label(f"Scaffold only - {entity}: {desc}. Not auto-run in this build to avoid unbounded/high-risk calls.", -450 + (idx % 3) * 620, 900 + (idx // 3) * 120, 560, 80)

    return inspect()


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
            i = 2
            while fk in used:
                fk = f"{base}_{i}"
                i += 1
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
    grouped = OrderedDict()
    for child in list(elem):
        ck = safe_field_name(child.tag)
        grouped.setdefault(ck, []).append(xml_to_obj(child, depth + 1))
    for ck, vals in grouped.items():
        fk = ck
        base = fk
        i = 2
        while fk in used:
            fk = f"{base}_{i}"
            i += 1
        used.add(fk)
        obj[fk] = vals if len(vals) > 1 else vals[0]
    text = (elem.text or "").strip()
    if text:
        if obj:
            obj["text"] = text
        else:
            return text
    return obj


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
            self.int = self.int or (-(2**31) <= value <= 2**31 - 1)
            self.long = self.long or not (-(2**31) <= value <= 2**31 - 1)
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


def type_from_node(node, parent_name, namespace, depth):
    scalars = []
    if node.bool:
        scalars.append("boolean")
    if node.double:
        scalars.append("double")
    elif node.long:
        scalars.append("long")
    elif node.int:
        scalars.append("int")
    if node.string:
        scalars.append("string")
    complex_count = int(bool(node.records)) + int(bool(node.array))
    if len(scalars) + complex_count > 1:
        return "string"
    if node.array:
        return {"type": "array", "items": type_from_node(node.array, f"{parent_name}_item", namespace, depth + 1)}
    if node.records:
        if depth >= MAX_DEPTH:
            return "string"
        return {"type": "record", "name": safe_field_name(parent_name), "namespace": namespace, "fields": fields_from_record(node.records, parent_name, namespace, depth + 1)}
    if scalars:
        if len(set(scalars)) > 1:
            if "string" in scalars:
                return "string"
            if "double" in scalars:
                return "double"
            if "long" in scalars:
                return "long"
        return scalars[0]
    return "string"


def fields_from_record(records, parent_name, namespace, depth):
    return [{"name": safe_field_name(k), "type": ["null", type_from_node(v, f"{parent_name}_{k}", namespace, depth)], "default": None} for k, v in records.items()]


def schema_from_samples(samples, record_name, namespace):
    root = TypeNode()
    for sample in samples:
        root.add(sample)
    if not root.records:
        raise RuntimeError(f"No object fields inferred for {record_name}")
    return {"type": "record", "name": safe_field_name(record_name), "namespace": namespace, "fields": fields_from_record(root.records, safe_field_name(record_name), namespace, 1)}


def add_standard_value_fields(sample, entity, source_object_id="sample", payload_hash="sample"):
    out = OrderedDict()
    values = {
        "source_platform": "fortisiem",
        "customer_tenant_organization": sample.get("org_name") or sample.get("customer_tenant_organization") or "sample",
        "source_object_type": entity,
        "source_object_id": source_object_id,
        "extraction_timestamp": "1970-01-01T00:00:00Z",
        "source_event_update_timestamp": "",
        "api_endpoint_export_query_identity": "sample",
        "cursor_window": "",
        "payload_hash_fingerprint": payload_hash,
        "ingestion_run_batch_identity": "sample",
    }
    for field in STANDARD_VALUE_FIELDS:
        out[field] = values[field]
    for key, value in sample.items():
        if key not in out:
            out[key] = value
    return out


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


def register_schema(subject, schema):
    payload = {"schema": json.dumps(schema, separators=(",", ":")), "schemaType": "AVRO"}
    url = f"{APICURIO_CCOMPAT}/subjects/{urllib.parse.quote(subject, safe='')}/versions"
    r = requests.post(url, json=payload, verify=False, timeout=30)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Register schema failed for {subject}: HTTP {r.status_code}: {r.text[:1000]}")
    latest = requests.get(f"{APICURIO_CCOMPAT}/subjects/{urllib.parse.quote(subject, safe='')}/versions/latest", verify=False, timeout=30)
    latest.raise_for_status()
    return latest.json()


def schema_subject_exists(subject):
    try:
        r = requests.get(f"{APICURIO_CCOMPAT}/subjects/{urllib.parse.quote(subject, safe='')}/versions/latest", verify=False, timeout=20)
        return r.status_code == 200
    except Exception:
        return False


def infer_register():
    os.makedirs("generated_schemas", exist_ok=True)
    result = {}
    for ent in CORE_ENTITIES:
        vals = fetch_topic_values(ent["topic"], SAMPLE_LIMIT)
        if ent["entity"] == "organization":
            vals = [v for v in vals if "<domain" in v]
        elif ent["entity"] == "device":
            vals = [v for v in vals if "<device>" in v and "ip=&" not in v and len(v) > 500]
        else:
            vals = [v for v in vals if '"raw_xml":"<"' not in v and '"fields":{"text":"<"}' not in v and len(v) > 250]
        if not vals:
            result[ent["entity"]] = {"topic": ent["topic"], "status": "no_samples"}
            continue
        normalized = []
        for val in vals:
            if ent["format"] == "xml":
                normalized.append(add_standard_value_fields(xml_to_obj(ET.fromstring(val), 0), ent["entity"]))
            else:
                normalized.append(add_standard_value_fields(normalize_json(json.loads(val), 0), ent["entity"]))
        schema = schema_from_samples(normalized, ent["record"], "bronze.fortisiem")
        subject = f"{ent['topic']}.avro-value"
        path = os.path.join("generated_schemas", f"{subject}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(schema, f, indent=2, ensure_ascii=False)
        reg = register_schema(subject, schema)
        result[ent["entity"]] = {"topic": ent["topic"], "avro_topic": f"{ent['topic']}.avro", "samples": len(vals), "subject": subject, "schema_file": path, "schema_id": reg.get("id"), "schema_version": reg.get("version"), "field_count": len(schema.get("fields", []))}
    return result


def add_avro():
    for p in processors_by_name().values():
        stop_processor(p["id"])
    result = {}
    for idx, ent in enumerate(CORE_ENTITIES):
        source = processors_by_name().get(ent["source"])
        if not source:
            result[ent["entity"]] = {"status": "missing_source", "source": ent["source"]}
            continue
        subject = f"{ent['topic']}.avro-value"
        if not schema_subject_exists(subject):
            result[ent["entity"]] = {"status": "skipped_no_schema", "subject": subject}
            continue
        reader = create_controller_service(
            f"fortisiem.{ent['entity']}__avro_json_reader",
            "org.apache.nifi.json.JsonTreeReader",
            {"Schema Access Strategy": "schema-name", "Schema Registry": SCHEMA_REGISTRY_SERVICE_ID, "Schema Name": subject, "Schema Version": None, "Schema Branch": None, "Schema Text": "${avro.schema}", "Schema Reference Reader": None, "Schema Inference Cache": None, "Starting Field Strategy": "ROOT_NODE", "Starting Field Name": None, "Schema Application Strategy": "SELECTED_PART"},
        )
        writer = create_controller_service(
            f"fortisiem.{ent['entity']}__avro_writer",
            "org.apache.nifi.avro.AvroRecordSetWriter",
            {"Schema Write Strategy": "schema-reference-writer", "Schema Reference Writer": SCHEMA_REF_WRITER_SERVICE_ID, "Schema Access Strategy": "schema-name", "Schema Registry": SCHEMA_REGISTRY_SERVICE_ID, "Schema Name": subject, "Schema Version": None, "Schema Branch": None, "Schema Text": "${avro.schema}", "Schema Reference Reader": None},
        )
        y = -500 + idx * 160
        script = XML_TO_JSON_SCRIPT if ent["format"] == "xml" else JSON_NORMALIZE_SCRIPT
        normalizer = create_processor(f"fortisiem.{ent['entity']}__avro__normalize_json", "org.apache.nifi.processors.groovyx.ExecuteGroovyScript", 4300, y, {"Script Body": script}, ["failure"])
        publisher = create_processor(f"fortisiem.{ent['entity']}__avro__publish", "org.apache.nifi.kafka.processors.PublishKafka", 4650, y, publish_props(f"{ent['topic']}.avro", True, reader, writer), ["success", "failure"])
        create_connection(source["id"], ent["source"], normalizer, f"fortisiem.{ent['entity']}__avro__normalize_json", ["success"])
        create_connection(normalizer, f"fortisiem.{ent['entity']}__avro__normalize_json", publisher, f"fortisiem.{ent['entity']}__avro__publish", ["success"])
        result[ent["entity"]] = {"normalizer": normalizer, "publisher": publisher, "topic": f"{ent['topic']}.avro", "subject": subject}
    return result


def inspect():
    out = {"process_group": {"id": pg_id(), "name": PG_NAME}, "processors": {}, "topics": []}
    for name, p in processors_by_name().items():
        if name.startswith("fortisiem."):
            comp = p.get("component", {})
            out["processors"][name] = {
                "id": p["id"],
                "state": comp.get("state"),
                "validation": comp.get("validationStatus"),
                "validation_errors": comp.get("validationErrors"),
            }
    for ent in CORE_ENTITIES:
        out["topics"].append(ent["topic"])
        out["topics"].append(f"{ent['topic']}.avro")
    return out


def queued_summary():
    return [
        {"id": c["id"], "source": c["component"]["source"].get("name"), "destination": c["component"]["destination"].get("name"), "queued": c["status"]["aggregateSnapshot"].get("queued"), "bytes": c["status"]["aggregateSnapshot"].get("queuedSize")}
        for c in connections()
        if c["status"]["aggregateSnapshot"].get("queued") not in ("0", "0 (0 bytes)")
    ]


def drop_nonempty_queues():
    dropped = []
    for c in connections():
        queued = c["status"]["aggregateSnapshot"].get("queued")
        queued_count = c["status"]["aggregateSnapshot"].get("queuedCount")
        if queued_count and queued_count != "0":
            cid = c["id"]
            req = nifi("POST", f"/nifi-api/flowfile-queues/{cid}/drop-requests", {"revision": {"clientId": CLIENT_ID, "version": 0}})
            drop_id = req["dropRequest"]["id"]
            last = None
            for _ in range(30):
                last = nifi("GET", f"/nifi-api/flowfile-queues/{cid}/drop-requests/{drop_id}")
                if last["dropRequest"].get("finished"):
                    break
                time.sleep(1)
            nifi("DELETE", f"/nifi-api/flowfile-queues/{cid}/drop-requests/{drop_id}")
            dropped.append({"connection": cid, "source": c["component"]["source"].get("name"), "destination": c["component"]["destination"].get("name"), "queued": queued, "drop": last["dropRequest"] if last else None})
    return dropped


def start_all_except_trigger():
    procs = processors_by_name()
    for name, p in procs.items():
        if (
            name == "fortisiem.maximum__trigger"
            or "__admin_" in name
            or "__test_" in name
        ):
            continue
        ent = nifi("GET", f"/nifi-api/processors/{p['id']}")
        if ent["component"].get("validationStatus") == "VALID":
            set_processor_state(p["id"], "RUNNING")


def stop_all():
    for p in processors_by_name().values():
        stop_processor(p["id"])


def run_once(wait_seconds=300):
    start_all_except_trigger()
    trigger = processors_by_name()["fortisiem.maximum__trigger"]
    set_processor_state(trigger["id"], "RUNNING")
    time.sleep(4)
    stop_processor(trigger["id"])
    deadline = time.time() + wait_seconds
    last = None
    while time.time() < deadline:
        q = queued_summary()
        last = q
        if not q:
            break
        time.sleep(10)
    stop_all()
    return {"queued_remaining": last or [], "inspect": inspect()}


def start_controlled_run():
    start_all_except_trigger()
    trigger = processors_by_name()["fortisiem.maximum__trigger"]
    set_processor_state(trigger["id"], "RUNNING")
    time.sleep(4)
    stop_processor(trigger["id"])
    return {"started": True, "trigger_stopped": True, "queued": queued_summary()}


def start_drain_only():
    start_all_except_trigger()
    stop_processor(processors_by_name()["fortisiem.maximum__trigger"]["id"])
    return {"started_downstream_only": True, "queued": queued_summary()}


def verify_topics():
    result = {}
    for ent in CORE_ENTITIES:
        for topic in [ent["topic"], f"{ent['topic']}.avro"]:
            try:
                vals = fetch_topic_values(topic, 5)
                result[topic] = {"samples_seen": len(vals), "has_data": bool(vals)}
            except Exception as e:
                result[topic] = {"error": str(e)}
    return result


def latest_device_xml_sample():
    vals = fetch_topic_values("bronze.fortisiem.device__raw", 100)
    for val in vals:
        if "<device>" in val and "<storages>" in val and len(val) > 5000:
            return val
    raise RuntimeError("No suitable detailed FortiSIEM device XML sample found in Kafka")


def build_test_injector():
    sample = latest_device_xml_sample()
    injector = create_processor(
        "fortisiem.maximum__test_replay_device_detail_from_kafka",
        "org.apache.nifi.processors.standard.GenerateFlowFile",
        2600,
        -320,
        {"File Size": "0B", "Batch Size": "1", "Data Format": "Text", "Unique FlowFiles": "false", "Custom Text": sample},
        [],
        scheduling_period="2 hours",
    )
    detail_extract = processors_by_name()["fortisiem.maximum__device_detail__extract_key"]
    create_connection(injector, "fortisiem.maximum__test_replay_device_detail_from_kafka", detail_extract["id"], "fortisiem.maximum__device_detail__extract_key", ["success"])
    return {"injector": injector, "sample_bytes": len(sample)}


def run_test_replay(wait_seconds=120):
    build_test_injector()
    start_all_except_trigger()
    injector = processors_by_name()["fortisiem.maximum__test_replay_device_detail_from_kafka"]
    set_processor_state(injector["id"], "RUNNING")
    time.sleep(4)
    stop_processor(injector["id"])
    deadline = time.time() + wait_seconds
    last = None
    while time.time() < deadline:
        q = queued_summary()
        last = q
        if not q:
            break
        time.sleep(5)
    stop_all()
    return {"queued_remaining": last or [], "verify": verify_topics()}


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "inspect"
    if cmd == "build-raw":
        print(json.dumps(build_raw(), indent=2))
    elif cmd == "run-once":
        print(json.dumps(run_once(int(os.environ.get("WAIT_SECONDS", "300"))), indent=2))
    elif cmd == "start-run":
        print(json.dumps(start_controlled_run(), indent=2))
    elif cmd == "start-drain":
        print(json.dumps(start_drain_only(), indent=2))
    elif cmd == "queued":
        print(json.dumps({"queued": queued_summary(), "inspect": inspect()}, indent=2))
    elif cmd == "drop-queues":
        print(json.dumps({"dropped": drop_nonempty_queues(), "queued_after": queued_summary()}, indent=2))
    elif cmd == "infer-register":
        print(json.dumps(infer_register(), indent=2))
    elif cmd == "add-avro":
        print(json.dumps(add_avro(), indent=2))
    elif cmd == "verify":
        print(json.dumps({"nifi": inspect(), "kafka": verify_topics()}, indent=2))
    elif cmd == "build-test-injector":
        print(json.dumps(build_test_injector(), indent=2))
    elif cmd == "run-test-replay":
        print(json.dumps(run_test_replay(int(os.environ.get("WAIT_SECONDS", "120"))), indent=2))
    elif cmd == "stop":
        stop_all()
        print(json.dumps({"stopped": True, "inspect": inspect()}, indent=2))
    else:
        print(json.dumps(inspect(), indent=2))


if __name__ == "__main__":
    requests.packages.urllib3.disable_warnings()
    main()
