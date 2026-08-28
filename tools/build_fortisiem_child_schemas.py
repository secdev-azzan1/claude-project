"""
Build real per-field Avro schemas for the fortisiem entities that today are stored as a generic
wrapper (source_parent/fields/raw_xml). Field shapes are taken from real live samples captured
during the fortisiem native-rebuild planning session (device 172.16.20.82 "ArcSql-Lab" for
processors/storages/applications, device 172.16.20.10 "SECURADO-DC" for interfaces).
"""
import json
import os
import subprocess
import sys
import time
import urllib.parse
import xml.etree.ElementTree as ET


APICURIO_CCOMPAT = os.environ.get("APICURIO_CCOMPAT", "https://apicurio.datapasc.com/apis/ccompat/v7").rstrip("/")

STANDARD_FIELDS = [
    "source_platform", "customer_tenant_organization", "source_object_type", "source_object_id",
    "extraction_timestamp", "source_event_update_timestamp", "api_endpoint_export_query_identity",
    "cursor_window", "payload_hash_fingerprint", "ingestion_run_batch_identity",
]


def run_curl(args, input_text=None, timeout=30, attempts=3):
    last = None
    for i in range(attempts):
        proc = subprocess.run(["curl.exe", "--http1.1", "-k", "-sS"] + args, input=input_text, text=True, capture_output=True, timeout=timeout)
        if proc.returncode == 0:
            return proc.stdout
        last = f"curl exit {proc.returncode}: {proc.stderr[:400]} {proc.stdout[:400]}"
        time.sleep(1 + i)
    raise RuntimeError(last)


def apicurio_post(path, body, timeout=30):
    args = ["-X", "POST", "-H", "Content-Type: application/vnd.schemaregistry.v1+json", "--data-binary", "@-", "-w", "\nHTTP_STATUS:%{http_code}", f"{APICURIO_CCOMPAT}{path}"]
    out = run_curl(args, json.dumps(body), timeout=timeout)
    raw, status_txt = out.rsplit("\nHTTP_STATUS:", 1)
    status = int(status_txt.strip()[:3])
    if status < 200 or status > 299:
        raise RuntimeError(f"POST {path} HTTP {status}: {raw[:1000]}")
    return json.loads(raw.strip())


def infer_field_type(text_value):
    if text_value is None or text_value == "":
        return ["null", "string"]
    v = text_value.strip()
    if v in ("true", "false"):
        return ["null", "boolean"]
    if v.lstrip("-").isdigit():
        return ["null", "long"]
    return ["null", "string"]


def element_to_avro_fields(elem, record_name_prefix, namespace):
    """Build Avro field list from one representative XML element (its children become fields)."""
    fields = []
    used = set()
    for child in elem:
        tag = child.tag
        base = tag
        idx = 2
        while tag in used:
            tag = f"{base}_{idx}"
            idx += 1
        used.add(tag)
        if len(child) > 0:
            nested_name = f"{record_name_prefix}_{tag}"
            nested_fields = element_to_avro_fields(child, nested_name, namespace)
            fields.append({
                "name": tag,
                "type": ["null", {"type": "record", "name": nested_name, "namespace": namespace, "fields": nested_fields}],
                "default": None,
            })
        else:
            fields.append({"name": tag, "type": infer_field_type(child.text), "default": None})
    return fields


def build_schema(entity, record_name, sample_xml_fragment):
    """sample_xml_fragment: XML text of ONE representative child element (e.g. one <networkinterface>)."""
    elem = ET.fromstring(sample_xml_fragment)
    namespace = "bronze.fortisiem"
    entity_fields = element_to_avro_fields(elem, record_name, namespace)

    fields = []
    for f in STANDARD_FIELDS:
        fields.append({"name": f, "type": ["null", "string"], "default": None})
    fields.append({"name": "object_id", "type": ["null", "string"], "default": None})
    fields.append({"name": "ingest_ts", "type": ["null", "long"], "default": None})
    fields.extend(entity_fields)

    return {
        "type": "record",
        "name": record_name,
        "namespace": namespace,
        "fields": fields,
    }


def infer_json_field_type(value):
    if value is None:
        return ["null", "string"]
    if isinstance(value, bool):
        return ["null", "boolean"]
    if isinstance(value, int):
        return ["null", "long"]
    if isinstance(value, float):
        return ["null", "double"]
    return ["null", "string"]


def build_json_schema(record_name, sample_dict, field_order, type_overrides=None):
    """sample_dict: flat dict of ONE representative record (e.g. one incident). field_order: full
    documented field name list (superset of sample_dict's keys) so undocumented-but-real fields not
    present in the one captured example still get a column, typed by best-guess convention.
    type_overrides: {field_name: avro_type} for fields absent from sample_dict whose type shouldn't
    fall back to the string default (e.g. a *Time field that follows the epoch-millis convention)."""
    namespace = "bronze.fortisiem"
    type_overrides = type_overrides or {}
    entity_fields = []
    for name in field_order:
        if name not in sample_dict and name in type_overrides:
            avro_type = type_overrides[name]
        else:
            avro_type = infer_json_field_type(sample_dict.get(name))
        entity_fields.append({"name": name, "type": avro_type, "default": None})

    fields = []
    for f in STANDARD_FIELDS:
        fields.append({"name": f, "type": ["null", "string"], "default": None})
    fields.append({"name": "object_id", "type": ["null", "string"], "default": None})
    fields.append({"name": "ingest_ts", "type": ["null", "long"], "default": None})
    fields.extend(entity_fields)

    return {
        "type": "record",
        "name": record_name,
        "namespace": namespace,
        "fields": fields,
    }


SAMPLES = {
    "interface": (
        "sentinelone_placeholder",  # unused, overwritten below
        "<networkinterface><ipv4Mask>32</ipv4Mask><description></description><outSpeed>100Mbps</outSpeed>"
        "<inSpeed>100Mbps</inSpeed><type>Ethernet</type><speed>100Mbps</speed><adminStatus>up</adminStatus>"
        "<ipv4Addr>172.16.20.10</ipv4Addr><isWAN>false</isWAN><snmpIndex>1</snmpIndex><isTrunk>false</isTrunk>"
        "<ipv4IsVirtual>false</ipv4IsVirtual><macIsVirtual>false</macIsVirtual><isMonitor>false</isMonitor>"
        "<name>AOMgmt</name><operStatus>up</operStatus><isCritical>false</isCritical><macAddr></macAddr>"
        "</networkinterface>"
    ),
    "processor": (
        "x",
        "<processor><addrWidth>64</addrWidth><count>0</count><manufacturer>GenuineIntel</manufacturer>"
        "<maxClockSpeed>2294</maxClockSpeed><currClockSpeed>2294</currClockSpeed><l2CacheSpeed>0</l2CacheSpeed>"
        "<name>Intel64 Family 6 Model 45 Stepping 7</name><l2CacheSize>0</l2CacheSize><dataWidth>64</dataWidth>"
        "</processor>"
    ),
    "storage": (
        "x",
        "<storage><description>Physical Memory</description><used>2.45 GBytes</used><type>hrStorageRam</type>"
        "<size>3.04 GBytes</size><name>Physical Memory</name></storage>"
    ),
    "agent_status": (
        "x",
        # No live populated sample -- confirmed empty (<Statuses></Statuses>) on the one WMI-monitored
        # device tested; this tenant has no agent-based devices to sample from. Field names/wrapper
        # element from the API Guide (p.18): "AnXMLfilecontainingType,AgentStatus,PolicyID,
        # HeartbeatTime,LastEventReceiveTime" -- <Statuses><Status>...single child element, matching
        # every other list wrapper convention seen elsewhere in this API (devices/device,
        # interfaces/networkinterface, etc). Time fields typed as numeric epoch millis to match the
        # convention already confirmed for discoverTime on the real device schema.
        "<Status><Type>Windows</Type><AgentStatus>Active</AgentStatus><PolicyID>1</PolicyID>"
        "<HeartbeatTime>1754381265000</HeartbeatTime><LastEventReceiveTime>1754381265000</LastEventReceiveTime>"
        "</Status>"
    ),
    "installed_software": (
        "x",
        "<application><appPackage><appGroupName>Microsoft Remote Desktop Clip Manager</appGroupName>"
        "<naturalId>Microsoft%20Remote%20Desktop%20Clip%20Manager</naturalId><processName>rdpclip.exe</processName>"
        "<name>Microsoft Remote Desktop Clip Manager</name></appPackage>"
        "<naturalId>172.16.20.82_Microsoft Remote Desktop Clip Manager</naturalId><creationMethod>WMI</creationMethod>"
        "<processName>rdpclip.exe</processName><canSetImportantProc>true</canSetImportantProc>"
        "<accessIp>172.16.20.82</accessIp><processParam></processParam><updateMethod>WMI</updateMethod>"
        "<uptime>148</uptime><isMonitor>false</isMonitor><name>Microsoft Remote Desktop Clip Manager</name>"
        "<isCritical>false</isCritical><status>running</status></application>"
    ),
}

RECORD_NAMES = {
    "interface": "fortisiem_interface_raw_avro",
    "processor": "fortisiem_processor_raw_avro",
    "storage": "fortisiem_storage_raw_avro",
    "installed_software": "fortisiem_installed_software_raw_avro",
    "agent_status": "fortisiem_agent_status_raw_avro",
}

# Real documented example response (API Guide p.50-51, FetchIncidents) for the fields the example
# actually populates, plus every other field name from the same page's full incident "fields" list
# (the request payload lists more queryable attributes than the one truncated example object shows).
# Types for the not-in-example fields follow the doc's own conventions: *Time fields are epoch millis
# (long, matching incidentClearedTime/incidentFirstSeen/incidentLastSeen in the real example);
# incidentReso/incidentStatus/phIncidentCategory are documented as integer enum codes (p.55), so
# phSubIncidentCategory -- same "category" family, undocumented sibling of phIncidentCategory -- is
# typed the same way; everything else (comments, external-ticket fields, cleared-by user) is free text.
INCIDENT_SAMPLE = {
    "incidentTitle": "SNMP service down on wk5794.fortinet.com",
    "eventSeverity": 10,
    "incidentFirstSeen": 1621941030000,
    "incidentReso": 1,
    "incidentRptIp": "172.30.57.94",
    "incidentLastSeen": 1621987770000,
    "incidentSrc": "",
    "count": 54,
    "attackTechnique": "[{\"name\":\"Service Stop\",\"techniqueid\":\"T1489\"}]",
    "eventType": "PH_RULE_SNMP_DOWN",
    "phIncidentCategory": 1,
    "incidentClearedTime": 0,
    "incidentTarget": "hostIpAddr:172.30.57.94,hostName:wk5794.fortinet.com,",
    "attackTactic": "Impact",
    "eventSeverityCat": "HIGH",
    "incidentDetail": "",
    "incidentRptDevName": "wk5794.fortinet.com",
    "eventName": "SNMPServiceUnavailable",
    "incidentId": 114780,
    "incidentStatus": 0,
    "customer": "Super",
}
INCIDENT_FIELD_ORDER = [
    "eventSeverityCat", "eventSeverity", "incidentLastSeen", "incidentFirstSeen", "eventType",
    "eventName", "incidentSrc", "incidentTarget", "incidentDetail", "incidentRptIp",
    "incidentRptDevName", "incidentStatus", "incidentComments", "customer", "incidentClearedReason",
    "incidentClearedTime", "incidentClearedUser", "count", "incidentId", "incidentExtUser",
    "incidentExtClearedTime", "incidentExtTicketId", "incidentExtTicketState", "incidentExtTicketType",
    "incidentReso", "phIncidentCategory", "phSubIncidentCategory", "incidentTitle", "attackTechnique",
    "attackTactic",
]
INCIDENT_RECORD_NAME = "fortisiem_incident_raw_avro"
# Fields absent from the one captured example, typed by documented convention rather than left to
# the string default: incidentExtClearedTime is a *Time field (epoch millis, like incidentClearedTime/
# incidentFirstSeen/incidentLastSeen in the real example); phSubIncidentCategory is the undocumented
# sibling of phIncidentCategory, same integer-enum-code family (p.55).
INCIDENT_TYPE_OVERRIDES = {
    "incidentExtClearedTime": ["null", "long"],
    "phSubIncidentCategory": ["null", "long"],
}


def build_device_custom_property_schema():
    # No live populated sample available -- confirmed empty on 2 real devices via the dedicated
    # /cmdbDeviceInfo/properties endpoint. Custom property names are per-device/admin-defined
    # (arbitrary keys), matching the UpdateDeviceCustomProperty API's own documented vocabulary
    # (propertyName/propertyValue) -- modeled as an Avro map rather than guessed fixed fields,
    # since the key set is inherently dynamic.
    fields = []
    for f in STANDARD_FIELDS:
        fields.append({"name": f, "type": ["null", "string"], "default": None})
    fields.append({"name": "object_id", "type": ["null", "string"], "default": None})
    fields.append({"name": "ingest_ts", "type": ["null", "long"], "default": None})
    fields.append({"name": "properties", "type": ["null", {"type": "map", "values": ["null", "string"]}], "default": None})
    return {
        "type": "record",
        "name": "fortisiem_device_custom_property_raw_avro",
        "namespace": "bronze.fortisiem",
        "fields": fields,
    }


def build_and_register(entity):
    if entity == "device_custom_property":
        schema = build_device_custom_property_schema()
    elif entity == "incident":
        schema = build_json_schema(INCIDENT_RECORD_NAME, INCIDENT_SAMPLE, INCIDENT_FIELD_ORDER, INCIDENT_TYPE_OVERRIDES)
    else:
        _, xml_fragment = SAMPLES[entity]
        schema = build_schema(entity, RECORD_NAMES[entity], xml_fragment)
    subject = f"bronze.fortisiem.{entity}__raw.avro-value"
    result = apicurio_post(f"/subjects/{urllib.parse.quote(subject, safe='')}/versions", {"schema": json.dumps(schema)})
    return {"entity": entity, "subject": subject, "id": result.get("id"), "field_count": len(schema["fields"])}


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "all"
    if cmd == "all":
        results = []
        for e in SAMPLES:
            try:
                results.append(build_and_register(e))
            except Exception as exc:
                results.append({"entity": e, "error": str(exc)})
        print(json.dumps(results, indent=2))
    elif cmd == "print":
        e = sys.argv[2]
        if e == "device_custom_property":
            print(json.dumps(build_device_custom_property_schema(), indent=2))
        elif e == "incident":
            print(json.dumps(build_json_schema(INCIDENT_RECORD_NAME, INCIDENT_SAMPLE, INCIDENT_FIELD_ORDER, INCIDENT_TYPE_OVERRIDES), indent=2))
        else:
            _, xml_fragment = SAMPLES[e]
            print(json.dumps(build_schema(e, RECORD_NAMES[e], xml_fragment), indent=2))
    else:
        raise SystemExit(f"unknown command: {cmd}")
