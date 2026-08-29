import json
import random
import string
import urllib.request

BASE = "http://localhost:8000"
SERVICE_ID = "svc-m4zt4o"

FLOW_ID = "flow-" + "".join(random.choices(string.ascii_lowercase + string.digits, k=6))

DEDUP_EXCLUDES = ["extraction_timestamp", "ingestion_run_batch_identity", "cursor_window"]


def meta_stack(entity, source_object_id_expr, api_path):
    object_id_expr = f"fortisiem:${{org_name}}:{entity}:{source_object_id_expr}"
    fields = [
        ("source_platform", "fortisiem"),
        ("customer_tenant_organization", "${org_name}"),
        ("source_object_type", entity),
        ("source_object_id", source_object_id_expr),
        ("object_id", object_id_expr),
        ("cursor_window", ""),
        ("api_endpoint_export_query_identity", api_path),
        ("ingest_ts", "${now():toNumber()}"),
        ("extraction_timestamp", "${now():format(\"yyyy-MM-dd'T'HH:mm:ss.SSSXXX\")}"),
        ("ingestion_run_batch_identity", "${org_name}-${now():toNumber()}-${uuid}"),
        ("source_event_update_timestamp", ""),
    ]
    transforms = []
    for f, v in fields:
        transforms.append({"id": f"t-meta-{entity}-{f}", "kind": "add_field", "config": {"field": f, "value": v}})
    transforms.append({
        "id": f"t-dedup-{entity}", "kind": "dedup",
        "config": {"identityFields": ["object_id"], "excludedFields": DEDUP_EXCLUDES, "windowHours": 24},
    })
    return transforms


def extract_rule(rule_id, attribute, path):
    return {"id": rule_id, "kind": "extract", "config": {"attribute": attribute, "path": path}}


def read_block(bid, parent_id, path, record_path, extracts):
    return {
        "id": bid, "adapter": "http", "mode": "read", "name": bid, "parentId": parent_id, "branch": None,
        "serviceId": SERVICE_ID, "entity": None,
        "config": {
            "method": "GET", "path": path, "responseFormat": "xml", "split": True,
            "pagination": {"type": "none", "fields": {}}, "proxyId": None, "recordPath": record_path,
        },
        "transforms": extracts, "topicOverride": None, "testResult": None,
    }


def write_block(bid, parent_id, entity, transforms, topic):
    return {
        "id": bid, "adapter": "kafka", "mode": "write", "name": bid, "parentId": parent_id, "branch": None,
        "serviceId": None, "entity": entity, "config": {}, "transforms": transforms,
        "topicOverride": topic, "testResult": None,
    }


def topic(tid, name, writer_block_id):
    return {"id": tid, "kind": "materialized", "name": name, "sealed": False,
            "writerBlockId": writer_block_id, "backlogEstimate": None}


blocks = []
topics = []

# organization
blocks.append(read_block("b-org", None, "/phoenix/rest/config/Domain",
                          "$[0].result.domains.domain[*]",
                          [extract_rule("t0-extract-org_name", "org_name", "$.name")]))
blocks.append(write_block("b-org-write", "b-org", "organization",
                           meta_stack("organization", "${org_name}", "/phoenix/rest/config/Domain"),
                           "bronze.fortisiem.organization__raw"))
topics.append(topic("t-org", "bronze.fortisiem.organization__raw", "b-org-write"))

# device
blocks.append(read_block("b-device", "b-org", "/phoenix/rest/cmdbDeviceInfo/devices?organization=${org_name}",
                          "$[0].device[*]",
                          [extract_rule("t0-extract-access_ip", "access_ip", "$.accessIp"),
                           extract_rule("t1-extract-natural_id", "natural_id", "$.naturalId")]))
blocks.append(write_block("b-device-write", "b-device", "device",
                           meta_stack("device", "${natural_id}_${access_ip}",
                                      "/phoenix/rest/cmdbDeviceInfo/devices"),
                           "bronze.fortisiem.device__raw"))
topics.append(topic("t-device", "bronze.fortisiem.device__raw", "b-device-write"))

DETAIL_PATH = "/phoenix/rest/cmdbDeviceInfo/device?organization=${org_name}&ip=${access_ip}&loadDepend=true"
DETAIL_API_ID = "/phoenix/rest/cmdbDeviceInfo/device"

DETAIL_CHILDREN = [
    # entity, section, child_tag, local_attr, local_json_path
    ("interface", "interfaces", "networkinterface", "if_name", "$.name"),
    ("processor", "processors", "processor", "cpu_name", "$.name"),
    ("storage", "storages", "storage", "storage_name", "$.name"),
    ("installed_software", "applications", "application", "app_natural_id", "$.naturalId"),
    ("software_service", "softwareServices", "softwareservice", "svc_name", "$.name"),
    ("software_patch", "softwarePatches", "softwarepatch", "patch_name", "$.name"),
    ("device_custom_property", "properties", "customproperty", "prop_name", "$.propertyName"),
    ("device_business_service_membership", None, "appGroupName", "group_name", "$"),
]

for entity, section, child_tag, local_attr, local_path in DETAIL_CHILDREN:
    bid = f"b-{entity.replace('_', '-')}"
    wid = f"{bid}-write"
    if section:
        record_path = f"$[0].{section}.{child_tag}[*]"
    else:
        record_path = f"$[0].{child_tag}[*]"
    blocks.append(read_block(bid, "b-device", DETAIL_PATH, record_path,
                              [extract_rule(f"t0-extract-{local_attr}", local_attr, local_path)]))
    blocks.append(write_block(wid, bid, entity,
                               meta_stack(entity, "${natural_id}_${" + local_attr + "}", DETAIL_API_ID),
                               f"bronze.fortisiem.{entity}__raw"))
    topics.append(topic(f"t-{entity}", f"bronze.fortisiem.{entity}__raw", wid))

flow_doc = {
    "id": FLOW_ID,
    "name": "FortiSIEM Device Inventory",
    "description": "organization -> device -> 8 device-detail children (raw)",
    "state": "Stopped",
    "enabled": True,
    "cron": "*/5 * * * *",
    "blocks": blocks,
    "topics": topics,
    "variables": [],
    "servicePins": {SERVICE_ID: 2},
}

print(f"FLOW_ID={FLOW_ID}")
print(f"blocks={len(blocks)} topics={len(topics)}")

req = urllib.request.Request(
    f"{BASE}/api/v2/flows/", data=json.dumps(flow_doc).encode(),
    headers={"Content-Type": "application/json"}, method="POST",
)
try:
    with urllib.request.urlopen(req) as resp:
        print("STATUS", resp.status)
        print(resp.read().decode()[:2000])
except urllib.error.HTTPError as e:
    print("HTTP ERROR", e.code)
    print(e.read().decode()[:3000])
