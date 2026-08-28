import json

PLATFORM = "sentinelone"
TENANT = "sentinelone_securado"
SERVICE_ID = "svc-b09gdg"
CRON = "0 */2 * * *"

BASE_DEDUP_EXCLUDES = ["extraction_timestamp", "ingestion_run_batch_identity", "cursor_window"]


def meta_transforms(prefix, entity, source_object_id_expr, cursor_window_expr, endpoint_expr):
    object_id_expr = f"{PLATFORM}:{TENANT}:{entity}:{source_object_id_expr}"
    fields = [
        ("source_platform", PLATFORM),
        ("customer_tenant_organization", TENANT),
        ("source_object_type", entity),
        ("source_object_id", source_object_id_expr),
        ("object_id", object_id_expr),
        ("cursor_window", cursor_window_expr),
        ("api_endpoint_export_query_identity", endpoint_expr),
        ("ingest_ts", "${now():toNumber()}"),
        ("extraction_timestamp", "${now():format(\"yyyy-MM-dd'T'HH:mm:ss.SSSXXX\")}"),
        ("ingestion_run_batch_identity", f"{TENANT}-${{now():toNumber()}}-${{uuid}}"),
        ("source_event_update_timestamp", ""),
    ]
    return [
        {"id": f"t-meta-{prefix}-{field}", "kind": "add_field", "config": {"field": field, "value": value}}
        for field, value in fields
    ]


def extract(attr, path, block_prefix):
    return {"id": f"t-extract-{attr}-{block_prefix}", "kind": "extract", "config": {"attribute": attr, "path": path, "default": ""}}


def dedup(prefix, window_hours=24, extra_excludes=None):
    excludes = list(BASE_DEDUP_EXCLUDES) + list(extra_excludes or [])
    return {"id": f"t-dedup-{prefix}", "kind": "dedup",
            "config": {"identityFields": ["object_id"], "excludedFields": excludes, "windowHours": window_hours}}


def http_block(id_, name, parent, path, record_path, split, pagination, transforms, branch=None):
    return {
        "id": id_, "adapter": "http", "mode": "read", "name": name, "parentId": parent,
        "branch": branch, "serviceId": SERVICE_ID, "entity": None,
        "config": {"method": "GET", "path": path, "responseFormat": "json", "recordPath": record_path,
                   "split": split, "pagination": pagination},
        "transforms": transforms, "topicOverride": None, "testResult": None,
    }


def kafka_write_block(id_, name, parent, entity, topic, transforms=None):
    return {
        "id": id_, "adapter": "kafka", "mode": "write", "name": name, "parentId": parent,
        "branch": None, "serviceId": None, "entity": entity, "config": {},
        "transforms": transforms or [], "topicOverride": topic, "testResult": None,
    }


def topic(writer_block_id, name):
    return {"id": f"t-{writer_block_id}", "kind": "materialized", "name": name,
            "sealed": False, "writerBlockId": writer_block_id, "backlogEstimate": None}


# Full pagination: no page-count cap (matches reference's unbounded cursor
# loop), limit=1000/page (matches reference's CURSOR_LIMIT). Raised from the
# initial maxPages=1 safety cap after batch 1/2's first controlled runs came
# back clean.
CURSOR_FULL = {"type": "cursor", "fields": {
    "cursorParam": "cursor", "cursorPath": "$.pagination.nextCursor",
    "sizeParam": "limit", "sizeValue": "1000",
}}
NONE_PAGINATION = {"type": "none", "fields": {}}


def redact(prefix, field):
    """Strip a literal top-level JSON field before Kafka -- mirrors the
    reference's secret scrub for entities known to carry credential-shaped
    fields (apiToken, licenseKey). Scoped to fields we've actually confirmed
    present in the live payload, not a blind blanket removal."""
    return {"id": f"t-redact-{field}-{prefix}", "kind": "remove_field", "config": {"field": field}}


def display_name(entity):
    return " ".join(w.capitalize() for w in entity.split("_"))


def simple_entity_flow(entity, path, *, id_path="$.id", paginated=True, cursor_window_note="cursor=${cursor}",
                        extra_extract=None, redact_fields=None):
    """One-hop catalog flow: paginated list read (extract id only) -> kafka
    write sibling (meta_transforms + dedup). SentinelOne list endpoints return
    full records, so no separate detail hop is needed (unlike Rapid7).

    extra_extract: optional [(attr_name, json_path), ...] pulled onto the read
    block alongside the id. Not wired into object_id/dedup today (object_id
    stays bare-native-id per current decision) -- staged as FlowFile
    attributes so a future composite-key fix is a config change, not a
    re-plumbing job.
    redact_fields: optional [literal_json_field, ...] stripped before Kafka,
    for entities confirmed to carry credential-shaped fields.
    """
    name = display_name(entity)
    root_id = f"b-{entity}"
    write_id = f"b-{entity}-write"
    id_attr = f"{entity}_id"
    pagination = CURSOR_FULL if paginated else NONE_PAGINATION
    read_transforms = [extract(id_attr, id_path, root_id)]
    for attr, jpath in (extra_extract or []):
        read_transforms.append(extract(attr, jpath, root_id))
    write_transforms = [redact(entity, field) for field in (redact_fields or [])]
    write_transforms += meta_transforms(entity.replace("_", "-"), entity, f"${{{id_attr}}}",
                                         cursor_window_note, path) + [dedup(entity)]
    blocks = [
        http_block(root_id, f"List {name}", None, path, "$.data[*]", True, pagination, read_transforms),
        kafka_write_block(write_id, f"Publish {name}", root_id, entity, f"bronze.{TENANT}.{entity}",
                           write_transforms),
    ]
    topics = [topic(write_id, f"bronze.{TENANT}.{entity}")]
    return {
        "id": f"flow-s1-{entity.replace('_', '-')}", "name": f"SentinelOne - {name}",
        "description": f"{name} catalog: {path} -> raw JSON to Kafka",
        "state": "Draft", "enabled": False, "cron": CRON,
        "blocks": blocks, "topics": topics, "variables": [], "servicePins": {},
    }


def singleton_entity_flow(entity, path):
    """No native id in the payload, single object, no pagination -- built as a
    plain read->write pair with a fixed composite object_id. Same class of
    singleton as site_policy/group_policy (see flow-s1-site/-group): dedup is
    not applicable to a split=false stream, and would just suppress the
    per-run refresh anyway, so it's intentionally omitted here too."""
    name = display_name(entity)
    root_id = f"b-{entity}"
    write_id = f"b-{entity}-write"
    blocks = [
        http_block(root_id, name, None, path, "$", False, NONE_PAGINATION,
                   meta_transforms(entity.replace("_", "-"), entity, "singleton", "", path)),
        kafka_write_block(write_id, f"Publish {name}", root_id, entity, f"bronze.{TENANT}.{entity}"),
    ]
    topics = [topic(write_id, f"bronze.{TENANT}.{entity}")]
    return {
        "id": f"flow-s1-{entity.replace('_', '-')}", "name": f"SentinelOne - {name}",
        "description": f"{name} (singleton settings object): {path} -> raw JSON to Kafka",
        "state": "Draft", "enabled": False, "cron": CRON,
        "blocks": blocks, "topics": topics, "variables": [], "servicePins": {},
    }


flows = []

ORDERED_STANDARD = [
    # near-empty / 1-row or 0-row catalogs
    ("service_user", "/service-users", "$.id"),
    ("location", "/locations", "$.id"),
    ("ioc", "/threat-intelligence/iocs", "$.id"),
    # small catalogs
    ("activity_type", "/activities/types", "$.id"),  # no pagination, still split=true
    ("role", "/rbac/roles", "$.id"),
    ("cloud_detection_rule", "/cloud-detection/rules", "$.id"),
    ("agent_tag", "/agents/tags", "$.id"),
    ("xdr_asset_tag", "/xdr/assets/tags", "$.id"),
    ("agent_package", "/update/agent/packages", "$.id"),
    ("config_override", "/config-override", "$.id"),
    ("exclusion", "/exclusions", "$.id"),
    ("user", "/users", "$.id"),
    ("application_cve", "/installed-applications/cves", "$.id"),
    ("alert", "/cloud-detection/alerts", "$.alertInfo.alertId"),
    # higher-volume cursor-paginated, last
    ("xdr_asset", "/xdr/assets", "$.id"),
    ("activity",
     "/activities?createdAt__gte=${now():toNumber():minus(14400000):format(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\", \"GMT\")}",
     "$.id"),
    ("agent", "/agents", "$.id"),
    ("restriction", "/restrictions", "$.id"),
    ("installed_application", "/installed-applications", "$.id"),
]

# Prep for a future composite object_id/dedup-key fix (deferred per user
# decision) -- pulls the parent/scope field each entity would need onto the
# FlowFile as an attribute now, so wiring it in later is a config change.
EXTRA_EXTRACT = {
    "installed_application": [("agent_id", "$.agentId")],
    "activity": [("agent_id", "$.agentId")],
    "exclusion": [("scope_name", "$.scopeName")],
    "restriction": [("scope_name", "$.scopeName")],
    "alert": [("agent_id", "$.agentRealtimeInfo.agentId")],
    "xdr_asset": [("site_id", "$.s1SiteId")],
    "xdr_asset_tag": [("tag_key", "$.key")],
    "agent_tag": [("scope", "$.scope")],
}

# Fields confirmed present (via live Kafka spot-check) on these entities'
# payloads that match the reference's secret-key patterns.
REDACT_FIELDS = {
    "user": ["apiToken"],
    "service_user": ["apiToken"],
    "agent": ["licenseKey"],
}

for entity, path, id_path in ORDERED_STANDARD:
    paginated = entity != "activity_type"
    flows.append(simple_entity_flow(
        entity, path, id_path=id_path, paginated=paginated,
        extra_extract=EXTRA_EXTRACT.get(entity),
        redact_fields=REDACT_FIELDS.get(entity),
    ))

flows.append(singleton_entity_flow("tenant_policy", "/tenant/policy"))
flows.append(singleton_entity_flow("system_info", "/system/info"))

assert len(flows) == 21, f"expected 21 standalone flows, got {len(flows)}"

for f in flows:
    with open(f".tmp_work/{f['id']}.json", "w") as fh:
        json.dump(f, fh)
    print(f["id"], "blocks:", len(f["blocks"]))
