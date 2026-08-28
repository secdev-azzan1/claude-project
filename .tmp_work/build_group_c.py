import json

PLATFORM = "rapid7"
TENANT = "rapid7_securado"
SERVICE_ID = "svc-nsdqhv"


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


def http_block(id_, name, parent, path, record_path, split, pagination, transforms, branch=None):
    return {
        "id": id_, "adapter": "http", "mode": "read", "name": name, "parentId": parent,
        "branch": branch, "serviceId": SERVICE_ID, "entity": None,
        "config": {"method": "GET", "path": path, "responseFormat": "json", "recordPath": record_path,
                   "split": split, "pagination": pagination},
        "transforms": transforms, "topicOverride": None, "testResult": None,
    }


def kafka_write_block(id_, name, parent, entity, topic):
    return {
        "id": id_, "adapter": "kafka", "mode": "write", "name": name, "parentId": parent,
        "branch": None, "serviceId": None, "entity": entity, "config": {}, "transforms": [],
        "topicOverride": topic, "testResult": None,
    }


def topic(writer_block_id, name):
    return {"id": f"t-{writer_block_id}", "kind": "materialized", "name": name,
            "sealed": False, "writerBlockId": writer_block_id, "backlogEstimate": None}


PAGE_CAPPED = {"type": "page", "fields": {"pageParam": "page", "sizeParam": "size", "firstPage": "0", "sizeValue": "100", "maxPages": "1"}}
NONE_PAGINATION = {"type": "none", "fields": {}}


def simple_catalog_flow(flow_id, name, entity, list_path, detail_path_tmpl, cron):
    """paged list -> per-id detail -> kafka write, single-entity standalone flow."""
    list_id = f"b-{entity}-list"
    detail_id = f"b-{entity}-detail"
    write_id = f"b-{entity}-write"
    blocks = [
        http_block(list_id, f"List {name}", None, list_path, "$.resources[*]", True, PAGE_CAPPED,
                   [extract(f"{entity}_id", "$.id", list_id)]),
        http_block(detail_id, f"{name} Detail", list_id, detail_path_tmpl, "$", False, NONE_PAGINATION,
                   meta_transforms(entity.replace("_", "-"), entity, f"${{{entity}_id}}",
                                    f"id=${{{entity}_id}}", detail_path_tmpl)),
        kafka_write_block(write_id, f"Publish {name}", detail_id, entity, f"bronze.{TENANT}.{entity}"),
    ]
    topics = [topic(write_id, f"bronze.{TENANT}.{entity}")]
    return {
        "id": flow_id, "name": name, "description": f"{name} catalog: {list_path} -> per-id detail, raw JSON to Kafka",
        "state": "Draft", "enabled": False, "cron": cron,
        "blocks": blocks, "topics": topics, "variables": [], "servicePins": {},
    }


flows = []

# ---- Tags family: tag (root) -> tag_detail, tag_asset, tag_site (siblings) ----
tag_blocks = [
    http_block("b-tag", "List Tags", None, "/tags", "$.resources[*]", True, PAGE_CAPPED,
               [extract("tag_id", "$.id", "b-tag")]),
    http_block("b-tag-detail", "Tag Detail", "b-tag", "/tags/${tag_id}", "$", False, NONE_PAGINATION,
               meta_transforms("tag", "tag", "${tag_id}", "id=${tag_id}", "/tags/${tag_id}")),
    kafka_write_block("b-tag-write", "Publish Tag", "b-tag-detail", "tag", f"bronze.{TENANT}.tag"),
    http_block("b-tag-asset", "Tag Assets", "b-tag", "/tags/${tag_id}/assets", "$.resources[*]", True, PAGE_CAPPED,
               [extract("asset_id", "$.id", "b-tag-asset")] +
               meta_transforms("tag-asset", "tag_asset", "${tag_id}_${asset_id}",
                                "tag=${tag_id};page=${page}", "/tags/${tag_id}/assets")),
    kafka_write_block("b-tag-asset-write", "Publish Tag Asset", "b-tag-asset", "tag_asset", f"bronze.{TENANT}.tag_asset"),
    http_block("b-tag-site", "Tag Sites", "b-tag", "/tags/${tag_id}/sites", "$.resources[*]", True, PAGE_CAPPED,
               [extract("site_id", "$.id", "b-tag-site")] +
               meta_transforms("tag-site", "tag_site", "${tag_id}_${site_id}",
                                "tag=${tag_id};page=${page}", "/tags/${tag_id}/sites")),
    kafka_write_block("b-tag-site-write", "Publish Tag Site", "b-tag-site", "tag_site", f"bronze.{TENANT}.tag_site"),
]
tag_topics = [
    topic("b-tag-write", f"bronze.{TENANT}.tag"),
    topic("b-tag-asset-write", f"bronze.{TENANT}.tag_asset"),
    topic("b-tag-site-write", f"bronze.{TENANT}.tag_site"),
]
flows.append({
    "id": "flow-tags", "name": "Rapid7 Securado - Tags", "description": "Tags -> tag assets + tag sites, raw JSON to Kafka",
    "state": "Draft", "enabled": False, "cron": "0 3 * * *",
    "blocks": tag_blocks, "topics": tag_topics, "variables": [], "servicePins": {},
})

# ---- 4 standalone catalogs ----
flows.append(simple_catalog_flow("flow-vuln-reference", "Rapid7 Securado - Vulnerability References",
                                  "vulnerability_reference", "/vulnerability_references",
                                  "/vulnerability_references/${vulnerability_reference_id}", "0 4 * * 1"))
flows.append(simple_catalog_flow("flow-vuln-category", "Rapid7 Securado - Vulnerability Categories",
                                  "vulnerability_category", "/vulnerability_categories",
                                  "/vulnerability_categories/${vulnerability_category_id}", "0 4 * * 1"))
flows.append(simple_catalog_flow("flow-exploit", "Rapid7 Securado - Exploits",
                                  "exploit", "/exploits",
                                  "/exploits/${exploit_id}", "0 4 * * 1"))
flows.append(simple_catalog_flow("flow-malware-kit", "Rapid7 Securado - Malware Kits",
                                  "malware_kit", "/malware_kits",
                                  "/malware_kits/${malware_kit_id}", "0 4 * * 1"))

for f in flows:
    with open(f".tmp_work/{f['id']}.json", "w") as fh:
        json.dump(f, fh)
    print(f["id"], "blocks:", len(f["blocks"]), "topics:", len(f["topics"]))
