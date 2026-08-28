import json

with open(".tmp_work/flow_def_fresh2.json") as f:
    flow = json.load(f)

blocks = flow["blocks"]
topics = flow["topics"]
by_id = {b["id"]: b for b in blocks}

PLATFORM = "rapid7"
TENANT = "rapid7_securado"


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
        "branch": branch, "serviceId": "svc-nsdqhv", "entity": None,
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


def add_topic(writer_block_id, name):
    topics.append({
        "id": f"t-{writer_block_id}", "kind": "materialized", "name": name,
        "sealed": False, "writerBlockId": writer_block_id, "backlogEstimate": None,
    })


PAGE_CAPPED = {"type": "page", "fields": {"pageParam": "page", "sizeParam": "size", "firstPage": "0", "sizeValue": "100", "maxPages": "1"}}
NONE_PAGINATION = {"type": "none", "fields": {}}

# ---- b-asset-detail gets an asset_id alias extract (id -> asset_id), prepended ----
by_id["b-asset-detail"]["transforms"].insert(0, extract("asset_id", "$.id", "b-asset-detail"))

new_blocks = []
new_topics = []

# ---- 1. asset_software ----
new_blocks.append(http_block(
    "b-asset-software", "Asset Software", "b-asset-detail",
    "/assets/${id}/software", "$.resources[*]", True, PAGE_CAPPED,
    meta_transforms("asset-software", "asset_software", "${asset_id}_${software_id}",
                     "asset=${id};page=${page}", "/assets/${id}/software"),
))
# software_id must be extracted before the meta transforms reference it
by_id_pending = new_blocks[-1]
by_id_pending["transforms"].insert(0, extract("software_id", "$.id", "b-asset-software"))
new_blocks.append(kafka_write_block("b-asset-software-write", "Publish Asset Software", "b-asset-software",
                                     "asset_software", "bronze.rapid7_securado.asset_software"))
new_topics.append(("b-asset-software-write", "bronze.rapid7_securado.asset_software"))

# ---- 2. asset_service (two-hop) ----
new_blocks.append(http_block(
    "b-asset-service-list", "Asset Services", "b-asset-detail",
    "/assets/${id}/services", "$.resources[*]", True, PAGE_CAPPED,
    [extract("protocol", "$.protocol", "b-asset-service-list"), extract("port", "$.port", "b-asset-service-list")],
))
new_blocks.append(http_block(
    "b-asset-service-detail", "Asset Service Detail", "b-asset-service-list",
    "/assets/${id}/services/${protocol}/${port}", "$", False, NONE_PAGINATION,
    meta_transforms("asset-service", "asset_service", "${asset_id}_${protocol}_${port}",
                     "asset=${id};protocol=${protocol};port=${port}",
                     "/assets/${id}/services/${protocol}/${port}"),
))
new_blocks.append(kafka_write_block("b-asset-service-write", "Publish Asset Service", "b-asset-service-detail",
                                     "asset_service", "bronze.rapid7_securado.asset_service"))
new_topics.append(("b-asset-service-write", "bronze.rapid7_securado.asset_service"))

# ---- 3. asset_vulnerability (two-hop) + asset_vulnerability_solution (three-hop) ----
new_blocks.append(http_block(
    "b-asset-vulnerability-list", "Asset Vulnerabilities", "b-asset-detail",
    "/assets/${id}/vulnerabilities", "$.resources[*]", True, PAGE_CAPPED,
    [extract("vulnerability_id", "$.id", "b-asset-vulnerability-list")],
))
new_blocks.append(http_block(
    "b-asset-vulnerability-detail", "Asset Vulnerability Detail", "b-asset-vulnerability-list",
    "/assets/${id}/vulnerabilities/${vulnerability_id}", "$", False, NONE_PAGINATION,
    meta_transforms("asset-vulnerability", "asset_vulnerability", "${asset_id}_${vulnerability_id}",
                     "asset=${id};vuln=${vulnerability_id}",
                     "/assets/${id}/vulnerabilities/${vulnerability_id}"),
))
new_blocks.append(kafka_write_block("b-asset-vulnerability-write", "Publish Asset Vulnerability", "b-asset-vulnerability-detail",
                                     "asset_vulnerability", "bronze.rapid7_securado.asset_vulnerability"))
new_topics.append(("b-asset-vulnerability-write", "bronze.rapid7_securado.asset_vulnerability"))

sol_transforms = [extract("solution_id", "$.id", "b-asset-vulnerability-solution")]
sol_transforms += meta_transforms("asset-vulnerability-solution", "asset_vulnerability_solution",
                                   "${asset_id}_${vulnerability_id}_${solution_id}",
                                   "asset=${id};vuln=${vulnerability_id}",
                                   "/assets/${id}/vulnerabilities/${vulnerability_id}/solution")
new_blocks.append(http_block(
    "b-asset-vulnerability-solution", "Asset Vulnerability Solution", "b-asset-vulnerability-detail",
    "/assets/${id}/vulnerabilities/${vulnerability_id}/solution", "$", False, NONE_PAGINATION,
    sol_transforms,
))
new_blocks.append(kafka_write_block("b-asset-vulnerability-solution-write", "Publish Asset Vulnerability Solution",
                                     "b-asset-vulnerability-solution", "asset_vulnerability_solution",
                                     "bronze.rapid7_securado.asset_vulnerability_solution"))
new_topics.append(("b-asset-vulnerability-solution-write", "bronze.rapid7_securado.asset_vulnerability_solution"))

# ---- 4. site_organization (singleton, child of b-site, excludes CCED Windows QUARTER like siblings) ----
org_branch = {
    "name": "exclude-cced-windows-quarter-orgpublish",
    "rules": [{"field": "name", "op": "not_equals", "value": "CCED Windows QUARTER"}],
    "match": "all",
}
new_blocks.append(http_block(
    "b-site-organization", "Site Organization", "b-site",
    "/sites/${site_id}/organization", "$", False, NONE_PAGINATION,
    meta_transforms("site-organization", "site_organization", "${site_id}", "site=${site_id}",
                     "/sites/${site_id}/organization"),
    branch=org_branch,
))
new_blocks.append(kafka_write_block("b-site-organization-write", "Publish Site Organization", "b-site-organization",
                                     "site_organization", "bronze.rapid7_securado.site_organization"))
new_topics.append(("b-site-organization-write", "bronze.rapid7_securado.site_organization"))

blocks.extend(new_blocks)
for writer_id, name in new_topics:
    add_topic(writer_id, name)

with open(".tmp_work/flow_9pey8p_group_a.json", "w") as f:
    json.dump(flow, f)

print("new block ids:", [b["id"] for b in new_blocks])
print("new topics:", new_topics)
print("total blocks:", len(blocks), "total topics:", len(topics))
