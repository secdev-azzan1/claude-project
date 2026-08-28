import json

PLATFORM = "sentinelone"
TENANT = "sentinelone_securado"
SERVICE_ID = "svc-b09gdg"

# Ingestion-mechanics fields that change on every poll regardless of whether the
# underlying record changed -- must be excluded from the dedup hash or every
# record hashes differently every run and dedup becomes a no-op.
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
CRON = "0 */2 * * *"  # 2h cadence, matches the reference's own poll assumption

flows = []

# ============================================================================
# flow-s1-site: site (root, paginated) -> [site-write, site-policy (singleton,
# no dedup, always refetched)]. Dedup lives on site-write only, NOT on the
# shared root -- dedup on the root would drop unchanged-site FlowFiles before
# they ever reach the site-policy branch, silently starving policy refreshes
# for any site whose own record didn't change between polls.
# ============================================================================
site_blocks = [
    http_block("b-site", "List Sites", None, "/sites", "$.data.sites[*]", True, CURSOR_FULL,
               [extract("site_id", "$.id", "b-site")]),
    kafka_write_block("b-site-write", "Publish Site", "b-site", "site", f"bronze.{TENANT}.site",
                       meta_transforms("site", "site", "${site_id}", "cursor=${cursor}", "/sites") +
                       [dedup("site")]),
    http_block("b-site-policy", "Site Policy", "b-site", "/sites/${site_id}/policy", "$", False, NONE_PAGINATION,
               meta_transforms("site-policy", "site_policy", "${site_id}", "site=${site_id}",
                                "/sites/${site_id}/policy")),
]
site_blocks.append(kafka_write_block("b-site-policy-write", "Publish Site Policy", "b-site-policy",
                                      "site_policy", f"bronze.{TENANT}.site_policy"))
site_topics = [
    topic("b-site-write", f"bronze.{TENANT}.site"),
    topic("b-site-policy-write", f"bronze.{TENANT}.site_policy"),
]
flows.append({
    "id": "flow-s1-site", "name": "SentinelOne - Sites",
    "description": "Sites -> site policy (singleton, always refreshed), raw JSON to Kafka",
    "state": "Draft", "enabled": False, "cron": CRON,
    "blocks": site_blocks, "topics": site_topics, "variables": [], "servicePins": {},
})

# ============================================================================
# flow-s1-group: group (root, paginated) -> [group-write, group-policy]
# ============================================================================
group_blocks = [
    http_block("b-group", "List Groups", None, "/groups", "$.data[*]", True, CURSOR_FULL,
               [extract("group_id", "$.id", "b-group")]),
    kafka_write_block("b-group-write", "Publish Group", "b-group", "group", f"bronze.{TENANT}.group",
                       meta_transforms("group", "group", "${group_id}", "cursor=${cursor}", "/groups") +
                       [dedup("group")]),
    http_block("b-group-policy", "Group Policy", "b-group", "/groups/${group_id}/policy", "$", False, NONE_PAGINATION,
               meta_transforms("group-policy", "group_policy", "${group_id}", "group=${group_id}",
                                "/groups/${group_id}/policy")),
]
group_blocks.append(kafka_write_block("b-group-policy-write", "Publish Group Policy", "b-group-policy",
                                       "group_policy", f"bronze.{TENANT}.group_policy"))
group_topics = [
    topic("b-group-write", f"bronze.{TENANT}.group"),
    topic("b-group-policy-write", f"bronze.{TENANT}.group_policy"),
]
flows.append({
    "id": "flow-s1-group", "name": "SentinelOne - Groups",
    "description": "Groups -> group policy (singleton, always refreshed), raw JSON to Kafka",
    "state": "Draft", "enabled": False, "cron": CRON,
    "blocks": group_blocks, "topics": group_topics, "variables": [], "servicePins": {},
})

# ============================================================================
# flow-s1-threat: threat (root, 24h lookback, paginated) -> [threat-write,
# threat-timeline (paginated, dedup ok), threat-note (paginated, dedup ok)].
# Reference gates timeline/note fetches on "threat touched in last 4h"
# (RouteOnAttribute numeric-date compare -- branch-rule engine has no gt/date
# comparison, see plan item 2); built here as unconditional children instead
# -- bounded (~50 threats/24h window), not unbounded, just less optimized.
# ============================================================================
threat_blocks = [
    http_block(
        "b-threat", "List Threats", None,
        "/threats?updatedAt__gte=${now():toNumber():minus(86400000):format(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\", \"GMT\")}",
        "$.data[*]", True, CURSOR_FULL,
        [extract("threat_id", "$.id", "b-threat")],
    ),
    kafka_write_block("b-threat-write", "Publish Threat", "b-threat", "threat", f"bronze.{TENANT}.threat",
                       meta_transforms("threat", "threat", "${threat_id}", "updatedAt__gte=24h;cursor=${cursor}",
                                        "/threats") + [dedup("threat")]),
    http_block("b-threat-timeline", "Threat Timeline", "b-threat", "/threats/${threat_id}/timeline",
               "$.data[*]", True, CURSOR_FULL,
               [extract("timeline_id", "$.id", "b-threat-timeline")]),
    http_block("b-threat-note", "Threat Notes", "b-threat", "/threats/${threat_id}/notes",
               "$.data[*]", True, CURSOR_FULL,
               [extract("note_id", "$.id", "b-threat-note")]),
]
threat_blocks.append(kafka_write_block(
    "b-threat-timeline-write", "Publish Threat Timeline", "b-threat-timeline", "threat_timeline",
    f"bronze.{TENANT}.threat_timeline",
    meta_transforms("threat-timeline", "threat_timeline", "${threat_id}_${timeline_id}",
                     "threat=${threat_id};cursor=${cursor}", "/threats/${threat_id}/timeline") +
    [dedup("threat-timeline")],
))
threat_blocks.append(kafka_write_block(
    "b-threat-note-write", "Publish Threat Note", "b-threat-note", "threat_note",
    f"bronze.{TENANT}.threat_note",
    meta_transforms("threat-note", "threat_note", "${threat_id}_${note_id}",
                     "threat=${threat_id};cursor=${cursor}", "/threats/${threat_id}/notes") +
    [dedup("threat-note")],
))
threat_topics = [
    topic("b-threat-write", f"bronze.{TENANT}.threat"),
    topic("b-threat-timeline-write", f"bronze.{TENANT}.threat_timeline"),
    topic("b-threat-note-write", f"bronze.{TENANT}.threat_note"),
]
flows.append({
    "id": "flow-s1-threat", "name": "SentinelOne - Threats",
    "description": "Threats (24h lookback) -> threat timeline + threat notes (unconditional children), raw JSON to Kafka",
    "state": "Draft", "enabled": False, "cron": CRON,
    "blocks": threat_blocks, "topics": threat_topics, "variables": [], "servicePins": {},
})

for f in flows:
    with open(f".tmp_work/{f['id']}.json", "w") as fh:
        json.dump(f, fh)
    print(f["id"], "blocks:", len(f["blocks"]), "topics:", len(f["topics"]))
