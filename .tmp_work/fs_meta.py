"""Shared: the FortiSIEM meta convention + per-entity id specs.

Convention lifted verbatim from `flow-vipjvz`'s existing blocks (which match the
SentinelOne / Rapid7 stacks): 11 add_field keys + 1 dedup on object_id.
"""

SINK_SERVICE = "svc-dmya5u"          # "ice polaris" iceberg_catalog — same sink S1/R7 use
DEDUP_EXCLUDES = ["extraction_timestamp", "ingestion_run_batch_identity", "cursor_window"]


def meta_stack(entity, *, source_object_id, api_path, org_expr="${org_name}",
               cursor_window="", prefix="t-meta"):
    """The 11 keys + dedup, in the same order the existing flows use."""
    fields = [
        ("source_platform", "fortisiem"),
        ("customer_tenant_organization", org_expr),
        ("source_object_type", entity),
        ("source_object_id", source_object_id),
        ("object_id", f"fortisiem:{org_expr}:{entity}:{source_object_id}"),
        ("cursor_window", cursor_window),
        ("api_endpoint_export_query_identity", api_path),
        ("ingest_ts", "${now():toNumber()}"),
        ("extraction_timestamp", "${now():format(\"yyyy-MM-dd'T'HH:mm:ss.SSSXXX\")}"),
        ("ingestion_run_batch_identity", org_expr + "-${now():toNumber()}-${uuid}"),
        ("source_event_update_timestamp", ""),
    ]
    out = [{"id": f"{prefix}-{entity}-{f}", "kind": "add_field",
            "config": {"field": f, "value": v}} for f, v in fields]
    out.append({"id": f"{prefix}-{entity}-dedup", "kind": "dedup",
                "config": {"identityFields": ["object_id"],
                           "excludedFields": list(DEDUP_EXCLUDES), "windowHours": 24}})
    return out


def avro_twin(kafka_block, sink_service=SINK_SERVICE):
    """A kafka_kc sibling of an existing kafka/write block.

    Mirrors `flow-s1-site`'s shape exactly: same parentId and entity, `mode:
    null`, empty config, the sink service on `serviceId`, and a copy of the
    kafka sibling's transform stack with `-avro-` ids so the two branches carry
    identical metadata. No entry is added to `topics[]` — the Avro topic is
    derived by the app, exactly as it is for SentinelOne.
    """
    tf = []
    for t in kafka_block.get("transforms") or []:
        c = dict(t)
        c["id"] = t["id"].replace("t-meta-", "t-meta-avro-", 1) if t["id"].startswith("t-meta-") \
            else f"{t['id']}-avro"
        tf.append(c)
    return {
        "id": f"{kafka_block['id']}-avro",
        "adapter": "kafka_kc",
        "mode": None,
        "name": f"{kafka_block.get('name') or kafka_block['id']} Avro",
        "parentId": kafka_block.get("parentId"),
        "branch": kafka_block.get("branch"),
        "serviceId": sink_service,
        "entity": kafka_block.get("entity"),
        "config": {},
        "transforms": tf,
        "topicOverride": None,
        "testResult": None,
    }


def extract(attr, path, bid=None):
    return {"id": f"t-extract-{attr}-{bid or attr}", "kind": "extract",
            "config": {"attribute": attr, "path": path, "default": ""}}
