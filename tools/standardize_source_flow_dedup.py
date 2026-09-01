"""Standardize object identity and deduplication on source-family v2 flows.

This utility intentionally uses the application's public v2 API for every flow
mutation.  Run without ``--apply`` for a read-only plan, then with ``--apply``
to save the idempotent changes.
"""

from __future__ import annotations

import argparse
import copy
import json
from typing import Any

import requests


API = "http://127.0.0.1:8010/api/v2"
FAMILY_PREFIXES = ("Rapid7 Securado", "SentinelOne", "FortiSIEM")
STANDARD_EXCLUDES = [
    "source_platform",
    "customer_tenant_organization",
    "source_object_type",
    "source_object_id",
    "object_id",
    "cursor_window",
    "api_endpoint_export_query_identity",
    "ingest_ts",
    "extraction_timestamp",
    "ingestion_run_batch_identity",
    "source_event_update_timestamp",
]


def get_json(path: str) -> Any:
    response = requests.get(f"{API}{path}", timeout=30)
    response.raise_for_status()
    return response.json()


def list_flows() -> list[dict[str, Any]]:
    payload = get_json("/flows")
    if isinstance(payload, list):
        return payload
    return payload.get("items") or payload.get("flows") or []


def add_field(field: str, value: str, suffix: str) -> dict[str, Any]:
    return {
        "id": f"t-standard-{suffix}-{field}",
        "kind": "add_field",
        "config": {"field": field, "value": value},
    }


def ensure_demo_identity(flow: dict[str, Any], changes: list[str]) -> None:
    """Give the two legacy CMDB pagination demos a stable natural composite key."""
    if flow.get("id") not in {"flow-pgdemo1", "flow-pgdemo2"}:
        return
    blocks = {block.get("id"): block for block in flow.get("blocks") or []}
    source = blocks.get("b-cmdb")
    writer = blocks.get("b-pub")
    if not source or not writer:
        return

    transforms = source.setdefault("transforms", [])
    extracts = {
        transform.get("config", {}).get("attribute"): transform
        for transform in transforms
        if transform.get("kind") == "extract"
    }
    for attribute, path in (("customer_id", "$.Customer_ID"), ("user_name", "$.User_Name")):
        if attribute not in extracts:
            transforms.append(
                {
                    "id": f"t-standard-cmdb-{attribute}",
                    "kind": "extract",
                    "config": {"attribute": attribute, "path": path, "default": ""},
                }
            )
            changes.append(f"{source['id']}: extract {attribute}")

    values = {
        "source_platform": "fortisiem",
        "customer_tenant_organization": "${customer_id}",
        "source_object_type": "cmdb_user",
        "source_object_id": "${customer_id}_${user_name}",
        "object_id": "fortisiem:${customer_id}:cmdb_user:${customer_id}_${user_name}",
        "cursor_window": "offset-pagination",
        "api_endpoint_export_query_identity": "/query/cmdb:USER",
        "ingest_ts": "${now():toNumber()}",
        "extraction_timestamp": "${now():format(\"yyyy-MM-dd'T'HH:mm:ss.SSSXXX\")}",
        "ingestion_run_batch_identity": "fortisiem-${now():toNumber()}-${uuid}",
        "source_event_update_timestamp": "",
    }
    writer_transforms = writer.setdefault("transforms", [])
    existing = {
        transform.get("config", {}).get("field"): transform
        for transform in writer_transforms
        if transform.get("kind") == "add_field"
    }
    for field, value in values.items():
        if field not in existing:
            writer_transforms.append(add_field(field, value, "cmdb-user"))
            changes.append(f"{writer['id']}: add {field}")


def align_sentinel_native_ids(flow: dict[str, Any], changes: list[str]) -> None:
    """Use SentinelOne's native child IDs, as verified in its E2E journey."""
    replacements = {
        "threat_timeline": ("${timeline_id}", "sentinelone:sentinelone_securado:threat_timeline:${timeline_id}"),
        "threat_note": ("${note_id}", "sentinelone:sentinelone_securado:threat_note:${note_id}"),
    }
    for block in flow.get("blocks") or []:
        entity = block.get("entity")
        if entity not in replacements:
            continue
        source_id, object_id = replacements[entity]
        for transform in block.get("transforms") or []:
            if transform.get("kind") != "add_field":
                continue
            config = transform.get("config") or {}
            field = config.get("field")
            target = source_id if field == "source_object_id" else object_id if field == "object_id" else None
            if target is not None and config.get("value") != target:
                config["value"] = target
                changes.append(f"{block['id']}: align {field} to native {entity} id")


def standardize_writer_dedup(flow: dict[str, Any], changes: list[str]) -> None:
    for block in flow.get("blocks") or []:
        if block.get("adapter") not in {"kafka", "kafka_kc"}:
            continue
        transforms = block.setdefault("transforms", [])
        dedups = [transform for transform in transforms if transform.get("kind") == "dedup"]
        dedup = dedups[-1] if dedups else {
            "id": f"t-standard-dedup-{block['id']}",
            "kind": "dedup",
            "config": {},
        }
        desired = {
            "identityFields": ["object_id"],
            "excludedFields": STANDARD_EXCLUDES,
            "windowHours": 24,
        }
        prior_epoch = (dedup.get("config") or {}).get("dedupEpoch")
        desired["dedupEpoch"] = prior_epoch if isinstance(prior_epoch, int) else 1
        if dedup.get("config") != desired:
            dedup["config"] = copy.deepcopy(desired)
            changes.append(f"{block['id']}: standardize dedup")
        # A dedup transform must be unique and last in its branch.
        non_dedup = [transform for transform in transforms if transform.get("kind") != "dedup"]
        normalized = non_dedup + [dedup]
        if normalized != transforms:
            block["transforms"] = normalized
            if not dedups:
                changes.append(f"{block['id']}: add dedup")
            elif len(dedups) > 1:
                changes.append(f"{block['id']}: remove duplicate dedup rules")


def verify_identity(flow: dict[str, Any]) -> list[str]:
    blocks = {block.get("id"): block for block in flow.get("blocks") or []}
    errors: list[str] = []
    for writer in blocks.values():
        if writer.get("adapter") not in {"kafka", "kafka_kc"}:
            continue
        lineage: list[dict[str, Any]] = []
        current = writer
        while current:
            lineage.append(current)
            current = blocks.get(current.get("parentId"))
        fields = {
            transform.get("config", {}).get("field")
            for item in lineage
            for transform in item.get("transforms") or []
            if transform.get("kind") == "add_field"
        }
        missing = {"source_object_id", "object_id"} - fields
        if missing:
            errors.append(f"{writer['id']} missing {sorted(missing)} in its lineage")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    summaries = []
    for summary in list_flows():
        if not str(summary.get("name", "")).startswith(FAMILY_PREFIXES):
            continue
        flow = get_json(f"/flows/{summary['id']}")
        changes: list[str] = []
        ensure_demo_identity(flow, changes)
        align_sentinel_native_ids(flow, changes)
        standardize_writer_dedup(flow, changes)
        errors = verify_identity(flow)
        if errors:
            raise RuntimeError(f"{flow['name']}: " + "; ".join(errors))
        if args.apply and changes:
            response = requests.post(f"{API}/flows/", json=flow, timeout=60)
            response.raise_for_status()
        summaries.append({"id": flow["id"], "name": flow["name"], "changes": changes})

    print(json.dumps({"applied": args.apply, "flowCount": len(summaries), "flows": summaries}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
