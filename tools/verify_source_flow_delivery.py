"""Verify source-flow runtime state and the Desktop/To Send delivery tree."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any

import requests


APP_BASE = "http://127.0.0.1:8010"
PREFIXES = ("Rapid7 Securado", "SentinelOne", "FortiSIEM")
EXCLUDES = [
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


def get(path: str) -> Any:
    response = requests.get(f"{APP_BASE}{path}", timeout=60)
    response.raise_for_status()
    return response.json()


def slug(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip("-.") or "unnamed"


def family(name: str) -> str:
    if name.startswith("Rapid7 Securado"):
        return "Rapid7 Securado"
    if name.startswith("SentinelOne"):
        return "SentinelOne"
    return "FortiSIEM"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = args.output.resolve()
    errors: list[str] = []

    summaries = get("/api/v2/flows")
    summaries = summaries if isinstance(summaries, list) else summaries.get("items", [])
    flows = [get(f"/api/v2/flows/{item['id']}") for item in summaries if item.get("name", "").startswith(PREFIXES)]
    schemas = get("/api/v2/schemas").get("approved") or []
    syncs = get("/api/kafka-connect/syncs")

    writer_count = 0
    for flow in flows:
        if flow.get("state") != "Stopped" or not flow.get("deployedAt") or not flow.get("nifiProcessGroupId"):
            errors.append(f"{flow['id']}: incomplete stopped deployment")
        blocks = {block.get("id"): block for block in flow.get("blocks") or []}
        for writer in blocks.values():
            if writer.get("adapter") not in {"kafka", "kafka_kc"}:
                continue
            writer_count += 1
            transforms = writer.get("transforms") or []
            dedups = [item for item in transforms if item.get("kind") == "dedup"]
            if len(dedups) != 1 or transforms[-1].get("kind") != "dedup":
                errors.append(f"{flow['id']}/{writer['id']}: dedup is not unique and last")
            elif dedups[0].get("config") != {
                "identityFields": ["object_id"],
                "excludedFields": EXCLUDES,
                "windowHours": 24,
                "dedupEpoch": dedups[0].get("config", {}).get("dedupEpoch"),
            }:
                errors.append(f"{flow['id']}/{writer['id']}: non-standard dedup config")

            lineage = []
            current = writer
            while current:
                lineage.append(current)
                current = blocks.get(current.get("parentId"))
            field_values = {
                item.get("config", {}).get("field"): item.get("config", {}).get("value")
                for block in lineage
                for item in block.get("transforms") or []
                if item.get("kind") == "add_field"
            }
            missing = {"source_object_id", "object_id"} - set(field_values)
            if missing:
                errors.append(f"{flow['id']}/{writer['id']}: missing identity fields {sorted(missing)}")
            elif str(field_values["object_id"]).count(":") < 3:
                errors.append(f"{flow['id']}/{writer['id']}: object_id is not a four-part composite")

        flow_dir = root / family(flow["name"]) / slug(flow["name"])
        app_path = flow_dir / "application" / "flow.json"
        nifi_path = flow_dir / "nifi" / f"{slug(flow['name'])}.json"
        if not app_path.is_file() or not nifi_path.is_file():
            errors.append(f"{flow['id']}: missing application or NiFi export")
            continue
        exported_flow = json.loads(app_path.read_text(encoding="utf-8"))
        nifi = json.loads(nifi_path.read_text(encoding="utf-8"))
        if exported_flow.get("nifiProcessGroupId") != flow.get("nifiProcessGroupId"):
            errors.append(f"{flow['id']}: application export has stale process-group id")
        if (nifi.get("flowContents") or {}).get("instanceIdentifier") != flow.get("nifiProcessGroupId"):
            errors.append(f"{flow['id']}: NiFi download does not match process-group id")

    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("flowCount") != len(flows):
        errors.append("manifest flow count mismatch")
    if manifest.get("schemaCount") != sum(1 for schema in schemas if any(schema.get("flowId") == f["id"] for f in flows)):
        errors.append("manifest schema count mismatch")
    expected_syncs = sum(1 for sync in syncs if any(sync.get("linked_flow_id") == f["id"] for f in flows))
    if manifest.get("applicationSyncCount") != expected_syncs:
        errors.append("manifest application sync count mismatch")

    report = {
        "ok": not errors,
        "flowCount": len(flows),
        "writerCount": writer_count,
        "stoppedDeploymentCount": sum(
            1 for flow in flows if flow.get("state") == "Stopped" and flow.get("deployedAt") and flow.get("nifiProcessGroupId")
        ),
        "approvedSchemaCount": manifest.get("schemaCount"),
        "applicationSyncCount": manifest.get("applicationSyncCount"),
        "liveConnectorConfigCount": manifest.get("liveConnectorConfigCount"),
        "schemaCoverageGapCount": len(json.loads((root / "schema-coverage-gaps.json").read_text(encoding="utf-8"))),
        "errors": errors,
    }
    report_path = root / "verification-report.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    files = sorted(path for path in root.rglob("*") if path.is_file() and path.name != "checksums.sha256")
    lines = [f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(root).as_posix()}" for path in files]
    (root / "checksums.sha256").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
