from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

from services import content_store
from services.flow_secret_refs import (
    SECRETS_FILE,
    is_openapi_metadata_url,
    is_secret_key,
    is_secret_ref,
    requires_import_reentry,
)


EXPORT_KIND = "lovable.flow.export"
EXPORT_FORMAT_VERSION = 2
EXPORT_FILE_EXTENSION = ".flowpack.json"

RUNTIME_KEYS = {
    "nifi_process_group_id",
    "nifi_processor_id",
    "nifi_processor_ids",
    "nifi_controller_service_id",
    "processor_id",
    "processor_ids",
    "controller_service_id",
    "controller_service_ids",
    "live",
    "last_run",
    "last_error",
    "runs",
    "total_records",
    "total_errors",
    "deployed_at",
    "started_at",
    "stopped_at",
    "schema_inference_job_id",
    "schema_inference_active",
    "schema_inference_started_at",
    "schema_inference_completed_at",
    "schema_inference_error",
}


def _bundle_id(flow: Dict[str, Any]) -> str:
    return _safe_filename_token(str(flow.get("name") or flow.get("id") or "")).strip()


def _source_required_fields_to_suppress(payload: Any, rel_path: str) -> Set[str]:
    if rel_path != "source/source.json" or not isinstance(payload, dict):
        return set()
    if payload.get("connection_setup_mode") != "application_service":
        return set()
    if not isinstance(payload.get("designer_payload"), dict):
        return set()
    return {
        "designer_payload.restSource.baseUrl",
        "designer_payload.restSource.username",
        "designer_payload.restSource.password",
    }


def _sanitize_payload(
    value: Any,
    rel_path: str,
    field_path: str = "",
    suppress_required_fields: Set[str] | None = None,
) -> Tuple[Any, List[Dict[str, str]]]:
    suppress_required_fields = suppress_required_fields or set()
    required: List[Dict[str, str]] = []
    if isinstance(value, list):
        sanitized_list = []
        for index, item in enumerate(value):
            child_path = f"{field_path}.{index}" if field_path else str(index)
            sanitized, child_required = _sanitize_payload(item, rel_path, child_path, suppress_required_fields)
            sanitized_list.append(sanitized)
            required.extend(child_required)
        return sanitized_list, required

    if not isinstance(value, dict):
        return value, required

    if rel_path == SECRETS_FILE:
        return {str(key): None for key in value}, required

    sanitized_dict: Dict[str, Any] = {}
    for key, item in value.items():
        if key == "_id" or key in RUNTIME_KEYS:
            continue
        next_path = f"{field_path}.{key}" if field_path else key
        if is_openapi_metadata_url(rel_path, next_path):
            sanitized_dict[key] = item
            continue
        if (is_secret_key(key) or requires_import_reentry(key)) and item not in (None, ""):
            sanitized_dict[key] = item if is_secret_ref(item) else None
            if next_path in suppress_required_fields or rel_path == "services/nifi.json":
                continue
            required.append({"path": rel_path, "field": next_path})
            continue
        sanitized, child_required = _sanitize_payload(item, rel_path, next_path, suppress_required_fields)
        sanitized_dict[key] = sanitized
        required.extend(child_required)
    return sanitized_dict, required


def _relative_json_files(bundle_dir: Path) -> List[Path]:
    return sorted(path for path in bundle_dir.rglob("*.json") if path.is_file())


def _posix_relative(path: Path, root: Path) -> str:
    rel = path.resolve().relative_to(root.resolve())
    return rel.as_posix()


def _checksum(value: Any) -> str:
    payload = content_store.to_canonical_json(value).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _safe_filename_token(value: str) -> str:
    token = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "_", value.strip())
    token = re.sub(r"\s+", " ", token).strip(" .")
    return token or "flow"


def export_filename(package: Dict[str, Any]) -> str:
    name = str((package.get("bundle") or {}).get("name") or "flow")
    return f"{_safe_filename_token(name)}{EXPORT_FILE_EXTENSION}"


def dumps_export_package(package: Dict[str, Any]) -> str:
    return json.dumps(package, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


async def build_flow_export_package(db: Any, flow_id: str) -> Dict[str, Any]:
    flow = await db.flows.find_one({"id": flow_id}, {"_id": 0})
    if not flow:
        raise ValueError("Flow not found")
    if flow.get("state") == "Draft":
        raise ValueError("Draft flow cannot be exported. Save changes first.")
    if not str(flow.get("primary_stream_id") or "").strip():
        raise ValueError("primary_stream_id is required before export")

    bundle_id = _bundle_id(flow)
    if not bundle_id:
        raise ValueError("Flow has no exportable bundle id")
    bundle_dir = content_store.flow_dir(bundle_id)

    files: Dict[str, Any] = {}
    checksums: Dict[str, str] = {}
    credentials_required: List[Dict[str, str]] = []

    payloads = await content_store.flow_bundle_payloads(db, flow)
    logical_schema_dirs = {
        path.parent.resolve(): str(raw_payload.get("artifact_id") or "")
        for path, raw_payload in payloads.items()
        if path.name == "artifact.json"
        and isinstance(raw_payload, dict)
        and str(raw_payload.get("artifact_id") or "").strip()
    }
    for path, raw_payload in sorted(payloads.items(), key=lambda item: str(item[0])):
        rel_path = _posix_relative(path, bundle_dir)
        resolved = path.resolve()
        schema_root = resolved.parent if resolved.name == "artifact.json" else resolved.parent.parent
        logical_artifact_id = logical_schema_dirs.get(schema_root)
        if logical_artifact_id:
            if resolved.name == "artifact.json":
                rel_path = f"schemas/{logical_artifact_id}/artifact.json"
            elif resolved.parent.name == "versions":
                rel_path = f"schemas/{logical_artifact_id}/versions/{resolved.name}"
        sanitized, required = _sanitize_payload(
            raw_payload,
            rel_path,
            suppress_required_fields=_source_required_fields_to_suppress(raw_payload, rel_path),
        )
        sanitized = content_store.to_jsonable(sanitized)
        files[rel_path] = sanitized
        checksums[rel_path] = _checksum(sanitized)
        credentials_required.extend(required)

    credentials_required.sort(key=lambda item: (item["path"], item["field"]))

    return {
        "kind": EXPORT_KIND,
        "format_version": EXPORT_FORMAT_VERSION,
        "exported_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "bundle": {
            "flow_id": str(flow.get("id") or ""),
            "source_id": str(flow.get("source_id") or ""),
            "bundle_id": bundle_id,
            "name": str(flow.get("name") or ""),
        },
        "security": {
            "secrets_sanitized": True,
            "credentials_required": credentials_required,
        },
        "files": files,
        "checksums": checksums,
    }
