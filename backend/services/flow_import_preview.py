from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional

from services import content_store
from services.flow_secret_refs import SECRETS_FILE, SECRET_REF_KEY, is_secret_ref


SUPPORTED_KIND = "lovable.flow.export"
SUPPORTED_FORMAT_VERSION = 2
MAX_FLOWPACK_BYTES = 25 * 1024 * 1024


class FlowImportPreviewError(ValueError):
    pass


def _canonical_checksum(value: Any) -> str:
    return hashlib.sha256(content_store.to_canonical_json(value).encode("utf-8")).hexdigest()


def _require_dict(value: Any, label: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise FlowImportPreviewError(f"{label} must be an object")
    return value


def _safe_path(path: str) -> bool:
    if not path or path.startswith("/") or "\\" in path:
        return False
    parts = path.split("/")
    return all(part not in {"", ".", ".."} for part in parts)


def _validate_path_layout(path: str) -> None:
    if path in {"flow.json", "source/source.json", SECRETS_FILE, "services/application.json", "services/nifi.json"}:
        return
    parts = path.split("/")
    if len(parts) == 3 and parts[0] == "source" and parts[1] == "streams" and parts[2].endswith(".json"):
        return
    if len(parts) == 3 and parts[0] == "schemas" and parts[2] == "artifact.json":
        return
    if len(parts) == 4 and parts[0] == "schemas" and parts[2] == "versions" and parts[3].endswith(".json"):
        return
    if len(parts) == 3 and parts[0] == "openapi" and parts[2] == "spec.json":
        return
    raise FlowImportPreviewError(f"Unsupported file path layout: {path}")


def _nested_value(payload: Dict[str, Any], dotted_path: str) -> Any:
    value: Any = payload
    parts = dotted_path.split(".")
    index = 0
    while index < len(parts):
        if isinstance(value, dict):
            remaining = ".".join(parts[index:])
            if remaining in value:
                return value.get(remaining)
            value = value.get(parts[index])
            index += 1
            continue
        if isinstance(value, list):
            try:
                value = value[int(parts[index])]
            except (ValueError, IndexError):
                return None
            index += 1
            continue
        if not isinstance(value, dict):
            return None
    return value


def _secret_file_value(files: Dict[str, Any], ref: str) -> Any:
    secrets = files.get(SECRETS_FILE)
    if not isinstance(secrets, dict):
        return "__missing_secret_file__"
    if ref not in secrets:
        return "__missing_secret_ref__"
    return secrets.get(ref)


def _schema_artifact_paths(files: Dict[str, Any]) -> List[str]:
    return sorted(
        path
        for path in files
        if path.startswith("schemas/") and path.endswith("/artifact.json") and len(path.split("/")) == 3
    )


def _stream_paths(files: Dict[str, Any]) -> List[str]:
    return sorted(
        path
        for path in files
        if path.startswith("source/streams/") and path.endswith(".json") and len(path.split("/")) == 3
    )


def _schema_version_paths(files: Dict[str, Any]) -> List[str]:
    return sorted(
        path
        for path in files
        if path.startswith("schemas/") and "/versions/" in path and path.endswith(".json")
    )


def _count_prefix(files: Dict[str, Any], prefix: str, suffix: str = ".json") -> int:
    return len([path for path in files if path.startswith(prefix) and path.endswith(suffix)])


def _grouped_service_count(files: Dict[str, Any], path: str) -> int:
    payload = files.get(path)
    if not isinstance(payload, dict):
        return 0
    services = payload.get("services")
    return len(services) if isinstance(services, list) else 0


def _validate_grouped_services(files: Dict[str, Any], path: str) -> None:
    payload = files.get(path)
    if payload is None:
        return
    payload = _require_dict(payload, path)
    services = payload.get("services")
    if not isinstance(services, list):
        raise FlowImportPreviewError(f"{path}.services must be a list")
    for index, service in enumerate(services):
        if not isinstance(service, dict):
            raise FlowImportPreviewError(f"{path}.services.{index} must be an object")
        if path == "services/nifi.json":
            service_kind = str(service.get("service_kind") or "").strip()
            resolution = str(service.get("resolution") or "default").strip()
            if service_kind in {"kafka", "schema_registry"} and resolution != "default":
                raise FlowImportPreviewError(f"{path}.services.{index}.resolution must be default")


def _nifi_service_resolutions(files: Dict[str, Any]) -> List[Dict[str, Any]]:
    payload = files.get("services/nifi.json")
    if not isinstance(payload, dict) or not isinstance(payload.get("services"), list):
        return []
    resolutions: List[Dict[str, Any]] = []
    for service in payload["services"]:
        if not isinstance(service, dict):
            continue
        service_kind = str(service.get("service_kind") or "").strip()
        if service_kind not in {"kafka", "schema_registry"}:
            continue
        resolutions.append({
            "service_kind": service_kind,
            "name": str(service.get("name") or service_kind).strip(),
            "resolution": "default",
            "import_enabled": False,
            "import_reason": "Imported global NiFi services are visible for review but cannot be activated while this environment enforces one default service per kind.",
        })
    return resolutions


def _schema_refs(payload: Any) -> List[str]:
    refs: List[str] = []
    if isinstance(payload, list):
        for item in payload:
            refs.extend(_schema_refs(item))
        return refs
    if not isinstance(payload, dict):
        return refs
    artifact_id = str(payload.get("schema_artifact_id") or "").strip()
    if artifact_id:
        refs.append(artifact_id)
    for value in payload.values():
        refs.extend(_schema_refs(value))
    return refs


def _validate_stream_graph_references(files: Dict[str, Any], stream_paths: List[str], exported_stream_ids: set[str]) -> None:
    def require_ref(value: Any, label: str) -> None:
        ref = str(value or "").strip()
        if ref and ref not in exported_stream_ids:
            raise FlowImportPreviewError(f"{label} must reference an exported stream id: {ref}")

    for path in stream_paths:
        stream = _require_dict(files[path], path)
        stream_id = str(stream.get("id") or path.split("/")[-1].removesuffix(".json")).strip()
        label = stream.get("name") or stream_id or path

        fan_out = stream.get("fan_out")
        if isinstance(fan_out, dict) and fan_out.get("enabled"):
            require_ref(fan_out.get("parent_stream_id"), f"Stream {label} fan_out.parent_stream_id")

        route_source = stream.get("route_source")
        if isinstance(route_source, dict):
            require_ref(route_source.get("parent_stream_id"), f"Stream {label} route_source.parent_stream_id")

        require_ref(stream.get("default_route_child_stream_id"), f"Stream {label} default_route_child_stream_id")

        routes = stream.get("routes")
        if isinstance(routes, list):
            for index, route in enumerate(routes):
                if not isinstance(route, dict):
                    raise FlowImportPreviewError(f"Stream {label} routes.{index} must be an object")
                require_ref(route.get("child_stream_id"), f"Stream {label} routes.{index}.child_stream_id")

        bindings = stream.get("parameter_bindings")
        if isinstance(bindings, list):
            for index, binding in enumerate(bindings):
                if not isinstance(binding, dict):
                    raise FlowImportPreviewError(f"Stream {label} parameter_bindings.{index} must be an object")
                require_ref(binding.get("parent_stream_id"), f"Stream {label} parameter_bindings.{index}.parent_stream_id")


async def _flow_name_exists(db: Any, name: str) -> bool:
    if not name:
        return False
    existing = await db.flows.find_one({"name": name}, {"_id": 1})
    return existing is not None


def _schema_body(value: Any) -> Optional[Any]:
    if isinstance(value, dict):
        if "avro_schema" in value:
            return value.get("avro_schema")
        if "schema" in value:
            return value.get("schema")
    return None


def _schema_bodies_match(left: Any, right: Any) -> bool:
    return content_store.to_canonical_json(left) == content_store.to_canonical_json(right)


def _exported_schema_bodies(files: Dict[str, Any], artifact_id: str) -> List[Any]:
    bodies: List[Any] = []
    prefix = f"schemas/{artifact_id}/versions/"
    for path, payload in files.items():
        if path.startswith(prefix) and path.endswith(".json"):
            body = _schema_body(payload)
            if body is not None:
                bodies.append(body)
    return bodies


async def _schema_conflicts(db: Any, files: Dict[str, Any], artifact_ids: List[str]) -> List[Dict[str, Any]]:
    conflicts: List[Dict[str, Any]] = []
    for artifact_id in artifact_ids:
        existing = await db.schema_artifacts.find_one({"artifact_id": artifact_id}, {"_id": 0})
        if existing:
            exported_bodies = _exported_schema_bodies(files, artifact_id)
            compatible_version: Optional[int] = None
            for version_doc in existing.get("versions") or []:
                existing_body = _schema_body(version_doc)
                if existing_body is None:
                    continue
                if any(_schema_bodies_match(existing_body, exported_body) for exported_body in exported_bodies):
                    compatible_version = version_doc.get("version")
                    break
            if compatible_version is not None:
                reason = f"Schema body matches existing version {compatible_version}"
                compatible = True
            else:
                reason = "Existing schema artifact has no matching exported schema version"
                compatible = False
            conflicts.append({
                "artifact_id": artifact_id,
                "exists": True,
                "compatible": compatible,
                "reason": reason,
            })
    return conflicts


async def preview_flow_import(raw: bytes, db: Any) -> Dict[str, Any]:
    if len(raw) > MAX_FLOWPACK_BYTES:
        raise FlowImportPreviewError(f"Flowpack is too large ({len(raw)} bytes).")
    try:
        package = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FlowImportPreviewError("Flowpack must be valid JSON") from exc

    package = _require_dict(package, "Flowpack")
    if package.get("kind") != SUPPORTED_KIND:
        raise FlowImportPreviewError(f"Unsupported flowpack kind: {package.get('kind')}")
    if package.get("format_version") != SUPPORTED_FORMAT_VERSION:
        raise FlowImportPreviewError(f"Unsupported flowpack format_version: {package.get('format_version')}")

    files = _require_dict(package.get("files"), "files")
    checksums = _require_dict(package.get("checksums"), "checksums")
    security = _require_dict(package.get("security"), "security")

    for path, payload in files.items():
        if not isinstance(path, str) or not _safe_path(path):
            raise FlowImportPreviewError(f"Unsafe file path: {path}")
        _validate_path_layout(path)
        expected = checksums.get(path)
        if not expected:
            raise FlowImportPreviewError(f"Missing checksum: {path}")
        if expected != _canonical_checksum(payload):
            raise FlowImportPreviewError(f"Checksum mismatch: {path}")

    for path in checksums:
        if path not in files:
            raise FlowImportPreviewError(f"Checksum has no matching file: {path}")

    for required_path in ("flow.json", "source/source.json"):
        if required_path not in files:
            raise FlowImportPreviewError(f"Missing required file: {required_path}")
    _validate_grouped_services(files, "services/application.json")
    _validate_grouped_services(files, "services/nifi.json")
    stream_paths = _stream_paths(files)
    if not stream_paths:
        raise FlowImportPreviewError("Missing required stream files")

    flow_doc = _require_dict(files["flow.json"], "flow.json")
    source_doc = _require_dict(files["source/source.json"], "source/source.json")
    if "primary_stream" in flow_doc:
        raise FlowImportPreviewError("flow.json.primary_stream is not supported; use primary_stream_id")
    primary_stream_id = str(flow_doc.get("primary_stream_id") or "").strip()
    if not primary_stream_id:
        raise FlowImportPreviewError("flow.json.primary_stream_id is required")
    exported_stream_ids = {
        str(_require_dict(files[path], path).get("id") or "").strip()
        for path in stream_paths
    }
    if primary_stream_id not in exported_stream_ids:
        raise FlowImportPreviewError("flow.json.primary_stream_id must reference an exported stream id")
    _validate_stream_graph_references(files, stream_paths, exported_stream_ids)

    schema_artifact_paths = _schema_artifact_paths(files)
    schema_ids = sorted({str(files[path].get("artifact_id") or path.split("/")[1]) for path in schema_artifact_paths})
    exported_schema_ids = {path.split("/")[1] for path in schema_artifact_paths}
    missing_schema_refs = sorted(
        {
            ref
            for stream_path in stream_paths
            for ref in _schema_refs(files.get(stream_path))
            if ref and ref not in exported_schema_ids
        }
    )
    if missing_schema_refs:
        raise FlowImportPreviewError(f"Missing schema artifact for stream reference: {missing_schema_refs[0]}")

    credentials_required = security.get("credentials_required") or []
    if not isinstance(credentials_required, list):
        raise FlowImportPreviewError("security.credentials_required must be a list")
    normalized_credentials: List[Dict[str, str]] = []
    for item in credentials_required:
        item = _require_dict(item, "credential requirement")
        path = str(item.get("path") or "").strip()
        field = str(item.get("field") or "").strip()
        if path not in files or not field:
            raise FlowImportPreviewError("Invalid credential requirement")
        payload = _require_dict(files[path], path)
        field_value = _nested_value(payload, field)
        if is_secret_ref(field_value):
            ref = field_value[SECRET_REF_KEY]
            if _secret_file_value(files, ref) is not None:
                raise FlowImportPreviewError(f"Credential secret value must be null: {ref}")
        elif field_value is not None:
            raise FlowImportPreviewError(f"Credential field must be null: {path}:{field}")
        normalized_credentials.append({"path": path, "field": field})

    secrets_doc = files.get(SECRETS_FILE)
    if secrets_doc is not None:
        secrets_doc = _require_dict(secrets_doc, SECRETS_FILE)
        for ref, value in secrets_doc.items():
            if value is not None:
                raise FlowImportPreviewError(f"Credential secret value must be null: {ref}")

    flow_name = str(flow_doc.get("name") or package.get("bundle", {}).get("name") or "").strip()
    source_name = str(source_doc.get("name") or "").strip()
    source_type = str(source_doc.get("type") or flow_doc.get("source_type") or "").strip()

    return {
        "ok": True,
        "summary": {
            "flow_name": flow_name,
            "source_name": source_name,
            "source_type": source_type,
            "streams": len(stream_paths),
            "schemas": len(schema_artifact_paths),
            "schema_versions": len(_schema_version_paths(files)),
            "application_services": _grouped_service_count(files, "services/application.json"),
            "nifi_services": _grouped_service_count(files, "services/nifi.json"),
            "openapi_specs": _count_prefix(files, "openapi/", suffix="/spec.json"),
            "credentials_required": len(normalized_credentials),
        },
        "credentials_required": normalized_credentials,
        "nifi_service_resolutions": _nifi_service_resolutions(files),
        "conflicts": {
            "flow_name_exists": await _flow_name_exists(db, flow_name),
            "schema_artifacts": await _schema_conflicts(db, files, schema_ids),
        },
        "errors": [],
        "warnings": [],
    }
