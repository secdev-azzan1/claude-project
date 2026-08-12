from __future__ import annotations

import copy
import json
import re
import uuid
from datetime import datetime
from typing import Any, Dict, List, Set, Tuple

from pymongo.errors import DuplicateKeyError

from services import content_store
from services.flow_import_credentials import FlowImportCredentialError, validate_import_credentials
from services.flow_file_compactor import PAGINATION_DEFAULTS, SOURCE_DEFAULTS, STREAM_DEFAULTS
from services.flow_import_preview import FlowImportPreviewError, preview_flow_import


class FlowImportFinalizeError(ValueError):
    pass


def _new_id() -> str:
    return str(uuid.uuid4())


def _require_dict(value: Any, label: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise FlowImportFinalizeError(f"{label} must be an object")
    return value


def _safe_artifact_id(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9_.:-]+", value or ""))


def _topic_token(value: Any, fallback: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")
    return text or fallback


def _derive_kafka_topic(flow_name: str, stream_name: Any) -> str:
    return f"bronze.{_topic_token(flow_name, 'flow')}.{_topic_token(stream_name, 'stream')}__history"


def _artifact_prefix(artifact_id: str) -> str:
    prefix = artifact_id.split(".", 1)[0].strip()
    return prefix if prefix and _safe_artifact_id(prefix) else "bronze"


def _stream_name_from_artifact_id(artifact_id: str) -> str:
    parts = artifact_id.split(".")
    suffix = parts[-1] if parts else artifact_id
    suffix = suffix.removesuffix("-value").removesuffix("__history")
    return suffix or "stream"


def _schema_stream_names(files: Dict[str, Any]) -> Dict[str, str]:
    names: Dict[str, str] = {}
    for path in _stream_paths(files):
        stream = files.get(path)
        if not isinstance(stream, dict):
            continue
        stream_name = str(stream.get("name") or stream.get("id") or "").strip()
        if not stream_name:
            continue
        entity = stream.get("entity")
        if not isinstance(entity, dict):
            continue
        artifact_ids = [entity.get("schema_artifact_id")]
        kafka = entity.get("kafka")
        if isinstance(kafka, dict):
            artifact_ids.append(kafka.get("schema_artifact_id"))
        for artifact_id in artifact_ids:
            if artifact_id:
                names[str(artifact_id)] = stream_name
    return names


def _derive_schema_artifact_id(old_artifact_id: str, flow_name: str, schema_stream_names: Dict[str, str]) -> str:
    stream_name = schema_stream_names.get(old_artifact_id) or _stream_name_from_artifact_id(old_artifact_id)
    return f"{_artifact_prefix(old_artifact_id)}.{_topic_token(flow_name, 'flow')}.{_topic_token(stream_name, 'stream')}__history-value"


def _rewrite_designer_payload_source_name(source_doc: Dict[str, Any], flow_name: str) -> None:
    payload = source_doc.get("designer_payload")
    if not isinstance(payload, dict):
        return

    source_type = str(payload.get("sourceType") or "").strip()
    config_keys = {
        "rest": "restSource",
        "postgres": "postgresSource",
        "mongo": "mongoSource",
        "smb": "smbSource",
        "webhook": "webhookSource",
    }
    keys = [config_keys[source_type]] if source_type in config_keys else list(config_keys.values())
    for key in keys:
        config = payload.get(key)
        if isinstance(config, dict):
            config["sourceName"] = flow_name


def _regenerate_stream_topics(streams: List[Dict[str, Any]], flow_name: str, primary_stream_id: Any) -> str:
    primary_topic = ""
    first_entity_topic = ""
    primary_id = str(primary_stream_id or "").strip()
    for stream in streams:
        stream_topic = _derive_kafka_topic(flow_name, stream.get("name") or stream.get("id"))
        if str(stream.get("id") or "").strip() == primary_id:
            primary_topic = stream_topic
        entity = stream.get("entity")
        if not isinstance(entity, dict):
            continue
        kafka = entity.get("kafka")
        if not isinstance(kafka, dict):
            kafka = {}
            entity["kafka"] = kafka
        kafka["topic"] = stream_topic
        if not first_entity_topic:
            first_entity_topic = stream_topic
    if not primary_topic:
        raise FlowImportFinalizeError("primary_stream_id does not exist in imported streams")
    return first_entity_topic or primary_topic


def _designer_entity_destination(stream: Dict[str, Any]) -> Dict[str, str]:
    entity = stream.get("entity")
    if not isinstance(entity, dict):
        return {}
    kafka = entity.get("kafka")
    if not isinstance(kafka, dict):
        kafka = {}

    schema_artifact_id = str(kafka.get("schema_artifact_id") or entity.get("schema_artifact_id") or "")
    schema_version = kafka.get("schema_version", entity.get("schema_version"))
    return {
        "partitions": str(kafka.get("partitions") or 6),
        "replicationFactor": str(kafka.get("replication_factor") or 3),
        "keyStrategy": str(kafka.get("key_strategy") or "none"),
        "valueFormat": str(kafka.get("value_format") or "avro"),
        "schemaMode": str(kafka.get("schema_mode") or "existing" if schema_artifact_id else "auto_generate"),
        "existingSchemaArtifactId": schema_artifact_id,
        "existingSchemaVersion": "" if schema_version in (None, "") else str(schema_version),
    }


def _designer_entity_config(stream: Dict[str, Any]) -> Dict[str, Any] | None:
    entity = stream.get("entity")
    if not isinstance(entity, dict):
        return None
    kafka = entity.get("kafka")
    if not isinstance(kafka, dict):
        kafka = {}

    schema_artifact_id = str(kafka.get("schema_artifact_id") or entity.get("schema_artifact_id") or "")
    schema_version = kafka.get("schema_version", entity.get("schema_version"))
    schema_version_text = "" if schema_version in (None, "") else str(schema_version)
    return {
        "kafka": {
            "topic": str(kafka.get("topic") or ""),
            "partitions": int(kafka.get("partitions") or 6),
            "replicationFactor": int(kafka.get("replication_factor") or 3),
            "keyStrategy": str(kafka.get("key_strategy") or "none"),
            "valueFormat": str(kafka.get("value_format") or "avro"),
            "schemaMode": str(kafka.get("schema_mode") or "existing" if schema_artifact_id else "auto_generate"),
            "schemaArtifactId": schema_artifact_id,
            "schemaVersion": schema_version_text,
        },
        "schemaArtifactId": schema_artifact_id,
        "schemaVersion": schema_version_text,
    }


def _sync_designer_payload_after_import(
    source_doc: Dict[str, Any],
    streams: List[Dict[str, Any]],
    stream_id_map: Dict[str, str],
    flow_name: str,
) -> None:
    payload = source_doc.get("designer_payload")
    if not isinstance(payload, dict):
        return

    _rewrite_designer_payload_source_name(source_doc, flow_name)

    stream_by_id = {str(stream.get("id") or ""): stream for stream in streams}
    stream_by_name = {str(stream.get("name") or ""): stream for stream in streams if stream.get("name")}

    raw_destinations = payload.get("entityDestinations")
    remapped_destinations: Dict[str, Any] = {}
    if isinstance(raw_destinations, dict):
        for old_key, value in raw_destinations.items():
            new_key = stream_id_map.get(str(old_key), str(old_key))
            remapped_destinations[new_key] = value

    payload_streams = payload.get("streams")
    if isinstance(payload_streams, list):
        for item in payload_streams:
            if not isinstance(item, dict):
                continue
            stream = stream_by_id.get(str(item.get("id") or "")) or stream_by_name.get(str(item.get("name") or ""))
            if not stream:
                continue
            stream_id = str(stream.get("id") or "")
            entity_config = _designer_entity_config(stream)
            if entity_config:
                item["entity"] = entity_config
                remapped_destinations[stream_id] = _designer_entity_destination(stream)
            else:
                item["entity"] = None

    payload["entityDestinations"] = remapped_destinations

    first_entity_stream = next((stream for stream in streams if isinstance(stream.get("entity"), dict)), None)
    if first_entity_stream:
        destination = _designer_entity_destination(first_entity_stream)
        kafka_output = payload.get("kafkaOutput")
        if not isinstance(kafka_output, dict):
            kafka_output = {}
            payload["kafkaOutput"] = kafka_output
        kafka = (first_entity_stream.get("entity") or {}).get("kafka") or {}
        if isinstance(kafka, dict):
            kafka_output["topic"] = str(kafka.get("topic") or "")
        kafka_output.update(destination)

        source_kafka = source_doc.get("kafka_output")
        if isinstance(source_kafka, dict) and isinstance(kafka, dict):
            source_kafka["topic"] = str(kafka.get("topic") or source_kafka.get("topic") or "")
            source_kafka["schema_artifact_id"] = destination["existingSchemaArtifactId"] or None
            source_kafka["schema_version"] = int(destination["existingSchemaVersion"] or 0) or None
            source_kafka["schema_mode"] = destination["schemaMode"]


def _set_nested(payload: Dict[str, Any], dotted_path: str, value: Any) -> None:
    target: Any = payload
    parts = dotted_path.split(".")
    index = 0
    while index < len(parts) - 1:
        if isinstance(target, dict):
            full_remaining = ".".join(parts[index:])
            if full_remaining in target:
                target[full_remaining] = value
                return
            remaining = ".".join(parts[index:-1])
            if remaining in target:
                target = target[remaining]
                index = len(parts) - 1
                break
            target = target.setdefault(parts[index], {})
            index += 1
            continue
        if isinstance(target, list):
            try:
                target = target[int(parts[index])]
            except (ValueError, IndexError) as exc:
                raise FlowImportFinalizeError(f"Cannot set credential field: {dotted_path}") from exc
            index += 1
            continue
        if not isinstance(target, dict):
            raise FlowImportFinalizeError(f"Cannot set credential field: {dotted_path}")
    if not isinstance(target, dict):
        raise FlowImportFinalizeError(f"Cannot set credential field: {dotted_path}")
    final_key = parts[-1]
    target[final_key] = value


def _replace_refs(value: Any, replacements: Dict[str, str], *, key: str | None = None) -> Any:
    if isinstance(value, str):
        if key == "name":
            return value
        return replacements.get(value, value)
    if isinstance(value, list):
        return [_replace_refs(item, replacements) for item in value]
    if isinstance(value, dict):
        return {child_key: _replace_refs(item, replacements, key=child_key) for child_key, item in value.items()}
    return value


def _reset_renamed_schema_version(version_doc: Dict[str, Any]) -> Dict[str, Any]:
    reset = copy.deepcopy(version_doc)
    reset["version"] = 1
    reset["status"] = "Needs Verification"
    for key in (
        "_id",
        "id",
        "verified_at",
        "verified_by",
        "apicurio_global_id",
        "apicurio_content_id",
        "apicurio_version",
        "registry_id",
        "registry_version",
        "global_id",
        "content_id",
    ):
        reset.pop(key, None)
    return reset


def _normalize_schema_versions_for_renames(value: Any, renamed_artifact_ids: Set[str]) -> Any:
    if isinstance(value, list):
        return [_normalize_schema_versions_for_renames(item, renamed_artifact_ids) for item in value]
    if not isinstance(value, dict):
        return value

    normalized: Dict[str, Any] = {}
    for key, item in value.items():
        normalized[key] = _normalize_schema_versions_for_renames(item, renamed_artifact_ids)

    if normalized.get("schema_artifact_id") in renamed_artifact_ids:
        normalized["schema_version"] = 1
    if normalized.get("schemaArtifactId") in renamed_artifact_ids:
        normalized["schemaVersion"] = "1"
    if normalized.get("existingSchemaArtifactId") in renamed_artifact_ids:
        normalized["existingSchemaVersion"] = "1"
    return normalized


def _hydrate_source_defaults(source_doc: Dict[str, Any]) -> Dict[str, Any]:
    hydrated = copy.deepcopy(source_doc)
    defaults = dict(SOURCE_DEFAULTS)
    if hydrated.get("application_service_id") and not hydrated.get("connection_setup_mode"):
        defaults["connection_setup_mode"] = "application_service"
    for key, value in defaults.items():
        hydrated.setdefault(key, value)
    return hydrated


def _hydrate_stream_defaults(stream_doc: Dict[str, Any]) -> Dict[str, Any]:
    hydrated = copy.deepcopy(stream_doc)
    for key, value in STREAM_DEFAULTS.items():
        hydrated.setdefault(key, value)
    pagination = hydrated.get("pagination")
    if not isinstance(pagination, dict):
        pagination = {}
    pagination = {**PAGINATION_DEFAULTS, **pagination}
    hydrated["pagination"] = pagination
    hydrated.pop("polling", None)
    return hydrated


def _stream_paths(files: Dict[str, Any]) -> List[str]:
    return sorted(
        path
        for path in files
        if path.startswith("source/streams/") and path.endswith(".json") and len(path.split("/")) == 3
    )


def _schema_artifact_paths(files: Dict[str, Any]) -> List[str]:
    return sorted(
        path
        for path in files
        if path.startswith("schemas/") and path.endswith("/artifact.json") and len(path.split("/")) == 3
    )


def _schema_version_paths(files: Dict[str, Any], artifact_id: str) -> List[str]:
    prefix = f"schemas/{artifact_id}/versions/"
    return sorted(path for path in files if path.startswith(prefix) and path.endswith(".json"))


def _application_service_docs(files: Dict[str, Any], credentials: Any) -> List[Dict[str, Any]]:
    path = "services/application.json"
    payload = files.get(path)
    if payload is None:
        return []
    group = copy.deepcopy(_require_dict(payload, path))
    for field, value in (credentials or {}).get(path, {}).items():
        _set_nested(group, field, value)
    services = group.get("services")
    if not isinstance(services, list):
        raise FlowImportFinalizeError("services/application.json.services must be a list")
    return [copy.deepcopy(_require_dict(service, f"{path}.services.{index}")) for index, service in enumerate(services)]


def _openapi_paths(files: Dict[str, Any]) -> List[str]:
    return sorted(path for path in files if path.startswith("openapi/") and path.endswith("/spec.json"))


def _schema_conflict(preview: Dict[str, Any], artifact_id: str) -> Dict[str, Any] | None:
    for item in preview.get("conflicts", {}).get("schema_artifacts", []) or []:
        if item.get("artifact_id") == artifact_id:
            return item
    return None


def _created_result(
    *,
    flow: bool,
    source: bool,
    streams: int,
    schemas: int,
    linked_schemas: int,
    application_services: int,
    nifi_services: int,
    openapi_specs: int,
) -> Dict[str, Any]:
    return {
        "flow": flow,
        "source": source,
        "streams": streams,
        "schemas": schemas,
        "linked_schemas": linked_schemas,
        "application_services": application_services,
        "nifi_services": nifi_services,
        "openapi_specs": openapi_specs,
    }


async def _flow_name_exists(db: Any, name: str) -> bool:
    return await db.flows.find_one({"name": name}, {"_id": 1}) is not None


async def _rollback(db: Any, inserted: Dict[str, List[str]]) -> None:
    for flow_id in inserted.get("flows", []):
        await db.flows.delete_one({"id": flow_id})
    for source_id in inserted.get("sources", []):
        await db.sources.delete_one({"id": source_id})
    for artifact_id in inserted.get("schema_artifacts", []):
        await db.schema_artifacts.delete_one({"artifact_id": artifact_id})
    for service_id in inserted.get("application_services", []):
        await db.application_services.delete_one({"id": service_id})
    for spec_id in inserted.get("openapi_specs", []):
        await db.openapi_specs.delete_one({"id": spec_id})


async def finalize_flow_import(raw: bytes, credentials: Any, options: Any, db: Any) -> Dict[str, Any]:
    options = _require_dict(options or {}, "options_json")
    flow_name = str(options.get("flow_name") or "").strip()
    if not flow_name:
        raise FlowImportFinalizeError("flow_name is required")
    if await _flow_name_exists(db, flow_name):
        raise FlowImportFinalizeError("Flow name already exists. Please choose a different name.")

    try:
        preview = await preview_flow_import(raw, db=db)
        await validate_import_credentials(raw, credentials, db=db)
        package = json.loads(raw.decode("utf-8"))
    except (FlowImportPreviewError, FlowImportCredentialError) as exc:
        raise FlowImportFinalizeError(str(exc)) from exc

    files = _require_dict(package.get("files"), "files")
    schema_resolutions = _require_dict(options.get("schema_resolutions") or {}, "schema_resolutions")

    flow_doc = copy.deepcopy(_require_dict(files["flow.json"], "flow.json"))
    source_doc = _hydrate_source_defaults(_require_dict(files["source/source.json"], "source/source.json"))
    if "primary_stream" in flow_doc:
        raise FlowImportFinalizeError("primary_stream is not supported; use primary_stream_id")
    if not str(flow_doc.get("primary_stream_id") or "").strip():
        raise FlowImportFinalizeError("primary_stream_id is required")

    app_service_id_map: Dict[str, str] = {}
    openapi_id_map: Dict[str, str] = {}
    stream_id_map: Dict[str, str] = {}
    schema_id_map: Dict[str, str] = {}
    replacements: Dict[str, str] = {}

    new_flow_id = _new_id()
    new_source_id = _new_id()
    replacements[str(flow_doc.get("id") or "")] = new_flow_id
    replacements[str(source_doc.get("id") or "")] = new_source_id

    app_service_docs: List[Dict[str, Any]] = []
    for doc in _application_service_docs(files, credentials):
        old_id = str(doc.get("id") or "")
        new_id = _new_id()
        app_service_id_map[old_id] = new_id
        replacements[old_id] = new_id
        doc["id"] = new_id
        doc["created_at"] = datetime.utcnow()
        doc["updated_at"] = datetime.utcnow()
        app_service_docs.append(doc)

    openapi_docs: List[Dict[str, Any]] = []
    for path in _openapi_paths(files):
        doc = copy.deepcopy(_require_dict(files[path], path))
        old_id = str(doc.get("id") or path.split("/")[1])
        new_id = _new_id()
        openapi_id_map[old_id] = new_id
        replacements[old_id] = new_id
        doc["id"] = new_id
        doc["created_at"] = datetime.utcnow()
        doc["updated_at"] = datetime.utcnow()
        openapi_docs.append(doc)

    schema_docs: List[Dict[str, Any]] = []
    linked_schemas = 0
    schema_stream_names = _schema_stream_names(files)
    renamed_artifact_ids: Set[str] = set()
    for artifact_path in _schema_artifact_paths(files):
        artifact = copy.deepcopy(_require_dict(files[artifact_path], artifact_path))
        old_artifact_id = str(artifact.get("artifact_id") or artifact_path.split("/")[1])
        conflict = _schema_conflict(preview, old_artifact_id)
        resolution = schema_resolutions.get(old_artifact_id) or {}
        action = str(resolution.get("action") or "").strip()
        is_rename = False

        if conflict and conflict.get("exists"):
            if action == "link_existing":
                if not conflict.get("compatible"):
                    raise FlowImportFinalizeError(f"Schema artifact {old_artifact_id} is not compatible with existing schema")
                schema_id_map[old_artifact_id] = old_artifact_id
                linked_schemas += 1
                continue
            if action != "rename":
                raise FlowImportFinalizeError(f"Schema artifact {old_artifact_id} needs a resolution")
            new_artifact_id = _derive_schema_artifact_id(old_artifact_id, flow_name, schema_stream_names)
            if new_artifact_id == old_artifact_id:
                raise FlowImportFinalizeError(f"Schema artifact {old_artifact_id} needs a different imported flow name")
            if await db.schema_artifacts.find_one({"artifact_id": new_artifact_id}, {"_id": 1}):
                raise FlowImportFinalizeError(f"Schema artifact already exists: {new_artifact_id}")
            is_rename = True
        else:
            new_artifact_id = old_artifact_id

        schema_id_map[old_artifact_id] = new_artifact_id
        replacements[old_artifact_id] = new_artifact_id
        artifact["artifact_id"] = new_artifact_id
        versions = []
        for version_path in _schema_version_paths(files, old_artifact_id):
            version_doc = copy.deepcopy(_require_dict(files[version_path], version_path))
            version_doc = _replace_refs(version_doc, {old_artifact_id: new_artifact_id})
            if is_rename:
                if versions:
                    continue
                version_doc = _reset_renamed_schema_version(version_doc)
            versions.append(version_doc)
        if is_rename:
            renamed_artifact_ids.add(new_artifact_id)
        artifact["versions"] = versions
        artifact["created_at"] = datetime.utcnow()
        artifact["updated_at"] = datetime.utcnow()
        schema_docs.append(artifact)

    stream_docs: List[Dict[str, Any]] = []
    for path in _stream_paths(files):
        stream = _hydrate_stream_defaults(_require_dict(files[path], path))
        old_id = str(stream.get("id") or path.split("/")[-1].removesuffix(".json"))
        new_id = _new_id()
        stream_id_map[old_id] = new_id
        replacements[old_id] = new_id
        stream_docs.append(stream)

    stream_docs = [_replace_refs(stream, replacements) for stream in stream_docs]
    if renamed_artifact_ids:
        stream_docs = [
            _normalize_schema_versions_for_renames(stream, renamed_artifact_ids) for stream in stream_docs
        ]
    for stream in stream_docs:
        old_name = stream.get("name")
        stream["id"] = stream_id_map.get(str(stream.get("id") or ""), stream.get("id"))
        if old_name:
            stream["name"] = old_name

    for field, value in (credentials or {}).get("source/source.json", {}).items():
        _set_nested(source_doc, field, value)
    source_doc = _replace_refs(source_doc, replacements)
    if renamed_artifact_ids:
        source_doc = _normalize_schema_versions_for_renames(source_doc, renamed_artifact_ids)
    source_doc["id"] = new_source_id
    source_doc["name"] = flow_name
    source_doc["streams"] = stream_docs
    source_doc["state"] = "Draft"
    source_doc["nifi_process_group_id"] = None
    source_doc["created_at"] = datetime.utcnow()
    source_doc["updated_at"] = datetime.utcnow()

    flow_doc = _replace_refs(flow_doc, replacements)
    if renamed_artifact_ids:
        flow_doc = _normalize_schema_versions_for_renames(flow_doc, renamed_artifact_ids)
    flow_doc["id"] = new_flow_id
    flow_doc["name"] = flow_name
    flow_doc["source_id"] = new_source_id
    flow_doc["source_type"] = source_doc.get("type") or flow_doc.get("source_type")
    flow_doc["kafka_topic"] = _regenerate_stream_topics(stream_docs, flow_name, flow_doc.get("primary_stream_id"))
    _sync_designer_payload_after_import(source_doc, stream_docs, stream_id_map, flow_name)
    flow_doc["state"] = "Draft"
    flow_doc["enabled"] = False
    flow_doc["nifi_process_group_id"] = None
    flow_doc["schema_inference_job_id"] = None
    flow_doc["schema_inference_active"] = False
    flow_doc.pop("schema_inference_started_at", None)
    flow_doc.pop("schema_inference_completed_at", None)
    flow_doc.pop("schema_inference_error", None)
    flow_doc["last_run"] = None
    flow_doc["total_records"] = 0
    flow_doc["total_errors"] = 0
    flow_doc["runs"] = []
    flow_doc.pop("deployed_at", None)
    flow_doc.pop("started_at", None)
    flow_doc.pop("stopped_at", None)
    flow_doc.pop("last_error", None)
    flow_doc["created_at"] = datetime.utcnow()
    flow_doc["updated_at"] = datetime.utcnow()

    inserted: Dict[str, List[str]] = {
        "flows": [],
        "sources": [],
        "schema_artifacts": [],
        "application_services": [],
        "openapi_specs": [],
    }
    try:
        for doc in app_service_docs:
            await db.application_services.insert_one(doc)
            inserted["application_services"].append(doc["id"])
        for doc in openapi_docs:
            await db.openapi_specs.insert_one(doc)
            inserted["openapi_specs"].append(doc["id"])
        await db.sources.insert_one(source_doc)
        inserted["sources"].append(new_source_id)
        for doc in schema_docs:
            await db.schema_artifacts.insert_one(doc)
            inserted["schema_artifacts"].append(doc["artifact_id"])
        await db.flows.insert_one(flow_doc)
        inserted["flows"].append(new_flow_id)
        await content_store.sync_flow(db, new_flow_id)
    except DuplicateKeyError as exc:
        await _rollback(db, inserted)
        content_store.delete_flow(new_flow_id, source_id=new_source_id)
        raise FlowImportFinalizeError(
            "Flow name or imported schema already exists. Please choose a different flow name.",
        ) from exc
    except Exception:
        await _rollback(db, inserted)
        content_store.delete_flow(new_flow_id, source_id=new_source_id)
        raise

    response_flow = content_store.to_jsonable({key: value for key, value in flow_doc.items() if key != "_id"})
    return {
        "ok": True,
        "flow": response_flow,
        "created": _created_result(
            flow=True,
            source=True,
            streams=len(stream_docs),
            schemas=len(schema_docs),
            linked_schemas=linked_schemas,
            application_services=len(app_service_docs),
            nifi_services=0,
            openapi_specs=len(openapi_docs),
        ),
    }
