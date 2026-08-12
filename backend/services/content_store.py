from __future__ import annotations

import json
import hashlib
import os
import re
import shutil
import tempfile
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from services.flow_file_compactor import compact_flow_file
from services.flow_secret_refs import extract_secret_refs_from_payloads


BACKEND_DIR = Path(__file__).resolve().parents[1]
DEFAULT_CONTENT_STORE_ROOT = BACKEND_DIR / ".content-store"
MAX_PATH_SEGMENT_LENGTH = 64


@dataclass
class ContentStoreReport:
    ok: bool = True
    root: str = ""
    written: List[str] = field(default_factory=list)
    deleted: List[str] = field(default_factory=list)
    missing: List[str] = field(default_factory=list)
    stale: List[str] = field(default_factory=list)
    extra: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    counts: Dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok and not self.errors and not self.missing and not self.stale and not self.extra,
            "root": self.root,
            "written": self.written,
            "deleted": self.deleted,
            "missing": self.missing,
            "stale": self.stale,
            "extra": self.extra,
            "errors": self.errors,
            "counts": self.counts,
        }


def get_content_store_root() -> Path:
    configured = os.environ.get("CONTENT_STORE_ROOT")
    if not configured:
        return DEFAULT_CONTENT_STORE_ROOT.expanduser().resolve()
    p = Path(configured).expanduser()
    if not p.is_absolute():
        p = BACKEND_DIR.parent / p  # anchor: repo root, NOT cwd
    return p.resolve()


def to_jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items() if k != "_id"}
    if isinstance(value, list):
        return [to_jsonable(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def to_canonical_json(value: Any) -> str:
    return json.dumps(to_jsonable(value), indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def _root_child(*parts: str) -> Path:
    return get_content_store_root().joinpath(*[str(part) for part in parts]).resolve()


def _bounded_path_segment(value: Any) -> str:
    text = str(value or "")
    if len(text) <= MAX_PATH_SEGMENT_LENGTH:
        return text
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]
    prefix_length = MAX_PATH_SEGMENT_LENGTH - len(digest) - 2
    return f"{text[:prefix_length]}--{digest}"


def flow_dir(flow_id: str) -> Path:
    return _root_child("flows", _bounded_path_segment(flow_id))


def flow_path(flow_id: str) -> Path:
    return flow_dir(flow_id) / "flow.json"


def secrets_path(flow_id: str) -> Path:
    return flow_dir(flow_id) / "secrets.json"


def source_path(flow_id: str) -> Path:
    return flow_dir(flow_id) / "source" / "source.json"


def stream_path(flow_id: str, stream_id: str) -> Path:
    return flow_dir(flow_id) / "source" / "streams" / f"{stream_id}.json"


def application_services_path(flow_id: str) -> Path:
    return flow_dir(flow_id) / "services" / "application.json"


def nifi_services_path(flow_id: str) -> Path:
    return flow_dir(flow_id) / "services" / "nifi.json"


def schema_dir(flow_id: str, artifact_id: str) -> Path:
    return flow_dir(flow_id) / "schemas" / _bounded_path_segment(artifact_id)


def schema_artifact_path(flow_id: str, artifact_id: str) -> Path:
    return schema_dir(flow_id, artifact_id) / "artifact.json"


def schema_version_path(flow_id: str, artifact_id: str, version: int | str) -> Path:
    return schema_dir(flow_id, artifact_id) / "versions" / f"{version}.json"


def openapi_spec_path(flow_id: str, spec_id: str) -> Path:
    return flow_dir(flow_id) / "openapi" / spec_id / "spec.json"


def manifest_path() -> Path:
    return _root_child("manifest.json")


def _assert_inside_root(path: Path) -> Path:
    root = get_content_store_root()
    resolved = path.resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"Refusing to modify path outside content store root: {resolved}") from exc
    return resolved


def atomic_write_json(path: Path, value: Any) -> None:
    target = _assert_inside_root(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = to_canonical_json(value)
    fd, tmp_name = tempfile.mkstemp(prefix=".t", suffix="", dir=str(target.parent))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, target)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def safe_delete_file(path: Path) -> None:
    target = _assert_inside_root(path)
    if target.exists():
        target.unlink()


def safe_delete_tree(path: Path) -> None:
    target = _assert_inside_root(path)
    if target.exists():
        shutil.rmtree(target)


def _require_id(value: Any, label: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{label} id is required")
    return text


def _safe_folder_name(value: Any) -> str:
    text = str(value or "").strip()
    text = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "_", text)
    text = re.sub(r"\s+", " ", text).strip(" .")
    return text


async def _find_one(collection: Any, query: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return await collection.find_one(query, {"_id": 0})


async def _find_all(collection: Any) -> List[Dict[str, Any]]:
    return await collection.find({}, {"_id": 0}).to_list(10000)


def _write_report(report: Optional[ContentStoreReport], path: Path, payload: Dict[str, Any]) -> None:
    atomic_write_json(path, payload)
    if report is not None:
        report.written.append(str(path))


def _delete_report_tree(report: Optional[ContentStoreReport], path: Path) -> None:
    if path.exists():
        safe_delete_tree(path)
        if report is not None:
            report.deleted.append(str(path))


def _delete_bundle_dirs_for_flow(report: Optional[ContentStoreReport], flow_id: str, keep_dir: Optional[Path] = None) -> None:
    flows_root = _root_child("flows")
    if not flows_root.exists():
        return
    keep = keep_dir.resolve() if keep_dir else None
    for candidate in flows_root.iterdir():
        if not candidate.is_dir():
            continue
        resolved = candidate.resolve()
        if keep is not None and resolved == keep:
            continue
        flow_file = candidate / "flow.json"
        if not flow_file.exists():
            continue
        try:
            payload = json.loads(flow_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if str(payload.get("id") or "").strip() == flow_id:
            _delete_report_tree(report, candidate)


def _stream_id(stream: Dict[str, Any], index: int) -> str:
    return str(stream.get("id") or stream.get("name") or index).strip()


def _bundle_id(flow: Dict[str, Any]) -> str:
    return _require_id(_safe_folder_name(flow.get("name")) or flow.get("id"), "flow bundle")


def _timestamp_value(value: Any) -> float:
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return 0.0
    return 0.0


def _flow_rank(flow: Dict[str, Any]) -> Tuple[int, float, float, str]:
    state = str(flow.get("state") or "").strip().lower()
    state_rank = {"running": 3, "deployed": 2, "stopped": 1, "draft": 0}.get(state, 0)
    updated = _timestamp_value(flow.get("updated_at"))
    created = _timestamp_value(flow.get("created_at"))
    return (state_rank, updated, created, str(flow.get("id") or ""))


def _canonical_flows(flows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_bundle: Dict[str, Dict[str, Any]] = {}
    for flow in flows:
        try:
            bid = _bundle_id(flow)
        except ValueError:
            continue
        existing = by_bundle.get(bid)
        if existing is None or _flow_rank(flow) > _flow_rank(existing):
            by_bundle[bid] = flow
    return list(by_bundle.values())


def _schema_refs_from_container(container: Optional[Dict[str, Any]]) -> Set[Tuple[str, Optional[int]]]:
    refs: Set[Tuple[str, Optional[int]]] = set()
    if not isinstance(container, dict):
        return refs

    artifact_id = str(container.get("schema_artifact_id") or "").strip()
    version = container.get("schema_version")
    if not artifact_id:
        kafka = container.get("kafka")
        if isinstance(kafka, dict):
            artifact_id = str(kafka.get("schema_artifact_id") or "").strip()
            version = kafka.get("schema_version")
    if artifact_id:
        try:
            parsed_version = int(version) if version not in (None, "") else None
        except (TypeError, ValueError):
            parsed_version = None
        refs.add((artifact_id, parsed_version))
    return refs


def _schema_refs(flow: Dict[str, Any], source: Optional[Dict[str, Any]], streams: Optional[List[Dict[str, Any]]] = None) -> Set[Tuple[str, Optional[int]]]:
    refs = _schema_refs_from_container(flow)
    if isinstance(source, dict):
        refs.update(_schema_refs_from_container(source.get("kafka_output")))
        for stream in streams if streams is not None else source.get("streams") or []:
            if not isinstance(stream, dict):
                continue
            refs.update(_schema_refs_from_container(stream.get("entity")))
            refs.update(_schema_refs_from_container((stream.get("entity") or {}).get("kafka") if isinstance(stream.get("entity"), dict) else None))
    return refs


def _nifi_service_ids(source: Optional[Dict[str, Any]]) -> Set[str]:
    if not isinstance(source, dict):
        return set()
    refs = source.get("nifi_service_refs")
    if not isinstance(refs, dict):
        return set()
    return {str(value).strip() for value in refs.values() if str(value or "").strip()}


def _has_kafka_output(flow: Dict[str, Any], source: Optional[Dict[str, Any]], streams: Optional[List[Dict[str, Any]]] = None) -> bool:
    if str(flow.get("kafka_topic") or "").strip():
        return True
    if isinstance(source, dict) and isinstance(source.get("kafka_output"), dict):
        if str(source["kafka_output"].get("topic") or "").strip():
            return True
    for stream in streams or []:
        entity = stream.get("entity") if isinstance(stream, dict) else None
        kafka = entity.get("kafka") if isinstance(entity, dict) else None
        if isinstance(kafka, dict) and str(kafka.get("topic") or "").strip():
            return True
    return bool(_schema_refs(flow, source, streams))


async def _default_nifi_service(db: Any, service_kind: str) -> Optional[Dict[str, Any]]:
    services = [
        service
        for service in await _find_all(db.nifi_global_services)
        if str(service.get("service_kind") or "").strip() == service_kind
    ]
    defaults = [service for service in services if bool(service.get("is_default"))]
    if defaults:
        return sorted(defaults, key=lambda item: str(item.get("id") or ""))[0]
    if len(services) == 1:
        return services[0]
    return None


_SERVICE_SECRET_HINTS = ("password", "secret", "token", "key")


def _redact_service_secrets(service: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of a NiFi global-service doc with secret-looking property
    values removed. Global-service secrets must never be written to the
    content-store bundle as recoverable plaintext (decision 2: they require
    re-entry). The secret-ref extraction only catches ``_password``-style keys,
    not NiFi's dotted/spaced property keys (e.g. "SSL Keystore Password"), so we
    redact them here before the doc enters the bundle."""
    out = dict(service or {})
    props = out.get("properties")
    if isinstance(props, dict):
        out["properties"] = {
            k: (None if any(h in str(k or "").lower() for h in _SERVICE_SECRET_HINTS) else v)
            for k, v in props.items()
        }
    return out


async def _flow_nifi_services(db: Any, flow: Dict[str, Any], source: Optional[Dict[str, Any]], streams: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    services: List[Dict[str, Any]] = []
    seen: Set[str] = set()

    for service_id in sorted(_nifi_service_ids(source)):
        nifi_service = await _find_one(db.nifi_global_services, {"id": service_id})
        if nifi_service:
            services.append(_redact_service_secrets(nifi_service))
            seen.add(str(nifi_service.get("id") or service_id))

    if _has_kafka_output(flow, source, streams):
        for kind in ("schema_registry", "kafka"):
            service = await _default_nifi_service(db, kind)
            if not service:
                continue
            service_id = str(service.get("id") or "").strip()
            if service_id and service_id in seen:
                continue
            service_doc = _redact_service_secrets(service)
            service_doc.setdefault("resolution", "default")
            services.append(service_doc)
            if service_id:
                seen.add(service_id)

    return services


async def _flow_bundle_payloads(db: Any, flow: Dict[str, Any]) -> Dict[Path, Dict[str, Any]]:
    bundle_id = _bundle_id(flow)
    flow_doc = dict(flow)
    flow_doc.pop("primary_stream", None)
    payloads: Dict[Path, Dict[str, Any]] = {flow_path(bundle_id): flow_doc}

    source: Optional[Dict[str, Any]] = None
    source_id = str(flow.get("source_id") or "").strip()
    if source_id:
        source = await _find_one(db.sources, {"id": source_id})
    if source:
        source_doc = dict(source)
        source_doc.pop("streams", None)
        streams = [stream for stream in (source.get("streams") or []) if isinstance(stream, dict)]
        payloads[source_path(bundle_id)] = source_doc
        for index, stream in enumerate(streams, start=1):
            sid = _stream_id(stream, index)
            if sid:
                payloads[stream_path(bundle_id, sid)] = stream

        app_service_id = str(source.get("application_service_id") or "").strip()
        if app_service_id:
            app_service = await _find_one(db.application_services, {"id": app_service_id})
            if app_service:
                payloads[application_services_path(bundle_id)] = {"services": [app_service]}

        openapi_spec_id = str(source.get("openapi_spec_id") or "").strip()
        if openapi_spec_id:
            spec = await _find_one(db.openapi_specs, {"id": openapi_spec_id})
            if spec:
                payloads[openapi_spec_path(bundle_id, openapi_spec_id)] = spec

        nifi_services = await _flow_nifi_services(db, flow, source, streams)
        if nifi_services:
            payloads[nifi_services_path(bundle_id)] = {"services": nifi_services}

    for artifact_id, linked_version in sorted(
        _schema_refs(flow, source, streams if source else None),
        key=lambda item: (item[0], -1 if item[1] is None else item[1]),
    ):
        artifact = await _find_one(db.schema_artifacts, {"artifact_id": artifact_id})
        if not artifact:
            continue
        artifact_doc = dict(artifact)
        versions = [version for version in (artifact_doc.pop("versions", None) or []) if isinstance(version, dict)]
        payloads[schema_artifact_path(bundle_id, artifact_id)] = artifact_doc
        for version in versions:
            version_number = version.get("version")
            if version_number in (None, ""):
                continue
            if linked_version is not None and str(version_number) != str(linked_version):
                continue
            payloads[schema_version_path(bundle_id, artifact_id, version_number)] = version

    bundle_root = flow_dir(bundle_id).resolve()
    compacted: Dict[Path, Dict[str, Any]] = {}
    for path, payload in payloads.items():
        resolved = path.resolve()
        rel_path = resolved.relative_to(bundle_root).as_posix()
        compacted[resolved] = compact_flow_file(rel_path, payload)
    return extract_secret_refs_from_payloads(compacted, bundle_root)


async def flow_bundle_payloads(db: Any, flow: Dict[str, Any]) -> Dict[Path, Dict[str, Any]]:
    return await _flow_bundle_payloads(db, flow)


async def _sync_flow_doc(db: Any, flow: Dict[str, Any], report: Optional[ContentStoreReport] = None) -> None:
    flow_id = _require_id(flow.get("id"), "flow")
    bundle_id = _bundle_id(flow)
    raw_flow_dir = flow_dir(flow_id)
    if flow_id != bundle_id:
        _delete_report_tree(report, raw_flow_dir)
    source_id = str(flow.get("source_id") or "").strip()
    if source_id and source_id != bundle_id:
        _delete_report_tree(report, flow_dir(source_id))
    bundle_dir = flow_dir(bundle_id)
    _delete_bundle_dirs_for_flow(report, flow_id, keep_dir=bundle_dir)
    _delete_report_tree(report, bundle_dir)
    bundle_dir.mkdir(parents=True, exist_ok=True)
    for path, payload in (await _flow_bundle_payloads(db, flow)).items():
        _write_report(report, path, payload)


async def _flows_for_source(db: Any, source_id: str) -> List[Dict[str, Any]]:
    return [flow for flow in await _find_all(db.flows) if str(flow.get("source_id") or "").strip() == source_id]


async def _flows_for_schema(db: Any, artifact_id: str) -> List[Dict[str, Any]]:
    flows = await _find_all(db.flows)
    sources = {str(source.get("id") or "").strip(): source for source in await _find_all(db.sources)}
    affected = []
    for flow in flows:
        source = sources.get(str(flow.get("source_id") or "").strip())
        if any(ref_artifact == artifact_id for ref_artifact, _ in _schema_refs(flow, source)):
            affected.append(flow)
    return affected


async def _flows_for_application_service(db: Any, service_id: str) -> List[Dict[str, Any]]:
    source_ids = {
        str(source.get("id") or "").strip()
        for source in await _find_all(db.sources)
        if str(source.get("application_service_id") or "").strip() == service_id
    }
    return [flow for flow in await _find_all(db.flows) if str(flow.get("source_id") or "").strip() in source_ids]


async def _flows_for_nifi_service(db: Any, service_id: str) -> List[Dict[str, Any]]:
    source_ids = {
        str(source.get("id") or "").strip()
        for source in await _find_all(db.sources)
        if service_id in _nifi_service_ids(source)
    }
    return [flow for flow in await _find_all(db.flows) if str(flow.get("source_id") or "").strip() in source_ids]


async def _flows_for_openapi_spec(db: Any, spec_id: str) -> List[Dict[str, Any]]:
    source_ids = {
        str(source.get("id") or "").strip()
        for source in await _find_all(db.sources)
        if str(source.get("openapi_spec_id") or "").strip() == spec_id
    }
    return [flow for flow in await _find_all(db.flows) if str(flow.get("source_id") or "").strip() in source_ids]


async def _sync_flows(db: Any, flows: Iterable[Dict[str, Any]], report: Optional[ContentStoreReport] = None) -> None:
    seen_bundles: Set[str] = set()
    flow_list = list(flows)
    for flow in flow_list:
        flow_id = str(flow.get("id") or "").strip()
        if not flow_id:
            continue
        bundle_id = str(flow.get("source_id") or flow_id).strip()
        if bundle_id and bundle_id != flow_id:
            _delete_report_tree(report, flow_dir(flow_id))
    for flow in _canonical_flows(flow_list):
        bundle_id = _bundle_id(flow)
        if bundle_id in seen_bundles:
            continue
        seen_bundles.add(bundle_id)
        await _sync_flow_doc(db, flow, report=report)


async def sync_flow(db: Any, flow_id: str, report: Optional[ContentStoreReport] = None) -> None:
    fid = _require_id(flow_id, "flow")
    doc = await _find_one(db.flows, {"id": fid})
    if not doc:
        _delete_report_tree(report, flow_dir(fid))
        return
    source_id = str(doc.get("source_id") or "").strip()
    if source_id:
        await _sync_flows(db, await _flows_for_source(db, source_id), report=report)
        return
    await _sync_flows(db, [doc], report=report)


async def sync_source(db: Any, source_id: str, report: Optional[ContentStoreReport] = None) -> None:
    sid = _require_id(source_id, "source")
    await _sync_flows(db, await _flows_for_source(db, sid), report=report)


async def sync_application_service(db: Any, service_id: str, report: Optional[ContentStoreReport] = None) -> None:
    sid = _require_id(service_id, "application service")
    await _sync_flows(db, await _flows_for_application_service(db, sid), report=report)


async def sync_nifi_global_service(db: Any, service_id: str, report: Optional[ContentStoreReport] = None) -> None:
    sid = _require_id(service_id, "nifi global service")
    await _sync_flows(db, await _flows_for_nifi_service(db, sid), report=report)


async def sync_schema_artifact(db: Any, artifact_id: str, report: Optional[ContentStoreReport] = None) -> None:
    aid = _require_id(artifact_id, "schema artifact")
    await _sync_flows(db, await _flows_for_schema(db, aid), report=report)


async def sync_openapi_spec(db: Any, spec_id: str, report: Optional[ContentStoreReport] = None) -> None:
    sid = _require_id(spec_id, "openapi spec")
    await _sync_flows(db, await _flows_for_openapi_spec(db, sid), report=report)


def delete_flow(flow_id: str, source_id: Optional[str] = None, report: Optional[ContentStoreReport] = None) -> None:
    _delete_bundle_dirs_for_flow(report, _require_id(flow_id, "flow"))
    _delete_report_tree(report, flow_dir(_require_id(flow_id, "flow")))
    if source_id:
        _delete_report_tree(report, flow_dir(_require_id(source_id, "source")))


def _delete_matching_bundle_paths(parts: List[str], report: Optional[ContentStoreReport] = None) -> None:
    flows_root = _root_child("flows")
    if not flows_root.exists():
        return
    for flow_bundle in flows_root.iterdir():
        if not flow_bundle.is_dir():
            continue
        target = flow_bundle.joinpath(*parts)
        _delete_report_tree(report, target)


def delete_source(source_id: str, report: Optional[ContentStoreReport] = None) -> None:
    # Source deletion is rejected while flows reference it. With no DB context here,
    # there is no safe flow bundle to remove by source id.
    _require_id(source_id, "source")


def delete_application_service(service_id: str, report: Optional[ContentStoreReport] = None) -> None:
    _require_id(service_id, "application service")
    _delete_matching_bundle_paths(["services", "application.json"], report=report)


def delete_nifi_global_service(service_id: str, report: Optional[ContentStoreReport] = None) -> None:
    _require_id(service_id, "nifi global service")
    _delete_matching_bundle_paths(["services", "nifi.json"], report=report)


def delete_schema_artifact(artifact_id: str, report: Optional[ContentStoreReport] = None) -> None:
    aid = _require_id(artifact_id, "schema artifact")
    _delete_matching_bundle_paths(["schemas", aid], report=report)


def delete_openapi_spec(spec_id: str, report: Optional[ContentStoreReport] = None) -> None:
    sid = _require_id(spec_id, "openapi spec")
    _delete_matching_bundle_paths(["openapi", sid], report=report)


async def expected_files(db: Any) -> Dict[Path, Dict[str, Any]]:
    expected: Dict[Path, Dict[str, Any]] = {}
    for flow in _canonical_flows(await _find_all(db.flows)):
        expected.update(await _flow_bundle_payloads(db, flow))
    return expected


async def rematerialize_content_store(db: Any) -> ContentStoreReport:
    root = get_content_store_root()
    report = ContentStoreReport(root=str(root))
    if root.exists():
        for child in root.iterdir():
            if child.is_file() or child.is_symlink():
                safe_delete_file(child)
            else:
                safe_delete_tree(child)
            report.deleted.append(str(child))
    root.mkdir(parents=True, exist_ok=True)

    flows = await _find_all(db.flows)
    await _sync_flows(db, flows, report=report)

    report.counts = {
        "flows": len(_canonical_flows(flows)),
    }
    atomic_write_json(manifest_path(), {
        "generated_at": datetime.utcnow(),
        "layout_version": 2,
        "layout": "flow-bundle",
        "counts": report.counts,
    })
    report.written.append(str(manifest_path()))
    return report


async def validate_content_store(db: Any) -> ContentStoreReport:
    root = get_content_store_root()
    report = ContentStoreReport(root=str(root))
    expected = await expected_files(db)
    expected_paths = set(expected.keys())

    for path, payload in expected.items():
        if not path.exists():
            report.missing.append(str(path))
            continue
        actual = path.read_text(encoding="utf-8")
        if actual != to_canonical_json(payload):
            report.stale.append(str(path))

    if root.exists():
        for path in root.rglob("*.json"):
            resolved = path.resolve()
            if resolved == manifest_path().resolve():
                continue
            if resolved not in expected_paths:
                report.extra.append(str(path))

    report.counts = {
        "expected_files": len(expected_paths),
        "missing": len(report.missing),
        "stale": len(report.stale),
        "extra": len(report.extra),
    }
    report.ok = not report.missing and not report.stale and not report.extra and not report.errors
    return report
