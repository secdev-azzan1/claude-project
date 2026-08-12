import copy
import hashlib
import json
import re
from typing import Any, Dict, List, Optional, Tuple

HTTP_METHODS = {"get", "post", "put", "patch", "delete", "head", "options", "trace"}
PATH_TEMPLATE_PARAM_PATTERN = re.compile(r"(?<!\$)\{([a-zA-Z0-9_.-]+)\}")


class OpenApiParseError(ValueError):
    pass


def _sha256_hex(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _to_runtime_path(path: str) -> str:
    return PATH_TEMPLATE_PARAM_PATTERN.sub(r"${\1}", path or "/")


def _load_json(raw_bytes: bytes, filename: str = "") -> Dict[str, Any]:
    if raw_bytes.startswith(b"\xff\xfe") or raw_bytes.startswith(b"\xfe\xff"):
        text = raw_bytes.decode("utf-16")
    else:
        # Some exported API specs are UTF-16 without BOM; fall back if NUL density is high.
        nul_ratio = raw_bytes.count(b"\x00") / max(len(raw_bytes), 1)
        if nul_ratio > 0.05:
            try:
                text = raw_bytes.decode("utf-16")
            except Exception:
                text = raw_bytes.decode("utf-8", errors="replace")
        else:
            text = raw_bytes.decode("utf-8", errors="replace")
    try:
        loaded = json.loads(text)
    except Exception as exc:
        raise OpenApiParseError(f"Failed to parse OpenAPI JSON: {exc}") from exc
    if not isinstance(loaded, dict):
        raise OpenApiParseError("OpenAPI document root must be a JSON/YAML object.")
    return loaded


def _json_pointer_get(root: Dict[str, Any], pointer: str) -> Any:
    if pointer == "#":
        return root
    if not pointer.startswith("#/"):
        raise OpenApiParseError(f"Only local $ref values are supported. Unsupported ref: {pointer}")
    node: Any = root
    for raw_part in pointer[2:].split("/"):
        part = raw_part.replace("~1", "/").replace("~0", "~")
        if not isinstance(node, dict) or part not in node:
            raise OpenApiParseError(f"Unresolvable $ref: {pointer}")
        node = node[part]
    return node


def _resolve_refs(value: Any, root: Dict[str, Any], seen: Optional[set] = None, depth: int = 0) -> Any:
    if depth > 60:
        return {}
    if seen is None:
        seen = set()
    if isinstance(value, dict):
        ref = value.get("$ref")
        if isinstance(ref, str):
            if ref in seen:
                return {}
            resolved = _json_pointer_get(root, ref)
            merged = copy.deepcopy(resolved)
            for k, v in value.items():
                if k != "$ref":
                    merged[k] = v
            return _resolve_refs(merged, root, seen | {ref}, depth + 1)
        return {k: _resolve_refs(v, root, seen, depth + 1) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_refs(item, root, seen, depth + 1) for item in value]
    return value


def _normalize_schema_type(schema: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Any, Any]:
    if not isinstance(schema, dict):
        return None, None, None, None
    s_type = schema.get("type")
    if isinstance(s_type, list):
        s_type = ",".join(str(x) for x in s_type)
    elif s_type is not None:
        s_type = str(s_type)
    s_format = schema.get("format")
    if s_format is not None:
        s_format = str(s_format)
    return s_type, s_format, schema.get("default"), schema.get("enum")


def _operation_fallback_id(method: str, path: str) -> str:
    token = f"{method.lower()}_{path.strip('/') or 'root'}"
    token = re.sub(r"[^a-zA-Z0-9_]+", "_", token).strip("_")
    return token or "operation"


def _extract_servers(doc: Dict[str, Any], fmt: str) -> List[Dict[str, str]]:
    if fmt == "openapi3":
        servers = doc.get("servers") or []
        out: List[Dict[str, str]] = []
        for item in servers:
            if not isinstance(item, dict):
                continue
            url = str(item.get("url") or "").strip()
            if not url:
                continue
            out.append({"url": url, "description": str(item.get("description") or "").strip()})
        return out

    # swagger2
    host = str(doc.get("host") or "").strip()
    base_path = str(doc.get("basePath") or "").strip()
    schemes = doc.get("schemes") if isinstance(doc.get("schemes"), list) else []
    if not host:
        return []
    if not base_path:
        base_path = ""
    if base_path and not base_path.startswith("/"):
        base_path = f"/{base_path}"
    if not schemes:
        schemes = ["https", "http"]
    return [{"url": f"{scheme}://{host}{base_path}", "description": ""} for scheme in schemes]


def _merge_parameters(
    path_parameters: List[Dict[str, Any]],
    op_parameters: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    merged: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for param in path_parameters + op_parameters:
        if not isinstance(param, dict):
            continue
        name = str(param.get("name") or "").strip()
        where = str(param.get("in") or "").strip().lower()
        if not name or not where:
            continue
        merged[(where, name)] = param
    return list(merged.values())


def _normalize_parameter(p: Dict[str, Any], fmt: str) -> Dict[str, Any]:
    where = str(p.get("in") or "").strip().lower()
    required = bool(p.get("required", False) or where == "path")
    if fmt == "swagger2" and where != "body":
        p_type = p.get("type")
        p_format = p.get("format")
        p_default = p.get("default")
        p_enum = p.get("enum")
    else:
        schema = p.get("schema") if isinstance(p.get("schema"), dict) else {}
        p_type, p_format, p_default, p_enum = _normalize_schema_type(schema)
    return {
        "name": str(p.get("name") or "").strip(),
        "in": where,
        "required": required,
        "schema_type": p_type,
        "schema_format": p_format if p_format is None else str(p_format),
        "default": p_default,
        "enum": p_enum,
        "description": str(p.get("description") or "").strip(),
    }


def parse_openapi_document(raw_bytes: bytes, filename: str = "") -> Dict[str, Any]:
    if not raw_bytes:
        raise OpenApiParseError("Uploaded file is empty.")

    doc = _load_json(raw_bytes, filename=filename)
    fmt: Optional[str] = None
    version_text = ""
    if "openapi" in doc:
        fmt = "openapi3"
        version_text = str(doc.get("openapi") or "")
    elif "swagger" in doc:
        fmt = "swagger2"
        version_text = str(doc.get("swagger") or "")
    if not fmt:
        raise OpenApiParseError("Document is not OpenAPI/Swagger (missing 'openapi' or 'swagger' field).")

    resolved = _resolve_refs(doc, doc)
    paths = resolved.get("paths")
    if not isinstance(paths, dict) or not paths:
        raise OpenApiParseError("Document has no paths.")

    info = resolved.get("info") if isinstance(resolved.get("info"), dict) else {}
    servers = _extract_servers(resolved, fmt)
    warnings: List[str] = []
    if not servers:
        warnings.append("No server/base URL could be derived from the OpenAPI document.")

    operations: List[Dict[str, Any]] = []
    seen_operation_ids: set = set()
    for raw_path, path_item in paths.items():
        if not isinstance(path_item, dict):
            continue
        path_str = str(raw_path or "").strip() or "/"
        path_parameters = path_item.get("parameters") if isinstance(path_item.get("parameters"), list) else []
        for method_name, operation in path_item.items():
            method_l = str(method_name or "").lower()
            if method_l not in HTTP_METHODS:
                continue
            if not isinstance(operation, dict):
                continue
            op_parameters = operation.get("parameters") if isinstance(operation.get("parameters"), list) else []
            params = _merge_parameters(path_parameters, op_parameters)
            normalized_params = [_normalize_parameter(p, fmt) for p in params]
            op_id = str(operation.get("operationId") or "").strip() or _operation_fallback_id(method_l, path_str)
            unique_op_id = op_id
            i = 2
            while unique_op_id in seen_operation_ids:
                unique_op_id = f"{op_id}_{i}"
                i += 1
            seen_operation_ids.add(unique_op_id)

            request_content_types: List[str] = []
            if fmt == "openapi3":
                rb = operation.get("requestBody") if isinstance(operation.get("requestBody"), dict) else {}
                content = rb.get("content") if isinstance(rb.get("content"), dict) else {}
                request_content_types = sorted([str(k) for k in content.keys()])
            else:
                consumes = operation.get("consumes")
                if isinstance(consumes, list):
                    request_content_types = sorted([str(c) for c in consumes if c])
                elif isinstance(resolved.get("consumes"), list):
                    request_content_types = sorted([str(c) for c in resolved.get("consumes") if c])

            tags = operation.get("tags") if isinstance(operation.get("tags"), list) else []
            tags = [str(t).strip() for t in tags if str(t).strip()]

            operations.append(
                {
                    "operation_id": unique_op_id,
                    "method": method_l.upper(),
                    "path_original": path_str,
                    "path_runtime": _to_runtime_path(path_str),
                    "summary": str(operation.get("summary") or "").strip(),
                    "description": str(operation.get("description") or "").strip(),
                    "tags": tags,
                    "deprecated": bool(operation.get("deprecated", False)),
                    "security": operation.get("security") if isinstance(operation.get("security"), list) else [],
                    "request_content_types": request_content_types,
                    "parameters": normalized_params,
                }
            )

    if not operations:
        raise OpenApiParseError("Document contained no valid HTTP operations.")
    if len(operations) > 5000:
        warnings.append(
            f"Large OpenAPI document detected ({len(operations)} operations). UI will use paged operation search."
        )

    return {
        "checksum_sha256": _sha256_hex(raw_bytes),
        "raw_size_bytes": len(raw_bytes),
        "format": fmt,
        "openapi_version": version_text,
        "title": str(info.get("title") or "").strip() or (filename or "OpenAPI"),
        "version": str(info.get("version") or "").strip(),
        "servers": servers,
        "operations": operations,
        "warnings": warnings,
        "errors": [],
    }
