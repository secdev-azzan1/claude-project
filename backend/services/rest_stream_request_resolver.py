from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib.parse import urljoin
from xml.etree import ElementTree as ET
from services.rest_stream_value_mapping import (
    RestStreamValueMappingError,
    substitute_runtime_values,
    unresolved_placeholders_in_values,
)


MUTATING_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
SUPPORTED_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE"}
BODY_CONTENT_TYPES = {
    "json": "application/json",
    "xml": "application/xml",
    "csv": "text/csv",
}
RESPONSE_ACCEPTS = {
    "json": "application/json",
    "xml": "application/xml",
    "csv": "text/csv",
}


class RestStreamRequestError(ValueError):
    pass


@dataclass(frozen=True)
class ResolvedRestStreamRequest:
    method: str
    url: str
    masked_url: str
    headers: Dict[str, str]
    masked_headers: Dict[str, str]
    params: Dict[str, str]
    masked_params: Dict[str, str]
    body_format: str
    body: Optional[bytes]
    body_text: Optional[str]
    masked_body_text: Optional[str]
    body_json: Optional[Any]
    is_mutating: bool


def _substitute_params(template: str, values: Dict[str, Any]) -> str:
    try:
        return substitute_runtime_values(template, values)
    except RestStreamValueMappingError as exc:
        raise RestStreamRequestError(str(exc)) from exc


def _find_unresolved_placeholders(value: str) -> list[str]:
    return unresolved_placeholders_in_values([value])


def _header_get(headers: Dict[str, str], name: str) -> Optional[str]:
    lowered = name.lower()
    for key, value in headers.items():
        if key.lower() == lowered:
            return value
    return None


def _mask_header_value(key: str, value: str) -> str:
    if key.lower() == "authorization":
        prefix = value.split(" ", 1)[0] if " " in value else ""
        return f"{prefix} ***".strip()
    if any(part in key.lower() for part in ("key", "token", "secret", "password")):
        return "***"
    return value


def _mask_url(url: str, masked_params: Dict[str, str]) -> str:
    masked = url
    for value in masked_params.values():
        if value and value != "***":
            masked = masked.replace(value, "***")
    return masked


def _body_for_format(stream_name: str, body_format: str, body_template: str, values: Dict[str, Any]) -> tuple[Optional[bytes], Optional[str], Optional[Any]]:
    if body_format == "empty":
        if str(body_template or "").strip():
            raise RestStreamRequestError(
                f'Stream "{stream_name}": body_template is configured but body_format is empty.'
            )
        return None, None, None

    if not str(body_template or "").strip():
        raise RestStreamRequestError(
            f'Stream "{stream_name}": body_format={body_format} requires a request body template.'
        )

    body_text = _substitute_params(body_template, values)
    unresolved = _find_unresolved_placeholders(body_text)
    if unresolved:
        raise RestStreamRequestError(
            f'Stream "{stream_name}": unresolved placeholder in request body: {", ".join(unresolved)}.'
        )

    if body_format == "json":
        try:
            parsed = json.loads(body_text)
        except Exception as exc:
            raise RestStreamRequestError(
                f'Stream "{stream_name}": resolved request body must be valid JSON.'
            ) from exc
        compact = json.dumps(parsed, separators=(",", ":"), ensure_ascii=False)
        return compact.encode("utf-8"), compact, parsed

    if body_format == "xml":
        try:
            ET.fromstring(body_text)
        except Exception as exc:
            raise RestStreamRequestError(
                f'Stream "{stream_name}": resolved request body must be well-formed XML.'
            ) from exc
        return body_text.encode("utf-8"), body_text, None

    if body_format == "csv":
        return body_text.encode("utf-8"), body_text, None

    raise RestStreamRequestError(f'Stream "{stream_name}": body_format must be one of: empty, json, xml, csv.')


def resolve_rest_stream_request(
    source: Dict[str, Any],
    stream: Dict[str, Any],
    runtime_values: Optional[Dict[str, Any]] = None,
    *,
    confirmed_mutation: bool = False,
) -> ResolvedRestStreamRequest:
    stream_name = str(stream.get("name") or "stream")
    method = str(stream.get("method") or "GET").strip().upper()
    if method not in SUPPORTED_METHODS:
        raise RestStreamRequestError(f'Stream "{stream_name}": method must be one of: GET, POST, PUT, PATCH, DELETE.')

    is_mutating = method in MUTATING_METHODS
    if is_mutating and not confirmed_mutation:
        raise RestStreamRequestError(f'Stream "{stream_name}": {method} test requires explicit confirmation.')

    values: Dict[str, Any] = {}
    defaults = stream.get("path_param_defaults") or {}
    if isinstance(defaults, dict):
        for key, value in defaults.items():
            text = str(value).strip()
            if key and text:
                values[str(key)] = text
    values.update(runtime_values or {})

    base_url = str(source.get("base_url") or "").rstrip("/")
    endpoint_path = str(stream.get("endpoint_path") or "/")
    resolved_path = _substitute_params(endpoint_path, values)
    unresolved_path = _find_unresolved_placeholders(resolved_path)
    if unresolved_path:
        raise RestStreamRequestError(
            f'Stream "{stream_name}": unresolved placeholder in endpoint path: {", ".join(unresolved_path)}.'
        )
    url = urljoin(f"{base_url}/", resolved_path.lstrip("/"))

    headers: Dict[str, str] = {}
    params: Dict[str, str] = {}
    sensitive_param_names: set[str] = set()

    auth_type = str(source.get("auth_type") or "NONE").strip().upper()
    if auth_type == "BEARER":
        token = str(source.get("bearer_token") or "")
        if token:
            headers["Authorization"] = f"Bearer {token}"
    elif auth_type == "API_KEY":
        key_name = str(source.get("api_key_name") or "")
        key_value = str(source.get("api_key_value") or "")
        key_location = str(source.get("api_key_location") or "HEADER").strip().upper()
        if key_name and key_location == "QUERY":
            params[key_name] = key_value
            sensitive_param_names.add(key_name)
        elif key_name:
            headers[key_name] = key_value
    elif auth_type == "BASIC":
        username = str(source.get("rest_username") or "")
        password = str(source.get("rest_password") or "")
        if username:
            credentials = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
            headers["Authorization"] = f"Basic {credentials}"

    stream_headers = stream.get("headers") or {}
    if isinstance(stream_headers, dict):
        for key, value in stream_headers.items():
            rendered = _substitute_params(str(value or ""), values).strip()
            if not rendered:
                continue
            unresolved = _find_unresolved_placeholders(rendered)
            if unresolved:
                raise RestStreamRequestError(
                    f'Stream "{stream_name}": unresolved placeholder in header "{key}": {", ".join(unresolved)}.'
                )
            headers[str(key)] = rendered

    stream_params = stream.get("query_params") or {}
    if isinstance(stream_params, dict):
        for key, value in stream_params.items():
            rendered = _substitute_params(str(value or ""), values).strip()
            if not rendered:
                continue
            unresolved = _find_unresolved_placeholders(rendered)
            if unresolved:
                raise RestStreamRequestError(
                    f'Stream "{stream_name}": unresolved placeholder in query param "{key}": {", ".join(unresolved)}.'
                )
            params[str(key)] = rendered

    body_format = str(stream.get("body_format") or "empty").strip().lower()
    body, body_text, body_json = _body_for_format(stream_name, body_format, str(stream.get("body_template") or ""), values)

    if body_format != "empty" and _header_get(headers, "Content-Type") is None:
        headers["Content-Type"] = BODY_CONTENT_TYPES[body_format]
    response_format = str(stream.get("response_format") or "json").strip().lower()
    if _header_get(headers, "Accept") is None:
        headers["Accept"] = RESPONSE_ACCEPTS.get(response_format, "application/json")

    masked_headers = {key: _mask_header_value(key, value) for key, value in headers.items()}
    masked_params = {key: ("***" if key in sensitive_param_names else value) for key, value in params.items()}
    masked_url = _mask_url(url, masked_params)

    return ResolvedRestStreamRequest(
        method=method,
        url=url,
        masked_url=masked_url,
        headers=headers,
        masked_headers=masked_headers,
        params=params,
        masked_params=masked_params,
        body_format=body_format,
        body=body,
        body_text=body_text,
        masked_body_text=body_text,
        body_json=body_json,
        is_mutating=is_mutating,
    )
