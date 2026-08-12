from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Tuple


SECRETS_FILE = "secrets.json"
SECRET_REF_KEY = "$secretRef"

SECRET_KEYS = {
    "api_key_value",
    "bearer_token",
    "rest_password",
    "db_password",
    "db_connection_uri",
    "smb_password",
    "webhook_secret",
    "password",
    "secret",
    "token",
    "api_key",
    "private_key",
    "client_secret",
    "access_token",
    "refresh_token",
}

IMPORT_REENTRY_KEYS = {
    "base_url",
    "baseUrl",
    "rest_username",
    "username",
    "endpoint",
    "url",
    "urls",
    "bootstrap.servers",
    "Schema Registry URLs",
}


def is_secret_key(key: str) -> bool:
    lowered = key.lower()
    if lowered in SECRET_KEYS:
        return True
    if lowered.endswith("_password") or lowered.endswith("_secret") or lowered.endswith("_token"):
        return True
    return lowered.endswith("_private_key") or lowered.endswith("_client_secret")


def requires_import_reentry(key: str) -> bool:
    lowered = key.lower()
    return key in IMPORT_REENTRY_KEYS or lowered in {item.lower() for item in IMPORT_REENTRY_KEYS} or lowered.endswith("_url") or lowered.endswith("_uri")


def is_sensitive_key(key: str) -> bool:
    return is_secret_key(key) or requires_import_reentry(key)


def is_openapi_metadata_url(rel_path: str, field_path: str) -> bool:
    if rel_path == "source/source.json" and field_path == "openapi_selected_server_url":
        return True
    parts = field_path.split(".")
    return (
        rel_path.startswith("openapi/")
        and rel_path.endswith("/spec.json")
        and len(parts) == 3
        and parts[0] == "servers"
        and parts[1].isdigit()
        and parts[2] == "url"
    )


def is_secret_ref(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value.keys()) == {SECRET_REF_KEY}
        and isinstance(value.get(SECRET_REF_KEY), str)
        and bool(value.get(SECRET_REF_KEY).strip())
    )


def secret_ref(rel_path: str, field_path: str) -> str:
    return f"{rel_path}#{field_path}"


def extract_secret_refs_from_payloads(
    payloads: Dict[Path, Dict[str, Any]],
    bundle_root: Path,
) -> Dict[Path, Dict[str, Any]]:
    updated: Dict[Path, Dict[str, Any]] = {}
    secrets: Dict[str, Any] = {}

    for path, payload in payloads.items():
        resolved = path.resolve()
        rel_path = resolved.relative_to(bundle_root.resolve()).as_posix()
        if rel_path == SECRETS_FILE:
            continue
        updated[resolved] = _extract_from_value(deepcopy(payload), rel_path, "", secrets)

    if secrets:
        updated[(bundle_root / SECRETS_FILE).resolve()] = dict(sorted(secrets.items()))
    return updated


def _extract_from_value(value: Any, rel_path: str, field_path: str, secrets: Dict[str, Any]) -> Any:
    if isinstance(value, list):
        return [
            _extract_from_value(item, rel_path, f"{field_path}.{index}" if field_path else str(index), secrets)
            for index, item in enumerate(value)
        ]
    if not isinstance(value, dict) or is_secret_ref(value):
        return value

    output: Dict[str, Any] = {}
    for key, item in value.items():
        next_path = f"{field_path}.{key}" if field_path else str(key)
        if is_openapi_metadata_url(rel_path, next_path):
            output[str(key)] = deepcopy(item)
            continue
        if is_sensitive_key(str(key)) and item not in (None, "") and not is_secret_ref(item):
            ref = secret_ref(rel_path, next_path)
            secrets[ref] = item
            output[str(key)] = {SECRET_REF_KEY: ref}
            continue
        output[str(key)] = _extract_from_value(item, rel_path, next_path, secrets)
    return output
