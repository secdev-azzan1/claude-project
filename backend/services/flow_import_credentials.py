from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple

from services.flow_import_preview import FlowImportPreviewError, preview_flow_import


CredentialMap = Dict[str, Dict[str, str]]
CredentialRef = Dict[str, str]


class FlowImportCredentialError(ValueError):
    def __init__(self, result: Dict[str, Any]):
        super().__init__("Import credentials are incomplete or invalid")
        self.result = result


def _credential_key(item: CredentialRef) -> Tuple[str, str]:
    return str(item.get("path") or ""), str(item.get("field") or "")


def _normalize_credentials(credentials: Any) -> CredentialMap:
    if credentials is None:
        return {}
    if not isinstance(credentials, dict):
        raise FlowImportCredentialError(
            _result(missing=[], unexpected=[], provided=[], valid=False)
        )

    normalized: CredentialMap = {}
    for raw_path, raw_fields in credentials.items():
        path = str(raw_path or "").strip()
        if not path or not isinstance(raw_fields, dict):
            normalized[path] = {}
            continue
        normalized[path] = {
            str(field or "").strip(): value
            for field, value in raw_fields.items()
            if str(field or "").strip()
        }
    return normalized


def _result(
    *,
    missing: List[CredentialRef],
    unexpected: List[CredentialRef],
    provided: List[CredentialRef],
    valid: bool,
) -> Dict[str, Any]:
    return {
        "ok": valid,
        "credentials_valid": valid,
        "missing": missing,
        "unexpected": unexpected,
        "provided": provided,
        "ready_for_import": valid,
    }


async def validate_import_credentials(raw: bytes, credentials: Any, db: Any) -> Dict[str, Any]:
    try:
        preview = await preview_flow_import(raw, db=db)
    except FlowImportPreviewError:
        raise

    normalized = _normalize_credentials(credentials)
    required: List[CredentialRef] = list(preview.get("credentials_required") or [])
    required_keys: Set[Tuple[str, str]] = {_credential_key(item) for item in required}

    missing: List[CredentialRef] = []
    provided: List[CredentialRef] = []
    for item in required:
        path, field = _credential_key(item)
        value = normalized.get(path, {}).get(field)
        if not isinstance(value, str) or not value.strip():
            missing.append({"path": path, "field": field})
        else:
            provided.append({"path": path, "field": field})

    unexpected: List[CredentialRef] = []
    for path, fields in normalized.items():
        for field, value in fields.items():
            if (path, field) not in required_keys and isinstance(value, str) and value.strip():
                unexpected.append({"path": path, "field": field})

    missing.sort(key=lambda item: (item["path"], item["field"]))
    unexpected.sort(key=lambda item: (item["path"], item["field"]))
    provided.sort(key=lambda item: (item["path"], item["field"]))

    valid = not missing and not unexpected
    result = _result(missing=missing, unexpected=unexpected, provided=provided, valid=valid)
    if not valid:
        raise FlowImportCredentialError(result)
    return result
