"""REST stream placeholder mapping helpers."""
from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List


PLACEHOLDER_TOKEN_PATTERN = re.compile(r"\$\{([A-Za-z0-9_.-]+)\}")
LEGACY_BRACE_PLACEHOLDER_PATTERN = re.compile(r"(?<!\$)\{([A-Za-z0-9_.-]+)\}")


class RestStreamValueMappingError(ValueError):
    pass


def find_placeholders(value: str) -> List[str]:
    found: List[str] = []
    seen: set[str] = set()
    for match in PLACEHOLDER_TOKEN_PATTERN.finditer(str(value or "")):
        token = (match.group(1) or "").strip()
        if token and token not in seen:
            found.append(token)
            seen.add(token)
    return found


def find_legacy_brace_placeholders(value: str) -> List[str]:
    found: List[str] = []
    seen: set[str] = set()
    for match in LEGACY_BRACE_PLACEHOLDER_PATTERN.finditer(str(value or "")):
        token = (match.group(1) or "").strip()
        if token and token not in seen:
            found.append(token)
            seen.add(token)
    return found


def assert_no_legacy_brace_placeholders(value: str, *, location: str = "value") -> None:
    legacy = find_legacy_brace_placeholders(value)
    if legacy:
        examples = ", ".join(f"${{{token}}}" for token in legacy)
        raise RestStreamValueMappingError(
            f"Unsupported placeholder syntax in {location}. Use {examples}."
        )


def substitute_runtime_values(template: str, values: Dict[str, Any], *, location: str = "value") -> str:
    text = str(template or "")
    assert_no_legacy_brace_placeholders(text, location=location)
    for key, value in values.items():
        text = text.replace(f"${{{key}}}", str(value))
    return text


def unresolved_placeholders_in_values(values: Iterable[str]) -> List[str]:
    found: List[str] = []
    seen: set[str] = set()
    for value in values:
        for token in find_placeholders(value):
            if token not in seen:
                found.append(token)
                seen.add(token)
    return found


def to_nifi_runtime_template(template: str, defaults: Dict[str, Any] | None = None) -> str:
    """Compile ${field} placeholders to NiFi runtime attributes with optional fallback defaults."""
    text = str(template or "")
    defaults = defaults if isinstance(defaults, dict) else {}

    def replace(match: re.Match[str]) -> str:
        token = (match.group(1) or "").strip()
        if not token:
            return match.group(0)
        if ":" in token:
            return match.group(0)
        raw_default = defaults.get(token)
        if raw_default is None or str(raw_default).strip() == "":
            return f"${{{token}}}"
        escaped = str(raw_default).strip().replace("\\", "\\\\").replace("'", "\\'")
        return f"${{{token}:replaceEmpty('{escaped}')}}"

    return PLACEHOLDER_TOKEN_PATTERN.sub(replace, text)
