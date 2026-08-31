"""Shared JDBC endpoint and identifier handling for V2 adapters."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping
from urllib.parse import urlsplit


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class TrinoEndpoint:
    """Normalized Trino coordinator endpoint."""

    host: str
    port: int
    secure: bool

    @property
    def http_scheme(self) -> str:
        return "https" if self.secure else "http"


def _as_port(value: Any, default: int) -> int:
    if value in (None, ""):
        return default
    try:
        port = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("Trino port must be a number") from exc
    if not 1 <= port <= 65535:
        raise ValueError("Trino port must be between 1 and 65535")
    return port


def trino_endpoint(config: Mapping[str, Any]) -> TrinoEndpoint:
    """Parse the preferred URL form or a backwards-compatible host/port form.

    Preferred config is ``url`` (or ``endpoint``), for example
    ``https://trino.datapasc.com``. Existing records with only ``host`` and
    ``port`` continue to work; ``ssl`` or ``tls`` can opt them into HTTPS.
    """

    raw = str(config.get("url") or config.get("endpoint") or config.get("host") or "").strip()
    if not raw:
        raise ValueError("Trino coordinator URL or host is required")

    has_scheme = "://" in raw
    parsed = urlsplit(raw if has_scheme else f"//{raw}")
    if has_scheme and parsed.scheme.lower() not in ("http", "https"):
        raise ValueError("Trino coordinator URL must use http or https")
    if parsed.username or parsed.password:
        raise ValueError("Trino coordinator URL must not contain credentials")
    if parsed.path not in ("", "/") or parsed.query or parsed.fragment:
        raise ValueError("Trino coordinator URL must not include a path, query, or fragment")
    host = parsed.hostname
    if not host:
        raise ValueError("Trino coordinator host is required")

    secure = parsed.scheme.lower() == "https" if has_scheme else bool(config.get("ssl") or config.get("tls"))
    default_port = 443 if secure else 8080
    port = _as_port(parsed.port if parsed.port is not None else config.get("port"), default_port)
    return TrinoEndpoint(host=host, port=port, secure=secure)


def trino_table_parts(value: Any) -> tuple[str, str, str]:
    """Return ``catalog, schema, table`` from a safe Trino table reference."""

    table = str(value or "").strip()
    parts = table.split(".")
    if len(parts) != 3 or not all(_IDENTIFIER.fullmatch(part) for part in parts):
        raise ValueError("Trino table must be written as catalog.schema.table using simple identifiers")
    return parts[0], parts[1], parts[2]


def trino_jdbc_url(config: Mapping[str, Any], table: Any) -> str:
    """Build the NiFi DBCP URL for a fully-qualified Trino table."""

    endpoint = trino_endpoint(config)
    catalog, schema, _ = trino_table_parts(table)
    url = f"jdbc:trino://{endpoint.host}:{endpoint.port}/{catalog}/{schema}"
    if endpoint.secure:
        url += "?SSL=true"
    return url

