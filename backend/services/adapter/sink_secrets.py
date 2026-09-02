"""Shared secret redaction/preservation for stored Kafka Connect sink configs.

Both `routers/kafka_connect.py` (user-managed syncs) and `routers/v2/flows.py`
(a flow block's `config.sinkConfig`, populated by the sinkConfig migration)
store a Kafka Connect connector config that can contain real credentials
(S3 secret keys, catalog OAuth credentials, ...). Anything returned to a
client must have those values redacted; anything written back from a client
must have the redaction placeholder resolved back to the real stored value
before it reaches Mongo -- otherwise the first read/write round-trip through
the UI overwrites the real credential with the literal placeholder string.

A read-side redactor (`redact_config`) and a write-side preserver
(`merge_preserving_secrets`) must always agree on the same sentinel
(`SECRET_PLACEHOLDER`), or that round-trip silently destroys the credential.
"""

from __future__ import annotations

import re
from typing import Any, Dict

SECRET_PLACEHOLDER = "[secret]"

# Case-insensitive match against a property NAME (not its value): password,
# secret, token, credential, api key, access key, private key.
SECRET_PROPERTY_RE = re.compile(
    r"(pass(word)?|secret|token|credential|api.?key|access.?key|private.?key)", re.I
)

# Boolean feature toggles whose name happens to contain a secret-ish
# substring (e.g. the real property `iceberg.catalog.token-refresh-enabled`
# contains "token") are not credentials. A name ending in `-enabled` /
# `.enabled` is always a boolean toggle in practice -- no real credential
# property is named that way -- so this exclusion wins over the positive
# match above.
_ENABLED_SUFFIX_RE = re.compile(r"[-.]enabled$", re.I)


def is_secret_property(key: str) -> bool:
    key = str(key)
    if _ENABLED_SUFFIX_RE.search(key):
        return False
    return bool(SECRET_PROPERTY_RE.search(key))


def redact_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of config with secret-named non-empty values redacted."""
    return {
        key: SECRET_PLACEHOLDER if is_secret_property(key) and value not in (None, "") else value
        for key, value in (config or {}).items()
    }


def merge_preserving_secrets(incoming: Dict[str, Any], existing: Dict[str, Any]) -> Dict[str, Any]:
    """Resolve an incoming config's placeholders against the stored config.

    - Any incoming value equal to `SECRET_PLACEHOLDER` is replaced by the
      stored value from `existing`, if present.
    - Any secret-named key present in `existing` but missing from `incoming`
      is carried forward, so a partial editor payload cannot silently delete
      a credential.
    """
    merged = dict(incoming or {})
    existing = existing or {}
    for key, value in list(merged.items()):
        if value == SECRET_PLACEHOLDER and key in existing:
            merged[key] = existing[key]
    for key, value in existing.items():
        if key not in merged and is_secret_property(key) and value not in (None, ""):
            merged[key] = value
    return merged
