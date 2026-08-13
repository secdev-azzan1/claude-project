"""Shared secret-redaction helpers for the adapter model package.

Both `AppService` (service.py) and `PlatformConnection` (connection.py) store a
loosely typed `config: dict` that mirrors the frontend TypeScript shape, plus a
server-only convention: unlike the frontend mock (which never persists secret
values at all — see ServiceFormFields.tsx / pages/Connections.tsx, where
`buildConfig` / `draftToConnection` deliberately drop secret fields and only
record `hasSecret`), the real backend DOES need the secret value to actually
call out to the service/connection. So server-side, `config` may legitimately
contain one of the keys below with a real secret value in it.

`redact()` on those models produces the browser-safe payload: every known
secret key is replaced with `None` (never removed — the key's *presence*
still round-trips) and a companion `has<Key>` boolean is added at the top
level of the returned dict for every key in SECRET_CONFIG_KEYS, so the UI can
render "a secret is set" without ever seeing the value.

The literal key set was assembled by reading the actual field names the forms
use, not guessed:
  - password        - http basic auth (ServiceFormFields.tsx `password`),
                       database password (form field `dbPassword` submits
                       under the same generic `password` config key), nifi
                       basic auth / apicurio basic auth / redis password
                       (pages/Connections.tsx `defaultDraft`).
  - token            - http bearer auth, nifi/apicurio bearer auth (`token`).
  - clientSecret     - http oauth2 auth (`clientSecret`).
  - keyValue         - http api_key auth (`keyValue`).
  - saslPassword     - external_kafka service + kafka connection SASL auth
                       (`saslPassword` in both ServiceFormFields.tsx and
                       pages/Connections.tsx).
  - adminKey         - apisix connection admin key (pages/Connections.tsx
                       `defaultDraft` for type "apisix").
  - s3SecretKey      - not present anywhere in the current frontend
                       prototype (no direct S3 connection/service type
                       exists yet — S3 only shows up as a Kafka Connect
                       `connector.class` on a block's `sinkConfig`, which is
                       not a service/connection config at all). Included
                       anyway per the task spec's explicit example list, as
                       forward-compatibility for a future S3-backed
                       service/connection type.
  - oauthClientSecret - added for routers/v2/services.py's T3.3
                       sink_destination (iceberg_catalog) extended config:
                       `oauthClientId`/`oauthClientSecret` for the
                       catalog's own OAuth2 client-credentials probe,
                       distinct from the http-service `clientSecret` above.
"""

from typing import Any, Dict, List, Tuple

SECRET_CONFIG_KEYS: List[str] = [
    "password",
    "token",
    "clientSecret",
    "keyValue",
    "saslPassword",
    "adminKey",
    "s3SecretKey",
    "oauthClientSecret",
    # kafka platform connection in kafbat mode authenticates to the Kafbat UI
    "kafbatPassword",
]


def _has_flag_name(key: str) -> str:
    return "has" + key[0].upper() + key[1:]


def redact_config(config: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, bool]]:
    """Return (safe_config, has_flags) for a raw config dict.

    - `safe_config` is a shallow copy of `config` with every known secret key
      present replaced by `None` (the key stays, so shape is unchanged).
    - `has_flags` carries a `has<Key>` boolean for every key in
      SECRET_CONFIG_KEYS, True when that key held a truthy (non-empty)
      secret value in the input.
    """
    safe = dict(config or {})
    flags: Dict[str, bool] = {}
    for key in SECRET_CONFIG_KEYS:
        flags[_has_flag_name(key)] = bool(safe.get(key))
        if key in safe:
            safe[key] = None
    return safe, flags
