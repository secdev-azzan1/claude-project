import httpx
import logging
from typing import Optional, Dict, Any
from services.http_tls import tls_verify_enabled

logger = logging.getLogger(__name__)

# Apicurio Registry v2/v3 API paths to try during health check
API_PROBE_PATHS = [
    "/apis/registry/v3/admin/config",  # Apicurio 3.x
    "/apis/registry/v2/groups",        # Apicurio 2.x
    "/api/groups",                      # Apicurio 2.x (alt path)
    "/api/artifacts",                   # Legacy Apicurio
]


def _normalize_apicurio_endpoints(url: str) -> Dict[str, str]:
    """Normalize configured endpoint into registry root + ccompat base.

    Supports connection endpoints saved as:
    - http(s)://host
    - http(s)://host/apis/registry/v3
    - http(s)://host/apis/registry/v2
    - http(s)://host/apis/ccompat/v7
    """
    raw = (url or "").rstrip("/")
    if not raw:
        return {"root": "", "ccompat": ""}

    # Strip known API suffixes to get registry root.
    markers = [
        "/apis/ccompat/",
        "/apis/registry/v3",
        "/apis/registry/v2",
        "/api/groups",
        "/api/artifacts",
    ]
    root = raw
    for marker in markers:
        idx = root.find(marker)
        if idx != -1:
            root = root[:idx]
            break
    root = root.rstrip("/")

    # If caller already gave a ccompat URL, keep that exact base.
    if "/apis/ccompat/" in raw:
        ccompat = raw
    else:
        ccompat = f"{root}/apis/ccompat/v7"

    return {"root": root, "ccompat": ccompat.rstrip("/")}


async def test_apicurio_connection(
    url: str,
    auth_type: str = "NONE",
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    """Test Apicurio Schema Registry connection by probing known API paths."""
    endpoints = _normalize_apicurio_endpoints(url)
    root_url = endpoints["root"]
    ccompat_url = endpoints["ccompat"]

    headers = {}
    auth = None
    if auth_type == "BEARER" and token:
        headers["Authorization"] = f"Bearer {token}"
    elif auth_type == "BASIC" and username and password:
        auth = (username, password)

    try:
        async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=10.0) as client:
            # First probe ccompat directly (works for ccompat-only configured endpoints).
            try:
                cc = await client.get(f"{ccompat_url}/subjects", headers=headers, auth=auth)
                if cc.status_code in (200, 404):
                    return {
                        "ok": True,
                        "message": "Apicurio Registry reachable (Confluent-compat).",
                        "status_code": cc.status_code,
                    }
                if cc.status_code == 401:
                    return {"ok": False, "error": "Authentication required. Add credentials.", "status_code": 401}
            except httpx.ConnectError:
                pass
            except Exception:
                pass

            for path in API_PROBE_PATHS:
                endpoint = f"{root_url}{path}"
                try:
                    response = await client.get(endpoint, headers=headers, auth=auth)
                    if response.status_code in (200, 404):
                        # 404 can mean the registry exists but has no artifacts/groups yet
                        version_hint = ""
                        if "/v3/" in path:
                            version_hint = " (Apicurio 3.x)"
                        elif "/v2/" in path:
                            version_hint = " (Apicurio 2.x)"
                        return {
                            "ok": True,
                            "message": f"Apicurio Registry reachable{version_hint}.",
                            "status_code": response.status_code,
                        }
                    elif response.status_code == 401:
                        return {"ok": False, "error": "Authentication required. Add credentials.", "status_code": 401}
                except httpx.ConnectError:
                    continue  # try next path
                except Exception:
                    continue

            return {"ok": False, "error": f"Cannot reach Apicurio Registry at {url}. None of the known API paths responded."}
    except httpx.ConnectError as e:
        logger.warning(f"Apicurio connect error: {e}")
        return {"ok": False, "error": f"Cannot connect to Apicurio at {url}."}
    except httpx.TimeoutException:
        return {"ok": False, "error": "Connection timed out."}
    except Exception as e:
        logger.error(f"Apicurio test error: {e}")
        return {"ok": False, "error": str(e)[:300]}


async def _register_schema_ccompat_only(
    ccompat_url: str,
    artifact_id: str,
    schema_str: str,
    bearer_header: Dict[str, str],
    auth: Optional[tuple],
) -> Dict[str, Any]:
    """E6 fix: register exactly once, via the Confluent-compatible API only
    — no native v3/v2 follow-up POST. `register_schema`'s default dual-write
    (ccompat POST + native v3/v2 POST) lands on the same underlying Apicurio
    artifact and BOTH increment the subject's ccompat version counter even
    for byte-identical content, so one logical registration always consumed
    2 ccompat version slots. `routers/v2/schemas.py::delete_approved_schema_
    version` then forwarded the app's own 1-per-approval version counter
    straight through as the literal ccompat version to delete — from the
    second approval onward the two numbering schemes were no longer
    aligned, so delete targeted the WRONG registry version (live-verified,
    docs/orchestration/e2e/journey-c-d.md DEFECT-2). Live E2E evidence also
    showed content registered via ccompat IS resolvable through the native
    v3 API for the same subject/group (Apicurio unifies both APIs over one
    content store) — so skipping the native POST here does not make the
    schema any less visible to a native-API reader (e.g. NiFi's own
    ConfluentSchemaRegistry CS already reads ccompat directly, per the
    compiled flow structure verified in journey-a-e.md).

    Callers of `register_schema(..., ccompat_only=True)` — every v2 router
    call site (approve/register) — get a `version` field back (the ccompat
    version this call just created), which the legacy dual-write path never
    returned (its `version` is always the native API's `version`, which is
    NOT the ccompat version needed for the delete-by-version path)."""
    async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=15.0) as client:
        cc_ep = f"{ccompat_url}/subjects/{artifact_id}/versions"
        cc_payload = {"schema": schema_str, "schemaType": "AVRO"}
        try:
            r = await client.post(
                cc_ep,
                json=cc_payload,
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json", **bearer_header},
                auth=auth,
            )
        except Exception as e:
            return {"ok": False, "error": f"ccompat subject register error: {str(e)[:200]}"}
        if r.status_code not in (200, 201):
            return {"ok": False, "error": f"ccompat subject register failed {r.status_code}: {r.text[:200]}"}
        try:
            data = r.json()
        except Exception:
            data = {}
        schema_id = data.get("id")

        version = None
        try:
            latest = await client.get(
                f"{ccompat_url}/subjects/{artifact_id}/versions/latest",
                headers=bearer_header,
                auth=auth,
            )
            if latest.status_code == 200:
                version = latest.json().get("version")
        except Exception as e:
            logger.warning(f"ccompat-only register: could not resolve the new version number: {e}")

        return {"ok": True, "api_version": "ccompat", "global_id": schema_id, "version": version, "ccompat_id": schema_id}


async def register_schema(
    url: str,
    group_id: str,
    artifact_id: str,
    avro_schema: Dict[str, Any],
    auth_type: str = "NONE",
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
    ccompat_only: bool = False,
) -> Dict[str, Any]:
    """Register or update an Avro schema in Apicurio Registry.

    Strategy:
    1) Try Confluent-compatible subjects API first (works for ccompat-only endpoints).
    2) Fallback to native Apicurio v3, then v2 endpoints when available.

    `ccompat_only=True` (E6 — see `_register_schema_ccompat_only`'s
    docstring) skips step 2 entirely: exactly one ccompat registration call,
    no native v3/v2 follow-up write, so ccompat's per-subject version
    counter advances by exactly 1 per logical registration. Every v2 router
    call site (`routers/v2/schemas.py`'s `approve_schema` /
    `register_schema_standalone`) passes this; the legacy alpha router
    (`routers/flows.py`, `routers/schemas.py`) does not pass it and keeps
    the original dual-write behaviour unchanged.
    """
    import json
    endpoints = _normalize_apicurio_endpoints(url)
    root_url = endpoints["root"]
    ccompat_url = endpoints["ccompat"]
    group = group_id or "default"

    auth = None
    bearer_header: Dict[str, str] = {}
    if auth_type == "BEARER" and token:
        bearer_header["Authorization"] = f"Bearer {token}"
    elif auth_type == "BASIC" and username and password:
        auth = (username, password)

    schema_str = json.dumps(avro_schema)

    if ccompat_only:
        return await _register_schema_ccompat_only(ccompat_url, artifact_id, schema_str, bearer_header, auth)

    async def _sync_ccompat_subject() -> Dict[str, Any]:
        """
        Ensure schema is also available via Confluent-compatible subjects API.
        NiFi ConfluentSchemaRegistry looks up schemas through this endpoint.
        """
        cc_ep = f"{ccompat_url}/subjects/{artifact_id}/versions"
        cc_payload = {
            "schema": schema_str,
            "schemaType": "AVRO",
        }
        try:
            cc = await client.post(
                cc_ep,
                json=cc_payload,
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json", **bearer_header},
                auth=auth,
            )
            if cc.status_code in (200, 201):
                data = cc.json()
                return {"ok": True, "id": data.get("id")}
            return {
                "ok": False,
                "error": f"ccompat subject register failed {cc.status_code}: {cc.text[:200]}",
            }
        except Exception as e:
            return {"ok": False, "error": f"ccompat subject register error: {str(e)[:200]}"}

    async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=15.0) as client:
        # Keep the Confluent-compatible subject in sync, but do not stop here.
        # NiFi's ApicurioSchemaRegistry controller service reads native Apicurio
        # artifacts by group/artifact id, not ccompat subjects.
        cc_initial = await _sync_ccompat_subject()
        cc_id = cc_initial.get("id") if cc_initial.get("ok") else None
        cc_error = cc_initial.get("error")
        if cc_error:
            logger.warning(f"Apicurio ccompat register failed before v3/v2 fallback: {cc_error}")

        # Try v3 first: POST a new version under the artifact (creates artifact if missing via fallback)
        v3_versions_ep = f"{root_url}/apis/registry/v3/groups/{group}/artifacts/{artifact_id}/versions"
        v3_create_ep = f"{root_url}/apis/registry/v3/groups/{group}/artifacts"
        v3_content_obj = {"content": schema_str, "contentType": "application/json"}
        v3_payload_version = {"content": v3_content_obj}
        v3_payload_create = {
            "artifactId": artifact_id,
            "artifactType": "AVRO",
            "firstVersion": {"content": v3_content_obj},
        }
        try:
            r = await client.post(
                v3_versions_ep,
                json=v3_payload_version,
                headers={"Content-Type": "application/json", **bearer_header},
                auth=auth,
            )
            if r.status_code in (200, 201):
                data = r.json()
                return {
                    "ok": True,
                    "api_version": "v3",
                    "global_id": data.get("globalId"),
                    "version": data.get("version"),
                    "ccompat_id": cc_id,
                }
            if r.status_code == 404:
                # Artifact does not exist → create it with first version
                r2 = await client.post(
                    v3_create_ep,
                    json=v3_payload_create,
                    headers={"Content-Type": "application/json", **bearer_header},
                    auth=auth,
                )
                if r2.status_code in (200, 201):
                    data = r2.json()
                    version_data = data.get("version") or {}
                    return {
                        "ok": True,
                        "api_version": "v3",
                        "global_id": version_data.get("globalId"),
                        "version": version_data.get("version"),
                        "ccompat_id": cc_id,
                    }
                logger.warning(f"Apicurio v3 create failed {r2.status_code}: {r2.text[:200]}")
            else:
                logger.warning(f"Apicurio v3 versions POST failed {r.status_code}: {r.text[:200]}")
        except httpx.RequestError as e:
            logger.warning(f"Apicurio v3 register error: {e}")

        # Fallback to v2 (legacy)
        v2_ep = f"{root_url}/apis/registry/v2/groups/{group}/artifacts/{artifact_id}/versions"
        try:
            r = await client.post(
                v2_ep,
                content=schema_str,
                headers={"Content-Type": "application/json", "X-Registry-ArtifactType": "AVRO", **bearer_header},
                auth=auth,
            )
            if r.status_code in (200, 201):
                data = r.json()
                return {
                    "ok": True,
                    "api_version": "v2",
                    "global_id": data.get("globalId") or data.get("id"),
                    "version": data.get("version"),
                    "ccompat_id": cc_id,
                }
        except httpx.RequestError as e:
            logger.warning(f"Apicurio v2 register error: {e}")

        if cc_initial.get("ok"):
            return {
                "ok": False,
                "error": "Schema was registered through Apicurio ccompat, but native Apicurio artifact registration failed. NiFi ApicurioSchemaRegistry requires the native artifact.",
                "ccompat_id": cc_id,
            }
        if cc_error:
            return {
                "ok": False,
                "error": f"Schema registration failed on Apicurio ccompat, v3 and v2 endpoints. ccompat error: {cc_error}",
            }
        return {"ok": False, "error": "Schema registration failed on Apicurio ccompat, v3 and v2 endpoints."}


async def schema_available(
    url: str,
    group_id: str,
    artifact_id: str,
    auth_type: str = "NONE",
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    """Check whether a schema exists in both native and ccompat Apicurio APIs."""
    endpoints = _normalize_apicurio_endpoints(url)
    root_url = endpoints["root"]
    ccompat_url = endpoints["ccompat"]
    group = group_id or "default"

    auth = None
    bearer_header: Dict[str, str] = {}
    if auth_type == "BEARER" and token:
        bearer_header["Authorization"] = f"Bearer {token}"
    elif auth_type == "BASIC" and username and password:
        auth = (username, password)

    native_ok = False
    ccompat_ok = False
    native_status = None
    ccompat_status = None
    async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=10.0) as client:
        native_paths = [
            f"{root_url}/apis/registry/v3/groups/{group}/artifacts/{artifact_id}",
            f"{root_url}/apis/registry/v2/groups/{group}/artifacts/{artifact_id}",
        ]
        for endpoint in native_paths:
            try:
                resp = await client.get(endpoint, headers=bearer_header, auth=auth)
                native_status = resp.status_code
                if resp.status_code == 200:
                    native_ok = True
                    break
            except httpx.RequestError:
                continue

        try:
            resp = await client.get(
                f"{ccompat_url}/subjects/{artifact_id}/versions/latest",
                headers=bearer_header,
                auth=auth,
            )
            ccompat_status = resp.status_code
            ccompat_ok = resp.status_code == 200
        except httpx.RequestError:
            ccompat_status = None

    return {
        "ok": native_ok and ccompat_ok,
        "native_ok": native_ok,
        "ccompat_ok": ccompat_ok,
        "native_status": native_status,
        "ccompat_status": ccompat_status,
    }
