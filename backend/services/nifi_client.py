"""NiFi REST API client.

NiFi uses JWT authentication:
1. POST /nifi-api/access/token  with form body username= password= → returns JWT string
2. All subsequent calls use Authorization: Bearer <jwt>

For Basic auth connections we transparently do the login exchange before each API call.
"""
import time
import httpx
import logging
from typing import Optional, Dict, Any, Tuple
from services.http_tls import tls_verify_enabled

logger = logging.getLogger(__name__)


def _normalize_nifi_base_url(url: str) -> str:
    """Normalize NiFi base URL by stripping trailing slashes only.

    Does NOT strip /nifi suffix — a reverse-proxy deployment at /nifi/nifi-api/...
    requires the /nifi path to be preserved.
    """
    return (url or "").strip().rstrip("/")

# ---------------------------------------------------------------------------
# Token cache — avoids re-authenticating on every API call within a session
# Cache TTL: 8 minutes (NiFi JWTs typically expire after 12 hours)
# ---------------------------------------------------------------------------
_TOKEN_CACHE: Dict[str, Tuple[str, float]] = {}  # key: "url:username" → (token, expiry)
_TOKEN_TTL_SECONDS = 480  # 8 minutes


def _nifi_ui_path_error(status_code: Optional[int] = None) -> Dict[str, Any]:
    return {
        "ok": False,
        "reachable": True,
        "error": "This looks like the NiFi UI path, not the API base. Use the API base URL (for example drop a trailing `/nifi`).",
        "status_code": status_code,
    }


def _nifi_sni_error(status_code: Optional[int] = None) -> Dict[str, Any]:
    return {
        "ok": False,
        "reachable": False,
        "error": "NiFi rejected the request (Invalid SNI). Use a hostname (for example https://localhost:8443) rather than an IP address.",
        "status_code": status_code,
    }


def _classify_nifi_http_response(response: httpx.Response) -> Optional[Dict[str, Any]]:
    if response.status_code in (301, 302, 303, 307, 308):
        return _nifi_ui_path_error(response.status_code)

    text = response.text or ""
    content_type = response.headers.get("content-type", "").lower()
    if "Invalid SNI" in text:
        return _nifi_sni_error(response.status_code)
    if "text/html" in content_type or text.lstrip().startswith("<"):
        return _nifi_ui_path_error(response.status_code)
    return None


async def _get_nifi_token(base_url: str, username: str, password: str) -> Dict[str, Any]:
    """Obtain a NiFi JWT by exchanging username/password via the access/token endpoint.

    Results are cached for _TOKEN_TTL_SECONDS to avoid repeated round-trips.
    """
    base_url = _normalize_nifi_base_url(base_url)
    cache_key = f"{base_url}:{username}"
    now = time.monotonic()

    # Return cached token if still valid
    if cache_key in _TOKEN_CACHE:
        cached_token, expiry = _TOKEN_CACHE[cache_key]
        if now < expiry:
            return {"ok": True, "token": cached_token, "reachable": True}

    token_url = f"{base_url}/nifi-api/access/token"
    try:
        async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=15.0) as client:
            resp = await client.post(
                token_url,
                data={"username": username, "password": password},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            if resp.status_code in (200, 201):
                token = resp.text.strip()
                _TOKEN_CACHE[cache_key] = (token, now + _TOKEN_TTL_SECONDS)
                return {"ok": True, "token": token, "reachable": True, "status_code": resp.status_code}

            classified = _classify_nifi_http_response(resp)
            if classified:
                logger.warning("NiFi token endpoint returned %s: %s", resp.status_code, classified["error"])
                return classified

            logger.warning(f"NiFi token endpoint returned {resp.status_code}")
            if resp.status_code == 401:
                return {"ok": False, "reachable": True, "error": "Authentication failed. Check credentials.", "status_code": 401}
            if resp.status_code == 403:
                return {"ok": False, "reachable": True, "error": "Access forbidden. Check user permissions.", "status_code": 403}
            return {"ok": False, "reachable": True, "error": f"Unexpected HTTP {resp.status_code} from NiFi token endpoint.", "status_code": resp.status_code}
    except httpx.ConnectError as e:
        logger.error(f"Failed to obtain NiFi token: {e}")
        return {"ok": False, "reachable": False, "error": f"Cannot connect to NiFi at {base_url}. Verify the URL and network."}
    except httpx.TimeoutException:
        logger.error("Failed to obtain NiFi token: timeout")
        return {"ok": False, "reachable": False, "error": "Connection timed out. NiFi may be slow or unreachable."}
    except Exception as e:
        logger.error(f"Failed to obtain NiFi token: {e}")
        return {"ok": False, "reachable": False, "error": str(e)[:300]}


async def _resolve_bearer(
    base_url: str,
    auth_type: str,
    username: Optional[str],
    password: Optional[str],
    token: Optional[str],
) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """Resolve the final Bearer token to use for NiFi API calls.

    Returns (bearer_token, error_message).
    """
    if auth_type == "BEARER" and token:
        return token, None

    if auth_type == "BASIC" and username and password:
        token_result = await _get_nifi_token(base_url, username, password)
        if token_result.get("ok") and token_result.get("token"):
            return token_result["token"], None
        return None, token_result

    return None, None  # No auth — try unauthenticated (some NiFi setups allow it)


async def test_nifi_connection(
    url: str,
    auth_type: str = "NONE",
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    """Test NiFi connectivity — obtains JWT if needed, then calls system-diagnostics."""
    base_url = _normalize_nifi_base_url(url)

    bearer, err = await _resolve_bearer(base_url, auth_type, username, password, token)
    if err:
        return {"ok": False, **err}

    headers = {}
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"

    api_url = f"{base_url}/nifi-api/system-diagnostics"
    try:
        async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=15.0) as client:
            response = await client.get(api_url, headers=headers)

            classified = _classify_nifi_http_response(response)
            if classified:
                return classified

            if response.status_code == 200:
                try:
                    data = response.json()
                    version_info = (
                        data.get("systemDiagnostics", {})
                        .get("aggregateSnapshot", {})
                        .get("versionInfo", {})
                    )
                    nifi_ver = version_info.get("niFiVersion", "unknown")
                    build_ts = version_info.get("buildTimestamp", "")
                except Exception:
                    nifi_ver = "unknown"
                    build_ts = ""
                msg = f"NiFi connected successfully. Version: {nifi_ver}"
                if build_ts:
                    msg += f" ({build_ts})"
                return {"ok": True, "message": msg, "status_code": 200, "reachable": True}

            elif response.status_code == 401:
                return {"ok": False, "error": "Authentication failed. Check credentials.", "status_code": 401, "reachable": True}
            elif response.status_code == 403:
                return {"ok": False, "error": "Access forbidden. Check user permissions.", "status_code": 403, "reachable": True}
            else:
                return {"ok": False, "error": f"Unexpected HTTP {response.status_code}", "status_code": response.status_code, "reachable": True}

    except httpx.ConnectError as e:
        logger.warning(f"NiFi connect error: {e}")
        return {"ok": False, "error": f"Cannot connect to NiFi at {url}. Verify the URL and network.", "reachable": False}
    except httpx.TimeoutException:
        return {"ok": False, "error": "Connection timed out. NiFi may be slow or unreachable.", "reachable": False}
    except Exception as e:
        logger.error(f"NiFi test error: {e}")
        return {"ok": False, "error": str(e)[:300], "reachable": False}


async def nifi_api_request(
    base_url: str,
    method: str,
    path: str,
    auth_type: str = "NONE",
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
    json_body: Optional[Dict] = None,
    params: Optional[Dict] = None,
) -> Dict[str, Any]:
    """Generic NiFi API request helper used by the flow generator."""
    url = _normalize_nifi_base_url(base_url)
    bearer, err = await _resolve_bearer(url, auth_type, username, password, token)
    if err:
        return {"ok": False, **err}

    headers = {"Content-Type": "application/json"}
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"

    try:
        async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=30.0) as client:
            response = await client.request(
                method=method.upper(),
                url=f"{url}{path}",
                headers=headers,
                json=json_body,
                params=params,
            )
            if response.status_code in (200, 201, 202):
                return {"ok": True, "data": response.json(), "status_code": response.status_code, "reachable": True}
            elif response.status_code == 204:
                return {"ok": True, "data": None, "status_code": 204, "reachable": True}
            else:
                classified = _classify_nifi_http_response(response)
                if classified:
                    return classified
                try:
                    detail = response.json()
                except Exception:
                    detail = response.text[:300]
                return {"ok": False, "error": f"HTTP {response.status_code}: {detail}", "status_code": response.status_code, "reachable": True}
    except httpx.ConnectError as e:
        logger.error(f"NiFi API request failed [{method} {path}]: {e}")
        return {"ok": False, "error": f"Cannot connect to NiFi at {base_url}. Verify the URL and network.", "reachable": False}
    except httpx.TimeoutException:
        logger.error(f"NiFi API request timed out [{method} {path}]")
        return {"ok": False, "error": "Connection timed out. NiFi may be slow or unreachable.", "reachable": False}
    except Exception as e:
        logger.error(f"NiFi API request failed [{method} {path}]: {e}")
        return {"ok": False, "error": str(e)[:300], "reachable": False}


async def get_nifi_root_process_group_result(
    url: str,
    auth_type: str = "NONE",
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    """Get the root process group request result, preserving reachability details."""
    base_url = _normalize_nifi_base_url(url)
    result = await nifi_api_request(
        base_url,
        "GET",
        "/nifi-api/process-groups/root",
        auth_type=auth_type,
        username=username,
        password=password,
        token=token,
    )
    if result.get("ok"):
        data = result.get("data") or {}
        result["root_process_group_id"] = data.get("id")
    return result


async def get_nifi_root_process_group_id(
    url: str,
    auth_type: str = "NONE",
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
) -> Optional[str]:
    """Get the root process group ID from NiFi."""
    result = await get_nifi_root_process_group_result(
        url,
        auth_type=auth_type,
        username=username,
        password=password,
        token=token,
    )
    if result.get("ok"):
        return result.get("root_process_group_id")
    return None
