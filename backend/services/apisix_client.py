import httpx
import logging
from typing import Any, Dict, Optional

from services.http_tls import tls_verify_enabled

logger = logging.getLogger(__name__)

# Error codes returned in the "error_code" field of the uniform response contract.
APISIX_UNREACHABLE = "APISIX_UNREACHABLE"
APISIX_AUTH_FAILED = "APISIX_AUTH_FAILED"
APISIX_NOT_FOUND = "APISIX_NOT_FOUND"
APISIX_INVALID = "APISIX_INVALID"
APISIX_TIMEOUT = "APISIX_TIMEOUT"
APISIX_ERROR = "APISIX_ERROR"


def _error_message(data: Any, status_code: int) -> str:
    """Extract APISIX's error text from a response body, with a sane fallback.

    APISIX error bodies typically look like {"error_msg": "..."} or
    {"message": "..."}.
    """
    if isinstance(data, dict):
        for key in ("error_msg", "message"):
            message = data.get(key)
            if isinstance(message, str) and message.strip():
                return message.strip()
    if isinstance(data, str) and data.strip():
        return data.strip()
    return f"APISIX Admin API returned HTTP {status_code}."


async def _request(
    admin_url: str,
    admin_key: str,
    method: str,
    path: str,
    json_body: Optional[Any] = None,
    timeout: float = 15.0,
) -> Dict[str, Any]:
    """Issue an APISIX Admin API call and map exceptions to the uniform contract.

    On success, returns {"ok": True, "reachable": True, "status_code": ..., "data": <response>}.
    The caller is expected to interpret status_code/data further (e.g. treat 404 as ok=True).
    """
    base_url = (admin_url or "").rstrip("/")
    url = f"{base_url}{path}"
    headers = {"X-API-KEY": admin_key or ""}
    try:
        async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=timeout) as client:
            response = await client.request(
                method,
                url,
                json=json_body,
                headers=headers,
            )
            data: Any = None
            try:
                if response.content:
                    data = response.json()
            except Exception:
                data = response.text

            if response.status_code in (401, 403):
                return {
                    "ok": False,
                    "reachable": True,
                    "status_code": response.status_code,
                    "error": f"Authentication failed ({response.status_code}).",
                    "error_code": APISIX_AUTH_FAILED,
                    "data": data,
                }

            if 200 <= response.status_code < 300:
                return {
                    "ok": True,
                    "reachable": True,
                    "status_code": response.status_code,
                    "data": data,
                }

            # Any other non-2xx is a failed request. Callers that tolerate a specific
            # status (e.g. delete_route on 404) re-interpret it from this result.
            if response.status_code == 400:
                error_code = APISIX_INVALID
            elif response.status_code == 404:
                error_code = APISIX_NOT_FOUND
            else:
                error_code = APISIX_ERROR
            return {
                "ok": False,
                "reachable": True,
                "status_code": response.status_code,
                "error": _error_message(data, response.status_code)[:300],
                "error_code": error_code,
                "data": data,
            }
    except httpx.ConnectError as e:
        logger.warning(f"APISIX Admin API unreachable at {url}: {e}")
        return {
            "ok": False,
            "reachable": False,
            "status_code": None,
            "error": f"Cannot connect to APISIX Admin API at {base_url}."[:300],
            "error_code": APISIX_UNREACHABLE,
        }
    except httpx.TimeoutException:
        return {
            "ok": False,
            "reachable": False,
            "status_code": None,
            "error": "Connection to APISIX Admin API timed out.",
            "error_code": APISIX_TIMEOUT,
        }
    except Exception as e:
        logger.error(f"APISIX Admin API request error: {e}")
        return {
            "ok": False,
            "reachable": False,
            "status_code": None,
            "error": str(e)[:300],
            "error_code": APISIX_ERROR,
        }


async def test_admin(admin_url: str, admin_key: str) -> Dict[str, Any]:
    """Test APISIX Admin API reachability + auth by listing routes."""
    result = await _request(admin_url, admin_key, "GET", "/apisix/admin/routes")
    if not result["ok"]:
        return result
    return {
        "ok": True,
        "reachable": True,
        "status_code": result["status_code"],
        "message": "APISIX Admin API reachable.",
        "data": result.get("data"),
        "error_code": None,
    }


# --- Routes -----------------------------------------------------------------


async def list_routes(admin_url: str, admin_key: str) -> Dict[str, Any]:
    """List all APISIX routes."""
    result = await _request(admin_url, admin_key, "GET", "/apisix/admin/routes")
    if not result["ok"]:
        return result
    return {
        "ok": True,
        "reachable": True,
        "status_code": result["status_code"],
        "message": "Fetched APISIX routes.",
        "data": result.get("data"),
        "error_code": None,
    }


async def get_route(admin_url: str, admin_key: str, route_id: str) -> Dict[str, Any]:
    """Get a single APISIX route by id."""
    result = await _request(admin_url, admin_key, "GET", f"/apisix/admin/routes/{route_id}")
    if not result["ok"]:
        if result.get("status_code") == 404:
            result["error_code"] = APISIX_NOT_FOUND
            result["error"] = f"Route '{route_id}' not found."
        return result
    return {
        "ok": True,
        "reachable": True,
        "status_code": result["status_code"],
        "message": f"Fetched route '{route_id}'.",
        "data": result.get("data"),
        "error_code": None,
    }


async def put_route(admin_url: str, admin_key: str, route_id: str, body: Dict[str, Any]) -> Dict[str, Any]:
    """Create or replace an APISIX route at the given id."""
    result = await _request(admin_url, admin_key, "PUT", f"/apisix/admin/routes/{route_id}", json_body=body)
    if not result["ok"]:
        if result.get("status_code") == 400:
            result["error_code"] = APISIX_INVALID
        return result
    return {
        "ok": True,
        "reachable": True,
        "status_code": result["status_code"],
        "message": f"Route '{route_id}' created/updated.",
        "data": result.get("data"),
        "error_code": None,
    }


async def delete_route(admin_url: str, admin_key: str, route_id: str) -> Dict[str, Any]:
    """Delete an APISIX route. A 404 is treated as ok=True since the route is already gone."""
    result = await _request(admin_url, admin_key, "DELETE", f"/apisix/admin/routes/{route_id}")
    if not result["ok"]:
        if result.get("status_code") == 404:
            return {
                "ok": True,
                "reachable": True,
                "status_code": 404,
                "message": f"Route '{route_id}' already deleted (not found).",
                "data": result.get("data"),
                "error_code": None,
            }
        return result
    return {
        "ok": True,
        "reachable": True,
        "status_code": result["status_code"],
        "message": f"Route '{route_id}' deleted.",
        "data": result.get("data"),
        "error_code": None,
    }


# --- Upstreams ----------------------------------------------------------------


async def list_upstreams(admin_url: str, admin_key: str) -> Dict[str, Any]:
    """List all APISIX upstreams."""
    result = await _request(admin_url, admin_key, "GET", "/apisix/admin/upstreams")
    if not result["ok"]:
        return result
    return {
        "ok": True,
        "reachable": True,
        "status_code": result["status_code"],
        "message": "Fetched APISIX upstreams.",
        "data": result.get("data"),
        "error_code": None,
    }


async def get_upstream(admin_url: str, admin_key: str, upstream_id: str) -> Dict[str, Any]:
    """Get a single APISIX upstream by id."""
    result = await _request(admin_url, admin_key, "GET", f"/apisix/admin/upstreams/{upstream_id}")
    if not result["ok"]:
        if result.get("status_code") == 404:
            result["error_code"] = APISIX_NOT_FOUND
            result["error"] = f"Upstream '{upstream_id}' not found."
        return result
    return {
        "ok": True,
        "reachable": True,
        "status_code": result["status_code"],
        "message": f"Fetched upstream '{upstream_id}'.",
        "data": result.get("data"),
        "error_code": None,
    }


async def put_upstream(admin_url: str, admin_key: str, upstream_id: str, body: Dict[str, Any]) -> Dict[str, Any]:
    """Create or replace an APISIX upstream at the given id."""
    result = await _request(admin_url, admin_key, "PUT", f"/apisix/admin/upstreams/{upstream_id}", json_body=body)
    if not result["ok"]:
        if result.get("status_code") == 400:
            result["error_code"] = APISIX_INVALID
        return result
    return {
        "ok": True,
        "reachable": True,
        "status_code": result["status_code"],
        "message": f"Upstream '{upstream_id}' created/updated.",
        "data": result.get("data"),
        "error_code": None,
    }


async def delete_upstream(admin_url: str, admin_key: str, upstream_id: str) -> Dict[str, Any]:
    """Delete an APISIX upstream. A 404 is treated as ok=True since the upstream is already gone."""
    result = await _request(admin_url, admin_key, "DELETE", f"/apisix/admin/upstreams/{upstream_id}")
    if not result["ok"]:
        if result.get("status_code") == 404:
            return {
                "ok": True,
                "reachable": True,
                "status_code": 404,
                "message": f"Upstream '{upstream_id}' already deleted (not found).",
                "data": result.get("data"),
                "error_code": None,
            }
        return result
    return {
        "ok": True,
        "reachable": True,
        "status_code": result["status_code"],
        "message": f"Upstream '{upstream_id}' deleted.",
        "data": result.get("data"),
        "error_code": None,
    }


# --- SSLs -----------------------------------------------------------------


async def list_ssls(admin_url: str, admin_key: str) -> Dict[str, Any]:
    """List all APISIX SSL certificates."""
    result = await _request(admin_url, admin_key, "GET", "/apisix/admin/ssls")
    if not result["ok"]:
        return result
    return {
        "ok": True,
        "reachable": True,
        "status_code": result["status_code"],
        "message": "Fetched APISIX SSL certificates.",
        "data": result.get("data"),
        "error_code": None,
    }


async def put_ssl(admin_url: str, admin_key: str, ssl_id: str, body: Dict[str, Any]) -> Dict[str, Any]:
    """Create or replace an APISIX SSL certificate at the given id."""
    result = await _request(admin_url, admin_key, "PUT", f"/apisix/admin/ssls/{ssl_id}", json_body=body)
    if not result["ok"]:
        if result.get("status_code") == 400:
            result["error_code"] = APISIX_INVALID
        return result
    return {
        "ok": True,
        "reachable": True,
        "status_code": result["status_code"],
        "message": f"SSL certificate '{ssl_id}' created/updated.",
        "data": result.get("data"),
        "error_code": None,
    }


async def delete_ssl(admin_url: str, admin_key: str, ssl_id: str) -> Dict[str, Any]:
    """Delete an APISIX SSL certificate. A 404 is treated as ok=True since it is already gone."""
    result = await _request(admin_url, admin_key, "DELETE", f"/apisix/admin/ssls/{ssl_id}")
    if not result["ok"]:
        if result.get("status_code") == 404:
            return {
                "ok": True,
                "reachable": True,
                "status_code": 404,
                "message": f"SSL certificate '{ssl_id}' already deleted (not found).",
                "data": result.get("data"),
                "error_code": None,
            }
        return result
    return {
        "ok": True,
        "reachable": True,
        "status_code": result["status_code"],
        "message": f"SSL certificate '{ssl_id}' deleted.",
        "data": result.get("data"),
        "error_code": None,
    }
