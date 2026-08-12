import httpx
import logging
import urllib.parse
from typing import Optional, Dict, Any, Tuple
from services.http_tls import tls_verify_enabled

logger = logging.getLogger(__name__)

# Error codes returned in the "error_code" field of the uniform response contract.
CONNECT_UNREACHABLE = "CONNECT_UNREACHABLE"
CONNECT_AUTH_FAILED = "CONNECT_AUTH_FAILED"
CONNECT_REBALANCING = "CONNECT_REBALANCING"
CONNECT_CONFIG_INVALID = "CONNECT_CONFIG_INVALID"
CONNECT_PLUGIN_MISSING = "CONNECT_PLUGIN_MISSING"
CONNECT_NOT_FOUND = "CONNECT_NOT_FOUND"
CONNECT_TIMEOUT = "CONNECT_TIMEOUT"


def _auth_headers(
    auth_type: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
) -> Tuple[Dict[str, str], Optional[Tuple[str, str]]]:
    """Build request headers/auth tuple for the given auth type (NONE/BEARER/BASIC)."""
    headers: Dict[str, str] = {}
    auth: Optional[Tuple[str, str]] = None
    if auth_type == "BEARER" and token:
        headers["Authorization"] = f"Bearer {token}"
    elif auth_type == "BASIC" and username and password:
        auth = (username, password)
    return headers, auth


def _error_message(data: Any, status_code: int) -> str:
    """Extract Kafka Connect's error text from a response body, with a sane fallback.

    Connect error bodies look like {"error_code": 400, "message": "..."}.
    """
    if isinstance(data, dict):
        message = data.get("message")
        if isinstance(message, str) and message.strip():
            return message.strip()
    if isinstance(data, str) and data.strip():
        return data.strip()
    return f"Kafka Connect returned HTTP {status_code}."


async def _request(
    conn: Dict[str, Any],
    method: str,
    path: str,
    json_body: Optional[Any] = None,
    timeout: float = 15.0,
) -> Dict[str, Any]:
    """Issue a Kafka Connect REST call and map exceptions to the uniform contract.

    On success, returns {"ok": True, "reachable": True, "status_code": ..., "data": <response>}.
    The caller is expected to interpret status_code/data further (e.g. treat 404 as ok=True).
    """
    base_url = (conn.get("endpoint") or "").rstrip("/")
    url = f"{base_url}{path}"
    headers, auth = _auth_headers(
        conn.get("auth_type", "NONE"),
        conn.get("username"),
        conn.get("password"),
        conn.get("token"),
    )
    try:
        async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=timeout) as client:
            response = await client.request(
                method,
                url,
                json=json_body,
                headers=headers,
                auth=auth,
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
                    "error_code": CONNECT_AUTH_FAILED,
                    "data": data,
                }
            if response.status_code == 409:
                return {
                    "ok": False,
                    "reachable": True,
                    "status_code": response.status_code,
                    "error": "Kafka Connect cluster is rebalancing. Retry shortly.",
                    "error_code": CONNECT_REBALANCING,
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
            # status (e.g. delete_connector on 404) re-interpret it from this result.
            if response.status_code == 400:
                error_code = CONNECT_CONFIG_INVALID
            elif response.status_code == 404:
                error_code = CONNECT_NOT_FOUND
            else:
                error_code = None
            return {
                "ok": False,
                "reachable": True,
                "status_code": response.status_code,
                "error": _error_message(data, response.status_code)[:300],
                "error_code": error_code,
                "data": data,
            }
    except httpx.ConnectError as e:
        logger.warning(f"Kafka Connect unreachable at {url}: {e}")
        return {
            "ok": False,
            "reachable": False,
            "status_code": None,
            "error": f"Cannot connect to Kafka Connect at {base_url}."[:300],
            "error_code": CONNECT_UNREACHABLE,
        }
    except httpx.TimeoutException:
        return {
            "ok": False,
            "reachable": False,
            "status_code": None,
            "error": "Connection to Kafka Connect timed out.",
            "error_code": CONNECT_TIMEOUT,
        }
    except Exception as e:
        logger.error(f"Kafka Connect request error: {e}")
        return {
            "ok": False,
            "reachable": False,
            "status_code": None,
            "error": str(e)[:300],
            "error_code": None,
        }


async def test_kafka_connect_connection(
    url: str,
    auth_type: str = "NONE",
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    """Test a Kafka Connect connection by hitting the cluster root endpoint."""
    conn = {
        "endpoint": url,
        "auth_type": auth_type,
        "username": username,
        "password": password,
        "token": token,
    }
    result = await _request(conn, "GET", "/")
    if not result["ok"]:
        return result
    if result["status_code"] == 200:
        return {
            "ok": True,
            "reachable": True,
            "status_code": 200,
            "message": "Kafka Connect cluster reachable.",
            "data": result.get("data"),
            "error_code": None,
        }
    return {
        "ok": False,
        "reachable": True,
        "status_code": result["status_code"],
        "error": f"Unexpected response from Kafka Connect ({result['status_code']}).",
        "error_code": None,
        "data": result.get("data"),
    }


async def get_cluster_info(conn: Dict[str, Any]) -> Dict[str, Any]:
    """Get Kafka Connect cluster info (version, commit, kafka_cluster_id)."""
    result = await _request(conn, "GET", "/")
    if not result["ok"]:
        return result
    return {
        "ok": True,
        "reachable": True,
        "status_code": result["status_code"],
        "message": "Fetched Kafka Connect cluster info.",
        "data": result.get("data"),
        "error_code": None,
    }


async def list_connector_plugins(conn: Dict[str, Any]) -> Dict[str, Any]:
    """List available Kafka Connect connector plugins."""
    result = await _request(conn, "GET", "/connector-plugins")
    if not result["ok"]:
        return result
    return {
        "ok": True,
        "reachable": True,
        "status_code": result["status_code"],
        "message": "Fetched connector plugins.",
        "data": result.get("data"),
        "error_code": None,
    }


async def list_connectors_with_status(conn: Dict[str, Any]) -> Dict[str, Any]:
    """List all connectors with expanded status and info."""
    result = await _request(conn, "GET", "/connectors?expand=status&expand=info")
    if not result["ok"]:
        return result
    return {
        "ok": True,
        "reachable": True,
        "status_code": result["status_code"],
        "message": "Fetched connectors with status.",
        "data": result.get("data"),
        "error_code": None,
    }


async def get_connector_status(conn: Dict[str, Any], name: str) -> Dict[str, Any]:
    """Get the runtime status of a connector."""
    encoded = urllib.parse.quote(name, safe="")
    result = await _request(conn, "GET", f"/connectors/{encoded}/status")
    if not result["ok"]:
        if result.get("status_code") == 404:
            result["error_code"] = CONNECT_NOT_FOUND
            result["error"] = f"Connector '{name}' not found."
        return result
    return {
        "ok": True,
        "reachable": True,
        "status_code": result["status_code"],
        "message": f"Fetched status for connector '{name}'.",
        "data": result.get("data"),
        "error_code": None,
    }


async def get_connector_config(conn: Dict[str, Any], name: str) -> Dict[str, Any]:
    """Get the current configuration of a connector."""
    encoded = urllib.parse.quote(name, safe="")
    result = await _request(conn, "GET", f"/connectors/{encoded}/config")
    if not result["ok"]:
        if result.get("status_code") == 404:
            result["error_code"] = CONNECT_NOT_FOUND
            result["error"] = f"Connector '{name}' not found."
        return result
    return {
        "ok": True,
        "reachable": True,
        "status_code": result["status_code"],
        "message": f"Fetched config for connector '{name}'.",
        "data": result.get("data"),
        "error_code": None,
    }


async def upsert_connector(conn: Dict[str, Any], name: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """Create or update a connector via PUT /connectors/{name}/config."""
    encoded = urllib.parse.quote(name, safe="")
    result = await _request(conn, "PUT", f"/connectors/{encoded}/config", json_body=config)
    if not result["ok"]:
        if result.get("status_code") == 400:
            result["error_code"] = CONNECT_CONFIG_INVALID
        return result
    return {
        "ok": True,
        "reachable": True,
        "status_code": result["status_code"],
        "message": f"Connector '{name}' created/updated.",
        "data": result.get("data"),
        "error_code": None,
    }


async def pause_connector(conn: Dict[str, Any], name: str) -> Dict[str, Any]:
    """Pause a connector."""
    encoded = urllib.parse.quote(name, safe="")
    result = await _request(conn, "PUT", f"/connectors/{encoded}/pause")
    if not result["ok"]:
        if result.get("status_code") == 404:
            result["error_code"] = CONNECT_NOT_FOUND
            result["error"] = f"Connector '{name}' not found."
        return result
    if result["status_code"] not in (202, 204):
        return {
            "ok": False,
            "reachable": True,
            "status_code": result["status_code"],
            "error": f"Unexpected response pausing connector '{name}' ({result['status_code']}).",
            "error_code": None,
            "data": result.get("data"),
        }
    return {
        "ok": True,
        "reachable": True,
        "status_code": result["status_code"],
        "message": f"Connector '{name}' paused.",
        "data": result.get("data"),
        "error_code": None,
    }


async def resume_connector(conn: Dict[str, Any], name: str) -> Dict[str, Any]:
    """Resume a connector."""
    encoded = urllib.parse.quote(name, safe="")
    result = await _request(conn, "PUT", f"/connectors/{encoded}/resume")
    if not result["ok"]:
        if result.get("status_code") == 404:
            result["error_code"] = CONNECT_NOT_FOUND
            result["error"] = f"Connector '{name}' not found."
        return result
    if result["status_code"] not in (202, 204):
        return {
            "ok": False,
            "reachable": True,
            "status_code": result["status_code"],
            "error": f"Unexpected response resuming connector '{name}' ({result['status_code']}).",
            "error_code": None,
            "data": result.get("data"),
        }
    return {
        "ok": True,
        "reachable": True,
        "status_code": result["status_code"],
        "message": f"Connector '{name}' resumed.",
        "data": result.get("data"),
        "error_code": None,
    }


async def restart_connector(
    conn: Dict[str, Any],
    name: str,
    include_tasks: bool = True,
    only_failed: bool = True,
) -> Dict[str, Any]:
    """Restart a connector (and optionally its tasks)."""
    encoded = urllib.parse.quote(name, safe="")
    include_tasks_str = str(bool(include_tasks)).lower()
    only_failed_str = str(bool(only_failed)).lower()
    path = f"/connectors/{encoded}/restart?includeTasks={include_tasks_str}&onlyFailed={only_failed_str}"
    result = await _request(conn, "POST", path)
    if not result["ok"]:
        if result.get("status_code") == 404:
            result["error_code"] = CONNECT_NOT_FOUND
            result["error"] = f"Connector '{name}' not found."
        return result
    if result["status_code"] not in (200, 202, 204):
        return {
            "ok": False,
            "reachable": True,
            "status_code": result["status_code"],
            "error": f"Unexpected response restarting connector '{name}' ({result['status_code']}).",
            "error_code": None,
            "data": result.get("data"),
        }
    return {
        "ok": True,
        "reachable": True,
        "status_code": result["status_code"],
        "message": f"Connector '{name}' restarted.",
        "data": result.get("data"),
        "error_code": None,
    }


async def delete_connector(conn: Dict[str, Any], name: str) -> Dict[str, Any]:
    """Delete a connector. A 404 is treated as ok=True since the connector is already gone."""
    encoded = urllib.parse.quote(name, safe="")
    result = await _request(conn, "DELETE", f"/connectors/{encoded}")
    if not result["ok"]:
        if result.get("status_code") == 404:
            return {
                "ok": True,
                "reachable": True,
                "status_code": 404,
                "message": f"Connector '{name}' already deleted (not found).",
                "data": result.get("data"),
                "error_code": None,
            }
        return result
    return {
        "ok": True,
        "reachable": True,
        "status_code": result["status_code"],
        "message": f"Connector '{name}' deleted.",
        "data": result.get("data"),
        "error_code": None,
    }


async def validate_connector_config(
    conn: Dict[str, Any],
    connector_class: str,
    config: Dict[str, Any],
) -> Dict[str, Any]:
    """Validate a connector configuration against a connector plugin's config definitions."""
    encoded = urllib.parse.quote(connector_class, safe="")
    result = await _request(conn, "PUT", f"/connector-plugins/{encoded}/config/validate", json_body=config)
    if not result["ok"]:
        if result.get("status_code") == 404:
            result["error_code"] = CONNECT_PLUGIN_MISSING
            result["error"] = f"Connector plugin '{connector_class}' not found."
        return result

    data = result.get("data") or {}
    error_count = data.get("error_count", 0) if isinstance(data, dict) else 0
    if error_count == 0:
        return {
            "ok": True,
            "reachable": True,
            "status_code": result["status_code"],
            "message": "Connector configuration is valid.",
            "data": data,
            "error_code": None,
        }
    return {
        "ok": False,
        "reachable": True,
        "status_code": result["status_code"],
        "error": f"Connector configuration has {error_count} error(s).",
        "error_code": CONNECT_CONFIG_INVALID,
        "data": data,
    }
