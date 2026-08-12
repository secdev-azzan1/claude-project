import httpx
import logging
from typing import Optional, Dict, Any
from services.http_tls import tls_verify_enabled

logger = logging.getLogger(__name__)

# Error codes
ICEBERG_UNREACHABLE = "ICEBERG_UNREACHABLE"
ICEBERG_AUTH_FAILED = "ICEBERG_AUTH_FAILED"
ICEBERG_WAREHOUSE_NOT_FOUND = "ICEBERG_WAREHOUSE_NOT_FOUND"
ICEBERG_TIMEOUT = "ICEBERG_TIMEOUT"


def derive_oauth2_server_uri(endpoint: str) -> str:
    """Derive the OAuth2 token endpoint from an Iceberg REST catalog endpoint."""
    return f"{(endpoint or '').rstrip('/')}/v1/oauth/tokens"


async def fetch_catalog_token(conn: Dict[str, Any]) -> Dict[str, Any]:
    """Fetch an OAuth2 client-credentials token for the Iceberg REST catalog.

    Only attempts a token fetch when `iceberg_credential` is configured on the
    connection. Without a credential, the catalog is treated as anonymous.
    """
    credential = conn.get("iceberg_credential")
    if not credential:
        return {"ok": True, "token": None}

    if ":" not in credential:
        return {
            "ok": False,
            "token": None,
            "error": "iceberg_credential must be in the form client_id:client_secret.",
            "status_code": None,
        }

    client_id, client_secret = credential.split(":", 1)
    oauth2_server_uri = conn.get("iceberg_oauth2_server_uri") or derive_oauth2_server_uri(conn.get("endpoint", ""))
    scope = conn.get("iceberg_scope") or "PRINCIPAL_ROLE:ALL"

    try:
        async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=10.0) as client:
            response = await client.post(
                oauth2_server_uri,
                data={
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "scope": scope,
                },
            )
            if response.status_code == 200:
                try:
                    body = response.json()
                except Exception:
                    body = {}
                token = body.get("access_token")
                if not token:
                    return {
                        "ok": False,
                        "token": None,
                        "error": "Oauth2 token response did not contain an access_token.",
                        "status_code": response.status_code,
                    }
                return {"ok": True, "token": token, "error": None, "status_code": response.status_code}

            return {
                "ok": False,
                "token": None,
                "error": f"Oauth2 token request failed with status {response.status_code}.",
                "status_code": response.status_code,
            }
    except httpx.TimeoutException:
        return {"ok": False, "token": None, "error": "Oauth2 token request timed out.", "status_code": None}
    except httpx.ConnectError as e:
        logger.warning(f"Iceberg oauth2 connect error: {e}")
        return {"ok": False, "token": None, "error": f"Cannot connect to oauth2 server at {oauth2_server_uri}.", "status_code": None}
    except Exception as e:
        logger.error(f"Iceberg oauth2 token error: {e}")
        return {"ok": False, "token": None, "error": str(e)[:300], "status_code": None}


async def test_iceberg_connection(conn: Dict[str, Any]) -> Dict[str, Any]:
    """Test an Iceberg REST catalog connection.

    Fetches an oauth2 token (if credentials are configured) then probes the
    catalog's /v1/config endpoint for the configured warehouse.
    """
    endpoint = (conn.get("endpoint") or "").rstrip("/")
    warehouse = conn.get("iceberg_warehouse") or "bronze"

    token_result = await fetch_catalog_token(conn)
    if not token_result.get("ok"):
        return {
            "ok": False,
            "reachable": False,
            "status_code": token_result.get("status_code"),
            "error": token_result.get("error") or "Failed to obtain Iceberg catalog token.",
            "error_code": ICEBERG_AUTH_FAILED,
            "data": None,
        }

    token = token_result.get("token")
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=10.0) as client:
            response = await client.get(
                f"{endpoint}/v1/config",
                params={"warehouse": warehouse},
                headers=headers,
            )

            if response.status_code == 200:
                try:
                    data = response.json()
                except Exception:
                    data = None
                return {
                    "ok": True,
                    "reachable": True,
                    "status_code": response.status_code,
                    "message": "Iceberg catalog reachable.",
                    "error_code": None,
                    "data": data,
                }

            if response.status_code in (401, 403):
                return {
                    "ok": False,
                    "reachable": True,
                    "status_code": response.status_code,
                    "error": "Authentication required or insufficient permissions for the Iceberg catalog.",
                    "error_code": ICEBERG_AUTH_FAILED,
                    "data": None,
                }

            if response.status_code == 404:
                return {
                    "ok": False,
                    "reachable": True,
                    "status_code": response.status_code,
                    "error": f"Warehouse '{warehouse}' not found on the Iceberg catalog.",
                    "error_code": ICEBERG_WAREHOUSE_NOT_FOUND,
                    "data": None,
                }

            return {
                "ok": False,
                "reachable": True,
                "status_code": response.status_code,
                "error": f"Iceberg catalog returned status {response.status_code}.",
                "error_code": ICEBERG_UNREACHABLE,
                "data": None,
            }
    except httpx.TimeoutException:
        return {
            "ok": False,
            "reachable": False,
            "status_code": None,
            "error": "Connection to Iceberg catalog timed out.",
            "error_code": ICEBERG_TIMEOUT,
            "data": None,
        }
    except httpx.ConnectError as e:
        logger.warning(f"Iceberg catalog connect error: {e}")
        return {
            "ok": False,
            "reachable": False,
            "status_code": None,
            "error": f"Cannot connect to Iceberg catalog at {endpoint}.",
            "error_code": ICEBERG_UNREACHABLE,
            "data": None,
        }
    except Exception as e:
        logger.error(f"Iceberg catalog test error: {e}")
        return {
            "ok": False,
            "reachable": False,
            "status_code": None,
            "error": str(e)[:300],
            "error_code": ICEBERG_UNREACHABLE,
            "data": None,
        }
