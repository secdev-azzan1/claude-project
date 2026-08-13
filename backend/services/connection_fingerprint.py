"""Connection fingerprinting helpers.

Identify systems by stable per-instance identifiers:
- NiFi: root process group ID
- Kafka: cluster ID
- Apicurio: unsupported (no stable ID)
- Kafka Connect: kafka_cluster_id
- Iceberg: unsupported (no stable ID)
"""
import asyncio
import httpx
from typing import Optional, TypedDict
from services.nifi_client import get_nifi_root_process_group_result
from services.http_tls import tls_verify_enabled

# Import at module level to allow monkeypatching
from services.kafka_connect_client import get_cluster_info as kafka_connect_get_cluster_info
from services.iceberg_catalog_client import test_iceberg_connection

# Import kafka components at module level to allow monkeypatching
try:
    from kafka.admin import KafkaAdminClient
except ImportError:
    KafkaAdminClient = None

# kafka-python 3.x removed NoBrokersAvailable; import error names individually
# so one missing name cannot disable the whole client (it did, silently).
try:
    from kafka.errors import NoBrokersAvailable
except ImportError:
    try:
        from kafka.errors import KafkaConnectionError as NoBrokersAvailable
    except ImportError:
        NoBrokersAvailable = ConnectionError
try:
    from kafka.errors import KafkaConnectionError
except ImportError:
    KafkaConnectionError = ConnectionError
try:
    from kafka.errors import KafkaTimeoutError
except ImportError:
    KafkaTimeoutError = TimeoutError


class FingerprintResult(TypedDict):
    ok: bool
    fingerprint: Optional[str]
    reachable: bool
    error: Optional[str]


async def probe_nifi_fingerprint(conn: dict) -> FingerprintResult:
    """Get NiFi root process group ID as a stable fingerprint.

    States:
    - Unreachable: connection refused / timeout / DNS
    - Reachable + identified: got the root PG ID
    - Reachable but unknown: server responded but no stable id
    """
    try:
        result = await get_nifi_root_process_group_result(
            url=conn.get("endpoint", ""),
            auth_type=conn.get("auth_type", "NONE"),
            username=conn.get("username"),
            password=conn.get("password"),
            token=conn.get("token"),
        )
        pg_id = result.get("root_process_group_id")
        if pg_id:
            return {
                "ok": True,
                "fingerprint": pg_id,
                "reachable": True,
                "error": None,
            }

        if not result.get("ok"):
            return {
                "ok": False,
                "fingerprint": None,
                "reachable": bool(result.get("reachable")),
                "error": result.get("error") or "NiFi request failed",
            }

        return {
            "ok": False,
            "fingerprint": None,
            "reachable": True,
            "error": "NiFi responded but returned no root process group ID (auth failure or unexpected response)",
        }
    except (ConnectionRefusedError, TimeoutError, OSError) as e:
        return {
            "ok": False,
            "fingerprint": None,
            "reachable": False,
            "error": f"NiFi unreachable: {str(e)[:100]}",
        }
    except Exception as e:
        return {
            "ok": False,
            "fingerprint": None,
            "reachable": False,
            "error": f"NiFi connection error: {str(e)[:100]}",
        }


async def probe_kafka_fingerprint(conn: dict) -> FingerprintResult:
    """Get Kafka cluster ID as a stable fingerprint.

    States:
    - Unreachable: connect/timeout exception
    - Reachable + identified: got the cluster ID
    - Reachable but unknown: Kafbat mode (not direct TCP)
    """
    endpoint = conn.get("endpoint", "")
    kafbat_url = conn.get("kafbat_url")
    kafka_connection_mode = conn.get("kafka_connection_mode", "native")

    def _get_kafka_cluster_id_sync() -> dict:
        """Return structured outcome: {connected, cluster_id, error}.

        connected=True: broker responded (may or may not have cluster_id)
        connected=False: broker never responded
        """
        if KafkaAdminClient is None:
            return {
                "connected": False,
                "cluster_id": None,
                "error": "KafkaAdminClient not available",
            }

        try:
            admin = KafkaAdminClient(
                bootstrap_servers=endpoint,
                request_timeout_ms=8000,
            )
            try:
                cluster_info = admin.describe_cluster()
                cluster_id = cluster_info.get("cluster_id")
                error = None if cluster_id else "Kafka responded but returned no cluster ID"
                return {
                    "connected": True,
                    "cluster_id": cluster_id,
                    "error": error,
                }
            finally:
                admin.close()
        except (NoBrokersAvailable, KafkaConnectionError, KafkaTimeoutError) as e:
            # Unreachable errors
            return {
                "connected": False,
                "cluster_id": None,
                "error": f"Kafka unreachable: {str(e)[:100]}",
            }
        except (PermissionError, TimeoutError) as e:
            # Auth/permission errors or timeouts: broker responded but denied
            return {
                "connected": True,
                "cluster_id": None,
                "error": f"Kafka responded but error: {str(e)[:100]}",
            }
        except (ConnectionRefusedError, OSError) as e:
            # Connection-level errors: unreachable
            return {
                "connected": False,
                "cluster_id": None,
                "error": f"Kafka unreachable: {str(e)[:100]}",
            }
        except Exception as e:
            # Other errors after connection: reachable but unknown
            return {
                "connected": True,
                "cluster_id": None,
                "error": f"Kafka responded but error: {str(e)[:100]}",
            }

    # Try bootstrap first (even in kafbat mode)
    if endpoint:
        result = await asyncio.to_thread(_get_kafka_cluster_id_sync)
        if result["cluster_id"]:
            # Got a cluster ID (state 2)
            return {
                "ok": True,
                "fingerprint": result["cluster_id"],
                "reachable": True,
                "error": None,
            }
        elif result["connected"]:
            # Broker responded but no cluster_id (state 3)
            return {
                "ok": False,
                "fingerprint": None,
                "reachable": True,
                "error": result["error"],
            }
        # else: connected=False (bootstrap unreachable)
        # In native mode, return that error immediately
        # In kafbat mode, fall back to kafbat
        if kafka_connection_mode != "kafbat":
            return {
                "ok": False,
                "fingerprint": None,
                "reachable": False,
                "error": result["error"],
            }

    # Kafbat is only a reachability fallback
    if kafka_connection_mode == "kafbat" and kafbat_url:
        # Resolve URL: if no scheme, prepend http://
        url = kafbat_url
        if not url.startswith(("http://", "https://")):
            url = "http://" + url

        try:
            async with httpx.AsyncClient(verify=False, timeout=5.0) as client:
                await client.get(url)
                # Kafbat responded: reachable but unknown (state 3)
                return {
                    "ok": False,
                    "fingerprint": None,
                    "reachable": True,
                    "error": "Kafbat mode: no stable cluster ID available via direct connection",
                }
        except Exception as e:
            # Kafbat did not respond: unreachable (state 1)
            return {
                "ok": False,
                "fingerprint": None,
                "reachable": False,
                "error": f"Kafbat endpoint unreachable: {str(e)[:100]}",
            }

    # No bootstrap and no kafbat
    return {
        "ok": False,
        "fingerprint": None,
        "reachable": False,
        "error": "Kafka endpoint not configured",
    }


async def probe_apicurio_fingerprint(conn: dict) -> FingerprintResult:
    """Check Apicurio reachability. No stable per-instance ID exists.

    States:
    - Unreachable: connection refused / timeout
    - Reachable: responds to HTTP request
    - Always returns unsupported error (no stable ID)
    """
    endpoint = conn.get("endpoint", "")
    if not endpoint:
        return {
            "ok": False,
            "fingerprint": None,
            "reachable": False,
            "error": "Apicurio endpoint not configured",
        }

    try:
        async with httpx.AsyncClient(verify=False, timeout=8.0) as client:
            # Try the system/info endpoint, or fall back to base endpoint
            info_url = endpoint.rstrip("/") + "/apis/registry/v2/system/info"
            try:
                await client.get(info_url)
            except Exception:
                # Fall back to base endpoint
                await client.get(endpoint)

        return {
            "ok": False,
            "fingerprint": None,
            "reachable": True,
            "error": "unsupported: apicurio has no stable instance id",
        }
    except (ConnectionRefusedError, TimeoutError, OSError, httpx.ConnectError) as e:
        return {
            "ok": False,
            "fingerprint": None,
            "reachable": False,
            "error": f"Apicurio unreachable: {str(e)[:100]}",
        }
    except Exception as e:
        return {
            "ok": False,
            "fingerprint": None,
            "reachable": False,
            "error": f"Apicurio connection error: {str(e)[:100]}",
        }


async def probe_kafka_connect_fingerprint(conn: dict) -> FingerprintResult:
    """Get Kafka Connect's underlying Kafka cluster ID as a stable fingerprint.

    States:
    - Unreachable: connection refused / timeout / DNS
    - Reachable + identified: got kafka_cluster_id from the Connect root
    - Reachable but unknown: server responded but no kafka_cluster_id
    """
    try:
        result = await kafka_connect_get_cluster_info(conn)
        if not result.get("ok"):
            return {
                "ok": False,
                "fingerprint": None,
                "reachable": bool(result.get("reachable")),
                "error": result.get("error") or "Kafka Connect request failed",
            }

        data = result.get("data") or {}
        cluster_id = data.get("kafka_cluster_id") if isinstance(data, dict) else None
        if cluster_id:
            return {
                "ok": True,
                "fingerprint": cluster_id,
                "reachable": True,
                "error": None,
            }

        return {
            "ok": False,
            "fingerprint": None,
            "reachable": True,
            "error": "Kafka Connect responded but returned no kafka_cluster_id",
        }
    except Exception as e:
        return {
            "ok": False,
            "fingerprint": None,
            "reachable": False,
            "error": f"Kafka Connect connection error: {str(e)[:100]}",
        }


async def probe_iceberg_fingerprint(conn: dict) -> FingerprintResult:
    """Check Iceberg REST catalog reachability. No stable per-instance ID exists.

    States:
    - Unreachable: connection refused / timeout
    - Reachable: responds to HTTP request
    - Always returns unsupported error (no stable ID)
    """
    try:
        result = await test_iceberg_connection(conn)
        reachable = bool(result.get("reachable"))
        return {
            "ok": False,
            "fingerprint": None,
            "reachable": reachable,
            "error": "unsupported: iceberg has no stable instance id"
            if reachable
            else (result.get("error") or "Iceberg catalog unreachable"),
        }
    except Exception as e:
        return {
            "ok": False,
            "fingerprint": None,
            "reachable": False,
            "error": f"Iceberg connection error: {str(e)[:100]}",
        }
