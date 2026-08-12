"""Kafka connection test service.

Tests Kafka connectivity via:
1. Direct kafka-python KafkaAdminClient (TCP)  — works on same-network deployments
2. Kafbat Management API fallback              — works when a Kafbat UI instance is available
   (used transparently, not exposed in the UI)
"""
import asyncio
import logging
from typing import Optional, Dict, Any
from datetime import datetime, timezone
import json
from urllib.parse import quote

import httpx
from services.http_tls import tls_verify_enabled

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Error classification helpers
# ---------------------------------------------------------------------------

def _classify_kafka_error(
    raw_error: str,
    bootstrap_servers: str,
    security_protocol: str = "PLAINTEXT",
    sasl_mechanism: Optional[str] = None,
) -> Dict[str, str]:
    """Convert low-level kafka-python errors into actionable user messages."""
    e = (raw_error or "").lower()

    # DNS / host resolution
    if any(k in e for k in ("name or service not known", "nodename nor servname", "getaddrinfo", "temporary failure in name resolution")):
        return {
            "error_code": "DNS_RESOLUTION_FAILED",
            "error": (
                f"Could not resolve Kafka host in bootstrap servers '{bootstrap_servers}'. "
                "Verify hostname spelling and DNS visibility from the backend host."
            ),
        }

    # SSL/TLS mismatch
    if any(k in e for k in ("ssl", "tls", "certificate verify failed", "wrong version number", "sslv3", "handshake failure")):
        return {
            "error_code": "SSL_MISMATCH_OR_CERT_ERROR",
            "error": (
                "Kafka TLS/SSL handshake failed. Check security protocol/certificates. "
                f"Current protocol is '{security_protocol}'."
            ),
        }

    # SASL / auth mismatch
    if any(k in e for k in ("sasl", "authentication failed", "sasl_authentication_failed", "unsupported sasl mechanism", "invalid sasl")):
        mechanism = sasl_mechanism or "unset"
        return {
            "error_code": "SASL_AUTH_FAILED",
            "error": (
                "Kafka authentication failed. Verify SASL username/password/mechanism and security protocol. "
                f"Current mechanism is '{mechanism}', protocol is '{security_protocol}'."
            ),
        }

    # Advertised listeners / broker metadata issue
    if any(k in e for k in ("node -1", "bootstrap broker", "broker may not be available", "unable to bootstrap", "metadata", "advertised.listeners")):
        return {
            "error_code": "BROKER_METADATA_OR_ADVERTISED_LISTENER",
            "error": (
                f"Kafka bootstrap endpoint '{bootstrap_servers}' is reachable but broker metadata points to "
                "unreachable/internal addresses. Check Kafka 'advertised.listeners' for external/backend-reachable hosts."
            ),
        }

    # Generic network path failures
    if any(k in e for k in ("nobrokersavailable", "connection refused", "timed out", "network is unreachable", "cannot assign requested address", "connect call failed")):
        return {
            "error_code": "NETWORK_UNREACHABLE",
            "error": (
                f"Cannot reach Kafka broker at {bootstrap_servers} from the application backend network. "
                "If Kafka is only reachable from NiFi's network, deployment may still work while this backend-side test fails."
            ),
        }

    # Fallback
    return {
        "error_code": "KAFKA_CONNECTION_FAILED",
        "error": (
            f"Kafka connection test failed for {bootstrap_servers}. "
            "Check broker endpoint, listener protocol, auth settings, and broker logs."
        ),
    }


# ---------------------------------------------------------------------------
# Direct TCP test via kafka-python
# ---------------------------------------------------------------------------

def _test_kafka_sync(
    bootstrap_servers: str,
    security_protocol: str = "PLAINTEXT",
    sasl_mechanism: Optional[str] = None,
    sasl_username: Optional[str] = None,
    sasl_password: Optional[str] = None,
) -> Dict[str, Any]:
    try:
        from kafka import KafkaAdminClient

        kwargs: Dict[str, Any] = {
            "bootstrap_servers": bootstrap_servers,
            "request_timeout_ms": 10000,
            "connections_max_idle_ms": 15000,
            "security_protocol": security_protocol,
        }
        if security_protocol in ("SASL_SSL", "SASL_PLAINTEXT") and sasl_mechanism:
            kwargs["sasl_mechanism"] = sasl_mechanism
            kwargs["sasl_plain_username"] = sasl_username or ""
            kwargs["sasl_plain_password"] = sasl_password or ""

        admin = KafkaAdminClient(**kwargs)
        topics = admin.list_topics()
        admin.close()
        return {
            "ok": True,
            "message": f"Kafka connected. {len(topics)} topic(s) available.",
            "topic_count": len(topics),
        }
    except ImportError:
        return {"ok": False, "error": "kafka-python library not installed.", "error_code": "MISSING_KAFKA_CLIENT"}
    except Exception as e:
        raw = str(e)[:400]
        classified = _classify_kafka_error(raw, bootstrap_servers, security_protocol, sasl_mechanism)
        return {
            "ok": False,
            "error": classified["error"],
            "error_code": classified["error_code"],
            "raw_error": raw,
        }


# ---------------------------------------------------------------------------
# Kafbat REST API fallback
# ---------------------------------------------------------------------------

async def _test_via_kafbat(
    kafbat_url: str,
    kafbat_username: Optional[str] = None,
    kafbat_password: Optional[str] = None,
) -> Dict[str, Any]:
    """Use Kafbat's cluster status API to verify Kafka is reachable."""
    base = kafbat_url.rstrip("/")
    try:
        async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=10.0) as client:
            # Kafbat uses form-based login
            login_resp = await client.post(
                f"{base}/login",
                data={"username": kafbat_username or "", "password": kafbat_password or ""},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                follow_redirects=True,
            )

            clusters_resp = await client.get(f"{base}/api/clusters")
            if clusters_resp.status_code == 200:
                try:
                    clusters = clusters_resp.json()
                    if clusters:
                        c = clusters[0]
                        status = c.get("status", "UNKNOWN")
                        brokers = c.get("brokerCount", "?")
                        topics = c.get("topicCount", "?")
                        if status == "ONLINE":
                            return {
                                "ok": True,
                                "message": f"Kafka cluster online. {brokers} broker(s), {topics} topic(s).",
                                "topic_count": topics,
                            }
                        else:
                            return {"ok": False, "error": f"Kafka cluster status: {status}"}
                except Exception:
                    pass
        return {"ok": False, "error": "Kafbat API did not return cluster info."}
    except Exception as e:
        return {"ok": False, "error": f"Kafbat fallback failed: {str(e)[:200]}", "error_code": "KAFBAT_FALLBACK_FAILED"}


def _should_try_kafbat_fallback(result: Dict[str, Any]) -> bool:
    """Only fallback when the backend could not talk to Kafka, not for valid Kafka responses."""
    if result.get("ok"):
        return False
    return result.get("error_code") in {
        "DNS_RESOLUTION_FAILED",
        "BROKER_METADATA_OR_ADVERTISED_LISTENER",
        "NETWORK_UNREACHABLE",
        "KAFKA_CONNECTION_FAILED",
        "TIMEOUT",
    }


async def _kafbat_login(
    client: httpx.AsyncClient,
    base: str,
    username: Optional[str],
    password: Optional[str],
) -> None:
    if not username:
        return
    await client.post(
        f"{base}/login",
        data={"username": username or "", "password": password or ""},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        follow_redirects=True,
    )


async def _kafbat_get_cluster_name(client: httpx.AsyncClient, base: str) -> Optional[str]:
    resp = await client.get(f"{base}/api/clusters")
    if resp.status_code != 200:
        return None
    clusters = resp.json()
    if not clusters:
        return None
    return clusters[0].get("name") or clusters[0].get("id")


def _parse_kafbat_sse_events(body: str) -> list[Dict[str, Any]]:
    events: list[Dict[str, Any]] = []
    data_lines: list[str] = []

    def flush() -> None:
        nonlocal data_lines
        if not data_lines:
            return
        payload = "\n".join(data_lines).strip()
        data_lines = []
        if not payload:
            return
        try:
            parsed = json.loads(payload)
            if isinstance(parsed, dict):
                events.append(parsed)
        except Exception as exc:
            logger.debug("Failed to parse Kafbat SSE event: %s", exc)

    for raw_line in body.splitlines():
        line = raw_line.rstrip("\r")
        if line == "":
            flush()
            continue
        if line.startswith(":"):
            continue
        if line.startswith("data:"):
            value = line[5:]
            if value.startswith(" "):
                value = value[1:]
            data_lines.append(value)

    flush()
    return events


def _timestamp_ms_from_iso(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return int(parsed.timestamp() * 1000)
    except Exception:
        return None


async def _kafbat_topic_message_count(
    kafbat_url: str,
    kafbat_username: Optional[str],
    kafbat_password: Optional[str],
    topic: str,
) -> Dict[str, Any]:
    base = (kafbat_url or "").rstrip("/")
    if not base:
        return {"ok": False, "error": "Kafbat URL not configured.", "error_code": "KAFBAT_NOT_CONFIGURED"}

    try:
        async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=15.0, follow_redirects=True) as client:
            await _kafbat_login(client, base, kafbat_username, kafbat_password)
            cluster = await _kafbat_get_cluster_name(client, base)
            if not cluster:
                return {"ok": False, "error": "Could not determine Kafbat cluster name.", "error_code": "KAFBAT_CLUSTER_NOT_FOUND"}

            resp = await client.get(f"{base}/api/clusters/{quote(cluster, safe='')}/topics/{quote(topic, safe='')}")
            if resp.status_code == 404:
                return {"ok": False, "error": f"Kafka topic '{topic}' was not found.", "error_code": "TOPIC_NOT_FOUND"}
            if resp.status_code != 200:
                return {"ok": False, "error": f"Kafbat topic details returned {resp.status_code}: {resp.text[:200]}", "error_code": "KAFBAT_TOPIC_DETAILS_FAILED"}

            data = resp.json()
            count = data.get("messagesCount")
            if count is None:
                count = sum(
                    max(int(partition.get("offsetMax") or 0) - int(partition.get("offsetMin") or 0), 0)
                    for partition in data.get("partitions", [])
                    if isinstance(partition, dict)
                )
            return {
                "ok": True,
                "topic": topic,
                "total_messages": int(count) if count is not None else 0,
                "source": "kafbat",
            }
    except Exception as e:
        return {"ok": False, "error": f"Kafbat topic count failed: {str(e)[:200]}", "error_code": "KAFBAT_TOPIC_COUNT_FAILED"}


async def _kafbat_recent_topic_messages(
    kafbat_url: str,
    kafbat_username: Optional[str],
    kafbat_password: Optional[str],
    topic: str,
    limit: int,
) -> Dict[str, Any]:
    base = (kafbat_url or "").rstrip("/")
    if not base:
        return {"ok": False, "error": "Kafbat URL not configured.", "error_code": "KAFBAT_NOT_CONFIGURED"}

    try:
        async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=30.0, follow_redirects=True) as client:
            await _kafbat_login(client, base, kafbat_username, kafbat_password)
            cluster = await _kafbat_get_cluster_name(client, base)
            if not cluster:
                return {"ok": False, "error": "Could not determine Kafbat cluster name.", "error_code": "KAFBAT_CLUSTER_NOT_FOUND"}

            resp = await client.get(
                f"{base}/api/clusters/{quote(cluster, safe='')}/topics/{quote(topic, safe='')}/messages/v2",
                params={"mode": "LATEST", "limit": limit},
            )
            if resp.status_code == 404:
                return {"ok": False, "error": f"Kafka topic '{topic}' was not found.", "error_code": "TOPIC_NOT_FOUND"}
            if resp.status_code != 200:
                return {"ok": False, "error": f"Kafbat messages/v2 returned {resp.status_code}: {resp.text[:200]}", "error_code": "KAFBAT_MESSAGES_FAILED"}

            rows = []
            for event in _parse_kafbat_sse_events(resp.text):
                if str(event.get("type") or "").upper() != "MESSAGE":
                    continue
                msg = event.get("message") or {}
                if not isinstance(msg, dict):
                    continue
                timestamp = msg.get("timestamp")
                rows.append({
                    "partition": int(msg.get("partition") or 0),
                    "offset": int(msg.get("offset") or 0),
                    "timestamp_ms": _timestamp_ms_from_iso(timestamp),
                    "timestamp": timestamp,
                    "key": msg.get("key"),
                    "value": msg.get("value"),
                })

            rows.sort(key=lambda r: ((r.get("timestamp_ms") or -1), r.get("offset", -1)), reverse=True)
            return {"ok": True, "topic": topic, "messages": rows[:limit], "source": "kafbat"}
    except Exception as e:
        return {"ok": False, "error": f"Kafbat message lookup failed: {str(e)[:200]}", "error_code": "KAFBAT_MESSAGES_FAILED"}


async def _kafbat_clear_topic_messages(
    kafbat_url: str,
    kafbat_username: Optional[str],
    kafbat_password: Optional[str],
    topic: str,
) -> Dict[str, Any]:
    base = (kafbat_url or "").rstrip("/")
    if not base:
        return {"ok": False, "error": "Kafbat URL not configured.", "error_code": "KAFBAT_NOT_CONFIGURED"}

    try:
        before = await _kafbat_topic_message_count(kafbat_url, kafbat_username, kafbat_password, topic)
        before_total = int(before.get("total_messages") or 0) if before.get("ok") else None

        async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=30.0, follow_redirects=True) as client:
            await _kafbat_login(client, base, kafbat_username, kafbat_password)
            cluster = await _kafbat_get_cluster_name(client, base)
            if not cluster:
                return {"ok": False, "error": "Could not determine Kafbat cluster name.", "error_code": "KAFBAT_CLUSTER_NOT_FOUND"}

            resp = await client.delete(
                f"{base}/api/clusters/{quote(cluster, safe='')}/topics/{quote(topic, safe='')}/messages"
            )
            if resp.status_code == 404:
                return {"ok": False, "error": f"Kafka topic '{topic}' was not found.", "error_code": "TOPIC_NOT_FOUND"}
            if resp.status_code not in (200, 204):
                return {"ok": False, "error": f"Kafbat clear returned {resp.status_code}: {resp.text[:200]}", "error_code": "KAFBAT_CLEAR_FAILED"}

        after = await _kafbat_topic_message_count(kafbat_url, kafbat_username, kafbat_password, topic)
        after_total = int(after.get("total_messages") or 0) if after.get("ok") else None
        cleared = None
        if before_total is not None and after_total is not None:
            cleared = max(before_total - after_total, 0)
        return {
            "ok": True,
            "topic": topic,
            "cleared_messages": cleared,
            "before_total_messages": before_total,
            "after_total_messages": after_total,
            "source": "kafbat",
        }
    except Exception as e:
        return {"ok": False, "error": f"Kafbat topic clear failed: {str(e)[:200]}", "error_code": "KAFBAT_CLEAR_FAILED"}


# ---------------------------------------------------------------------------
# Public test function
# ---------------------------------------------------------------------------

async def test_kafka_connection(
    bootstrap_servers: str,
    security_protocol: str = "PLAINTEXT",
    sasl_mechanism: Optional[str] = None,
    sasl_username: Optional[str] = None,
    sasl_password: Optional[str] = None,
    # Optional Kafbat fallback (read from DB by the router)
    kafbat_url: Optional[str] = None,
    kafbat_username: Optional[str] = None,
    kafbat_password: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Test Kafka connectivity.
    Tries direct TCP first; falls back to Kafbat REST API if available.
    """
    loop = asyncio.get_event_loop()

    # 1. Try direct TCP connection
    try:
        tcp_result = await asyncio.wait_for(
            loop.run_in_executor(
                None,
                _test_kafka_sync,
                bootstrap_servers,
                security_protocol,
                sasl_mechanism,
                sasl_username,
                sasl_password,
            ),
            timeout=20.0,
        )
        if tcp_result.get("ok"):
            return tcp_result
    except asyncio.TimeoutError:
        tcp_result = {"ok": False, "error": "TCP connection timed out."}
    except Exception as e:
        tcp_result = {"ok": False, "error": str(e)[:300]}

    # 2. Fallback: try Kafbat management API
    if kafbat_url:
        logger.info("Kafka TCP test failed, trying Kafbat REST API fallback...")
        kafbat_result = await _test_via_kafbat(kafbat_url, kafbat_username, kafbat_password)
        if kafbat_result.get("ok"):
            return kafbat_result
        # Both failed — prefer classified TCP error, keep Kafbat context for debugging.
        return {
            "ok": False,
            "error": tcp_result.get("error", "Kafka connectivity check failed."),
            "error_code": tcp_result.get("error_code", "KAFKA_CONNECTION_FAILED"),
            "raw_error": tcp_result.get("raw_error", ""),
            "kafbat_error": kafbat_result.get("error", ""),
        }

    # No Kafbat — return the TCP error
    return tcp_result


async def test_kafbat_connection(
    kafbat_url: str,
    kafbat_username: Optional[str] = None,
    kafbat_password: Optional[str] = None,
) -> Dict[str, Any]:
    """Test Kafka visibility through an explicitly configured Kafbat HTTP connection."""
    return await _test_via_kafbat(kafbat_url, kafbat_username, kafbat_password)


def _get_topic_message_count_sync(
    bootstrap_servers: str,
    topic: str,
    security_protocol: str = "PLAINTEXT",
    sasl_mechanism: Optional[str] = None,
    sasl_username: Optional[str] = None,
    sasl_password: Optional[str] = None,
) -> Dict[str, Any]:
    consumer = None
    try:
        from kafka import KafkaConsumer, TopicPartition

        kwargs: Dict[str, Any] = {
            "bootstrap_servers": bootstrap_servers,
            "enable_auto_commit": False,
            "group_id": None,
            "request_timeout_ms": 10000,
            "connections_max_idle_ms": 15000,
            "security_protocol": security_protocol,
        }
        if security_protocol in ("SASL_SSL", "SASL_PLAINTEXT") and sasl_mechanism:
            kwargs["sasl_mechanism"] = sasl_mechanism
            kwargs["sasl_plain_username"] = sasl_username or ""
            kwargs["sasl_plain_password"] = sasl_password or ""

        consumer = KafkaConsumer(**kwargs)
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return {
                "ok": False,
                "error_code": "TOPIC_NOT_FOUND",
                "error": f"Kafka topic '{topic}' was not found.",
            }

        topic_partitions = [TopicPartition(topic, p) for p in partitions]
        begin_offsets = consumer.beginning_offsets(topic_partitions)
        end_offsets = consumer.end_offsets(topic_partitions)
        # Retained message count must account for log start offsets (after deletes/retention).
        # Using only end offsets can overcount after topic truncation.
        total_messages = int(
            sum(
                max(int(end_offsets.get(tp, 0)) - int(begin_offsets.get(tp, 0)), 0)
                for tp in topic_partitions
            )
        )

        return {
            "ok": True,
            "topic": topic,
            "total_messages": total_messages,
        }
    except ImportError:
        return {"ok": False, "error": "kafka-python library not installed.", "error_code": "MISSING_KAFKA_CLIENT"}
    except Exception as e:
        raw = str(e)[:400]
        classified = _classify_kafka_error(raw, bootstrap_servers, security_protocol, sasl_mechanism)
        return {
            "ok": False,
            "error": classified["error"],
            "error_code": classified["error_code"],
            "raw_error": raw,
        }
    finally:
        if consumer is not None:
            try:
                consumer.close()
            except Exception:
                pass


async def get_topic_message_count(
    bootstrap_servers: str,
    topic: str,
    security_protocol: str = "PLAINTEXT",
    sasl_mechanism: Optional[str] = None,
    sasl_username: Optional[str] = None,
    sasl_password: Optional[str] = None,
    kafbat_url: Optional[str] = None,
    kafbat_username: Optional[str] = None,
    kafbat_password: Optional[str] = None,
) -> Dict[str, Any]:
    """Return total retained messages in topic (sum of end offsets across partitions)."""
    if not topic:
        return {"ok": False, "error": "Flow has no Kafka topic configured.", "error_code": "MISSING_TOPIC"}
    if not bootstrap_servers:
        if kafbat_url:
            return await _kafbat_topic_message_count(kafbat_url, kafbat_username, kafbat_password, topic)
        return {"ok": False, "error": "Kafka bootstrap endpoint is not configured.", "error_code": "MISSING_BOOTSTRAP"}

    loop = asyncio.get_event_loop()
    try:
        result = await asyncio.wait_for(
            loop.run_in_executor(
                None,
                _get_topic_message_count_sync,
                bootstrap_servers,
                topic,
                security_protocol,
                sasl_mechanism,
                sasl_username,
                sasl_password,
            ),
            timeout=20.0,
        )
        if result.get("ok"):
            result.setdefault("source", "kafka")
            return result
        if kafbat_url and _should_try_kafbat_fallback(result):
            fallback = await _kafbat_topic_message_count(kafbat_url, kafbat_username, kafbat_password, topic)
            if fallback.get("ok"):
                fallback["direct_error"] = result.get("error")
                return fallback
        return result
    except asyncio.TimeoutError:
        result = {"ok": False, "error": "Kafka topic count lookup timed out.", "error_code": "TIMEOUT"}
        if kafbat_url:
            fallback = await _kafbat_topic_message_count(kafbat_url, kafbat_username, kafbat_password, topic)
            if fallback.get("ok"):
                fallback["direct_error"] = result.get("error")
                return fallback
        return result
    except Exception as e:
        return {"ok": False, "error": str(e)[:300], "error_code": "TOPIC_COUNT_FAILED"}


def _decode_kafka_bytes(value: Optional[bytes]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _get_recent_topic_messages_sync(
    bootstrap_servers: str,
    topic: str,
    limit: int = 20,
    security_protocol: str = "PLAINTEXT",
    sasl_mechanism: Optional[str] = None,
    sasl_username: Optional[str] = None,
    sasl_password: Optional[str] = None,
) -> Dict[str, Any]:
    consumer = None
    try:
        from kafka import KafkaConsumer, TopicPartition

        kwargs: Dict[str, Any] = {
            "bootstrap_servers": bootstrap_servers,
            "enable_auto_commit": False,
            "group_id": None,
            "request_timeout_ms": 10000,
            "connections_max_idle_ms": 15000,
            "security_protocol": security_protocol,
            "consumer_timeout_ms": 2000,
        }
        if security_protocol in ("SASL_SSL", "SASL_PLAINTEXT") and sasl_mechanism:
            kwargs["sasl_mechanism"] = sasl_mechanism
            kwargs["sasl_plain_username"] = sasl_username or ""
            kwargs["sasl_plain_password"] = sasl_password or ""

        consumer = KafkaConsumer(**kwargs)
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return {
                "ok": False,
                "error_code": "TOPIC_NOT_FOUND",
                "error": f"Kafka topic '{topic}' was not found.",
            }

        topic_partitions = [TopicPartition(topic, p) for p in sorted(partitions)]
        end_offsets = consumer.end_offsets(topic_partitions)
        if not end_offsets:
            return {"ok": True, "topic": topic, "messages": []}

        # Fetch a small tail window from each partition, then trim globally.
        per_partition_window = max(10, min(200, limit * 2))
        consumer.assign(topic_partitions)
        for tp in topic_partitions:
            end = int(end_offsets.get(tp, 0))
            start = max(0, end - per_partition_window)
            consumer.seek(tp, start)

        rows = []
        max_collect = max(limit * 8, 100)
        idle_polls = 0
        while len(rows) < max_collect and idle_polls < 3:
            polled = consumer.poll(timeout_ms=700, max_records=500)
            if not polled:
                idle_polls += 1
                continue
            idle_polls = 0
            for tp, records in polled.items():
                for rec in records:
                    ts_iso = None
                    if rec.timestamp is not None and rec.timestamp >= 0:
                        ts_iso = datetime.fromtimestamp(rec.timestamp / 1000.0, tz=timezone.utc).isoformat()
                    rows.append({
                        "partition": int(tp.partition),
                        "offset": int(rec.offset),
                        "timestamp_ms": int(rec.timestamp) if rec.timestamp is not None else None,
                        "timestamp": ts_iso,
                        "key": _decode_kafka_bytes(rec.key),
                        "value": _decode_kafka_bytes(rec.value),
                    })

        rows.sort(key=lambda r: ((r.get("timestamp_ms") or -1), r.get("offset", -1)), reverse=True)
        return {"ok": True, "topic": topic, "messages": rows[:limit]}
    except ImportError:
        return {"ok": False, "error": "kafka-python library not installed.", "error_code": "MISSING_KAFKA_CLIENT"}
    except Exception as e:
        raw = str(e)[:400]
        classified = _classify_kafka_error(raw, bootstrap_servers, security_protocol, sasl_mechanism)
        return {
            "ok": False,
            "error": classified["error"],
            "error_code": classified["error_code"],
            "raw_error": raw,
        }
    finally:
        if consumer is not None:
            try:
                consumer.close()
            except Exception:
                pass


async def get_recent_topic_messages(
    bootstrap_servers: str,
    topic: str,
    limit: int = 20,
    security_protocol: str = "PLAINTEXT",
    sasl_mechanism: Optional[str] = None,
    sasl_username: Optional[str] = None,
    sasl_password: Optional[str] = None,
    kafbat_url: Optional[str] = None,
    kafbat_username: Optional[str] = None,
    kafbat_password: Optional[str] = None,
) -> Dict[str, Any]:
    """Return recent messages from a topic for UI inspection."""
    if not topic:
        return {"ok": False, "error": "Flow has no Kafka topic configured.", "error_code": "MISSING_TOPIC"}
    if not bootstrap_servers:
        if kafbat_url:
            safe_limit = max(1, min(limit, 100))
            return await _kafbat_recent_topic_messages(kafbat_url, kafbat_username, kafbat_password, topic, safe_limit)
        return {"ok": False, "error": "Kafka bootstrap endpoint is not configured.", "error_code": "MISSING_BOOTSTRAP"}

    safe_limit = max(1, min(limit, 100))
    loop = asyncio.get_event_loop()
    try:
        result = await asyncio.wait_for(
            loop.run_in_executor(
                None,
                _get_recent_topic_messages_sync,
                bootstrap_servers,
                topic,
                safe_limit,
                security_protocol,
                sasl_mechanism,
                sasl_username,
                sasl_password,
            ),
            timeout=25.0,
        )
        if result.get("ok"):
            result.setdefault("source", "kafka")
            return result
        if kafbat_url and _should_try_kafbat_fallback(result):
            fallback = await _kafbat_recent_topic_messages(kafbat_url, kafbat_username, kafbat_password, topic, safe_limit)
            if fallback.get("ok"):
                fallback["direct_error"] = result.get("error")
                return fallback
        return result
    except asyncio.TimeoutError:
        result = {"ok": False, "error": "Kafka messages lookup timed out.", "error_code": "TIMEOUT"}
        if kafbat_url:
            fallback = await _kafbat_recent_topic_messages(kafbat_url, kafbat_username, kafbat_password, topic, safe_limit)
            if fallback.get("ok"):
                fallback["direct_error"] = result.get("error")
                return fallback
        return result
    except Exception as e:
        return {"ok": False, "error": str(e)[:300], "error_code": "TOPIC_MESSAGES_FAILED"}


def _clear_topic_messages_sync(
    bootstrap_servers: str,
    topic: str,
    security_protocol: str = "PLAINTEXT",
    sasl_mechanism: Optional[str] = None,
    sasl_username: Optional[str] = None,
    sasl_password: Optional[str] = None,
) -> Dict[str, Any]:
    consumer = None
    admin = None
    try:
        from kafka import KafkaConsumer, TopicPartition, KafkaAdminClient

        common_kwargs: Dict[str, Any] = {
            "bootstrap_servers": bootstrap_servers,
            "request_timeout_ms": 10000,
            "connections_max_idle_ms": 15000,
            "security_protocol": security_protocol,
        }
        if security_protocol in ("SASL_SSL", "SASL_PLAINTEXT") and sasl_mechanism:
            common_kwargs["sasl_mechanism"] = sasl_mechanism
            common_kwargs["sasl_plain_username"] = sasl_username or ""
            common_kwargs["sasl_plain_password"] = sasl_password or ""

        consumer = KafkaConsumer(
            enable_auto_commit=False,
            group_id=None,
            **common_kwargs,
        )
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return {
                "ok": False,
                "error_code": "TOPIC_NOT_FOUND",
                "error": f"Kafka topic '{topic}' was not found.",
            }

        topic_partitions = [TopicPartition(topic, partition) for partition in sorted(partitions)]
        begin_offsets = consumer.beginning_offsets(topic_partitions)
        end_offsets = consumer.end_offsets(topic_partitions)

        total_before = int(sum(max(int(end_offsets.get(tp, 0)) - int(begin_offsets.get(tp, 0)), 0) for tp in topic_partitions))
        if total_before == 0:
            return {
                "ok": True,
                "topic": topic,
                "cleared_messages": 0,
                "message": "Topic already empty.",
            }

        # Delete all records before each partition's current end offset.
        records_to_delete = {
            tp: int(end_offsets.get(tp, 0))
            for tp in topic_partitions
            if int(end_offsets.get(tp, 0)) > 0
        }
        if not records_to_delete:
            return {
                "ok": True,
                "topic": topic,
                "cleared_messages": 0,
                "message": "Topic already empty.",
            }

        admin = KafkaAdminClient(**common_kwargs)
        result = admin.delete_records(records_to_delete=records_to_delete, timeout_ms=10000)

        details = []
        for tp, metadata in result.items():
            # kafka-python returns dict-like metadata; keep generic for compatibility.
            low_watermark = metadata.get("low_watermark") if isinstance(metadata, dict) else None
            details.append({
                "partition": int(tp.partition),
                "before_offset": int(begin_offsets.get(tp, 0)),
                "end_offset": int(end_offsets.get(tp, 0)),
                "low_watermark": int(low_watermark) if isinstance(low_watermark, int) else low_watermark,
            })

        return {
            "ok": True,
            "topic": topic,
            "cleared_messages": total_before,
            "partitions": details,
        }
    except ImportError:
        return {"ok": False, "error": "kafka-python library not installed.", "error_code": "MISSING_KAFKA_CLIENT"}
    except Exception as e:
        raw = str(e)[:400]
        classified = _classify_kafka_error(raw, bootstrap_servers, security_protocol, sasl_mechanism)
        return {
            "ok": False,
            "error": classified["error"],
            "error_code": classified["error_code"],
            "raw_error": raw,
        }
    finally:
        if consumer is not None:
            try:
                consumer.close()
            except Exception:
                pass
        if admin is not None:
            try:
                admin.close()
            except Exception:
                pass


async def clear_topic_messages(
    bootstrap_servers: str,
    topic: str,
    security_protocol: str = "PLAINTEXT",
    sasl_mechanism: Optional[str] = None,
    sasl_username: Optional[str] = None,
    sasl_password: Optional[str] = None,
    kafbat_url: Optional[str] = None,
    kafbat_username: Optional[str] = None,
    kafbat_password: Optional[str] = None,
) -> Dict[str, Any]:
    """Delete all retained records in a topic by advancing each partition start offset to current end offset."""
    if not topic:
        return {"ok": False, "error": "Flow has no Kafka topic configured.", "error_code": "MISSING_TOPIC"}
    if not bootstrap_servers:
        if kafbat_url:
            return await _kafbat_clear_topic_messages(kafbat_url, kafbat_username, kafbat_password, topic)
        return {"ok": False, "error": "Kafka bootstrap endpoint is not configured.", "error_code": "MISSING_BOOTSTRAP"}

    loop = asyncio.get_event_loop()
    try:
        result = await asyncio.wait_for(
            loop.run_in_executor(
                None,
                _clear_topic_messages_sync,
                bootstrap_servers,
                topic,
                security_protocol,
                sasl_mechanism,
                sasl_username,
                sasl_password,
            ),
            timeout=25.0,
        )
        if result.get("ok"):
            result.setdefault("source", "kafka")
            return result
        if kafbat_url and _should_try_kafbat_fallback(result):
            fallback = await _kafbat_clear_topic_messages(kafbat_url, kafbat_username, kafbat_password, topic)
            if fallback.get("ok"):
                fallback["direct_error"] = result.get("error")
                return fallback
        return result
    except asyncio.TimeoutError:
        result = {"ok": False, "error": "Kafka topic clear timed out.", "error_code": "TIMEOUT"}
        if kafbat_url:
            fallback = await _kafbat_clear_topic_messages(kafbat_url, kafbat_username, kafbat_password, topic)
            if fallback.get("ok"):
                fallback["direct_error"] = result.get("error")
                return fallback
        return result
    except Exception as e:
        return {"ok": False, "error": str(e)[:300], "error_code": "TOPIC_CLEAR_FAILED"}


def _produce_kafka_message_sync(
    bootstrap_servers: str,
    topic: str,
    value: Any,
    key: Optional[str] = None,
    security_protocol: str = "PLAINTEXT",
    sasl_mechanism: Optional[str] = None,
    sasl_username: Optional[str] = None,
    sasl_password: Optional[str] = None,
) -> Dict[str, Any]:
    producer = None
    try:
        from kafka import KafkaProducer

        kwargs: Dict[str, Any] = {
            "bootstrap_servers": bootstrap_servers,
            "request_timeout_ms": 10000,
            "connections_max_idle_ms": 15000,
            "retries": 1,
            "acks": "all",
            "security_protocol": security_protocol,
        }
        if security_protocol in ("SASL_SSL", "SASL_PLAINTEXT") and sasl_mechanism:
            kwargs["sasl_mechanism"] = sasl_mechanism
            kwargs["sasl_plain_username"] = sasl_username or ""
            kwargs["sasl_plain_password"] = sasl_password or ""

        producer = KafkaProducer(
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
            if not isinstance(v, (bytes, bytearray))
            else v,
            key_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
            **kwargs,
        )
        metadata = producer.send(topic, key=key, value=value).get(timeout=10)
        producer.flush(timeout=10)
        return {
            "ok": True,
            "topic": metadata.topic,
            "partition": int(metadata.partition),
            "offset": int(metadata.offset),
        }
    except ImportError:
        return {"ok": False, "error": "kafka-python library not installed.", "error_code": "MISSING_KAFKA_CLIENT"}
    except Exception as e:
        raw = str(e)[:400]
        classified = _classify_kafka_error(raw, bootstrap_servers, security_protocol, sasl_mechanism)
        return {
            "ok": False,
            "error": classified["error"],
            "error_code": classified["error_code"],
            "raw_error": raw,
        }
    finally:
        if producer is not None:
            try:
                producer.close()
            except Exception:
                pass


def _ensure_topic_exists_sync(
    bootstrap_servers: str,
    topic: str,
    partitions: int = 6,
    replication_factor: int = 3,
    security_protocol: str = "PLAINTEXT",
    sasl_mechanism: Optional[str] = None,
    sasl_username: Optional[str] = None,
    sasl_password: Optional[str] = None,
) -> Dict[str, Any]:
    admin = None
    try:
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError

        kwargs: Dict[str, Any] = {
            "bootstrap_servers": bootstrap_servers,
            "request_timeout_ms": 10000,
            "connections_max_idle_ms": 15000,
            "security_protocol": security_protocol,
        }
        if security_protocol in ("SASL_SSL", "SASL_PLAINTEXT") and sasl_mechanism:
            kwargs["sasl_mechanism"] = sasl_mechanism
            kwargs["sasl_plain_username"] = sasl_username or ""
            kwargs["sasl_plain_password"] = sasl_password or ""

        admin = KafkaAdminClient(**kwargs)

        try:
            existing_topics = admin.list_topics()
        except Exception:
            existing_topics = []
        if topic in existing_topics:
            return {"ok": True, "created": False, "message": "Topic already exists."}

        try:
            new_topic = NewTopic(name=topic, num_partitions=partitions, replication_factor=replication_factor)
            admin.create_topics(new_topics=[new_topic], validate_only=False)
            return {"ok": True, "created": True, "message": f"Topic '{topic}' created."}
        except TopicAlreadyExistsError:
            return {"ok": True, "created": False, "message": "Topic already exists."}
    except ImportError:
        return {"ok": False, "created": False, "error": "kafka-python library not installed.", "error_code": "MISSING_KAFKA_CLIENT"}
    except Exception as e:
        raw = str(e)[:400]
        classified = _classify_kafka_error(raw, bootstrap_servers, security_protocol, sasl_mechanism)
        return {
            "ok": False,
            "created": False,
            "error": classified["error"],
            "error_code": classified["error_code"],
            "raw_error": raw,
        }
    finally:
        if admin is not None:
            try:
                admin.close()
            except Exception:
                pass


async def ensure_topic_exists(
    conn: Dict[str, Any],
    topic: str,
    partitions: int = 6,
    replication_factor: int = 3,
) -> Dict[str, Any]:
    """Ensure a Kafka topic exists, creating it explicitly if needed.

    `conn` is a Kafka connection dict: `endpoint` is used as bootstrap servers,
    plus optional `security_protocol` / `sasl_mechanism` / `sasl_username` / `sasl_password`.
    Never raises; always returns {"ok", "created", "message"/"error"}.
    """
    bootstrap_servers = (conn or {}).get("endpoint") or ""
    if not bootstrap_servers:
        return {"ok": False, "created": False, "error": "Kafka bootstrap endpoint is not configured.", "error_code": "MISSING_BOOTSTRAP"}
    if not topic:
        return {"ok": False, "created": False, "error": "Kafka topic is not configured.", "error_code": "MISSING_TOPIC"}

    kafka_mode = (conn.get("kafka_connection_mode") or "native").strip().lower()
    kafbat_url = conn.get("kafbat_url") or (bootstrap_servers if kafka_mode == "kafbat" else None)
    if kafka_mode == "kafbat" and kafbat_url:
        fallback = await _kafbat_topic_message_count(
            kafbat_url,
            conn.get("kafbat_username"),
            conn.get("kafbat_password"),
            topic,
        )
        if fallback.get("ok"):
            return {
                "ok": True,
                "created": False,
                "message": f"Topic '{topic}' exists (verified via Kafbat; creation skipped).",
                "source": "kafbat",
            }
        return {
            "ok": False,
            "created": False,
            "error": fallback.get("error") or f"Kafka topic '{topic}' could not be verified via Kafbat.",
            "error_code": fallback.get("error_code") or "KAFBAT_TOPIC_VERIFY_FAILED",
        }

    security_protocol = conn.get("security_protocol") or "PLAINTEXT"
    sasl_mechanism = conn.get("sasl_mechanism")
    sasl_username = conn.get("sasl_username")
    sasl_password = conn.get("sasl_password")

    loop = asyncio.get_event_loop()
    try:
        result = await asyncio.wait_for(
            loop.run_in_executor(
                None,
                _ensure_topic_exists_sync,
                bootstrap_servers,
                topic,
                partitions,
                replication_factor,
                security_protocol,
                sasl_mechanism,
                sasl_username,
                sasl_password,
            ),
            timeout=20.0,
        )
        if result.get("ok") or not kafbat_url:
            return result
        fallback = await _kafbat_topic_message_count(
            kafbat_url,
            conn.get("kafbat_username"),
            conn.get("kafbat_password"),
            topic,
        )
        if fallback.get("ok"):
            return {
                "ok": True,
                "created": False,
                "message": (
                    f"Topic '{topic}' exists (verified via Kafbat after native Kafka "
                    "admin failed; creation skipped)."
                ),
                "source": "kafbat",
                "native_error": result.get("error"),
                "native_error_code": result.get("error_code"),
            }
        return result
    except asyncio.TimeoutError:
        return {"ok": False, "created": False, "error": "Kafka topic creation timed out.", "error_code": "TIMEOUT"}
    except Exception as e:
        return {"ok": False, "created": False, "error": str(e)[:300], "error_code": "TOPIC_CREATE_FAILED"}


async def produce_kafka_message(
    bootstrap_servers: str,
    topic: str,
    value: Any,
    key: Optional[str] = None,
    security_protocol: str = "PLAINTEXT",
    sasl_mechanism: Optional[str] = None,
    sasl_username: Optional[str] = None,
    sasl_password: Optional[str] = None,
) -> Dict[str, Any]:
    if not bootstrap_servers:
        return {"ok": False, "error": "Kafka bootstrap endpoint is not configured.", "error_code": "MISSING_BOOTSTRAP"}
    if not topic:
        return {"ok": False, "error": "Kafka topic is not configured.", "error_code": "MISSING_TOPIC"}

    loop = asyncio.get_event_loop()
    try:
        result = await asyncio.wait_for(
            loop.run_in_executor(
                None,
                _produce_kafka_message_sync,
                bootstrap_servers,
                topic,
                value,
                key,
                security_protocol,
                sasl_mechanism,
                sasl_username,
                sasl_password,
            ),
            timeout=20.0,
        )
        return result
    except asyncio.TimeoutError:
        return {"ok": False, "error": "Kafka produce timed out.", "error_code": "TIMEOUT"}
    except Exception as e:
        return {"ok": False, "error": str(e)[:300], "error_code": "KAFKA_PRODUCE_FAILED"}
