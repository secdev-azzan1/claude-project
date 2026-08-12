"""
Kafka Schema Consumer.

Reads messages from a Kafka topic for schema inference purposes.
Strategy:
  1. Try direct kafka-python KafkaConsumer (works on same-network deployments)
  2. Fallback to Kafbat REST API (works in devtunnel/proxied environments)

Returns list of parsed Python objects for common payload formats:
- JSON
- XML
- CSV
- key=value
- plain text
"""
from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import re
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

import httpx
from services.http_tls import tls_verify_enabled

logger = logging.getLogger(__name__)

BOOL_TRUE = {"true", "t", "yes", "y"}
BOOL_FALSE = {"false", "f", "no", "n"}
INT_RE = re.compile(r"^[+-]?\d+$")
FLOAT_RE = re.compile(r"^[+-]?(?:\d+\.\d*|\d*\.\d+)(?:[eE][+-]?\d+)?$")


def _decode_payload_bytes(payload: bytes) -> str:
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError:
        return payload.decode("utf-8", errors="replace")


def _coerce_scalar(text: str, coerce_numbers: bool = True, coerce_bools: bool = True) -> Any:
    value = text.strip()
    if value == "":
        return None

    if coerce_numbers:
        if INT_RE.match(value):
            try:
                return int(value)
            except ValueError:
                pass
        if FLOAT_RE.match(value):
            try:
                return float(value)
            except ValueError:
                pass

    low = value.lower()
    if coerce_bools:
        if low in BOOL_TRUE:
            return True
        if low in BOOL_FALSE:
            return False

    return value


def _normalize_xml_name(name: str) -> str:
    if name.startswith("{"):
        _, _, stripped = name.partition("}")
        if stripped:
            return stripped
    return name


def _pick_most_common(counter: Counter) -> Optional[str]:
    if not counter:
        return None
    top_count = max(counter.values())
    candidates = [name for name, count in counter.items() if count == top_count]
    return sorted(candidates)[0] if candidates else None


@dataclass
class XmlHints:
    path_child_counts: Dict[str, Counter] = field(default_factory=lambda: defaultdict(Counter))
    tag_child_counts: Dict[str, Counter] = field(default_factory=lambda: defaultdict(Counter))
    force_list_paths: Set[str] = field(default_factory=set)


def _infer_empty_container_child_tag(path: str, tag: str, xml_hints: Optional[XmlHints]) -> Optional[str]:
    normalized_tag = _normalize_xml_name(tag)
    if xml_hints is not None:
        path_choice = _pick_most_common(xml_hints.path_child_counts.get(path, Counter()))
        if path_choice:
            return path_choice
        tag_choice = _pick_most_common(xml_hints.tag_child_counts.get(normalized_tag, Counter()))
        if tag_choice:
            return tag_choice
    return None


def _collect_xml_hints_from_element(elem: ET.Element, path: str, hints: XmlHints) -> None:
    children = list(elem)
    if not children:
        return

    parent_tag = _normalize_xml_name(elem.tag)
    grouped: Dict[str, List[ET.Element]] = defaultdict(list)
    for child in children:
        grouped[_normalize_xml_name(child.tag)].append(child)

    for child_tag, elems in grouped.items():
        hints.path_child_counts[path][child_tag] += len(elems)
        hints.tag_child_counts[parent_tag][child_tag] += len(elems)
        child_path = f"{path}.{child_tag}" if path else child_tag
        if len(elems) > 1:
            hints.force_list_paths.add(child_path)

    for child in children:
        child_tag = _normalize_xml_name(child.tag)
        child_path = f"{path}.{child_tag}" if path else child_tag
        _collect_xml_hints_from_element(child, child_path, hints)


def _collect_xml_hints(texts: List[str]) -> XmlHints:
    hints = XmlHints()
    for text in texts:
        try:
            root = ET.fromstring(text)
        except Exception:
            continue
        root_tag = _normalize_xml_name(root.tag)
        _collect_xml_hints_from_element(root, root_tag, hints)
    return hints


def _xml_to_object(
    elem: ET.Element,
    path: str,
    xml_hints: Optional[XmlHints] = None,
    coerce_numbers: bool = True,
    coerce_bools: bool = True,
) -> Any:
    normalized_tag = _normalize_xml_name(elem.tag)
    children = list(elem)
    attrs: Dict[str, Any] = {}
    for key, value in elem.attrib.items():
        attr_name = f"@{_normalize_xml_name(key)}"
        attrs[attr_name] = _coerce_scalar(value, coerce_numbers=coerce_numbers, coerce_bools=coerce_bools)
    text_value = _coerce_scalar(elem.text or "", coerce_numbers=coerce_numbers, coerce_bools=coerce_bools)

    if children:
        grouped: Dict[str, List[ET.Element]] = defaultdict(list)
        for child in children:
            grouped[_normalize_xml_name(child.tag)].append(child)

        out: Dict[str, Any] = {}
        out.update(attrs)
        for tag, elems in grouped.items():
            child_path = f"{path}.{tag}" if path else tag
            values = [
                _xml_to_object(
                    child,
                    path=child_path,
                    xml_hints=xml_hints,
                    coerce_numbers=coerce_numbers,
                    coerce_bools=coerce_bools,
                )
                for child in elems
            ]
            force_list = xml_hints is not None and child_path in xml_hints.force_list_paths
            if force_list:
                out[tag] = values
            else:
                out[tag] = values[0] if len(values) == 1 else values

        if text_value is not None:
            out["#text"] = text_value
        return out

    if attrs:
        if text_value is not None:
            attrs["#text"] = text_value
        return attrs

    if text_value is not None:
        return text_value

    child_name = _infer_empty_container_child_tag(path=path, tag=normalized_tag, xml_hints=xml_hints)
    if child_name:
        return {child_name: None}
    return None


def _parse_xml(
    text: str,
    coerce_numbers: bool = True,
    coerce_bools: bool = True,
    xml_hints: Optional[XmlHints] = None,
) -> Any:
    root = ET.fromstring(text)
    root_path = _normalize_xml_name(root.tag)
    return _xml_to_object(
        root,
        path=root_path,
        xml_hints=xml_hints,
        coerce_numbers=coerce_numbers,
        coerce_bools=coerce_bools,
    )


def _parse_csv(text: str, coerce_numbers: bool = True, coerce_bools: bool = True) -> Any:
    sample = text[:4096]
    sniffer = csv.Sniffer()
    try:
        dialect = sniffer.sniff(sample)
    except Exception:
        dialect = csv.excel
    try:
        has_header = sniffer.has_header(sample)
    except Exception:
        has_header = True

    rows = list(csv.reader(io.StringIO(text), dialect=dialect))
    if not rows:
        return None

    if has_header:
        header = [col.strip() for col in rows[0]]
        data_rows = rows[1:]
        out_rows: List[Dict[str, Any]] = []
        for row in data_rows:
            obj: Dict[str, Any] = {}
            for index, column in enumerate(header):
                value = row[index] if index < len(row) else ""
                obj[column] = _coerce_scalar(value, coerce_numbers=coerce_numbers, coerce_bools=coerce_bools)
            out_rows.append(obj)
        if len(out_rows) == 1:
            return out_rows[0]
        return out_rows

    if len(rows) == 1:
        return [_coerce_scalar(value, coerce_numbers=coerce_numbers, coerce_bools=coerce_bools) for value in rows[0]]
    return [
        [_coerce_scalar(value, coerce_numbers=coerce_numbers, coerce_bools=coerce_bools) for value in row]
        for row in rows
    ]


def _parse_kv(text: str, coerce_numbers: bool = True, coerce_bools: bool = True) -> Any:
    parts = re.split(r"[;,]\s*|\s+(?=\w+\s*[:=])", text.strip())
    obj: Dict[str, Any] = {}
    for part in parts:
        part = part.strip()
        if not part:
            continue
        if "=" in part:
            key, value = part.split("=", 1)
        elif ":" in part:
            key, value = part.split(":", 1)
        else:
            continue
        obj[key.strip()] = _coerce_scalar(value, coerce_numbers=coerce_numbers, coerce_bools=coerce_bools)
    return obj if obj else text


def _detect_format(text: str) -> str:
    stripped = text.lstrip("\ufeff").lstrip()
    if not stripped:
        return "plain"
    if stripped[0] in "{[":
        return "json"
    if stripped[0] == "<":
        return "xml"
    if re.search(r"\b\w+\s*[:=]", stripped):
        return "kv"
    if "," in stripped or "\t" in stripped or ";" in stripped:
        return "csv"
    return "plain"


def _parse_text_by_format(
    text: str,
    fmt: str,
    coerce_numbers: bool = True,
    coerce_bools: bool = True,
    xml_hints: Optional[XmlHints] = None,
) -> Any:
    if fmt == "json":
        return json.loads(text)
    if fmt == "xml":
        return _parse_xml(text, coerce_numbers=coerce_numbers, coerce_bools=coerce_bools, xml_hints=xml_hints)
    if fmt == "csv":
        return _parse_csv(text, coerce_numbers=coerce_numbers, coerce_bools=coerce_bools)
    if fmt == "kv":
        return _parse_kv(text, coerce_numbers=coerce_numbers, coerce_bools=coerce_bools)
    if fmt == "plain":
        return _coerce_scalar(text, coerce_numbers=coerce_numbers, coerce_bools=coerce_bools)
    return text


def _parse_text_messages(
    texts: List[str],
    coerce_numbers: bool = True,
    coerce_bools: bool = True,
) -> Tuple[List[Any], Dict[str, int], int]:
    formats = [_detect_format(text) for text in texts]
    xml_texts = [text for text, fmt in zip(texts, formats) if fmt == "xml"]
    xml_hints = _collect_xml_hints(xml_texts) if xml_texts else None

    parsed: List[Any] = []
    format_counts: Counter = Counter()
    parse_failures = 0

    for index, text in enumerate(texts):
        fmt = formats[index]
        try:
            parsed.append(
                _parse_text_by_format(
                    text,
                    fmt=fmt,
                    coerce_numbers=coerce_numbers,
                    coerce_bools=coerce_bools,
                    xml_hints=xml_hints,
                )
            )
            format_counts[fmt] += 1
        except Exception as exc:
            parse_failures += 1
            format_counts["parse_error"] += 1
            logger.debug(f"Failed to parse message as {fmt}: {exc}")

    if parsed:
        logger.info(
            "Parsed %s messages for inference (formats=%s, parse_failures=%s)",
            len(parsed),
            dict(format_counts),
            parse_failures,
        )
    return parsed, dict(format_counts), parse_failures


# ---------------------------------------------------------------------------
# Direct kafka-python consumer
# ---------------------------------------------------------------------------

def _sync_consume_kafka(
    bootstrap_servers: str,
    topic: str,
    max_messages: int = 50,
    timeout_ms: int = 10000,
    from_beginning: bool = True,
) -> Tuple[List[bytes], Optional[str]]:
    """Synchronous kafka-python consumer. Returns (raw_messages_bytes, error_or_None)."""
    try:
        from kafka import KafkaConsumer, TopicPartition
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=None,
            enable_auto_commit=False,
            auto_offset_reset="earliest" if from_beginning else "latest",
            consumer_timeout_ms=timeout_ms,
        )
        parts = consumer.partitions_for_topic(topic)
        if not parts:
            consumer.close()
            return [], f"Topic '{topic}' not found or no partitions available"

        tps = [TopicPartition(topic, p) for p in sorted(parts)]
        consumer.assign(tps)
        if from_beginning:
            consumer.seek_to_beginning(*tps)

        messages: List[bytes] = []
        consumed = 0
        while consumed < max_messages:
            polled = consumer.poll(timeout_ms=1000, max_records=min(100, max_messages - consumed))
            if not polled:
                break
            for tp, recs in polled.items():
                for msg in recs:
                    if msg.value:
                        messages.append(msg.value)
                        consumed += 1
                    if consumed >= max_messages:
                        break
                if consumed >= max_messages:
                    break
        consumer.close()
        return messages, None
    except ImportError:
        return [], "kafka-python library not installed"
    except Exception as e:
        return [], str(e)[:300]


async def consume_from_kafka(
    bootstrap_servers: str,
    topic: str,
    max_messages: int = 50,
    timeout_ms: int = 10000,
    from_beginning: bool = True,
) -> Tuple[List[Any], Optional[str]]:
    """Async wrapper: consume raw bytes from Kafka and return parsed payload objects."""
    loop = asyncio.get_event_loop()
    raw_bytes, error = await asyncio.wait_for(
        loop.run_in_executor(
            None,
            _sync_consume_kafka,
            bootstrap_servers,
            topic,
            max_messages,
            timeout_ms,
            from_beginning,
        ),
        timeout=timeout_ms / 1000 + 30,  # extra buffer
    )
    if error:
        return [], error

    texts = [_decode_payload_bytes(raw) for raw in raw_bytes]
    parsed, _, _ = _parse_text_messages(texts)
    return parsed, None


# ---------------------------------------------------------------------------
# Kafbat REST API fallback
# ---------------------------------------------------------------------------

async def _kafbat_get_cluster_name(
    client: httpx.AsyncClient,
    kafbat_url: str,
) -> Optional[str]:
    """Get the first Kafbat cluster name."""
    try:
        resp = await client.get(f"{kafbat_url}/api/clusters")
        if resp.status_code == 200:
            clusters = resp.json()
            if clusters:
                return clusters[0].get("name") or clusters[0].get("id")
    except Exception as e:
        logger.debug(f"Kafbat get clusters failed: {e}")
    return None


def _parse_sse_events(body: str) -> List[Dict[str, Any]]:
    """
    Parse a text/event-stream payload into JSON event objects.

    Kafbat `/messages/v2` emits SSE frames where each event's JSON is in `data:`.
    """
    events: List[Dict[str, Any]] = []
    data_lines: List[str] = []

    def _flush_event() -> None:
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
            logger.debug(f"Failed to parse SSE event payload: {exc}")

    for raw_line in body.splitlines():
        line = raw_line.rstrip("\r")
        if line == "":
            _flush_event()
            continue
        if line.startswith(":"):
            # SSE comment line
            continue
        if line.startswith("data:"):
            value = line[5:]
            if value.startswith(" "):
                value = value[1:]
            data_lines.append(value)

    _flush_event()
    return events


async def consume_via_kafbat(
    kafbat_url: str,
    kafbat_username: Optional[str],
    kafbat_password: Optional[str],
    topic: str,
    max_messages: int = 50,
) -> Tuple[List[Any], Optional[str]]:
    """
    Read messages from Kafka via Kafbat REST API.
    Returns (parsed_objects, error_or_None).
    """
    base = (kafbat_url or "").rstrip("/")
    if not base:
        return [], "Kafbat URL not configured"

    try:
        async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=30.0, follow_redirects=True) as client:
            # Kafbat uses Spring Security form-based login
            # POST /login with form data; cookies are automatically stored by the client
            if kafbat_username:
                await client.post(
                    f"{base}/login",
                    data={"username": kafbat_username, "password": kafbat_password or ""},
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )

            # Get cluster name
            cluster = await _kafbat_get_cluster_name(client, base)
            if not cluster:
                return [], "Could not determine Kafbat cluster name. Check Kafbat connection."

            # Read messages via Kafbat's supported v2 endpoint (SSE response).
            params = {
                "mode": "EARLIEST",
                "limit": max_messages,
            }
            resp = await client.get(
                f"{base}/api/clusters/{cluster}/topics/{topic}/messages/v2",
                params=params,
            )
            if resp.status_code == 404:
                return [], f"Topic '{topic}' not found in Kafbat cluster '{cluster}'"
            if resp.status_code != 200:
                return [], f"Kafbat messages/v2 API returned {resp.status_code}: {resp.text[:200]}"

            events = _parse_sse_events(resp.text)
            event_counts: Counter = Counter(
                str(event.get("type") or "UNKNOWN").upper() for event in events
            )

            parsed: List[Any] = []
            text_payloads: List[str] = []
            for event in events:
                if str(event.get("type") or "").upper() != "MESSAGE":
                    continue

                msg = event.get("message") or {}
                if not isinstance(msg, dict):
                    continue

                # Kafbat v2 payload uses `message.value`; keep `content` fallback for compatibility.
                content = msg.get("value")
                if content is None:
                    content = msg.get("content")
                if content is None:
                    continue

                if isinstance(content, dict):
                    parsed.append(content)
                elif isinstance(content, str) and content.strip():
                    text_payloads.append(content)
                else:
                    text_payloads.append(str(content))

            if text_payloads:
                parsed_text, _, _ = _parse_text_messages(text_payloads)
                parsed.extend(parsed_text)

            logger.info(
                "Kafbat v2: parsed %s messages from topic '%s' (events=%s)",
                len(parsed),
                topic,
                dict(event_counts),
            )
            return parsed, None

    except Exception as e:
        return [], f"Kafbat REST consumer error: {str(e)[:300]}"


# ---------------------------------------------------------------------------
# Combined consumer (kafka-python first, Kafbat fallback)
# ---------------------------------------------------------------------------

async def consume_messages_for_inference(
    bootstrap_servers: str,
    topic: str,
    max_messages: int = 50,
    kafbat_url: Optional[str] = None,
    kafbat_username: Optional[str] = None,
    kafbat_password: Optional[str] = None,
    timeout_ms: int = 10000,
) -> Tuple[List[Any], str, Optional[str]]:
    """
    Combined consumer: tries kafka-python first, falls back to Kafbat.
    Returns (parsed_objects, method_used, error_or_None).
    """
    # Try direct kafka-python first
    try:
        messages, error = await asyncio.wait_for(
            consume_from_kafka(bootstrap_servers, topic, max_messages, timeout_ms),
            timeout=min(timeout_ms / 1000 + 30, 60),
        )
        if not error and len(messages) > 0:
            logger.info(f"kafka-python: got {len(messages)} messages from '{topic}'")
            return messages, "kafka-python", None
        if error:
            logger.info(f"kafka-python failed: {error}. Trying Kafbat fallback.")
        else:
            logger.info(f"kafka-python: 0 messages from '{topic}' (topic may be empty or not yet written). Trying Kafbat.")
    except asyncio.TimeoutError:
        logger.info("kafka-python timed out. Trying Kafbat fallback.")
    except Exception as e:
        logger.info(f"kafka-python exception: {e}. Trying Kafbat fallback.")

    # Fallback: Kafbat REST API
    if kafbat_url:
        messages, error = await consume_via_kafbat(
            kafbat_url, kafbat_username, kafbat_password, topic, max_messages
        )
        if error:
            return [], "kafbat", f"Both consumers failed. Kafbat error: {error}"
        return messages, "kafbat", None

    return [], "none", f"kafka-python returned 0 messages and Kafbat is not configured"
