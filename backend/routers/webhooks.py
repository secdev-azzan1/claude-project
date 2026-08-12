import hashlib
import hmac
import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from xml.etree import ElementTree as ET

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from motor.motor_asyncio import AsyncIOMotorDatabase

from db import get_db
from services.application_services import resolve_source_application_service
from services.kafka_client import produce_kafka_message
from services.connection_resolver import resolve_connection

router = APIRouter(prefix="/api/webhooks", tags=["webhooks"])
WEBHOOK_MAX_PAYLOAD_BYTES = int(os.environ.get("WEBHOOK_MAX_PAYLOAD_BYTES", "1048576"))
WEBHOOK_IDEMPOTENCY_HEADERS = [
    "Idempotency-Key",
    "X-Request-Id",
    "X-Event-Id",
    "X-Webhook-Id",
]


def _strip_xml_ns(tag: str) -> str:
    out = tag or ""
    if out.startswith("{") and "}" in out:
        out = out.split("}", 1)[1]
    if ":" in out:
        out = out.split(":", 1)[1]
    return out


def _xml_to_obj(element: ET.Element) -> Any:
    attrs = {f"@{_strip_xml_ns(k)}": v for k, v in (element.attrib or {}).items()}
    children = list(element)
    text = (element.text or "").strip()
    if not children:
        if attrs:
            if text:
                attrs["#text"] = text
            return attrs
        return text

    grouped: Dict[str, List[Any]] = {}
    for child in children:
        key = _strip_xml_ns(child.tag)
        grouped.setdefault(key, []).append(_xml_to_obj(child))

    payload: Dict[str, Any] = {}
    payload.update(attrs)
    for key, values in grouped.items():
        payload[key] = values[0] if len(values) == 1 else values
    if text:
        payload["#text"] = text
    return payload


def _parse_json_path(path: str) -> List[Any]:
    if not path or path == "$":
        return []
    p = path.strip()
    if p.startswith("$"):
        p = p[1:]
    tokens: List[Any] = []
    i = 0
    while i < len(p):
        c = p[i]
        if c == ".":
            i += 1
            j = i
            while j < len(p) and re.match(r"[A-Za-z0-9_]", p[j]):
                j += 1
            token = p[i:j]
            if token:
                tokens.append(token)
            i = j
            continue
        if c == "[":
            j = p.find("]", i)
            if j == -1:
                break
            token = p[i + 1 : j].strip()
            if (token.startswith("\"") and token.endswith("\"")) or (token.startswith("'") and token.endswith("'")):
                token = token[1:-1]
                tokens.append(token)
            else:
                try:
                    tokens.append(int(token))
                except Exception:
                    tokens.append(token)
            i = j + 1
            continue
        i += 1
    return tokens


def _get_json_path_value(payload: Any, path: str) -> Any:
    current = payload
    for tok in _parse_json_path(path):
        if isinstance(tok, int):
            if not isinstance(current, list) or tok >= len(current):
                return None
            current = current[tok]
            continue
        if not isinstance(current, dict):
            return None
        current = current.get(tok)
    return current


def _set_field(payload: Any, field: str, value: Any) -> Any:
    if not isinstance(payload, dict):
        return payload
    key = (field or "").strip()
    if not key:
        return payload
    if key.startswith("$."):
        key = key[2:]
    segments = [segment for segment in key.split(".") if segment]
    if not segments:
        return payload
    current: Dict[str, Any] = payload
    for segment in segments[:-1]:
        existing = current.get(segment)
        if not isinstance(existing, dict):
            existing = {}
            current[segment] = existing
        current = existing
    current[segments[-1]] = value
    return payload


def _remove_field(payload: Any, field: str) -> Any:
    if not isinstance(payload, dict):
        return payload
    key = (field or "").strip()
    if not key:
        return payload
    if key.startswith("$."):
        key = key[2:]
    segments = [segment for segment in key.split(".") if segment]
    if not segments:
        return payload
    current: Dict[str, Any] = payload
    for segment in segments[:-1]:
        next_obj = current.get(segment)
        if not isinstance(next_obj, dict):
            return payload
        current = next_obj
    current.pop(segments[-1], None)
    return payload


def _xml_find_split_records(root: ET.Element, stream: Dict[str, Any]) -> List[ET.Element]:
    if not stream.get("split_response_records"):
        return [root]

    record_name = (stream.get("xml_record_element") or "").strip()
    if record_name:
        out = [elem for elem in root.iter() if _strip_xml_ns(elem.tag) == record_name]
        return out or [root]

    depth_raw = stream.get("xml_split_depth")
    try:
        depth = int(depth_raw) if depth_raw is not None else None
    except Exception:
        depth = None

    if not depth or depth < 1:
        return [root]

    out: List[ET.Element] = []

    def walk(node: ET.Element, level: int) -> None:
        if level == depth:
            out.append(node)
            return
        for child in list(node):
            walk(child, level + 1)

    walk(root, 1)
    return out or [root]


def _eval_localname_xpath(elem: ET.Element, expr: str) -> Optional[str]:
    expression = (expr or "").strip()
    if not expression:
        return None

    if expression.startswith("$"):
        return None

    if expression.endswith("/text()"):
        expression = expression[:-7]
        want_text = True
    else:
        want_text = False

    parts = [p for p in expression.split("/") if p]
    current_nodes = [elem]
    for idx, part in enumerate(parts):
        if "local-name()" in part:
            m = re.search(r"local-name\(\)\s*=\s*'([^']+)'", part)
            if not m:
                return None
            name = m.group(1)
        else:
            name = _strip_xml_ns(part)

        next_nodes: List[ET.Element] = []
        for node in current_nodes:
            candidates = list(node) if idx > 0 else [node]
            for cand in candidates:
                if _strip_xml_ns(cand.tag) == name:
                    next_nodes.append(cand)
        if not next_nodes:
            if idx == 0:
                for node in current_nodes:
                    for child in list(node):
                        if _strip_xml_ns(child.tag) == name:
                            next_nodes.append(child)
            if not next_nodes:
                return None
        current_nodes = next_nodes

    target = current_nodes[0] if current_nodes else None
    if target is None:
        return None
    if want_text:
        return (target.text or "").strip()
    return json.dumps(_xml_to_obj(target), ensure_ascii=False)


def _evaluate_extractions(
    stream: Dict[str, Any],
    payload: Any,
    xml_elem: Optional[ET.Element],
) -> Dict[str, Any]:
    attrs: Dict[str, Any] = {}
    for rule in stream.get("extraction_rules") or []:
        name = (rule.get("attribute_name") or "").strip()
        path = (rule.get("path") or "").strip()
        default_value = rule.get("default_value")
        if not name or not path:
            continue

        value = None
        if xml_elem is not None:
            value = _eval_localname_xpath(xml_elem, path)
            if value is None and isinstance(payload, dict):
                value = _get_json_path_value(payload, path)
        else:
            value = _get_json_path_value(payload, path)

        if value in (None, ""):
            value = default_value if default_value not in (None, "") else None
        attrs[name] = value
    return attrs


def _apply_transformations(stream: Dict[str, Any], payload: Any, attrs: Dict[str, Any]) -> Any:
    out = payload
    for rule in stream.get("transformation_rules") or []:
        rtype = (rule.get("type") or "").strip().lower()
        field_name = (rule.get("field_name") or "").strip()
        if rtype == "add_field":
            out = _set_field(out, field_name, rule.get("value"))
        elif rtype == "remove_field":
            out = _remove_field(out, field_name)
        elif rtype == "set_from_attribute":
            src = (rule.get("source_attribute") or "").strip()
            out = _set_field(out, field_name, attrs.get(src))
    return out


def _string_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _routing_decision(stream: Dict[str, Any], payload: Any, attrs: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    for rule in stream.get("routing_rules") or []:
        attr_name = (rule.get("attribute") or "").strip()
        cond = (rule.get("condition") or "").strip().lower()
        val = (rule.get("value") or "")
        dest = (rule.get("destination") or "include").strip().lower()
        topic_name = (rule.get("topic_name") or "").strip() or None

        if not attr_name:
            continue
        candidate = attrs.get(attr_name)
        if candidate is None and isinstance(payload, dict):
            candidate = payload.get(attr_name)
        text = _string_value(candidate)

        matched = False
        if cond == "equals":
            matched = text == val
        elif cond == "contains":
            matched = val in text
        elif cond == "starts_with":
            matched = text.startswith(val)
        elif cond == "ends_with":
            matched = text.endswith(val)
        elif cond == "regex":
            try:
                matched = bool(re.search(val, text))
            except re.error:
                matched = False
        elif cond == "is_empty":
            matched = text.strip() == ""

        if matched:
            if dest == "exclude":
                return "exclude", None
            if dest == "route_to_topic":
                return "route_to_topic", topic_name
            return "include", None

    return "include", None


def _derive_kafka_key(source: Dict[str, Any], stream: Dict[str, Any], payload: Any, attrs: Dict[str, Any]) -> Optional[str]:
    kafka_output = source.get("kafka_output") or {}
    strategy = (kafka_output.get("key_strategy") or "none").strip().lower()
    if strategy != "primary_key":
        return None
    keys = stream.get("primary_key_fields") or []
    if not keys:
        return None
    primary = str(keys[0]).strip()
    if not primary:
        return None
    value = attrs.get(primary)
    if value is None and isinstance(payload, dict):
        value = payload.get(primary)
    if value is None:
        return None
    return str(value)


def _verify_signature(secret: str, algo: str, body: bytes, provided: str) -> bool:
    if not secret:
        return True
    if not provided:
        return False
    a = (algo or "hmac-sha256").strip().lower()
    if a != "hmac-sha256":
        return False

    signature = provided.strip()
    if signature.startswith("sha256="):
        signature = signature.split("=", 1)[1]

    expected = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


def _choose_format(source: Dict[str, Any], stream: Dict[str, Any], content_type: str) -> str:
    configured = (source.get("webhook_content_type") or "auto").strip().lower()
    if configured in {"json", "xml", "text"}:
        return configured
    stream_fmt = (stream.get("response_format") or "json").strip().lower()
    if stream_fmt == "xml":
        return "xml"
    if "xml" in (content_type or "").lower():
        return "xml"
    if "json" in (content_type or "").lower():
        return "json"
    return "json"


def _derive_request_key(request: Request, endpoint: str, body: bytes) -> str:
    for header in WEBHOOK_IDEMPOTENCY_HEADERS:
        value = (request.headers.get(header) or "").strip()
        if value:
            return f"header:{header.lower()}:{value}"
    digest = hashlib.sha256(body).hexdigest()
    return f"body:{endpoint}:{digest}"


def _request_key_hash(request_key: str) -> str:
    return hashlib.sha256(request_key.encode("utf-8")).hexdigest()[:24]


async def _log_audit(db: AsyncIOMotorDatabase, action: str, target: str, status: str, details: str = "") -> None:
    from models.audit import AuditEvent

    event = AuditEvent(
        action=action,
        object_type="Webhook",
        target=target,
        status=status,
        details=details[:500] if details else None,
    )
    await db.audit_events.insert_one(event.dict())


@router.get("/{endpoint_id}/health", summary="Webhook endpoint health")
async def webhook_health(endpoint_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    endpoint = (endpoint_id or "").strip().lower()
    source = await db.sources.find_one({"type": "Webhook", "webhook_endpoint_id": endpoint}, {"_id": 0})
    if not source:
        raise HTTPException(status_code=404, detail="Webhook endpoint not found")
    source = await resolve_source_application_service(db, source)

    flow = await db.flows.find_one({"source_id": source.get("id")}, {"_id": 0, "state": 1, "enabled": 1})
    accepting = bool(source.get("webhook_enabled", True)) and bool(flow and flow.get("enabled")) and flow.get("state") == "Running"
    return {
        "ok": True,
        "endpoint_id": endpoint,
        "webhook_enabled": bool(source.get("webhook_enabled", True)),
        "accepting_events": accepting,
    }


@router.post("/{endpoint_id}", summary="Receive webhook event")
async def receive_webhook(
    endpoint_id: str,
    request: Request,
    x_signature: Optional[str] = Header(default=None),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    endpoint = (endpoint_id or "").strip().lower()
    source = await db.sources.find_one({"type": "Webhook", "webhook_endpoint_id": endpoint}, {"_id": 0})
    if not source:
        raise HTTPException(status_code=404, detail="Webhook endpoint not found")
    source = await resolve_source_application_service(db, source)

    flow = await db.flows.find_one({"source_id": source.get("id")}, {"_id": 0})
    if not flow:
        raise HTTPException(status_code=404, detail="No flow found for webhook source")

    if not source.get("webhook_enabled", True):
        raise HTTPException(status_code=409, detail="Webhook source is disabled")
    if not flow.get("enabled", True):
        raise HTTPException(status_code=409, detail="Flow is disabled")
    if flow.get("state") != "Running":
        raise HTTPException(status_code=409, detail="Flow is not running")

    signature_header = (source.get("webhook_signature_header") or "X-Signature").strip()
    sig_value = request.headers.get(signature_header) or x_signature
    secret = source.get("webhook_secret") or ""
    algo = source.get("webhook_signature_algo") or "hmac-sha256"
    body = await request.body()
    if len(body) > WEBHOOK_MAX_PAYLOAD_BYTES:
        raise HTTPException(status_code=413, detail=f"Webhook payload exceeds {WEBHOOK_MAX_PAYLOAD_BYTES} bytes limit.")
    body_text = body.decode("utf-8", errors="replace")
    if secret and not _verify_signature(secret, algo, body, sig_value or ""):
        await _log_audit(db, "Webhook rejected", endpoint, "Failed", "signature validation failed")
        raise HTTPException(status_code=401, detail="Invalid webhook signature")

    request_key = _derive_request_key(request, endpoint, body)
    request_state_filter = {
        "flow_id": flow.get("id"),
        "request_key": request_key,
    }
    request_state = await db.webhook_request_state.find_one(request_state_filter, {"_id": 0})
    if request_state:
        status = (request_state.get("status") or "").lower()
        if status == "completed":
            prior = request_state.get("result") or {}
            return {
                **prior,
                "duplicate": True,
                "request_key": request_key,
            }
        if status == "processing":
            return {
                "ok": True,
                "accepted": int(request_state.get("accepted") or 0),
                "published": int(request_state.get("published") or 0),
                "excluded": int(request_state.get("excluded") or 0),
                "routed": int(request_state.get("routed") or 0),
                "topic": flow.get("kafka_topic"),
                "duplicate": True,
                "message": "Webhook request is already being processed.",
            }
        await db.webhook_request_state.update_one(
            request_state_filter,
            {"$set": {"status": "processing", "updated_at": datetime.utcnow(), "last_error": None}},
        )
        request_state = await db.webhook_request_state.find_one(request_state_filter, {"_id": 0})
    else:
        now = datetime.utcnow()
        request_state = {
            "id": f"whr-{flow.get('id')}-{_request_key_hash(request_key)}",
            "flow_id": flow.get("id"),
            "source_id": source.get("id"),
            "endpoint_id": endpoint,
            "request_key": request_key,
            "status": "processing",
            "accepted": 0,
            "published": 0,
            "excluded": 0,
            "routed": 0,
            "processed_indices": [],
            "last_error": None,
            "result": None,
            "created_at": now,
            "updated_at": now,
        }
        await db.webhook_request_state.insert_one(request_state)

    streams = source.get("streams") or []
    if len(streams) != 1:
        await db.webhook_request_state.update_one(
            request_state_filter,
            {"$set": {"status": "failed", "last_error": "Webhook source must contain exactly one stream", "updated_at": datetime.utcnow()}},
        )
        raise HTTPException(status_code=422, detail="Webhook source must contain exactly one stream")
    stream = streams[0]

    content_type = request.headers.get("content-type", "")
    format_hint = _choose_format(source, stream, content_type)

    parsed_payload: Any
    root_xml: Optional[ET.Element] = None
    if format_hint == "xml":
        try:
            root_xml = ET.fromstring(body_text)
            parsed_payload = {_strip_xml_ns(root_xml.tag): _xml_to_obj(root_xml)}
        except Exception:
            await db.webhook_request_state.update_one(
                request_state_filter,
                {"$set": {"status": "failed", "last_error": "Invalid XML payload", "updated_at": datetime.utcnow()}},
            )
            raise HTTPException(status_code=422, detail="Invalid XML payload")
    elif format_hint == "json":
        try:
            parsed_payload = json.loads(body_text)
        except Exception:
            await db.webhook_request_state.update_one(
                request_state_filter,
                {"$set": {"status": "failed", "last_error": "Invalid JSON payload", "updated_at": datetime.utcnow()}},
            )
            raise HTTPException(status_code=422, detail="Invalid JSON payload")
    else:
        parsed_payload = body_text

    records: List[Any] = []
    xml_records: List[Optional[ET.Element]] = []
    if root_xml is not None:
        elems = _xml_find_split_records(root_xml, stream)
        records = [_xml_to_obj(elem) for elem in elems]
        xml_records = elems
    else:
        if stream.get("split_response_records"):
            path = (stream.get("response_data_path") or "$").strip() or "$"
            selected = _get_json_path_value(parsed_payload, path)
            if isinstance(selected, list):
                records = selected
            elif selected is None:
                records = []
            else:
                records = [selected]
        else:
            records = [parsed_payload]
        xml_records = [None] * len(records)

    if not records:
        result = {"ok": True, "accepted": 0, "published": 0, "excluded": 0, "routed": 0, "message": "No records after split/filter"}
        await db.webhook_request_state.update_one(
            request_state_filter,
            {"$set": {"status": "completed", "result": result, "accepted": 0, "published": 0, "excluded": 0, "routed": 0, "updated_at": datetime.utcnow()}},
        )
        return result

    kafka_conn = await resolve_connection(db, "kafka")
    if not kafka_conn:
        await db.webhook_request_state.update_one(
            request_state_filter,
            {"$set": {"status": "failed", "last_error": "No Kafka connection configured", "updated_at": datetime.utcnow()}},
        )
        raise HTTPException(status_code=400, detail="No Kafka connection configured")

    bootstrap = (kafka_conn.get("endpoint") or "").strip()
    security_protocol = kafka_conn.get("security_protocol") or "PLAINTEXT"
    sasl_mechanism = kafka_conn.get("sasl_mechanism")
    sasl_username = kafka_conn.get("sasl_username")
    sasl_password = kafka_conn.get("sasl_password")

    main_topic = (flow.get("kafka_topic") or "").strip()
    if not main_topic:
        await db.webhook_request_state.update_one(
            request_state_filter,
            {"$set": {"status": "failed", "last_error": "Flow has no Kafka topic configured", "updated_at": datetime.utcnow()}},
        )
        raise HTTPException(status_code=400, detail="Flow has no Kafka topic configured")

    processed_indices = set(int(i) for i in (request_state.get("processed_indices") or []) if isinstance(i, int) or str(i).isdigit())
    published = int(request_state.get("published") or 0)
    routed = int(request_state.get("routed") or 0)
    excluded = int(request_state.get("excluded") or 0)

    for idx, record in enumerate(records):
        if idx in processed_indices:
            continue
        xml_elem = xml_records[idx] if idx < len(xml_records) else None
        attrs = _evaluate_extractions(stream, record, xml_elem)
        transformed = _apply_transformations(stream, record, attrs)
        route_action, route_topic = _routing_decision(stream, transformed, attrs)

        target_topic = main_topic
        if route_action == "exclude":
            excluded += 1
            target_topic = ""
        elif route_action == "route_to_topic" and route_topic:
            target_topic = route_topic
            routed += 1

        event_doc = {
            "id": f"whs-{flow.get('id')}-{_request_key_hash(request_key)}-{idx}",
            "endpoint_id": endpoint,
            "source_id": source.get("id"),
            "flow_id": flow.get("id"),
            "request_key": request_key,
            "record_index": idx,
            "topic": target_topic or None,
            "routing_action": route_action,
            "attributes": attrs,
            "payload": transformed,
            "payload_raw": body_text,
            "content_type": content_type,
            "created_at": datetime.utcnow(),
        }
        if target_topic:
            key = _derive_kafka_key(source, stream, transformed, attrs)
            result = await produce_kafka_message(
                bootstrap_servers=bootstrap,
                topic=target_topic,
                key=key,
                value=transformed,
                security_protocol=security_protocol,
                sasl_mechanism=sasl_mechanism,
                sasl_username=sasl_username,
                sasl_password=sasl_password,
            )
            if not result.get("ok"):
                error_text = result.get("error") or "Kafka publish failed"
                await db.webhook_request_state.update_one(
                    request_state_filter,
                    {
                        "$set": {
                            "status": "failed",
                            "accepted": len(records),
                            "published": published,
                            "excluded": excluded,
                            "routed": routed,
                            "processed_indices": sorted(processed_indices),
                            "last_error": error_text,
                            "updated_at": datetime.utcnow(),
                        }
                    },
                )
                await _log_audit(
                    db,
                    "Webhook publish failed",
                    endpoint,
                    "Failed",
                    error_text,
                )
                raise HTTPException(status_code=502, detail=error_text)
            published += 1

        await db.webhook_samples.update_one(
            {"id": event_doc["id"]},
            {"$setOnInsert": event_doc},
            upsert=True,
        )
        processed_indices.add(idx)
        await db.webhook_request_state.update_one(
            request_state_filter,
            {
                "$set": {
                    "accepted": len(records),
                    "published": published,
                    "excluded": excluded,
                    "routed": routed,
                    "processed_indices": sorted(processed_indices),
                    "updated_at": datetime.utcnow(),
                }
            },
        )

    await _log_audit(
        db,
        "Webhook accepted",
        endpoint,
        "Success",
        f"records={len(records)}, published={published}, excluded={excluded}, routed={routed}",
    )

    result_payload = {
        "ok": True,
        "accepted": len(records),
        "published": published,
        "excluded": excluded,
        "routed": routed,
        "topic": main_topic,
        "request_key": request_key,
    }
    await db.webhook_request_state.update_one(
        request_state_filter,
        {
            "$set": {
                "status": "completed",
                "accepted": len(records),
                "published": published,
                "excluded": excluded,
                "routed": routed,
                "processed_indices": sorted(processed_indices),
                "result": result_payload,
                "updated_at": datetime.utcnow(),
            }
        },
    )
    return result_payload
