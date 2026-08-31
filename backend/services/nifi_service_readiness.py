"""NiFi platform-service readiness and repair.

This module deliberately has a narrower responsibility than the NiFi repoint
workflow.  It reconciles only the platform-level controller services owned by
the application on one NiFi root process group.  Generated flow services
(record readers/writers, per-flow schema writers, and other block-scoped
services) are not discovered or modified here.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
import hashlib
import json
import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple
import uuid

from fastapi import HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase

from services.nifi_client import get_nifi_root_process_group_id, nifi_api_request
from services.nifi_flow_manager import get_controller_service_config, update_controller_service_config


logger = logging.getLogger(__name__)

PLATFORM_SERVICE_SPECS: Tuple[Dict[str, Any], ...] = (
    {
        "kind": "kafka",
        "name": "Data Mobility - Kafka",
        "aliases": ("kafka_connection",),
        "type": "org.apache.nifi.kafka.service.Kafka3ConnectionService",
        "dependency": "kafka",
    },
    {
        "kind": "schema_registry",
        "name": "Data Mobility - Apicurio Schema Registry",
        "aliases": ("schema_registry",),
        "type": "org.apache.nifi.confluent.schemaregistry.ConfluentSchemaRegistry",
        "dependency": "apicurio",
    },
    {
        "kind": "redis",
        "name": "Data Mobility - Redis",
        "aliases": ("redis_pool",),
        "type": "org.apache.nifi.redis.service.RedisConnectionPoolService",
        "dependency": "redis",
    },
)

_SECRET_PROPERTIES = {
    "Password",
    "sasl.password",
}


def _auth(conn: Dict[str, Any]) -> Dict[str, Any]:
    config = dict(conn.get("config") or {})
    auth_mode = str(config.get("authMode") or "none").lower()
    if auth_mode == "basic":
        auth_type = "BASIC"
    elif auth_mode == "bearer":
        auth_type = "BEARER"
    else:
        # Legacy connection documents already carry their auth fields at the
        # top level.  V2 documents always use config.authMode.
        auth_type = str(conn.get("auth_type") or "NONE").upper()
    return {
        "auth_type": auth_type,
        "username": config.get("username") or conn.get("username"),
        "password": config.get("password") or conn.get("password"),
        "token": config.get("token") or conn.get("token"),
    }


def _endpoint(conn: Dict[str, Any]) -> str:
    config = dict(conn.get("config") or {})
    return str(config.get("url") or conn.get("endpoint") or "").rstrip("/")


def _apicurio_ccompat_url(raw_url: str) -> str:
    root = (raw_url or "").rstrip("/")
    if not root:
        return ""
    if "/apis/ccompat/" in root:
        return root
    for marker in ("/apis/registry/v3", "/apis/registry/v2"):
        idx = root.find(marker)
        if idx != -1:
            root = root[:idx]
            break
    return f"{root.rstrip('/')}/apis/ccompat/v7"


def _desired_properties(kind: str, dependency: Dict[str, Any]) -> Dict[str, Any]:
    cfg = dict(dependency.get("config") or {})
    if kind == "kafka":
        security = str(cfg.get("securityProtocol") or "PLAINTEXT")
        props: Dict[str, Any] = {
            "bootstrap.servers": str(cfg.get("bootstrapServers") or ""),
            "security.protocol": security,
            "ack.wait.time": "5 sec",
            "max.block.ms": "5 sec",
            "default.api.timeout.ms": "60 sec",
            "max.poll.records": "10000",
            "isolation.level": "read_committed",
        }
        if security.startswith("SASL"):
            mechanism = str(cfg.get("saslMechanism") or "PLAIN").upper()
            props["sasl.mechanism"] = mechanism
            props["sasl.username"] = str(cfg.get("saslUsername") or "")
            props["sasl.password"] = str(cfg.get("saslPassword") or "")
        return props
    if kind == "schema_registry":
        auth_mode = str(cfg.get("authMode") or "none").upper()
        if auth_mode not in {"NONE", "BASIC", "BEARER"}:
            auth_mode = "NONE"
        return {
            "Schema Registry URLs": _apicurio_ccompat_url(str(cfg.get("url") or dependency.get("endpoint") or "")),
            "Cache Size": "1000",
            "Cache Expiration": "1 hour",
            "Communications Timeout": "30 secs",
            "Authentication Type": auth_mode,
            "Username": str(cfg.get("username") or "") if auth_mode != "NONE" else None,
            "Password": str(cfg.get("password") or "") if auth_mode == "BASIC" else None,
        }
    if kind == "redis":
        return {
            "Connection String": f"{cfg.get('host') or 'redis'}:{cfg.get('port') or 6379}",
            "Redis Mode": "Standalone",
            "Database Index": str(cfg.get("dedupDb") if cfg.get("dedupDb") is not None else 0),
            "Password": str(cfg.get("password") or ""),
        }
    raise ValueError(f"Unsupported platform service kind: {kind}")


def _service_component(item: Dict[str, Any]) -> Dict[str, Any]:
    return dict(item.get("component") or {})


def _service_id(item: Dict[str, Any]) -> Optional[str]:
    component = _service_component(item)
    return component.get("id") or item.get("id")


def _is_root_owned(item: Dict[str, Any], root_id: str) -> bool:
    component = _service_component(item)
    owner = component.get("parentGroupId") or component.get("processGroupId") or component.get("groupId")
    return not owner or owner == root_id


def _properties_fingerprint(properties: Dict[str, Any]) -> str:
    encoded = json.dumps(properties or {}, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _visible_properties_match(current: Dict[str, Any], desired: Dict[str, Any]) -> bool:
    """Compare properties without treating NiFi's masked secrets as drift.

    NiFi intentionally returns null for sensitive property values.  The
    persisted managed fingerprint below still lets us reapply a changed
    application secret once, while repeated checks remain idempotent.
    """
    for key, expected in desired.items():
        actual = current.get(key)
        if key in _SECRET_PROPERTIES and expected and (
            actual is None or (isinstance(actual, str) and set(actual) == {"*"})
        ):
            continue
        if str(actual or "") != str(expected or ""):
            return False
    return True


async def _connection_by_type(db: AsyncIOMotorDatabase, type_name: str) -> Optional[Dict[str, Any]]:
    collection = db["connections_v2"]
    doc = await collection.find_one({"type": type_name, "active": True}, {"_id": 0})
    if doc:
        return doc
    # Keep this compatible with older installations whose platform records
    # predate connections_v2.  This fallback does not alter which connection
    # is active; it only reads the existing legacy record.
    try:
        legacy = await db.connections.find_one(
            {"type": type_name, "$or": [{"is_active": True}, {"active": True}]},
            {"_id": 0},
        )
    except AttributeError:
        legacy = None
    return legacy


async def _list_root_services(
    endpoint: str,
    root_id: str,
    auth: Dict[str, Any],
) -> List[Dict[str, Any]]:
    result = await nifi_api_request(
        endpoint,
        "GET",
        f"/nifi-api/flow/process-groups/{root_id}/controller-services",
        **auth,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=502, detail=result.get("error") or "Could not list NiFi controller services.")
    return [item for item in ((result.get("data") or {}).get("controllerServices") or []) if _is_root_owned(item, root_id)]


async def _create_service(endpoint: str, root_id: str, auth: Dict[str, Any], spec: Dict[str, Any]) -> Optional[str]:
    result = await nifi_api_request(
        endpoint,
        "POST",
        f"/nifi-api/process-groups/{root_id}/controller-services",
        json_body={
            "revision": {"version": 0},
            "component": {
                "type": spec["type"],
                "name": spec["name"],
                "comments": "Managed by Data Mobility Platform; platform dependency service.",
                "properties": {},
            },
        },
        **auth,
    )
    if not result.get("ok"):
        return None
    data = result.get("data") or {}
    return (data.get("component") or {}).get("id") or data.get("id")


async def _set_state(endpoint: str, service_id: str, state: str, auth: Dict[str, Any]) -> Dict[str, Any]:
    current = await get_controller_service_config(endpoint, service_id, **auth)
    if not current.get("ok"):
        return current
    return await nifi_api_request(
        endpoint,
        "PUT",
        f"/nifi-api/controller-services/{service_id}/run-status",
        json_body={
            "revision": {"version": current.get("revision", 0)},
            "state": state,
            "disconnectedNodeAcknowledged": False,
        },
        **auth,
    )


async def _wait_enabled(endpoint: str, service_id: str, auth: Dict[str, Any], timeout: float = 12.0) -> Dict[str, Any]:
    deadline = asyncio.get_running_loop().time() + timeout
    last: Dict[str, Any] = {"ok": False, "state": "UNKNOWN", "validation_errors": []}
    while asyncio.get_running_loop().time() < deadline:
        last = await get_controller_service_config(endpoint, service_id, **auth)
        state = str(last.get("state") or "").upper()
        if last.get("ok") and state == "ENABLED":
            return last
        if last.get("ok") and state in {"DISABLED", "INVALID"} and last.get("validation_errors"):
            return last
        await asyncio.sleep(0.4)
    return last


async def _managed_records(db: AsyncIOMotorDatabase, nifi_connection_id: str, kind: str) -> List[Dict[str, Any]]:
    try:
        return await db["nifi_global_services"].find(
            {"nifi_connection_id": nifi_connection_id, "service_kind": kind},
            {"_id": 0},
        ).to_list(20)
    except (AttributeError, TypeError):
        return []


def _choose_live_service(
    services: Iterable[Dict[str, Any]],
    spec: Dict[str, Any],
    records: Iterable[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    by_id = {_service_id(item): item for item in services if _service_id(item)}
    for record in records:
        tracked = by_id.get(str(record.get("nifi_controller_service_id") or ""))
        if tracked:
            return tracked
    names = {spec["name"], *spec.get("aliases", ())}
    named = [item for item in services if str(_service_component(item).get("name") or "") in names]
    if named:
        return named[0]
    typed = [item for item in services if _service_component(item).get("type") == spec["type"]]
    # A single root service of one of the three platform types is safe to
    # adopt. Multiple services require an explicit managed name/record.
    return typed[0] if len(typed) == 1 else None


async def _upsert_record(
    db: AsyncIOMotorDatabase,
    nifi_connection: Dict[str, Any],
    root_id: str,
    spec: Dict[str, Any],
    service_id: str,
    desired: Dict[str, Any],
) -> None:
    stable_key = f"{nifi_connection.get('id')}:{spec['kind']}"
    record_id = f"nifi-ready-{uuid.uuid5(uuid.NAMESPACE_URL, stable_key)}"
    doc = {
        "id": record_id,
        "name": spec["name"],
        "service_kind": spec["kind"],
        "nifi_controller_service_id": service_id,
        "nifi_process_group_id": root_id,
        "nifi_type": spec["type"],
        "properties": desired,
        "properties_fingerprint": _properties_fingerprint(desired),
        "is_default": True,
        "managed_by": "platform_readiness_v1",
        "nifi_connection_id": nifi_connection.get("id"),
        "updated_at": datetime.utcnow(),
    }
    collection = db["nifi_global_services"]
    prior = await collection.find_one(
        {"nifi_connection_id": nifi_connection.get("id"), "service_kind": spec["kind"]},
        {"_id": 0},
    )
    if prior and prior.get("id"):
        await collection.update_one({"id": prior["id"]}, {"$set": {k: v for k, v in doc.items() if k != "id"}})
    else:
        await collection.insert_one(doc)


async def reconcile_platform_services(
    db: AsyncIOMotorDatabase,
    nifi_connection_id: str,
) -> Dict[str, Any]:
    """Audit and repair managed root-level platform controller services."""
    nifi_connection = await db["connections_v2"].find_one({"id": nifi_connection_id}, {"_id": 0})
    if not nifi_connection:
        raise HTTPException(status_code=404, detail="NiFi connection not found.")
    if nifi_connection.get("type") != "nifi":
        raise HTTPException(status_code=400, detail="Controller-service readiness is available for Apache NiFi connections only.")

    endpoint = _endpoint(nifi_connection)
    auth = _auth(nifi_connection)
    root_id = await get_nifi_root_process_group_id(endpoint, **auth)
    if not root_id:
        raise HTTPException(status_code=502, detail="Could not resolve the NiFi root process group.")

    root_services = await _list_root_services(endpoint, root_id, auth)
    dependencies = {
        kind: await _connection_by_type(db, kind)
        for kind in ("kafka", "apicurio", "redis")
    }
    results: List[Dict[str, Any]] = []

    for spec in PLATFORM_SERVICE_SPECS:
        kind = spec["kind"]
        dependency = dependencies.get(spec["dependency"])
        base = {
            "kind": kind,
            "name": spec["name"],
            "type": spec["type"],
            "status": "blocked",
            "changedProperties": [],
            "validationErrors": [],
        }
        if not dependency:
            base["message"] = f"No active {spec['dependency']} platform connection is configured."
            results.append(base)
            continue

        desired = _desired_properties(kind, dependency)
        records = await _managed_records(db, nifi_connection_id, kind)
        live_item = _choose_live_service(root_services, spec, records)
        service_id = _service_id(live_item) if live_item else None
        created = False

        if live_item and _service_component(live_item).get("type") != spec["type"]:
            base["status"] = "failed"
            base["message"] = (
                f"A root service named '{_service_component(live_item).get('name')}' exists with an incompatible type "
                f"({_service_component(live_item).get('type')}); NiFi cannot change a service's type in place."
            )
            results.append(base)
            continue

        if not service_id:
            service_id = await _create_service(endpoint, root_id, auth, spec)
            if not service_id:
                base["status"] = "failed"
                base["message"] = f"NiFi could not create the {kind} controller service."
                results.append(base)
                continue
            created = True

        live = await get_controller_service_config(endpoint, service_id, **auth)
        if not live.get("ok"):
            base["status"] = "failed"
            base["serviceId"] = service_id
            base["message"] = live.get("error") or "Could not read the controller service after discovery."
            results.append(base)
            continue

        current_props = dict(live.get("properties") or {})
        prior_record = records[0] if records else None
        prior_fingerprint = (prior_record or {}).get("properties_fingerprint")
        desired_fingerprint = _properties_fingerprint(desired)
        needs_update = created or not _visible_properties_match(current_props, desired)
        if prior_fingerprint and prior_fingerprint != desired_fingerprint:
            needs_update = True

        if needs_update:
            changed = [
                key for key, value in desired.items()
                if key not in current_props or not _visible_properties_match(
                    {key: current_props.get(key)}, {key: value}
                )
            ]
            update_result = await update_controller_service_config(endpoint, service_id, desired, **auth)
            if not update_result.get("ok"):
                base.update({"status": "failed", "serviceId": service_id, "message": update_result.get("error") or "Failed to configure the controller service."})
                results.append(base)
                continue
            base["changedProperties"] = sorted(set(changed or desired.keys()))

        # A newly-created service is DISABLED by NiFi, and updating a
        # previously disabled service intentionally leaves it disabled.  The
        # readiness action owns the final state, so explicitly enable it and
        # then poll the asynchronous transition.
        current_state = str(live.get("state") or "").upper()
        if current_state != "ENABLED":
            enabled = await _set_state(endpoint, service_id, "ENABLED", auth)
            if not enabled.get("ok"):
                base.update({"status": "failed", "serviceId": service_id, "message": enabled.get("error") or "NiFi could not enable the controller service."})
                results.append(base)
                continue

        final = await _wait_enabled(endpoint, service_id, auth)
        final_state = str(final.get("state") or "UNKNOWN").upper()
        base["serviceId"] = service_id
        base["state"] = final_state
        base["validationErrors"] = final.get("validation_errors") or []
        if final_state == "ENABLED":
            base["status"] = "created" if created else ("repaired" if needs_update else "healthy")
            base["message"] = "Ready and enabled." if base["status"] == "healthy" else "Configuration reconciled and enabled."
            await _upsert_record(db, nifi_connection, root_id, spec, service_id, desired)
        else:
            base["status"] = "failed"
            base["message"] = (
                f"Service did not reach ENABLED (state: {final_state or 'unknown'}). "
                + ("; ".join(str(x) for x in base["validationErrors"]) if base["validationErrors"] else "Check the NiFi bulletin and dependency settings.")
            )
        results.append(base)

    counts = {status: sum(1 for item in results if item["status"] == status) for status in ("healthy", "created", "repaired", "failed", "blocked")}
    failed = counts["failed"] > 0
    return {
        "ok": not failed and counts["blocked"] == 0,
        "connectionId": nifi_connection_id,
        "connectionName": nifi_connection.get("name") or nifi_connection_id,
        "rootProcessGroupId": root_id,
        "services": results,
        "summary": counts,
        "flowScopedServicesUntouched": True,
        "message": (
            "All managed NiFi platform services are ready."
            if not failed and counts["blocked"] == 0
            else "NiFi platform-service readiness needs attention; see the individual service results."
        ),
    }
