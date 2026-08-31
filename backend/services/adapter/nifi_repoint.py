"""Rollback-safe NiFi repointing for the v2 adapter domain.

The normal flow deploy path replaces a process group in place. A connection
migration has different safety requirements: stage every target process
group first, retain every source process group, cut over database provenance
only after the complete target validates, and restore the source on any
failure. Kafka, Kafka Connect, schemas, and topics are not repointed here.
"""
from __future__ import annotations

import logging
import asyncio
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple

from services.connection_fingerprint import probe_nifi_fingerprint
from services.nifi_client import get_nifi_root_process_group_id, nifi_api_request
from services.adapter.common import COLLECTIONS, audit, new_id, now_iso
from services.adapter.deployer import lifecycle, nifi_apply
from services.adapter.naming import tokenize

logger = logging.getLogger(__name__)


class NifiRepointError(RuntimeError):
    pass


def connection_dict(doc: Dict[str, Any]) -> Dict[str, Any]:
    cfg = doc.get("config") or {}
    return {
        "endpoint": str(cfg.get("url") or ""),
        "auth_type": "BASIC" if str(cfg.get("authMode") or "bearer") == "basic" else "BEARER",
        "username": cfg.get("username"),
        "password": cfg.get("password"),
        "token": cfg.get("token"),
    }


def _auth(conn: Dict[str, Any]) -> Dict[str, Any]:
    return {k: conn.get(k) for k in ("auth_type", "username", "password", "token")}


async def _request(conn: Dict[str, Any], method: str, path: str, **kwargs: Any) -> Dict[str, Any]:
    return await nifi_api_request(conn["endpoint"], method, path, **_auth(conn), **kwargs)


async def _root_services(conn: Dict[str, Any]) -> Tuple[str, List[Dict[str, Any]]]:
    root_id = await get_nifi_root_process_group_id(conn["endpoint"], **_auth(conn))
    if not root_id:
        raise NifiRepointError("Could not resolve the NiFi root process group.")
    listed = await _request(conn, "GET", f"/nifi-api/flow/process-groups/{root_id}/controller-services")
    if not listed.get("ok"):
        raise NifiRepointError(listed.get("error") or "Could not list root controller services.")
    services: List[Dict[str, Any]] = []
    for entity in (listed.get("data") or {}).get("controllerServices") or []:
        component = entity.get("component") or {}
        # The endpoint can include inherited services. Only copy services
        # physically owned by the source root.
        owner = component.get("parentGroupId") or component.get("processGroupId") or component.get("groupId")
        if owner and owner != root_id:
            continue
        service_id = component.get("id") or entity.get("id")
        detail = await _request(conn, "GET", f"/nifi-api/controller-services/{service_id}")
        if not detail.get("ok"):
            raise NifiRepointError(detail.get("error") or f"Could not read controller service {service_id}.")
        services.append(detail.get("data") or {})
    return root_id, services


async def _target_root_groups(conn: Dict[str, Any], root_id: str) -> List[Dict[str, Any]]:
    response = await _request(conn, "GET", f"/nifi-api/flow/process-groups/{root_id}")
    if not response.get("ok"):
        raise NifiRepointError(response.get("error") or "Could not inspect the target NiFi root.")
    flow = ((response.get("data") or {}).get("processGroupFlow") or {}).get("flow") or {}
    return [entity.get("component") or {} for entity in flow.get("processGroups") or []]


async def _clone_root_services(
    source: Dict[str, Any], target: Dict[str, Any]
) -> Tuple[List[str], List[Dict[str, Any]]]:
    """Clone root-owned controller services and prove they enable VALID."""
    _, source_entities = await _root_services(source)
    target_root, target_entities = await _root_services(target)
    existing = {
        ((e.get("component") or {}).get("name"), (e.get("component") or {}).get("type")): e
        for e in target_entities
    }
    created_ids: List[str] = []
    results: List[Dict[str, Any]] = []
    for entity in source_entities:
        component = entity.get("component") or {}
        name, type_ = component.get("name"), component.get("type")
        match = existing.get((name, type_))
        if match:
            target_id = (match.get("component") or {}).get("id") or match.get("id")
        else:
            descriptors = component.get("descriptors") or {}
            properties = {
                key: value
                for key, value in (component.get("properties") or {}).items()
                if value is not None and not bool((descriptors.get(key) or {}).get("sensitive"))
            }
            body: Dict[str, Any] = {
                "revision": {"version": 0},
                "component": {"name": name, "type": type_, "properties": properties},
            }
            if component.get("bundle"):
                body["component"]["bundle"] = component["bundle"]
            made = await _request(
                target,
                "POST",
                f"/nifi-api/process-groups/{target_root}/controller-services",
                json_body=body,
            )
            if not made.get("ok"):
                raise NifiRepointError(f"Could not create root controller service '{name}': {made.get('error')}")
            made_data = made.get("data") or {}
            target_id = made_data.get("id") or (made_data.get("component") or {}).get("id")
            if not target_id:
                raise NifiRepointError(f"NiFi created root controller service '{name}' without returning its id.")
            created_ids.append(target_id)

        current = await _request(target, "GET", f"/nifi-api/controller-services/{target_id}")
        if not current.get("ok"):
            raise NifiRepointError(current.get("error") or f"Could not verify root controller service '{name}'.")
        data = current.get("data") or {}
        revision = ((data.get("revision") or {}).get("version", 0))
        state = ((data.get("component") or {}).get("state") or "")
        if state != "ENABLED":
            enabled = await _request(
                target,
                "PUT",
                f"/nifi-api/controller-services/{target_id}/run-status",
                json_body={"revision": {"version": revision}, "state": "ENABLED", "disconnectedNodeAcknowledged": False},
            )
            if not enabled.get("ok"):
                raise NifiRepointError(f"Could not enable root controller service '{name}': {enabled.get('error')}")

        valid = False
        last_component: Dict[str, Any] = {}
        for _ in range(30):
            check = await _request(target, "GET", f"/nifi-api/controller-services/{target_id}")
            last_component = (check.get("data") or {}).get("component") or {} if check.get("ok") else {}
            if last_component.get("state") == "ENABLED" and last_component.get("validationStatus") == "VALID":
                valid = True
                break
            import asyncio
            await asyncio.sleep(1)
        if not valid:
            errors = last_component.get("validationErrors") or []
            raise NifiRepointError(f"Root controller service '{name}' is not enabled and valid: {errors}")
        results.append({"id": target_id, "name": name, "state": "ENABLED", "validationStatus": "VALID"})
    return created_ids, results


async def _delete_controller_services(conn: Dict[str, Any], ids: List[str]) -> None:
    for service_id in reversed(ids):
        try:
            current = await _request(conn, "GET", f"/nifi-api/controller-services/{service_id}")
            if not current.get("ok"):
                continue
            revision = ((current.get("data") or {}).get("revision") or {}).get("version", 0)
            component = (current.get("data") or {}).get("component") or {}
            if component.get("state") != "DISABLED":
                disabled = await _request(
                    conn,
                    "PUT",
                    f"/nifi-api/controller-services/{service_id}/run-status",
                    json_body={"revision": {"version": revision}, "state": "DISABLED", "disconnectedNodeAcknowledged": False},
                )
                if disabled.get("ok"):
                    current = await _request(conn, "GET", f"/nifi-api/controller-services/{service_id}")
                    revision = ((current.get("data") or {}).get("revision") or {}).get("version", revision)
            await _request(
                conn,
                "DELETE",
                f"/nifi-api/controller-services/{service_id}",
                params={"version": revision, "disconnectedNodeAcknowledged": "false"},
            )
        except Exception:
            logger.exception("Could not clean staged root controller service %s", service_id)


async def _verify_process_group(conn: Dict[str, Any], pg_id: str) -> Dict[str, Any]:
    entity = await _request(conn, "GET", f"/nifi-api/process-groups/{pg_id}")
    if not entity.get("ok"):
        raise NifiRepointError(entity.get("error") or f"Target process group {pg_id} does not exist.")
    status = await _request(conn, "GET", f"/nifi-api/flow/process-groups/{pg_id}/status", params={"recursive": "true"})
    if not status.get("ok"):
        raise NifiRepointError(status.get("error") or f"Could not read target process group {pg_id}.")
    snapshot = (((status.get("data") or {}).get("processGroupStatus") or {}).get("aggregateSnapshot") or {})
    entity_data = entity.get("data") or {}
    invalid = int(entity_data.get("invalidCount") or snapshot.get("invalidCount") or 0)

    # NiFi's aggregate invalidCount is not reliable across versions and can
    # be zero while a nested controller service is INVALID. Walk every child
    # group and prove processors plus group-owned controller services valid.
    queue = [pg_id]
    group_count = 0
    processor_count = 0
    controller_services: List[Dict[str, Any]] = []
    invalid_components: List[str] = []
    while queue:
        current_id = queue.pop(0)
        group_count += 1
        flow_response = await _request(conn, "GET", f"/nifi-api/flow/process-groups/{current_id}")
        if not flow_response.get("ok"):
            raise NifiRepointError(flow_response.get("error") or f"Could not inspect target process group {current_id}.")
        flow = (((flow_response.get("data") or {}).get("processGroupFlow") or {}).get("flow") or {})
        for child in flow.get("processGroups") or []:
            component = child.get("component") or {}
            child_id = component.get("id") or child.get("id")
            if child_id:
                queue.append(child_id)
        for processor in flow.get("processors") or []:
            component = processor.get("component") or {}
            processor_count += 1
            validation_status = str(component.get("validationStatus") or "").upper()
            errors = component.get("validationErrors") or []
            if validation_status == "INVALID" or errors:
                invalid_components.append(
                    f"processor {component.get('name') or component.get('id')}: "
                    f"{'; '.join(str(error) for error in errors) or validation_status}"
                )

        services_response = await _request(
            conn, "GET", f"/nifi-api/flow/process-groups/{current_id}/controller-services"
        )
        if not services_response.get("ok"):
            raise NifiRepointError(
                services_response.get("error") or f"Could not inspect controller services in {current_id}."
            )
        for service in (services_response.get("data") or {}).get("controllerServices") or []:
            component = service.get("component") or {}
            owner = component.get("parentGroupId") or component.get("processGroupId") or component.get("groupId")
            if owner and owner != current_id:
                continue
            service_id = component.get("id") or service.get("id")
            detail_response = await _request(conn, "GET", f"/nifi-api/controller-services/{service_id}")
            if not detail_response.get("ok"):
                raise NifiRepointError(
                    detail_response.get("error") or f"Could not inspect controller service {service_id}."
                )
            detail = (detail_response.get("data") or {}).get("component") or {}
            summary = {
                "id": service_id,
                "name": detail.get("name"),
                "type": str(detail.get("type") or "").rsplit(".", 1)[-1],
                "state": detail.get("state"),
                "validationStatus": detail.get("validationStatus"),
            }
            controller_services.append(summary)
            errors = detail.get("validationErrors") or []
            if detail.get("state") != "ENABLED" or detail.get("validationStatus") != "VALID" or errors:
                invalid_components.append(
                    f"controller service {detail.get('name') or service_id}: "
                    f"{'; '.join(str(error) for error in errors) or detail.get('validationStatus') or detail.get('state')}"
                )

    if invalid > 0 or invalid_components:
        detail = "; ".join(invalid_components[:10])
        if len(invalid_components) > 10:
            detail += f"; and {len(invalid_components) - 10} more"
        raise NifiRepointError(
            f"Target process group {pg_id} has invalid component(s)"
            + (f": {detail}" if detail else f" ({invalid} reported by NiFi).")
        )
    return {
        "processGroupId": pg_id,
        "running": int(entity_data.get("runningCount") or snapshot.get("runningCount") or 0),
        "stopped": int(entity_data.get("stoppedCount") or snapshot.get("stoppedCount") or 0),
        "invalid": invalid,
        "queued": int(snapshot.get("flowFilesQueued") or 0),
        "activeThreads": int(snapshot.get("activeThreadCount") or 0),
        "processGroups": group_count,
        "processors": processor_count,
        "controllerServiceCount": len(controller_services),
        "controllerServices": controller_services,
    }


async def _cleanup_staged(target: Dict[str, Any], staged: List[lifecycle.StagedNifiDeployment], service_ids: List[str]) -> None:
    for item in reversed(staged):
        await nifi_apply.delete_flow_pg(target, item.update.get("nifiProcessGroupId"))
        if item.parameter_context_id and item.parameter_context_created:
            try:
                current = await _request(target, "GET", f"/nifi-api/parameter-contexts/{item.parameter_context_id}")
                if current.get("ok"):
                    revision = ((current.get("data") or {}).get("revision") or {}).get("version", 0)
                    await _request(
                        target,
                        "DELETE",
                        f"/nifi-api/parameter-contexts/{item.parameter_context_id}",
                        params={"version": revision, "disconnectedNodeAcknowledged": "false"},
                    )
            except Exception:
                logger.exception("Could not clean staged parameter context %s", item.parameter_context_id)
    await _delete_controller_services(target, service_ids)


async def _rename_process_group(conn: Dict[str, Any], pg_id: str, name: str) -> None:
    current = await _request(conn, "GET", f"/nifi-api/process-groups/{pg_id}")
    if not current.get("ok"):
        raise NifiRepointError(current.get("error") or f"Could not read process group {pg_id} before renaming it.")
    data = current.get("data") or {}
    revision = data.get("revision") or {"version": 0}
    renamed = await _request(
        conn,
        "PUT",
        f"/nifi-api/process-groups/{pg_id}",
        json_body={"revision": revision, "component": {"id": pg_id, "name": name}},
    )
    if not renamed.get("ok"):
        raise NifiRepointError(renamed.get("error") or f"Could not rename process group {pg_id}.")


async def adopt(db: Any, target_doc: Dict[str, Any]) -> Dict[str, Any]:
    """Rebind only when both cards identify the exact same NiFi instance."""
    old = await db[COLLECTIONS.connections].find_one(
        {"type": "nifi", "active": True, "id": {"$ne": target_doc["id"]}}, {"_id": 0}
    )
    if not old:
        return {"mode": "adopt", "targetConnectionId": target_doc["id"], "flowCount": 0}
    source_probe = await probe_nifi_fingerprint(connection_dict(old))
    target_probe = await probe_nifi_fingerprint(connection_dict(target_doc))
    if not source_probe.get("ok") or not target_probe.get("ok"):
        raise NifiRepointError("Both NiFi connections must be reachable and identifiable before adoption.")
    if source_probe.get("fingerprint") != target_probe.get("fingerprint"):
        raise NifiRepointError("Adopt is only safe when both connections point to the same NiFi instance. Use Migrate for a different NiFi.")
    flows = await db[COLLECTIONS.flows].find({"deployedAt": {"$ne": None}}, {"_id": 0}).to_list(None)
    target = connection_dict(target_doc)
    for flow in flows:
        check = await _request(target, "GET", f"/nifi-api/process-groups/{flow.get('nifiProcessGroupId')}")
        if not check.get("ok"):
            raise NifiRepointError(f"Cannot adopt: flow '{flow.get('name')}' is missing from the target endpoint.")
    await db[COLLECTIONS.connections].update_many({"type": "nifi"}, {"$set": {"active": False}})
    await db[COLLECTIONS.connections].update_one({"id": target_doc["id"]}, {"$set": {"active": True}})
    for flow in flows:
        provenance = deepcopy(flow.get("provenance") or {})
        provenance["nifi"] = {"connectionId": target_doc["id"], "fingerprint": target_probe.get("fingerprint")}
        await db[COLLECTIONS.flows].update_one({"id": flow["id"]}, {"$set": {"provenance": provenance, "updatedAt": now_iso()}})
    await audit(db, "NiFi connection adopted", target_doc.get("name") or target_doc["id"], object="Platform Connection", details=f"Verified and rebound {len(flows)} deployed flow(s).")
    return {"mode": "adopt", "targetConnectionId": target_doc["id"], "flowCount": len(flows), "fingerprint": target_probe.get("fingerprint")}


async def migrate(db: Any, target_doc: Dict[str, Any]) -> Dict[str, Any]:
    old = await db[COLLECTIONS.connections].find_one(
        {"type": "nifi", "active": True, "id": {"$ne": target_doc["id"]}}, {"_id": 0}
    )
    if not old:
        raise NifiRepointError("There is no different active NiFi connection to migrate from.")
    source, target = connection_dict(old), connection_dict(target_doc)
    source_probe = await probe_nifi_fingerprint(source)
    target_probe = await probe_nifi_fingerprint(target)
    if not source_probe.get("ok") or not target_probe.get("ok"):
        raise NifiRepointError("Both source and target NiFi connections must be reachable and identifiable.")
    if source_probe.get("fingerprint") == target_probe.get("fingerprint"):
        raise NifiRepointError("Both cards point to the same NiFi instance. Use Adopt instead of Migrate.")

    active_jobs = await db[COLLECTIONS.bulk_jobs].find(
        {"status": {"$in": ["queued", "pending", "running"]}}, {"_id": 0, "id": 1}
    ).to_list(1)
    if active_jobs:
        raise NifiRepointError("Wait for the flow operation queue to finish before repointing NiFi.")
    flows = await db[COLLECTIONS.flows].find({"deployedAt": {"$ne": None}}, {"_id": 0}).to_list(None)
    if any(flow.get("migrationLock") for flow in flows):
        raise NifiRepointError("Another NiFi migration is already in progress.")

    target_root = await get_nifi_root_process_group_id(target["endpoint"], **_auth(target))
    if not target_root:
        raise NifiRepointError("Could not resolve the target NiFi root process group.")
    target_groups = await _target_root_groups(target, target_root)
    target_groups_by_name: Dict[str, List[Dict[str, Any]]] = {}
    for group in target_groups:
        target_groups_by_name.setdefault(str(group.get("name") or ""), []).append(group)

    # A previous successful migration intentionally retains the old target
    # process group as rollback insurance. That retained copy must not make
    # a later migration back impossible. Only a group whose id is recorded
    # in this flow's rollback metadata is considered managed/replacable;
    # an unrelated same-named group remains a hard collision.
    replace_after_commit: Dict[str, str] = {}
    unmanaged_collisions: List[str] = []
    for flow in flows:
        canonical_name = tokenize(str(flow.get("name") or ""))
        matches = target_groups_by_name.get(canonical_name) or []
        if not matches:
            continue
        rollback = flow.get("nifiMigrationRollback") or {}
        retained_id = rollback.get("processGroupId") if rollback.get("connectionId") == target_doc["id"] else None
        managed = next((g for g in matches if (g.get("id") or "") == retained_id), None)
        if managed and len(matches) == 1:
            replace_after_commit[flow["id"]] = str(retained_id)
        else:
            unmanaged_collisions.append(canonical_name)
    if unmanaged_collisions:
        raise NifiRepointError(
            "Target NiFi has process-group name(s) that are not a recorded rollback copy: "
            + ", ".join(sorted(set(unmanaged_collisions)))
            + ". Rename or remove those unmanaged groups before migration."
        )

    migration_id = new_id("nifi-migration")
    lock = {"id": migration_id, "sourceConnectionId": old["id"], "targetConnectionId": target_doc["id"], "startedAt": now_iso()}
    flow_ids = [flow["id"] for flow in flows]
    claimed_source = await db[COLLECTIONS.connections].update_one(
        {"id": old["id"], "repointInProgress": None}, {"$set": {"repointInProgress": lock}}
    )
    if getattr(claimed_source, "modified_count", 0) != 1:
        raise NifiRepointError("Another NiFi migration is already in progress.")
    claimed_target = await db[COLLECTIONS.connections].update_one(
        {"id": target_doc["id"], "repointInProgress": None}, {"$set": {"repointInProgress": lock}}
    )
    if getattr(claimed_target, "modified_count", 0) != 1:
        await db[COLLECTIONS.connections].update_one({"id": old["id"]}, {"$unset": {"repointInProgress": ""}})
        raise NifiRepointError("The target NiFi is already part of another migration.")
    await db[COLLECTIONS.flows].update_many({"id": {"$in": flow_ids}}, {"$set": {"migrationLock": lock}})

    staged: List[lifecycle.StagedNifiDeployment] = []
    created_services: List[str] = []
    stopped_source: List[Dict[str, Any]] = []
    source_presence: Dict[str, bool] = {}
    committed = False
    try:
        # Managed flows own their controller services inside their block
        # groups. Recompiling from the platform's canonical Kafka, Redis and
        # schema-registry connection records is both complete and safe for
        # sensitive values; copying NiFi GET responses would lose every
        # masked secret and preserve source-only controller-service ids.
        for flow in flows:
            source_pg_id = flow.get("nifiProcessGroupId")
            if not source_pg_id:
                source_presence[flow["id"]] = False
                continue
            source_check = await _request(source, "GET", f"/nifi-api/process-groups/{source_pg_id}")
            source_presence[flow["id"]] = bool(source_check.get("ok"))

        for flow in flows:
            canonical_name = tokenize(str(flow.get("name") or ""))
            staging_name = f"{canonical_name}__migration_{migration_id.rsplit('-', 1)[-1]}"
            staged.append(
                await lifecycle.stage_nifi_migration(
                    db,
                    flow,
                    target_doc,
                    staged_group_name=staging_name,
                )
            )
            await _verify_process_group(target, staged[-1].update["nifiProcessGroupId"])

        service_results = [
            service
            for verification in [
                await _verify_process_group(target, item.update["nifiProcessGroupId"])
                for item in staged
            ]
            for service in verification.get("controllerServices") or []
        ]

        # Quiesce only after the complete target is ready, minimizing the
        # interruption window and ensuring no source/target double ingestion.
        for flow in flows:
            if flow.get("state") in ("Running", "Paused") and source_presence.get(flow["id"]):
                stopped = await nifi_apply.stop_pg(source, flow["nifiProcessGroupId"])
                if not stopped.get("ok"):
                    raise NifiRepointError(f"Could not quiesce source flow '{flow.get('name')}': {stopped.get('error')}")
                stopped_source.append(flow)

        for item, original in zip(staged, flows):
            update = deepcopy(item.update)
            update["state"] = "Stopped"
            if source_presence.get(original["id"]):
                update["nifiMigrationRollback"] = {
                    "migrationId": migration_id,
                    "connectionId": old["id"],
                    "processGroupId": original.get("nifiProcessGroupId"),
                    "state": original.get("state"),
                    "runtimeScopeMap": deepcopy(original.get("runtimeScopeMap")),
                    "provenance": deepcopy(original.get("provenance")),
                    "deployedAt": original.get("deployedAt"),
                    "migratedAt": now_iso(),
                }
            else:
                update.pop("nifiMigrationRollback", None)
            update.pop("migrationLock", None)
            unset_fields = {"migrationLock": ""}
            if not source_presence.get(original["id"]):
                unset_fields["nifiMigrationRollback"] = ""
            await db[COLLECTIONS.flows].update_one(
                {"id": item.flow_id}, {"$set": update, "$unset": unset_fields}
            )
        await db[COLLECTIONS.connections].update_many({"type": "nifi"}, {"$set": {"active": False}})
        await db[COLLECTIONS.connections].update_one(
            {"id": target_doc["id"]},
            {"$set": {"active": True, "health": "Healthy", "reachability": "Reachable", "lastTestedAt": now_iso()}},
        )
        committed = True

        for item, original in zip(staged, flows):
            desired = original.get("state")
            if desired == "Running":
                started = await nifi_apply.start_pg(target, item.update["nifiProcessGroupId"])
                if not started.get("ok"):
                    raise NifiRepointError(f"Could not restore running flow '{item.flow_name}' on target: {started.get('error')}")
                await db[COLLECTIONS.flows].update_one({"id": item.flow_id}, {"$set": {"state": "Running", "updatedAt": now_iso()}})
            elif desired == "Paused":
                started = await nifi_apply.start_pg(target, item.update["nifiProcessGroupId"])
                if not started.get("ok"):
                    raise NifiRepointError(f"Could not restore paused flow '{item.flow_name}' on target: {started.get('error')}")
                migrated_doc = {**original, **item.update}
                trigger_ids = lifecycle._trigger_component_ids(lifecycle.Flow(**migrated_doc), migrated_doc)
                paused = await nifi_apply.pause_trigger(target, trigger_ids)
                if not paused.get("ok"):
                    raise NifiRepointError(f"Could not restore paused flow '{item.flow_name}' on target.")
                await db[COLLECTIONS.flows].update_one({"id": item.flow_id}, {"$set": {"state": "Paused", "updatedAt": now_iso()}})

        verification = [await _verify_process_group(target, item.update["nifiProcessGroupId"]) for item in staged]

        finalization_warnings: List[str] = []
        for item in staged:
            canonical_name = tokenize(item.flow_name)
            try:
                await _rename_process_group(target, item.update["nifiProcessGroupId"], canonical_name)
                retained_id = replace_after_commit.get(item.flow_id)
                if retained_id and retained_id != item.update["nifiProcessGroupId"]:
                    await nifi_apply.delete_flow_pg(target, retained_id)
            except Exception as finalize_exc:
                # The staged group is already active, valid, and referenced
                # by Mongo. A cosmetic rename/old-copy cleanup failure must
                # not roll back a healthy migration.
                logger.exception("Could not finalize target group name/rollback cleanup for %s", item.flow_name)
                finalization_warnings.append(f"{item.flow_name}: {finalize_exc}")
        await audit(
            db,
            "NiFi connection migrated",
            target_doc.get("name") or target_doc["id"],
            object="Platform Connection",
            details=(
                f"Migrated and verified {len(staged)} flow(s) and {len(service_results)} flow-scoped controller service(s); "
                f"source resources retained for rollback where present."
                + (f" Finalization warnings: {'; '.join(finalization_warnings)}" if finalization_warnings else "")
            ),
        )
        return {
            "mode": "migrate",
            "migrationId": migration_id,
            "sourceConnectionId": old["id"],
            "targetConnectionId": target_doc["id"],
            "sourceFingerprint": source_probe.get("fingerprint"),
            "targetFingerprint": target_probe.get("fingerprint"),
            "flowCount": len(staged),
            "controllerServices": service_results,
            "flows": verification,
            "rollbackRetained": all(source_presence.values()) if flows else False,
            "sourceRuntimeMissing": [flow.get("name") for flow in flows if not source_presence.get(flow["id"])],
            "finalizationWarnings": finalization_warnings,
        }
    except Exception as exc:
        logger.exception("NiFi migration %s failed; restoring source", migration_id)
        if committed:
            for original in flows:
                restore = {
                    key: deepcopy(original.get(key))
                    for key in ("runtimeScopeMap", "nifiProcessGroupId", "provenance", "state", "deployedAt", "servicePins", "updatedAt", "blocks")
                    if key in original
                }
                await db[COLLECTIONS.flows].update_one(
                    {"id": original["id"]}, {"$set": restore, "$unset": {"migrationLock": "", "nifiMigrationRollback": ""}}
                )
            await db[COLLECTIONS.connections].update_many({"type": "nifi"}, {"$set": {"active": False}})
            await db[COLLECTIONS.connections].update_one({"id": old["id"]}, {"$set": {"active": True}})
        for flow in stopped_source:
            if flow.get("state") in ("Running", "Paused") and source_presence.get(flow["id"]):
                await nifi_apply.start_pg(source, flow["nifiProcessGroupId"])
        await _cleanup_staged(target, staged, created_services)
        await audit(db, "NiFi migration failed", target_doc.get("name") or target_doc["id"], status="Failed", object="Platform Connection", details=str(exc)[:500])
        if isinstance(exc, NifiRepointError):
            raise
        raise NifiRepointError(str(exc)) from exc
    finally:
        await db[COLLECTIONS.flows].update_many({"id": {"$in": flow_ids}}, {"$unset": {"migrationLock": ""}})
        await db[COLLECTIONS.connections].update_many(
            {"id": {"$in": [old["id"], target_doc["id"]]}}, {"$unset": {"repointInProgress": ""}}
        )
