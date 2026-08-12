"""NiFi Flow Manager — Phase 4.

Start, stop, and retrieve metrics for NiFi process groups.

Public functions:
    start_process_group(url, process_group_id, auth_type, username, password)
    stop_process_group(url, process_group_id, auth_type, username, password)
    get_process_group_metrics(url, process_group_id, auth_type, username, password)
"""
import asyncio
import logging
from typing import Any, Dict, Optional, List, Tuple, Set

from services.nifi_client import nifi_api_request

logger = logging.getLogger(__name__)


async def start_process_group(
    url: str,
    process_group_id: str,
    auth_type: str = "NONE",
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    """Schedule all processors in a process group to RUNNING state.

    NiFi 2.x endpoint: PUT /nifi-api/flow/process-groups/{id}
    Body: {"id": "...", "state": "RUNNING", "disconnectedNodeAcknowledged": false}
    """
    nifi_url = url.rstrip("/")
    body = {
        "id": process_group_id,
        "state": "RUNNING",
        "disconnectedNodeAcknowledged": False,
    }
    result = await nifi_api_request(
        nifi_url, "PUT",
        f"/nifi-api/flow/process-groups/{process_group_id}",
        auth_type=auth_type, username=username, password=password, token=token,
        json_body=body,
    )
    if result.get("ok"):
        logger.info(f"Started process group {process_group_id}")
        return {"ok": True, "state": "Running"}
    err = result.get("error", "NiFi start failed")
    logger.warning(f"Failed to start process group {process_group_id}: {err}")
    return {"ok": False, "error": err}


async def _refresh_revision(
    url: str,
    component_type: str,  # "processors" | "controller-services"
    component_id: str,
    auth_type: str,
    username: Optional[str],
    password: Optional[str],
    token: Optional[str],
) -> Optional[int]:
    r = await nifi_api_request(
        url, "GET",
        f"/nifi-api/{component_type}/{component_id}",
        auth_type=auth_type, username=username, password=password, token=token,
    )
    if not r.get("ok"):
        return None
    return (r.get("data") or {}).get("revision", {}).get("version", 0)


async def _set_component_state(
    url: str,
    component_type: str,  # "processors" | "controller-services"
    component_id: str,
    revision: int,
    target_state: str,
    auth_type: str,
    username: Optional[str],
    password: Optional[str],
    token: Optional[str] = None,
) -> bool:
    path = (
        f"/nifi-api/processors/{component_id}/run-status"
        if component_type == "processors"
        else f"/nifi-api/controller-services/{component_id}/run-status"
    )
    payload = {
        "revision": {"version": revision},
        "state": target_state,
        "disconnectedNodeAcknowledged": False,
    }
    first = await nifi_api_request(
        url, "PUT", path,
        auth_type=auth_type, username=username, password=password, token=token,
        json_body=payload,
    )
    if first.get("ok"):
        return True

    # Retry once with refreshed revision (avoids 409 revision conflicts).
    refreshed = await _refresh_revision(url, component_type, component_id, auth_type, username, password, token)
    if refreshed is None:
        return False
    payload["revision"]["version"] = refreshed
    second = await nifi_api_request(
        url, "PUT", path,
        auth_type=auth_type, username=username, password=password, token=token,
        json_body=payload,
    )
    return bool(second.get("ok"))


async def _list_pg_flow(
    url: str,
    process_group_id: str,
    auth_type: str,
    username: Optional[str],
    password: Optional[str],
    token: Optional[str],
) -> Optional[Dict[str, Any]]:
    r = await nifi_api_request(
        url, "GET",
        f"/nifi-api/flow/process-groups/{process_group_id}",
        auth_type=auth_type, username=username, password=password, token=token,
    )
    if not r.get("ok"):
        return None
    return ((r.get("data") or {}).get("processGroupFlow") or {}).get("flow") or {}


async def _collect_pg_processors(
    url: str,
    process_group_id: str,
    auth_type: str,
    username: Optional[str],
    password: Optional[str],
    token: Optional[str],
    seen: Optional[Set[str]] = None,
) -> List[Tuple[str, int, str]]:
    seen = seen or set()
    if process_group_id in seen:
        return []
    seen.add(process_group_id)

    flow = await _list_pg_flow(url, process_group_id, auth_type, username, password, token)
    if flow is None:
        return []

    rows: List[Tuple[str, int, str]] = []
    for p in (flow.get("processors") or []):
        pid = (p.get("component") or {}).get("id") or p.get("id")
        rev = (p.get("revision") or {}).get("version", 0)
        run_status = (p.get("component") or {}).get("state") or ""
        if pid:
            rows.append((pid, rev, str(run_status)))

    for child in (flow.get("processGroups") or []):
        cid = (child.get("component") or {}).get("id") or child.get("id")
        if cid:
            rows.extend(
                await _collect_pg_processors(url, cid, auth_type, username, password, token, seen)
            )
    return rows


async def _collect_pg_controller_services(
    url: str,
    process_group_id: str,
    auth_type: str,
    username: Optional[str],
    password: Optional[str],
    token: Optional[str],
) -> List[Tuple[str, int, str]]:
    r = await nifi_api_request(
        url, "GET",
        f"/nifi-api/flow/process-groups/{process_group_id}/controller-services",
        auth_type=auth_type, username=username, password=password, token=token,
    )
    if not r.get("ok"):
        return []
    rows: List[Tuple[str, int, str]] = []
    for cs in ((r.get("data") or {}).get("controllerServices") or []):
        component = cs.get("component") or {}
        service_pg_id = component.get("parentGroupId") or component.get("processGroupId") or component.get("groupId")
        if service_pg_id and service_pg_id != process_group_id:
            continue
        cid = component.get("id") or cs.get("id")
        rev = (cs.get("revision") or {}).get("version", 0)
        state = component.get("state") or ""
        if cid:
            rows.append((cid, rev, str(state)))
    return rows


async def _collect_pg_connection_ids(
    url: str,
    process_group_id: str,
    auth_type: str,
    username: Optional[str],
    password: Optional[str],
    token: Optional[str],
    seen: Optional[Set[str]] = None,
) -> List[str]:
    seen = seen or set()
    if process_group_id in seen:
        return []
    seen.add(process_group_id)

    flow = await _list_pg_flow(url, process_group_id, auth_type, username, password, token)
    if flow is None:
        return []

    connection_ids: List[str] = []
    for connection in (flow.get("connections") or []):
        cid = (connection.get("component") or {}).get("id") or connection.get("id")
        if cid:
            connection_ids.append(cid)

    for child in (flow.get("processGroups") or []):
        child_id = (child.get("component") or {}).get("id") or child.get("id")
        if child_id:
            connection_ids.extend(
                await _collect_pg_connection_ids(url, child_id, auth_type, username, password, token, seen)
            )
    return connection_ids


async def _drop_connection_queue(
    url: str,
    connection_id: str,
    auth_type: str,
    username: Optional[str],
    password: Optional[str],
    token: Optional[str],
    *,
    timeout_seconds: float = 30.0,
) -> Dict[str, Any]:
    create = await nifi_api_request(
        url, "POST",
        f"/nifi-api/flowfile-queues/{connection_id}/drop-requests",
        auth_type=auth_type, username=username, password=password, token=token,
        json_body={},
    )
    if not create.get("ok"):
        return {"ok": False, "connection_id": connection_id, "error": create.get("error") or "Failed to create drop request."}

    drop_data = create.get("data") or {}
    drop_request = drop_data.get("dropRequest") or drop_data
    request_id = drop_request.get("id")
    if not request_id:
        return {"ok": False, "connection_id": connection_id, "error": "NiFi did not return a drop request id."}

    deadline = asyncio.get_event_loop().time() + timeout_seconds
    latest = drop_request
    while asyncio.get_event_loop().time() < deadline:
        latest_result = await nifi_api_request(
            url, "GET",
            f"/nifi-api/flowfile-queues/{connection_id}/drop-requests/{request_id}",
            auth_type=auth_type, username=username, password=password, token=token,
        )
        if latest_result.get("ok"):
            latest_data = latest_result.get("data") or {}
            latest = latest_data.get("dropRequest") or latest_data
            if bool(latest.get("finished")):
                break
        await asyncio.sleep(0.5)

    cleanup = await nifi_api_request(
        url, "DELETE",
        f"/nifi-api/flowfile-queues/{connection_id}/drop-requests/{request_id}",
        auth_type=auth_type, username=username, password=password, token=token,
    )
    if not cleanup.get("ok"):
        logger.warning("Failed to clean up queue drop request %s for connection %s: %s", request_id, connection_id, cleanup.get("error"))

    dropped = (
        latest.get("droppedCount")
        or latest.get("dropped")
        or latest.get("currentCount")
        or 0
    )
    return {
        "ok": bool(latest.get("finished", True)),
        "connection_id": connection_id,
        "drop_request_id": request_id,
        "dropped": dropped,
    }


async def clear_process_group_queues(
    url: str,
    process_group_id: str,
    auth_type: str = "NONE",
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    """Drop queued FlowFiles for every connection in a process group."""
    nifi_url = url.rstrip("/")
    connection_ids = await _collect_pg_connection_ids(nifi_url, process_group_id, auth_type, username, password, token)
    dropped = 0
    failed: List[Dict[str, Any]] = []
    for connection_id in connection_ids:
        result = await _drop_connection_queue(
            nifi_url,
            connection_id,
            auth_type,
            username,
            password,
            token,
        )
        if result.get("ok"):
            try:
                dropped += int(result.get("dropped") or 0)
            except Exception:
                pass
        else:
            failed.append(result)

    return {
        "ok": not failed,
        "connections": len(connection_ids),
        "dropped": dropped,
        "failed": failed,
    }


async def stop_and_clear_process_group(
    url: str,
    process_group_id: str,
    auth_type: str = "NONE",
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    """Stop processors, then drop all queued FlowFiles for the process group."""
    stop_result = await stop_process_group(url, process_group_id, auth_type, username, password, token)
    if not stop_result.get("ok"):
        return stop_result

    # Give NiFi a moment to unschedule processors before dropping queues.
    await asyncio.sleep(1)
    clear_result = await clear_process_group_queues(url, process_group_id, auth_type, username, password, token)
    if not clear_result.get("ok"):
        return {
            "ok": False,
            "state": "Stopped",
            "error": "Flow stopped, but one or more queues could not be cleared.",
            "queue_clear": clear_result,
        }
    return {"ok": True, "state": "Stopped", "queue_clear": clear_result}


async def deactivate_process_group(
    url: str,
    process_group_id: str,
    auth_type: str = "NONE",
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Stop a process group, then disable all processors and controller services in it.
    """
    stop_result = await stop_process_group(url, process_group_id, auth_type, username, password, token)

    nifi_url = url.rstrip("/")
    processors = await _collect_pg_processors(nifi_url, process_group_id, auth_type, username, password, token)
    disabled_proc = 0
    for pid, rev, state in processors:
        if state.lower() == "disabled":
            continue
        if await _set_component_state(
            nifi_url, "processors", pid, rev, "DISABLED",
            auth_type, username, password, token,
        ):
            disabled_proc += 1

    # Give NiFi a short moment before disabling CSes that may still be in transition.
    await asyncio.sleep(1)
    services = await _collect_pg_controller_services(nifi_url, process_group_id, auth_type, username, password, token)
    disabled_cs = 0
    for cid, rev, state in services:
        if state.upper() == "DISABLED":
            continue
        if await _set_component_state(
            nifi_url, "controller-services", cid, rev, "DISABLED",
            auth_type, username, password, token,
        ):
            disabled_cs += 1

    return {
        "ok": bool(stop_result.get("ok", False)),
        "disabled_processors": disabled_proc,
        "disabled_controller_services": disabled_cs,
    }


async def prepare_process_group_for_start(
    url: str,
    process_group_id: str,
    auth_type: str = "NONE",
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Re-enable components previously disabled, so the process group can start.
    """
    nifi_url = url.rstrip("/")

    # Enable controller services first (dependencies among services can require multiple passes).
    enabled_cs = 0
    for _ in range(3):
        changed = 0
        services = await _collect_pg_controller_services(nifi_url, process_group_id, auth_type, username, password, token)
        for cid, rev, state in services:
            if state.upper() == "ENABLED":
                continue
            ok = await _set_component_state(
                nifi_url, "controller-services", cid, rev, "ENABLED",
                auth_type, username, password, token,
            )
            if ok:
                changed += 1
                enabled_cs += 1
        if changed == 0:
            break
        await asyncio.sleep(1)

    # Enable processors to STOPPED state (enabled but not yet running).
    processors = await _collect_pg_processors(nifi_url, process_group_id, auth_type, username, password, token)
    enabled_proc = 0
    for pid, rev, state in processors:
        if state.lower() != "disabled":
            continue
        ok = await _set_component_state(
            nifi_url, "processors", pid, rev, "STOPPED",
            auth_type, username, password, token,
        )
        if ok:
            enabled_proc += 1

    return {"ok": True, "enabled_processors": enabled_proc, "enabled_controller_services": enabled_cs}


async def stop_process_group(
    url: str,
    process_group_id: str,
    auth_type: str = "NONE",
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    """Stop all processors in a process group (STOPPED state).

    NiFi 2.x endpoint: PUT /nifi-api/flow/process-groups/{id}
    Body: {"id": "...", "state": "STOPPED", "disconnectedNodeAcknowledged": false}
    """
    nifi_url = url.rstrip("/")
    body = {
        "id": process_group_id,
        "state": "STOPPED",
        "disconnectedNodeAcknowledged": False,
    }
    result = await nifi_api_request(
        nifi_url, "PUT",
        f"/nifi-api/flow/process-groups/{process_group_id}",
        auth_type=auth_type, username=username, password=password, token=token,
        json_body=body,
    )
    if result.get("ok"):
        logger.info(f"Stopped process group {process_group_id}")
        return {"ok": True, "state": "Stopped"}
    err = result.get("error", "NiFi stop failed")
    logger.warning(f"Failed to stop process group {process_group_id}: {err}")
    return {"ok": False, "error": err}


async def get_process_group_metrics(
    url: str,
    process_group_id: str,
    auth_type: str = "NONE",
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    """Fetch throughput / queue / processor metrics for a process group.

    NiFi 2.x endpoint: GET /nifi-api/flow/process-groups/{id}/status
    """
    nifi_url = url.rstrip("/")
    result = await nifi_api_request(
        nifi_url, "GET",
        f"/nifi-api/flow/process-groups/{process_group_id}/status",
        auth_type=auth_type, username=username, password=password, token=token,
    )
    if not result.get("ok"):
        logger.warning(f"Failed to get metrics for {process_group_id}: {result.get('error')}")
        return {
            "throughput": 0,
            "queued": 0,
            "records_processed": 0,
            "kafka_messages": 0,
            "bytes_in": 0,
            "bytes_out": 0,
            "active_threads": 0,
            "processors": [],
        }

    snapshot = (
        result["data"]
        .get("processGroupStatus", {})
        .get("aggregateSnapshot", {})
    )

    # NiFi returns human-readable strings for some fields; parse them if needed
    def _int(val: Any) -> int:
        try:
            return int(val)
        except (TypeError, ValueError):
            return 0

    proc_rows = snapshot.get("processorStatusSnapshots", [])
    processors = []
    for row in proc_rows:
        p = row.get("processorStatusSnapshot", row) if isinstance(row, dict) else {}
        processors.append(
            {
                "id": p.get("id"),
                "name": (p.get("name") or ""),
                "type": (p.get("type") or ""),
                "state": (p.get("runStatus") or p.get("executionStatus") or ""),
                "active_threads": _int(p.get("activeThreadCount", 0)),
                "files_in": _int(p.get("flowFilesIn", 0)),
                "files_out": _int(p.get("flowFilesOut", 0)),
            }
        )

    raw_output = snapshot.get("output")
    output_size = 0
    if isinstance(raw_output, dict):
        output_size = _int(raw_output.get("contentSize", 0))

    return {
        "throughput": _int(snapshot.get("flowFilesPerSecond") or output_size or snapshot.get("flowFilesOut", 0)),
        "queued": _int(snapshot.get("flowFilesQueued", 0)),
        "records_processed": _int(snapshot.get("flowFilesOut", 0)),
        "kafka_messages": _int(snapshot.get("flowFilesOut", 0)),
        "bytes_in": _int(snapshot.get("bytesIn", 0)),
        "bytes_out": _int(snapshot.get("bytesOut", 0)),
        "active_threads": _int(snapshot.get("activeThreadCount", 0)),
        "processors": processors,
    }


async def list_pg_controller_services_detail(
    url: str,
    process_group_id: str,
    auth_type: str = "NONE",
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    """List all controller services in a process group with full component details."""
    nifi_url = url.rstrip("/")
    r = await nifi_api_request(
        nifi_url, "GET",
        f"/nifi-api/flow/process-groups/{process_group_id}/controller-services",
        auth_type=auth_type, username=username, password=password, token=token,
    )
    if not r.get("ok"):
        return {"ok": False, "error": r.get("error", "Failed to fetch controller services"), "services": []}
    services = []
    for cs in ((r.get("data") or {}).get("controllerServices") or []):
        component = cs.get("component") or {}
        services.append({
            "id": component.get("id") or cs.get("id"),
            "name": component.get("name", ""),
            "type": component.get("type", ""),
            "state": component.get("state", ""),
            "validation_errors": component.get("validationErrors") or [],
            "bulletin_level": component.get("bulletinLevel", ""),
            "revision": (cs.get("revision") or {}).get("version", 0),
        })
    return {"ok": True, "services": services}


async def get_controller_service_config(
    url: str,
    service_id: str,
    auth_type: str = "NONE",
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    """Fetch full configuration and property descriptors for a single controller service."""
    nifi_url = url.rstrip("/")
    r = await nifi_api_request(
        nifi_url, "GET",
        f"/nifi-api/controller-services/{service_id}",
        auth_type=auth_type, username=username, password=password, token=token,
    )
    if not r.get("ok"):
        return {"ok": False, "error": r.get("error", "Failed to fetch controller service")}
    data = r.get("data") or {}
    component = data.get("component") or {}
    revision = (data.get("revision") or {}).get("version", 0)
    return {
        "ok": True,
        "id": component.get("id") or service_id,
        "name": component.get("name", ""),
        "type": component.get("type", ""),
        "state": component.get("state", ""),
        "properties": component.get("properties") or {},
        "descriptors": component.get("descriptors") or {},
        "validation_errors": component.get("validationErrors") or [],
        "revision": revision,
    }


async def update_controller_service_config(
    url: str,
    service_id: str,
    properties: Dict[str, Any],
    auth_type: str = "NONE",
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    """Update a controller service's properties.

    NiFi requires DISABLED state before modifying properties.
    Sequence: fetch current state → disable if needed → PUT properties → re-enable if was enabled.
    """
    nifi_url = url.rstrip("/")

    current = await get_controller_service_config(nifi_url, service_id, auth_type, username, password, token)
    if not current.get("ok"):
        return current

    current_state = current.get("state", "")
    current_revision = current.get("revision", 0)
    was_enabled = current_state.upper() in ("ENABLED", "ENABLING")

    if current_state.upper() not in ("DISABLED", "DISABLING"):
        disable_ok = await _set_component_state(
            nifi_url, "controller-services", service_id, current_revision, "DISABLED",
            auth_type, username, password, token,
        )
        if not disable_ok:
            return {"ok": False, "error": "Failed to disable controller service before updating. It may be in use by a running processor."}
        await asyncio.sleep(2)
        refreshed = await _refresh_revision(nifi_url, "controller-services", service_id, auth_type, username, password, token)
        if refreshed is not None:
            current_revision = refreshed

    update_body = {
        "revision": {"version": current_revision},
        "component": {"id": service_id, "properties": properties},
        "disconnectedNodeAcknowledged": False,
    }
    update_result = await nifi_api_request(
        nifi_url, "PUT",
        f"/nifi-api/controller-services/{service_id}",
        auth_type=auth_type, username=username, password=password, token=token,
        json_body=update_body,
    )
    if not update_result.get("ok"):
        return {"ok": False, "error": f"Failed to update controller service: {update_result.get('error', 'Unknown error')}"}

    new_revision = ((update_result.get("data") or {}).get("revision") or {}).get("version", current_revision + 1)

    final_state = "DISABLED"
    if was_enabled:
        enable_ok = await _set_component_state(
            nifi_url, "controller-services", service_id, new_revision, "ENABLED",
            auth_type, username, password, token,
        )
        await asyncio.sleep(1)
        check = await nifi_api_request(
            nifi_url, "GET",
            f"/nifi-api/controller-services/{service_id}",
            auth_type=auth_type, username=username, password=password, token=token,
        )
        if check.get("ok"):
            final_state = ((check.get("data") or {}).get("component") or {}).get("state", "ENABLING")
        else:
            final_state = "ENABLING" if enable_ok else "DISABLED"

    return {
        "ok": True,
        "id": service_id,
        "state": final_state,
        "was_enabled": was_enabled,
        "message": "Configuration updated successfully" + (" — service is re-enabling" if was_enabled and final_state not in ("ENABLED",) else ""),
    }


async def list_pg_processors_detail(
    url: str,
    process_group_id: str,
    auth_type: str = "NONE",
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    """List all processors in a process group tree with full live NiFi details."""
    nifi_url = url.rstrip("/")
    processors: List[Dict[str, Any]] = []
    seen: Set[str] = set()

    async def collect(pg_id: str, group_name: str = "") -> None:
        if pg_id in seen:
            return
        seen.add(pg_id)
        flow = await _list_pg_flow(nifi_url, pg_id, auth_type, username, password, token)
        if flow is None:
            return
        for proc in flow.get("processors") or []:
            component = proc.get("component") or {}
            processors.append({
                "id": component.get("id") or proc.get("id"),
                "name": component.get("name", ""),
                "type": component.get("type", ""),
                "state": component.get("state", ""),
                "validation_errors": component.get("validationErrors") or [],
                "bulletin_level": component.get("bulletinLevel", ""),
                "revision": (proc.get("revision") or {}).get("version", 0),
                "process_group_id": pg_id,
                "process_group_name": group_name,
            })
        for child in flow.get("processGroups") or []:
            child_component = child.get("component") or {}
            child_id = child_component.get("id") or child.get("id")
            if child_id:
                await collect(child_id, child_component.get("name", ""))

    await collect(process_group_id)
    return {"ok": True, "processors": processors}


async def get_processor_config(
    url: str,
    processor_id: str,
    auth_type: str = "NONE",
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    """Fetch full processor configuration and property descriptors."""
    nifi_url = url.rstrip("/")
    r = await nifi_api_request(
        nifi_url, "GET",
        f"/nifi-api/processors/{processor_id}",
        auth_type=auth_type, username=username, password=password, token=token,
    )
    if not r.get("ok"):
        return {"ok": False, "error": r.get("error", "Failed to fetch processor")}
    data = r.get("data") or {}
    component = data.get("component") or {}
    config = component.get("config") or {}
    revision = (data.get("revision") or {}).get("version", 0)
    return {
        "ok": True,
        "id": component.get("id") or processor_id,
        "name": component.get("name", ""),
        "type": component.get("type", ""),
        "state": component.get("state", ""),
        "properties": config.get("properties") or {},
        "descriptors": config.get("descriptors") or component.get("descriptors") or {},
        "validation_errors": component.get("validationErrors") or [],
        "revision": revision,
    }


async def update_processor_config(
    url: str,
    processor_id: str,
    properties: Dict[str, Any],
    auth_type: str = "NONE",
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    """Update processor properties from live NiFi descriptors."""
    nifi_url = url.rstrip("/")
    current = await get_processor_config(nifi_url, processor_id, auth_type, username, password, token)
    if not current.get("ok"):
        return current

    current_state = str(current.get("state") or "")
    current_revision = int(current.get("revision") or 0)
    was_running = current_state.upper() == "RUNNING"

    if was_running:
        stop_ok = await _set_component_state(
            nifi_url, "processors", processor_id, current_revision, "STOPPED",
            auth_type, username, password, token,
        )
        if not stop_ok:
            return {"ok": False, "error": "Failed to stop processor before updating. Stop the flow and try again."}
        await asyncio.sleep(1)
        refreshed = await _refresh_revision(nifi_url, "processors", processor_id, auth_type, username, password, token)
        if refreshed is not None:
            current_revision = refreshed

    update_body = {
        "revision": {"version": current_revision},
        "component": {
            "id": processor_id,
            "config": {"properties": properties},
        },
        "disconnectedNodeAcknowledged": False,
    }
    update_result = await nifi_api_request(
        nifi_url, "PUT",
        f"/nifi-api/processors/{processor_id}",
        auth_type=auth_type, username=username, password=password, token=token,
        json_body=update_body,
    )
    if not update_result.get("ok"):
        return {"ok": False, "error": f"Failed to update processor: {update_result.get('error', 'Unknown error')}"}

    new_revision = ((update_result.get("data") or {}).get("revision") or {}).get("version", current_revision + 1)
    final_state = current_state.upper() or "STOPPED"
    if was_running:
        start_ok = await _set_component_state(
            nifi_url, "processors", processor_id, new_revision, "RUNNING",
            auth_type, username, password, token,
        )
        final_state = "RUNNING" if start_ok else "STOPPED"

    return {
        "ok": True,
        "id": processor_id,
        "state": final_state,
        "was_running": was_running,
        "message": "Processor configuration updated successfully" + (" and restarted" if was_running and final_state == "RUNNING" else ""),
    }
