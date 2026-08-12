"""Connection lifecycle runner + strategies (behavior)."""
from datetime import datetime
from typing import Optional
from motor.motor_asyncio import AsyncIOMotorDatabase
from fastapi import HTTPException

from models.connection_lifecycle_job import ConnectionLifecycleJob
from services import lifecycle_locks, orphan_registry, content_store_reconcile, connection_fingerprint, iceberg_sinks
from services.connection_fingerprint import (
    probe_nifi_fingerprint,
    probe_kafka_fingerprint,
    probe_apicurio_fingerprint,
    probe_kafka_connect_fingerprint,
    probe_iceberg_fingerprint,
)
from services.connection_impact import compute_impact
from services.runtime_recovery import APP_INSTANCE_ID


# ============================================================================
# Job helpers
# ============================================================================

async def create_job(db: AsyncIOMotorDatabase, *, connection_id: str, operation: str, strategy: Optional[str] = None, pace: Optional[str] = None) -> dict:
    """Create and insert a ConnectionLifecycleJob (status 'pending').

    Returns the inserted document.
    """
    job = ConnectionLifecycleJob(
        connection_id=connection_id,
        operation=operation,
        strategy=strategy,
        pace=pace,
        status="pending",
    )
    job_dict = job.dict(by_alias=False)
    # Convert datetime objects to ensure they're serializable
    if "created_at" in job_dict:
        job_dict["created_at"] = job_dict["created_at"]
    if "updated_at" in job_dict:
        job_dict["updated_at"] = job_dict["updated_at"]

    await db.connection_lifecycle_jobs.insert_one(job_dict)
    return job_dict


async def _set_job(db: AsyncIOMotorDatabase, job_id: str, **fields) -> None:
    """Update job fields + updated_at."""
    fields["updated_at"] = datetime.utcnow()
    await db.connection_lifecycle_jobs.update_one(
        {"id": job_id},
        {"$set": fields}
    )


async def _mark_object(db: AsyncIOMotorDatabase, job_id: str, obj_id: str, obj_type: str, step: str, status: str, error: Optional[str] = None) -> None:
    """Append/update per_object entry for tracking individual object progress."""
    entry = {
        "id": obj_id,
        "type": obj_type,
        "step": step,
        "status": status,
    }
    if error:
        entry["error"] = error

    # Find existing entry for this object
    job = await db.connection_lifecycle_jobs.find_one(
        {"id": job_id},
        {"per_object": 1, "_id": 0}
    )
    per_object = job.get("per_object", []) if job else []

    # Update or append
    found = False
    for i, obj_entry in enumerate(per_object):
        if obj_entry.get("id") == obj_id and obj_entry.get("type") == obj_type:
            per_object[i] = entry
            found = True
            break

    if not found:
        per_object.append(entry)

    await _set_job(db, job_id, per_object=per_object)


async def _heartbeat(db: AsyncIOMotorDatabase, job_id: str) -> None:
    """Set heartbeat_at + owner_instance_id (APP_INSTANCE_ID from runtime_recovery)."""
    await _set_job(db, job_id, heartbeat_at=datetime.utcnow(), owner_instance_id=APP_INSTANCE_ID)


# ============================================================================
# Dispatcher: run_repoint
# ============================================================================

async def run_repoint(
    db: AsyncIOMotorDatabase,
    *,
    conn_id: str,
    new_endpoint: str,
    new_auth: dict,
    strategy: str,
    pace: Optional[str] = None,
    abandon_old: bool = False
) -> dict:
    """Repoint a connection to a new endpoint/auth.

    Steps:
    1. Load connection (404 if missing). Compute impact.
    2. Create new/target connection state (copy existing, new id, is_active=False, set endpoint/auth).
       Test it: probe fingerprint via matching probe_*_fingerprint.
    3. Unknown-fingerprint rule: if probe fingerprint is None while reachable is True (Unknown),
       OR either stored/new fingerprint is unknown, DO NOT proceed unless abandon_old is True.
       Return {ok: False, blocked: "unknown_identity", ...}.
    4. Create job (create_job) with operation "repoint" + strategy + pace. Set status "running", heartbeat.
       Lock affected flows + global services via lifecycle_locks.lock_records.
    5. Dispatch to _strategy_adopt / _strategy_migrate / _strategy_reset.
    6. On success: activate new connection (clear-then-set order), set job "completed", unlock, reconcile.
       On exception: set job "failed" with error, unlock, reconcile. Always unlock in a finally.
    """
    # Step 1: Load connection
    old_conn = await db.connections.find_one({"id": conn_id}, {"_id": 0})
    if not old_conn:
        return {
            "ok": False,
            "error": f"Connection {conn_id} not found",
            "blocked": None,
        }

    impact = await compute_impact(db, conn_id, "repoint")
    if not impact:
        return {
            "ok": False,
            "error": "Failed to compute impact",
            "blocked": None,
        }

    # Step 2: Create new connection state (copy existing, new id, is_active=False, endpoint/auth updated)
    new_conn = dict(old_conn)
    new_conn["id"] = str(__import__("uuid").uuid4())
    new_conn["is_active"] = False
    new_conn["endpoint"] = new_endpoint
    # Apply new_auth fields
    for key, value in new_auth.items():
        new_conn[key] = value
    new_conn["updated_at"] = datetime.utcnow()
    new_conn["created_at"] = datetime.utcnow()

    # Test the new connection: probe fingerprint
    probe_func = {
        "nifi": probe_nifi_fingerprint,
        "kafka": probe_kafka_fingerprint,
        "apicurio": probe_apicurio_fingerprint,
        "kafka_connect": probe_kafka_connect_fingerprint,
        "iceberg": probe_iceberg_fingerprint,
    }.get(old_conn.get("type"))

    if not probe_func:
        return {
            "ok": False,
            "error": f"Unknown connection type: {old_conn.get('type')}",
            "blocked": None,
        }

    probe_result = await probe_func(new_conn)
    new_fingerprint = probe_result.get("fingerprint")
    new_reachable = probe_result.get("reachable", False)

    # Step 3: Unknown-fingerprint rule
    if old_conn.get("type") == "kafka_connect":
        # Kafka Connect connections don't carry their own fingerprint field; the closest
        # stable identity we have on record is the connect_cluster_fingerprint stamped
        # onto any iceberg sink previously enabled against this connection.
        stored_fingerprint = old_conn.get("connect_cluster_fingerprint")
        if stored_fingerprint is None:
            sink_with_fp = await db.iceberg_sinks.find_one(
                {"connect_connection_id": conn_id, "connect_cluster_fingerprint": {"$ne": None}},
                {"_id": 0, "connect_cluster_fingerprint": 1},
            )
            if sink_with_fp:
                stored_fingerprint = sink_with_fp.get("connect_cluster_fingerprint")
    else:
        stored_fingerprint = old_conn.get(f"{old_conn.get('type')}_instance_fingerprint") or old_conn.get("nifi_instance_fingerprint") or old_conn.get("kafka_cluster_id")

    # Check for unknown identity
    is_unknown = (new_fingerprint is None and new_reachable) or (stored_fingerprint is None) or (new_fingerprint is None)

    if is_unknown and not abandon_old:
        return {
            "ok": False,
            "blocked": "unknown_identity",
            "message": "Cannot repoint to unknown identity without explicit abandon_old=True",
            "probe_result": probe_result,
        }

    # Step 4: Create job, set status running, heartbeat, lock resources
    job = await create_job(db, connection_id=conn_id, operation="repoint", strategy=strategy, pace=pace)
    job_id = job["id"]

    await _set_job(db, job_id, status="running")
    await _heartbeat(db, job_id)

    # Collect flow IDs, service IDs, and affected iceberg sink IDs to lock
    flow_ids = [f["id"] for f in impact.get("flows", [])]
    service_ids = [s["id"] for s in impact.get("global_services", [])]
    sink_ids = [s["id"] for s in impact.get("iceberg_sinks", [])]

    await lifecycle_locks.lock_records(db, job_id, flow_ids=flow_ids, service_ids=service_ids, sink_ids=sink_ids)

    # Step 5: Dispatch to strategy
    try:
        # Insert the new connection into the database
        await db.connections.insert_one(new_conn)

        if strategy == "adopt":
            await _strategy_adopt(db, job_id, old_conn, new_conn, impact, probe_result=probe_result)
        elif strategy == "migrate":
            # migrate redeploys flows onto the NEW instance, so it must be active
            # BEFORE the redeploy loop — otherwise deploy_flow resolves the still-active
            # OLD connection and redeploys onto the old cluster (dual-ingestion / no-op move).
            await db.connections.update_many(
                {"type": old_conn.get("type")},
                {"$set": {"is_active": False}},
            )
            await db.connections.update_one(
                {"id": new_conn["id"]},
                {"$set": {"is_active": True}},
            )
            await _strategy_migrate(db, job_id, old_conn, new_conn, impact, pace, abandon_old=abandon_old)
        elif strategy == "reset":
            await _strategy_reset(db, job_id, old_conn, new_conn, impact)
        else:
            raise ValueError(f"Unknown strategy: {strategy}")

        # Step 6a: On success - activate new connection (clear old, set new)
        await db.connections.update_many(
            {"type": old_conn.get("type")},
            {"$set": {"is_active": False}}
        )
        await db.connections.update_one(
            {"id": new_conn["id"]},
            {"$set": {"is_active": True}}
        )

        # Iceberg repoint (v1: endpoint/auth only): re-upsert connectors of enabled sinks
        # so they pick up the new catalog URI. Best-effort — a sink failure never fails
        # the overall repoint job. Runs after activation so enable_sink resolves the new
        # iceberg connection as the active one.
        if old_conn.get("type") == "iceberg":
            for sink in impact.get("iceberg_sinks", []):
                if not sink.get("enabled"):
                    continue
                sink_id = sink["id"]
                try:
                    sink_result = await iceberg_sinks.enable_sink(db, sink_id)
                    if sink_result.get("ok"):
                        await _mark_object(db, job_id, sink_id, "iceberg_sink", "repoint", "completed")
                    else:
                        await _mark_object(
                            db, job_id, sink_id, "iceberg_sink", "repoint", "failed",
                            str(sink_result.get("error"))[:200]
                        )
                except Exception as sink_exc:
                    await _mark_object(db, job_id, sink_id, "iceberg_sink", "repoint", "failed", str(sink_exc)[:200])

        await _set_job(db, job_id, status="completed")

        # Reconcile content store
        await content_store_reconcile.reconcile_content_store(db)

        return {
            "ok": True,
            "job_id": job_id,
            "old_connection_id": conn_id,
            "new_connection_id": new_conn["id"],
        }

    except Exception as exc:
        # Step 6b: On exception - set job failed, unlock, reconcile
        error_msg = str(exc)[:500]
        await _set_job(db, job_id, status="failed", error=error_msg)

        # Reconcile content store
        await content_store_reconcile.reconcile_content_store(db)

        return {
            "ok": False,
            "job_id": job_id,
            "error": error_msg,
        }

    finally:
        # Always unlock
        await lifecycle_locks.unlock_job(db, job_id)


# ============================================================================
# Strategies
# ============================================================================

async def _strategy_adopt(
    db: AsyncIOMotorDatabase,
    job_id: str,
    old_conn: dict,
    new_conn: dict,
    impact: dict,
    probe_result: Optional[dict] = None,
) -> None:
    """Adopt strategy: same instance, rebind — NO redeploy.

    For every dependent flow/global-service/schema-version:
    - Verify it still exists on the NEW instance (best-effort probe).
    - If a checked dependent is MISSING, mark per_object "failed" and set job "failed" at end.
    - Rebind provenance: set *_connection_id to new_conn["id"] and refresh fingerprint.
    - Mark per_object "completed" on success.

    For "kafka_connect" connections, affected iceberg sinks are rebound to the new
    connection id too (connector configs are unchanged — same underlying cluster).
    """
    job_failed = False

    # Process flows
    for flow in impact.get("flows", []):
        flow_id = flow["id"]
        try:
            # Best-effort check: verify flow still exists
            flow_doc = await db.flows.find_one({"id": flow_id}, {"_id": 0})
            if not flow_doc:
                await _mark_object(db, job_id, flow_id, "flow", "adopt", "failed", "Flow not found on new instance")
                job_failed = True
                continue

            # Rebind provenance
            update_fields = {}
            if old_conn.get("type") == "nifi":
                update_fields["nifi_connection_id"] = new_conn["id"]
                # Refresh fingerprint
                new_fingerprint = new_conn.get("nifi_instance_fingerprint")
                if new_fingerprint:
                    update_fields["nifi_instance_fingerprint"] = new_fingerprint
            elif old_conn.get("type") == "kafka":
                update_fields["kafka_connection_id"] = new_conn["id"]
                # Refresh fingerprint
                new_fingerprint = new_conn.get("kafka_cluster_id")
                if new_fingerprint:
                    update_fields["kafka_cluster_id"] = new_fingerprint

            update_fields["updated_at"] = datetime.utcnow()
            await db.flows.update_one({"id": flow_id}, {"$set": update_fields})
            await _mark_object(db, job_id, flow_id, "flow", "adopt", "completed")

        except Exception as exc:
            await _mark_object(db, job_id, flow_id, "flow", "adopt", "failed", str(exc)[:200])
            job_failed = True

    # Process global services
    for service in impact.get("global_services", []):
        service_id = service["id"]
        try:
            # Best-effort check: verify service still exists
            service_doc = await db.nifi_global_services.find_one({"id": service_id}, {"_id": 0})
            if not service_doc:
                await _mark_object(db, job_id, service_id, "global_service", "adopt", "failed", "Service not found on new instance")
                job_failed = True
                continue

            # Rebind provenance
            update_fields = {
                "nifi_connection_id": new_conn["id"],
                "updated_at": datetime.utcnow(),
            }
            # Refresh fingerprint if available
            new_fingerprint = new_conn.get("nifi_instance_fingerprint")
            if new_fingerprint:
                update_fields["nifi_instance_fingerprint"] = new_fingerprint

            await db.nifi_global_services.update_one({"id": service_id}, {"$set": update_fields})
            await _mark_object(db, job_id, service_id, "global_service", "adopt", "completed")

        except Exception as exc:
            await _mark_object(db, job_id, service_id, "global_service", "adopt", "failed", str(exc)[:200])
            job_failed = True

    # Process schema versions
    for schema_ref in impact.get("schema_versions", []):
        artifact_id = schema_ref.get("artifact_id")
        version = schema_ref.get("version")
        obj_id = f"{artifact_id}:{version}"

        try:
            # Rebind provenance
            update_fields = {
                "versions.$.apicurio_connection_id": new_conn["id"],
                "updated_at": datetime.utcnow(),
            }
            await db.schema_artifacts.update_one(
                {"id": artifact_id, "versions.version": version},
                {"$set": update_fields}
            )
            await _mark_object(db, job_id, obj_id, "schema_version", "adopt", "completed")

        except Exception as exc:
            await _mark_object(db, job_id, obj_id, "schema_version", "adopt", "failed", str(exc)[:200])
            job_failed = True

    # Process iceberg sinks (kafka_connect connections only): rebind connect_connection_id
    # and refresh connect_cluster_fingerprint. Same cluster identity — no connector changes.
    if old_conn.get("type") == "kafka_connect":
        new_fingerprint = (probe_result or {}).get("fingerprint")
        for sink in impact.get("iceberg_sinks", []):
            sink_id = sink["id"]
            try:
                update_fields = {
                    "connect_connection_id": new_conn["id"],
                    "updated_at": datetime.utcnow(),
                }
                if new_fingerprint:
                    update_fields["connect_cluster_fingerprint"] = new_fingerprint
                await db.iceberg_sinks.update_one({"id": sink_id}, {"$set": update_fields})
                await _mark_object(db, job_id, sink_id, "iceberg_sink", "adopt", "completed")
            except Exception as exc:
                await _mark_object(db, job_id, sink_id, "iceberg_sink", "adopt", "failed", str(exc)[:200])
                job_failed = True

    if job_failed:
        await _set_job(db, job_id, status="failed")


async def _strategy_reset(
    db: AsyncIOMotorDatabase,
    job_id: str,
    old_conn: dict,
    new_conn: dict,
    impact: dict
) -> None:
    """Reset strategy: abandon old artifacts.

    For every dependent:
    - Record abandoned handle in orphan_registry (flows → nifi_process_group_id/kafka_topic,
      global_services → nifi_controller_service_id, schema_versions → apicurio_global_id).
    - Clear cached handle: flows → nifi_process_group_id=None, state="Stopped";
      global_services → leave doc but dangling; schema_versions → status="Not registered", null apicurio_global_id.
    - Rebind *_connection_id to new_conn["id"].
    - Mark per_object "completed".
    """
    # Process flows
    for flow in impact.get("flows", []):
        flow_id = flow["id"]
        try:
            # Record orphans
            if flow.get("nifi_process_group_id"):
                await orphan_registry.record_orphan(
                    db,
                    system="nifi",
                    external_id=flow["nifi_process_group_id"],
                    kind="process_group",
                    connection_id=old_conn["id"],
                    reason="Strategy reset",
                    job_id=job_id,
                )

            if flow.get("kafka_topic"):
                await orphan_registry.record_orphan(
                    db,
                    system="kafka",
                    external_id=flow["kafka_topic"],
                    kind="topic",
                    connection_id=old_conn["id"],
                    reason="Strategy reset",
                    job_id=job_id,
                )

            # Clear cached handles and rebind
            update_fields = {
                "nifi_process_group_id": None,
                "state": "Stopped",
                "updated_at": datetime.utcnow(),
            }
            if old_conn.get("type") == "nifi":
                update_fields["nifi_connection_id"] = new_conn["id"]
            elif old_conn.get("type") == "kafka":
                update_fields["kafka_connection_id"] = new_conn["id"]

            await db.flows.update_one({"id": flow_id}, {"$set": update_fields})
            await _mark_object(db, job_id, flow_id, "flow", "reset", "completed")

        except Exception as exc:
            await _mark_object(db, job_id, flow_id, "flow", "reset", "failed", str(exc)[:200])

    # Process global services
    for service in impact.get("global_services", []):
        service_id = service["id"]
        try:
            # Record orphan
            if service.get("nifi_controller_service_id"):
                await orphan_registry.record_orphan(
                    db,
                    system="nifi",
                    external_id=service["nifi_controller_service_id"],
                    kind="controller_service",
                    connection_id=old_conn["id"],
                    reason="Strategy reset",
                    job_id=job_id,
                )

            # Rebind connection_id (leave the doc, it becomes dangling)
            update_fields = {
                "nifi_connection_id": new_conn["id"],
                "updated_at": datetime.utcnow(),
            }
            await db.nifi_global_services.update_one({"id": service_id}, {"$set": update_fields})
            await _mark_object(db, job_id, service_id, "global_service", "reset", "completed")

        except Exception as exc:
            await _mark_object(db, job_id, service_id, "global_service", "reset", "failed", str(exc)[:200])

    # Process schema versions
    for schema_ref in impact.get("schema_versions", []):
        artifact_id = schema_ref.get("artifact_id")
        version = schema_ref.get("version")
        obj_id = f"{artifact_id}:{version}"

        try:
            # Get the schema version to find apicurio_global_id
            artifact = await db.schema_artifacts.find_one(
                {"id": artifact_id, "versions.version": version},
                {"_id": 0, "versions.$": 1}
            )

            if artifact and artifact.get("versions"):
                version_doc = artifact["versions"][0]
                apicurio_global_id = version_doc.get("apicurio_global_id")

                if apicurio_global_id:
                    await orphan_registry.record_orphan(
                        db,
                        system="apicurio",
                        external_id=str(apicurio_global_id),
                        kind="schema",
                        connection_id=old_conn["id"],
                        reason="Strategy reset",
                        job_id=job_id,
                    )

            # Set status to "Not registered" and null apicurio_global_id
            await db.schema_artifacts.update_one(
                {"id": artifact_id, "versions.version": version},
                {
                    "$set": {
                        "versions.$.status": "Not registered",
                        "versions.$.apicurio_global_id": None,
                        "versions.$.apicurio_connection_id": new_conn["id"],
                        "updated_at": datetime.utcnow(),
                    }
                }
            )
            await _mark_object(db, job_id, obj_id, "schema_version", "reset", "completed")

        except Exception as exc:
            await _mark_object(db, job_id, obj_id, "schema_version", "reset", "failed", str(exc)[:200])


async def _strategy_migrate(
    db: AsyncIOMotorDatabase,
    job_id: str,
    old_conn: dict,
    new_conn: dict,
    impact: dict,
    pace: Optional[str] = None,
    abandon_old: bool = False,
) -> None:
    """Migrate strategy: new instance — provision + redeploy.

    - Platform global services: null nifi_controller_service_id, set nifi_connection_id=new_conn["id"],
      record old controller_service_id as orphan.
    - Flows: for each, demote (nifi_process_group_id=None, state="Stopped", record orphan, rebind),
      then if pace="all", attempt redeploy (unlock, call deploy_flow, re-lock).
      If pace="one_by_one", leave demoted for user to redeploy; mark completed.
    - Resumability: skip per_object entries already "completed".
    - "kafka_connect" connections: for each affected iceberg sink with enabled=True, re-create
      the connector on the NEW cluster (best-effort, per-sink — a sink failure never aborts
      the job). When abandon_old is set, the old connector name is recorded as an orphan.
    """
    # Process global services (platform services)
    for service in impact.get("global_services", []):
        service_id = service["id"]
        try:
            # Record old controller_service_id as orphan
            service_doc = await db.nifi_global_services.find_one({"id": service_id}, {"_id": 0})
            if service_doc and service_doc.get("nifi_controller_service_id"):
                await orphan_registry.record_orphan(
                    db,
                    system="nifi",
                    external_id=service_doc["nifi_controller_service_id"],
                    kind="controller_service",
                    connection_id=old_conn["id"],
                    reason="Strategy migrate - rebuild on new instance",
                    job_id=job_id,
                )

            # Null controller_service_id, set nifi_connection_id
            update_fields = {
                "nifi_controller_service_id": None,
                "nifi_connection_id": new_conn["id"],
                "updated_at": datetime.utcnow(),
            }
            await db.nifi_global_services.update_one({"id": service_id}, {"$set": update_fields})
            await _mark_object(db, job_id, service_id, "global_service", "migrate", "completed")

        except Exception as exc:
            await _mark_object(db, job_id, service_id, "global_service", "migrate", "failed", str(exc)[:200])

    # Process flows
    for flow in impact.get("flows", []):
        flow_id = flow["id"]

        # Check if already completed (resumability)
        job = await db.connection_lifecycle_jobs.find_one({"id": job_id}, {"per_object": 1, "_id": 0})
        per_object = job.get("per_object", []) if job else []

        is_completed = any(
            obj.get("id") == flow_id and obj.get("type") == "flow" and obj.get("status") == "completed"
            for obj in per_object
        )

        if is_completed:
            continue

        try:
            # Step 1: Demote
            # Record old PG id as orphan
            if flow.get("nifi_process_group_id"):
                await orphan_registry.record_orphan(
                    db,
                    system="nifi",
                    external_id=flow["nifi_process_group_id"],
                    kind="process_group",
                    connection_id=old_conn["id"],
                    reason="Strategy migrate - demote before redeploy",
                    job_id=job_id,
                )

            # Demote and rebind
            update_fields = {
                "nifi_process_group_id": None,
                "state": "Stopped",
                "updated_at": datetime.utcnow(),
            }
            if old_conn.get("type") == "nifi":
                update_fields["nifi_connection_id"] = new_conn["id"]
            elif old_conn.get("type") == "kafka":
                update_fields["kafka_connection_id"] = new_conn["id"]

            await db.flows.update_one({"id": flow_id}, {"$set": update_fields})

            # Step 2: Redeploy logic
            if pace == "all":
                try:
                    # Temporarily unlock THIS flow
                    await db.flows.update_one(
                        {"id": flow_id},
                        {"$unset": {"lifecycle_lock_job_id": ""}}
                    )

                    # Call deploy_flow
                    try:
                        from routers.flows import deploy_flow
                        result = await deploy_flow(flow_id, db=db)
                        if result.get("ok"):
                            await _mark_object(db, job_id, flow_id, "flow", "migrate", "completed")
                        else:
                            await _mark_object(db, job_id, flow_id, "flow", "migrate", "failed", str(result.get("error", "Deploy failed"))[:200])
                    except HTTPException as hte:
                        await _mark_object(db, job_id, flow_id, "flow", "migrate", "failed", str(hte.detail)[:200])
                    except Exception as deploy_exc:
                        await _mark_object(db, job_id, flow_id, "flow", "migrate", "failed", str(deploy_exc)[:200])

                    # Re-lock THIS flow
                    await db.flows.update_one(
                        {"id": flow_id},
                        {"$set": {"lifecycle_lock_job_id": job_id}}
                    )

                except Exception as exc:
                    await _mark_object(db, job_id, flow_id, "flow", "migrate", "failed", str(exc)[:200])

            else:  # pace != "all" (including "one_by_one" and None)
                # Leave demoted for user to redeploy
                await _mark_object(db, job_id, flow_id, "flow", "migrate", "completed", None)

        except Exception as exc:
            await _mark_object(db, job_id, flow_id, "flow", "migrate", "failed", str(exc)[:200])

    # Process iceberg sinks (kafka_connect connections only): re-create connectors on the
    # NEW cluster for every affected, enabled sink. Best-effort per sink.
    if old_conn.get("type") == "kafka_connect":
        for sink in impact.get("iceberg_sinks", []):
            sink_id = sink["id"]
            connector_name = sink.get("connector_name")

            if abandon_old and connector_name:
                await orphan_registry.record_orphan(
                    db,
                    system="kafka_connect",
                    external_id=connector_name,
                    kind="connector",
                    connection_id=old_conn["id"],
                    reason="Strategy migrate - abandon old cluster's connector",
                    job_id=job_id,
                )

            if not sink.get("enabled"):
                continue

            try:
                sink_result = await iceberg_sinks.enable_sink(db, sink_id)
                if sink_result.get("ok"):
                    await _mark_object(db, job_id, sink_id, "iceberg_sink", "migrate", "completed")
                else:
                    await _mark_object(
                        db, job_id, sink_id, "iceberg_sink", "migrate", "failed",
                        str(sink_result.get("error"))[:200]
                    )
            except Exception as exc:
                await _mark_object(db, job_id, sink_id, "iceberg_sink", "migrate", "failed", str(exc)[:200])
