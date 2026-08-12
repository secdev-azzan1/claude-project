"""Sink-sync service: reconciles designer intent (entity.iceberg) into iceberg_sinks documents.

Reads streams directly off the source document (not `_collect_entity_destinations`, which
drops the entity.iceberg block — see plan WP2.4). Pure Mongo at this stage: no Kafka Connect
calls happen here.
"""
from datetime import datetime
from typing import Any, Dict, List, Optional

from models.iceberg_sink import IcebergSink
from services import apicurio_client, kafka_client
from services.audit import log_audit
from services.connection_resolver import resolve_connection
from services.iceberg_sink_config import (
    ICEBERG_CONNECTOR_CLASS,
    build_connector_config,
    derive_connector_name,
    derive_table_name,
    derive_table_name_from_names,
)
from services.kafka_connect_client import (
    delete_connector,
    get_cluster_info,
    get_connector_status,
    list_connector_plugins,
    list_connectors_with_status,
    pause_connector,
    restart_connector,
    resume_connector,
    upsert_connector,
    validate_connector_config,
)
from services.orphan_registry import record_orphan


def sink_response(doc: Optional[dict]) -> Optional[dict]:
    """Return a public sink document with only the supported intent snapshot."""
    if doc is None:
        return None
    result = dict(doc)
    overrides = result.get("overrides") if isinstance(result.get("overrides"), dict) else {}
    result["overrides"] = {"enabled": bool(overrides.get("enabled", result.get("enabled", False)))}
    return result


async def sync_sinks_for_flow(db, flow: dict, source: Optional[dict]) -> List[dict]:
    """Reconcile iceberg_sinks documents for a single flow against its source's designer intent.

    Returns the current sink dicts for the flow after reconciliation.
    """
    flow_id = flow["id"]
    computed_stream_ids: set = set()

    for stream in (source or {}).get("streams") or []:
        if not isinstance(stream, dict):
            continue
        entity = stream.get("entity") or {}
        if not isinstance(entity, dict):
            entity = {}
        iceberg_intent = entity.get("iceberg")
        if not iceberg_intent:
            continue
        kafka_cfg = entity.get("kafka") or {}
        if not isinstance(kafka_cfg, dict):
            kafka_cfg = {}
        topic = str(kafka_cfg.get("topic") or "").strip()
        if not topic:
            continue

        stream_id = str(stream.get("id") or stream.get("name") or "").strip()
        computed_stream_ids.add(stream_id)

        existing = await db.iceberg_sinks.find_one(
            {"flow_id": flow_id, "stream_id": stream_id}, {"_id": 0}
        )

        intent_now = bool(iceberg_intent.get("enabled"))
        if existing is None:
            enabled = intent_now
        else:
            intent_at_sync = bool((existing.get("overrides") or {}).get("enabled"))
            if intent_now != intent_at_sync:
                enabled = intent_now
            else:
                enabled = existing.get("enabled")

        namespace_name = str(flow.get("name") or (source or {}).get("name") or "").strip()
        entity_name = str(stream.get("name") or stream_id).strip()
        table_name = derive_table_name_from_names(namespace_name, entity_name, topic)
        connector_name = derive_connector_name(topic)
        intent_snapshot = {"enabled": intent_now}

        if existing is None:
            sink = IcebergSink(
                flow_id=flow_id,
                stream_id=stream_id,
                topic=topic,
                connector_name=connector_name,
                table_name=table_name,
                enabled=enabled,
                overrides=intent_snapshot,
            )
            await db.iceberg_sinks.insert_one(sink.dict())
        else:
            await db.iceberg_sinks.update_one(
                {"flow_id": flow_id, "stream_id": stream_id},
                {
                    "$set": {
                        "topic": topic,
                        "connector_name": connector_name,
                        "table_name": table_name,
                        "enabled": enabled,
                        "overrides": intent_snapshot,
                        "updated_at": datetime.utcnow(),
                    }
                },
            )

    await db.iceberg_sinks.delete_many(
        {"flow_id": flow_id, "stream_id": {"$nin": list(computed_stream_ids)}}
    )

    return await list_sinks_for_flow(db, flow_id)


async def sync_sinks_for_source(db, source: dict) -> None:
    """Streams live on the source, so one source edit can affect several flows."""
    async for flow in db.flows.find({"source_id": source.get("id")}, {"_id": 0}):
        await sync_sinks_for_flow(db, flow, source)


async def list_sinks_for_flow(db, flow_id) -> List[dict]:
    cursor = db.iceberg_sinks.find({"flow_id": flow_id}, {"_id": 0})
    return [sink_response(doc) async for doc in cursor]


async def find_shared_table_conflicts(db, flow_id) -> List[dict]:
    """Sinks in OTHER flows writing the same table_name as this flow's sinks.

    Warning data only — never raises or blocks (plan WP2.2: shared tables are
    warned about, not forbidden).
    """
    own_sinks = await list_sinks_for_flow(db, flow_id)
    table_names = {sink.get("table_name") for sink in own_sinks if sink.get("table_name")}
    if not table_names:
        return []

    cursor = db.iceberg_sinks.find(
        {"table_name": {"$in": list(table_names)}, "flow_id": {"$ne": flow_id}}, {"_id": 0}
    )
    return [doc async for doc in cursor]


async def _default_table_name_for_sink(db, sink: dict) -> str:
    """Resolve the default Iceberg namespace.table for a sink from its flow and stream."""
    topic = str(sink.get("topic") or "").strip()
    try:
        flow = await db.flows.find_one({"id": sink.get("flow_id")}, {"_id": 0, "name": 1, "source_id": 1})
        source = (
            await db.sources.find_one({"id": flow.get("source_id")}, {"_id": 0, "name": 1, "streams": 1})
            if flow
            else None
        )
        stream_name = ""
        for stream in (source or {}).get("streams") or []:
            if not isinstance(stream, dict):
                continue
            stream_id = str(stream.get("id") or stream.get("name") or "")
            if stream_id == str(sink.get("stream_id")):
                stream_name = str(stream.get("name") or stream_id).strip()
                break
        namespace_name = str((flow or {}).get("name") or (source or {}).get("name") or "").strip()
        if namespace_name and stream_name:
            return derive_table_name_from_names(namespace_name, stream_name, topic)
    except Exception:
        pass
    return derive_table_name(topic)


# ---------------------------------------------------------------------------
# Connector lifecycle + preflight (plan WP2.5/WP2.6): everything below this
# point talks to Kafka Connect / Apicurio / Kafka. The functions above stay
# pure-Mongo reconciliation.
# ---------------------------------------------------------------------------


async def _resolve_sink_connections(db) -> Dict[str, Any]:
    """Resolve the active connections an Iceberg sink deploy/preflight needs."""
    connect = await resolve_connection(db, "kafka_connect", required=False)
    iceberg = await resolve_connection(db, "iceberg", required=False)
    apicurio = await resolve_connection(db, "apicurio", required=False)
    kafka = await resolve_connection(db, "kafka", required=False)

    missing = []
    if not connect:
        missing.append("kafka_connect")
    if not iceberg:
        missing.append("iceberg")
    if not apicurio:
        missing.append("apicurio")
    if not kafka:
        missing.append("kafka")

    return {
        "ok": not missing,
        "missing": missing,
        "connect": connect,
        "iceberg": iceberg,
        "apicurio": apicurio,
        "kafka": kafka,
    }


async def preflight_sink(db, sink: dict) -> Dict[str, Any]:
    """Run every preflight check for a sink, always returning the full checklist.

    Never raises: an unexpected exception is captured as a failed check so the
    UI still gets a checklist back instead of a 500.
    """
    checks: List[Dict[str, Any]] = []
    try:
        conns = await _resolve_sink_connections(db)
        connect_conn = conns.get("connect")
        iceberg_conn = conns.get("iceberg")
        apicurio_conn = conns.get("apicurio")
        kafka_conn = conns.get("kafka")

        # 1. connections
        missing_required = [t for t in conns.get("missing") or [] if t in ("kafka_connect", "iceberg")]
        checks.append(
            {
                "name": "connections",
                "ok": not missing_required,
                "detail": (
                    "Kafka Connect and Iceberg connections are configured and active."
                    if not missing_required
                    else f"Missing or inactive connection(s): {', '.join(missing_required)}."
                ),
            }
        )

        # 2. connect_reachable
        cluster_info_result: Dict[str, Any] = {"ok": False}
        if connect_conn:
            cluster_info_result = await get_cluster_info(connect_conn)
            checks.append(
                {
                    "name": "connect_reachable",
                    "ok": bool(cluster_info_result.get("ok")),
                    "detail": cluster_info_result.get("message") or cluster_info_result.get("error") or "",
                }
            )
        else:
            checks.append(
                {"name": "connect_reachable", "ok": False, "detail": "No Kafka Connect connection configured."}
            )

        # 3. cluster_match
        connect_cluster_id = None
        connect_data = cluster_info_result.get("data") or {}
        if isinstance(connect_data, dict):
            connect_cluster_id = connect_data.get("kafka_cluster_id")

        proven_kafka_cluster_id = None
        if kafka_conn:
            kafka_flow = await db.flows.find_one(
                {
                    "kafka_connection_id": kafka_conn.get("id"),
                    "kafka_cluster_id": {"$exists": True, "$ne": None},
                },
                {"_id": 0, "kafka_cluster_id": 1},
            )
            if kafka_flow:
                proven_kafka_cluster_id = kafka_flow.get("kafka_cluster_id")

        if not connect_cluster_id or not proven_kafka_cluster_id:
            checks.append(
                {
                    "name": "cluster_match",
                    "ok": True,
                    "detail": "Could not verify Kafka Connect and Kafka point to the same cluster (no proven cluster id available yet).",
                }
            )
        elif connect_cluster_id == proven_kafka_cluster_id:
            checks.append(
                {"name": "cluster_match", "ok": True, "detail": "Kafka Connect and Kafka cluster IDs match."}
            )
        else:
            checks.append(
                {
                    "name": "cluster_match",
                    "ok": False,
                    "detail": (
                        f"Kafka Connect cluster id '{connect_cluster_id}' does not match the Kafka "
                        f"connection's proven cluster id '{proven_kafka_cluster_id}'."
                    ),
                }
            )

        # 4. plugin_installed
        if connect_conn:
            plugins_result = await list_connector_plugins(connect_conn)
            plugin_classes = set()
            if plugins_result.get("ok") and isinstance(plugins_result.get("data"), list):
                plugin_classes = {
                    plugin.get("class") for plugin in plugins_result["data"] if isinstance(plugin, dict)
                }
            plugin_ok = ICEBERG_CONNECTOR_CLASS in plugin_classes
            if plugin_ok:
                detail = "Iceberg Sink connector plugin is installed on Kafka Connect."
            elif plugins_result.get("ok"):
                detail = f"Iceberg Sink connector plugin ('{ICEBERG_CONNECTOR_CLASS}') was not found on Kafka Connect."
            else:
                detail = plugins_result.get("error") or "Could not list Kafka Connect connector plugins."
            checks.append({"name": "plugin_installed", "ok": plugin_ok, "detail": detail})
        else:
            checks.append(
                {"name": "plugin_installed", "ok": False, "detail": "No Kafka Connect connection configured."}
            )

        # 5. schema_available
        if apicurio_conn:
            subject = f"{sink.get('topic')}-value"
            group_id = apicurio_conn.get("group_id") or "nif-platform"
            schema_result = await apicurio_client.schema_available(
                apicurio_conn.get("endpoint") or "",
                group_id,
                subject,
                auth_type=apicurio_conn.get("auth_type", "NONE"),
                username=apicurio_conn.get("username"),
                password=apicurio_conn.get("password"),
                token=apicurio_conn.get("token"),
            )
            schema_ok = bool(schema_result.get("ok"))
            if schema_ok:
                detail = f"Schema for subject '{subject}' is available in Apicurio."
            else:
                detail = (
                    f"Schema for subject '{subject}' is not fully available "
                    f"(native={schema_result.get('native_ok')}, ccompat={schema_result.get('ccompat_ok')})."
                )
            checks.append({"name": "schema_available", "ok": schema_ok, "detail": detail})
        else:
            checks.append(
                {"name": "schema_available", "ok": False, "detail": "No Apicurio connection configured."}
            )

        # 6. topic_exists
        if kafka_conn:
            partitions, replication_factor = 6, 3
            try:
                flow_doc = await db.flows.find_one({"id": sink.get("flow_id")}, {"_id": 0, "source_id": 1})
                source_doc = (
                    await db.sources.find_one({"id": flow_doc.get("source_id")}, {"_id": 0, "streams": 1})
                    if flow_doc
                    else None
                )
                for stream in (source_doc or {}).get("streams") or []:
                    if not isinstance(stream, dict):
                        continue
                    stream_id = str(stream.get("id") or stream.get("name") or "")
                    if stream_id != str(sink.get("stream_id")):
                        continue
                    entity = stream.get("entity") or {}
                    kafka_cfg = entity.get("kafka") if isinstance(entity, dict) else None
                    if isinstance(kafka_cfg, dict):
                        partitions = int(kafka_cfg.get("partitions") or partitions)
                        replication_factor = int(kafka_cfg.get("replication_factor") or replication_factor)
                    break
            except Exception:
                pass

            topic_result = await kafka_client.ensure_topic_exists(
                kafka_conn, sink.get("topic") or "", partitions, replication_factor
            )
            checks.append(
                {
                    "name": "topic_exists",
                    "ok": bool(topic_result.get("ok")),
                    "detail": topic_result.get("message") or topic_result.get("error") or "",
                }
            )
        else:
            checks.append({"name": "topic_exists", "ok": False, "detail": "No Kafka connection configured."})

        # 7. config_valid
        if connect_conn and iceberg_conn and apicurio_conn:
            try:
                table_name = await _default_table_name_for_sink(db, sink)
                config = build_connector_config(
                    sink.get("topic"), table_name, apicurio_conn or {}, iceberg_conn
                )
                validation_config = {
                    **config,
                    "name": sink.get("connector_name") or derive_connector_name(sink.get("topic") or ""),
                }
                validate_result = await validate_connector_config(
                    connect_conn, ICEBERG_CONNECTOR_CLASS, validation_config
                )
                checks.append(
                    {
                        "name": "config_valid",
                        "ok": bool(validate_result.get("ok")),
                        "detail": validate_result.get("message") or validate_result.get("error") or "",
                    }
                )
            except Exception as e:
                checks.append(
                    {"name": "config_valid", "ok": False, "detail": f"Could not build/validate connector config: {e}"}
                )
        else:
            checks.append(
                {
                    "name": "config_valid",
                    "ok": False,
                    "detail": "Missing connection(s) required to build the connector config.",
                }
            )

        return {"ok": all(check["ok"] for check in checks), "checks": checks}
    except Exception as e:
        checks.append({"name": "unexpected_error", "ok": False, "detail": str(e)[:300]})
        return {"ok": False, "checks": checks}


async def build_config_for_sink(db, sink) -> Dict[str, str]:
    """Resolve connections and build the full Iceberg Sink connector config for a sink."""
    conns = await _resolve_sink_connections(db)
    apicurio_conn = conns.get("apicurio") or {}
    iceberg_conn = conns.get("iceberg") or {}
    table_name = await _default_table_name_for_sink(db, sink)
    return build_connector_config(
        sink["topic"], table_name, apicurio_conn, iceberg_conn
    )


async def _log_sink_audit(db, action: str, sink: dict, sink_id: str, status: str, details=None) -> None:
    await log_audit(
        db,
        action,
        "IcebergSink",
        sink.get("connector_name") or sink_id,
        status,
        details,
    )


async def enable_sink(db, sink_id) -> Dict[str, Any]:
    """Preflight then upsert the connector on Kafka Connect. Stamps provenance on success."""
    sink = await db.iceberg_sinks.find_one({"id": sink_id}, {"_id": 0})
    if sink is None:
        return {"ok": False, "error": "Iceberg sink not found."}

    preflight = await preflight_sink(db, sink)
    if not preflight.get("ok"):
        await _log_sink_audit(
            db, "iceberg_sink.enable", sink, sink_id, "Failed", {"checks": preflight.get("checks")}
        )
        return {"ok": False, "checks": preflight.get("checks"), "error": "Preflight checks failed."}

    conns = await _resolve_sink_connections(db)
    connect_conn = conns.get("connect")
    iceberg_conn = conns.get("iceberg")
    kafka_conn = conns.get("kafka")

    try:
        deployed_table_name = await _default_table_name_for_sink(db, sink)
        config = await build_config_for_sink(db, sink)
        result = await upsert_connector(connect_conn, sink["connector_name"], config)
    except Exception as e:
        result = {"ok": False, "error": str(e)[:300]}
        deployed_table_name = sink.get("table_name")

    if not result.get("ok"):
        error_message = result.get("error") or "Failed to create/update the Iceberg Sink connector."
        await db.iceberg_sinks.update_one(
            {"id": sink_id}, {"$set": {"last_error": error_message, "updated_at": datetime.utcnow()}}
        )
        await _log_sink_audit(db, "iceberg_sink.enable", sink, sink_id, "Failed", error_message)
        return {"ok": False, "error": error_message}

    connect_cluster_fingerprint = None
    try:
        cluster_info = await get_cluster_info(connect_conn)
        if cluster_info.get("ok") and isinstance(cluster_info.get("data"), dict):
            connect_cluster_fingerprint = cluster_info["data"].get("kafka_cluster_id")
    except Exception:
        pass

    await db.iceberg_sinks.update_one(
        {"id": sink_id},
        {
            "$set": {
                "enabled": True,
                "table_name": deployed_table_name,
                "last_error": None,
                "updated_at": datetime.utcnow(),
                "connect_connection_id": connect_conn.get("id") if connect_conn else None,
                "connect_cluster_fingerprint": connect_cluster_fingerprint,
                "iceberg_connection_id": iceberg_conn.get("id") if iceberg_conn else None,
                "kafka_connection_id": kafka_conn.get("id") if kafka_conn else None,
            }
        },
    )
    await _log_sink_audit(db, "iceberg_sink.enable", sink, sink_id, "Success")
    updated = await db.iceberg_sinks.find_one({"id": sink_id}, {"_id": 0})
    return {"ok": True, "sink": sink_response(updated)}


async def disable_sink(db, sink_id) -> Dict[str, Any]:
    """Delete the connector on Kafka Connect (404 tolerated) and mark the sink disabled. Keeps the doc."""
    sink = await db.iceberg_sinks.find_one({"id": sink_id}, {"_id": 0})
    if sink is None:
        return {"ok": False, "error": "Iceberg sink not found."}

    conns = await _resolve_sink_connections(db)
    connect_conn = conns.get("connect")
    if connect_conn is None:
        error_message = "No Kafka Connect connection configured."
        await db.iceberg_sinks.update_one(
            {"id": sink_id}, {"$set": {"last_error": error_message, "updated_at": datetime.utcnow()}}
        )
        await _log_sink_audit(db, "iceberg_sink.disable", sink, sink_id, "Failed", error_message)
        return {"ok": False, "error": error_message}

    try:
        result = await delete_connector(connect_conn, sink["connector_name"])
    except Exception as e:
        result = {"ok": False, "error": str(e)[:300]}

    if not result.get("ok"):
        error_message = result.get("error") or "Failed to delete the Iceberg Sink connector."
        await db.iceberg_sinks.update_one(
            {"id": sink_id}, {"$set": {"last_error": error_message, "updated_at": datetime.utcnow()}}
        )
        await _log_sink_audit(db, "iceberg_sink.disable", sink, sink_id, "Failed", error_message)
        return {"ok": False, "error": error_message}

    await db.iceberg_sinks.update_one(
        {"id": sink_id}, {"$set": {"enabled": False, "last_error": None, "updated_at": datetime.utcnow()}}
    )
    await _log_sink_audit(db, "iceberg_sink.disable", sink, sink_id, "Success")
    updated = await db.iceberg_sinks.find_one({"id": sink_id}, {"_id": 0})
    return {"ok": True, "sink": sink_response(updated)}


async def _lifecycle_action(db, sink_id, action_name: str, client_fn) -> Dict[str, Any]:
    """Shared thin-wrapper body for pause/resume/restart."""
    sink = await db.iceberg_sinks.find_one({"id": sink_id}, {"_id": 0})
    if sink is None:
        return {"ok": False, "error": "Iceberg sink not found."}

    conns = await _resolve_sink_connections(db)
    connect_conn = conns.get("connect")
    if connect_conn is None:
        error_message = "No Kafka Connect connection configured."
        await db.iceberg_sinks.update_one(
            {"id": sink_id}, {"$set": {"last_error": error_message, "updated_at": datetime.utcnow()}}
        )
        await _log_sink_audit(db, f"iceberg_sink.{action_name}", sink, sink_id, "Failed", error_message)
        return {"ok": False, "error": error_message}

    try:
        result = await client_fn(connect_conn, sink["connector_name"])
    except Exception as e:
        result = {"ok": False, "error": str(e)[:300]}

    if not result.get("ok"):
        error_message = result.get("error") or f"Failed to {action_name} the Iceberg Sink connector."
        await db.iceberg_sinks.update_one(
            {"id": sink_id}, {"$set": {"last_error": error_message, "updated_at": datetime.utcnow()}}
        )
        await _log_sink_audit(db, f"iceberg_sink.{action_name}", sink, sink_id, "Failed", error_message)
        return {"ok": False, "error": error_message}

    update_fields: Dict[str, Any] = {"last_error": None, "updated_at": datetime.utcnow()}
    try:
        status_result = await get_connector_status(connect_conn, sink["connector_name"])
        if status_result.get("ok"):
            status_block = status_result.get("data") or {}
            connector_block = status_block.get("connector") or {} if isinstance(status_block, dict) else {}
            update_fields["last_status"] = {
                "state": connector_block.get("state") or "Unknown",
                "trace": connector_block.get("trace"),
                "source": "live",
                "checked_at": datetime.utcnow().isoformat(),
                "connector": connector_block,
            }
    except Exception:
        pass

    await db.iceberg_sinks.update_one({"id": sink_id}, {"$set": update_fields})
    await _log_sink_audit(db, f"iceberg_sink.{action_name}", sink, sink_id, "Success")
    updated = await db.iceberg_sinks.find_one({"id": sink_id}, {"_id": 0})
    return {"ok": True, "sink": sink_response(updated)}


async def pause_sink(db, sink_id) -> Dict[str, Any]:
    return await _lifecycle_action(db, sink_id, "pause", pause_connector)


async def resume_sink(db, sink_id) -> Dict[str, Any]:
    return await _lifecycle_action(db, sink_id, "resume", resume_connector)


async def restart_sink(db, sink_id) -> Dict[str, Any]:
    async def _restart(conn, name):
        return await restart_connector(conn, name, include_tasks=True, only_failed=True)

    return await _lifecycle_action(db, sink_id, "restart", _restart)


async def get_flow_sink_statuses(db, flow_id, live: bool = True) -> Dict[str, Any]:
    """One Kafka Connect call, matched to sink docs by connector_name.

    Persists the merged snapshot to sink.last_status. Falls back to the cached
    last_status (source="cache") when Connect is unreachable.
    """
    sinks = await list_sinks_for_flow(db, flow_id)
    if not sinks:
        return {"items": [], "connect_reachable": True}

    conns = await _resolve_sink_connections(db)
    connect_conn = conns.get("connect")

    connect_reachable = False
    connectors_by_name: Dict[str, Any] = {}
    if live and connect_conn:
        try:
            result = await list_connectors_with_status(connect_conn)
            connect_reachable = bool(result.get("reachable"))
            if result.get("ok") and isinstance(result.get("data"), dict):
                connectors_by_name = result["data"]
        except Exception:
            connect_reachable = False

    checked_at = datetime.utcnow().isoformat()
    items: List[Dict[str, Any]] = []

    for sink in sinks:
        connector_name = sink.get("connector_name")

        if not sink.get("enabled"):
            last_status = {"state": "Disabled", "source": "live", "checked_at": checked_at}
            await db.iceberg_sinks.update_one({"id": sink["id"]}, {"$set": {"last_status": last_status}})
            sink["last_status"] = last_status
        elif not connect_reachable:
            cached = sink.get("last_status")
            sink["last_status"] = (
                {**cached, "source": "cache"} if cached else {"state": "Unknown", "source": "cache", "checked_at": checked_at}
            )
        else:
            connector_info = connectors_by_name.get(connector_name) if connector_name else None
            if connector_info is None:
                last_status = {"state": "Not Deployed", "source": "live", "checked_at": checked_at}
            else:
                status_block = connector_info.get("status") or {} if isinstance(connector_info, dict) else {}
                connector_block = status_block.get("connector") or {} if isinstance(status_block, dict) else {}
                connector_state = connector_block.get("state") or "Unknown"

                truncated_tasks = []
                for task in status_block.get("tasks") or []:
                    if not isinstance(task, dict):
                        continue
                    task_copy = dict(task)
                    trace = task_copy.get("trace")
                    if isinstance(trace, str) and len(trace) > 500:
                        task_copy["trace"] = trace[:500]
                    truncated_tasks.append(task_copy)

                last_status = {
                    "state": connector_state,
                    "trace": connector_block.get("trace"),
                    "source": "live",
                    "checked_at": checked_at,
                    "connector": connector_block,
                    "tasks": truncated_tasks,
                }
            await db.iceberg_sinks.update_one({"id": sink["id"]}, {"$set": {"last_status": last_status}})
            sink["last_status"] = last_status

        items.append(sink)

    return {"items": items, "connect_reachable": connect_reachable}


async def deploy_sinks_for_flow(db, flow, source) -> List[Dict[str, Any]]:
    """Sync sinks for the flow, then best-effort enable every enabled sink."""
    sinks = await sync_sinks_for_flow(db, flow, source)
    results: List[Dict[str, Any]] = []

    for sink in sinks:
        stream_id = sink.get("stream_id")
        topic = sink.get("topic")

        if not sink.get("enabled"):
            results.append({"stream_id": stream_id, "topic": topic, "ok": True, "error": None})
            continue

        try:
            result = await enable_sink(db, sink["id"])
            ok = bool(result.get("ok"))
            error = None if ok else (result.get("error") or "Failed to enable Iceberg sink.")
        except Exception as e:
            ok = False
            error = str(e)[:300]
            try:
                await db.iceberg_sinks.update_one(
                    {"id": sink["id"]}, {"$set": {"last_error": error, "updated_at": datetime.utcnow()}}
                )
            except Exception:
                pass

        results.append({"stream_id": stream_id, "topic": topic, "ok": ok, "error": error})

    return results


async def delete_sinks_for_flow(db, flow_id) -> Dict[str, Any]:
    """Best-effort delete every connector for the flow's sinks, record orphaned Iceberg tables,
    then delete the sink docs. Iceberg TABLES ARE NEVER DROPPED.
    """
    sinks = await list_sinks_for_flow(db, flow_id)
    conns = await _resolve_sink_connections(db)
    connect_conn = conns.get("connect")
    iceberg_conn = conns.get("iceberg")

    for sink in sinks:
        connector_name = sink.get("connector_name")
        if connect_conn and connector_name:
            try:
                await delete_connector(connect_conn, connector_name)
            except Exception:
                pass

        try:
            warehouse = (iceberg_conn or {}).get("iceberg_warehouse") or "bronze"
            await record_orphan(
                db,
                system="iceberg",
                external_id=sink.get("table_name") or "",
                kind="table",
                connection_id=(iceberg_conn or {}).get("id") or sink.get("iceberg_connection_id"),
                reason=(
                    f"Flow '{flow_id}' deleted; Iceberg table '{sink.get('table_name')}' in warehouse "
                    f"'{warehouse}' (topic '{sink.get('topic')}') retained — Iceberg tables are never dropped."
                ),
            )
        except Exception:
            pass

    if sinks:
        await db.iceberg_sinks.delete_many({"flow_id": flow_id})

    return {"ok": True, "deleted_count": len(sinks)}
