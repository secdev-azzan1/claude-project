from fastapi import APIRouter, Depends
from motor.motor_asyncio import AsyncIOMotorDatabase

from db import get_db
from services.connection_resolver import resolve_connection
from services.iceberg_sink_config import ICEBERG_CONNECTOR_CLASS
from services.kafka_connect_client import get_cluster_info, list_connector_plugins, list_connectors_with_status


router = APIRouter(prefix="/api/kafka-connect", tags=["kafka-connect"])


@router.get("/cluster", summary="Get Kafka Connect cluster info + Iceberg plugin/cluster-match status")
async def get_cluster(db: AsyncIOMotorDatabase = Depends(get_db)):
    connect_conn = await resolve_connection(db, "kafka_connect", required=False)
    if not connect_conn:
        return {"ok": False, "reachable": False, "error": "No active Kafka Connect connection."}

    cluster_result = await get_cluster_info(connect_conn)
    if not cluster_result.get("ok"):
        return {
            "ok": False,
            "reachable": bool(cluster_result.get("reachable")),
            "error": cluster_result.get("error") or "Could not reach Kafka Connect.",
        }

    data = cluster_result.get("data") or {}
    version = data.get("version") if isinstance(data, dict) else None
    commit = data.get("commit") if isinstance(data, dict) else None
    connect_cluster_id = data.get("kafka_cluster_id") if isinstance(data, dict) else None

    plugins_result = await list_connector_plugins(connect_conn)
    plugin_classes = set()
    if plugins_result.get("ok") and isinstance(plugins_result.get("data"), list):
        plugin_classes = {
            plugin.get("class") for plugin in plugins_result["data"] if isinstance(plugin, dict)
        }
    iceberg_plugin_installed = ICEBERG_CONNECTOR_CLASS in plugin_classes

    # Mirror services.iceberg_sinks.preflight_sink's cluster_match resolution: never guess a
    # cluster match, only report it when both sides have a proven cluster id.
    kafka_conn = await resolve_connection(db, "kafka", required=False)
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
        cluster_match = None
        cluster_match_detail = (
            "Could not verify Kafka Connect and Kafka point to the same cluster "
            "(no proven cluster id available yet)."
        )
    elif connect_cluster_id == proven_kafka_cluster_id:
        cluster_match = True
        cluster_match_detail = "Kafka Connect and Kafka cluster IDs match."
    else:
        cluster_match = False
        cluster_match_detail = (
            f"Kafka Connect cluster id '{connect_cluster_id}' does not match the Kafka "
            f"connection's proven cluster id '{proven_kafka_cluster_id}'."
        )

    return {
        "ok": True,
        "reachable": True,
        "version": version,
        "commit": commit,
        "kafka_cluster_id": connect_cluster_id,
        "iceberg_plugin_installed": iceberg_plugin_installed,
        "cluster_match": cluster_match,
        "cluster_match_detail": cluster_match_detail,
    }


@router.get("/orphans", summary="Report unmanaged/missing Iceberg Sink connectors (report-only)")
async def get_orphans(db: AsyncIOMotorDatabase = Depends(get_db)):
    connect_conn = await resolve_connection(db, "kafka_connect", required=False)
    if not connect_conn:
        return {"ok": False, "error": "No active Kafka Connect connection."}

    result = await list_connectors_with_status(connect_conn)
    if not result.get("ok"):
        return {"ok": False, "error": result.get("error") or "Could not list Kafka Connect connectors."}

    connectors_by_name = result.get("data") or {}
    if not isinstance(connectors_by_name, dict):
        connectors_by_name = {}
    iceberg_connector_names = {
        name for name in connectors_by_name if isinstance(name, str) and name.endswith("__iceberg")
    }

    sink_connector_names = set()
    cursor = db.iceberg_sinks.find({"enabled": True}, {"_id": 0, "connector_name": 1})
    async for doc in cursor:
        connector_name = doc.get("connector_name")
        if connector_name:
            sink_connector_names.add(connector_name)

    unmanaged_connectors = sorted(iceberg_connector_names - sink_connector_names)
    missing_connectors = sorted(sink_connector_names - iceberg_connector_names)

    return {
        "ok": True,
        "unmanaged_connectors": unmanaged_connectors,
        "missing_connectors": missing_connectors,
    }
