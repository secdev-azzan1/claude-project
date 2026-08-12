"""Impact preview service for connection operations (read-only)."""
from motor.motor_asyncio import AsyncIOMotorDatabase


async def compute_impact(db: AsyncIOMotorDatabase, conn_id: str, operation: str) -> dict:
    """
    Compute the impact of an operation on a connection.

    Args:
        db: MongoDB database connection
        conn_id: Connection ID
        operation: Operation name (delete|repoint|migrate|reset|activate)

    Returns:
        Dict containing connection info, affected flows, services, schemas, and counts
    """
    # Get the connection document
    conn = await db.connections.find_one({"id": conn_id}, {"_id": 0})
    if not conn:
        return None

    # Query flows where nifi_connection_id==conn_id OR kafka_connection_id==conn_id (cap 500)
    flows_cursor = db.flows.find(
        {
            "$or": [
                {"nifi_connection_id": conn_id},
                {"kafka_connection_id": conn_id},
            ]
        },
        {
            "_id": 0,
            "id": 1,
            "name": 1,
            "state": 1,
            "nifi_process_group_id": 1,
            "kafka_topic": 1,
        }
    ).limit(500)
    flows = await flows_cursor.to_list(500)

    # Query nifi_global_services where nifi_connection_id==conn_id (cap 500)
    services_cursor = db.nifi_global_services.find(
        {"nifi_connection_id": conn_id},
        {
            "_id": 0,
            "id": 1,
            "name": 1,
            "service_kind": 1,
            "is_default": 1,
        }
    ).limit(500)
    global_services = await services_cursor.to_list(500)

    # Query schema_artifacts versions where apicurio_connection_id==conn_id (cap 500)
    # Since versions is an array of sub-documents, we use $elemMatch to find matching versions
    schema_artifacts_cursor = db.schema_artifacts.find(
        {"versions.apicurio_connection_id": conn_id},
        {
            "_id": 0,
            "id": 1,
            "artifact_id": 1,
            "versions": 1,
        }
    ).limit(500)
    schema_artifacts = await schema_artifacts_cursor.to_list(500)

    # Extract schema versions matching this connection
    schema_versions = []
    for artifact in schema_artifacts:
        artifact_id = artifact.get("id")
        for version_doc in artifact.get("versions", []):
            if version_doc.get("apicurio_connection_id") == conn_id:
                schema_versions.append({
                    "artifact_id": artifact_id,
                    "version": version_doc.get("version"),
                })
                if len(schema_versions) >= 500:
                    break
        if len(schema_versions) >= 500:
            break

    # Query iceberg_sinks depending on connection type (cap 500)
    conn_type = conn.get("type")
    if conn_type == "kafka_connect":
        iceberg_sink_filter = {"connect_connection_id": conn_id}
    elif conn_type == "iceberg":
        iceberg_sink_filter = {"iceberg_connection_id": conn_id}
    elif conn_type == "kafka":
        iceberg_sink_filter = {"kafka_connection_id": conn_id}
    else:
        iceberg_sink_filter = None

    if iceberg_sink_filter is not None:
        iceberg_sinks_cursor = db.iceberg_sinks.find(
            iceberg_sink_filter,
            {
                "_id": 0,
                "id": 1,
                "flow_id": 1,
                "topic": 1,
                "table_name": 1,
                "connector_name": 1,
                "enabled": 1,
            }
        ).limit(500)
        iceberg_sinks = await iceberg_sinks_cursor.to_list(500)
    else:
        iceberg_sinks = []

    # Determine kafka_identity
    # "known" if this is a kafka connection whose kafka_cluster_id can be proven
    kafka_identity = "unknown"
    if conn.get("type") == "kafka":
        # Check if any flow with this kafka_connection_id has a kafka_cluster_id
        kafka_flow = await db.flows.find_one(
            {
                "kafka_connection_id": conn_id,
                "kafka_cluster_id": {"$exists": True, "$ne": None},
            },
            {"_id": 0, "kafka_cluster_id": 1}
        )
        if kafka_flow and kafka_flow.get("kafka_cluster_id"):
            kafka_identity = "known"

    return {
        "connection": {
            "id": conn.get("id"),
            "type": conn.get("type"),
            "name": conn.get("name"),
            "is_active": conn.get("is_active", False),
            "reachability": conn.get("reachability", "Unknown"),
        },
        "operation": operation,
        "flows": flows,
        "global_services": global_services,
        "schema_versions": schema_versions,
        "iceberg_sinks": iceberg_sinks,
        "kafka_identity": kafka_identity,
        "counts": {
            "flows": len(flows),
            "global_services": len(global_services),
            "schema_versions": len(schema_versions),
            "iceberg_sinks": len(iceberg_sinks),
        },
    }
