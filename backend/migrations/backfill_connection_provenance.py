"""Backfill connection provenance: stamp connection IDs and fingerprints on existing docs.

Idempotent migration: only sets fields that are currently missing/None.
Re-running this script should result in no changes.
"""
import sys
from pathlib import Path

# Bootstrap sys.path to allow service imports from standalone execution
BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import os
import asyncio
from pymongo import MongoClient

MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "nif_abstractor")

client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=5000)
db = client[DB_NAME]


async def probe_nifi_fingerprint(conn: dict):
    """Probe NiFi fingerprint. Reuses the fingerprint service logic."""
    from services.connection_fingerprint import probe_nifi_fingerprint as probe_fn
    return await probe_fn(conn)


async def probe_kafka_fingerprint(conn: dict):
    """Probe Kafka fingerprint. Reuses the fingerprint service logic."""
    from services.connection_fingerprint import probe_kafka_fingerprint as probe_fn
    return await probe_fn(conn)


async def probe_apicurio_fingerprint(conn: dict):
    """Probe Apicurio fingerprint. Reuses the fingerprint service logic."""
    from services.connection_fingerprint import probe_apicurio_fingerprint as probe_fn
    return await probe_fn(conn)


async def backfill_nifi():
    """Backfill NiFi connection ID and fingerprint to flows, nifi_global_services, and schema_inference_jobs."""
    print("\n--- Backfilling NiFi ---")

    # Load the single NiFi connection doc
    nifi_conn = db.connections.find_one({"type": "nifi"})
    if not nifi_conn:
        print("No NiFi connection found. Skipping.")
        return

    nifi_id = nifi_conn.get("id")
    print(f"Found NiFi connection: {nifi_id}")

    # Probe fingerprint
    result = await probe_nifi_fingerprint(nifi_conn)
    print(f"  Probe result: reachable={result['reachable']}, ok={result['ok']}, error={result['error']}")

    nifi_fp = result.get("fingerprint") if result["ok"] else None

    # Backfill db.flows
    flows_filter = {"$or": [{"nifi_connection_id": {"$exists": False}}, {"nifi_connection_id": None}]}
    update_flows = {"$set": {"nifi_connection_id": nifi_id}}
    if nifi_fp:
        update_flows["$set"]["nifi_instance_fingerprint"] = nifi_fp

    flows_result = db.flows.update_many(flows_filter, update_flows)
    print(f"  Updated db.flows: {flows_result.modified_count} docs")

    # Backfill db.nifi_global_services
    services_filter = {"$or": [{"nifi_connection_id": {"$exists": False}}, {"nifi_connection_id": None}]}
    update_services = {"$set": {"nifi_connection_id": nifi_id}}
    if nifi_fp:
        update_services["$set"]["nifi_instance_fingerprint"] = nifi_fp

    services_result = db.nifi_global_services.update_many(services_filter, update_services)
    print(f"  Updated db.nifi_global_services: {services_result.modified_count} docs")

    # Fill fingerprints INDEPENDENTLY of connection_id: an earlier UNREACHABLE run
    # may have stamped connection_id but left the fingerprint None. A later reachable
    # run must still fill it. Filter on the fingerprint field being missing/None.
    if nifi_fp:
        fp_filter = {"$or": [{"nifi_instance_fingerprint": {"$exists": False}}, {"nifi_instance_fingerprint": None}]}
        fp_flows = db.flows.update_many(fp_filter, {"$set": {"nifi_instance_fingerprint": nifi_fp}})
        fp_services = db.nifi_global_services.update_many(fp_filter, {"$set": {"nifi_instance_fingerprint": nifi_fp}})
        print(f"  Filled NiFi fingerprint: {fp_flows.modified_count} flows, {fp_services.modified_count} services")

    # Backfill db.schema_inference_jobs
    jobs_filter = {"$or": [{"nifi_connection_id": {"$exists": False}}, {"nifi_connection_id": None}]}
    jobs_result = db.schema_inference_jobs.update_many(jobs_filter, {"$set": {"nifi_connection_id": nifi_id}})
    print(f"  Updated db.schema_inference_jobs: {jobs_result.modified_count} docs")


async def backfill_kafka():
    """Backfill Kafka connection ID and cluster ID to flows."""
    print("\n--- Backfilling Kafka ---")

    # Load the single Kafka connection doc
    kafka_conn = db.connections.find_one({"type": "kafka"})
    if not kafka_conn:
        print("No Kafka connection found. Skipping.")
        return

    kafka_id = kafka_conn.get("id")
    print(f"Found Kafka connection: {kafka_id}")

    # Probe fingerprint
    result = await probe_kafka_fingerprint(kafka_conn)
    print(f"  Probe result: reachable={result['reachable']}, ok={result['ok']}, error={result['error']}")

    kafka_fp = result.get("fingerprint") if result["ok"] else None

    # Backfill db.flows
    flows_filter = {"$or": [{"kafka_connection_id": {"$exists": False}}, {"kafka_connection_id": None}]}
    update_flows = {"$set": {"kafka_connection_id": kafka_id}}
    if kafka_fp:
        update_flows["$set"]["kafka_cluster_id"] = kafka_fp

    flows_result = db.flows.update_many(flows_filter, update_flows)
    print(f"  Updated db.flows: {flows_result.modified_count} docs")

    # Fill kafka_cluster_id independently of connection_id (unreachable-first, reachable-second).
    if kafka_fp:
        fp_filter = {"$or": [{"kafka_cluster_id": {"$exists": False}}, {"kafka_cluster_id": None}]}
        fp_flows = db.flows.update_many(fp_filter, {"$set": {"kafka_cluster_id": kafka_fp}})
        print(f"  Filled Kafka cluster id: {fp_flows.modified_count} flows")


async def backfill_apicurio():
    """Backfill Apicurio connection ID to schema_artifacts versions."""
    print("\n--- Backfilling Apicurio ---")

    # Load the single Apicurio connection doc
    apicurio_conn = db.connections.find_one({"type": "apicurio"})
    if not apicurio_conn:
        print("No Apicurio connection found. Skipping.")
        return

    apicurio_id = apicurio_conn.get("id")
    print(f"Found Apicurio connection: {apicurio_id}")

    # Probe fingerprint (will be None for Apicurio)
    result = await probe_apicurio_fingerprint(apicurio_conn)
    print(f"  Probe result: reachable={result['reachable']}, ok={result['ok']}, error={result['error']}")

    # For each schema_artifacts doc, update versions that lack apicurio_connection_id
    artifacts = list(db.schema_artifacts.find({}))
    total_updated = 0

    for artifact in artifacts:
        versions = artifact.get("versions", [])
        updated_versions = []
        needs_update = False

        for version in versions:
            if version.get("apicurio_connection_id") is None:
                version["apicurio_connection_id"] = apicurio_id
                needs_update = True
            updated_versions.append(version)

        if needs_update:
            db.schema_artifacts.update_one(
                {"_id": artifact["_id"]},
                {"$set": {"versions": updated_versions}}
            )
            total_updated += 1

    print(f"  Updated db.schema_artifacts: {total_updated} docs with versions")


async def main():
    """Main migration entry point."""
    print("Starting backfill migration...")

    try:
        await backfill_nifi()
        await backfill_kafka()
        await backfill_apicurio()
        print("\n=== Backfill migration complete ===")
    except Exception as e:
        print(f"ERROR: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
