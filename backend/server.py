from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from pathlib import Path
import os
import logging
from datetime import datetime
from pymongo import ASCENDING
import asyncio
from services.runtime_recovery import APP_INSTANCE_ID, reconcile_runtime_state

ROOT_DIR = Path(__file__).parent


def load_environment(root_dir: Path = ROOT_DIR) -> None:
    """Load backend-local env first, then repo-root env for local development."""
    load_dotenv(root_dir / ".env")
    load_dotenv(root_dir.parent / ".env")


load_environment()

import db as database
from routers import application_services, connections, dashboard, audit, settings, sources, schemas, flows, flow_import, webhooks, openapi_specs, nifi_services, content_store
from routers import schema_inference
from routers import iceberg_sinks, kafka_connect
from routers.v2 import openapi as v2_openapi
from routers.v2 import connections as v2_connections
from routers.v2 import services as v2_services
from routers.v2 import gateway as v2_gateway
from routers.v2 import schemas as v2_schemas
from routers.v2 import flows as v2_flows
from routers.v2 import dashboard as v2_dashboard
from routers.v2 import audit as v2_audit
from routers.v2 import schema_inference as v2_schema_inference

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

tags_metadata = [
    {
        "name": "connections",
        "description": "Configure and test NiFi, Kafka, and Apicurio service connections.",
    },
    {
        "name": "nifi-services",
        "description": "Create and manage inherited NiFi global controller services.",
    },
    {
        "name": "application-services",
        "description": "Create and manage reusable external source connection profiles.",
    },
    {
        "name": "sources",
        "description": "Create source definitions, configure streams, and test REST, SMB, or MongoDB stream previews.",
    },
    {
        "name": "flows",
        "description": "Create, deploy, run, stop, undeploy, inspect metrics, and inspect Kafka output for flows.",
    },
    {
        "name": "schemas",
        "description": "Manage Avro schema artifacts, versions, generation, verification, and deletion.",
    },
    {
        "name": "schema-inference",
        "description": "Run temporary NiFi/Kafka schema inference jobs and accept generated schemas.",
    },
    {
        "name": "webhooks",
        "description": "Receive inbound webhook events for Webhook sources and publish processed records to Kafka.",
    },
    {
        "name": "settings",
        "description": "Read and update platform defaults.",
    },
    {
        "name": "dashboard",
        "description": "Dashboard summary and flow summary endpoints.",
    },
    {
        "name": "audit",
        "description": "Read audit events produced by API operations.",
    },
    {
        "name": "openapi",
        "description": "Upload and parse OpenAPI/Swagger specs to power REST stream setup suggestions.",
    },
    {
        "name": "content-store",
        "description": "Internal operations for validating and rematerializing the content-store mirror.",
    },
]

app = FastAPI(
    title="NIF Abstractor API",
    version="1.0.0",
    description=(
        "Backend API for building source-to-Kafka/NiFi flows. "
        "Authentication is intentionally disabled for the current MVP test environment. "
        "Use this Swagger UI to manually create connections, sources, flows, schemas, "
        "test streams, deploy flows, inspect Kafka messages, and run schema inference."
    ),
    openapi_tags=tags_metadata,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    swagger_ui_parameters={
        "docExpansion": "none",
        "defaultModelsExpandDepth": 1,
        "defaultModelExpandDepth": 2,
        "displayRequestDuration": True,
        "persistAuthorization": False,
        "tryItOutEnabled": True,
    },
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)

# Include all routers
app.include_router(connections.router)
app.include_router(application_services.router)
app.include_router(nifi_services.router)
app.include_router(dashboard.router)
app.include_router(audit.router)
app.include_router(settings.router)
app.include_router(sources.router)
app.include_router(schemas.router)
app.include_router(flows.router)
app.include_router(flow_import.router)
app.include_router(schema_inference.router)
app.include_router(webhooks.router)
app.include_router(openapi_specs.router)
app.include_router(content_store.router)
app.include_router(iceberg_sinks.router)
app.include_router(kafka_connect.router)
app.include_router(v2_openapi.router)
app.include_router(v2_connections.router)
app.include_router(v2_services.router)
app.include_router(v2_gateway.router)
app.include_router(v2_schemas.router)
app.include_router(v2_flows.router)
app.include_router(v2_dashboard.router)
app.include_router(v2_audit.router)
app.include_router(v2_schema_inference.router)


@app.get("/api", tags=["health"], summary="Backend health/version check")
async def root():
    return {"message": "NIF Abstractor API", "version": "1.0.0"}


async def seed_default_settings():
    """Seed the platform settings document on first startup if none exists.

    Deliberately does not touch connections: no default connection should
    ever be created or live-tested automatically on boot.
    """
    db = database.get_db()
    from models.settings import PlatformSettings
    settings_count = await db.settings.count_documents({})
    if settings_count == 0:
        defaults = PlatformSettings()
        doc = defaults.dict()
        doc["id"] = "platform"
        await db.settings.insert_one(doc)
        logger.info("Seeded default platform settings.")


async def ensure_indexes():
    db = database.get_db()
    index_specs = [
        (db.connections, [("id", ASCENDING)], {"unique": True, "name": "uniq_connections_id"}),
        (db.connections, [("type", ASCENDING)], {"unique": True, "name": "uniq_active_connection_per_type", "partialFilterExpression": {"is_active": True}}),
        (db.connections_v2, [("type", ASCENDING)], {"unique": True, "name": "uniq_v2_connection_type"}),
        (db.application_services, [("id", ASCENDING)], {"unique": True, "name": "uniq_application_services_id"}),
        (db.sources, [("id", ASCENDING)], {"unique": True, "name": "uniq_sources_id"}),
        (db.flows, [("id", ASCENDING)], {"unique": True, "name": "uniq_flows_id"}),
        (
            db.flows,
            [("name", ASCENDING)],
            {
                "unique": True,
                "name": "uniq_flows_name",
                "collation": {"locale": "en", "strength": 2},
            },
        ),
        (db.schema_artifacts, [("artifact_id", ASCENDING)], {"unique": True, "name": "uniq_schema_artifact"}),
        (db.schema_inference_jobs, [("id", ASCENDING)], {"unique": True, "name": "uniq_inference_job_id"}),
        (db.settings, [("id", ASCENDING)], {"unique": True, "name": "uniq_settings_id"}),
        (db.webhook_samples, [("flow_id", ASCENDING), ("created_at", ASCENDING)], {"name": "idx_webhook_samples_flow_created"}),
        (db.webhook_samples, [("id", ASCENDING)], {"unique": True, "name": "uniq_webhook_sample_id"}),
        (db.webhook_request_state, [("flow_id", ASCENDING), ("request_key", ASCENDING)], {"unique": True, "name": "uniq_webhook_request_key"}),
        (db.flows, [("schema_artifact_id", ASCENDING), ("schema_version", ASCENDING)], {"name": "idx_flows_schema_link"}),
        (db.flows, [("source_id", ASCENDING)], {"name": "idx_flows_source_id"}),
        (db.openapi_specs, [("id", ASCENDING)], {"unique": True, "name": "uniq_openapi_specs_id"}),
        (db.openapi_specs, [("checksum_sha256", ASCENDING)], {"name": "idx_openapi_specs_checksum"}),
        (db.connection_lifecycle_jobs, [("id", ASCENDING)], {"unique": True, "name": "uniq_lifecycle_job_id"}),
        (db.connection_lifecycle_jobs, [("status", ASCENDING)], {"name": "idx_lifecycle_job_status"}),
        (db.connection_lifecycle_jobs, [("owner_instance_id", ASCENDING)], {"name": "idx_lifecycle_job_owner"}),
        (db.bulk_jobs_v2, [("id", ASCENDING)], {"unique": True, "name": "uniq_bulk_job_id"}),
        (db.bulk_jobs_v2, [("status", ASCENDING), ("created_at", ASCENDING)], {"name": "idx_bulk_job_status_created"}),
        (db.orphaned_artifacts, [("id", ASCENDING)], {"unique": True, "name": "uniq_orphaned_artifact_id"}),
        (db.iceberg_sinks, [("id", ASCENDING)], {"unique": True, "name": "uniq_iceberg_sink_id"}),
        (db.iceberg_sinks, [("flow_id", ASCENDING), ("stream_id", ASCENDING)], {"unique": True, "name": "uniq_iceberg_sink_flow_stream"}),
        (db.iceberg_sinks, [("connector_name", ASCENDING)], {"unique": True, "name": "uniq_iceberg_sink_connector"}),
        (db.iceberg_sinks, [("flow_id", ASCENDING)], {"name": "idx_iceberg_sinks_flow_id"}),
        (db.kafka_connect_syncs_v2, [("id", ASCENDING)], {"unique": True, "name": "uniq_kafka_connect_sync_id"}),
        (db.kafka_connect_syncs_v2, [("connector_name", ASCENDING)], {"unique": True, "name": "uniq_kafka_connect_sync_connector"}),
        (db.kafka_connect_syncs_v2, [("linked_flow_id", ASCENDING)], {"name": "idx_kafka_connect_sync_flow"}),
        (db.bulk_queue_leases_v2, [("id", ASCENDING)], {"unique": True, "name": "uniq_bulk_queue_lease_id"}),
        (db.schema_inference_jobs_v2, [("id", ASCENDING)], {"unique": True, "name": "uniq_v2_schema_inference_job_id"}),
        (db.schema_inference_jobs_v2, [("flowId", ASCENDING), ("targetBlockId", ASCENDING), ("status", ASCENDING)], {"name": "idx_v2_schema_inference_target_status"}),
    ]
    for coll, keys, options in index_specs:
        try:
            await asyncio.wait_for(coll.create_index(keys, **options), timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("Index creation timed out for %s (%s). Skipping for startup.", coll.name, options.get("name"))
        except Exception as exc:
            logger.warning("Index creation skipped for %s (%s): %s", coll.name, options.get("name"), exc)


async def recover_runtime_state_background():
    try:
        recovery_report = await reconcile_runtime_state(
            database.get_db(),
            instance_id=APP_INSTANCE_ID,
            now=datetime.utcnow(),
        )
        logger.info("Runtime recovery report: %s", recovery_report.to_dict())
    except Exception as exc:
        logger.warning("Runtime recovery did not complete: %s", exc)


@app.on_event("startup")
async def startup():
    await database.init_db()
    logger.info("Database connection initialized.")
    asyncio.create_task(recover_runtime_state_background())
    asyncio.create_task(ensure_indexes())
    await seed_default_settings()
    logger.info("NIF Abstractor API started successfully.")


@app.on_event("shutdown")
async def shutdown():
    await database.close_db()
    logger.info("Database connection closed.")
