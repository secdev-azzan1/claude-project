from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from pathlib import Path
import os
import logging
import uuid
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


@app.get("/api", tags=["health"], summary="Backend health/version check")
async def root():
    return {"message": "NIF Abstractor API", "version": "1.0.0"}


async def seed_default_connections():
    """Seed default connections on first startup if none exist, then test them."""
    db = database.get_db()
    count = await db.connections.count_documents({})
    if count > 0:
        logger.info(f"Connections already seeded ({count} found). Skipping.")
        return

    now = datetime.utcnow()
    nifi_url = os.environ.get('NIFI_URL', 'https://f4bj6zvj-8443.inc1.devtunnels.ms')
    nifi_username = os.environ.get('NIFI_USERNAME', 'admin')
    nifi_password = os.environ.get('NIFI_PASSWORD', '')
    kafka_bootstrap = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'f4bj6zvj-9092.inc1.devtunnels.ms:9092')
    apicurio_url = os.environ.get('APICURIO_URL', 'https://f4bj6zvj-8084.inc1.devtunnels.ms')

    # ── Test each connection now so they start as Healthy ──────────────────
    from services.nifi_client import test_nifi_connection
    from services.kafka_client import test_kafka_connection
    from services.apicurio_client import test_apicurio_connection

    logger.info("Testing connections during seed...")

    nifi_result = await test_nifi_connection(nifi_url, auth_type="BASIC",
                                             username=nifi_username, password=nifi_password)
    nifi_health = "Healthy" if nifi_result.get("ok") else "Failed"
    logger.info(f"NiFi seed test: {nifi_health} — {nifi_result.get('message') or nifi_result.get('error')}")

    kafka_result = await test_kafka_connection(kafka_bootstrap)
    kafka_health = "Healthy" if kafka_result.get("ok") else "Failed"
    logger.info(f"Kafka seed test: {kafka_health} — {kafka_result.get('message') or kafka_result.get('error')}")

    apicurio_result = await test_apicurio_connection(apicurio_url)
    apicurio_health = "Healthy" if apicurio_result.get("ok") else "Failed"
    logger.info(f"Apicurio seed test: {apicurio_health} — {apicurio_result.get('message') or apicurio_result.get('error')}")

    tested_at = datetime.utcnow()

    default_connections = [
        {
            "id": str(uuid.uuid4()),
            "name": "Apache NiFi",
            "type": "nifi",
            "description": "NiFi cluster API for flow deployment",
            "endpoint": nifi_url,
            "auth_type": "BASIC",
            "username": nifi_username,
            "password": nifi_password,
            "health": nifi_health,
            "last_tested": tested_at,
            "created_at": now,
            "updated_at": tested_at,
            "is_active": True,
        },
        {
            "id": str(uuid.uuid4()),
            "name": "Kafka",
            "type": "kafka",
            "description": "Bootstrap broker cluster",
            "endpoint": kafka_bootstrap,
            "kafka_connection_mode": "native",
            "security_protocol": "PLAINTEXT",
            "health": kafka_health,
            "last_tested": tested_at,
            "created_at": now,
            "updated_at": tested_at,
            "is_active": True,
        },
        {
            "id": str(uuid.uuid4()),
            "name": "Apicurio Schema Registry",
            "type": "apicurio",
            "description": "Avro schema registry for NiFi flows",
            "endpoint": apicurio_url,
            "auth_type": "NONE",
            "group_id": "nif-platform",
            "health": apicurio_health,
            "last_tested": tested_at,
            "created_at": now,
            "updated_at": tested_at,
            "is_active": True,
        },
    ]

    await db.connections.insert_many(default_connections)
    logger.info(f"Seeded 3 connections — NiFi: {nifi_health}, Kafka: {kafka_health}, Apicurio: {apicurio_health}")

    # Seed default settings
    from models.settings import PlatformSettings
    settings_count = await db.settings.count_documents({})
    if settings_count == 0:
        defaults = PlatformSettings()
        doc = defaults.dict()
        doc["id"] = "platform"
        await db.settings.insert_one(doc)
        logger.info("Seeded default platform settings.")

    # Seed initial audit event
    from models.audit import AuditEvent
    event = AuditEvent(action="Platform initialized", object_type="System", target="NIF Abstractor", status="Success",
                       details=f"Default connections seeded — NiFi: {nifi_health}, Kafka: {kafka_health}, Apicurio: {apicurio_health}")
    await db.audit_events.insert_one(event.dict())


async def ensure_indexes():
    db = database.get_db()
    index_specs = [
        (db.connections, [("id", ASCENDING)], {"unique": True, "name": "uniq_connections_id"}),
        (db.connections, [("type", ASCENDING)], {"unique": True, "name": "uniq_active_connection_per_type", "partialFilterExpression": {"is_active": True}}),
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
        (db.orphaned_artifacts, [("id", ASCENDING)], {"unique": True, "name": "uniq_orphaned_artifact_id"}),
        (db.iceberg_sinks, [("id", ASCENDING)], {"unique": True, "name": "uniq_iceberg_sink_id"}),
        (db.iceberg_sinks, [("flow_id", ASCENDING), ("stream_id", ASCENDING)], {"unique": True, "name": "uniq_iceberg_sink_flow_stream"}),
        (db.iceberg_sinks, [("connector_name", ASCENDING)], {"unique": True, "name": "uniq_iceberg_sink_connector"}),
        (db.iceberg_sinks, [("flow_id", ASCENDING)], {"name": "idx_iceberg_sinks_flow_id"}),
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
    await seed_default_connections()
    logger.info("NIF Abstractor API started successfully.")


@app.on_event("shutdown")
async def shutdown():
    await database.close_db()
    logger.info("Database connection closed.")
