"""Build the rapid7_asyad.maximum_useful NiFi flow using native processors only.

No ExecuteGroovyScript anywhere. The dedupe fingerprint is produced by
CryptographicHashContent over the *raw source payload*, before any metadata is
added, which is what keeps the 10 standard fields out of the hash.

Shape per entity, mirroring rapid7_securado.asset:

    init_page -> fetch -> split -+- original -> page_meta -> has_more -> next_page -> fetch
                                 `- split    -> extract -> [detail_fetch] -> hash
                                                -> set_ids -> set_metadata -> dedupe_key
                                                -> dedupe -> raw_publish

Subcommands: build-raw | inspect | stop
Nothing here triggers the flow. Run it manually and deliberately.
"""

import json
import os
import sys
import time
import urllib.parse

import requests

import build_fortisiem_maximum_useful as n


SOURCE_INSTANCE = os.environ.get("RAPID7_ASYAD_SOURCE_INSTANCE", "rapid7_asyad")
PG_NAME = os.environ.get("RAPID7_ASYAD_PG_NAME", f"{SOURCE_INSTANCE}.maximum_useful")
PARENT_PG_ID = os.environ.get("RAPID7_ASYAD_PARENT_PG_ID", "0a00e822-01a0-1000-68b7-f28e69779c95")
CTX_NAME = os.environ.get("RAPID7_ASYAD_CTX_NAME", f"{SOURCE_INSTANCE}.maximum")

SOURCE_API_BASE = os.environ.get("RAPID7_ASYAD_API_BASE", "http://apisix:9080/rapid7_asyad/api/3")
HTTP_USERNAME = os.environ.get("RAPID7_ASYAD_HTTP_USERNAME", "apiuser")
HTTP_PASSWORD = os.environ.get("RAPID7_ASYAD_HTTP_PASSWORD")  # required on first build only
PAGE_SIZE = os.environ.get("RAPID7_ASYAD_PAGE_SIZE", "500")
BLOCKED_SITES = os.environ.get("RAPID7_ASYAD_BLOCKED_SITES", "")
ASSET_RATE_PER_SEC = os.environ.get("RAPID7_ASYAD_ASSET_RATE", "2")

REDIS_POOL_ID = os.environ.get("REDIS_POOL_ID", "b90bcbdb-d69c-3725-51d1-444dd57b9336")

n.PG_NAME = PG_NAME
n.PARENT_PG_ID = PARENT_PG_ID
n.CLIENT_ID = "codex-rapid7-asyad-maximum"
n.PG_ID = None

HASH_ATTR = "${'content_SHA-256'}"

STANDARD_VALUE_FIELDS = n.STANDARD_VALUE_FIELDS


def topic(entity):
    return f"bronze.{SOURCE_INSTANCE}.{entity}__raw"


# --------------------------------------------------------------------------- context


def ensure_param_context():
    existing = n.nifi("GET", "/nifi-api/flow/parameter-contexts")
    for c in existing.get("parameterContexts", []):
        if c["component"]["name"] == CTX_NAME:
            return c["id"]
    if not HTTP_PASSWORD:
        raise RuntimeError("Set RAPID7_ASYAD_HTTP_PASSWORD on first build so the sensitive parameter can be created")
    params = [
        {"parameter": {"name": "SOURCE_API_BASE", "value": SOURCE_API_BASE, "sensitive": False}},
        {"parameter": {"name": "HTTP_USERNAME", "value": HTTP_USERNAME, "sensitive": False}},
        {"parameter": {"name": "HTTP_PASSWORD", "value": HTTP_PASSWORD, "sensitive": True}},
        {"parameter": {"name": "PAGE_SIZE", "value": PAGE_SIZE, "sensitive": False}},
        {"parameter": {"name": "BLOCKED_SITES", "value": BLOCKED_SITES, "sensitive": False}},
        {"parameter": {"name": "SOURCE_INSTANCE", "value": SOURCE_INSTANCE, "sensitive": False}},
    ]
    payload = {"revision": {"clientId": n.CLIENT_ID, "version": 0}, "component": {"name": CTX_NAME, "description": "Rapid7 Asyad maximum-useful ingestion", "parameters": params}}
    return n.nifi("POST", "/nifi-api/parameter-contexts", payload)["id"]


# --------------------------------------------------------------------------- services


def services():
    dmc = n.create_controller_service(
        f"{SOURCE_INSTANCE}.maximum__dedupe__cache",
        "org.apache.nifi.redis.service.RedisDistributedMapCacheClientService",
        {"Redis Connection Pool": REDIS_POOL_ID, "TTL": "24 hours"},
    )
    reader = n.create_controller_service(
        f"{SOURCE_INSTANCE}.maximum__json_reader",
        "org.apache.nifi.json.JsonTreeReader",
        {"Schema Access Strategy": "infer-schema", "Starting Field Strategy": "ROOT_NODE"},
    )
    writer = n.create_controller_service(
        f"{SOURCE_INSTANCE}.maximum__json_writer",
        "org.apache.nifi.json.JsonRecordSetWriter",
        {"Schema Write Strategy": "no-schema", "Schema Access Strategy": "inherit-record-schema", "Output Grouping": "output-oneline"},
    )
    return dmc, reader, writer


# --------------------------------------------------------------------------- helpers


def invoke(name, url, x, y):
    props = n.invoke_props(url)
    props["Request Username"] = "#{HTTP_USERNAME}"
    props["Request Password"] = "#{HTTP_PASSWORD}"
    return n.create_processor(name, "org.apache.nifi.processors.standard.InvokeHTTP", x, y, props, ["Original", "Retry", "No Retry", "Failure"])


def paged_branch(entity, list_path, x, y, extract_paths):
    """init_page -> fetch -> split (+ pagination loop) -> extract. Returns (init_id, extract_id)."""
    init = n.create_processor(
        f"{SOURCE_INSTANCE}.{entity}__init_page", "org.apache.nifi.processors.attributes.UpdateAttribute", x, y,
        {"Store State": "Do not store state", "page": "0", "entity": entity}, [],
    )
    sep = "&" if "?" in list_path else "?"
    fetch = invoke(f"{SOURCE_INSTANCE}.{entity}__fetch", f"#{{SOURCE_API_BASE}}{list_path}{sep}page=${{page}}&size=#{{PAGE_SIZE}}", x + 300, y)
    split = n.create_processor(
        f"{SOURCE_INSTANCE}.{entity}__split", "org.apache.nifi.processors.standard.SplitJson", x + 600, y,
        {"JsonPath Expression": "$.resources[*]", "Max String Length": "20 MB", "Null Value Representation": "empty string"}, ["failure"],
    )
    meta = n.create_processor(
        f"{SOURCE_INSTANCE}.{entity}__page_meta", "org.apache.nifi.processors.standard.EvaluateJsonPath", x + 600, y + 120,
        {"Destination": "flowfile-attribute", "Return Type": "auto-detect", "Path Not Found Behavior": "ignore", "Null Value Representation": "empty string", "total_pages": "$.page.totalPages"}, ["failure"],
    )
    more = n.create_processor(
        f"{SOURCE_INSTANCE}.{entity}__has_more", "org.apache.nifi.processors.standard.RouteOnAttribute", x + 900, y + 120,
        {"Routing Strategy": "Route to Property name", "has_more": "${page:toNumber():lt(${total_pages:toNumber():minus(1)})}"}, ["unmatched"],
    )
    nxt = n.create_processor(
        f"{SOURCE_INSTANCE}.{entity}__next_page", "org.apache.nifi.processors.attributes.UpdateAttribute", x + 1200, y + 120,
        {"Store State": "Do not store state", "page": "${page:toNumber():plus(1)}"}, [],
    )
    props = {"Destination": "flowfile-attribute", "Return Type": "auto-detect", "Path Not Found Behavior": "ignore", "Null Value Representation": "empty string"}
    props.update(extract_paths)
    extract = n.create_processor(
        f"{SOURCE_INSTANCE}.{entity}__extract", "org.apache.nifi.processors.standard.EvaluateJsonPath", x + 900, y,
        props, ["failure", "unmatched"],
    )
    n.create_connection(init, f"{SOURCE_INSTANCE}.{entity}__init_page", fetch, f"{SOURCE_INSTANCE}.{entity}__fetch", ["success"])
    n.create_connection(fetch, f"{SOURCE_INSTANCE}.{entity}__fetch", split, f"{SOURCE_INSTANCE}.{entity}__split", ["Response"])
    n.create_connection(split, f"{SOURCE_INSTANCE}.{entity}__split", extract, f"{SOURCE_INSTANCE}.{entity}__extract", ["split"])
    n.create_connection(split, f"{SOURCE_INSTANCE}.{entity}__split", meta, f"{SOURCE_INSTANCE}.{entity}__page_meta", ["original"])
    n.create_connection(meta, f"{SOURCE_INSTANCE}.{entity}__page_meta", more, f"{SOURCE_INSTANCE}.{entity}__has_more", ["matched", "unmatched"])
    n.create_connection(more, f"{SOURCE_INSTANCE}.{entity}__has_more", nxt, f"{SOURCE_INSTANCE}.{entity}__next_page", ["has_more"])
    n.create_connection(nxt, f"{SOURCE_INSTANCE}.{entity}__next_page", fetch, f"{SOURCE_INSTANCE}.{entity}__fetch", ["success"])
    return init, extract


def publish_tail(entity, object_id_expr, api_path_expr, cursor_expr, x, y, dmc, reader, writer, upstream_id, upstream_name, upstream_rel, detail_url=None):
    """[detail_fetch] -> hash -> set_ids -> set_metadata -> dedupe_key -> dedupe -> publish."""
    src_id, src_name, src_rel = upstream_id, upstream_name, upstream_rel
    if detail_url:
        det = invoke(f"{SOURCE_INSTANCE}.{entity}__detail_fetch", detail_url, x, y)
        n.create_connection(src_id, src_name, det, f"{SOURCE_INSTANCE}.{entity}__detail_fetch", [src_rel])
        src_id, src_name, src_rel = det, f"{SOURCE_INSTANCE}.{entity}__detail_fetch", "Response"

    # Hash the raw source payload BEFORE metadata is added -> the 10 standard
    # fields are excluded from the fingerprint by construction.
    h = n.create_processor(
        f"{SOURCE_INSTANCE}.{entity}__hash", "org.apache.nifi.processors.standard.CryptographicHashContent", x + 300, y,
        {"Hash Algorithm": "SHA-256"}, ["failure"],
    )
    ids = n.create_processor(
        f"{SOURCE_INSTANCE}.{entity}__set_ids", "org.apache.nifi.processors.attributes.UpdateAttribute", x + 600, y,
        {
            "Store State": "Do not store state",
            "entity": entity,
            "object_id": object_id_expr,
            "api_path": api_path_expr,
            "cursor_window": cursor_expr,
            "kafka_topic": topic(entity),
        }, [],
    )
    meta = n.create_processor(
        f"{SOURCE_INSTANCE}.{entity}__set_metadata", "org.apache.nifi.processors.standard.UpdateRecord", x + 900, y,
        {
            "Record Reader": reader,
            "Record Writer": writer,
            "Replacement Value Strategy": "literal-value",
            "/source_platform": "rapid7",
            "/customer_tenant_organization": SOURCE_INSTANCE,
            "/source_object_type": entity,
            "/source_object_id": "${object_id}",
            "/extraction_timestamp": "${extraction_timestamp}",
            "/source_event_update_timestamp": "",
            "/api_endpoint_export_query_identity": "${api_path}",
            "/cursor_window": "${cursor_window}",
            "/payload_hash_fingerprint": HASH_ATTR,
            "/ingestion_run_batch_identity": "${ingestion_run_batch_identity}",
        }, ["failure"],
    )
    key = n.create_processor(
        f"{SOURCE_INSTANCE}.{entity}__dedupe_key", "org.apache.nifi.processors.attributes.UpdateAttribute", x + 1200, y,
        {"Store State": "Do not store state", "dedupe.key": f"{SOURCE_INSTANCE}:{entity}:${{object_id}}:{HASH_ATTR}"}, [],
    )
    dd = n.create_processor(
        f"{SOURCE_INSTANCE}.{entity}__dedupe", "org.apache.nifi.processors.standard.DetectDuplicate", x + 1500, y,
        {"Cache Entry Identifier": "${dedupe.key}", "Cache The Entry Identifier": "true", "Age Off Duration": "24 hours", "Distributed Cache Service": dmc}, ["duplicate", "failure"],
    )
    pub = n.create_processor(
        f"{SOURCE_INSTANCE}.{entity}__raw__publish", "org.apache.nifi.kafka.processors.PublishKafka", x + 1800, y,
        {
            "Kafka Connection Service": n.KAFKA_SERVICE_ID,
            "Topic Name": topic(entity),
            "Kafka Key": "${object_id}",
            "Kafka Key Attribute Encoding": "utf-8",
            "Publish Strategy": "USE_VALUE",
            "Record Metadata Strategy": "FROM_PROPERTIES",
            "FlowFile Attribute Header Pattern": n.STANDARD_HEADER_PATTERN,
            "Header Encoding": "UTF-8",
            "Transactions Enabled": "false",
            "acks": "all",
            "compression.type": "gzip",
            "max.request.size": "16 MB",
            "Failure Strategy": "Route to Failure",
        }, ["success", "failure"],
    )
    n.create_connection(src_id, src_name, h, f"{SOURCE_INSTANCE}.{entity}__hash", [src_rel])
    n.create_connection(h, f"{SOURCE_INSTANCE}.{entity}__hash", ids, f"{SOURCE_INSTANCE}.{entity}__set_ids", ["success"])
    n.create_connection(ids, f"{SOURCE_INSTANCE}.{entity}__set_ids", meta, f"{SOURCE_INSTANCE}.{entity}__set_metadata", ["success"])
    n.create_connection(meta, f"{SOURCE_INSTANCE}.{entity}__set_metadata", key, f"{SOURCE_INSTANCE}.{entity}__dedupe_key", ["success"])
    n.create_connection(key, f"{SOURCE_INSTANCE}.{entity}__dedupe_key", dd, f"{SOURCE_INSTANCE}.{entity}__dedupe", ["success"])
    n.create_connection(dd, f"{SOURCE_INSTANCE}.{entity}__dedupe", pub, f"{SOURCE_INSTANCE}.{entity}__raw__publish", ["non-duplicate"])
    return pub


# --------------------------------------------------------------------------- build


def build_raw():
    ctx = ensure_param_context()
    n.REFERENCE_PARAM_CONTEXT_ID = ctx
    n.pg_id()
    dmc, reader, writer = services()

    trigger = n.create_processor(
        f"{SOURCE_INSTANCE}.maximum__trigger", "org.apache.nifi.processors.standard.GenerateFlowFile", -700, 0,
        {"File Size": "0B", "Batch Size": "1", "Data Format": "Text", "Unique FlowFiles": "false", "Custom Text": "rapid7-asyad-run"}, [], "6 hours",
    )
    run_meta = n.create_processor(
        f"{SOURCE_INSTANCE}.maximum__run_metadata", "org.apache.nifi.processors.attributes.UpdateAttribute", -400, 0,
        {
            "Store State": "Do not store state",
            "extraction_timestamp": "${now():format(\"yyyy-MM-dd'T'HH:mm:ss.SSSXXX\")}",
            "ingestion_run_batch_identity": f"{SOURCE_INSTANCE}-" + "${now():toNumber()}-${uuid}",
        }, [],
    )

    # ---- sites
    site_init, site_extract = paged_branch("site", "/sites", 0, 0, {"site_id": "$.id", "site_name": "$.name"})
    site_filter = n.create_processor(
        f"{SOURCE_INSTANCE}.site__filter", "org.apache.nifi.processors.standard.RouteOnAttribute", 1200, 0,
        {"Routing Strategy": "Route to Property name", "blocked": "${site_name:equals(#{BLOCKED_SITES})}"}, ["blocked"],
    )
    n.create_connection(trigger, f"{SOURCE_INSTANCE}.maximum__trigger", run_meta, f"{SOURCE_INSTANCE}.maximum__run_metadata", ["success"])
    n.create_connection(run_meta, f"{SOURCE_INSTANCE}.maximum__run_metadata", site_init, f"{SOURCE_INSTANCE}.site__init_page", ["success"])
    n.create_connection(site_extract, f"{SOURCE_INSTANCE}.site__extract", site_filter, f"{SOURCE_INSTANCE}.site__filter", ["matched"])
    publish_tail("site", "${site_id}", "/sites/${site_id}", "page=${page}&size=#{PAGE_SIZE}", 1500, -200, dmc, reader, writer,
                 site_filter, f"{SOURCE_INSTANCE}.site__filter", "unmatched", detail_url="#{SOURCE_API_BASE}/sites/${site_id}")

    # ---- assets, rooted from site
    asset_init, asset_extract = paged_branch("asset", "/sites/${site_id}/assets", 0, 400, {"asset_id": "$.id"})
    n.create_connection(site_filter, f"{SOURCE_INSTANCE}.site__filter", asset_init, f"{SOURCE_INSTANCE}.asset__init_page", ["unmatched"])
    rate = n.create_processor(
        f"{SOURCE_INSTANCE}.asset__rate_limit", "org.apache.nifi.processors.standard.ControlRate", 1200, 400,
        {"Rate Control Criteria": "flowfile count", "Maximum Rate": ASSET_RATE_PER_SEC, "Time Duration": "1 sec"}, ["failure"],
    )
    n.create_connection(asset_extract, f"{SOURCE_INSTANCE}.asset__extract", rate, f"{SOURCE_INSTANCE}.asset__rate_limit", ["matched"])
    publish_tail("asset", "${site_id}_${asset_id}", "/assets/${asset_id}", "site=${site_id};page=${page}", 1500, 400, dmc, reader, writer,
                 rate, f"{SOURCE_INSTANCE}.asset__rate_limit", "success", detail_url="#{SOURCE_API_BASE}/assets/${asset_id}")

    # ---- asset children, all rooted from the rate-limited asset stream
    sw_init, sw_extract = paged_branch("asset_software", "/assets/${asset_id}/software", 0, 800, {"software_id": "$.id"})
    n.create_connection(rate, f"{SOURCE_INSTANCE}.asset__rate_limit", sw_init, f"{SOURCE_INSTANCE}.asset_software__init_page", ["success"])
    publish_tail("asset_software", "${asset_id}_${software_id}", "/assets/${asset_id}/software", "asset=${asset_id};page=${page}", 1500, 800, dmc, reader, writer,
                 sw_extract, f"{SOURCE_INSTANCE}.asset_software__extract", "matched")

    svc_init, svc_extract = paged_branch("asset_service", "/assets/${asset_id}/services", 0, 1200, {"protocol": "$.protocol", "port": "$.port"})
    n.create_connection(rate, f"{SOURCE_INSTANCE}.asset__rate_limit", svc_init, f"{SOURCE_INSTANCE}.asset_service__init_page", ["success"])
    publish_tail("asset_service", "${asset_id}_${protocol}_${port}", "/assets/${asset_id}/services/${protocol}/${port}", "asset=${asset_id};page=${page}", 1500, 1200, dmc, reader, writer,
                 svc_extract, f"{SOURCE_INSTANCE}.asset_service__extract", "matched",
                 detail_url="#{SOURCE_API_BASE}/assets/${asset_id}/services/${protocol}/${port}")

    vul_init, vul_extract = paged_branch("asset_vulnerability", "/assets/${asset_id}/vulnerabilities", 0, 1600, {"vulnerability_id": "$.id"})
    n.create_connection(rate, f"{SOURCE_INSTANCE}.asset__rate_limit", vul_init, f"{SOURCE_INSTANCE}.asset_vulnerability__init_page", ["success"])
    publish_tail("asset_vulnerability", "${asset_id}_${vulnerability_id}", "/assets/${asset_id}/vulnerabilities/${vulnerability_id}", "asset=${asset_id};page=${page}", 1500, 1600, dmc, reader, writer,
                 vul_extract, f"{SOURCE_INSTANCE}.asset_vulnerability__extract", "matched",
                 detail_url="#{SOURCE_API_BASE}/assets/${asset_id}/vulnerabilities/${vulnerability_id}")

    n.stop_all()
    return inspect()


def inspect():
    out = {"process_group": {"id": n.pg_id(), "name": PG_NAME}, "processors": {}, "topics": []}
    groovy = []
    for name, p in n.processors_by_name().items():
        c = p["component"]
        out["processors"][name] = {"type": c["type"].split(".")[-1], "state": c.get("state"), "validation": c.get("validationStatus"), "validation_errors": c.get("validationErrors")}
        if c["type"].endswith("ExecuteGroovyScript"):
            groovy.append(name)
    out["groovy_processors"] = groovy
    for e in ["site", "asset", "asset_software", "asset_service", "asset_vulnerability"]:
        out["topics"].append(topic(e))
    return out


ENTITIES = ["site", "asset", "asset_software", "asset_service", "asset_vulnerability"]

KAFKA_CONNECT_BASE = os.environ.get("KAFKA_CONNECT_BASE", "https://kafkaconnect.datapasc.com").rstrip("/")


def subject(entity):
    return f"{topic(entity)}.avro-value"


def infer_register():
    """Infer an Avro schema per entity from real Kafka samples and register it in Apicurio."""
    os.makedirs("generated_schemas", exist_ok=True)
    out = {}
    limit = int(os.environ.get("SCHEMA_SAMPLE_LIMIT", "200"))
    for entity in ENTITIES:
        try:
            vals = n.fetch_topic_values(topic(entity), limit)
        except Exception as e:
            out[entity] = {"status": "no_topic", "error": str(e)[:200]}
            continue
        samples, renamed = [], set()
        for v in vals:
            try:
                obj = json.loads(v)
            except Exception:
                continue
            if not isinstance(obj, dict):
                continue
            norm = n.normalize_json(obj, 0)
            # The registered schema must use the field names that are actually on the
            # wire; flag any key the sanitiser had to rewrite.
            renamed |= {k for k in obj if n.safe_field_name(k) != k}
            samples.append(norm)
        if not samples:
            out[entity] = {"status": "no_samples", "topic": topic(entity)}
            continue
        schema = n.schema_from_samples(samples, f"{SOURCE_INSTANCE}_{entity}_raw_avro", f"bronze.{SOURCE_INSTANCE}")
        path = os.path.join("generated_schemas", f"{subject(entity)}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(schema, f, indent=2, ensure_ascii=False)
        reg = n.register_schema(subject(entity), schema)
        collapsed = [f["name"] for f in schema["fields"] if f["type"][1] == "string" and f["name"] not in STANDARD_VALUE_FIELDS]
        out[entity] = {
            "status": "registered", "subject": subject(entity), "samples": len(samples),
            "fields": len(schema["fields"]), "schema_id": reg.get("id"), "version": reg.get("version"),
            "unsafe_field_names": sorted(renamed) or None,
            "scalar_string_fields": len(collapsed),
        }
    return out


def add_avro():
    """One PublishKafka per entity, off the existing dedupe. No normaliser, no Groovy:
    JsonTreeReader reads with the registered schema, AvroRecordSetWriter emits Avro."""
    n.stop_all()
    out = {}
    for idx, entity in enumerate(ENTITIES):
        if not n.schema_subject_exists(subject(entity)):
            out[entity] = {"status": "missing_schema", "subject": subject(entity)}
            continue
        reader = n.create_controller_service(
            f"{SOURCE_INSTANCE}.{entity}__avro_json_reader", "org.apache.nifi.json.JsonTreeReader",
            {"Schema Access Strategy": "schema-name", "Schema Registry": n.SCHEMA_REGISTRY_SERVICE_ID, "Schema Name": subject(entity), "Starting Field Strategy": "ROOT_NODE", "Schema Application Strategy": "SELECTED_PART"},
        )
        writer = n.create_controller_service(
            f"{SOURCE_INSTANCE}.{entity}__avro_writer", "org.apache.nifi.avro.AvroRecordSetWriter",
            {"Schema Write Strategy": "schema-reference-writer", "Schema Reference Writer": n.SCHEMA_REF_WRITER_SERVICE_ID, "Schema Access Strategy": "schema-name", "Schema Registry": n.SCHEMA_REGISTRY_SERVICE_ID, "Schema Name": subject(entity)},
        )
        pub = n.create_processor(
            f"{SOURCE_INSTANCE}.{entity}__avro__publish", "org.apache.nifi.kafka.processors.PublishKafka",
            3600, -200 + idx * 400, n.publish_props(f"{topic(entity)}.avro", True, reader, writer), ["success", "failure"],
        )
        dd = n.processors_by_name()[f"{SOURCE_INSTANCE}.{entity}__dedupe"]
        n.create_connection(dd["id"], f"{SOURCE_INSTANCE}.{entity}__dedupe", pub, f"{SOURCE_INSTANCE}.{entity}__avro__publish", ["non-duplicate"])
        out[entity] = {"status": "added", "avro_topic": f"{topic(entity)}.avro", "subject": subject(entity)}
    n.stop_all()
    return out


def connector_config(entity):
    avro_topic = f"{topic(entity)}.avro"
    name = f"{avro_topic}__iceberg"
    group = f"cg-iceberg-bronze-{SOURCE_INSTANCE.replace('_','-')}-{entity.replace('_','-')}"
    return name, {
        "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
        "tasks.max": "1",
        "topics": avro_topic,
        "iceberg.tables": f"{SOURCE_INSTANCE}.{entity}",
        "iceberg.tables.auto-create-enabled": "true",
        "iceberg.tables.evolve-schema-enabled": "true",
        "iceberg.tables.schema-force-optional": "true",
        "iceberg.control.topic": "control-iceberg",
        "iceberg.control.group-id-prefix": group,
        "iceberg.control.commit.interval-ms": "60000",
        "iceberg.catalog": "polaris",
        "iceberg.catalog.type": "rest",
        "iceberg.catalog.uri": os.environ.get("POLARIS_URI", "https://polaris.datapasc.com/api/catalog"),
        "iceberg.catalog.warehouse": "bronze",
        "iceberg.catalog.rest.auth.type": "oauth2",
        "iceberg.catalog.credential": os.environ["POLARIS_CREDENTIAL"],
        "iceberg.catalog.scope": "PRINCIPAL_ROLE:ALL",
        "iceberg.catalog.oauth2-server-uri": os.environ.get("POLARIS_TOKEN_URI", "https://polaris.datapasc.com/api/catalog/v1/oauth/tokens"),
        "iceberg.catalog.token-refresh-enabled": "true",
        "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "iceberg.catalog.s3.endpoint": os.environ.get("OZONE_S3_ENDPOINT", "https://ozones3g.datapasc.com"),
        "iceberg.catalog.s3.access-key-id": os.environ["OZONE_S3_ACCESS_KEY"],
        "iceberg.catalog.s3.secret-access-key": os.environ["OZONE_S3_SECRET_KEY"],
        "iceberg.catalog.s3.path-style-access": "true",
        "iceberg.catalog.s3.region": "us-east-1",
        "iceberg.catalog.client.region": "us-east-1",
        "value.converter": "io.apicurio.registry.utils.converter.AvroConverter",
        "value.converter.schemas.enable": "true",
        "value.converter.apicurio.registry.url": "https://apicurio.datapasc.com/apis/registry/v3",
        "value.converter.apicurio.registry.as-confluent": "true",
        "value.converter.apicurio.registry.use-id": "contentId",
        "value.converter.apicurio.registry.auto-register": "false",
        "value.converter.apicurio.registry.find-latest": "true",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "consumer.override.auto.offset.reset": "earliest",
        "errors.tolerance": "none",
        "errors.log.enable": "true",
        "errors.log.include.messages": "true",
        "errors.deadletterqueue.topic.name": f"dlq.{avro_topic}.iceberg",
        "errors.deadletterqueue.context.headers.enable": "true",
        "errors.deadletterqueue.topic.replication.factor": "1",
    }


def upsert_connectors():
    out = {}
    for entity in ENTITIES:
        if not n.schema_subject_exists(subject(entity)):
            out[entity] = {"status": "skipped_missing_schema"}
            continue
        name, cfg = connector_config(entity)
        url = f"{KAFKA_CONNECT_BASE}/connectors/{urllib.parse.quote(name, safe='')}/config"
        r = requests.put(url, json=cfg, verify=False, timeout=40, headers={"User-Agent": "Mozilla/5.0"})
        out[name] = {"status": r.status_code, "body": r.text[:200]}
    return out


def build_replay():
    """ConsumeKafka(<entity>__raw) -> existing <entity>__avro__publish.

    Backfills the Avro topics from raw messages that were published before the Avro
    branch existed. Reads only from Kafka, so it makes ZERO calls to Rapid7.
    """
    n.stop_all()
    out = {}
    for idx, entity in enumerate(ENTITIES):
        procs = n.processors_by_name()
        pub_name = f"{SOURCE_INSTANCE}.{entity}__avro__publish"
        if pub_name not in procs:
            out[entity] = {"status": "missing_avro_publisher"}
            continue
        con = n.create_processor(
            f"{SOURCE_INSTANCE}.{entity}__replay__consume", "org.apache.nifi.kafka.processors.ConsumeKafka",
            3100, -200 + idx * 400,
            {
                "Kafka Connection Service": n.KAFKA_SERVICE_ID,
                "Group ID": f"replay-avro-{SOURCE_INSTANCE}-{entity}",
                "Topics": topic(entity),
                "Topic Format": "names",
                "auto.offset.reset": "earliest",
                "Processing Strategy": "FLOW_FILE",
                "Output Strategy": "USE_VALUE",
                "Commit Offsets": "true",
                "Header Name Pattern": n.STANDARD_HEADER_PATTERN,
                "Key Attribute Encoding": "utf-8",
            }, [],
        )
        n.create_connection(con, f"{SOURCE_INSTANCE}.{entity}__replay__consume", procs[pub_name]["id"], pub_name, ["success"])
        out[entity] = {"status": "added", "from": topic(entity), "to": f"{topic(entity)}.avro"}
    n.stop_all()
    return out


def start_replay():
    started = []
    for name, p in n.processors_by_name().items():
        if name.endswith("__replay__consume") or name.endswith("__avro__publish"):
            ent = n.nifi("GET", f"/nifi-api/processors/{p['id']}")
            if ent["component"].get("validationStatus") == "VALID":
                n.set_processor_state(p["id"], "RUNNING")
                started.append(name)
    return started


TAIL_SUFFIXES = ("__hash", "__set_ids", "__set_metadata", "__dedupe_key", "__dedupe", "__raw__publish", "__avro__publish")


def start_tail():
    """Start only post-fetch processors so already-fetched queued data drains.
    Deliberately never starts InvokeHTTP, so this makes ZERO calls to Rapid7."""
    started = []
    for name, p in n.processors_by_name().items():
        if name.endswith(TAIL_SUFFIXES):
            ent = n.nifi("GET", f"/nifi-api/processors/{p['id']}")
            if ent["component"].get("validationStatus") == "VALID":
                n.set_processor_state(p["id"], "RUNNING")
                started.append(name)
    return started


def main():
    requests.packages.urllib3.disable_warnings()
    cmd = sys.argv[1] if len(sys.argv) > 1 else "inspect"
    if cmd == "build-raw":
        print(json.dumps(build_raw(), indent=2))
    elif cmd == "infer-register":
        print(json.dumps(infer_register(), indent=2))
    elif cmd == "add-avro":
        print(json.dumps(add_avro(), indent=2))
    elif cmd == "connectors":
        print(json.dumps(upsert_connectors(), indent=2))
    elif cmd == "build-replay":
        print(json.dumps(build_replay(), indent=2))
    elif cmd == "start-replay":
        print(json.dumps({"started": start_replay()}, indent=2))
    elif cmd == "start-tail":
        print(json.dumps({"started": start_tail()}, indent=2))
    elif cmd == "stop":
        n.stop_all()
        print(json.dumps({"stopped": True}, indent=2))
    else:
        print(json.dumps(inspect(), indent=2))


if __name__ == "__main__":
    main()
