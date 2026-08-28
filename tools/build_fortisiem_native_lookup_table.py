"""
Native build of lookup_table -- brand new entity, GET-only (no Jolt transform needed: the response
is already an array of named-field objects). Single call with size=1000 returns all 30 rows (no
pagination loop needed). Triggered once per run off fortisiem.maximum__run_metadata, same fan-out
point incident/watchlist use.

'columnList' is a nested array of {key, name, type} objects -- kept NESTED as an Avro
array-of-records per the supervisor spec.
"""
import sys
sys.path.insert(0, "tools")
from build_fortisiem_native_lib import (
    login, create_controller_service, enable_controller_service, create_processor, create_connection,
    get_flow, STANDARD_HEADER_REGEX, HASH_SCRIPT, PG_ID, DMC_SERVICE_ID, KAFKA_CONNECTION_ID,
    SCHEMA_REGISTRY_ID, make_avro_writer_props, xml_reader_bundle,
)
import json
import os
import subprocess
import time
import urllib.parse

ENTITY = "lookup_table"
SCHEMA_NAME = f"bronze.fortisiem.{ENTITY}__raw.avro-value"
GROOVY_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-groovyx-nar", "version": "2.9.0"}
STANDARD_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-standard-nar", "version": "2.9.0"}
UPDATE_ATTR_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-update-attribute-nar", "version": "2.9.0"}
KAFKA_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-kafka-nar", "version": "2.9.0"}
JSON_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-record-serialization-services-nar", "version": "2.9.0"}

APICURIO_CCOMPAT = os.environ.get("APICURIO_CCOMPAT", "https://apicurio.datapasc.com/apis/ccompat/v7").rstrip("/")


def run_curl(args, input_text=None, timeout=30, attempts=3):
    last = None
    for i in range(attempts):
        proc = subprocess.run(["curl.exe", "--http1.1", "-k", "-sS"] + args, input=input_text, text=True, capture_output=True, timeout=timeout)
        if proc.returncode == 0:
            return proc.stdout
        last = f"curl exit {proc.returncode}: {proc.stderr[:400]} {proc.stdout[:400]}"
        time.sleep(1 + i)
    raise RuntimeError(last)


def apicurio_post(path, body, timeout=30):
    args = ["-X", "POST", "-H", "Content-Type: application/vnd.schemaregistry.v1+json", "--data-binary", "@-", "-w", "\nHTTP_STATUS:%{http_code}", f"{APICURIO_CCOMPAT}{path}"]
    out = run_curl(args, json.dumps(body), timeout=timeout)
    raw, status_txt = out.rsplit("\nHTTP_STATUS:", 1)
    status = int(status_txt.strip()[:3])
    if status < 200 or status > 299:
        raise RuntimeError(f"POST {path} HTTP {status}: {raw[:1000]}")
    return json.loads(raw.strip())


STANDARD_FIELDS = [
    "source_platform", "customer_tenant_organization", "source_object_type", "source_object_id",
    "extraction_timestamp", "source_event_update_timestamp", "api_endpoint_export_query_identity",
    "cursor_window", "payload_hash_fingerprint", "ingestion_run_batch_identity",
]

COLUMN_RECORD = {
    "type": "record",
    "name": "lookup_table_column",
    "namespace": "bronze.fortisiem",
    "fields": [
        {"name": "key", "type": ["null", "boolean"], "default": None},
        {"name": "name", "type": ["null", "string"], "default": None},
        {"name": "type", "type": ["null", "string"], "default": None},
    ],
}


def build_schema():
    fields = []
    for f in STANDARD_FIELDS:
        fields.append({"name": f, "type": ["null", "string"], "default": None})
    fields.append({"name": "object_id", "type": ["null", "string"], "default": None})
    fields.append({"name": "ingest_ts", "type": ["null", "long"], "default": None})
    fields.extend([
        {"name": "columnList", "type": ["null", {"type": "array", "items": COLUMN_RECORD}], "default": None},
        {"name": "description", "type": ["null", "string"], "default": None},
        {"name": "id", "type": ["null", "long"], "default": None},
        {"name": "lastUpdated", "type": ["null", "long"], "default": None},
        {"name": "name", "type": ["null", "string"], "default": None},
        {"name": "organizationName", "type": ["null", "string"], "default": None},
    ])
    return {"type": "record", "name": "fortisiem_lookup_table_raw_avro", "namespace": "bronze.fortisiem", "fields": fields}


def register_schema():
    schema = build_schema()
    result = apicurio_post(f"/subjects/{urllib.parse.quote(SCHEMA_NAME, safe='')}/versions", {"schema": json.dumps(schema)})
    return {"subject": SCHEMA_NAME, "id": result.get("id"), "field_count": len(schema["fields"])}


def main():
    schema_result = register_schema()

    token = login()
    flow = get_flow(token)
    procs = {p["component"]["name"]: p["component"] for p in flow["processGroupFlow"]["flow"]["processors"]}

    def pid(name):
        return procs[name]["id"]

    json_reader_id = create_controller_service(token, "org.apache.nifi.json.JsonTreeReader", f"fortisiem.{ENTITY}__json_reader", {
        "Schema Access Strategy": "schema-name", "Schema Registry": SCHEMA_REGISTRY_ID, "Schema Name": SCHEMA_NAME,
    }, JSON_BUNDLE)
    json_writer_id = create_controller_service(token, "org.apache.nifi.json.JsonRecordSetWriter", f"fortisiem.{ENTITY}__json_writer", {
        "Schema Write Strategy": "no-schema", "Schema Access Strategy": "schema-name",
        "Schema Registry": SCHEMA_REGISTRY_ID, "Schema Name": SCHEMA_NAME,
    }, JSON_BUNDLE)
    avro_writer_id = create_controller_service(token, "org.apache.nifi.avro.AvroRecordSetWriter", f"fortisiem.{ENTITY}__avro_writer",
                                                make_avro_writer_props(SCHEMA_NAME), xml_reader_bundle())
    enable_controller_service(token, json_reader_id)
    enable_controller_service(token, json_writer_id)
    enable_controller_service(token, avro_writer_id)

    fetch_id = create_processor(token, "org.apache.nifi.processors.standard.InvokeHTTP", f"fortisiem.{ENTITY}__fetch", {
        "HTTP Method": "GET",
        "HTTP URL": "#{SOURCE_API_BASE}/pub/lookupTable?size=1000",
        "HTTP/2 Disabled": "True",
        "Request Username": "#{HTTP_USERNAME}",
        "Request Password": "#{HTTP_PASSWORD}",
        "Connection Timeout": "10 secs",
        "Socket Read Timeout": "30 secs",
    }, STANDARD_BUNDLE, auto_terminate=["Retry", "No Retry", "Original", "Failure"])

    split_id = create_processor(token, "org.apache.nifi.processors.standard.SplitJson", f"fortisiem.{ENTITY}__split", {
        "JsonPath Expression": "$.data[*]",
        "Null Value Representation": "empty string",
    }, STANDARD_BUNDLE, auto_terminate=["failure", "original"])

    extract_id = create_processor(token, "org.apache.nifi.processors.standard.EvaluateJsonPath", f"fortisiem.{ENTITY}__extract", {
        "Destination": "flowfile-attribute",
        "lookup_table_org": "$.organizationName",
        "lookup_table_id": "$.id",
    }, STANDARD_BUNDLE, auto_terminate=["failure"])

    set_ids_id = create_processor(token, "org.apache.nifi.processors.attributes.UpdateAttribute", f"fortisiem.{ENTITY}__set_ids", {
        "entity": ENTITY,
        "object_id": "${lookup_table_id}",
        "api_path": "GET #{SOURCE_API_BASE}/pub/lookupTable?size=1000",
        "kafka_topic": f"bronze.fortisiem.{ENTITY}__raw",
        "cursor_window": "${literal('')}",
    }, UPDATE_ATTR_BUNDLE)

    hash_id = create_processor(token, "org.apache.nifi.processors.groovyx.ExecuteGroovyScript", f"fortisiem.{ENTITY}__hash", {
        "Script Body": HASH_SCRIPT,
        "EXCLUDE_FIELDS": "${literal('')}",
        "Failure Strategy": "rollback",
    }, GROOVY_BUNDLE, auto_terminate=["failure"])

    headers_id = create_processor(token, "org.apache.nifi.processors.attributes.UpdateAttribute", f"fortisiem.{ENTITY}__set_public_headers", {
        "source_platform": "fortisiem",
        "customer_tenant_organization": "${lookup_table_org}",
        "source_object_type": "${entity}",
        "source_object_id": "${object_id}",
        "source_event_update_timestamp": "${literal('')}",
        "api_endpoint_export_query_identity": "${api_path}",
        "payload_hash_fingerprint": "${'content_SHA-256'}",
        "object_id_composite": "fortisiem:${lookup_table_org}:${entity}:${object_id}",
        "ingest_ts": "${now():toNumber()}",
    }, UPDATE_ATTR_BUNDLE)

    metadata_id = create_processor(token, "org.apache.nifi.processors.standard.UpdateRecord", f"fortisiem.{ENTITY}__set_metadata", {
        "Record Reader": json_reader_id,
        "Record Writer": json_writer_id,
        "Replacement Value Strategy": "literal-value",
        "/source_platform": "${source_platform}",
        "/customer_tenant_organization": "${customer_tenant_organization}",
        "/source_object_type": "${source_object_type}",
        "/source_object_id": "${source_object_id}",
        "/extraction_timestamp": "${extraction_timestamp}",
        "/source_event_update_timestamp": "${source_event_update_timestamp}",
        "/api_endpoint_export_query_identity": "${api_endpoint_export_query_identity}",
        "/cursor_window": "${cursor_window}",
        "/payload_hash_fingerprint": "${payload_hash_fingerprint}",
        "/ingestion_run_batch_identity": "${ingestion_run_batch_identity}",
        "/object_id": "${object_id_composite}",
        "/ingest_ts": "${ingest_ts}",
    }, STANDARD_BUNDLE, auto_terminate=["failure"])

    dedupe_key_id = create_processor(token, "org.apache.nifi.processors.attributes.UpdateAttribute", f"fortisiem.{ENTITY}__dedupe_key", {
        "dedupe.key": f"fortisiem_max_v9:fortisiem:{ENTITY}:" + "${object_id}:${'content_SHA-256'}",
        "object_id": "${object_id_composite}",
    }, UPDATE_ATTR_BUNDLE)

    dedupe_id = create_processor(token, "org.apache.nifi.processors.standard.DetectDuplicate", f"fortisiem.{ENTITY}__dedupe", {
        "Cache Entry Identifier": "${dedupe.key}",
        "Age Off Duration": "24 hours",
        "Distributed Cache Service": DMC_SERVICE_ID,
        "Cache The Entry Identifier": "true",
    }, STANDARD_BUNDLE, auto_terminate=["duplicate", "failure"])

    replay_id = create_processor(token, "org.apache.nifi.kafka.processors.ConsumeKafka", f"fortisiem.{ENTITY}__replay__consume", {
        "Kafka Connection Service": KAFKA_CONNECTION_ID,
        "Group ID": f"replay-avro-fortisiem-{ENTITY}-v1",
        "Topic Format": "names",
        "Topics": f"bronze.fortisiem.{ENTITY}__raw",
        "auto.offset.reset": "earliest",
        "Commit Offsets": "true",
        "Header Name Pattern": STANDARD_HEADER_REGEX,
        "Header Encoding": "UTF-8",
        "Processing Strategy": "FLOW_FILE",
        "Output Strategy": "USE_VALUE",
        "Key Attribute Encoding": "utf-8",
        "Key Format": "byte-array",
    }, KAFKA_BUNDLE, auto_terminate=["parse-failure"])

    header_pattern_12 = STANDARD_HEADER_REGEX.replace(")$", "|object_id|ingest_ts)$")
    raw_publish_id = create_processor(token, "org.apache.nifi.kafka.processors.PublishKafka", f"fortisiem.{ENTITY}__raw__publish", {
        "Kafka Connection Service": KAFKA_CONNECTION_ID,
        "Topic Name": f"bronze.fortisiem.{ENTITY}__raw",
        "Failure Strategy": "Route to Failure",
        "acks": "all",
        "compression.type": "gzip", "Transactions Enabled": "false",
        "FlowFile Attribute Header Pattern": header_pattern_12,
        "Kafka Key": "${source_object_id}",
        "Kafka Key Attribute Encoding": "utf-8",
    }, KAFKA_BUNDLE, auto_terminate=["success", "failure"])

    avro_publish_id = create_processor(token, "org.apache.nifi.kafka.processors.PublishKafka", f"fortisiem.{ENTITY}__avro__publish", {
        "Kafka Connection Service": KAFKA_CONNECTION_ID,
        "Topic Name": f"bronze.fortisiem.{ENTITY}__raw.avro",
        "Failure Strategy": "Route to Failure",
        "acks": "all",
        "compression.type": "gzip", "Transactions Enabled": "false",
        "Record Reader": json_reader_id,
        "Record Writer": avro_writer_id,
        "FlowFile Attribute Header Pattern": header_pattern_12,
        "Kafka Key": "${source_object_id}",
        "Kafka Key Attribute Encoding": "utf-8",
    }, KAFKA_BUNDLE, auto_terminate=["success", "failure"])

    run_metadata_name = "fortisiem.maximum__run_metadata"
    run_metadata_id = pid(run_metadata_name)

    create_connection(token, run_metadata_id, run_metadata_name, "PROCESSOR", fetch_id, f"fortisiem.{ENTITY}__fetch", "PROCESSOR", ["success"])
    create_connection(token, fetch_id, f"fortisiem.{ENTITY}__fetch", "PROCESSOR", split_id, f"fortisiem.{ENTITY}__split", "PROCESSOR", ["Response"])
    create_connection(token, split_id, f"fortisiem.{ENTITY}__split", "PROCESSOR", extract_id, f"fortisiem.{ENTITY}__extract", "PROCESSOR", ["split"])
    create_connection(token, extract_id, f"fortisiem.{ENTITY}__extract", "PROCESSOR", set_ids_id, f"fortisiem.{ENTITY}__set_ids", "PROCESSOR", ["matched"])
    create_connection(token, set_ids_id, f"fortisiem.{ENTITY}__set_ids", "PROCESSOR", hash_id, f"fortisiem.{ENTITY}__hash", "PROCESSOR", ["success"])
    create_connection(token, hash_id, f"fortisiem.{ENTITY}__hash", "PROCESSOR", headers_id, f"fortisiem.{ENTITY}__set_public_headers", "PROCESSOR", ["success"])
    create_connection(token, headers_id, f"fortisiem.{ENTITY}__set_public_headers", "PROCESSOR", metadata_id, f"fortisiem.{ENTITY}__set_metadata", "PROCESSOR", ["success"])
    create_connection(token, metadata_id, f"fortisiem.{ENTITY}__set_metadata", "PROCESSOR", dedupe_key_id, f"fortisiem.{ENTITY}__dedupe_key", "PROCESSOR", ["success"])
    create_connection(token, dedupe_key_id, f"fortisiem.{ENTITY}__dedupe_key", "PROCESSOR", dedupe_id, f"fortisiem.{ENTITY}__dedupe", "PROCESSOR", ["success"])
    create_connection(token, dedupe_id, f"fortisiem.{ENTITY}__dedupe", "PROCESSOR", raw_publish_id, f"fortisiem.{ENTITY}__raw__publish", "PROCESSOR", ["non-duplicate"])
    create_connection(token, replay_id, f"fortisiem.{ENTITY}__replay__consume", "PROCESSOR", avro_publish_id, f"fortisiem.{ENTITY}__avro__publish", "PROCESSOR", ["success"])

    print(json.dumps({"schema": schema_result, "raw_publish_id": raw_publish_id, "avro_publish_id": avro_publish_id}, indent=2))


if __name__ == "__main__":
    main()
