"""
Native build of agent_status -- brand new entity, no prior processors to reuse. No live populated
sample exists (confirmed empty on the one WMI-monitored device tested in this tenant) and the API
Guide doesn't show a concrete example response either, only the field list (Type, AgentStatus,
PolicyID, HeartbeatTime, LastEventReceiveTime) and the fact hostName is a required single-host
parameter ("Get Agent Status for a Specific Host") -- treated as a single flat record per call, same
shape as organization/device, not a list needing SplitXml. Root tag assumed "Status" matching the
schema built earlier this session. This is the one entity in the rebuild that genuinely needs a live
test run to confirm the wrapper tag guess before trusting its output.
"""
import sys
sys.path.insert(0, "tools")
from build_fortisiem_native_lib import (
    login, nifi, nifi_ok, create_controller_service, enable_controller_service,
    create_processor, create_connection, get_flow, xml_reader_bundle,
    make_xml_reader_props, make_xml_writer_props, make_avro_writer_props,
    STANDARD_HEADER_REGEX, HASH_SCRIPT, PG_ID, DMC_SERVICE_ID, KAFKA_CONNECTION_ID,
)
import json

ENTITY = "agent_status"
SCHEMA_NAME = f"bronze.fortisiem.{ENTITY}__raw.avro-value"
ROOT_TAG = "Status"
GROOVY_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-groovyx-nar", "version": "2.9.0"}
STANDARD_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-standard-nar", "version": "2.9.0"}
UPDATE_ATTR_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-update-attribute-nar", "version": "2.9.0"}
KAFKA_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-kafka-nar", "version": "2.9.0"}


def main():
    token = login()
    flow = get_flow(token)
    procs = {p["component"]["name"]: p["component"] for p in flow["processGroupFlow"]["flow"]["processors"]}

    def pid(name):
        return procs[name]["id"]

    reader_id = create_controller_service(token, "org.apache.nifi.xml.XMLReader", f"fortisiem.{ENTITY}__xml_reader",
                                           make_xml_reader_props(SCHEMA_NAME), xml_reader_bundle())
    writer_id = create_controller_service(token, "org.apache.nifi.xml.XMLRecordSetWriter", f"fortisiem.{ENTITY}__xml_writer",
                                           make_xml_writer_props(SCHEMA_NAME, ROOT_TAG), xml_reader_bundle())
    avro_writer_id = create_controller_service(token, "org.apache.nifi.avro.AvroRecordSetWriter", f"fortisiem.{ENTITY}__avro_writer",
                                                make_avro_writer_props(SCHEMA_NAME), xml_reader_bundle())
    enable_controller_service(token, reader_id)
    enable_controller_service(token, writer_id)
    enable_controller_service(token, avro_writer_id)

    rate_id = create_processor(token, "org.apache.nifi.processors.standard.ControlRate", f"fortisiem.{ENTITY}__rate_limit", {
        "Rate Control Criteria": "flowfile count", "Time Duration": "1 sec", "Maximum Rate": "3",
    }, STANDARD_BUNDLE, auto_terminate=["failure"])

    fetch_id = create_processor(token, "org.apache.nifi.processors.standard.InvokeHTTP", f"fortisiem.{ENTITY}__fetch", {
        "HTTP Method": "GET",
        "HTTP URL": "#{SOURCE_API_BASE}/agentStatus/all?request=${organization__attr_id},${host_name}",
        "HTTP/2 Disabled": "True",
        "Request Username": "#{HTTP_USERNAME}",
        "Request Password": "#{HTTP_PASSWORD}",
        "Connection Timeout": "5 secs",
        "Socket Read Timeout": "30 secs",
    }, STANDARD_BUNDLE, auto_terminate=["Retry", "No Retry", "Original", "Failure"])

    set_ids_id = create_processor(token, "org.apache.nifi.processors.attributes.UpdateAttribute", f"fortisiem.{ENTITY}__set_ids", {
        "entity": ENTITY,
        "object_id": "${access_ip}",
        "api_path": "GET #{SOURCE_API_BASE}/agentStatus/all?request=${organization__attr_id},${host_name}",
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
        "customer_tenant_organization": "${org_name}",
        "source_object_type": "${entity}",
        "source_object_id": "${object_id}",
        "source_event_update_timestamp": "${literal('')}",
        "api_endpoint_export_query_identity": "${api_path}",
        "payload_hash_fingerprint": "${'content_SHA-256'}",
        "object_id_composite": "fortisiem:${org_name}:${entity}:${object_id}",
        "ingest_ts": "${now():toNumber()}",
    }, UPDATE_ATTR_BUNDLE)

    metadata_id = create_processor(token, "org.apache.nifi.processors.standard.UpdateRecord", f"fortisiem.{ENTITY}__set_metadata", {
        "Record Reader": reader_id,
        "Record Writer": writer_id,
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

    raw_publish_id = create_processor(token, "org.apache.nifi.kafka.processors.PublishKafka", f"fortisiem.{ENTITY}__raw__publish", {
        "Kafka Connection Service": KAFKA_CONNECTION_ID,
        "Topic Name": f"bronze.fortisiem.{ENTITY}__raw",
        "Failure Strategy": "Route to Failure",
        "acks": "all",
        "compression.type": "gzip", "Transactions Enabled": "false",
        "FlowFile Attribute Header Pattern": STANDARD_HEADER_REGEX.replace(")$", "|object_id|ingest_ts)$"),
        "Kafka Key": "${source_object_id}",
        "Kafka Key Attribute Encoding": "utf-8",
    }, KAFKA_BUNDLE, auto_terminate=["success", "failure"])

    avro_publish_id = create_processor(token, "org.apache.nifi.kafka.processors.PublishKafka", f"fortisiem.{ENTITY}__avro__publish", {
        "Kafka Connection Service": KAFKA_CONNECTION_ID,
        "Topic Name": f"bronze.fortisiem.{ENTITY}__raw.avro",
        "Failure Strategy": "Route to Failure",
        "acks": "all",
        "compression.type": "gzip", "Transactions Enabled": "false",
        "Record Reader": reader_id,
        "Record Writer": avro_writer_id,
        "FlowFile Attribute Header Pattern": STANDARD_HEADER_REGEX.replace(")$", "|object_id|ingest_ts)$"),
        "Kafka Key": "${source_object_id}",
        "Kafka Key Attribute Encoding": "utf-8",
    }, KAFKA_BUNDLE, auto_terminate=["success", "failure"])

    extract_key_name = "fortisiem.maximum__device_detail__extract_key"
    extract_key_id = pid(extract_key_name)

    create_connection(token, extract_key_id, extract_key_name, "PROCESSOR", rate_id, f"fortisiem.{ENTITY}__rate_limit", "PROCESSOR", ["matched"])
    create_connection(token, rate_id, f"fortisiem.{ENTITY}__rate_limit", "PROCESSOR", fetch_id, f"fortisiem.{ENTITY}__fetch", "PROCESSOR", ["success"])
    create_connection(token, fetch_id, f"fortisiem.{ENTITY}__fetch", "PROCESSOR", set_ids_id, f"fortisiem.{ENTITY}__set_ids", "PROCESSOR", ["Response"])
    create_connection(token, set_ids_id, f"fortisiem.{ENTITY}__set_ids", "PROCESSOR", hash_id, f"fortisiem.{ENTITY}__hash", "PROCESSOR", ["success"])
    create_connection(token, hash_id, f"fortisiem.{ENTITY}__hash", "PROCESSOR", headers_id, f"fortisiem.{ENTITY}__set_public_headers", "PROCESSOR", ["success"])
    create_connection(token, headers_id, f"fortisiem.{ENTITY}__set_public_headers", "PROCESSOR", metadata_id, f"fortisiem.{ENTITY}__set_metadata", "PROCESSOR", ["success"])
    create_connection(token, metadata_id, f"fortisiem.{ENTITY}__set_metadata", "PROCESSOR", dedupe_key_id, f"fortisiem.{ENTITY}__dedupe_key", "PROCESSOR", ["success"])
    create_connection(token, dedupe_key_id, f"fortisiem.{ENTITY}__dedupe_key", "PROCESSOR", dedupe_id, f"fortisiem.{ENTITY}__dedupe", "PROCESSOR", ["success"])
    create_connection(token, dedupe_id, f"fortisiem.{ENTITY}__dedupe", "PROCESSOR", raw_publish_id, f"fortisiem.{ENTITY}__raw__publish", "PROCESSOR", ["non-duplicate"])
    create_connection(token, replay_id, f"fortisiem.{ENTITY}__replay__consume", "PROCESSOR", avro_publish_id, f"fortisiem.{ENTITY}__avro__publish", "PROCESSOR", ["success"])

    print(json.dumps({"raw_publish_id": raw_publish_id, "avro_publish_id": avro_publish_id}, indent=2))


if __name__ == "__main__":
    main()
