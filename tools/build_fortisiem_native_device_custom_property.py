"""
Native build of device_custom_property. Deviates slightly from the pure "one Groovy step for hash
only" pattern: this entity's schema is intentionally map-typed (dynamic/admin-defined property
names, same justification already used when the schema itself was built) rather than fixed columns,
which doesn't fit NiFi's schema-based XMLReader (that needs known field names declared up front).
No documented example of the GET response's exact XML tags exists, and the endpoint is confirmed
empty on every device tested in this tenant -- so rather than guess wrong on tag names with no way
to verify, the one Groovy step does double duty: parse whatever XML comes back into a
{name: value} map (defensively handling both plausible tag conventions), emit it as JSON, and hash
the ORIGINAL raw XML bytes (before any transformation) so dedup still reflects true source content.
From there it's native JSON Record processing (JsonTreeReader/JsonRecordSetWriter) same as
everywhere else -- this is the one entity whose raw topic format changes from XML to JSON, justified
by having no fixed shape to preserve as XML in the first place.
"""
import sys
sys.path.insert(0, "tools")
from build_fortisiem_native_lib import (
    login, nifi, nifi_ok, create_controller_service, enable_controller_service,
    create_processor, update_processor, create_connection, delete_processor, delete_connection,
    get_flow, STANDARD_HEADER_REGEX, PG_ID, DMC_SERVICE_ID, KAFKA_CONNECTION_ID,
    SCHEMA_REGISTRY_ID, SCHEMA_REF_WRITER_ID,
)
import json
import time

ENTITY = "device_custom_property"
SCHEMA_NAME = f"bronze.fortisiem.{ENTITY}__raw.avro-value"
GROOVY_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-groovyx-nar", "version": "2.9.0"}
STANDARD_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-standard-nar", "version": "2.9.0"}
UPDATE_ATTR_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-update-attribute-nar", "version": "2.9.0"}
KAFKA_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-kafka-nar", "version": "2.9.0"}
JSON_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-record-serialization-services-nar", "version": "2.9.0"}

PARSE_AND_HASH_SCRIPT = '''import groovy.json.JsonOutput
import groovy.xml.XmlParser
import java.security.MessageDigest
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.io.OutputStreamCallback

def flowFile = session.get()
if (!flowFile) return

try {
    def textHolder = [value: '']
    session.read(flowFile, { inputStream -> textHolder.value = inputStream.getText('UTF-8') } as InputStreamCallback)
    def rawText = textHolder.value

    // Hash the ORIGINAL raw response bytes -- dedup must reflect true source content, not our
    // own map re-shaping below.
    def hash = MessageDigest.getInstance('SHA-256').digest(rawText.getBytes('UTF-8')).encodeHex().toString()
    flowFile = session.putAttribute(flowFile, 'content_SHA-256', hash)

    // No documented example of this endpoint's populated response exists, and it returns empty
    // for every device tested in this tenant -- so this defensively tries both plausible tag
    // conventions (name/value, and propertyName/propertyValue matching the UPDATE endpoint's own
    // vocabulary) rather than assuming one blind.
    def properties = [:]
    if (rawText != null && rawText.trim().length() > 0) {
        try {
            def root = new XmlParser(false, false).parseText(rawText)
            root.children().each { node ->
                def nameNode = node.name?.text() ?: node.propertyName?.text()
                def valueNode = node.value?.text() ?: node.propertyValue?.text()
                if (nameNode) properties[nameNode] = valueNode
            }
        } catch (Exception ignore) {
            // Empty/non-XML body (e.g. blank 200 response) -- leave properties empty, not an error.
        }
    }

    def body = [
        source_platform: flowFile.getAttribute('source_platform') ?: '',
        customer_tenant_organization: flowFile.getAttribute('customer_tenant_organization') ?: '',
        source_object_type: flowFile.getAttribute('source_object_type') ?: '',
        source_object_id: flowFile.getAttribute('source_object_id') ?: '',
        extraction_timestamp: flowFile.getAttribute('extraction_timestamp') ?: '',
        source_event_update_timestamp: flowFile.getAttribute('source_event_update_timestamp') ?: '',
        api_endpoint_export_query_identity: flowFile.getAttribute('api_endpoint_export_query_identity') ?: '',
        cursor_window: flowFile.getAttribute('cursor_window') ?: '',
        payload_hash_fingerprint: hash,
        ingestion_run_batch_identity: flowFile.getAttribute('ingestion_run_batch_identity') ?: '',
        object_id: flowFile.getAttribute('object_id') ?: '',
        ingest_ts: null,
        properties: properties,
    ]
    flowFile = session.write(flowFile, { outputStream -> outputStream.write(JsonOutput.toJson(body).getBytes('UTF-8')) } as OutputStreamCallback)
    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    flowFile = session.putAttribute(flowFile, 'hash.error', e.message ?: e.toString())
    log.error('fortisiem device_custom_property parse/hash failed: ' + e.message, e)
    session.transfer(flowFile, REL_FAILURE)
}
'''


def main():
    token = login()
    flow = get_flow(token)
    procs = {p["component"]["name"]: p["component"] for p in flow["processGroupFlow"]["flow"]["processors"]}
    conns = flow["processGroupFlow"]["flow"]["connections"]

    def pid(name):
        return procs[name]["id"]

    json_reader_id = create_controller_service(token, "org.apache.nifi.json.JsonTreeReader", f"fortisiem.{ENTITY}__json_reader", {
        "Schema Access Strategy": "schema-name",
        "Schema Registry": SCHEMA_REGISTRY_ID,
        "Schema Name": SCHEMA_NAME,
    }, JSON_BUNDLE)
    json_writer_id = create_controller_service(token, "org.apache.nifi.json.JsonRecordSetWriter", f"fortisiem.{ENTITY}__json_writer", {
        "Schema Write Strategy": "no-schema",
        "Schema Access Strategy": "schema-name",
        "Schema Registry": SCHEMA_REGISTRY_ID,
        "Schema Name": SCHEMA_NAME,
    }, JSON_BUNDLE)
    enable_controller_service(token, json_reader_id)
    enable_controller_service(token, json_writer_id)

    rate_id = create_processor(token, "org.apache.nifi.processors.standard.ControlRate", f"fortisiem.{ENTITY}__rate_limit", {
        "Rate Control Criteria": "flowfile count", "Time Duration": "1 sec", "Maximum Rate": "3",
    }, STANDARD_BUNDLE, auto_terminate=["failure"])

    fetch_id = create_processor(token, "org.apache.nifi.processors.standard.InvokeHTTP", f"fortisiem.{ENTITY}__fetch", {
        "HTTP Method": "GET",
        "HTTP URL": "#{SOURCE_API_BASE}/cmdbDeviceInfo/properties?organization=${org_name}&orgId=${organization__attr_id}&ip=${access_ip}",
        "HTTP/2 Disabled": "True",
        "Request Username": "#{HTTP_USERNAME}",
        "Request Password": "#{HTTP_PASSWORD}",
        "Connection Timeout": "5 secs",
        "Socket Read Timeout": "30 secs",
    }, STANDARD_BUNDLE, auto_terminate=["Retry", "No Retry", "Original", "Failure"])

    set_ids_id = create_processor(token, "org.apache.nifi.processors.attributes.UpdateAttribute", f"fortisiem.{ENTITY}__set_ids", {
        "entity": ENTITY,
        "object_id": "${access_ip}",
        "api_path": "GET #{SOURCE_API_BASE}/cmdbDeviceInfo/properties",
        "kafka_topic": f"bronze.fortisiem.{ENTITY}__raw",
        "cursor_window": "${literal('')}",
    }, UPDATE_ATTR_BUNDLE)

    headers_id = create_processor(token, "org.apache.nifi.processors.attributes.UpdateAttribute", f"fortisiem.{ENTITY}__set_public_headers", {
        "source_platform": "fortisiem",
        "customer_tenant_organization": "${org_name}",
        "source_object_type": "${entity}",
        "source_object_id": "${object_id}",
        "source_event_update_timestamp": "${literal('')}",
        "api_endpoint_export_query_identity": "${api_path}",
        "object_id_composite": "fortisiem:${org_name}:${entity}:${object_id}",
        "ingest_ts": "${now():toNumber()}",
    }, UPDATE_ATTR_BUNDLE)

    parse_hash_id = create_processor(token, "org.apache.nifi.processors.groovyx.ExecuteGroovyScript", f"fortisiem.{ENTITY}__parse_and_hash", {
        "Script Body": PARSE_AND_HASH_SCRIPT,
    }, GROOVY_BUNDLE, auto_terminate=["failure"])

    metadata_id = create_processor(token, "org.apache.nifi.processors.standard.UpdateRecord", f"fortisiem.{ENTITY}__set_metadata", {
        "Record Reader": json_reader_id,
        "Record Writer": json_writer_id,
        "Replacement Value Strategy": "literal-value",
        "/ingest_ts": "${ingest_ts}",
        "/object_id": "${object_id_composite}",
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

    raw_publish_id = pid(f"fortisiem.{ENTITY}__raw__publish")
    avro_publish_id = pid(f"fortisiem.{ENTITY}__avro__publish")
    update_processor(token, avro_publish_id, properties={"Record Reader": json_reader_id})

    extract_key_name = "fortisiem.maximum__device_detail__extract_key"
    extract_key_id = pid(extract_key_name)

    create_connection(token, extract_key_id, extract_key_name, "PROCESSOR", rate_id, f"fortisiem.{ENTITY}__rate_limit", "PROCESSOR", ["matched"])
    create_connection(token, rate_id, f"fortisiem.{ENTITY}__rate_limit", "PROCESSOR", fetch_id, f"fortisiem.{ENTITY}__fetch", "PROCESSOR", ["success"])
    create_connection(token, fetch_id, f"fortisiem.{ENTITY}__fetch", "PROCESSOR", set_ids_id, f"fortisiem.{ENTITY}__set_ids", "PROCESSOR", ["Response"])
    create_connection(token, set_ids_id, f"fortisiem.{ENTITY}__set_ids", "PROCESSOR", headers_id, f"fortisiem.{ENTITY}__set_public_headers", "PROCESSOR", ["success"])
    create_connection(token, headers_id, f"fortisiem.{ENTITY}__set_public_headers", "PROCESSOR", parse_hash_id, f"fortisiem.{ENTITY}__parse_and_hash", "PROCESSOR", ["success"])
    create_connection(token, parse_hash_id, f"fortisiem.{ENTITY}__parse_and_hash", "PROCESSOR", metadata_id, f"fortisiem.{ENTITY}__set_metadata", "PROCESSOR", ["success"])
    create_connection(token, metadata_id, f"fortisiem.{ENTITY}__set_metadata", "PROCESSOR", dedupe_key_id, f"fortisiem.{ENTITY}__dedupe_key", "PROCESSOR", ["success"])
    create_connection(token, dedupe_key_id, f"fortisiem.{ENTITY}__dedupe_key", "PROCESSOR", dedupe_id, f"fortisiem.{ENTITY}__dedupe", "PROCESSOR", ["success"])
    create_connection(token, dedupe_id, f"fortisiem.{ENTITY}__dedupe", "PROCESSOR", raw_publish_id, f"fortisiem.{ENTITY}__raw__publish", "PROCESSOR", ["non-duplicate"])
    create_connection(token, replay_id, f"fortisiem.{ENTITY}__replay__consume", "PROCESSOR", avro_publish_id, f"fortisiem.{ENTITY}__avro__publish", "PROCESSOR", ["success"])

    # cleanup old
    old_extract_name = f"fortisiem.{ENTITY}__extract_from_device_detail"
    old_hash_name = f"fortisiem.{ENTITY}__raw__dedupe_hash"
    old_normalize_name = f"fortisiem.{ENTITY}__avro__normalize_json"
    old_ids = [procs[n]["id"] for n in (old_extract_name, old_hash_name, old_normalize_name) if n in procs]
    old_conns = [c for c in conns if c["component"]["source"]["id"] in old_ids or c["component"]["destination"]["id"] in old_ids]
    deleted_conns, failed_conns = [], []
    for c in old_conns:
        ok, resp = delete_connection(token, c["id"])
        (deleted_conns if ok else failed_conns).append(c["id"] if ok else {"id": c["id"], "resp": str(resp)[:200]})
    deleted_procs, failed_procs = [], []
    for name in (old_extract_name, old_hash_name, old_normalize_name):
        if name not in procs:
            continue
        ok, resp = delete_processor(token, procs[name]["id"], name)
        (deleted_procs if ok else failed_procs).append(name if ok else {"name": name, "resp": str(resp)[:200]})

    old_reader_name = f"fortisiem.{ENTITY}__avro_json_reader"
    status, cs_list = nifi("GET", f"/nifi-api/flow/process-groups/{PG_ID}/controller-services", token)
    old_reader_id = None
    for cs in cs_list.get("controllerServices", []):
        if cs["component"]["name"] == old_reader_name:
            old_reader_id = cs["component"]["id"]
    cs_result = None
    if old_reader_id:
        status, cs = nifi("GET", f"/nifi-api/controller-services/{old_reader_id}", token)
        version = cs["revision"]["version"]
        nifi("PUT", f"/nifi-api/controller-services/{old_reader_id}/run-status", token, {"revision": {"version": version}, "state": "DISABLED"})
        time.sleep(1.5)
        status, cs = nifi("GET", f"/nifi-api/controller-services/{old_reader_id}", token)
        version = cs["revision"]["version"]
        dstatus, dresp = nifi("DELETE", f"/nifi-api/controller-services/{old_reader_id}?version={version}", token)
        cs_result = "DELETED" if dstatus == 200 else f"FAILED {dstatus} {dresp}"

    print(json.dumps({"deleted_conns": deleted_conns, "failed_conns": failed_conns, "deleted_procs": deleted_procs,
                       "failed_procs": failed_procs, "old_reader_cs": cs_result}, indent=2))


if __name__ == "__main__":
    main()
