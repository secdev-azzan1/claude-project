"""
Native rebuild of the `organization` entity in fortisiem.maximum_useful, matching rapid7_asyad's
exact architecture: set_ids -> hash (one Groovy step) -> set_public_headers -> set_metadata
(native UpdateRecord) -> dedupe_key -> dedupe (native DetectDuplicate) -> raw__publish, plus
replay__consume -> avro__publish. Uses native XMLReader/XMLRecordSetWriter since FortiSIEM's raw
topic stays XML (source-native), unlike rapid7/sentinelone's JSON.

Also fixes a latent bug found while researching this: today, list_devices__fetch is only triggered
off organization__raw__dedupe_hash's 'success' relationship -- since the old Groovy dedup silently
drops (not routes) duplicates, once org metadata stabilizes and gets deduped as unchanged, device
listing would silently stop firing entirely. rapid7 doesn't have this problem: children fan out
BEFORE dedup, not after. This rebuild fans list_devices__fetch out of list_organizations__extract
directly, independent of organization's own dedup outcome.

Also fixes a second latent gap found in rapid7 itself while designing this: rapid7's replay__consume
Header Name Pattern is only 10 fields (missing object_id/ingest_ts), so rapid7's avro topic is
missing those 2 as actual Kafka headers today (present in the body, absent as headers). Not fixing
rapid7 (out of scope here), but not replicating the gap into fortisiem either -- this build uses the
full 12-field pattern on replay__consume.
"""
import sys
sys.path.insert(0, "tools")
from build_fortisiem_native_lib import (
    NIFI_BASE, login, nifi, nifi_ok, create_controller_service, enable_controller_service,
    create_processor, update_processor, create_connection, delete_processor, delete_connection,
    get_flow, xml_reader_bundle, make_xml_reader_props, make_xml_writer_props,
    STANDARD_HEADER_REGEX, HASH_SCRIPT, PG_ID, DMC_SERVICE_ID, KAFKA_CONNECTION_ID,
)
import json

ENTITY = "organization"
SCHEMA_NAME = f"bronze.fortisiem.{ENTITY}__raw.avro-value"
ROOT_TAG = "domain"
GROOVY_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-groovyx-nar", "version": "2.9.0"}
STANDARD_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-standard-nar", "version": "2.9.0"}
UPDATE_ATTR_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-update-attribute-nar", "version": "2.9.0"}
KAFKA_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-kafka-nar", "version": "2.9.0"}


def main():
    token = login()
    flow = get_flow(token)
    procs = {p["component"]["name"]: p["component"] for p in flow["processGroupFlow"]["flow"]["processors"]}
    conns = flow["processGroupFlow"]["flow"]["connections"]

    def pid(name):
        return procs[name]["id"]

    # 1) controller services
    reader_id = create_controller_service(token, "org.apache.nifi.xml.XMLReader", f"fortisiem.{ENTITY}__xml_reader",
                                           make_xml_reader_props(SCHEMA_NAME), xml_reader_bundle())
    writer_id = create_controller_service(token, "org.apache.nifi.xml.XMLRecordSetWriter", f"fortisiem.{ENTITY}__xml_writer",
                                           make_xml_writer_props(SCHEMA_NAME, ROOT_TAG), xml_reader_bundle())
    enable_controller_service(token, reader_id)
    enable_controller_service(token, writer_id)

    # 2) new processors
    set_ids_id = create_processor(token, "org.apache.nifi.processors.attributes.UpdateAttribute", f"fortisiem.{ENTITY}__set_ids", {
        "entity": ENTITY,
        "object_id": "${org_name}",
        "api_path": "GET #{SOURCE_API_BASE}/config/Domain",
        "kafka_topic": f"bronze.fortisiem.{ENTITY}__raw",
        "cursor_window": "${literal('')}",
    }, UPDATE_ATTR_BUNDLE)

    hash_id = create_processor(token, "org.apache.nifi.processors.groovyx.ExecuteGroovyScript", f"fortisiem.{ENTITY}__hash", {
        "Script Body": HASH_SCRIPT,
        "EXCLUDE_FIELDS": "${literal('')}",
        "Failure Strategy": "rollback",
    }, GROOVY_BUNDLE)

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
    }, STANDARD_BUNDLE)

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

    print(json.dumps({
        "reader_id": reader_id, "writer_id": writer_id, "set_ids_id": set_ids_id, "hash_id": hash_id,
        "headers_id": headers_id, "metadata_id": metadata_id, "dedupe_key_id": dedupe_key_id,
        "dedupe_id": dedupe_id, "replay_id": replay_id,
    }, indent=2))

    # 3) reconfigure existing raw/avro publish + get their ids
    raw_publish_id = pid(f"fortisiem.{ENTITY}__raw__publish")
    avro_publish_id = pid(f"fortisiem.{ENTITY}__avro__publish")
    update_processor(token, avro_publish_id, properties={"Record Reader": reader_id})

    # 4) connections
    extract_name = "fortisiem.maximum__list_organizations__extract"
    extract_id = pid(extract_name)
    list_devices_fetch_id = pid("fortisiem.maximum__list_devices__fetch")

    create_connection(token, extract_id, extract_name, "PROCESSOR", set_ids_id, f"fortisiem.{ENTITY}__set_ids", "PROCESSOR", ["matched"])
    create_connection(token, extract_id, extract_name, "PROCESSOR", list_devices_fetch_id, "fortisiem.maximum__list_devices__fetch", "PROCESSOR", ["matched"])
    create_connection(token, set_ids_id, f"fortisiem.{ENTITY}__set_ids", "PROCESSOR", hash_id, f"fortisiem.{ENTITY}__hash", "PROCESSOR", ["success"])
    create_connection(token, hash_id, f"fortisiem.{ENTITY}__hash", "PROCESSOR", headers_id, f"fortisiem.{ENTITY}__set_public_headers", "PROCESSOR", ["success"])
    create_connection(token, headers_id, f"fortisiem.{ENTITY}__set_public_headers", "PROCESSOR", metadata_id, f"fortisiem.{ENTITY}__set_metadata", "PROCESSOR", ["success"])
    create_connection(token, metadata_id, f"fortisiem.{ENTITY}__set_metadata", "PROCESSOR", dedupe_key_id, f"fortisiem.{ENTITY}__dedupe_key", "PROCESSOR", ["success"])
    create_connection(token, dedupe_key_id, f"fortisiem.{ENTITY}__dedupe_key", "PROCESSOR", dedupe_id, f"fortisiem.{ENTITY}__dedupe", "PROCESSOR", ["success"])
    create_connection(token, dedupe_id, f"fortisiem.{ENTITY}__dedupe", "PROCESSOR", raw_publish_id, f"fortisiem.{ENTITY}__raw__publish", "PROCESSOR", ["non-duplicate"])
    create_connection(token, replay_id, f"fortisiem.{ENTITY}__replay__consume", "PROCESSOR", avro_publish_id, f"fortisiem.{ENTITY}__avro__publish", "PROCESSOR", ["success"])

    # 5) delete old processors + their connections
    old_hash_name = f"fortisiem.{ENTITY}__raw__dedupe_hash"
    old_normalize_name = f"fortisiem.{ENTITY}__avro__normalize_json"
    old_hash_id = pid(old_hash_name)
    old_normalize_id = pid(old_normalize_name)

    old_conns = [c for c in conns if c["component"]["source"]["id"] in (old_hash_id, old_normalize_id)
                 or c["component"]["destination"]["id"] in (old_hash_id, old_normalize_id)]
    deleted_conns, failed_conns = [], []
    for c in old_conns:
        ok, resp = delete_connection(token, c["id"])
        (deleted_conns if ok else failed_conns).append(c["id"] if ok else {"id": c["id"], "resp": str(resp)[:200]})

    deleted_procs, failed_procs = [], []
    for name, _id in ((old_hash_name, old_hash_id), (old_normalize_name, old_normalize_id)):
        ok, resp = delete_processor(token, _id, name)
        (deleted_procs if ok else failed_procs).append(name if ok else {"name": name, "resp": str(resp)[:200]})

    # 6) old avro_json_reader controller service is now orphaned -- disable + delete
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
        import time; time.sleep(1.5)
        status, cs = nifi("GET", f"/nifi-api/controller-services/{old_reader_id}", token)
        version = cs["revision"]["version"]
        dstatus, dresp = nifi("DELETE", f"/nifi-api/controller-services/{old_reader_id}?version={version}", token)
        cs_result = "DELETED" if dstatus == 200 else f"FAILED {dstatus} {dresp}"

    print(json.dumps({
        "deleted_conns": deleted_conns, "failed_conns": failed_conns,
        "deleted_procs": deleted_procs, "failed_procs": failed_procs,
        "old_reader_cs": cs_result,
    }, indent=2))


if __name__ == "__main__":
    main()
