"""
Native rebuild of interface/processor/storage/installed_software: switches extraction from
regex-parsing the full device-detail blob to the dedicated fields=<sectionName> endpoint (API Guide
p.17-18), confirmed live to still return a full <device> wrapper (not section-only) -- so extraction
narrows to just the target section via EvaluateXPath(Destination=flowfile-content) BEFORE SplitXml,
avoiding SplitXml's absolute-depth split polluting output with unrelated sibling elements at the
same tree depth (e.g. deviceType's children).

Each entity gets its own new InvokeHTTP + ControlRate (3/sec, matching device_detail__rate_limit's
existing pattern -- 4 new independent per-device calls instead of 1 shared one, so each needs its
own cap to avoid multiplying API load). Downstream from there: same set_ids -> hash -> set_public_headers
-> set_metadata -> dedupe_key -> dedupe -> raw__publish / replay__consume -> avro__publish pattern
already proven on organization/device.

Fan-out trigger point is device_detail__extract_key's 'matched' relationship directly (same
"fan out before dedup" fix already applied elsewhere), not the old device__raw__dedupe_hash's
'success' (which no longer exists after the device rebuild deleted it).
"""
import sys
sys.path.insert(0, "tools")
from build_fortisiem_native_lib import (
    login, nifi, nifi_ok, create_controller_service, enable_controller_service,
    create_processor, update_processor, create_connection, delete_processor, delete_connection,
    get_flow, xml_reader_bundle, make_xml_reader_props, make_xml_writer_props,
    STANDARD_HEADER_REGEX, HASH_SCRIPT, PG_ID, DMC_SERVICE_ID, KAFKA_CONNECTION_ID,
)
import json
import time

GROOVY_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-groovyx-nar", "version": "2.9.0"}
STANDARD_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-standard-nar", "version": "2.9.0"}
UPDATE_ATTR_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-update-attribute-nar", "version": "2.9.0"}
KAFKA_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-kafka-nar", "version": "2.9.0"}

ENTITIES = {
    "interface": {
        "section": "interfaces",
        "split_root": "networkinterface",
        "disambig_attr": "if_name",
        "disambig_xpath": "/networkinterface/name/text()",
        "object_id": "${access_ip}_${fragment.index}_${if_name}",
    },
    "processor": {
        "section": "processors",
        "split_root": "processor",
        "disambig_attr": "proc_name",
        "disambig_xpath": "/processor/name/text()",
        "object_id": "${access_ip}_${fragment.index}_${proc_name}",
    },
    "storage": {
        "section": "storages",
        "split_root": "storage",
        "disambig_attr": "storage_name",
        "disambig_xpath": "/storage/name/text()",
        "object_id": "${access_ip}_${fragment.index}_${storage_name}",
    },
    "installed_software": {
        "section": "applications",
        "split_root": "application",
        "disambig_attr": "sw_natural_id",
        "disambig_xpath": "/application/naturalId/text()",
        "object_id": "${sw_natural_id}",
    },
}


def build_entity(token, procs, entity, cfg):
    schema_name = f"bronze.fortisiem.{entity}__raw.avro-value"

    def pid(name):
        return procs[name]["id"]

    reader_id = create_controller_service(token, "org.apache.nifi.xml.XMLReader", f"fortisiem.{entity}__xml_reader",
                                           make_xml_reader_props(schema_name), xml_reader_bundle())
    writer_id = create_controller_service(token, "org.apache.nifi.xml.XMLRecordSetWriter", f"fortisiem.{entity}__xml_writer",
                                           make_xml_writer_props(schema_name, cfg["split_root"]), xml_reader_bundle())
    enable_controller_service(token, reader_id)
    enable_controller_service(token, writer_id)

    rate_id = create_processor(token, "org.apache.nifi.processors.standard.ControlRate", f"fortisiem.{entity}__rate_limit", {
        "Rate Control Criteria": "flowfile count",
        "Time Duration": "1 sec",
        "Maximum Rate": "3",
    }, STANDARD_BUNDLE, auto_terminate=["failure"])

    fetch_id = create_processor(token, "org.apache.nifi.processors.standard.InvokeHTTP", f"fortisiem.{entity}__fetch", {
        "HTTP Method": "GET",
        "HTTP URL": "#{SOURCE_API_BASE}/cmdbDeviceInfo/device?organization=${org_name}&ip=${access_ip}&loadDepend=true&fields=" + cfg["section"],
        "HTTP/2 Disabled": "True",
        "Request Username": "#{HTTP_USERNAME}",
        "Request Password": "#{HTTP_PASSWORD}",
        "Connection Timeout": "5 secs",
        "Socket Read Timeout": "30 secs",
    }, STANDARD_BUNDLE, auto_terminate=["Retry", "No Retry", "Original", "Failure"])

    narrow_id = create_processor(token, "org.apache.nifi.processors.standard.EvaluateXPath", f"fortisiem.{entity}__narrow", {
        "Destination": "flowfile-content",
        "Return Type": "auto-detect",
        "XPath Expression": "/device/" + cfg["section"],
    }, STANDARD_BUNDLE, auto_terminate=["unmatched", "failure"])

    split_id = create_processor(token, "org.apache.nifi.processors.standard.SplitXml", f"fortisiem.{entity}__split", {
        "Split Depth": "1",
    }, STANDARD_BUNDLE, auto_terminate=["original", "failure"])

    extract_id = create_processor(token, "org.apache.nifi.processors.standard.EvaluateXPath", f"fortisiem.{entity}__extract_fields", {
        "Destination": "flowfile-attribute",
        "Return Type": "auto-detect",
        "Path Not Found Behavior": "ignore",
        cfg["disambig_attr"]: cfg["disambig_xpath"],
    }, STANDARD_BUNDLE, auto_terminate=["unmatched", "failure"])

    set_ids_id = create_processor(token, "org.apache.nifi.processors.attributes.UpdateAttribute", f"fortisiem.{entity}__set_ids", {
        "entity": entity,
        "object_id": cfg["object_id"],
        "api_path": f"device detail /{cfg['section']}",
        "kafka_topic": f"bronze.fortisiem.{entity}__raw",
        "cursor_window": "${literal('')}",
    }, UPDATE_ATTR_BUNDLE)

    hash_id = create_processor(token, "org.apache.nifi.processors.groovyx.ExecuteGroovyScript", f"fortisiem.{entity}__hash", {
        "Script Body": HASH_SCRIPT,
        "EXCLUDE_FIELDS": "${literal('')}",
        "Failure Strategy": "rollback",
    }, GROOVY_BUNDLE, auto_terminate=["failure"])

    headers_id = create_processor(token, "org.apache.nifi.processors.attributes.UpdateAttribute", f"fortisiem.{entity}__set_public_headers", {
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

    metadata_id = create_processor(token, "org.apache.nifi.processors.standard.UpdateRecord", f"fortisiem.{entity}__set_metadata", {
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

    dedupe_key_id = create_processor(token, "org.apache.nifi.processors.attributes.UpdateAttribute", f"fortisiem.{entity}__dedupe_key", {
        "dedupe.key": f"fortisiem_max_v9:fortisiem:{entity}:" + "${object_id}:${'content_SHA-256'}",
        "object_id": "${object_id_composite}",
    }, UPDATE_ATTR_BUNDLE)

    dedupe_id = create_processor(token, "org.apache.nifi.processors.standard.DetectDuplicate", f"fortisiem.{entity}__dedupe", {
        "Cache Entry Identifier": "${dedupe.key}",
        "Age Off Duration": "24 hours",
        "Distributed Cache Service": DMC_SERVICE_ID,
        "Cache The Entry Identifier": "true",
    }, STANDARD_BUNDLE, auto_terminate=["duplicate", "failure"])

    replay_id = create_processor(token, "org.apache.nifi.kafka.processors.ConsumeKafka", f"fortisiem.{entity}__replay__consume", {
        "Kafka Connection Service": KAFKA_CONNECTION_ID,
        "Group ID": f"replay-avro-fortisiem-{entity}-v1",
        "Topic Format": "names",
        "Topics": f"bronze.fortisiem.{entity}__raw",
        "auto.offset.reset": "earliest",
        "Commit Offsets": "true",
        "Header Name Pattern": STANDARD_HEADER_REGEX,
        "Header Encoding": "UTF-8",
        "Processing Strategy": "FLOW_FILE",
        "Output Strategy": "USE_VALUE",
        "Key Attribute Encoding": "utf-8",
        "Key Format": "byte-array",
    }, KAFKA_BUNDLE, auto_terminate=["parse-failure"])

    raw_publish_id = pid(f"fortisiem.{entity}__raw__publish")
    avro_publish_id = pid(f"fortisiem.{entity}__avro__publish")
    update_processor(token, avro_publish_id, properties={"Record Reader": reader_id})

    extract_key_name = "fortisiem.maximum__device_detail__extract_key"
    extract_key_id = pid(extract_key_name)

    create_connection(token, extract_key_id, extract_key_name, "PROCESSOR", rate_id, f"fortisiem.{entity}__rate_limit", "PROCESSOR", ["matched"])
    create_connection(token, rate_id, f"fortisiem.{entity}__rate_limit", "PROCESSOR", fetch_id, f"fortisiem.{entity}__fetch", "PROCESSOR", ["success"])
    create_connection(token, fetch_id, f"fortisiem.{entity}__fetch", "PROCESSOR", narrow_id, f"fortisiem.{entity}__narrow", "PROCESSOR", ["Response"])
    create_connection(token, narrow_id, f"fortisiem.{entity}__narrow", "PROCESSOR", split_id, f"fortisiem.{entity}__split", "PROCESSOR", ["matched"])
    create_connection(token, split_id, f"fortisiem.{entity}__split", "PROCESSOR", extract_id, f"fortisiem.{entity}__extract_fields", "PROCESSOR", ["split"])
    create_connection(token, extract_id, f"fortisiem.{entity}__extract_fields", "PROCESSOR", set_ids_id, f"fortisiem.{entity}__set_ids", "PROCESSOR", ["matched"])
    create_connection(token, set_ids_id, f"fortisiem.{entity}__set_ids", "PROCESSOR", hash_id, f"fortisiem.{entity}__hash", "PROCESSOR", ["success"])
    create_connection(token, hash_id, f"fortisiem.{entity}__hash", "PROCESSOR", headers_id, f"fortisiem.{entity}__set_public_headers", "PROCESSOR", ["success"])
    create_connection(token, headers_id, f"fortisiem.{entity}__set_public_headers", "PROCESSOR", metadata_id, f"fortisiem.{entity}__set_metadata", "PROCESSOR", ["success"])
    create_connection(token, metadata_id, f"fortisiem.{entity}__set_metadata", "PROCESSOR", dedupe_key_id, f"fortisiem.{entity}__dedupe_key", "PROCESSOR", ["success"])
    create_connection(token, dedupe_key_id, f"fortisiem.{entity}__dedupe_key", "PROCESSOR", dedupe_id, f"fortisiem.{entity}__dedupe", "PROCESSOR", ["success"])
    create_connection(token, dedupe_id, f"fortisiem.{entity}__dedupe", "PROCESSOR", raw_publish_id, f"fortisiem.{entity}__raw__publish", "PROCESSOR", ["non-duplicate"])
    create_connection(token, replay_id, f"fortisiem.{entity}__replay__consume", "PROCESSOR", avro_publish_id, f"fortisiem.{entity}__avro__publish", "PROCESSOR", ["success"])

    return {
        "hash_id": hash_id, "metadata_id": metadata_id,
    }


def cleanup_old(token, procs, conns, entity):
    old_extract_name = f"fortisiem.{entity}__extract_from_device_detail"
    old_hash_name = f"fortisiem.{entity}__raw__dedupe_hash"
    old_normalize_name = f"fortisiem.{entity}__avro__normalize_json"
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

    old_reader_name = f"fortisiem.{entity}__avro_json_reader"
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

    return {"deleted_conns": deleted_conns, "failed_conns": failed_conns, "deleted_procs": deleted_procs,
            "failed_procs": failed_procs, "old_reader_cs": cs_result}


def main():
    token = login()
    only = sys.argv[1:] if len(sys.argv) > 1 else list(ENTITIES.keys())
    for entity in only:
        cfg = ENTITIES[entity]
        flow = get_flow(token)
        procs = {p["component"]["name"]: p["component"] for p in flow["processGroupFlow"]["flow"]["processors"]}
        conns = flow["processGroupFlow"]["flow"]["connections"]
        print(f"=== building {entity} ===")
        new_ids = build_entity(token, procs, entity, cfg)
        print(json.dumps(new_ids, indent=2))

        flow2 = get_flow(token)
        procs2 = {p["component"]["name"]: p["component"] for p in flow2["processGroupFlow"]["flow"]["processors"]}
        conns2 = flow2["processGroupFlow"]["flow"]["connections"]
        cleanup_result = cleanup_old(token, procs2, conns2, entity)
        print(json.dumps(cleanup_result, indent=2))


if __name__ == "__main__":
    main()
