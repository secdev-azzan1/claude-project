"""
Parameterized native build for the 7 query_cmdb-family FortiSIEM entities (user, rule, report,
task, monitor, event_pulling, case). All seven share one endpoint (POST #{SOURCE_API_BASE}/query/cmdb),
one pagination scheme (start/size over a COLUMN-ORIENTED response), and differ only by
target/selectFields/tenant_field/source_object_id_fields -- all read from
.tmp_work/fs_build_spec.json (the authoritative supervisor spec) at run time. Nothing
entity-specific is hardcoded here; run once per entity: `python tools/build_fortisiem_native_query_cmdb.py <entity>`.

Chain (mirrors build_fortisiem_native_incident.py's JSON-POST/paginated structure, adapted for the
column-oriented body):
  fortisiem.maximum__run_metadata -> init_page -> build_body (ReplaceText, EL start/size)
    -> fetch (InvokeHTTP POST)
       --Response--> transform (JoltTransformJSON: positional data[] rows -> named objects,
                                 root-array output, per spec's shift-spec convention)
                      --success--> split (SplitJson $.[*])
                        --split--> extract (EvaluateJsonPath: tenant + source_object_id fields,
                                             SANITIZED names) --matched--> set_ids -> hash
                                             -> set_public_headers -> set_metadata -> dedupe_key
                                             -> dedupe -> raw__publish
       --Response--> page_meta (EvaluateJsonPath $.totalCount off the RAW fetch response, since the
                                 Jolt shift only keeps "data" and drops totalCount)
                      --matched/unmatched--> has_more --has_more--> next_page -> build_body (loop)
  replay__consume -> avro__publish

object_id (4-segment HASHLESS form, per platform-wide rule -- NEVER 5-segment/hashed):
  fortisiem:${tenant}:<entity>:${source_object_id}
source_object_id = sanitized source_object_id_fields, in spec order, joined with "_".
dedupe.key stays separate and DOES keep the content hash, same as every other fortisiem entity.

Avro field names are sanitized to [A-Za-z_][A-Za-z0-9_]* (FortiSIEM field names such as
"User_Password_Age_(Days)" contain illegal chars). The Jolt shift spec, the EvaluateJsonPath
extraction, and the registered Avro schema all use the SAME sanitized name per source column, so
the mapping stays consistent end-to-end.
"""
import json
import re
import sys
import urllib.parse

sys.path.insert(0, "tools")
from build_fortisiem_native_lib import (
    login, nifi, run_curl, create_controller_service, enable_controller_service,
    create_processor, update_processor, create_connection, get_flow,
    STANDARD_HEADER_REGEX, HASH_SCRIPT, PG_ID, DMC_SERVICE_ID, KAFKA_CONNECTION_ID,
    SCHEMA_REGISTRY_ID, make_avro_writer_props, xml_reader_bundle,
)

APICURIO_CCOMPAT = "https://apicurio.datapasc.com/apis/ccompat/v7"
SPEC_PATH = ".tmp_work/fs_build_spec.json"

GROOVY_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-groovyx-nar", "version": "2.9.0"}
STANDARD_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-standard-nar", "version": "2.9.0"}
UPDATE_ATTR_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-update-attribute-nar", "version": "2.9.0"}
KAFKA_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-kafka-nar", "version": "2.9.0"}
JSON_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-record-serialization-services-nar", "version": "2.9.0"}
JOLT_BUNDLE = {"group": "org.apache.nifi", "artifact": "nifi-jolt-nar", "version": "2.9.0"}

QUERY_CMDB_ENTITIES = ["user", "rule", "report", "task", "monitor", "event_pulling", "case"]
PAGE_SIZE = 500

# Date -> long (epoch millis), matching the convention already used for every other *Time field in
# this flow (e.g. incidentFirstSeen/incidentLastSeen). DurationHour values in the samples (Age,
# Resolution_Time) are fractional (e.g. 20843.0) -> double.
TYPE_MAP = {
    "String": ["null", "string"],
    "Long": ["null", "long"],
    "Double": ["null", "double"],
    "Date": ["null", "long"],
    "DurationHour": ["null", "double"],
}


def sanitize(name):
    out = re.sub(r"[^A-Za-z0-9_]", "_", name)
    if not out or not re.match(r"[A-Za-z_]", out[0]):
        out = "_" + out
    return out


def apicurio_get(path, timeout=30):
    out = run_curl(["-w", "\nHTTP_STATUS:%{http_code}", f"{APICURIO_CCOMPAT}{path}"], timeout=timeout)
    raw, status_txt = out.rsplit("\nHTTP_STATUS:", 1)
    status = int(status_txt.strip()[:3])
    if status < 200 or status > 299:
        return status, None
    return status, json.loads(raw.strip())


def apicurio_post(path, body, timeout=30):
    args = ["-X", "POST", "-H", "Content-Type: application/vnd.schemaregistry.v1+json",
            "--data-binary", "@-", "-w", "\nHTTP_STATUS:%{http_code}", f"{APICURIO_CCOMPAT}{path}"]
    out = run_curl(args, json.dumps(body), timeout=timeout)
    raw, status_txt = out.rsplit("\nHTTP_STATUS:", 1)
    status = int(status_txt.strip()[:3])
    if status < 200 or status > 299:
        raise RuntimeError(f"POST {path} HTTP {status}: {raw[:1000]}")
    return json.loads(raw.strip())


def register_schema(subject, schema):
    status, latest = apicurio_get(f"/subjects/{urllib.parse.quote(subject, safe='')}/versions/latest")
    if status == 200 and latest is not None:
        try:
            if json.loads(latest["schema"]) == schema:
                return {"subject": subject, "changed": False, "version": latest["version"]}
        except Exception:
            pass
    result = apicurio_post(f"/subjects/{urllib.parse.quote(subject, safe='')}/versions", {"schema": json.dumps(schema)})
    return {"subject": subject, "changed": True, "id": result.get("id")}


def build_schema(entity, spec_entity, sanitized_names):
    standard_fields = [
        "source_platform", "customer_tenant_organization", "source_object_type", "source_object_id",
        "extraction_timestamp", "source_event_update_timestamp", "api_endpoint_export_query_identity",
        "cursor_window", "payload_hash_fingerprint", "ingestion_run_batch_identity",
    ]
    fields = [{"name": f, "type": ["null", "string"], "default": None} for f in standard_fields]
    fields.append({"name": "object_id", "type": ["null", "string"], "default": None})
    fields.append({"name": "ingest_ts", "type": ["null", "long"], "default": None})
    for sname, ctype in zip(sanitized_names, spec_entity["returned_columnTypes"]):
        fields.append({"name": sname, "type": TYPE_MAP.get(ctype, ["null", "string"]), "default": None})
    return {
        "type": "record",
        "name": f"fortisiem_{entity}_raw_avro",
        "namespace": "bronze.fortisiem",
        "fields": fields,
    }


def get_or_create_processor(token, procs, proc_type, name, properties, bundle, auto_terminate=None):
    if name in procs:
        update_processor(token, procs[name]["id"], properties=properties, auto_terminate=auto_terminate, name=name)
        return procs[name]["id"]
    return create_processor(token, proc_type, name, properties, bundle, auto_terminate=auto_terminate)


def get_or_create_connection(token, conns, source_id, source_name, source_type, dest_id, dest_name, dest_type, relationships):
    rel_set = set(relationships)
    for c in conns:
        comp = c["component"]
        if comp["source"]["id"] == source_id and comp["destination"]["id"] == dest_id and set(comp.get("selectedRelationships", [])) == rel_set:
            return c["id"]
    return create_connection(token, source_id, source_name, source_type, dest_id, dest_name, dest_type, relationships)


def get_or_create_controller_service(token, cs_list, service_type, name, properties, bundle):
    for cs in cs_list:
        if cs["component"]["name"] == name:
            return cs["component"]["id"], False
    return create_controller_service(token, service_type, name, properties, bundle), True


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in QUERY_CMDB_ENTITIES:
        raise SystemExit(f"usage: build_fortisiem_native_query_cmdb.py <entity>  (one of {QUERY_CMDB_ENTITIES})")
    entity = sys.argv[1]

    with open(SPEC_PATH, "r", encoding="utf-8") as f:
        spec = json.load(f)
    if entity not in spec["entities"] or spec["entities"][entity]["source_kind"] != "query_cmdb":
        raise SystemExit(f"{entity} is not a query_cmdb entity in {SPEC_PATH}")
    e = spec["entities"][entity]

    sanitized_names = [sanitize(n) for n in e["returned_columnNames"]]
    name_map = dict(zip(e["returned_columnNames"], sanitized_names))
    tenant_sanitized = name_map[e["tenant_field"]]
    sid_sanitized = [name_map[f] for f in e["source_object_id_fields"]]

    SCHEMA_NAME = f"bronze.fortisiem.{entity}__raw.avro-value"

    token = login()
    flow = get_flow(token)
    procs = {p["component"]["name"]: p["component"] for p in flow["processGroupFlow"]["flow"]["processors"]}
    conns = flow["processGroupFlow"]["flow"]["connections"]

    def pid(name):
        return procs[name]["id"]

    status, cs_resp = nifi("GET", f"/nifi-api/flow/process-groups/{PG_ID}/controller-services", token)
    cs_list = cs_resp.get("controllerServices", [])

    json_reader_id, _ = get_or_create_controller_service(token, cs_list, "org.apache.nifi.json.JsonTreeReader",
        f"fortisiem.{entity}__json_reader", {
            "Schema Access Strategy": "schema-name", "Schema Registry": SCHEMA_REGISTRY_ID, "Schema Name": SCHEMA_NAME,
        }, JSON_BUNDLE)
    json_writer_id, _ = get_or_create_controller_service(token, cs_list, "org.apache.nifi.json.JsonRecordSetWriter",
        f"fortisiem.{entity}__json_writer", {
            "Schema Write Strategy": "no-schema", "Schema Access Strategy": "schema-name",
            "Schema Registry": SCHEMA_REGISTRY_ID, "Schema Name": SCHEMA_NAME,
        }, JSON_BUNDLE)
    avro_writer_id, _ = get_or_create_controller_service(token, cs_list, "org.apache.nifi.avro.AvroRecordSetWriter",
        f"fortisiem.{entity}__avro_writer", make_avro_writer_props(SCHEMA_NAME), xml_reader_bundle())
    enable_controller_service(token, json_reader_id)
    enable_controller_service(token, json_writer_id)
    enable_controller_service(token, avro_writer_id)

    # ---- request body: spec's request_body_template + start/size added for paging ----
    parsed_body = json.loads(e["request_body_template"])
    base_body_str = json.dumps({"target": parsed_body["target"], "selectFields": parsed_body["selectFields"]})
    body_template = base_body_str[:-1] + ', "start": ${page_start}, "size": ' + str(PAGE_SIZE) + "}"

    # ---- Jolt: positional data[] rows -> named objects, root-array output ----
    # "Jolt Transform"=jolt-transform-shift takes the bare shift spec (a Map), NOT the
    # chainr [{"operation":"shift","spec":{...}}] wrapper (that form is for chained/custom specs).
    shift_inner = {str(i): f"[&1].{sanitized_names[i]}" for i in range(len(sanitized_names))}
    jolt_spec = json.dumps({"data": {"*": shift_inner}})

    init_page_id = get_or_create_processor(token, procs, "org.apache.nifi.processors.attributes.UpdateAttribute",
        f"fortisiem.{entity}__init_page", {
            "entity": entity,
            "page_start": "0",
            "kafka_topic": f"bronze.fortisiem.{entity}__raw",
        }, UPDATE_ATTR_BUNDLE)

    build_body_id = get_or_create_processor(token, procs, "org.apache.nifi.processors.standard.ReplaceText",
        f"fortisiem.{entity}__build_body", {
            "Replacement Strategy": "Always Replace",
            "Evaluation Mode": "Entire text",
            "Replacement Value": body_template,
        }, STANDARD_BUNDLE, auto_terminate=["failure"])

    fetch_id = get_or_create_processor(token, procs, "org.apache.nifi.processors.standard.InvokeHTTP",
        f"fortisiem.{entity}__fetch", {
            "HTTP Method": "POST",
            "HTTP URL": e["url"],
            "HTTP/2 Disabled": "True",
            "Request Username": "#{HTTP_USERNAME}",
            "Request Password": "#{HTTP_PASSWORD}",
            "Request Body Enabled": "true",
            "Request Content-Type": "application/json",
            "Connection Timeout": "10 secs",
            "Socket Read Timeout": "30 secs",
        }, STANDARD_BUNDLE, auto_terminate=["Retry", "No Retry", "Original", "Failure"])

    jolt_id = get_or_create_processor(token, procs, "org.apache.nifi.processors.jolt.JoltTransformJSON",
        f"fortisiem.{entity}__transform", {
            "Jolt Transform": "jolt-transform-shift",
            "Jolt Specification": jolt_spec,
        }, JOLT_BUNDLE, auto_terminate=["failure"])

    split_id = get_or_create_processor(token, procs, "org.apache.nifi.processors.standard.SplitJson",
        f"fortisiem.{entity}__split", {
            "JsonPath Expression": "$.[*]",
            "Null Value Representation": "empty string",
        }, STANDARD_BUNDLE, auto_terminate=["failure", "original"])

    extract_props = {"Destination": "flowfile-attribute", "tenant_val": f"$.{tenant_sanitized}"}
    for i, sname in enumerate(sid_sanitized):
        extract_props[f"sid_{i}"] = f"$.{sname}"
    extract_id = get_or_create_processor(token, procs, "org.apache.nifi.processors.standard.EvaluateJsonPath",
        f"fortisiem.{entity}__extract", extract_props, STANDARD_BUNDLE, auto_terminate=["failure", "unmatched"])

    page_meta_id = get_or_create_processor(token, procs, "org.apache.nifi.processors.standard.EvaluateJsonPath",
        f"fortisiem.{entity}__page_meta", {
            "Destination": "flowfile-attribute",
            "resp_total": "$.totalCount",
        }, STANDARD_BUNDLE, auto_terminate=["failure"])

    has_more_id = get_or_create_processor(token, procs, "org.apache.nifi.processors.standard.RouteOnAttribute",
        f"fortisiem.{entity}__has_more", {
            "has_more": "${page_start:plus(" + str(PAGE_SIZE) + "):lt(${resp_total})}",
        }, STANDARD_BUNDLE, auto_terminate=["unmatched"])

    next_page_id = get_or_create_processor(token, procs, "org.apache.nifi.processors.attributes.UpdateAttribute",
        f"fortisiem.{entity}__next_page", {
            "page_start": "${page_start:plus(" + str(PAGE_SIZE) + ")}",
        }, UPDATE_ATTR_BUNDLE)

    sid_expr = "_".join("${sid_%d}" % i for i in range(len(sid_sanitized)))
    set_ids_id = get_or_create_processor(token, procs, "org.apache.nifi.processors.attributes.UpdateAttribute",
        f"fortisiem.{entity}__set_ids", {
            "object_id": sid_expr,
            "api_path": "POST #{SOURCE_API_BASE}/query/cmdb",
            "cursor_window": "${page_start}",
        }, UPDATE_ATTR_BUNDLE)

    hash_id = get_or_create_processor(token, procs, "org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
        f"fortisiem.{entity}__hash", {
            "Script Body": HASH_SCRIPT,
            "EXCLUDE_FIELDS": "${literal('')}",
            "Failure Strategy": "rollback",
        }, GROOVY_BUNDLE, auto_terminate=["failure"])

    headers_id = get_or_create_processor(token, procs, "org.apache.nifi.processors.attributes.UpdateAttribute",
        f"fortisiem.{entity}__set_public_headers", {
            "source_platform": "fortisiem",
            "customer_tenant_organization": "${tenant_val}",
            "source_object_type": "${entity}",
            "source_object_id": "${object_id}",
            "source_event_update_timestamp": "${literal('')}",
            "api_endpoint_export_query_identity": "${api_path}",
            "payload_hash_fingerprint": "${'content_SHA-256'}",
            "object_id_composite": "fortisiem:${tenant_val}:${entity}:${object_id}",
            "ingest_ts": "${now():toNumber()}",
        }, UPDATE_ATTR_BUNDLE)

    metadata_id = get_or_create_processor(token, procs, "org.apache.nifi.processors.standard.UpdateRecord",
        f"fortisiem.{entity}__set_metadata", {
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

    dedupe_key_id = get_or_create_processor(token, procs, "org.apache.nifi.processors.attributes.UpdateAttribute",
        f"fortisiem.{entity}__dedupe_key", {
            "dedupe.key": f"fortisiem_max_v9:fortisiem:{entity}:" + "${object_id}:${'content_SHA-256'}",
            "object_id": "${object_id_composite}",
        }, UPDATE_ATTR_BUNDLE)

    dedupe_id = get_or_create_processor(token, procs, "org.apache.nifi.processors.standard.DetectDuplicate",
        f"fortisiem.{entity}__dedupe", {
            "Cache Entry Identifier": "${dedupe.key}",
            "Age Off Duration": "24 hours",
            "Distributed Cache Service": DMC_SERVICE_ID,
            "Cache The Entry Identifier": "true",
        }, STANDARD_BUNDLE, auto_terminate=["duplicate", "failure"])

    replay_id = get_or_create_processor(token, procs, "org.apache.nifi.kafka.processors.ConsumeKafka",
        f"fortisiem.{entity}__replay__consume", {
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

    header_pattern_12 = STANDARD_HEADER_REGEX.replace(")$", "|object_id|ingest_ts)$")
    raw_publish_id = get_or_create_processor(token, procs, "org.apache.nifi.kafka.processors.PublishKafka",
        f"fortisiem.{entity}__raw__publish", {
            "Kafka Connection Service": KAFKA_CONNECTION_ID,
            "Topic Name": f"bronze.fortisiem.{entity}__raw",
            "Failure Strategy": "Route to Failure",
            "acks": "all",
            "compression.type": "gzip", "Transactions Enabled": "false",
            "FlowFile Attribute Header Pattern": header_pattern_12,
            "Kafka Key": "${source_object_id}",
            "Kafka Key Attribute Encoding": "utf-8",
        }, KAFKA_BUNDLE, auto_terminate=["success", "failure"])

    avro_publish_id = get_or_create_processor(token, procs, "org.apache.nifi.kafka.processors.PublishKafka",
        f"fortisiem.{entity}__avro__publish", {
            "Kafka Connection Service": KAFKA_CONNECTION_ID,
            "Topic Name": f"bronze.fortisiem.{entity}__raw.avro",
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

    get_or_create_connection(token, conns, run_metadata_id, run_metadata_name, "PROCESSOR", init_page_id, f"fortisiem.{entity}__init_page", "PROCESSOR", ["success"])
    get_or_create_connection(token, conns, init_page_id, f"fortisiem.{entity}__init_page", "PROCESSOR", build_body_id, f"fortisiem.{entity}__build_body", "PROCESSOR", ["success"])
    get_or_create_connection(token, conns, next_page_id, f"fortisiem.{entity}__next_page", "PROCESSOR", build_body_id, f"fortisiem.{entity}__build_body", "PROCESSOR", ["success"])
    get_or_create_connection(token, conns, build_body_id, f"fortisiem.{entity}__build_body", "PROCESSOR", fetch_id, f"fortisiem.{entity}__fetch", "PROCESSOR", ["success"])
    get_or_create_connection(token, conns, fetch_id, f"fortisiem.{entity}__fetch", "PROCESSOR", jolt_id, f"fortisiem.{entity}__transform", "PROCESSOR", ["Response"])
    get_or_create_connection(token, conns, fetch_id, f"fortisiem.{entity}__fetch", "PROCESSOR", page_meta_id, f"fortisiem.{entity}__page_meta", "PROCESSOR", ["Response"])
    get_or_create_connection(token, conns, jolt_id, f"fortisiem.{entity}__transform", "PROCESSOR", split_id, f"fortisiem.{entity}__split", "PROCESSOR", ["success"])
    get_or_create_connection(token, conns, split_id, f"fortisiem.{entity}__split", "PROCESSOR", extract_id, f"fortisiem.{entity}__extract", "PROCESSOR", ["split"])
    get_or_create_connection(token, conns, page_meta_id, f"fortisiem.{entity}__page_meta", "PROCESSOR", has_more_id, f"fortisiem.{entity}__has_more", "PROCESSOR", ["matched", "unmatched"])
    get_or_create_connection(token, conns, has_more_id, f"fortisiem.{entity}__has_more", "PROCESSOR", next_page_id, f"fortisiem.{entity}__next_page", "PROCESSOR", ["has_more"])
    get_or_create_connection(token, conns, extract_id, f"fortisiem.{entity}__extract", "PROCESSOR", set_ids_id, f"fortisiem.{entity}__set_ids", "PROCESSOR", ["matched"])
    get_or_create_connection(token, conns, set_ids_id, f"fortisiem.{entity}__set_ids", "PROCESSOR", hash_id, f"fortisiem.{entity}__hash", "PROCESSOR", ["success"])
    get_or_create_connection(token, conns, hash_id, f"fortisiem.{entity}__hash", "PROCESSOR", headers_id, f"fortisiem.{entity}__set_public_headers", "PROCESSOR", ["success"])
    get_or_create_connection(token, conns, headers_id, f"fortisiem.{entity}__set_public_headers", "PROCESSOR", metadata_id, f"fortisiem.{entity}__set_metadata", "PROCESSOR", ["success"])
    get_or_create_connection(token, conns, metadata_id, f"fortisiem.{entity}__set_metadata", "PROCESSOR", dedupe_key_id, f"fortisiem.{entity}__dedupe_key", "PROCESSOR", ["success"])
    get_or_create_connection(token, conns, dedupe_key_id, f"fortisiem.{entity}__dedupe_key", "PROCESSOR", dedupe_id, f"fortisiem.{entity}__dedupe", "PROCESSOR", ["success"])
    get_or_create_connection(token, conns, dedupe_id, f"fortisiem.{entity}__dedupe", "PROCESSOR", raw_publish_id, f"fortisiem.{entity}__raw__publish", "PROCESSOR", ["non-duplicate"])
    get_or_create_connection(token, conns, replay_id, f"fortisiem.{entity}__replay__consume", "PROCESSOR", avro_publish_id, f"fortisiem.{entity}__avro__publish", "PROCESSOR", ["success"])

    # ---- register avro schema (ccompat v7, compatibility NONE; skip if identical already registered) ----
    schema = build_schema(entity, e, sanitized_names)
    schema_result = register_schema(SCHEMA_NAME, schema)

    print(json.dumps({
        "entity": entity,
        "schema": schema_result,
        "raw_publish_id": raw_publish_id,
        "avro_publish_id": avro_publish_id,
    }, indent=2))


if __name__ == "__main__":
    main()
