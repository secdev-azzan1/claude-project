"""
Add the 17 Tier 1 entities to rapid7_securado.maximum_useful (PG 1508dfff-...), matching the exact
architecture of the 5 entities already there (site/asset/asset_service/asset_software/
asset_vulnerability) -- NOT maximum_v2's architecture, which has the double-publish bug
(dedupe -> avro__publish directly) that was fixed out of maximum_useful earlier this session.

Every config value here (bundle coords, controller-service IDs, property names, JSONPaths, object_id
formulas, endpoint URLs) was read from live NiFi/maximum_v2 config -- nothing guessed.

Three entity shapes:
  A (8): independent paginated catalog, triggered off maximum__run_metadata
  B (5): parent-scoped, triggered off an existing entity's extract/filter step
  C (4): discovered-ID -- ID harvested from a parent's extract, deduped through a "gate"
         DetectDuplicate so each unique ID is detail-fetched only once per age-off window

Static only: creates processors/connections/controller-services. Nothing is started.
Avro readers/writers are created pointing at schema subjects that do not exist yet -- they get
registered in Phase 2 from real sampled data, which is why avro__publish stays invalid until then.
"""
import json
import os
import subprocess
import sys
import time

NIFI_BASE = os.environ.get("NIFI_BASE", "https://nifi.datapasc.com").rstrip("/")
NIFI_USER = os.environ.get("NIFI_USER", "admin")
NIFI_PASSWORD = os.environ.get("NIFI_PASSWORD")

# Tenant-specific wiring. Defaults target rapid7_securado; override via env to run the identical
# build against rapid7_asyad (same API, same architecture, different PG + per-flow services).
#   R7_FLOW=rapid7_asyad R7_PG_ID=14db305d-01a0-1000-11f0-c68b900bbdb5 \
#   R7_JSON_READER=14db37c1-01a0-1000-a60e-3802d3d66510 \
#   R7_JSON_WRITER=14db3b77-01a0-1000-2655-91889bb5b5be \
#   R7_DEDUPE_CACHE=14db339e-01a0-1000-a6ec-a212ddee677c
PG_ID = os.environ.get("R7_PG_ID", "1508dfff-01a0-1000-861c-4cbb8f1c946c")
FLOW = os.environ.get("R7_FLOW", "rapid7_securado")

# --- per-flow controller services (reuse, do NOT create) ---
CS_JSON_READER = os.environ.get("R7_JSON_READER", "1508f5df-01a0-1000-4770-0c2de343db6b")   # maximum__json_reader  (set_metadata)
CS_JSON_WRITER = os.environ.get("R7_JSON_WRITER", "15090014-01a0-1000-dc35-795ef8981882")   # maximum__json_writer  (set_metadata)
CS_DEDUPE_CACHE = os.environ.get("R7_DEDUPE_CACHE", "1508ea7e-01a0-1000-a3d3-aa6b08862db1")  # maximum__dedupe__cache
CS_KAFKA = "40675f79-8eaa-3193-8f8d-026c8c1ee947"          # global__kafka_connection
CS_SCHEMA_REGISTRY = "db86aea0-2bee-3687-9187-5679904d69b0"
CS_SCHEMA_REF_WRITER = "2c59d8ad-103a-3e0e-fb8f-54726496f8b9"

B_STD = {"group": "org.apache.nifi", "artifact": "nifi-standard-nar", "version": "2.9.0"}
B_UA = {"group": "org.apache.nifi", "artifact": "nifi-update-attribute-nar", "version": "2.9.0"}
B_GROOVY = {"group": "org.apache.nifi", "artifact": "nifi-groovyx-nar", "version": "2.9.0"}
B_KAFKA = {"group": "org.apache.nifi", "artifact": "nifi-kafka-nar", "version": "2.9.0"}
B_REC = {"group": "org.apache.nifi", "artifact": "nifi-record-serialization-services-nar", "version": "2.9.0"}

HEADER_PATTERN = ("^(source_platform|customer_tenant_organization|source_object_type|source_object_id|"
                  "extraction_timestamp|source_event_update_timestamp|api_endpoint_export_query_identity|"
                  "cursor_window|payload_hash_fingerprint|ingestion_run_batch_identity|object_id|ingest_ts)$")

HASH_SCRIPT = open(os.path.join(os.path.dirname(__file__), "..", ".tmp_work", "hash_script.groovy"), encoding="utf-8").read() \
    if os.path.exists(os.path.join(os.path.dirname(__file__), "..", ".tmp_work", "hash_script.groovy")) else None
CAST_SCRIPT = open(os.path.join(os.path.dirname(__file__), "..", ".tmp_work", "cast_script.groovy"), encoding="utf-8").read() \
    if os.path.exists(os.path.join(os.path.dirname(__file__), "..", ".tmp_work", "cast_script.groovy")) else None

PAGED = "page=${page}&size=#{PAGE_SIZE}"

# cat A: independent catalogs. cat B: parent-scoped. cat C: discovered-ID gate.
ENTITIES = {
    # ---------- Category A ----------
    "agent": dict(cat="A", paginated=True, split=True, detail=None,
                  list_url="/agents?" + PAGED, extract={"agent_id": "$.id"},
                  object_id="${agent_id}", api_path="/agents", cursor=PAGED),
    "asset_group": dict(cat="A", paginated=True, split=True, detail="/asset_groups/${asset_group_id}",
                        list_url="/asset_groups?" + PAGED, extract={"asset_group_id": "$.id"},
                        object_id="${asset_group_id}", api_path="/asset_groups/${asset_group_id}", cursor=PAGED),
    "tag": dict(cat="A", paginated=True, split=True, detail="/tags/${tag_id}",
                list_url="/tags?" + PAGED, extract={"tag_id": "$.id", "tag_name": "$.name"},
                object_id="${tag_id}", api_path="/tags/${tag_id}", cursor=PAGED),
    "exploit": dict(cat="A", paginated=True, split=True, detail="/exploits/${exploit_id}",
                    list_url="/exploits?" + PAGED, extract={"exploit_id": "$.id"},
                    object_id="${exploit_id}", api_path="/exploits/${exploit_id}", cursor=PAGED),
    "malware_kit": dict(cat="A", paginated=True, split=True, detail="/malware_kits/${malware_kit_id}",
                        list_url="/malware_kits?" + PAGED, extract={"malware_kit_id": "$.id"},
                        object_id="${malware_kit_id}", api_path="/malware_kits/${malware_kit_id}", cursor=PAGED),
    "vulnerability_category": dict(cat="A", paginated=True, split=True, detail="/vulnerability_categories/${category_id}",
                                   list_url="/vulnerability_categories?" + PAGED, extract={"category_id": "$.id"},
                                   object_id="${category_id}", api_path="/vulnerability_categories/${category_id}", cursor=PAGED),
    "vulnerability_exception": dict(cat="A", paginated=True, split=True, detail="/vulnerability_exceptions/${exception_id}",
                                    list_url="/vulnerability_exceptions?" + PAGED, extract={"exception_id": "$.id"},
                                    object_id="${exception_id}", api_path="/vulnerability_exceptions/${exception_id}", cursor=PAGED),
    "vulnerability_reference": dict(cat="A", paginated=True, split=True, detail="/vulnerability_references/${reference_id}",
                                    list_url="/vulnerability_references?" + PAGED, extract={"reference_id": "$.id"},
                                    object_id="${reference_id}", api_path="/vulnerability_references/${reference_id}", cursor=PAGED),

    # ---------- Category B ----------
    # 1:1 fetch off each site; no pagination, no split (returns a single organization object).
    "site_organization": dict(cat="B", paginated=False, split=False, detail=None,
                              trigger=(f"{FLOW}.site__filter", "unmatched"),
                              list_url="/sites/${site_id}/organization", extract=None,
                              object_id="${site_id}", api_path="/sites/${site_id}/organization",
                              cursor="site=${site_id}"),
    "asset_group_asset": dict(cat="B", paginated=True, split=True, detail=None,
                              trigger=(f"{FLOW}.asset_group__extract", "matched"),
                              list_url="/asset_groups/${asset_group_id}/assets?" + PAGED,
                              extract={"asset_id": "$.id"},
                              object_id="${asset_group_id}_${asset_id}",
                              api_path="/asset_groups/${asset_group_id}/assets",
                              cursor="asset_group=${asset_group_id};page=${page}"),
    "tag_asset": dict(cat="B", paginated=True, split=True, detail=None,
                      trigger=(f"{FLOW}.tag__extract", "matched"),
                      list_url="/tags/${tag_id}/assets?" + PAGED, extract={"asset_id": "$.id"},
                      object_id="${tag_id}_${asset_id}", api_path="/tags/${tag_id}/assets",
                      cursor="tag=${tag_id};page=${page}"),
    "tag_site": dict(cat="B", paginated=True, split=True, detail=None,
                     trigger=(f"{FLOW}.tag__extract", "matched"),
                     list_url="/tags/${tag_id}/sites?" + PAGED, extract={"site_id": "$.id"},
                     object_id="${tag_id}_${site_id}", api_path="/tags/${tag_id}/sites",
                     cursor="tag=${tag_id};page=${page}"),
    # split but not paginated -- endpoint returns all solutions for that finding in one response.
    "asset_vulnerability_solution": dict(cat="B", paginated=False, split=True, detail=None,
                                         trigger=(f"{FLOW}.asset_vulnerability__extract", "matched"),
                                         list_url="/assets/${asset_id}/vulnerabilities/${vulnerability_id}/solution",
                                         extract={"solution_id": "$.id"},
                                         object_id="${asset_id}_${vulnerability_id}_${solution_id}",
                                         api_path="/assets/${asset_id}/vulnerabilities/${vulnerability_id}/solution",
                                         cursor="asset=${asset_id};vuln=${vulnerability_id}"),

    # ---------- Category C (gate) ----------
    "operating_system": dict(cat="C", gate_attr="os_id",
                             gate_source=(f"{FLOW}.asset__os_extract", "matched"),
                             detail="/operating_systems/${os_id}",
                             object_id="${os_id}", api_path="/operating_systems/${os_id}",
                             cursor="${literal('')}"),
    "software": dict(cat="C", gate_attr="software_id",
                     gate_source=(f"{FLOW}.asset_software__extract", "matched"),
                     detail="/software/${software_id}",
                     object_id="${software_id}", api_path="/software/${software_id}",
                     cursor="${literal('')}"),
    "vulnerability": dict(cat="C", gate_attr="vulnerability_id",
                          gate_source=(f"{FLOW}.asset_vulnerability__extract", "matched"),
                          detail="/vulnerabilities/${vulnerability_id}",
                          object_id="${vulnerability_id}", api_path="/vulnerabilities/${vulnerability_id}",
                          cursor="${literal('')}"),
    "solution": dict(cat="C", gate_attr="solution_id",
                     gate_source=(f"{FLOW}.asset_vulnerability_solution__extract", "matched"),
                     detail="/solutions/${solution_id}",
                     object_id="${solution_id}", api_path="/solutions/${solution_id}",
                     cursor="${literal('')}"),
}


# ----------------------------- NiFi REST plumbing -----------------------------

def run_curl(args, input_text=None, timeout=60, attempts=3):
    last = None
    for i in range(attempts):
        p = subprocess.run(["curl.exe", "--http1.1", "-k", "-sS"] + args, input=input_text,
                           text=True, capture_output=True, timeout=timeout)
        if p.returncode == 0:
            return p.stdout
        last = f"curl exit {p.returncode}: {p.stderr[:300]}"
        time.sleep(1 + i)
    raise RuntimeError(last)


def login():
    if not NIFI_PASSWORD:
        raise RuntimeError("Set NIFI_PASSWORD")
    import urllib.parse
    body = urllib.parse.urlencode({"username": NIFI_USER, "password": NIFI_PASSWORD})
    return run_curl(["-H", "Content-Type: application/x-www-form-urlencoded", "--data-binary", "@-",
                     f"{NIFI_BASE}/nifi-api/access/token"], body).strip()


TOKEN = None


def nifi(method, path, body=None, timeout=60):
    args = ["-X", method, "-H", f"Authorization: Bearer {TOKEN}"]
    input_text = None
    if body is not None:
        args += ["-H", "Content-Type: application/json", "--data-binary", "@-"]
        input_text = json.dumps(body)
    args += ["-w", "\nHTTP_STATUS:%{http_code}", f"{NIFI_BASE}{path}"]
    out = run_curl(args, input_text, timeout=timeout)
    raw, status = out.rsplit("\nHTTP_STATUS:", 1)
    status = int(status.strip()[:3])
    resp = {}
    if raw.strip():
        try:
            resp = json.loads(raw)
        except json.JSONDecodeError:
            resp = {"raw_text": raw[:600]}
    return status, resp


def nifi_ok(method, path, body=None, ctx=""):
    s, r = nifi(method, path, body)
    if s not in (200, 201):
        raise RuntimeError(f"{ctx or path} HTTP {s}: {json.dumps(r)[:400]}")
    return r


def get_flow():
    return nifi_ok("GET", f"/nifi-api/flow/process-groups/{PG_ID}", ctx="get flow")


def existing_processors():
    f = get_flow()
    return {p["component"]["name"]: p["component"]["id"] for p in f["processGroupFlow"]["flow"]["processors"]}


def existing_services():
    s, r = nifi("GET", f"/nifi-api/flow/process-groups/{PG_ID}/controller-services")
    return {cs["component"]["name"]: cs["component"]["id"] for cs in r.get("controllerServices", [])}


def mk_cs(name, ctype, props, bundle=B_REC):
    payload = {"revision": {"version": 0},
               "component": {"type": ctype, "bundle": bundle, "name": name, "properties": props}}
    r = nifi_ok("POST", f"/nifi-api/process-groups/{PG_ID}/controller-services", payload, ctx=f"CS {name}")
    return r["component"]["id"]


def enable_cs(cs_id):
    s, cs = nifi("GET", f"/nifi-api/controller-services/{cs_id}")
    v = cs["revision"]["version"]
    return nifi("PUT", f"/nifi-api/controller-services/{cs_id}/run-status",
                {"revision": {"version": v}, "state": "ENABLED"})


# Performance defaults, derived from a measured tuning pass (2026-08-25) that took the asset
# stage from 0.23 to 1.65 assets/sec (7.1x). Without these, every processor is created at
# NiFi's default concurrency 1 / runDuration 0, which is what made this flow slow:
#   - concurrency: I/O-bound work (HTTP, Redis, Kafka) gets the most, since blocked threads
#     cost no CPU. Transform steps get 4 -- they were the queue's second-biggest pile-up.
#   - runDuration 25ms: batches multiple flowfiles per onTrigger, cutting scheduling overhead.
#     Only on lightweight non-I/O types, mirroring the fast reference flow (Ingest(3).json).
# Requires NiFi maxTimerDrivenThreadCount well above the default 10 (set to 64) and
# global__redis_pool "Pool - Max Total" above 8 (set to 32) -- both are GLOBAL settings not
# created by this script. See tools/tune_rapid7_securado_perf.py.
# ControlRate was the single hardest cap (asset at 2/sec). Effectively unlimited now.
RATE_LIMIT_PER_SEC = os.environ.get("R7_RATE_PER_SEC", "100000")

PERF_CONCURRENCY = {
    "org.apache.nifi.processors.standard.InvokeHTTP": 8,
    "org.apache.nifi.processors.standard.DetectDuplicate": 4,
    "org.apache.nifi.processors.kafka.pubsub.PublishKafka": 4,
    "org.apache.nifi.processors.standard.EvaluateJsonPath": 4,
    "org.apache.nifi.processors.groovyx.ExecuteGroovyScript": 4,
    "org.apache.nifi.processors.attributes.UpdateAttribute": 4,
    "org.apache.nifi.processors.standard.UpdateRecord": 4,
    "org.apache.nifi.processors.standard.RouteOnAttribute": 4,
    "org.apache.nifi.processors.standard.ControlRate": 4,
    "org.apache.nifi.processors.standard.SplitJson": 2,
}
PERF_RUNDUR_TYPES = {
    "org.apache.nifi.processors.attributes.UpdateAttribute",
    "org.apache.nifi.processors.standard.EvaluateJsonPath",
    "org.apache.nifi.processors.standard.RouteOnAttribute",
    "org.apache.nifi.processors.standard.SplitJson",
    "org.apache.nifi.processors.standard.UpdateRecord",
    "org.apache.nifi.processors.standard.DetectDuplicate",
}


def mk_proc(name, ptype, props=None, bundle=B_STD, auto_term=None, pos=None):
    comp = {"type": ptype, "name": name, "bundle": bundle}
    cfg = {}
    if props:
        cfg["properties"] = props
    if auto_term:
        cfg["autoTerminatedRelationships"] = auto_term
    # ConsumeKafka is deliberately left at concurrency 1 -- consumer parallelism above the
    # topic's partition count is wasted, and these topics are single-partition.
    if ptype in PERF_CONCURRENCY:
        cfg["concurrentlySchedulableTaskCount"] = PERF_CONCURRENCY[ptype]
    if ptype in PERF_RUNDUR_TYPES:
        cfg["runDurationMillis"] = 25
    if cfg:
        comp["config"] = cfg
    if pos:
        comp["position"] = {"x": float(pos[0]), "y": float(pos[1])}
    payload = {"revision": {"version": 0}, "component": comp}
    r = nifi_ok("POST", f"/nifi-api/process-groups/{PG_ID}/processors", payload, ctx=f"proc {name}")
    return r["component"]["id"]


def connect(src_id, src_name, dst_id, dst_name, rels):
    payload = {"revision": {"version": 0},
               "component": {"parentGroupId": PG_ID,
                             "source": {"id": src_id, "type": "PROCESSOR", "groupId": PG_ID, "name": src_name},
                             "destination": {"id": dst_id, "type": "PROCESSOR", "groupId": PG_ID, "name": dst_name},
                             "selectedRelationships": rels}}
    return nifi_ok("POST", f"/nifi-api/process-groups/{PG_ID}/connections", payload,
                   ctx=f"conn {src_name}->{dst_name}")["id"]


# ----------------------------- builders -----------------------------

def http_props(url):
    return {"HTTP Method": "GET", "HTTP URL": "#{SOURCE_API_BASE}" + url, "HTTP/2 Disabled": "True",
            "Request Username": "#{HTTP_USERNAME}", "Request Password": "#{HTTP_PASSWORD}",
            "Connection Timeout": "5 secs", "Socket Read Timeout": "30 secs",
            # Default is 5 idle connections; with concurrency 8 the pool itself becomes the
            # limiter, so raise it to match. (Measured tuning pass 2026-08-25.)
            "Socket Idle Connections": "20"}


HTTP_AUTOTERM = ["No Retry", "Retry", "Original", "Failure"]


def build_entity(entity, cfg, row):
    """Create every processor + connection for one entity. Returns dict of created ids."""
    E = f"{FLOW}.{entity}"
    topic = f"bronze.{FLOW}.{entity}__raw"
    subject = f"bronze.{FLOW}.{entity}__raw.avro-value"
    ids = {}
    y = 200 + row * 420
    x = 0

    def nx(step=260):
        nonlocal x
        x += step
        return (x, y)

    # per-entity avro reader/writer (only avro__publish uses these)
    svcs = existing_services()
    rname, wname = f"{E}__avro_json_reader", f"{E}__avro_writer"
    reader = svcs.get(rname) or mk_cs(rname, "org.apache.nifi.json.JsonTreeReader", {
        "Schema Access Strategy": "schema-name", "Schema Registry": CS_SCHEMA_REGISTRY,
        "Schema Name": subject})
    writer = svcs.get(wname) or mk_cs(wname, "org.apache.nifi.avro.AvroRecordSetWriter", {
        "Schema Write Strategy": "schema-reference-writer", "Schema Reference Writer": CS_SCHEMA_REF_WRITER,
        "Schema Access Strategy": "schema-name", "Schema Registry": CS_SCHEMA_REGISTRY,
        "Schema Name": subject})
    ids["reader"], ids["writer"] = reader, writer

    cat = cfg["cat"]
    chain_head = None   # processor that ultimately feeds hash

    if cat in ("A", "B"):
        if cfg.get("paginated"):
            ids["init_page"] = mk_proc(f"{E}__init_page", "org.apache.nifi.processors.attributes.UpdateAttribute",
                                       {"entity": entity, "page": "0"}, B_UA, pos=nx())
        ids["fetch"] = mk_proc(f"{E}__fetch", "org.apache.nifi.processors.standard.InvokeHTTP",
                               http_props(cfg["list_url"]), B_STD, HTTP_AUTOTERM, pos=nx())
        if cfg.get("split"):
            # 'original' is consumed by page_meta only when paginated; otherwise nothing wants it,
            # so it must be auto-terminated or the processor is invalid.
            split_autoterm = ["failure"] if cfg.get("paginated") else ["failure", "original"]
            ids["split"] = mk_proc(f"{E}__split", "org.apache.nifi.processors.standard.SplitJson",
                                   {"JsonPath Expression": "$.resources[*]",
                                    "Null Value Representation": "empty string"},
                                   B_STD, split_autoterm, pos=nx())
        if cfg.get("paginated"):
            ids["page_meta"] = mk_proc(f"{E}__page_meta", "org.apache.nifi.processors.standard.EvaluateJsonPath",
                                       {"Destination": "flowfile-attribute", "Return Type": "auto-detect",
                                        "Path Not Found Behavior": "ignore",
                                        "Null Value Representation": "empty string",
                                        "total_pages": "$.page.totalPages"},
                                       B_STD, ["failure"], pos=(x, y + 160))
            ids["has_more"] = mk_proc(f"{E}__has_more", "org.apache.nifi.processors.standard.RouteOnAttribute",
                                      {"Routing Strategy": "Route to Property name",
                                       "has_more": "${page:toNumber():lt(${total_pages:toNumber():minus(1)})}"},
                                      B_STD, ["unmatched"], pos=(x + 260, y + 160))
            ids["next_page"] = mk_proc(f"{E}__next_page", "org.apache.nifi.processors.attributes.UpdateAttribute",
                                       {"page": "${page:toNumber():plus(1)}"}, B_UA, pos=(x + 520, y + 160))
        if cfg.get("extract"):
            eprops = {"Destination": "flowfile-attribute", "Return Type": "auto-detect",
                      "Path Not Found Behavior": "ignore", "Null Value Representation": "empty string"}
            eprops.update(cfg["extract"])
            ids["extract"] = mk_proc(f"{E}__extract", "org.apache.nifi.processors.standard.EvaluateJsonPath",
                                     eprops, B_STD, ["failure", "unmatched"], pos=nx())
            chain_head = ("extract", "matched")
        else:
            chain_head = ("fetch", "Response")
        if cfg.get("detail"):
            ids["rate_limit"] = mk_proc(f"{E}__rate_limit", "org.apache.nifi.processors.standard.ControlRate",
                                        {"Rate Control Criteria": "flowfile count", "Time Duration": "1 sec",
                                         "Maximum Rate": RATE_LIMIT_PER_SEC}, B_STD, ["failure"], pos=nx())
            ids["detail_fetch"] = mk_proc(f"{E}__detail_fetch", "org.apache.nifi.processors.standard.InvokeHTTP",
                                          http_props(cfg["detail"]), B_STD, HTTP_AUTOTERM, pos=nx())

    else:  # Category C -- gate chain
        ga = cfg["gate_attr"]
        ids["gate_filter"] = mk_proc(f"{E}__gate_filter", "org.apache.nifi.processors.standard.RouteOnAttribute",
                                     {"Routing Strategy": "Route to Property name",
                                      "has_id": "${" + ga + ":isEmpty():not()}"},
                                     B_STD, ["unmatched"], pos=nx())
        ids["gate_key"] = mk_proc(f"{E}__gate_key", "org.apache.nifi.processors.attributes.UpdateAttribute",
                                  {"dedupe.key": f"rapid7:{FLOW}:{entity}_gate:" + "${" + ga + "}"},
                                  B_UA, pos=nx())
        ids["gate"] = mk_proc(f"{E}__gate", "org.apache.nifi.processors.standard.DetectDuplicate",
                              {"Cache Entry Identifier": "${dedupe.key}", "Age Off Duration": "24 hours",
                               "Distributed Cache Service": CS_DEDUPE_CACHE, "Cache The Entry Identifier": "true"},
                              B_STD, ["failure", "duplicate"], pos=nx())
        ids["rate_limit"] = mk_proc(f"{E}__rate_limit", "org.apache.nifi.processors.standard.ControlRate",
                                    {"Rate Control Criteria": "flowfile count", "Time Duration": "1 sec",
                                     "Maximum Rate": RATE_LIMIT_PER_SEC}, B_STD, ["failure"], pos=nx())
        ids["detail_fetch"] = mk_proc(f"{E}__detail_fetch", "org.apache.nifi.processors.standard.InvokeHTTP",
                                      http_props(cfg["detail"]), B_STD, HTTP_AUTOTERM, pos=nx())

    # ---- common tail: hash -> set_ids -> headers -> metadata -> dedupe_key -> dedupe -> cast -> publish
    ids["hash"] = mk_proc(f"{E}__hash", "org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
                          {"Script Body": HASH_SCRIPT, "Failure Strategy": "rollback",
                           "EXCLUDE_FIELDS": "${literal('')}"}, B_GROOVY, ["failure"], pos=nx())
    ids["set_ids"] = mk_proc(f"{E}__set_ids", "org.apache.nifi.processors.attributes.UpdateAttribute",
                             {"api_path": cfg["api_path"], "cursor_window": cfg["cursor"], "entity": entity,
                              "kafka_topic": topic, "object_id": cfg["object_id"]}, B_UA, pos=nx())
    ids["set_public_headers"] = mk_proc(
        f"{E}__set_public_headers", "org.apache.nifi.processors.attributes.UpdateAttribute",
        {"api_endpoint_export_query_identity": "${api_path}", "customer_tenant_organization": FLOW,
         "ingest_ts": "${now():toNumber()}",
         "object_id_composite": "rapid7:" + FLOW + ":${entity}:${object_id}",
         "payload_hash_fingerprint": "${'content_SHA-256'}", "source_event_update_timestamp": "",
         "source_object_id": "${object_id}", "source_object_type": "${entity}", "source_platform": "rapid7"},
        B_UA, pos=nx())
    ids["set_metadata"] = mk_proc(
        f"{E}__set_metadata", "org.apache.nifi.processors.standard.UpdateRecord",
        {"Record Reader": CS_JSON_READER, "Record Writer": CS_JSON_WRITER,
         "Replacement Value Strategy": "literal-value",
         "/api_endpoint_export_query_identity": "${api_path}", "/cursor_window": "${cursor_window}",
         "/customer_tenant_organization": FLOW, "/extraction_timestamp": "${extraction_timestamp}",
         "/ingest_ts": "${ingest_ts}", "/ingestion_run_batch_identity": "${ingestion_run_batch_identity}",
         "/object_id": "${object_id_composite}", "/payload_hash_fingerprint": "${'content_SHA-256'}",
         "/source_event_update_timestamp": "", "/source_object_id": "${object_id}",
         "/source_object_type": entity, "/source_platform": "rapid7"},
        B_STD, ["failure"], pos=nx())
    ids["dedupe_key"] = mk_proc(f"{E}__dedupe_key", "org.apache.nifi.processors.attributes.UpdateAttribute",
                                {"dedupe.key": f"{FLOW}:{entity}:" + "${object_id}:${'content_SHA-256'}",
                                 "object_id": "${object_id_composite}"}, B_UA, pos=nx())
    ids["dedupe"] = mk_proc(f"{E}__dedupe", "org.apache.nifi.processors.standard.DetectDuplicate",
                            {"Cache Entry Identifier": "${dedupe.key}", "Age Off Duration": "24 hours",
                             "Distributed Cache Service": CS_DEDUPE_CACHE, "Cache The Entry Identifier": "true"},
                            B_STD, ["failure", "duplicate"], pos=nx())
    ids["cast"] = mk_proc(f"{E}__raw__cast_ingest_ts", "org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
                          {"Script Body": CAST_SCRIPT, "Failure Strategy": "rollback"},
                          B_GROOVY, ["failure"], pos=nx())
    ids["raw_publish"] = mk_proc(
        f"{E}__raw__publish", "org.apache.nifi.kafka.processors.PublishKafka",
        {"Kafka Connection Service": CS_KAFKA, "Topic Name": topic, "Failure Strategy": "Route to Failure",
         "acks": "all", "compression.type": "lz4", "max.request.size": "16 MB",
         # MUST be false, matching the existing 5 entities. PublishKafka defaults this to true, which
         # wraps every publish in a Kafka transaction -- that adds commit-marker offsets and can park
         # a read_committed consumer (replay__consume) at the last stable offset, so Avro never flows.
         "Transactions Enabled": "false",
         "FlowFile Attribute Header Pattern": HEADER_PATTERN, "Header Encoding": "UTF-8",
         "Kafka Key": "${source_object_id}", "Kafka Key Attribute Encoding": "utf-8"},
        B_KAFKA, ["success", "failure"], pos=nx())
    ids["replay"] = mk_proc(
        f"{E}__replay__consume", "org.apache.nifi.kafka.processors.ConsumeKafka",
        {"Kafka Connection Service": CS_KAFKA, "Group ID": f"replay-avro-{FLOW}-{entity}",
         "Topic Format": "names", "Topics": topic, "auto.offset.reset": "earliest", "Commit Offsets": "true",
         "Header Name Pattern": HEADER_PATTERN, "Header Encoding": "UTF-8",
         "Processing Strategy": "FLOW_FILE", "Output Strategy": "USE_VALUE",
         "Key Attribute Encoding": "utf-8", "Key Format": "byte-array"},
        B_KAFKA, ["parse-failure"], pos=(x, y + 160))
    ids["avro_publish"] = mk_proc(
        f"{E}__avro__publish", "org.apache.nifi.kafka.processors.PublishKafka",
        {"Kafka Connection Service": CS_KAFKA, "Topic Name": topic + ".avro",
         "Failure Strategy": "Route to Failure", "acks": "all", "compression.type": "lz4",
         "max.request.size": "500 MB", "Transactions Enabled": "false",
         "Record Reader": reader, "Record Writer": writer,
         "FlowFile Attribute Header Pattern": HEADER_PATTERN, "Header Encoding": "UTF-8",
         "Kafka Key": "${source_object_id}", "Kafka Key Attribute Encoding": "utf-8"},
        B_KAFKA, ["success", "failure"], pos=(x + 260, y + 160))

    # ---- connections ----
    def C(a, b, rels):
        connect(ids[a], f"{E}__{PROC_SUFFIX[a]}", ids[b], f"{E}__{PROC_SUFFIX[b]}", rels)

    if cat in ("A", "B"):
        if cfg.get("paginated"):
            C("init_page", "fetch", ["success"])
        if cfg.get("split"):
            C("fetch", "split", ["Response"])
            if cfg.get("extract"):
                C("split", "extract", ["split"])
            if cfg.get("paginated"):
                C("split", "page_meta", ["original"])
        else:
            if cfg.get("extract"):
                C("fetch", "extract", ["Response"])
        if cfg.get("paginated"):
            C("page_meta", "has_more", ["matched", "unmatched"])
            C("has_more", "next_page", ["has_more"])
            C("next_page", "fetch", ["success"])
        head_step, head_rel = chain_head
        if cfg.get("detail"):
            C(head_step, "rate_limit", [head_rel])
            C("rate_limit", "detail_fetch", ["success"])
            C("detail_fetch", "hash", ["Response"])
        else:
            C(head_step, "hash", [head_rel])
    else:
        C("gate_filter", "gate_key", ["has_id"])
        C("gate_key", "gate", ["success"])
        C("gate", "rate_limit", ["non-duplicate"])
        C("rate_limit", "detail_fetch", ["success"])
        C("detail_fetch", "hash", ["Response"])

    C("hash", "set_ids", ["success"])
    C("set_ids", "set_public_headers", ["success"])
    C("set_public_headers", "set_metadata", ["success"])
    C("set_metadata", "dedupe_key", ["success"])
    C("dedupe_key", "dedupe", ["success"])
    C("dedupe", "cast", ["non-duplicate"])
    C("cast", "raw_publish", ["success"])
    C("replay", "avro_publish", ["success"])
    return ids


PROC_SUFFIX = {
    "init_page": "init_page", "fetch": "fetch", "split": "split", "page_meta": "page_meta",
    "has_more": "has_more", "next_page": "next_page", "extract": "extract", "rate_limit": "rate_limit",
    "detail_fetch": "detail_fetch", "hash": "hash", "set_ids": "set_ids",
    "set_public_headers": "set_public_headers", "set_metadata": "set_metadata", "dedupe_key": "dedupe_key",
    "dedupe": "dedupe", "cast": "raw__cast_ingest_ts", "raw_publish": "raw__publish",
    "replay": "replay__consume", "avro_publish": "avro__publish",
    "gate_filter": "gate_filter", "gate_key": "gate_key", "gate": "gate",
}


def wire_trigger(entity, cfg, ids, procs):
    """Hook the new entity's head into the existing flow."""
    E = f"{FLOW}.{entity}"
    if cfg["cat"] == "A":
        src_name = f"{FLOW}.maximum__run_metadata"
        head = "init_page" if cfg.get("paginated") else "fetch"
        connect(procs[src_name], src_name, ids[head], f"{E}__{PROC_SUFFIX[head]}", ["success"])
        return f"{src_name} -> {E}__{PROC_SUFFIX[head]}"
    if cfg["cat"] == "B":
        src_name, rel = cfg["trigger"]
        head = "init_page" if cfg.get("paginated") else "fetch"
        connect(procs[src_name], src_name, ids[head], f"{E}__{PROC_SUFFIX[head]}", [rel])
        return f"{src_name} --{rel}--> {E}__{PROC_SUFFIX[head]}"
    src_name, rel = cfg["gate_source"]
    connect(procs[src_name], src_name, ids["gate_filter"], f"{E}__gate_filter", [rel])
    return f"{src_name} --{rel}--> {E}__gate_filter"


def main():
    global TOKEN
    TOKEN = login()
    if not HASH_SCRIPT or not CAST_SCRIPT:
        raise RuntimeError("hash_script.groovy / cast_script.groovy missing from .tmp_work")

    which = sys.argv[1:] or list(ENTITIES.keys())
    order = list(ENTITIES.keys())          # stable row index so canvas rows never collide
    results = {}
    for entity in which:
        cfg = ENTITIES[entity]
        procs = existing_processors()
        if f"{FLOW}.{entity}__hash" in procs:
            results[entity] = "ALREADY EXISTS - skipped"
            continue
        ids = build_entity(entity, cfg, row=order.index(entity))
        procs = existing_processors()
        trig = wire_trigger(entity, cfg, ids, procs)
        results[entity] = {"processors": len(ids) - 2, "trigger": trig}
        print(f"built {entity}: {results[entity]}")
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
