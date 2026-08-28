"""Rapid7 native ingestion v2 — nested process groups, 22 entities, zero Groovy.

Instance-parameterised: same file builds rapid7_asyad and rapid7_securado via
RAPID7_INSTANCE / RAPID7_API_BASE / RAPID7_HTTP_PASSWORD.

    rapid7_<inst>.maximum_useful      parent: parameter context + controller services
    |-- 10_site_asset    trigger(2h)      7 entities, OUT ports feeding the id-gates
    |-- 20_catalog       trigger(daily)   Tier 1
    |-- 30_vuln_catalog  trigger(weekly)  Tier 2
    `-- 90_replay        ConsumeKafka -> backfill ingest_ts -> avro publish (isolated)

90_replay is separate so replay consumers can never be swept up by a "start everything" on a
live PG — replay + live chain both feeding an avro publisher caused the 2x Iceberg duplication
on 2026-08-18.

CryptographicHashContent runs on the raw payload BEFORE metadata is added, so the 10 standard
fields plus ingest_ts / ingest_id are excluded from the fingerprint structurally.

Subcommands: build | inspect | infer-register | add-avro | build-replay | start-replay |
             connectors | start-live | stop
"""

import json, os, sys, time, urllib.parse
import requests
import build_fortisiem_maximum_useful as n

INSTANCE = os.environ.get("RAPID7_INSTANCE", "rapid7_asyad")
PG_NAME = os.environ.get("RAPID7_PG_NAME", f"{INSTANCE}.maximum_v2")
PARENT_PG_ID = os.environ.get("RAPID7_PARENT_PG_ID", "0a00e822-01a0-1000-68b7-f28e69779c95")
CTX_NAME = os.environ.get("RAPID7_CTX_NAME", f"{INSTANCE}.maximum")
API_BASE = os.environ.get("RAPID7_API_BASE", f"http://apisix:9080/{INSTANCE}/api/3")
HTTP_USERNAME = os.environ.get("RAPID7_HTTP_USERNAME", "apiuser")
HTTP_PASSWORD = os.environ.get("RAPID7_HTTP_PASSWORD")
PAGE_SIZE = os.environ.get("RAPID7_PAGE_SIZE", "500")
BLOCKED_SITES = os.environ.get("RAPID7_BLOCKED_SITES", "")
ASSET_RATE = os.environ.get("RAPID7_ASSET_RATE", "2")
REDIS_POOL_ID = os.environ.get("REDIS_POOL_ID", "b90bcbdb-d69c-3725-51d1-444dd57b9336")
KC = os.environ.get("KAFKA_CONNECT_BASE", "https://kafkaconnect.datapasc.com").rstrip("/")

n.PG_NAME, n.PARENT_PG_ID, n.CLIENT_ID, n.PG_ID = PG_NAME, PARENT_PG_ID, f"codex-{INSTANCE}-v2", None
HASH = "${'content_SHA-256'}"

E = ENTITIES = [
 dict(nm="site", grp="10_site_asset", pat="paged_detail", path="/sites", det="/sites/${site_id}",
      ex={"site_id":"$.id","site_name":"$.name"}, oid="${site_id}", api="/sites/${site_id}", trig="2h", blk=True),
 dict(nm="asset", grp="10_site_asset", pat="paged_detail", path="/sites/${site_id}/assets",
      det="/assets/${asset_id}", ex={"asset_id":"$.id"}, det_ex={"os_id":"$.osFingerprint.id"},
      oid="${site_id}_${asset_id}", api="/assets/${asset_id}", root=("site","filtered"), rate=True),
 dict(nm="asset_software", grp="10_site_asset", pat="paged", path="/assets/${asset_id}/software",
      ex={"software_id":"$.id"}, oid="${asset_id}_${software_id}", api="/assets/${asset_id}/software",
      root=("asset","rated")),
 dict(nm="asset_service", grp="10_site_asset", pat="paged_detail", path="/assets/${asset_id}/services",
      det="/assets/${asset_id}/services/${protocol}/${port}", ex={"protocol":"$.protocol","port":"$.port"},
      oid="${asset_id}_${protocol}_${port}", api="/assets/${asset_id}/services/${protocol}/${port}",
      root=("asset","rated")),
 dict(nm="asset_vulnerability", grp="10_site_asset", pat="paged_detail",
      path="/assets/${asset_id}/vulnerabilities", det="/assets/${asset_id}/vulnerabilities/${vulnerability_id}",
      ex={"vulnerability_id":"$.id"}, oid="${asset_id}_${vulnerability_id}",
      api="/assets/${asset_id}/vulnerabilities/${vulnerability_id}", root=("asset","rated")),
 dict(nm="asset_vulnerability_solution", grp="10_site_asset", pat="collection",
      det="/assets/${asset_id}/vulnerabilities/${vulnerability_id}/solution", ex={"solution_id":"$.id"},
      oid="${asset_id}_${vulnerability_id}_${solution_id}",
      api="/assets/${asset_id}/vulnerabilities/${vulnerability_id}/solution",
      root=("asset_vulnerability","extract")),
 dict(nm="site_organization", grp="10_site_asset", pat="singleton", det="/sites/${site_id}/organization",
      ex={}, oid="${site_id}", api="/sites/${site_id}/organization", root=("site","filtered")),

 dict(nm="agent", grp="20_catalog", pat="paged", path="/agents", ex={"agent_id":"$.id"},
      oid="${agent_id}", api="/agents", trig="daily"),
 dict(nm="tag", grp="20_catalog", pat="paged_detail", path="/tags", det="/tags/${tag_id}",
      ex={"tag_id":"$.id","tag_name":"$.name"}, oid="${tag_id}", api="/tags/${tag_id}", trig="daily"),
 dict(nm="tag_asset", grp="20_catalog", pat="paged", path="/tags/${tag_id}/assets", ex={"asset_id":"$.id"},
      oid="${tag_id}_${asset_id}", api="/tags/${tag_id}/assets", root=("tag","extract")),
 dict(nm="tag_site", grp="20_catalog", pat="paged", path="/tags/${tag_id}/sites", ex={"site_id":"$.id"},
      oid="${tag_id}_${site_id}", api="/tags/${tag_id}/sites", root=("tag","extract")),
 dict(nm="asset_group", grp="20_catalog", pat="paged_detail", path="/asset_groups",
      det="/asset_groups/${asset_group_id}", ex={"asset_group_id":"$.id"}, oid="${asset_group_id}",
      api="/asset_groups/${asset_group_id}", trig="daily"),
 dict(nm="asset_group_asset", grp="20_catalog", pat="paged", path="/asset_groups/${asset_group_id}/assets",
      ex={"asset_id":"$.id"}, oid="${asset_group_id}_${asset_id}",
      api="/asset_groups/${asset_group_id}/assets", root=("asset_group","extract")),
 dict(nm="software", grp="20_catalog", pat="gated", gate="software_id", det="/software/${software_id}",
      oid="${software_id}", api="/software/${software_id}", port="software_ids"),
 dict(nm="operating_system", grp="20_catalog", pat="gated", gate="os_id",
      det="/operating_systems/${os_id}", oid="${os_id}", api="/operating_systems/${os_id}", port="os_ids"),

 dict(nm="vulnerability", grp="30_vuln_catalog", pat="gated", gate="vulnerability_id",
      det="/vulnerabilities/${vulnerability_id}", oid="${vulnerability_id}",
      api="/vulnerabilities/${vulnerability_id}", port="vuln_ids"),
 dict(nm="solution", grp="30_vuln_catalog", pat="gated", gate="solution_id", det="/solutions/${solution_id}",
      oid="${solution_id}", api="/solutions/${solution_id}", port="solution_ids"),
 dict(nm="vulnerability_reference", grp="30_vuln_catalog", pat="paged_detail", path="/vulnerability_references",
      det="/vulnerability_references/${reference_id}", ex={"reference_id":"$.id"}, oid="${reference_id}",
      api="/vulnerability_references/${reference_id}", trig="weekly"),
 dict(nm="vulnerability_category", grp="30_vuln_catalog", pat="paged_detail", path="/vulnerability_categories",
      det="/vulnerability_categories/${category_id}", ex={"category_id":"$.id"}, oid="${category_id}",
      api="/vulnerability_categories/${category_id}", trig="weekly"),
 dict(nm="exploit", grp="30_vuln_catalog", pat="paged_detail", path="/exploits", det="/exploits/${exploit_id}",
      ex={"exploit_id":"$.id"}, oid="${exploit_id}", api="/exploits/${exploit_id}", trig="weekly"),
 dict(nm="malware_kit", grp="30_vuln_catalog", pat="paged_detail", path="/malware_kits",
      det="/malware_kits/${malware_kit_id}", ex={"malware_kit_id":"$.id"}, oid="${malware_kit_id}",
      api="/malware_kits/${malware_kit_id}", trig="weekly"),
 dict(nm="vulnerability_exception", grp="30_vuln_catalog", pat="paged_detail", path="/vulnerability_exceptions",
      det="/vulnerability_exceptions/${exception_id}", ex={"exception_id":"$.id"}, oid="${exception_id}",
      api="/vulnerability_exceptions/${exception_id}", trig="daily"),
]
NAMES = [e["nm"] for e in E]
BY = {e["nm"]: e for e in E}
GRPS = ["10_site_asset", "20_catalog", "30_vuln_catalog", "90_replay"]
# (source entity, handle on that entity, port, target group)
FEEDS = [("asset_software","extract","software_ids","20_catalog"),
         ("asset","det_extract","os_ids","20_catalog"),
         ("asset_vulnerability","extract","vuln_ids","30_vuln_catalog"),
         ("asset_vulnerability_solution","extract","solution_ids","30_vuln_catalog")]
TRIGS = {"2h": "2 hours", "daily": "24 hours", "weekly": "168 hours"}

topic = lambda e: f"bronze.{INSTANCE}.{e}__raw"
subject = lambda e: f"{topic(e)}.avro-value"


def ensure_ctx():
    for c in n.nifi("GET", "/nifi-api/flow/parameter-contexts").get("parameterContexts", []):
        if c["component"]["name"] == CTX_NAME: return c["id"]
    if not HTTP_PASSWORD: raise RuntimeError("Set RAPID7_HTTP_PASSWORD on first build")
    ps = [{"parameter":{"name":k,"value":v,"sensitive":s}} for k,v,s in [
        ("SOURCE_API_BASE",API_BASE,False),("HTTP_USERNAME",HTTP_USERNAME,False),
        ("HTTP_PASSWORD",HTTP_PASSWORD,True),("PAGE_SIZE",PAGE_SIZE,False),
        ("BLOCKED_SITES",BLOCKED_SITES,False)]]
    return n.nifi("POST","/nifi-api/parameter-contexts",
        {"revision":{"clientId":n.CLIENT_ID,"version":0},"component":{"name":CTX_NAME,"parameters":ps}})["id"]


def bind_ctx(pg_id, ctx):
    """Parameter contexts are NOT inherited by child PGs in NiFi — bind explicitly."""
    e = n.nifi("GET", f"/nifi-api/process-groups/{pg_id}")
    cur = (e["component"].get("parameterContext") or {}).get("id")
    if cur == ctx: return
    n.nifi("PUT", f"/nifi-api/process-groups/{pg_id}",
        {"revision":{"clientId":n.CLIENT_ID,"version":e["revision"]["version"]},
         "component":{"id":pg_id,"parameterContext":{"id":ctx}}})


def child(parent, name, x, y, ctx=None):
    got = None
    for pg in n.get_flow(parent).get("processGroups", []):
        if pg["component"]["name"] == name: got = pg["id"]
    if not got:
        got = n.nifi("POST", f"/nifi-api/process-groups/{parent}/process-groups",
            {"revision":{"clientId":n.CLIENT_ID,"version":0},
             "component":{"name":name,"position":{"x":float(x),"y":float(y)}}})["id"]
    if ctx: bind_ctx(got, ctx)
    return got


def port(pg, name, kind, x, y):
    key = "inputPorts" if kind=="INPUT_PORT" else "outputPorts"
    for p in n.get_flow(pg).get(key, []):
        if p["component"]["name"] == name: return p["id"]
    seg = "input-ports" if kind=="INPUT_PORT" else "output-ports"
    return n.nifi("POST", f"/nifi-api/process-groups/{pg}/{seg}",
        {"revision":{"clientId":n.CLIENT_ID,"version":0},
         "component":{"name":name,"position":{"x":float(x),"y":float(y)}}})["id"]


def link(in_pg, s_id, s_grp, s_type, d_id, d_grp, d_type, rels):
    rels = sorted(rels)
    port_link = s_type != "PROCESSOR" or d_type != "PROCESSOR"
    for c in n.get_flow(in_pg).get("connections", []):
        k = c["component"]
        if k["source"]["id"] != s_id or k["destination"]["id"] != d_id:
            continue
        # NiFi normalises port-connection relationships to [] or [""] inconsistently, so
        # match on endpoints alone for port links or we create duplicates on rebuild.
        if port_link or sorted(k.get("selectedRelationships", [])) == rels:
            return c["id"]
    return n.nifi("POST", f"/nifi-api/process-groups/{in_pg}/connections",
        {"revision":{"clientId":n.CLIENT_ID,"version":0},"component":{"parentGroupId":in_pg,
         "source":{"id":s_id,"groupId":s_grp,"type":s_type},
         "destination":{"id":d_id,"groupId":d_grp,"type":d_type},
         "selectedRelationships":rels,"flowFileExpiration":"0 sec",
         "backPressureObjectThreshold":20000,"backPressureDataSizeThreshold":"1 GB"}})["id"]


def invoke(name, url, x, y):
    p = n.invoke_props(url); p["Request Username"]="#{HTTP_USERNAME}"; p["Request Password"]="#{HTTP_PASSWORD}"
    return n.create_processor(name,"org.apache.nifi.processors.standard.InvokeHTTP",x,y,p,
                              ["Original","Retry","No Retry","Failure"])


def pub_props(t, avro=False, r=None, w=None):
    p = {"Kafka Connection Service":n.KAFKA_SERVICE_ID,"Topic Name":t,"Kafka Key":"${object_id}",
         "Kafka Key Attribute Encoding":"utf-8","Publish Strategy":"USE_VALUE",
         "Record Metadata Strategy":"FROM_PROPERTIES",
         "FlowFile Attribute Header Pattern":n.STANDARD_HEADER_PATTERN,"Header Encoding":"UTF-8",
         "Transactions Enabled":"false","acks":"all","compression.type":"gzip",
         "max.request.size":"16 MB","Failure Strategy":"Route to Failure"}
    if avro: p["Record Reader"], p["Record Writer"] = r, w
    return p


def paged(e, x, y):
    nm = e["nm"]
    init = n.create_processor(f"{nm}__init_page","org.apache.nifi.processors.attributes.UpdateAttribute",
        x,y,{"Store State":"Do not store state","page":"0","entity":nm},[])
    sep = "&" if "?" in e["path"] else "?"
    f = invoke(f"{nm}__fetch", f"#{{SOURCE_API_BASE}}{e['path']}{sep}page=${{page}}&size=#{{PAGE_SIZE}}", x+300,y)
    sp = n.create_processor(f"{nm}__split","org.apache.nifi.processors.standard.SplitJson",x+600,y,
        {"JsonPath Expression":"$.resources[*]","Max String Length":"20 MB",
         "Null Value Representation":"empty string"},["failure"])
    pm = n.create_processor(f"{nm}__page_meta","org.apache.nifi.processors.standard.EvaluateJsonPath",
        x+600,y+130,{"Destination":"flowfile-attribute","Return Type":"auto-detect",
        "Path Not Found Behavior":"ignore","Null Value Representation":"empty string",
        "total_pages":"$.page.totalPages"},["failure"])
    hm = n.create_processor(f"{nm}__has_more","org.apache.nifi.processors.standard.RouteOnAttribute",
        x+900,y+130,{"Routing Strategy":"Route to Property name",
        "has_more":"${page:toNumber():lt(${total_pages:toNumber():minus(1)})}"},["unmatched"])
    np_ = n.create_processor(f"{nm}__next_page","org.apache.nifi.processors.attributes.UpdateAttribute",
        x+1200,y+130,{"Store State":"Do not store state","page":"${page:toNumber():plus(1)}"},[])
    pr = {"Destination":"flowfile-attribute","Return Type":"auto-detect",
          "Path Not Found Behavior":"ignore","Null Value Representation":"empty string"}
    pr.update(e.get("ex") or {})
    ex = n.create_processor(f"{nm}__extract","org.apache.nifi.processors.standard.EvaluateJsonPath",
        x+900,y,pr,["failure","unmatched"])
    for a,b,r in [(init,f,"success"),(f,sp,"Response"),(sp,ex,"split"),(sp,pm,"original"),
                  (pm,hm,"matched"),(hm,np_,"has_more"),(np_,f,"success")]:
        n.create_connection(a,"",b,"",[r])
    n.create_connection(pm,"",hm,"",["unmatched"])
    return init, ex


def tail(e, x, y, dmc, src, rel):
    nm = e["nm"]
    h = n.create_processor(f"{nm}__hash","org.apache.nifi.processors.standard.CryptographicHashContent",
        x,y,{"Hash Algorithm":"SHA-256"},["failure"])
    ids = n.create_processor(f"{nm}__set_ids","org.apache.nifi.processors.attributes.UpdateAttribute",x+300,y,
        {"Store State":"Do not store state","entity":nm,"object_id":e["oid"],"api_path":e["api"],
         "cursor_window":"page=${page}","source_platform":"rapid7",
         "customer_tenant_organization":INSTANCE,"ingest_ts":"${now():toNumber()}","ingest_id":"${uuid}",
         # these four must exist as ATTRIBUTES too, or PublishKafka emits no header for them
         "source_object_type":nm,"source_object_id":e["oid"],
         "payload_hash_fingerprint":HASH,"source_event_update_timestamp":""},[])
    md = n.create_processor(f"{nm}__set_metadata","org.apache.nifi.processors.standard.UpdateRecord",x+600,y,
        {"Record Reader":READER,"Record Writer":WRITER,"Replacement Value Strategy":"literal-value",
         "/source_platform":"rapid7","/customer_tenant_organization":INSTANCE,"/source_object_type":nm,
         "/source_object_id":"${object_id}","/extraction_timestamp":"${extraction_timestamp}",
         "/source_event_update_timestamp":"","/api_endpoint_export_query_identity":"${api_path}",
         "/cursor_window":"${cursor_window}","/payload_hash_fingerprint":HASH,
         "/ingestion_run_batch_identity":"${ingestion_run_batch_identity}",
         "/ingest_ts":"${ingest_ts}","/ingest_id":"${ingest_id}"},["failure"])
    ky = n.create_processor(f"{nm}__dedupe_key","org.apache.nifi.processors.attributes.UpdateAttribute",x+900,y,
        {"Store State":"Do not store state",
         "dedupe.key":f"${{source_platform}}:${{customer_tenant_organization}}:{nm}:${{object_id}}:{HASH}"},[])
    dd = n.create_processor(f"{nm}__dedupe","org.apache.nifi.processors.standard.DetectDuplicate",x+1200,y,
        {"Cache Entry Identifier":"${dedupe.key}","Cache The Entry Identifier":"true",
         "Age Off Duration":"24 hours","Distributed Cache Service":dmc},["duplicate","failure"])
    pb = n.create_processor(f"{nm}__raw__publish","org.apache.nifi.kafka.processors.PublishKafka",x+1500,y,
        pub_props(topic(nm)),["success","failure"])
    for a,b,r in [(src,h,rel),(h,ids,"success"),(ids,md,"success"),(md,ky,"success"),
                  (ky,dd,"success"),(dd,pb,"non-duplicate")]:
        n.create_connection(a,"",b,"",[r])
    return dd


READER = WRITER = DMC = None


def build():
    global READER, WRITER, DMC
    ctx = ensure_ctx()
    n.REFERENCE_PARAM_CONTEXT_ID = ctx
    parent = n.pg_id()
    DMC = n.create_controller_service(f"{INSTANCE}.v2__dedupe__cache",
        "org.apache.nifi.redis.service.RedisDistributedMapCacheClientService",
        {"Redis Connection Pool":REDIS_POOL_ID,"TTL":"24 hours"})
    READER = n.create_controller_service(f"{INSTANCE}.v2__json_reader","org.apache.nifi.json.JsonTreeReader",
        {"Schema Access Strategy":"infer-schema","Starting Field Strategy":"ROOT_NODE"})
    WRITER = n.create_controller_service(f"{INSTANCE}.v2__json_writer","org.apache.nifi.json.JsonRecordSetWriter",
        {"Schema Write Strategy":"no-schema","Schema Access Strategy":"inherit-record-schema",
         "Output Grouping":"output-oneline"})
    gid = {g: child(parent, g, 0, i*400, ctx) for i,g in enumerate(GRPS)}
    made = {}
    for gi, g in enumerate(GRPS[:3]):
        n.PG_ID = gid[g]
        y = 0
        # one trigger per distinct cadence used in this group
        trigs = {}
        for e in [x for x in E if x["grp"]==g and x.get("trig")]:
            t = e["trig"]
            if t not in trigs:
                tp = n.create_processor(f"{g}__trigger_{t}","org.apache.nifi.processors.standard.GenerateFlowFile",
                    -900, len(trigs)*200, {"File Size":"0B","Batch Size":"1","Data Format":"Text",
                    "Unique FlowFiles":"false","Custom Text":f"{INSTANCE}-{t}"},[],TRIGS[t])
                rm = n.create_processor(f"{g}__run_metadata_{t}","org.apache.nifi.processors.attributes.UpdateAttribute",
                    -600, len(trigs)*200, {"Store State":"Do not store state",
                    "extraction_timestamp":"${now():format(\"yyyy-MM-dd'T'HH:mm:ss.SSSXXX\")}",
                    "ingestion_run_batch_identity":f"{INSTANCE}-"+"${now():toNumber()}-${uuid}"},[])
                n.create_connection(tp,"",rm,"",["success"])
                trigs[t] = rm
        for e in [x for x in E if x["grp"]==g]:
            nm = e["nm"]
            if e["pat"] in ("paged","paged_detail"):
                init, ex = paged(e, 0, y)
                src, rel = ex, "matched"
                if e.get("blk"):
                    fl = n.create_processor(f"{nm}__filter","org.apache.nifi.processors.standard.RouteOnAttribute",
                        1200,y,{"Routing Strategy":"Route to Property name",
                        "blocked":"${site_name:equals(#{BLOCKED_SITES})}"},["blocked"])
                    n.create_connection(ex,"",fl,"",["matched"]); src, rel = fl, "unmatched"
                if e.get("rate"):
                    rt = n.create_processor(f"{nm}__rate_limit","org.apache.nifi.processors.standard.ControlRate",
                        1200,y,{"Rate Control Criteria":"flowfile count","Maximum Rate":ASSET_RATE,
                        "Time Duration":"1 sec"},["failure"])
                    n.create_connection(src,"",rt,"",[rel]); src, rel = rt, "success"
                made[nm] = {"init":init,"extract":ex,"src":src,"rel":rel}
                if e["pat"]=="paged_detail":
                    dt = invoke(f"{nm}__detail_fetch", "#{SOURCE_API_BASE}"+e["det"], 1500, y)
                    n.create_connection(src,"",dt,"",[rel]); src, rel = dt, "Response"
                    if e.get("det_ex"):
                        dpr = {"Destination":"flowfile-attribute","Return Type":"auto-detect",
                               "Path Not Found Behavior":"ignore","Null Value Representation":"empty string"}
                        dpr.update(e["det_ex"])
                        dx = n.create_processor(f"{nm}__detail_extract",
                            "org.apache.nifi.processors.standard.EvaluateJsonPath",1650,y,dpr,["failure","unmatched"])
                        n.create_connection(src,"",dx,"",[rel])
                        made[nm]["det_extract"] = dx; src, rel = dx, "matched"
                made[nm]["dedupe"] = tail(e, 1800, y, DMC, src, rel)
                if e.get("trig"): n.create_connection(trigs[e["trig"]],"",init,"",["success"])
            elif e["pat"]=="collection":
                # endpoint returns {"links":[...],"resources":[...]} with no paging
                dt = invoke(f"{nm}__fetch", "#{SOURCE_API_BASE}"+e["det"], 300, y)
                sp = n.create_processor(f"{nm}__split","org.apache.nifi.processors.standard.SplitJson",600,y,
                    {"JsonPath Expression":"$.resources[*]","Max String Length":"20 MB",
                     "Null Value Representation":"empty string"},["failure","original"])
                pr = {"Destination":"flowfile-attribute","Return Type":"auto-detect",
                      "Path Not Found Behavior":"ignore","Null Value Representation":"empty string"}
                pr.update(e.get("ex") or {})
                ex2 = n.create_processor(f"{nm}__extract","org.apache.nifi.processors.standard.EvaluateJsonPath",
                    900,y,pr,["failure","unmatched"])
                # if this entity was previously built as a singleton, drop the stale
                # fetch -> extract edge that would bypass the split
                for c in n.get_flow(gid[g]).get("connections", []):
                    k = c["component"]
                    if k["source"]["id"] == dt and k["destination"]["id"] == ex2:
                        ce = n.nifi("GET", f"/nifi-api/connections/{c['id']}")
                        n.nifi("DELETE", f"/nifi-api/connections/{c['id']}?version={ce['revision']['version']}&clientId=fix")
                n.create_connection(dt,"",sp,"",["Response"])
                n.create_connection(sp,"",ex2,"",["split"])
                made[nm] = {"fetch":dt,"src":ex2,"rel":"matched","extract":ex2}
                made[nm]["dedupe"] = tail(e, 1200, y, DMC, ex2, "matched")
            elif e["pat"]=="singleton":
                dt = invoke(f"{nm}__fetch", "#{SOURCE_API_BASE}"+e["det"], 300, y)
                pr = {"Destination":"flowfile-attribute","Return Type":"auto-detect",
                      "Path Not Found Behavior":"ignore","Null Value Representation":"empty string"}
                pr.update(e.get("ex") or {})
                src, rel = dt, "Response"
                if e.get("ex"):
                    ex2 = n.create_processor(f"{nm}__extract","org.apache.nifi.processors.standard.EvaluateJsonPath",
                        600,y,pr,["failure","unmatched"])
                    n.create_connection(dt,"",ex2,"",["Response"]); src, rel = ex2, "matched"
                made[nm] = {"fetch":dt,"src":src,"rel":rel,"extract":src}
                made[nm]["dedupe"] = tail(e, 900, y, DMC, src, rel)
            elif e["pat"]=="gated":
                ip = port(gid[g], e["port"], "INPUT_PORT", -600, y)
                # only gate on records that actually carry the id; otherwise every
                # id-less record would collapse onto one constant gate key
                gf = n.create_processor(f"{nm}__gate_filter","org.apache.nifi.processors.standard.RouteOnAttribute",
                    -300,y,{"Routing Strategy":"Route to Property name",
                    "has_id":f"${{{e['gate']}:isEmpty():not()}}"},["unmatched"])
                gk = n.create_processor(f"{nm}__gate_key","org.apache.nifi.processors.attributes.UpdateAttribute",
                    0,y,{"Store State":"Do not store state",
                    "dedupe.key":f"rapid7:{INSTANCE}:{nm}_gate:${{{e['gate']}}}"},[])
                n.create_connection(gf,"",gk,"",["has_id"])
                gt = n.create_processor(f"{nm}__gate","org.apache.nifi.processors.standard.DetectDuplicate",
                    300,y,{"Cache Entry Identifier":"${dedupe.key}","Cache The Entry Identifier":"true",
                    "Age Off Duration":"24 hours","Distributed Cache Service":DMC},["duplicate","failure"])
                dt = invoke(f"{nm}__detail_fetch", "#{SOURCE_API_BASE}"+e["det"], 600, y)
                link(gid[g], ip, gid[g], "INPUT_PORT", gf, gid[g], "PROCESSOR", [""])
                n.create_connection(gk,"",gt,"",["success"])
                n.create_connection(gt,"",dt,"",["non-duplicate"])
                made[nm] = {"in_port":ip,"dedupe":tail(e, 900, y, DMC, dt, "Response")}
            y += 320
        # roots inside the same group
        for e in [x for x in E if x["grp"]==g and x.get("root")]:
            pn, rl = e["root"]
            p = made[pn]
            s_id = p["src"] if rl in ("filtered","rated") else p["extract"]
            s_rel = p["rel"] if rl in ("filtered","rated") else "matched"
            tgt = made[e["nm"]].get("init") or made[e["nm"]].get("fetch")
            n.create_connection(s_id,"",tgt,"",[s_rel])
        # explicit pass: wire every scheduled entity to its cadence trigger, whatever its pattern
        for e in [x for x in E if x["grp"]==g and x.get("trig")]:
            tgt = made[e["nm"]].get("init") or made[e["nm"]].get("fetch")
            n.create_connection(trigs[e["trig"]],"",tgt,"",["success"])
    # cross-PG id-gate feeds
    for i, (src_ent, handle, pname, tgrp) in enumerate(FEEDS):
        sg = BY[src_ent]["grp"]
        n.PG_ID = gid[sg]
        op = port(gid[sg], pname, "OUTPUT_PORT", 2600, i*200)
        s_id = made[src_ent].get(handle)
        if not s_id:
            print(f"  SKIP feed {pname}: {src_ent} has no {handle}", file=sys.stderr); continue
        link(gid[sg], s_id, gid[sg], "PROCESSOR", op, gid[sg], "OUTPUT_PORT", ["matched"])
        link(parent, op, gid[sg], "OUTPUT_PORT", made_port(gid[tgrp], pname), gid[tgrp], "INPUT_PORT", [""])
    n.PG_ID = parent
    stop_all_deep(parent)
    return inspect()


def made_port(pg, name):
    for p in n.get_flow(pg).get("inputPorts", []):
        if p["component"]["name"] == name: return p["id"]
    raise RuntimeError(f"input port {name} missing in {pg}")


def groups():
    parent = n.pg_id()
    return {g["component"]["name"]: g["id"] for g in n.get_flow(parent).get("processGroups", [])}


def set_port_state(pid, kind, state):
    seg = "input-ports" if kind == "input" else "output-ports"
    e = n.nifi("GET", f"/nifi-api/{seg}/{pid}")
    if e["component"].get("state") == state: return
    n.nifi("PUT", f"/nifi-api/{seg}/{pid}/run-status",
        {"revision":{"clientId":n.CLIENT_ID,"version":e["revision"]["version"]},"state":state})


def all_ports(gid):
    f = n.get_flow(gid)
    return [(p["id"], "input") for p in f.get("inputPorts", [])] + \
           [(p["id"], "output") for p in f.get("outputPorts", [])]


def stop_all_deep(parent=None):
    parent = parent or n.pg_id()
    gs = list(groups().values())
    for gid in gs + [parent]:
        n.PG_ID = gid
        for p in n.processors_by_name().values():
            try: n.stop_processor(p["id"])
            except Exception: pass
    for gid in gs:
        for pid, kind in all_ports(gid):
            try: set_port_state(pid, kind, "STOPPED")
            except Exception: pass
    n.PG_ID = parent


def inspect():
    parent = n.pg_id()
    out = {"parent": parent, "groups": {}, "groovy": []}
    for g, gid in groups().items():
        n.PG_ID = gid
        ps = n.processors_by_name()
        out["groups"][g] = {"processors": len(ps),
            "invalid": {k: v["component"].get("validationErrors") for k,v in ps.items()
                        if v["component"].get("validationStatus") not in ("VALID", None)},
            "states": {}}
        for v in ps.values():
            s = v["component"].get("state"); out["groups"][g]["states"][s] = out["groups"][g]["states"].get(s,0)+1
            if v["component"]["type"].endswith("ExecuteGroovyScript"): out["groovy"].append(v["component"]["name"])
    n.PG_ID = parent
    return out


def find_proc(name):
    for gid in groups().values():
        n.PG_ID = gid
        ps = n.processors_by_name()
        if name in ps: return gid, ps[name]["id"]
    return None, None


def infer_register():
    os.makedirs("generated_schemas", exist_ok=True)
    out, lim = {}, int(os.environ.get("SCHEMA_SAMPLE_LIMIT", "200"))
    for nm in NAMES:
        try: vals = n.fetch_topic_values(topic(nm), lim)
        except Exception as ex: out[nm] = {"status":"no_topic","err":str(ex)[:90]}; continue
        samples = []
        for v in vals:
            try:
                o = json.loads(v)
                if isinstance(o, dict): samples.append(n.normalize_json(o, 0))
            except Exception: pass
        if not samples: out[nm] = {"status":"no_samples"}; continue
        sch = n.schema_from_samples(samples, f"{INSTANCE}_{nm}_raw_avro", f"bronze.{INSTANCE}")
        # UpdateRecord emits EL output as a JSON string; pin the epoch-millis fields to long
        # so Iceberg gets bigint rather than varchar.
        for f in sch["fields"]:
            if f["name"] == "ingest_ts": f["type"] = ["null", "long"]
        p = os.path.join("generated_schemas", f"{subject(nm)}.json")
        json.dump(sch, open(p, "w", encoding="utf-8"), indent=2, ensure_ascii=False)
        reg = n.register_schema(subject(nm), sch)
        out[nm] = {"status":"registered","fields":len(sch["fields"]),"samples":len(samples),
                   "id":reg.get("id"),"version":reg.get("version"),
                   "ingest_ts": next((f["type"] for f in sch["fields"] if f["name"]=="ingest_ts"), None)}
    return out


def _rw(nm, parent):
    n.PG_ID = parent
    r = n.create_controller_service(f"{INSTANCE}.{nm}__avro_reader","org.apache.nifi.json.JsonTreeReader",
        {"Schema Access Strategy":"schema-name","Schema Registry":n.SCHEMA_REGISTRY_SERVICE_ID,
         "Schema Name":subject(nm),"Starting Field Strategy":"ROOT_NODE",
         "Schema Application Strategy":"SELECTED_PART"})
    w = n.create_controller_service(f"{INSTANCE}.{nm}__avro_writer","org.apache.nifi.avro.AvroRecordSetWriter",
        {"Schema Write Strategy":"schema-reference-writer","Schema Reference Writer":n.SCHEMA_REF_WRITER_SERVICE_ID,
         "Schema Access Strategy":"schema-name","Schema Registry":n.SCHEMA_REGISTRY_SERVICE_ID,
         "Schema Name":subject(nm)})
    return r, w


def add_avro():
    parent = n.pg_id(); stop_all_deep(parent); out = {}
    for i, nm in enumerate(NAMES):
        if not n.schema_subject_exists(subject(nm)): out[nm] = {"status":"no_schema"}; continue
        r, w = _rw(nm, parent)
        gid, dd = find_proc(f"{nm}__dedupe")
        if not dd: out[nm] = {"status":"no_dedupe"}; continue
        n.PG_ID = gid
        pb = n.create_processor(f"{nm}__avro__publish","org.apache.nifi.kafka.processors.PublishKafka",
            3600, i*320, pub_props(f"{topic(nm)}.avro", True, r, w), ["success","failure"])
        n.create_connection(dd,"",pb,"",["non-duplicate"])
        out[nm] = {"status":"added"}
    n.PG_ID = parent; return out


def build_replay():
    parent = n.pg_id(); stop_all_deep(parent)
    rg = groups()["90_replay"]; out = {}
    for i, nm in enumerate(NAMES):
        if not n.schema_subject_exists(subject(nm)): out[nm] = {"status":"no_schema"}; continue
        r, w = _rw(nm, parent)
        n.PG_ID = rg
        con = n.create_processor(f"{nm}__replay__consume","org.apache.nifi.kafka.processors.ConsumeKafka",
            0, i*220, {"Kafka Connection Service":n.KAFKA_SERVICE_ID,
            "Group ID":f"replay-{INSTANCE}-{nm}-v2","Topics":topic(nm),"Topic Format":"names",
            "auto.offset.reset":"earliest","Processing Strategy":"FLOW_FILE","Output Strategy":"USE_VALUE",
            "Commit Offsets":"true","Header Name Pattern":n.STANDARD_HEADER_PATTERN,
            "Key Attribute Encoding":"utf-8"}, [])
        # backfill: historical raw messages predate ingest_ts
        bf = n.create_processor(f"{nm}__replay__backfill","org.apache.nifi.processors.standard.UpdateRecord",
            300, i*220, {"Record Reader":READER_G,"Record Writer":WRITER_G,
            "Replacement Value Strategy":"literal-value",
            "/ingest_ts":"${extraction_timestamp:toDate(\"yyyy-MM-dd'T'HH:mm:ss.SSSXXX\"):toNumber()}"},["failure"])
        pb = n.create_processor(f"{nm}__replay__publish","org.apache.nifi.kafka.processors.PublishKafka",
            600, i*220, pub_props(f"{topic(nm)}.avro", True, r, w), ["success","failure"])
        n.create_connection(con,"",bf,"",["success"])
        n.create_connection(bf,"",pb,"",["success"])
        out[nm] = {"status":"added"}
    n.PG_ID = parent; return out


READER_G = WRITER_G = None


def _load_services():
    global READER_G, WRITER_G
    parent = n.pg_id()
    d = n.nifi("GET", f"/nifi-api/flow/process-groups/{parent}/controller-services")
    for s in d.get("controllerServices", []):
        nm = s["component"]["name"]
        if nm == f"{INSTANCE}.v2__json_reader": READER_G = s["id"]
        if nm == f"{INSTANCE}.v2__json_writer": WRITER_G = s["id"]


def start_where(pred, with_ports=False):
    started, gs = [], groups()
    parent = n.pg_id()
    for g, gid in gs.items():
        n.PG_ID = gid
        for nm, p in n.processors_by_name().items():
            if not pred(g, nm): continue
            e = n.nifi("GET", f"/nifi-api/processors/{p['id']}")
            if e["component"].get("validationStatus") == "VALID":
                n.set_processor_state(p["id"], "RUNNING"); started.append(nm)
    if with_ports:
        # ports carry the id-gate feeds across PG boundaries; STOPPED ports silently
        # strand every gated entity
        for g, gid in gs.items():
            for pid, kind in all_ports(gid):
                try: set_port_state(pid, kind, "RUNNING"); started.append(f"{g}:port")
                except Exception: pass
    n.PG_ID = parent
    return started


def start_live():
    return start_where(lambda g, nm: g != "90_replay" and "__trigger_" not in nm, with_ports=True)


def start_replay():
    return start_where(lambda g, nm: g == "90_replay")


def connector_cfg(nm):
    t = f"{topic(nm)}.avro"; name = f"{t}__iceberg"
    return name, {"connector.class":"org.apache.iceberg.connect.IcebergSinkConnector","tasks.max":"1",
      "topics":t,"iceberg.tables":f"{INSTANCE}.{nm}","iceberg.tables.auto-create-enabled":"true",
      "iceberg.tables.evolve-schema-enabled":"true","iceberg.tables.schema-force-optional":"true",
      "iceberg.control.topic":"control-iceberg",
      "iceberg.control.group-id-prefix":f"cg-ice-{INSTANCE.replace('_','-')}-{nm.replace('_','-')}",
      "iceberg.control.commit.interval-ms":"60000","iceberg.catalog":"polaris","iceberg.catalog.type":"rest",
      "iceberg.catalog.uri":os.environ.get("POLARIS_URI","https://polaris.datapasc.com/api/catalog"),
      "iceberg.catalog.warehouse":"bronze","iceberg.catalog.rest.auth.type":"oauth2",
      "iceberg.catalog.credential":os.environ["POLARIS_CREDENTIAL"],
      "iceberg.catalog.scope":"PRINCIPAL_ROLE:ALL",
      "iceberg.catalog.oauth2-server-uri":os.environ.get("POLARIS_TOKEN_URI","https://polaris.datapasc.com/api/catalog/v1/oauth/tokens"),
      "iceberg.catalog.token-refresh-enabled":"true",
      "iceberg.catalog.io-impl":"org.apache.iceberg.aws.s3.S3FileIO",
      "iceberg.catalog.s3.endpoint":os.environ.get("OZONE_S3_ENDPOINT","https://ozones3g.datapasc.com"),
      "iceberg.catalog.s3.access-key-id":os.environ["OZONE_S3_ACCESS_KEY"],
      "iceberg.catalog.s3.secret-access-key":os.environ["OZONE_S3_SECRET_KEY"],
      "iceberg.catalog.s3.path-style-access":"true","iceberg.catalog.s3.region":"us-east-1",
      "iceberg.catalog.client.region":"us-east-1",
      "value.converter":"io.apicurio.registry.utils.converter.AvroConverter",
      "value.converter.schemas.enable":"true",
      "value.converter.apicurio.registry.url":"https://apicurio.datapasc.com/apis/registry/v3",
      "value.converter.apicurio.registry.as-confluent":"true",
      "value.converter.apicurio.registry.use-id":"contentId",
      "value.converter.apicurio.registry.auto-register":"false",
      "value.converter.apicurio.registry.find-latest":"true",
      "key.converter":"org.apache.kafka.connect.storage.StringConverter",
      "consumer.override.auto.offset.reset":"earliest","errors.tolerance":"none",
      "errors.log.enable":"true","errors.log.include.messages":"true",
      "errors.deadletterqueue.topic.name":f"dlq.{t}.iceberg",
      "errors.deadletterqueue.context.headers.enable":"true",
      "errors.deadletterqueue.topic.replication.factor":"1"}


def upsert_connectors():
    out = {}
    for nm in NAMES:
        if not n.schema_subject_exists(subject(nm)): out[nm] = "no_schema"; continue
        name, cfg = connector_cfg(nm)
        r = requests.put(f"{KC}/connectors/{urllib.parse.quote(name,safe='')}/config", json=cfg,
                         verify=False, timeout=40, headers={"User-Agent":"Mozilla/5.0"})
        out[nm] = r.status_code
    return out


def main():
    requests.packages.urllib3.disable_warnings()
    cmd = sys.argv[1] if len(sys.argv) > 1 else "inspect"
    if cmd == "build": print(json.dumps(build(), indent=2, default=str))
    elif cmd == "infer-register": print(json.dumps(infer_register(), indent=2, default=str))
    elif cmd == "add-avro": print(json.dumps(add_avro(), indent=2, default=str))
    elif cmd == "build-replay": _load_services(); print(json.dumps(build_replay(), indent=2, default=str))
    elif cmd == "start-live": print(json.dumps({"started": len(start_live())}))
    elif cmd == "start-replay": print(json.dumps({"started": len(start_replay())}))
    elif cmd == "connectors": print(json.dumps(upsert_connectors(), indent=2, default=str))
    elif cmd == "stop": stop_all_deep(); print(json.dumps({"stopped": True}))
    else: print(json.dumps(inspect(), indent=2, default=str))


if __name__ == "__main__":
    main()
