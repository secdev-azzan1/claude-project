import json
import os
import sys
import time
import urllib.parse
from collections import OrderedDict

import requests

import build_fortisiem_maximum_useful as n


SOURCE_INSTANCE = os.environ.get("RAPID7_SOURCE_INSTANCE", "rapid7_securado")
PG_NAME = os.environ.get("RAPID7_MAX_PG_NAME", f"{SOURCE_INSTANCE}.maximum_useful")
PARENT_PG_ID = os.environ.get("RAPID7_PARENT_PG_ID", "0a00e822-01a0-1000-68b7-f28e69779c95")
PARAM_CONTEXT_ID = os.environ.get("RAPID7_PARAM_CONTEXT_ID", "25970e3c-6214-3451-e45c-a19352a6e268")
SOURCE_API_BASE = os.environ.get("RAPID7_SOURCE_API_BASE", "#{SOURCE_API_BASE}")
HTTP_USERNAME = os.environ.get("RAPID7_HTTP_USERNAME", "#{HTTP_USERNAME}")
HTTP_PASSWORD = os.environ.get("RAPID7_HTTP_PASSWORD", "#{HTTP_PASSWORD}")
# 0 = uncapped. Set the matching RAPID7_* env var to a positive number only for bounded test runs.
PAGE_SIZE = os.environ.get("RAPID7_PAGE_SIZE", "500")
MAX_SITES = os.environ.get("RAPID7_MAX_SITES", "0")
MAX_ASSETS = os.environ.get("RAPID7_MAX_ASSETS", "0")
MAX_ASSETS_PER_SITE = os.environ.get("RAPID7_MAX_ASSETS_PER_SITE", "0")
MAX_CHILDREN_PER_ASSET = os.environ.get("RAPID7_MAX_CHILDREN_PER_ASSET", "0")
REQUEST_DELAY_MS = os.environ.get("RAPID7_REQUEST_DELAY_MS", "700")

KAFKA_CONNECT_BASE = os.environ.get("KAFKA_CONNECT_BASE", "https://kafkaconnect.datapasc.com").rstrip("/")

n.PG_NAME = PG_NAME
n.PARENT_PG_ID = PARENT_PG_ID
n.REFERENCE_PARAM_CONTEXT_ID = PARAM_CONTEXT_ID
n.CLIENT_ID = "codex-rapid7-securado-maximum"
n.PG_ID = None


STANDARD_VALUE_FIELDS = n.STANDARD_VALUE_FIELDS

ENTITIES = [
    {"entity": "site", "topic": f"bronze.{SOURCE_INSTANCE}.site__raw", "record": f"{SOURCE_INSTANCE}_site_raw_avro"},
    {"entity": "asset", "topic": f"bronze.{SOURCE_INSTANCE}.asset__raw", "record": f"{SOURCE_INSTANCE}_asset_raw_avro"},
    {"entity": "asset_service", "topic": f"bronze.{SOURCE_INSTANCE}.asset_service__raw", "record": f"{SOURCE_INSTANCE}_asset_service_raw_avro"},
    {"entity": "asset_software", "topic": f"bronze.{SOURCE_INSTANCE}.asset_software__raw", "record": f"{SOURCE_INSTANCE}_asset_software_raw_avro"},
    {"entity": "asset_vulnerability", "topic": f"bronze.{SOURCE_INSTANCE}.asset_vulnerability__raw", "record": f"{SOURCE_INSTANCE}_asset_vulnerability_raw_avro"},
]


EXTRACT_SCRIPT = r'''
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import java.security.MessageDigest
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.time.Instant
import javax.net.ssl.HostnameVerifier
import javax.net.ssl.HttpsURLConnection
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManager
import javax.net.ssl.X509TrustManager
import org.apache.nifi.processor.io.OutputStreamCallback

def input = session.get()
if (!input) return

def prop = { name -> context.getProperty(name).evaluateAttributeExpressions(input).getValue() }
def base = (prop('SOURCE_API_BASE') ?: '').replaceAll('/+$', '')
def user = prop('HTTP_USERNAME')
def pass = prop('HTTP_PASSWORD')
def sourceInstance = prop('SOURCE_INSTANCE') ?: 'rapid7_securado'
def pageSize = (prop('PAGE_SIZE') ?: '100') as int
def maxSites = (prop('MAX_SITES') ?: '25') as int
def maxAssets = (prop('MAX_ASSETS') ?: '10') as int
def maxAssetsPerSite = (prop('MAX_ASSETS_PER_SITE') ?: '0') as int
def maxChildrenPerAsset = (prop('MAX_CHILDREN_PER_ASSET') ?: '10') as int
def requestDelayMs = (prop('REQUEST_DELAY_MS') ?: '700') as int
def extractionTs = Instant.now().toString()
def runId = "rapid7-securado-" + System.currentTimeMillis() + "-" + input.getAttribute('uuid')
def auth = "Basic " + "${user}:${pass}".bytes.encodeBase64().toString()
def slurper = new JsonSlurper()
def emitted = 0
def assetCount = 0
def siteCount = 0

if (base?.toLowerCase()?.startsWith('https://')) {
    def trustAll = [
        getAcceptedIssuers: { -> new X509Certificate[0] },
        checkClientTrusted: { X509Certificate[] certs, String authType -> },
        checkServerTrusted: { X509Certificate[] certs, String authType -> }
    ] as X509TrustManager
    def sc = SSLContext.getInstance('TLS')
    sc.init(null, [trustAll] as TrustManager[], new SecureRandom())
    HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory())
    HttpsURLConnection.setDefaultHostnameVerifier({ hostname, session -> true } as HostnameVerifier)
}

def topicFor = { entity -> "bronze.${sourceInstance}.${entity}__raw" }
def subjectFor = { entity -> topicFor(entity) + ".avro-value" }
def avroTopicFor = { entity -> topicFor(entity) + ".avro" }

def canonical
canonical = { obj ->
    if (obj instanceof Map) {
        def out = new TreeMap()
        obj.each { k, v ->
            if ([
                'source_platform','customer_tenant_organization','source_object_type','source_object_id',
                'extraction_timestamp','source_event_update_timestamp','api_endpoint_export_query_identity',
                'cursor_window','payload_hash_fingerprint','ingestion_run_batch_identity'
            ].contains(k.toString())) return
            out[k.toString()] = canonical(v)
        }
        return out
    }
    if (obj instanceof List) return obj.collect { canonical(it) }
    return obj
}
def sha256 = { text -> MessageDigest.getInstance('SHA-256').digest(text.getBytes('UTF-8')).encodeHex().toString() }

def getJson = { String path ->
    Thread.sleep(requestDelayMs)
    def url = new URL(base + path)
    def conn = url.openConnection()
    conn.setConnectTimeout(15000)
    conn.setReadTimeout(45000)
    conn.setRequestProperty('Authorization', auth)
    conn.setRequestProperty('Accept', 'application/json')
    def code = conn.responseCode
    if (code < 200 || code > 299) {
        def err = conn.errorStream ? conn.errorStream.getText('UTF-8') : ''
        throw new RuntimeException("GET ${path} failed HTTP ${code}: ${err}")
    }
    return slurper.parse(conn.inputStream)
}

def tryJson = { String path ->
    try { return getJson(path) } catch (Exception e) {
        log.warn("Rapid7 optional GET ${path} skipped: ${e.message}")
        return null
    }
}

def paged = { String pathBase, int maxItems, Closure eachResource ->
    int page = 0
    int seen = 0
    while (true) {
        def sep = pathBase.contains('?') ? '&' : '?'
        def path = "${pathBase}${sep}page=${page}&size=${pageSize}"
        def data = getJson(path)
        def resources = data.resources ?: []
        for (r in resources) {
            if (maxItems > 0 && seen >= maxItems) return
            eachResource(r, page)
            seen++
        }
        def totalPages = data.page?.totalPages
        if (maxItems > 0 && seen >= maxItems) return
        if (totalPages == null) {
            if (resources.size() < pageSize) return
        } else if (page + 1 >= (totalPages as int)) {
            return
        }
        page++
    }
}

def emitRecord = { String entity, String objectId, String apiIdentity, String cursorWindow, Map payload, Map attrs = [:] ->
    def hash = sha256(JsonOutput.toJson(canonical(payload)))
    def rec = new LinkedHashMap()
    rec.source_platform = 'rapid7'
    rec.customer_tenant_organization = sourceInstance
    rec.source_object_type = entity
    rec.source_object_id = objectId
    rec.extraction_timestamp = extractionTs
    rec.source_event_update_timestamp = payload.updated ?: payload.modified ?: payload.lastSeen ?: payload.lastScanTime ?: ''
    rec.api_endpoint_export_query_identity = apiIdentity
    rec.cursor_window = cursorWindow ?: ''
    rec.payload_hash_fingerprint = hash
    rec.ingestion_run_batch_identity = runId
    payload.each { k, v -> if (!rec.containsKey(k.toString())) rec[k.toString()] = v }

    def out = session.create(input)
    attrs.each { k, v -> if (v != null) out = session.putAttribute(out, k.toString(), v.toString()) }
    [
        source_platform:'rapid7',
        customer_tenant_organization:sourceInstance,
        source_object_type:entity,
        source_object_id:objectId,
        extraction_timestamp:extractionTs,
        source_event_update_timestamp:rec.source_event_update_timestamp ?: '',
        api_endpoint_export_query_identity:apiIdentity,
        cursor_window:cursorWindow ?: '',
        payload_hash_fingerprint:hash,
        ingestion_run_batch_identity:runId,
        entity:entity,
        object_id:objectId,
        kafka_topic:topicFor(entity),
        avro_topic:avroTopicFor(entity),
        avro_subject:subjectFor(entity),
        'dedupe.key':"${sourceInstance}:${entity}:${objectId}:${hash}"
    ].each { k, v -> out = session.putAttribute(out, k, v == null ? '' : v.toString()) }
    out = session.write(out, { os -> os.write(JsonOutput.toJson(rec).getBytes('UTF-8')) } as OutputStreamCallback)
    session.transfer(out, REL_SUCCESS)
    emitted++
}

try {
    def sites = []
    paged('/sites', maxSites, { site, page ->
        siteCount++
        sites << site
        def sid = site.id
        def detail = sid == null ? site : (tryJson("/sites/${sid}") ?: site)
        if (detail instanceof Map) {
            detail.site_id = sid
            detail.api_path = sid == null ? '/sites' : "/sites/${sid}"
            emitRecord('site', sid == null ? "site_${siteCount}" : sid.toString(), detail.api_path, "page=${page}&size=${pageSize}", detail as Map, [site_id:sid, site_name:site.name])
        }
    })

    for (site in sites) {
        if (maxAssets > 0 && assetCount >= maxAssets) break
        def sid = site.id
        if (sid == null) continue
        def siteName = site.name
        def siteAssetCount = 0
        def perSiteLimit = maxAssetsPerSite > 0 ? maxAssetsPerSite : (maxAssets > 0 ? maxAssets - assetCount : 0)
        paged("/sites/${sid}/assets", perSiteLimit, { assetRef, page ->
            if (maxAssets > 0 && assetCount >= maxAssets) return
            if (maxAssetsPerSite > 0 && siteAssetCount >= maxAssetsPerSite) return
            assetCount++
            siteAssetCount++
            def aid = assetRef.id
            if (aid == null) return
            def assetDetail = tryJson("/assets/${aid}") ?: assetRef
            if (assetDetail instanceof Map) {
                assetDetail.site_id = sid
                assetDetail.site_name = siteName
                assetDetail.asset_id = aid
                assetDetail.api_path = "/assets/${aid}"
                emitRecord('asset', aid.toString(), "/assets/${aid}", "site=${sid};page=${page}&size=${pageSize}", assetDetail as Map, [site_id:sid, site_name:siteName, asset_id:aid])
            }

            int swCount = 0
            def swPage = tryJson("/assets/${aid}/software?page=0&size=${pageSize}")
            def swResources = swPage?.resources ?: []
            if (!swResources && assetDetail instanceof Map && assetDetail.software instanceof List) swResources = assetDetail.software
            for (sw in swResources) {
                if (maxChildrenPerAsset > 0 && swCount >= maxChildrenPerAsset) break
                def rec = new LinkedHashMap(sw as Map)
                rec.site_id = sid; rec.site_name = siteName; rec.asset_id = aid; rec.api_path = "/assets/${aid}/software"
                def oid = "${aid}_" + sha256(JsonOutput.toJson(canonical(rec))).take(16)
                emitRecord('asset_software', oid, "/assets/${aid}/software", "asset=${aid};limit=${maxChildrenPerAsset}", rec, [site_id:sid, site_name:siteName, asset_id:aid])
                swCount++
            }

            int svcCount = 0
            paged("/assets/${aid}/services", maxChildrenPerAsset, { svc, svcPage ->
                svcCount++
                def protocol = svc.protocol
                def port = svc.port
                def detail = (protocol != null && port != null) ? (tryJson("/assets/${aid}/services/${protocol}/${port}") ?: svc) : svc
                def rec = new LinkedHashMap(detail as Map)
                rec.site_id = sid; rec.site_name = siteName; rec.asset_id = aid; rec.protocol = protocol; rec.port = port
                rec.api_path = protocol != null && port != null ? "/assets/${aid}/services/${protocol}/${port}" : "/assets/${aid}/services"
                def oid = protocol != null && port != null ? "${aid}_${protocol}_${port}" : "${aid}_service_${svcCount}"
                emitRecord('asset_service', oid, rec.api_path, "asset=${aid};page=${svcPage}&size=${pageSize}", rec, [site_id:sid, site_name:siteName, asset_id:aid, protocol:protocol, port:port])
            })

            int vulnCount = 0
            paged("/assets/${aid}/vulnerabilities", maxChildrenPerAsset, { vf, vulnPage ->
                vulnCount++
                def vid = vf.id ?: vf.vulnerabilityId
                def detail = vid != null ? (tryJson("/assets/${aid}/vulnerabilities/${vid}") ?: vf) : vf
                def rec = new LinkedHashMap(detail as Map)
                rec.site_id = sid; rec.site_name = siteName; rec.asset_id = aid; rec.vulnerability_id = vid
                rec.api_path = vid != null ? "/assets/${aid}/vulnerabilities/${vid}" : "/assets/${aid}/vulnerabilities"
                def oid = vid != null ? "${aid}_${vid}" : "${aid}_vuln_${vulnCount}"
                emitRecord('asset_vulnerability', oid, rec.api_path, "asset=${aid};page=${vulnPage}&size=${pageSize}", rec, [site_id:sid, site_name:siteName, asset_id:aid, vulnerability_id:vid])
            })
        })
    }

    input = session.putAttribute(input, 'rapid7.emitted', emitted.toString())
    session.remove(input)
} catch (Exception e) {
    log.error("Rapid7 selected extraction failed: " + e.message, e)
    input = session.putAttribute(input, 'rapid7.error', e.message ?: e.toString())
    session.transfer(input, REL_FAILURE)
}
'''


def processors():
    return n.processors_by_name()


def delete_connection(conn_id):
    ent = n.nifi("GET", f"/nifi-api/connections/{conn_id}")
    n.nifi("DELETE", f"/nifi-api/connections/{conn_id}?version={ent['revision']['version']}&clientId={urllib.parse.quote(n.CLIENT_ID)}")


def delete_processor(proc_id):
    ent = n.nifi("GET", f"/nifi-api/processors/{proc_id}")
    if ent["component"].get("state") == "RUNNING":
        n.stop_processor(proc_id)
        time.sleep(1)
        ent = n.nifi("GET", f"/nifi-api/processors/{proc_id}")
    n.nifi("DELETE", f"/nifi-api/processors/{proc_id}?version={ent['revision']['version']}&clientId={urllib.parse.quote(n.CLIENT_ID)}")


def cleanup_old_convertrecord_avro():
    names = set()
    for ent in ENTITIES:
        names.add(f"{SOURCE_INSTANCE}.{ent['entity']}__avro__convert")
    for c in list(n.connections()):
        src = c["component"]["source"].get("name")
        dst = c["component"]["destination"].get("name")
        if src in names or dst in names:
            delete_connection(c["id"])
    for name, p in list(processors().items()):
        if name in names:
            delete_processor(p["id"])


def set_sensitive_dynamic(proc_id, names, properties=None):
    ent = n.nifi("GET", f"/nifi-api/processors/{proc_id}")
    comp = ent["component"]
    cfg = dict(comp.get("config") or {})
    props = dict(cfg.get("properties") or {})
    if properties:
        props.update(properties)
    cfg["properties"] = props
    cfg["sensitiveDynamicPropertyNames"] = names
    payload = {
        "revision": {"clientId": n.CLIENT_ID, "version": ent["revision"]["version"]},
        "component": {"id": proc_id, "name": comp["name"], "config": cfg},
    }
    return n.nifi("PUT", f"/nifi-api/processors/{proc_id}", payload)


def build_raw():
    dmc = n.create_controller_service(
        f"{SOURCE_INSTANCE}.maximum__dedupe__cache",
        "org.apache.nifi.redis.service.RedisDistributedMapCacheClientService",
        {"Redis Connection Pool": "b90bcbdb-d69c-3725-51d1-444dd57b9336", "TTL": "24 hours"},
    )
    trigger = n.create_processor(
        f"{SOURCE_INSTANCE}.maximum__trigger",
        "org.apache.nifi.processors.standard.GenerateFlowFile",
        -400, -100,
        {"File Size": "0B", "Batch Size": "1", "Data Format": "Text", "Unique FlowFiles": "false", "Custom Text": "rapid7-selected-run"},
        [],
        "6 hours",
    )
    extract_props = {
        "Script Body": EXTRACT_SCRIPT,
        "SOURCE_API_BASE": SOURCE_API_BASE,
        "HTTP_USERNAME": HTTP_USERNAME,
        "HTTP_PASSWORD": HTTP_PASSWORD,
        "SOURCE_INSTANCE": SOURCE_INSTANCE,
        "PAGE_SIZE": PAGE_SIZE,
        "MAX_SITES": MAX_SITES,
        "MAX_ASSETS": MAX_ASSETS,
        "MAX_ASSETS_PER_SITE": MAX_ASSETS_PER_SITE,
        "MAX_CHILDREN_PER_ASSET": MAX_CHILDREN_PER_ASSET,
        "REQUEST_DELAY_MS": REQUEST_DELAY_MS,
    }
    extract = n.create_processor(
        f"{SOURCE_INSTANCE}.maximum__extract_selected_entities",
        "org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
        0, -100,
        extract_props,
        ["failure"],
        "0 sec",
    )
    set_sensitive_dynamic(extract, ["HTTP_PASSWORD"], extract_props)
    dedupe = n.create_processor(
        f"{SOURCE_INSTANCE}.maximum__dedupe__detect",
        "org.apache.nifi.processors.standard.DetectDuplicate",
        400, -100,
        {
            "Cache Entry Identifier": "${dedupe.key}",
            "Cache The Entry Identifier": "true",
            "Age Off Duration": "24 hours",
            "Distributed Cache Service": dmc,
        },
        ["duplicate", "failure"],
        "0 sec",
    )
    raw_pub = n.create_processor(
        f"{SOURCE_INSTANCE}.maximum__raw__publish",
        "org.apache.nifi.kafka.processors.PublishKafka",
        800, -100,
        {
            "Kafka Connection Service": n.KAFKA_SERVICE_ID,
            "Topic Name": "${kafka_topic}",
            "Kafka Key": "${source_object_id}",
            "Kafka Key Attribute Encoding": "utf-8",
            "Publish Strategy": "USE_VALUE",
            "Record Metadata Strategy": "FROM_PROPERTIES",
            "acks": "all",
            "compression.type": "gzip",
            "max.request.size": "16 MB",
            "Failure Strategy": "Route to Failure",
        },
        ["success", "failure"],
        "0 sec",
    )
    n.create_connection(trigger, f"{SOURCE_INSTANCE}.maximum__trigger", extract, f"{SOURCE_INSTANCE}.maximum__extract_selected_entities", ["success"])
    n.create_connection(extract, f"{SOURCE_INSTANCE}.maximum__extract_selected_entities", dedupe, f"{SOURCE_INSTANCE}.maximum__dedupe__detect", ["success"])
    n.create_connection(dedupe, f"{SOURCE_INSTANCE}.maximum__dedupe__detect", raw_pub, f"{SOURCE_INSTANCE}.maximum__raw__publish", ["non-duplicate"])
    n.stop_all()
    return inspect()


def infer_register():
    os.makedirs("generated_schemas", exist_ok=True)
    out = {}
    for ent in ENTITIES:
        try:
            vals = n.fetch_topic_values(ent["topic"], int(os.environ.get("SCHEMA_SAMPLE_LIMIT", "100")))
        except Exception as e:
            out[ent["entity"]] = {"status": "no_topic_or_samples", "topic": ent["topic"], "error": str(e)[:300]}
            continue
        samples = []
        for val in vals:
            try:
                samples.append(n.normalize_json(json.loads(val), 0))
            except Exception:
                pass
        if not samples:
            out[ent["entity"]] = {"status": "no_samples", "topic": ent["topic"]}
            continue
        schema = n.schema_from_samples(samples, ent["record"], f"bronze.{SOURCE_INSTANCE}")
        subject = f"{ent['topic']}.avro-value"
        path = os.path.join("generated_schemas", f"{subject}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(schema, f, indent=2, ensure_ascii=False)
        reg = n.register_schema(subject, schema)
        out[ent["entity"]] = {"status": "registered", "topic": ent["topic"], "avro_topic": f"{ent['topic']}.avro", "subject": subject, "schema_file": path, "schema_id": reg.get("id"), "version": reg.get("version"), "fields": len(schema.get("fields", []))}
    return out


def add_avro():
    n.stop_all()
    cleanup_old_convertrecord_avro()
    result = {}
    reader_cache = {}
    writer_cache = {}
    source = processors().get(f"{SOURCE_INSTANCE}.maximum__dedupe__detect")
    if not source:
        raise RuntimeError("Build raw flow first")
    for idx, ent in enumerate(ENTITIES):
        subject = f"{ent['topic']}.avro-value"
        if not n.schema_subject_exists(subject):
            result[ent["entity"]] = {"status": "missing_schema", "subject": subject}
            continue
        reader = reader_cache.get(subject) or n.create_controller_service(
            f"{SOURCE_INSTANCE}.{ent['entity']}__avro_json_reader",
            "org.apache.nifi.json.JsonTreeReader",
            {"Schema Access Strategy": "schema-name", "Schema Registry": n.SCHEMA_REGISTRY_SERVICE_ID, "Schema Name": subject, "Schema Version": None, "Schema Branch": None, "Schema Text": "${avro.schema}", "Schema Reference Reader": None, "Schema Inference Cache": None, "Starting Field Strategy": "ROOT_NODE", "Starting Field Name": None, "Schema Application Strategy": "SELECTED_PART"},
        )
        writer = writer_cache.get(subject) or n.create_controller_service(
            f"{SOURCE_INSTANCE}.{ent['entity']}__avro_writer",
            "org.apache.nifi.avro.AvroRecordSetWriter",
            {"Schema Write Strategy": "schema-reference-writer", "Schema Reference Writer": n.SCHEMA_REF_WRITER_SERVICE_ID, "Schema Access Strategy": "schema-name", "Schema Registry": n.SCHEMA_REGISTRY_SERVICE_ID, "Schema Name": subject, "Schema Version": None, "Schema Branch": None, "Schema Text": "${avro.schema}", "Schema Reference Reader": None},
        )
        reader_cache[subject] = reader
        writer_cache[subject] = writer
        y = 180 + idx * 160
        route = n.create_processor(
            f"{SOURCE_INSTANCE}.{ent['entity']}__avro__route",
            "org.apache.nifi.processors.standard.RouteOnAttribute",
            800, y,
            {"Routing Strategy": "Route to Property name", ent["entity"]: f"${{source_object_type:equals('{ent['entity']}')}}"},
            ["unmatched"],
            "0 sec",
        )
        normalizer = n.create_processor(
            f"{SOURCE_INSTANCE}.{ent['entity']}__avro__normalize_json",
            "org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
            1120, y,
            {"Script Body": n.JSON_NORMALIZE_SCRIPT},
            ["failure"],
            "0 sec",
        )
        pub = n.create_processor(
            f"{SOURCE_INSTANCE}.{ent['entity']}__avro__publish",
            "org.apache.nifi.kafka.processors.PublishKafka",
            1780, y,
            n.publish_props(f"{ent['topic']}.avro", True, reader, writer),
            ["success", "failure"],
            "0 sec",
        )
        n.create_connection(source["id"], f"{SOURCE_INSTANCE}.maximum__dedupe__detect", route, f"{SOURCE_INSTANCE}.{ent['entity']}__avro__route", ["non-duplicate"])
        n.create_connection(route, f"{SOURCE_INSTANCE}.{ent['entity']}__avro__route", normalizer, f"{SOURCE_INSTANCE}.{ent['entity']}__avro__normalize_json", [ent["entity"]])
        n.create_connection(normalizer, f"{SOURCE_INSTANCE}.{ent['entity']}__avro__normalize_json", pub, f"{SOURCE_INSTANCE}.{ent['entity']}__avro__publish", ["success"])
        result[ent["entity"]] = {"status": "added", "subject": subject, "avro_topic": f"{ent['topic']}.avro"}
    n.stop_all()
    return result


def start_all_except_trigger():
    for name, p in processors().items():
        if not name.startswith(SOURCE_INSTANCE + "."):
            continue
        if name.endswith("__trigger") or "__admin_" in name or "__test_" in name:
            continue
        ent = n.nifi("GET", f"/nifi-api/processors/{p['id']}")
        if ent["component"].get("validationStatus") == "VALID":
            n.set_processor_state(p["id"], "RUNNING")


def run_once(wait_seconds=300):
    start_all_except_trigger()
    trig = processors()[f"{SOURCE_INSTANCE}.maximum__trigger"]
    n.set_processor_state(trig["id"], "RUNNING")
    time.sleep(4)
    n.stop_processor(trig["id"])
    deadline = time.time() + wait_seconds
    last = []
    while time.time() < deadline:
        last = queued_summary()
        if not last:
            break
        time.sleep(10)
    n.stop_all()
    return {"queued_remaining": last, "inspect": inspect()}


def clear_redis():
    # Reuses the Redis connection pool via a short-lived processor inside this PG.
    script = r'''
import groovy.json.JsonOutput
import org.apache.nifi.processor.io.OutputStreamCallback
def flowFile = session.get()
if (!flowFile) return
def poolId = context.getProperty('REDIS_POOL_ID').evaluateAttributeExpressions(flowFile).getValue()
def pattern = context.getProperty('KEY_PATTERN').evaluateAttributeExpressions(flowFile).getValue()
def pool = context.controllerServiceLookup.getControllerService(poolId)
def deleted = 0
def matched = 0
def conn = null
try {
    conn = pool.getConnection()
    def nativeConn = conn.getNativeConnection()
    def keys = nativeConn.keys(pattern)
    matched = keys == null ? 0 : keys.size()
    if (keys != null) keys.each { k -> nativeConn.del(k); deleted++ }
    def result = [pattern:pattern, matched:matched, deleted:deleted]
    log.warn('RAPID7_REDIS_CLEAR ' + JsonOutput.toJson(result))
    flowFile = session.write(flowFile, { os -> os.write(JsonOutput.toJson(result).getBytes('UTF-8')) } as OutputStreamCallback)
    session.transfer(flowFile, REL_SUCCESS)
} catch (Throwable t) {
    flowFile = session.putAttribute(flowFile, 'redis.clear.error', t.toString())
    session.transfer(flowFile, REL_FAILURE)
} finally { if (conn != null) conn.close() }
'''
    trig = n.create_processor(f"{SOURCE_INSTANCE}.maximum__admin_clear_redis__trigger", "org.apache.nifi.processors.standard.GenerateFlowFile", -400, 1260, {"File Size": "0B", "Batch Size": "1", "Data Format": "Text", "Unique FlowFiles": "false"}, [], "2 hours")
    proc = n.create_processor(f"{SOURCE_INSTANCE}.maximum__admin_clear_redis__run", "org.apache.nifi.processors.groovyx.ExecuteGroovyScript", 0, 1260, {"Script Body": script, "REDIS_POOL_ID": "b90bcbdb-d69c-3725-51d1-444dd57b9336", "KEY_PATTERN": f"{SOURCE_INSTANCE}:*"}, ["success", "failure"], "0 sec")
    n.create_connection(trig, f"{SOURCE_INSTANCE}.maximum__admin_clear_redis__trigger", proc, f"{SOURCE_INSTANCE}.maximum__admin_clear_redis__run", ["success"])
    n.set_processor_state(proc, "RUNNING")
    n.set_processor_state(trig, "RUNNING")
    time.sleep(3)
    n.stop_processor(trig)
    time.sleep(5)
    n.stop_processor(proc)
    ent = n.nifi("GET", f"/nifi-api/processors/{proc}")
    bulletins = []
    for item in ent.get("bulletins") or []:
        bb = item.get("bulletin") or item
        if bb and "RAPID7_REDIS_CLEAR" in (bb.get("message") or ""):
            bulletins.append({k: bb.get(k) for k in ["level", "message", "timestamp"]})
    return {"pattern": f"{SOURCE_INSTANCE}:*", "bulletins": bulletins[-5:], "queued": queued_summary()}


def inspect():
    out = {"process_group": {"id": n.pg_id(), "name": PG_NAME}, "processors": {}, "topics": []}
    for name, p in processors().items():
        if name.startswith(SOURCE_INSTANCE + "."):
            c = p["component"]
            out["processors"][name] = {"id": p["id"], "state": c.get("state"), "validation": c.get("validationStatus"), "validation_errors": c.get("validationErrors")}
    for ent in ENTITIES:
        out["topics"].append(ent["topic"])
        out["topics"].append(f"{ent['topic']}.avro")
    out["queued"] = queued_summary()
    return out


def queued_summary():
    return [
        {"id": c["id"], "source": c["component"]["source"].get("name"), "destination": c["component"]["destination"].get("name"), "queued": c["status"]["aggregateSnapshot"].get("queued"), "bytes": c["status"]["aggregateSnapshot"].get("queuedSize")}
        for c in n.connections()
        if c["status"]["aggregateSnapshot"].get("queued") not in ("0", "0 (0 bytes)")
    ]


def verify_kafka():
    result = {}
    required = set(STANDARD_VALUE_FIELDS)
    for ent in ENTITIES:
        for topic in [ent["topic"], f"{ent['topic']}.avro"]:
            try:
                vals = n.fetch_topic_values(topic, 3)
                rec = {"samples_seen": len(vals), "has_data": bool(vals)}
                if vals and topic.endswith(".avro"):
                    try:
                        obj = json.loads(vals[0]) if isinstance(vals[0], str) else vals[0]
                        rec["metadata_fields_in_value"] = len([k for k in required if isinstance(obj, dict) and k in obj])
                        rec["missing_metadata"] = sorted([k for k in required if not isinstance(obj, dict) or k not in obj])
                    except Exception as e:
                        rec["parse_error"] = str(e)[:300]
                result[topic] = rec
            except Exception as e:
                result[topic] = {"error": str(e)[:500]}
    return result


def connector_config(ent):
    table = ent["entity"]
    topic = f"{ent['topic']}.avro"
    name = f"bronze.{SOURCE_INSTANCE}.{table}__raw.avro__iceberg"
    safe_group = f"cg-iceberg-bronze-{SOURCE_INSTANCE.replace('_','-')}-{table.replace('_','-')}"
    return name, {
        "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
        "tasks.max": "1",
        "topics": topic,
        "iceberg.tables": f"{SOURCE_INSTANCE}.{table}",
        "iceberg.tables.auto-create-enabled": "true",
        "iceberg.tables.evolve-schema-enabled": "true",
        "iceberg.tables.schema-force-optional": "true",
        "iceberg.control.topic": "control-iceberg",
        "iceberg.control.group-id-prefix": safe_group,
        "iceberg.control.commit.interval-ms": "60000",
        "iceberg.catalog": "polaris",
        "iceberg.catalog.type": "rest",
        "iceberg.catalog.uri": "https://polaris.datapasc.com/api/catalog",
        "iceberg.catalog.warehouse": "bronze",
        "iceberg.catalog.rest.auth.type": "oauth2",
        "iceberg.catalog.credential": "root:s3cr3t",
        "iceberg.catalog.scope": "PRINCIPAL_ROLE:ALL",
        "iceberg.catalog.oauth2-server-uri": "https://polaris.datapasc.com/api/catalog/v1/oauth/tokens",
        "iceberg.catalog.token-refresh-enabled": "true",
        "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "iceberg.catalog.s3.endpoint": "https://ozones3g.datapasc.com",
        "iceberg.catalog.s3.access-key-id": "eltadmin",
        "iceberg.catalog.s3.secret-access-key": "OzoneS3Key123",
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
        "errors.tolerance": "all",
        "errors.log.enable": "true",
        "errors.log.include.messages": "true",
        "errors.deadletterqueue.topic.name": f"dlq.{topic}.iceberg",
        "errors.deadletterqueue.context.headers.enable": "true",
        "errors.deadletterqueue.topic.replication.factor": "1",
    }


def upsert_connectors():
    out = {}
    for ent in ENTITIES:
        subject = f"{ent['topic']}.avro-value"
        if not n.schema_subject_exists(subject):
            out[f"bronze.{SOURCE_INSTANCE}.{ent['entity']}__raw.avro__iceberg"] = {"status": "skipped_missing_schema", "subject": subject}
            continue
        name, cfg = connector_config(ent)
        url = f"{KAFKA_CONNECT_BASE}/connectors/{urllib.parse.quote(name, safe='')}/config"
        r = requests.put(url, json=cfg, verify=False, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 404:
            r = requests.post(f"{KAFKA_CONNECT_BASE}/connectors", json={"name": name, "config": cfg}, verify=False, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        out[name] = {"status": r.status_code, "body": r.text[:500]}
    return out


def main():
    requests.packages.urllib3.disable_warnings()
    cmd = sys.argv[1] if len(sys.argv) > 1 else "inspect"
    if cmd == "build-raw":
        print(json.dumps(build_raw(), indent=2))
    elif cmd == "clear-redis":
        print(json.dumps(clear_redis(), indent=2))
    elif cmd == "run-once":
        print(json.dumps(run_once(int(os.environ.get("WAIT_SECONDS", "300"))), indent=2))
    elif cmd == "infer-register":
        print(json.dumps(infer_register(), indent=2))
    elif cmd == "add-avro":
        print(json.dumps(add_avro(), indent=2))
    elif cmd == "verify-kafka":
        print(json.dumps(verify_kafka(), indent=2))
    elif cmd == "stop":
        n.stop_all()
        print(json.dumps({"stopped": True, "inspect": inspect()}, indent=2))
    elif cmd == "connectors":
        print(json.dumps(upsert_connectors(), indent=2))
    else:
        print(json.dumps(inspect(), indent=2))


if __name__ == "__main__":
    main()
