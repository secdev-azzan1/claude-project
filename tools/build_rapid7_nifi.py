import base64
import json
import os
import ssl
import subprocess
import sys
import time
import urllib.parse
import urllib.request


NIFI_BASE = os.environ.get("NIFI_BASE", "https://nifi.datapasc.com").rstrip("/")
NIFI_USER = os.environ.get("NIFI_USER")
NIFI_PASSWORD = os.environ.get("NIFI_PASSWORD")
NIFI_TOKEN = os.environ.get("NIFI_TOKEN")
RAPID7_PASSWORD = os.environ.get("RAPID7_PASSWORD", "#{HTTP_PASSWORD}")
RAPID7_SOURCE_API_BASE = os.environ.get("RAPID7_SOURCE_API_BASE", "#{SOURCE_API_BASE}")
SOURCE_INSTANCE = os.environ.get("RAPID7_SOURCE_INSTANCE", "rapid7_securado")
RAPID7_PAGE_SIZE = os.environ.get("RAPID7_PAGE_SIZE", "#{PAGE_SIZE}")
RAPID7_MAX_RECORDS = os.environ.get("RAPID7_MAX_RECORDS", "10")

ROOT_INGEST_GROUP = os.environ.get("NIFI_INGEST_GROUP_ID", "0a00e822-01a0-1000-68b7-f28e69779c95")
RAPID7_PARAM_CONTEXT_ID = os.environ.get("RAPID7_PARAM_CONTEXT_ID", "25970e3c-6214-3451-e45c-a19352a6e268")
KAFKA_SERVICE_ID = os.environ.get("NIFI_KAFKA_SERVICE_ID", "40675f79-8eaa-3193-8f8d-026c8c1ee947")
REDIS_POOL_ID = os.environ.get("NIFI_REDIS_POOL_ID", "b90bcbdb-d69c-3725-51d1-444dd57b9336")

CLIENT_ID = "codex-rapid7-max"
CTX = ssl._create_unverified_context()


def http(method, path, body=None, token=None, headers=None, timeout=60):
    url = f"{NIFI_BASE}{path}"
    cmd = ["curl.exe", "--http1.1", "-k", "-sS", "-X", method, "--connect-timeout", "15", "--max-time", str(timeout)]
    hdrs = headers.copy() if headers else {}
    if token:
        hdrs["Authorization"] = f"Bearer {token}"
    if body is not None:
        hdrs["Content-Type"] = "application/json"
        payload = json.dumps(body)
        cmd.extend(["--data-binary", "@-"])
    else:
        payload = None
    for k, v in hdrs.items():
        cmd.extend(["-H", f"{k}: {v}"])
    cmd.extend(["-w", "\nHTTP_STATUS:%{http_code}", url])
    last_error = None
    for attempt in range(1, 4):
        proc = subprocess.run(cmd, input=payload, text=True, capture_output=True, timeout=timeout + 10)
        if proc.returncode == 0:
            break
        recovered = proc.stdout.strip()
        if recovered.startswith("{") or recovered.startswith("["):
            try:
                return json.loads(recovered)
            except json.JSONDecodeError:
                pass
        last_error = (
            f"{method} {path} failed: curl exit {proc.returncode}: "
            f"stderr={proc.stderr.strip()[:1000]} stdout={proc.stdout.strip()[:1000]}"
        )
        time.sleep(attempt)
    else:
        raise RuntimeError(last_error or f"{method} {path} failed")
    if proc.returncode != 0:
        recovered = proc.stdout.strip()
        if recovered.startswith("{") or recovered.startswith("["):
            try:
                return json.loads(recovered)
            except json.JSONDecodeError:
                pass
        raise RuntimeError(
            f"{method} {path} failed: curl exit {proc.returncode}: "
            f"stderr={proc.stderr.strip()[:1000]} stdout={proc.stdout.strip()[:1000]}"
        )
    text = proc.stdout
    marker = "\nHTTP_STATUS:"
    if marker not in text:
        raise RuntimeError(f"{method} {path} failed: missing HTTP status")
    raw, status_text = text.rsplit(marker, 1)
    status = int(status_text.strip()[:3])
    if status < 200 or status > 299:
        raise RuntimeError(f"{method} {path} failed: HTTP {status}: {raw[:2000]}")
    raw = raw.strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def login():
    if NIFI_TOKEN:
        return NIFI_TOKEN
    if not NIFI_USER or not NIFI_PASSWORD:
        raise RuntimeError("Set NIFI_TOKEN or both NIFI_USER and NIFI_PASSWORD")
    form = urllib.parse.urlencode({"username": NIFI_USER, "password": NIFI_PASSWORD}).encode("utf-8")
    req = urllib.request.Request(
        f"{NIFI_BASE}/nifi-api/access/token",
        data=form,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urllib.request.urlopen(req, context=CTX, timeout=60) as resp:
        return resp.read().decode("utf-8")


def get_flow(token, pg_id):
    return http("GET", f"/nifi-api/flow/process-groups/{pg_id}", token=token)


def find_child_pg(token, parent_id, name):
    flow = get_flow(token, parent_id)["processGroupFlow"]["flow"]
    for pg in flow.get("processGroups", []):
        if pg["component"]["name"] == name:
            return pg
    return None


def create_pg(token, parent_id, name, x, y, comments="", parameter_context_id=None):
    found = find_child_pg(token, parent_id, name)
    if found:
        if parameter_context_id:
            update_pg_parameter_context(token, found["id"], parameter_context_id)
        return found["id"]
    component = {
        "name": name,
        "position": {"x": float(x), "y": float(y)},
        "comments": comments,
    }
    if parameter_context_id:
        component["parameterContext"] = {"id": parameter_context_id}
    payload = {
        "revision": {"clientId": CLIENT_ID, "version": 0},
        "component": component,
    }
    created = http("POST", f"/nifi-api/process-groups/{parent_id}/process-groups", payload, token=token)
    return created["id"]


def update_pg_parameter_context(token, pg_id, parameter_context_id):
    entity = http("GET", f"/nifi-api/process-groups/{pg_id}", token=token)
    comp = entity["component"]
    current = ((comp.get("parameterContext") or {}).get("id"))
    if current == parameter_context_id:
        return
    payload = {
        "revision": {"clientId": CLIENT_ID, "version": entity["revision"]["version"]},
        "component": {
            "id": pg_id,
            "name": comp["name"],
            "position": comp.get("position"),
            "comments": comp.get("comments", ""),
            "parameterContext": {"id": parameter_context_id},
        },
    }
    http("PUT", f"/nifi-api/process-groups/{pg_id}", payload, token=token)


def processors_by_name(token, pg_id):
    flow = get_flow(token, pg_id)["processGroupFlow"]["flow"]
    return {p["component"]["name"]: p for p in flow.get("processors", [])}


def controller_services_by_name(token, pg_id):
    data = http("GET", f"/nifi-api/flow/process-groups/{pg_id}/controller-services", token=token)
    return {s["component"]["name"]: s for s in data.get("controllerServices", [])}


def update_processor(token, proc_id, properties=None, auto_terms=None, sensitive_dynamic=None, scheduling_period=None, run_duration_millis=None):
    entity = http("GET", f"/nifi-api/processors/{proc_id}", token=token)
    comp = entity["component"]
    config = comp.get("config", {})
    merged_props = dict(config.get("properties") or {})
    if properties:
        merged_props.update(properties)
    new_config = {
        "properties": merged_props,
        "autoTerminatedRelationships": auto_terms if auto_terms is not None else config.get("autoTerminatedRelationships", []),
    }
    if scheduling_period is not None:
        new_config["schedulingPeriod"] = scheduling_period
    if run_duration_millis is not None:
        new_config["runDurationMillis"] = run_duration_millis
    if sensitive_dynamic:
        new_config["sensitiveDynamicPropertyNames"] = sensitive_dynamic
    payload = {
        "revision": {"clientId": CLIENT_ID, "version": entity["revision"]["version"]},
        "component": {
            "id": proc_id,
            "name": comp["name"],
            "config": new_config,
        },
    }
    updated = http("PUT", f"/nifi-api/processors/{proc_id}", payload, token=token)
    return updated["id"]


def create_processor(token, pg_id, name, proc_type, x, y, properties=None, auto_terms=None, scheduling_period="0 sec", sensitive_dynamic=None, run_duration_millis=0):
    existing = processors_by_name(token, pg_id).get(name)
    if existing:
        update_processor(token, existing["id"], properties, auto_terms, sensitive_dynamic, scheduling_period, run_duration_millis)
        return existing["id"]
    payload = {
        "revision": {"clientId": CLIENT_ID, "version": 0},
        "component": {
            "name": name,
            "type": proc_type,
            "position": {"x": float(x), "y": float(y)},
            "config": {
                "schedulingPeriod": scheduling_period,
                "schedulingStrategy": "TIMER_DRIVEN",
                "executionNode": "ALL",
                "penaltyDuration": "30 sec",
                "yieldDuration": "1 sec",
                "bulletinLevel": "WARN",
                "runDurationMillis": run_duration_millis,
                "concurrentlySchedulableTaskCount": 1,
                "autoTerminatedRelationships": auto_terms or [],
                "properties": properties or {},
            },
        },
    }
    if sensitive_dynamic:
        payload["component"]["config"]["sensitiveDynamicPropertyNames"] = sensitive_dynamic
    created = http("POST", f"/nifi-api/process-groups/{pg_id}/processors", payload, token=token)
    return created["id"]


def create_controller_service(token, pg_id, name, service_type, properties):
    existing = controller_services_by_name(token, pg_id).get(name)
    if existing:
        return existing["id"]
    payload = {
        "revision": {"clientId": CLIENT_ID, "version": 0},
        "component": {
            "name": name,
            "type": service_type,
            "properties": properties,
        },
    }
    created = http("POST", f"/nifi-api/process-groups/{pg_id}/controller-services", payload, token=token)
    return created["id"]


def enable_controller_service(token, service_id):
    entity = http("GET", f"/nifi-api/controller-services/{service_id}", token=token)
    if entity["component"]["state"] == "ENABLED":
        return
    payload = {
        "revision": {
            "clientId": CLIENT_ID,
            "version": entity["revision"]["version"],
        },
        "state": "ENABLED",
    }
    http("PUT", f"/nifi-api/controller-services/{service_id}/run-status", payload, token=token)


def connections(token, pg_id):
    flow = get_flow(token, pg_id)["processGroupFlow"]["flow"]
    return flow.get("connections", [])


def create_connection(token, pg_id, source_id, source_name, dest_id, dest_name, relationships):
    for c in connections(token, pg_id):
        comp = c["component"]
        if (
            comp["source"]["id"] == source_id
            and comp["destination"]["id"] == dest_id
            and sorted(comp["selectedRelationships"]) == sorted(relationships)
        ):
            return c["id"]
    payload = {
        "revision": {"clientId": CLIENT_ID, "version": 0},
        "component": {
            "parentGroupId": pg_id,
            "source": {
                "id": source_id,
                "type": "PROCESSOR",
                "groupId": pg_id,
                "name": source_name,
            },
            "destination": {
                "id": dest_id,
                "type": "PROCESSOR",
                "groupId": pg_id,
                "name": dest_name,
            },
            "selectedRelationships": relationships,
            "flowFileExpiration": "0 sec",
            "backPressureObjectThreshold": 10000,
            "backPressureDataSizeThreshold": "1 GB",
        },
    }
    created = http("POST", f"/nifi-api/process-groups/{pg_id}/connections", payload, token=token)
    return created["id"]


GROOVY_TEMPLATE = r'''
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import java.security.MessageDigest
import java.util.UUID
import org.apache.nifi.processor.io.OutputStreamCallback

def input = session.get()
if (!input) return

def prop = { name ->
    context.getProperty(name).evaluateAttributeExpressions(input).getValue()
}

def base = prop('SOURCE_API_BASE')
def user = prop('HTTP_USERNAME')
def pass = prop('HTTP_PASSWORD')
def pageSize = (prop('PAGE_SIZE') ?: '500') as int
def maxRecords = (prop('MAX_RECORDS') ?: '10') as int
def entity = prop('ENTITY')
def sourceInstance = prop('SOURCE_INSTANCE')
def sourceType = 'rapid7'

def auth = "Basic " + "${user}:${pass}".bytes.encodeBase64().toString()
def slurper = new JsonSlurper()
def emitted = 0

def canonical(obj) {
    if (obj instanceof Map) {
        def out = new TreeMap()
        obj.each { k, v ->
            if (k == 'ingest_id' || k == 'ingest_ts') return
            out[k] = canonical(v)
        }
        return out
    }
    if (obj instanceof List) return obj.collect { canonical(it) }
    return obj
}

def sha256(text) {
    MessageDigest.getInstance('SHA-256').digest(text.getBytes('UTF-8')).encodeHex().toString()
}

def getJson = { path ->
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

def paged = { pathBase, Closure eachResource ->
    int page = 0
    while (true) {
        def sep = pathBase.contains('?') ? '&' : '?'
        def data = getJson("${pathBase}${sep}page=${page}&size=${pageSize}")
        def resources = data.resources ?: []
        resources.each { r ->
            if (maxRecords > 0 && emitted >= maxRecords) return
            eachResource(r)
        }
        if (maxRecords > 0 && emitted >= maxRecords) break
        def pageInfo = data.page
        def totalPages = pageInfo?.totalPages
        if (totalPages == null) {
            if (resources.size() < pageSize) break
        } else if (page + 1 >= (totalPages as int)) {
            break
        }
        page++
    }
}

def emitRecord = { Map rec, String objectId, Map attrs ->
    def now = System.currentTimeMillis()
    rec.source_type = sourceType
    rec.source_instance = sourceInstance
    rec.entity = entity
    rec.object_id = objectId
    rec.ingest_id = UUID.randomUUID().toString()
    rec.ingest_ts = now
    def hash = sha256(JsonOutput.toJson(canonical(rec)))
    def out = session.create(input)
    attrs.each { k, v -> if (v != null) out = session.putAttribute(out, k, v.toString()) }
    out = session.putAttribute(out, 'entity', entity)
    out = session.putAttribute(out, 'object_id', objectId)
    out = session.putAttribute(out, 'dedupe.key', "${sourceInstance}:${entity}:${objectId}:${hash}")
    out = session.write(out, { os ->
        os.write(JsonOutput.toJson(rec).getBytes('UTF-8'))
    } as OutputStreamCallback)
    session.transfer(out, REL_SUCCESS)
    emitted++
}

def eachSite = { Closure c ->
    paged('/sites', { site ->
        def blockedSites = prop('BLOCKED_SITES')
        if (site.name != null && blockedSites && site.name.toString() == blockedSites) return
        c(site)
    })
}

def eachSiteAsset = { Closure c ->
    eachSite { site ->
        paged("/sites/${site.id}/assets", { assetRef ->
            c(site, assetRef)
        })
    }
}

try {
    if (entity == 'site') {
        eachSite { site ->
            def detail = getJson("/sites/${site.id}") as Map
            detail.api_path = "/sites/${site.id}"
            emitRecord(detail, "${site.id}", [site_id: site.id, site_name: site.name])
        }
    } else if (entity == 'site_asset_membership') {
        eachSiteAsset { site, assetRef ->
            def rec = new LinkedHashMap(assetRef as Map)
            rec.site_id = site.id
            rec.site_name = site.name
            rec.asset_id = assetRef.id
            rec.api_path = "/sites/${site.id}/assets"
            emitRecord(rec, "${site.id}_${assetRef.id}", [site_id: site.id, site_name: site.name, asset_id: assetRef.id])
        }
    } else if (entity == 'asset') {
        eachSiteAsset { site, assetRef ->
            def detail = getJson("/assets/${assetRef.id}") as Map
            detail.site_id = site.id
            detail.site_name = site.name
            detail.asset_id = assetRef.id
            detail.api_path = "/assets/${assetRef.id}"
            emitRecord(detail, "${site.id}_${assetRef.id}", [site_id: site.id, site_name: site.name, asset_id: assetRef.id])
        }
    } else if (entity == 'asset_service') {
        eachSiteAsset { site, assetRef ->
            paged("/assets/${assetRef.id}/services", { svc ->
                def detail = getJson("/assets/${assetRef.id}/services/${svc.protocol}/${svc.port}") as Map
                detail.site_id = site.id
                detail.site_name = site.name
                detail.asset_id = assetRef.id
                detail.protocol = svc.protocol
                detail.port = svc.port
                detail.api_path = "/assets/${assetRef.id}/services/${svc.protocol}/${svc.port}"
                emitRecord(detail, "${assetRef.id}_${svc.protocol}_${svc.port}", [site_id: site.id, site_name: site.name, asset_id: assetRef.id, protocol: svc.protocol, port: svc.port])
            })
        }
    } else if (entity == 'asset_vulnerability_finding') {
        eachSiteAsset { site, assetRef ->
            paged("/assets/${assetRef.id}/vulnerabilities", { vf ->
                def detail = getJson("/assets/${assetRef.id}/vulnerabilities/${vf.id}") as Map
                detail.site_id = site.id
                detail.site_name = site.name
                detail.asset_id = assetRef.id
                detail.vulnerability_id = vf.id
                detail.api_path = "/assets/${assetRef.id}/vulnerabilities/${vf.id}"
                emitRecord(detail, "${assetRef.id}_${vf.id}", [site_id: site.id, site_name: site.name, asset_id: assetRef.id, vulnerability_id: vf.id])
            })
        }
    } else if (entity == 'vulnerability') {
        paged('/vulnerabilities', { vuln ->
            def detail = getJson("/vulnerabilities/${vuln.id}") as Map
            detail.vulnerability_id = vuln.id
            detail.api_path = "/vulnerabilities/${vuln.id}"
            emitRecord(detail, "${vuln.id}", [vulnerability_id: vuln.id])
        })
    } else {
        throw new RuntimeException("Unsupported entity ${entity}")
    }
    session.remove(input)
} catch (Exception e) {
    log.error("Rapid7 extraction failed for ${entity}: " + e.message, e)
    input = session.putAttribute(input, 'rapid7.error', e.message)
    session.transfer(input, REL_FAILURE)
}
'''


ENTITY_NAMES = [
    "site",
    "site_asset_membership",
    "site_alert",
    "site_smtp_alert",
    "site_snmp_alert",
    "site_syslog_alert",
    "site_discovery_connection",
    "site_discovery_search_criteria",
    "site_included_target",
    "site_excluded_target",
    "site_included_asset_group",
    "site_excluded_asset_group",
    "site_organization",
    "site_scan_engine",
    "site_scan_template",
    "scan_schedule",
    "site_shared_credential",
    "site_credential",
    "site_tag",
    "site_user",
    "site_web_auth_html_form",
    "site_web_auth_http_header",
    "asset",
    "asset_service",
    "asset_service_configuration",
    "asset_service_database",
    "asset_service_user",
    "asset_service_user_group",
    "asset_web_application",
    "asset_software",
    "asset_database",
    "asset_file",
    "asset_user",
    "asset_user_group",
    "asset_tag",
    "operating_system",
    "software",
    "asset_vulnerability_finding",
    "asset_service_vulnerability",
    "asset_vulnerability_validation",
    "asset_vulnerability_solution",
    "vulnerability",
    "vulnerability_category",
    "vulnerability_category_membership",
    "vulnerability_reference",
    "vulnerability_reference_membership",
    "solution",
    "solution_prerequisite",
    "solution_supersedes",
    "solution_superseding",
    "exploit",
    "exploit_vulnerability",
    "malware_kit",
    "malware_kit_vulnerability",
    "vulnerability_check",
    "vulnerability_check_type",
    "vulnerability_check_for_vulnerability",
    "policy",
    "policy_child",
    "policy_group",
    "policy_group_child",
    "policy_group_rule",
    "policy_rule",
    "disabled_policy_rule",
    "policy_asset_result",
    "policy_group_asset_result",
    "policy_rule_asset_result",
    "policy_rule_asset_proof",
    "policy_rule_control",
    "policy_rule_rationale",
    "policy_rule_remediation",
    "policy_summary",
    "asset_policy",
    "asset_policy_child",
    "asset_policy_rule_summary",
    "asset_policy_group_child",
    "asset_policy_group_rule_assessment",
    "policy_override",
    "policy_override_expiration",
    "asset_policy_override",
    "vulnerability_exception",
    "vulnerability_exception_expiration",
    "scan",
    "site_scan",
    "scan_template",
    "scan_engine",
    "scan_engine_pool",
    "scan_engine_pool_engine",
    "scan_engine_pool_site",
    "scan_engine_scan",
    "scan_engine_site",
    "asset_group",
    "asset_group_asset",
    "asset_group_search_criteria",
    "asset_group_tag",
    "asset_group_user",
    "agent",
    "tag",
    "tag_asset",
    "tag_site",
    "tag_asset_group",
    "tag_search_criteria",
    "shared_credential",
    "discovery_connection",
    "sonar_query",
    "sonar_query_asset",
    "user",
    "user_asset_group",
    "user_privilege",
    "user_site",
    "role",
    "role_user",
    "privilege",
    "privilege_user",
    "authentication_source",
    "authentication_source_user",
    "report",
    "report_history",
    "report_template",
    "report_format",
    "administration_info",
    "administration_license",
    "administration_logs",
    "administration_properties",
    "administration_settings",
]


ENTITY_TOPICS = [(entity, f"bronze.{SOURCE_INSTANCE}.{entity}__raw") for entity in ENTITY_NAMES]


def build_entity_group(token, parent_id, dmc_service_id, entity, topic, x, y):
    pg_name = f"{SOURCE_INSTANCE}.{entity}"
    pg_id = create_pg(token, parent_id, pg_name, x, y, f"Rapid7 maximum pipeline entity: {entity}", RAPID7_PARAM_CONTEXT_ID)

    trigger = create_processor(
        token,
        pg_id,
        f"{pg_name}__trigger",
        "org.apache.nifi.processors.standard.GenerateFlowFile",
        0,
        0,
        {
            "File Size": "0B",
            "Batch Size": "1",
            "Data Format": "Text",
            "Unique FlowFiles": "false",
        },
        [],
        "1 hour",
    )
    extract = create_processor(
        token,
        pg_id,
        f"{pg_name}__extract__fetch_emit_json",
        "org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
        0,
        220,
        {
            "Script Body": GROOVY_TEMPLATE,
            "SOURCE_API_BASE": RAPID7_SOURCE_API_BASE,
            "HTTP_USERNAME": "#{HTTP_USERNAME}",
            "HTTP_PASSWORD": RAPID7_PASSWORD,
            "PAGE_SIZE": RAPID7_PAGE_SIZE,
            "BLOCKED_SITES": "#{BLOCKED_SITES}",
            "SOURCE_INSTANCE": SOURCE_INSTANCE,
            "ENTITY": entity,
            "MAX_RECORDS": RAPID7_MAX_RECORDS,
        },
        ["failure"],
        "0 sec",
        ["HTTP_PASSWORD"],
    )
    dedupe = create_processor(
        token,
        pg_id,
        f"{pg_name}__dedupe__detect",
        "org.apache.nifi.processors.standard.DetectDuplicate",
        0,
        460,
        {
            "Cache Entry Identifier": "${dedupe.key}",
            "Cache The Entry Identifier": "true",
            "Age Off Duration": "24 hours",
            "Distributed Cache Service": dmc_service_id,
        },
        ["duplicate", "failure"],
        "0 sec",
        None,
        25,
    )
    publish = create_processor(
        token,
        pg_id,
        f"{pg_name}__raw__publish",
        "org.apache.nifi.kafka.processors.PublishKafka",
        0,
        700,
        {
            "Kafka Connection Service": KAFKA_SERVICE_ID,
            "Topic Name": topic,
            "Kafka Key": "${object_id}",
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
    create_connection(token, pg_id, trigger, f"{pg_name}__trigger", extract, f"{pg_name}__extract__fetch_emit_json", ["success"])
    create_connection(token, pg_id, extract, f"{pg_name}__extract__fetch_emit_json", dedupe, f"{pg_name}__dedupe__detect", ["success"])
    create_connection(token, pg_id, dedupe, f"{pg_name}__dedupe__detect", publish, f"{pg_name}__raw__publish", ["non-duplicate"])
    return pg_id


def main():
    token = login()
    maximum_id = create_pg(
        token,
        ROOT_INGEST_GROUP,
        f"{SOURCE_INSTANCE}.maximum",
        680,
        104,
        f"Maximum useful Rapid7 JSON-only ingestion for {SOURCE_INSTANCE}. Built by Codex automation; groups are stopped for controlled verification.",
        RAPID7_PARAM_CONTEXT_ID,
    )

    dmc_id = create_controller_service(
        token,
        maximum_id,
        f"{SOURCE_INSTANCE}.maximum__dedupe__cache",
        "org.apache.nifi.redis.service.RedisDistributedMapCacheClientService",
        {"Redis Connection Pool": REDIS_POOL_ID, "TTL": "24 hours"},
    )
    enable_controller_service(token, dmc_id)

    for idx, (entity, topic) in enumerate(ENTITY_TOPICS):
        build_entity_group(token, maximum_id, dmc_id, entity, topic, (idx % 4) * 520, (idx // 4) * 900)

    print(json.dumps({"maximum_group_id": maximum_id, "dedupe_cache_id": dmc_id, "entities": [e for e, _ in ENTITY_TOPICS]}, indent=2))


if __name__ == "__main__":
    main()
