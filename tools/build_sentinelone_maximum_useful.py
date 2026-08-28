"""Build the sentinelone.maximum_useful NiFi ingestion flow.

Mirrors tools/build_rapid7_securado_maximum_useful.py in structure: it imports
tools/build_fortisiem_maximum_useful.py as the shared NiFi/Kafka/Apicurio library and
only adds SentinelOne-specific wiring.

Design rules taken from the existing flows and from the sentinelone.agent reference flow:

  * pagination is NiFi-native (init_cursor -> fetch -> split -> page_meta -> has_more ->
    next_cursor -> fetch), never Groovy;
  * a single GenerateFlowFile trigger on a 2 hour TIMER_DRIVEN schedule;
  * every PublishKafka carries FlowFile Attribute Header Pattern so all 10 standard fields
    ship as Kafka headers on the raw *and* the Avro topic;
  * the same 10 fields are also injected into the message value by the Avro normalizer.

Groovy is used only where sentinelone.agent already uses it: secret redaction, the
dedupe-hash/metadata processor, and the Avro normalizer.

Usage:
    S1_AUTH_TOKEN='ApiToken ...' NIFI_USER=... NIFI_PASSWORD=... \
        python build_sentinelone_maximum_useful.py build-raw
    python build_sentinelone_maximum_useful.py run-once
    python build_sentinelone_maximum_useful.py infer-register
    python build_sentinelone_maximum_useful.py add-avro
    python build_sentinelone_maximum_useful.py connectors
    python build_sentinelone_maximum_useful.py verify-kafka
"""

import json
import os
import subprocess
import sys
import time
import urllib.parse
from collections import OrderedDict

import requests

import build_fortisiem_maximum_useful as n


SOURCE_INSTANCE = os.environ.get("S1_SOURCE_INSTANCE", "sentinelone")
PG_NAME = os.environ.get("S1_MAX_PG_NAME", f"{SOURCE_INSTANCE}.maximum_useful")
PARENT_PG_ID = os.environ.get("S1_PARENT_PG_ID", "0a00e822-01a0-1000-68b7-f28e69779c95")
GLOBAL_INFRA_PC_ID = os.environ.get("GLOBAL_INFRA_PC_ID", "b36adc3c-f19e-3912-7997-cd68aeee69a9")
REDIS_POOL_ID = os.environ.get("REDIS_POOL_ID", "b90bcbdb-d69c-3725-51d1-444dd57b9336")

SOURCE_API_BASE = os.environ.get("S1_SOURCE_API_BASE", "https://euce1-120-mssp.sentinelone.net/web/api/v2.1")
AUTH_TOKEN = os.environ.get("S1_AUTH_TOKEN")
CURSOR_LIMIT = os.environ.get("S1_CURSOR_LIMIT", "1000")
DEFAULT_CUSTOMER = os.environ.get("S1_DEFAULT_CUSTOMER", "Securado")

KAFKA_CONNECT_BASE = os.environ.get("KAFKA_CONNECT_BASE", "https://kafkaconnect.datapasc.com").rstrip("/")
SCHEMA_SAMPLE_LIMIT = int(os.environ.get("SCHEMA_SAMPLE_LIMIT", "300"))

n.PG_NAME = PG_NAME
n.PARENT_PG_ID = PARENT_PG_ID
n.CLIENT_ID = "codex-sentinelone-maximum"
n.PG_ID = None

# ---------------------------------------------------------------------------
# ingest_ts -- SentinelOne-only 11th standard field.
#
# Modelled on fileshare.asset__enrich__set_key, which sets /ingest_ts = ${now():toNumber()}
# (epoch millis) with the dedupe hash carrying EXCLUDES = ingest_id,ingest_ts.
#
# Distinct from extraction_timestamp: that is ISO-8601 and set once per run in
# maximum__run_metadata, whereas ingest_ts is epoch millis stamped per record at hash time,
# so it measures per-record latency through the flow.
#
# These are deliberately LOCAL. tools/build_fortisiem_maximum_useful.py is shared with the
# fortisiem and both rapid7 flows, which stay on 10 fields.
# ---------------------------------------------------------------------------

# extraction_timestamp (run-level, ISO) is dropped in favor of ingest_ts (per-record, epoch
# millis) -- ingestion_run_batch_identity already tells you which run a record came from, so no
# run-grouping information is lost. object_id (the composite/natural key) is added alongside
# source_object_id (native only, may be blank). Net field count: 11.
STANDARD_VALUE_FIELDS = [f for f in n.STANDARD_VALUE_FIELDS if f != "extraction_timestamp"] + ["object_id", "ingest_ts"]

STANDARD_HEADER_PATTERN = (
    r"^(source_platform|customer_tenant_organization|source_object_type|"
    r"source_object_id|object_id|source_event_update_timestamp|"
    r"api_endpoint_export_query_identity|cursor_window|payload_hash_fingerprint|"
    r"ingestion_run_batch_identity|ingest_ts)$"
)

# n.JSON_NORMALIZE_SCRIPT injects the 10 shared fields (including extraction_timestamp) into the
# message value via out.put(k, flowFile.getAttribute(k) ?: ''). This is the same script with:
#   - extraction_timestamp removed
#   - object_id added (plain string, same handling as source_object_id)
#   - ingest_ts added, then immediately overwritten with a genuine Groovy Long
#
# FlowFile attributes are always strings in NiFi, so the generic out.put(k, flowFile.getAttribute(k))
# loop can only ever produce a quoted JSON string for ingest_ts. Getting a real unquoted Avro
# `long` requires converting it back to a numeric type before JsonOutput.toJson() serializes it --
# a Groovy Long serializes unquoted, a Groovy String does not.
JSON_NORMALIZE_SCRIPT = n.JSON_NORMALIZE_SCRIPT.replace(
    "        'extraction_timestamp',\n",
    "",
).replace(
    "        'source_object_id',\n",
    "        'source_object_id',\n        'object_id',\n",
).replace(
    "        'ingestion_run_batch_identity'\n"
    "    ].each { k ->\n"
    "        out.put(k, flowFile.getAttribute(k) ?: '')\n"
    "    }\n",
    "        'ingestion_run_batch_identity',\n"
    "        'ingest_ts'\n"
    "    ].each { k ->\n"
    "        out.put(k, flowFile.getAttribute(k) ?: '')\n"
    "    }\n"
    "    def ingestTsRaw = flowFile.getAttribute('ingest_ts')\n"
    "    out.put('ingest_ts', (ingestTsRaw && ingestTsRaw.isLong()) ? ingestTsRaw.toLong() : 0L)\n",
)
if "'ingest_ts'" not in JSON_NORMALIZE_SCRIPT or "'extraction_timestamp'" in JSON_NORMALIZE_SCRIPT or "'object_id'" not in JSON_NORMALIZE_SCRIPT:
    raise RuntimeError("Failed to adjust JSON_NORMALIZE_SCRIPT field list -- shared script changed shape")
if "ingestTsRaw.toLong()" not in JSON_NORMALIZE_SCRIPT:
    raise RuntimeError("Failed to inject the ingest_ts numeric-conversion step -- shared script changed shape")


def add_standard_value_fields(sample, entity):
    """Local copy of n.add_standard_value_fields that also seeds ingest_ts for inference.

    n.add_standard_value_fields() (shared) always injects all 10 shared fields, including
    extraction_timestamp. It must be dropped here, not just left out of the primary loop --
    otherwise the pass-through loop below re-adds it from `out` and it leaks back into every
    inferred schema even though the running flow never writes it.
    """
    out = n.add_standard_value_fields(sample, entity)
    out.pop("extraction_timestamp", None)
    merged = OrderedDict()
    for field in STANDARD_VALUE_FIELDS:
        if field == "ingest_ts":
            # A genuine Python int, not a string -- and large enough to exceed int32 range,
            # so TypeNode/schema_from_samples infers Avro "long", not "int". Real epoch-millis
            # values are always this size (any date after 1970 already exceeds 2^31), and the
            # raw topic never carries ingest_ts natively (it's injected downstream), so this
            # synthetic sample is the only source the schema inference ever sees for this field.
            merged[field] = out.get(field, 1700000000000)
        else:
            merged[field] = out.get(field, "")
    for key, value in out.items():
        if key not in merged:
            merged[key] = value
    return merged

# Set by ensure_param_context(); ensure_pg() attaches it to the process group.
PARAM_CONTEXT_ID = os.environ.get("S1_PARAM_CONTEXT_ID")


# ---------------------------------------------------------------------------
# Entity definitions
#
# Verified live against the tenant on 2026-08-18. Every collection endpoint below
# paginates on $.pagination.nextCursor. `site` is the only one whose records live at
# $.data.sites[*] rather than $.data[*]; `site_policy` is a single object with no
# pagination and is rooted from the site branch.
# ---------------------------------------------------------------------------

ENTITIES = [
    {
        "entity": "site",
        "paged": True,
        "path": "/sites",
        "query": "",
        "split": "$.data.sites[*]",
        "extract": {
            "s1_object_id": "$.id",
            "s1_site_id": "$.id",
            "s1_site_name": "$.name",
            "s1_account_id": "$.accountId",
            "s1_account_name": "$.accountName",
        },
        "object_id": "${s1_object_id}",
        "customer": "${s1_site_name}",
        "update_ts": "${s1_updated_at}",
        "updated_at_path": "$.updatedAt",
        "redact": True,
        "api_identity": "GET /web/api/v2.1/sites?limit={limit}&cursor={cursor}",
    },
    {
        "entity": "group",
        "paged": True,
        "path": "/groups",
        "query": "",
        "split": "$.data[*]",
        "extract": {
            "s1_object_id": "$.id",
            "s1_site_id": "$.siteId",
            "s1_group_name": "$.name",
        },
        "object_id": "${s1_object_id}",
        "customer": "",
        "update_ts": "${s1_updated_at}",
        "updated_at_path": "$.updatedAt",
        "redact": True,
        "api_identity": "GET /web/api/v2.1/groups?limit={limit}&cursor={cursor}",
    },
    {
        "entity": "agent",
        "paged": True,
        "path": "/agents",
        "query": "",
        "split": "$.data[*]",
        "extract": {
            "s1_object_id": "$.id",
            "s1_agent_uuid": "$.uuid",
            "s1_site_id": "$.siteId",
            "s1_site_name": "$.siteName",
            "s1_group_id": "$.groupId",
            "s1_account_id": "$.accountId",
            "s1_computer_name": "$.computerName",
        },
        "object_id": "${s1_object_id}",
        "customer": "${s1_site_name}",
        "update_ts": "${s1_updated_at}",
        "updated_at_path": "$.updatedAt",
        "redact": True,
        "api_identity": "GET /web/api/v2.1/agents?limit={limit}&cursor={cursor}",
    },
    {
        "entity": "installed_application",
        "paged": True,
        "path": "/installed-applications",
        "query": "",
        "split": "$.data[*]",
        "extract": {
            "s1_object_id": "$.id",
            "s1_agent_id": "$.agentId",
            "s1_agent_uuid": "$.agentUuid",
            "s1_app_name": "$.name",
            "s1_app_version": "$.version",
        },
        "object_id": "${s1_object_id}",
        "customer": "",
        "update_ts": "${s1_updated_at}",
        "updated_at_path": "$.updatedAt",
        "redact": False,
        "api_identity": "GET /web/api/v2.1/installed-applications?limit={limit}&cursor={cursor}",
    },
    {
        "entity": "application_cve",
        "paged": True,
        "path": "/installed-applications/cves",
        "query": "",
        "split": "$.data[*]",
        "extract": {
            "s1_object_id": "$.id",
            "s1_cve_id": "$.cveId",
        },
        "object_id": "${s1_object_id}",
        "customer": "",
        "update_ts": "${s1_updated_at}",
        "updated_at_path": "$.updatedAt",
        "redact": False,
        "api_identity": "GET /web/api/v2.1/installed-applications/cves?limit={limit}&cursor={cursor}",
    },
    {
        "entity": "threat",
        "paged": True,
        "path": "/threats",
        # Bounded incremental window: 24h lookback, wider than the 2h schedule so the
        # Redis dedupe absorbs the overlap. Keeps a 24k-row collection off the wire.
        "query": "&updatedAt__gte=${window_24h}",
        "split": "$.data[*]",
        "extract": {
            "s1_object_id": "$.id",
            # Held under a distinct name so the child lanes still have the parent threat id
            # after their own EvaluateJsonPath overwrites s1_object_id.
            "s1_threat_id": "$.id",
            "s1_agent_id": "$.agentRealtimeInfo.agentId",
            "s1_site_id": "$.agentRealtimeInfo.siteId",
            "s1_site_name": "$.agentRealtimeInfo.siteName",
            "s1_updated_at": "$.threatInfo.updatedAt",
        },
        "object_id": "${s1_object_id}",
        "customer": "${s1_site_name}",
        "update_ts": "${s1_updated_at}",
        "updated_at_path": None,
        "redact": False,
        "api_identity": "GET /web/api/v2.1/threats?limit={limit}&cursor={cursor}&updatedAt__gte={window}",
        "cursor_window": "updatedAt__gte=${window_24h}",
        # The threat list keeps a 24h lookback so an outage can be caught up, but the
        # per-threat child calls only fire for threats touched in the last 4h. Without this
        # gate every run would re-fetch timeline+notes for all ~50 threats in the window,
        # about 1,200 calls/day instead of ~290.
        "child_gate": ("${s1_updated_at:substring(0,19)"
                       ":toDate(\"yyyy-MM-dd'T'HH:mm:ss\",\"GMT\")"
                       ":toNumber():gt(${window_4h_epoch})}"),
    },
    {
        "entity": "activity",
        "paged": True,
        "path": "/activities",
        # 4h lookback against a 2h schedule.
        "query": "&createdAt__gte=${window_4h}",
        "split": "$.data[*]",
        "extract": {
            "s1_object_id": "$.id",
            "s1_agent_id": "$.agentId",
            "s1_site_id": "$.siteId",
            "s1_site_name": "$.siteName",
            "s1_threat_id": "$.threatId",
            "s1_updated_at": "$.updatedAt",
        },
        "object_id": "${s1_object_id}",
        "customer": "${s1_site_name}",
        "update_ts": "${s1_updated_at}",
        "updated_at_path": None,
        "redact": False,
        "api_identity": "GET /web/api/v2.1/activities?limit={limit}&cursor={cursor}&createdAt__gte={window}",
        "cursor_window": "createdAt__gte=${window_4h}",
    },
    {
        # Child of the site branch: one policy object per site, no pagination.
        "entity": "site_policy",
        "paged": False,
        "parent": "site",
        "path": "/sites/${s1_site_id}/policy",
        "query": "",
        "split": None,
        "extract": {},
        "object_id": "site_policy_${s1_site_id}",
        "customer": "${s1_site_name}",
        "update_ts": "",
        "updated_at_path": None,
        # Not for secrets -- this endpoint has none. The processor is here to strip the
        # {"data": {...}} envelope so policy settings become top-level columns.
        "redact": True,
        "unwrap_root": "data",
        "api_identity": "GET /web/api/v2.1/sites/{siteId}/policy",
    },

    # ---------------- Phase 2 ----------------
    {
        "entity": "user",
        "paged": True,
        "path": "/users",
        "query": "",
        "split": "$.data[*]",
        "extract": {"s1_object_id": "$.id", "s1_user_email": "$.email"},
        "object_id": "${s1_object_id}",
        "customer": "",
        "update_ts": "${s1_updated_at}",
        "updated_at_path": "$.updatedAt",
        # /users exposes an apiToken field.
        "redact": True,
        "api_identity": "GET /web/api/v2.1/users?limit={limit}&cursor={cursor}",
    },
    {
        "entity": "role",
        "paged": True,
        "path": "/rbac/roles",
        "query": "",
        "split": "$.data[*]",
        "extract": {"s1_object_id": "$.id", "s1_role_name": "$.name", "s1_scope_id": "$.scopeId"},
        "object_id": "${s1_object_id}",
        "customer": "",
        "update_ts": "${s1_updated_at}",
        "updated_at_path": "$.updatedAt",
        "redact": False,
        "api_identity": "GET /web/api/v2.1/rbac/roles?limit={limit}&cursor={cursor}",
    },
    {
        # Unscoped returns all 1,293 across every site; the site-scoped call only returns 170.
        "entity": "exclusion",
        "paged": True,
        "path": "/exclusions",
        "query": "",
        "split": "$.data[*]",
        "extract": {"s1_object_id": "$.id", "s1_exclusion_type": "$.type", "s1_scope_name": "$.scopeName"},
        "object_id": "${s1_object_id}",
        "customer": "${s1_scope_name}",
        "update_ts": "${s1_updated_at}",
        "updated_at_path": "$.updatedAt",
        "redact": False,
        "api_identity": "GET /web/api/v2.1/exclusions?limit={limit}&cursor={cursor}",
    },
    {
        "entity": "restriction",
        "paged": True,
        "path": "/restrictions",
        "query": "",
        "split": "$.data[*]",
        "extract": {"s1_object_id": "$.id", "s1_sha256": "$.sha256Value", "s1_scope_name": "$.scopeName"},
        "object_id": "${s1_object_id}",
        "customer": "${s1_scope_name}",
        "update_ts": "${s1_updated_at}",
        "updated_at_path": "$.updatedAt",
        "redact": False,
        "api_identity": "GET /web/api/v2.1/restrictions?limit={limit}&cursor={cursor}",
    },
    {
        "entity": "config_override",
        "paged": True,
        "path": "/config-override",
        "query": "",
        "split": "$.data[*]",
        "extract": {"s1_object_id": "$.id", "s1_override_name": "$.name"},
        "object_id": "${s1_object_id}",
        "customer": "",
        "update_ts": "${s1_updated_at}",
        "updated_at_path": "$.updatedAt",
        "redact": False,
        "api_identity": "GET /web/api/v2.1/config-override?limit={limit}&cursor={cursor}",
    },
    {
        # Reference/lookup table: returns all 760 in one response, no pagination block.
        "entity": "activity_type",
        "paged": False,
        "path": "/activities/types",
        "query": "",
        "split": "$.data[*]",
        "extract": {"s1_object_id": "$.id", "s1_activity_action": "$.action"},
        "object_id": "${s1_object_id}",
        "customer": "",
        "update_ts": "",
        "updated_at_path": None,
        "redact": False,
        "api_identity": "GET /web/api/v2.1/activities/types",
    },
    {
        "entity": "alert",
        "paged": True,
        "path": "/cloud-detection/alerts",
        "query": "",
        "split": "$.data[*]",
        "extract": {
            "s1_object_id": "$.alertInfo.alertId",
            "s1_agent_id": "$.agentRealtimeInfo.agentId",
            "s1_site_name": "$.agentRealtimeInfo.siteName",
            "s1_rule_id": "$.ruleInfo.id",
            "s1_updated_at": "$.alertInfo.updatedAt",
        },
        "object_id": "${s1_object_id}",
        "customer": "${s1_site_name}",
        "update_ts": "${s1_updated_at}",
        "updated_at_path": None,
        "redact": False,
        "api_identity": "GET /web/api/v2.1/cloud-detection/alerts?limit={limit}&cursor={cursor}",
    },
    {
        "entity": "threat_timeline",
        "paged": True,
        "parent": "threat",
        "via_gate": True,
        "path": "/threats/${s1_threat_id}/timeline",
        "query": "",
        "split": "$.data[*]",
        "extract": {"s1_object_id": "$.id", "s1_activity_type": "$.activityType"},
        "object_id": "${s1_object_id}",
        "customer": "${s1_site_name}",
        "update_ts": "${s1_updated_at}",
        "updated_at_path": "$.updatedAt",
        "redact": False,
        "api_identity": "GET /web/api/v2.1/threats/{threatId}/timeline?limit={limit}&cursor={cursor}",
    },
    {
        "entity": "threat_note",
        "paged": True,
        "parent": "threat",
        "via_gate": True,
        "path": "/threats/${s1_threat_id}/notes",
        "query": "",
        "split": "$.data[*]",
        "extract": {"s1_object_id": "$.id", "s1_note_creator": "$.creator"},
        "object_id": "${s1_object_id}",
        "customer": "${s1_site_name}",
        "update_ts": "${s1_updated_at}",
        "updated_at_path": "$.updatedAt",
        "redact": False,
        "api_identity": "GET /web/api/v2.1/threats/{threatId}/notes?limit={limit}&cursor={cursor}",
    },

    # ---------------- Phase 3: unblocked entities found in the full API reference ----------------
    {
        # SentinelOne's own unified asset inventory and the single biggest CMDB gain here.
        # 4,548 assets vs 4,465 agents. Carries serialNumber (raw.md section 8 ranks this a STRONG
        # identity signal), assetCriticality/Environment, category/subCategory, riskFactors,
        # missingCoverage, and identity.adMachine*/adUser* AD context.
        #
        # This is a SUPERSET of the typed sub-endpoints: surface/endpoint (4,463) and
        # surface/networkDiscovery (439) both sit inside these 4,548, exposed via the `surfaces`
        # array -- so it delivers unmanaged-device discovery even though /ranger/table-view is 403.
        # The 12 typed routes (/xdr/assets/device, /server, /workstation, ...) are deliberately NOT
        # ingested: they are filtered views of these same rows and would duplicate data.
        "entity": "xdr_asset",
        "paged": True,
        "path": "/xdr/assets",
        "query": "",
        "split": "$.data[*]",
        "extract": {
            "s1_object_id": "$.id",
            "s1_site_id": "$.s1SiteId",
            "s1_site_name": "$.s1SiteName",
            "s1_group_id": "$.s1GroupId",
            "s1_category": "$.category",
            "s1_serial_number": "$.serialNumber",
            "s1_updated_at": "$.s1UpdatedAt",
        },
        "object_id": "${s1_object_id}",
        "customer": "${s1_site_name}",
        "update_ts": "${s1_updated_at}",
        "updated_at_path": None,
        "redact": False,
        "api_identity": "GET /web/api/v2.1/xdr/assets?limit={limit}&cursor={cursor}",
    },
    {
        "entity": "xdr_asset_tag",
        "paged": True,
        "path": "/xdr/assets/tags",
        "query": "",
        "split": "$.data[*]",
        "extract": {"s1_object_id": "$.id", "s1_tag_key": "$.key", "s1_tag_value": "$.value"},
        "object_id": "${s1_object_id}",
        "customer": "",
        "update_ts": "",
        "updated_at_path": None,
        "redact": False,
        "api_identity": "GET /web/api/v2.1/xdr/assets/tags?limit={limit}&cursor={cursor}",
    },
    {
        "entity": "agent_tag",
        "paged": True,
        "path": "/agents/tags",
        "query": "",
        "split": "$.data[*]",
        "extract": {"s1_object_id": "$.id", "s1_tag_key": "$.key", "s1_scope": "$.scope"},
        "object_id": "${s1_object_id}",
        "customer": "",
        "update_ts": "${s1_updated_at}",
        "updated_at_path": "$.updatedAt",
        "redact": False,
        "api_identity": "GET /web/api/v2.1/agents/tags?limit={limit}&cursor={cursor}",
    },
    {
        "entity": "cloud_detection_rule",
        "paged": True,
        "path": "/cloud-detection/rules",
        "query": "",
        "split": "$.data[*]",
        "extract": {"s1_object_id": "$.id", "s1_rule_name": "$.name", "s1_scope": "$.scopeLevel"},
        "object_id": "${s1_object_id}",
        "customer": "",
        "update_ts": "${s1_updated_at}",
        "updated_at_path": "$.updatedAt",
        "redact": False,
        "api_identity": "GET /web/api/v2.1/cloud-detection/rules?limit={limit}&cursor={cursor}",
    },
    {
        # Exposes an apiToken field -- redaction is mandatory, not cosmetic.
        "entity": "service_user",
        "paged": True,
        "path": "/service-users",
        "query": "",
        "split": "$.data[*]",
        "extract": {"s1_object_id": "$.id", "s1_service_user_name": "$.name"},
        "object_id": "${s1_object_id}",
        "customer": "",
        "update_ts": "${s1_updated_at}",
        "updated_at_path": "$.updatedAt",
        "redact": True,
        "api_identity": "GET /web/api/v2.1/service-users?limit={limit}&cursor={cursor}",
    },
    {
        "entity": "agent_package",
        "paged": True,
        "path": "/update/agent/packages",
        "query": "",
        "split": "$.data[*]",
        "extract": {"s1_object_id": "$.id", "s1_package_version": "$.version", "s1_os_type": "$.osType"},
        "object_id": "${s1_object_id}",
        "customer": "",
        "update_ts": "${s1_updated_at}",
        "updated_at_path": "$.updatedAt",
        "redact": False,
        "api_identity": "GET /web/api/v2.1/update/agent/packages?limit={limit}&cursor={cursor}",
    },
    {
        "entity": "location",
        "paged": True,
        "path": "/locations",
        "query": "",
        "split": "$.data[*]",
        "extract": {"s1_object_id": "$.id", "s1_location_name": "$.name", "s1_scope": "$.scope"},
        "object_id": "${s1_object_id}",
        "customer": "",
        "update_ts": "${s1_updated_at}",
        "updated_at_path": "$.updatedAt",
        "redact": False,
        "api_identity": "GET /web/api/v2.1/locations?limit={limit}&cursor={cursor}",
    },
    {
        # Empty on this tenant today (0 IOCs) but the route is live; builds so it populates later.
        "entity": "ioc",
        "paged": True,
        "path": "/threat-intelligence/iocs",
        "query": "",
        "split": "$.data[*]",
        "extract": {"s1_object_id": "$.id", "s1_ioc_type": "$.type", "s1_ioc_value": "$.value"},
        "object_id": "${s1_object_id}",
        "customer": "",
        "update_ts": "${s1_updated_at}",
        "updated_at_path": "$.updatedAt",
        "redact": False,
        "api_identity": "GET /web/api/v2.1/threat-intelligence/iocs?limit={limit}&cursor={cursor}",
    },
    {
        # Singleton settings objects: no native id anywhere in the payload, so a deterministic key
        # from the parent scope is the only stable choice. Documented raw.md exception.
        # redact=True is used purely to unwrap the {"data": {...}} envelope, as site_policy does.
        "entity": "tenant_policy",
        "paged": False,
        "path": "/tenant/policy",
        "query": "",
        "split": None,
        "extract": {},
        "object_id": "tenant_policy_sentinelone",
        "customer": "",
        "update_ts": "",
        "updated_at_path": None,
        "redact": True,
        "unwrap_root": "data",
        "api_identity": "GET /web/api/v2.1/tenant/policy",
    },
    {
        "entity": "system_info",
        "paged": False,
        "path": "/system/info",
        "query": "",
        "split": None,
        "extract": {},
        "object_id": "system_info_sentinelone",
        "customer": "",
        "update_ts": "",
        "updated_at_path": None,
        "redact": True,
        "unwrap_root": "data",
        "api_identity": "GET /web/api/v2.1/system/info",
    },
    {
        # Child of group: raw.md family 1 "endpoint policy assignment". 146 calls per run.
        "entity": "group_policy",
        "paged": False,
        "parent": "group",
        "path": "/groups/${s1_object_id}/policy",
        "query": "",
        "split": None,
        "extract": {},
        "object_id": "group_policy_${s1_object_id}",
        "customer": "",
        "update_ts": "",
        "updated_at_path": None,
        "redact": True,
        "unwrap_root": "data",
        "api_identity": "GET /web/api/v2.1/groups/{groupId}/policy",
    },
]

# ---------------------------------------------------------------------------
# source_object_id vs. object_id
#
# raw.md keeps "Source object ID" as the vendor's NATIVE id -- entity["object_id"] above is
# that native expression (or, for the four endpoints below with no native id in their payload
# at all, blank). It is never a composite.
#
# object_id is a SEPARATE field: the natural/composite key used as the Kafka message key and
# the dedupe key. Where an entity is a genuine child of a parent (installed_application under
# agent, threat_timeline/threat_note under threat, etc.), it is "${parent}_${native}". Where
# there is no parent, it mirrors the native id so every entity still has a non-blank Kafka key.
# ---------------------------------------------------------------------------

_COMPOSITE_OVERRIDES = {
    "installed_application": "${s1_agent_id}_${s1_object_id}",
    "activity": "${s1_agent_id}_${s1_object_id}",
    "exclusion": "${s1_scope_name}_${s1_object_id}",
    "restriction": "${s1_scope_name}_${s1_object_id}",
    "alert": "${s1_agent_id}_${s1_object_id}",
    "threat_timeline": "${s1_threat_id}_${s1_object_id}",
    "threat_note": "${s1_threat_id}_${s1_object_id}",
    "xdr_asset": "${s1_site_id}_${s1_object_id}",
    "xdr_asset_tag": "${s1_tag_key}_${s1_object_id}",
    "agent_tag": "${s1_scope}_${s1_object_id}",
}
# These endpoints return a bare settings object with no native id field anywhere in the
# payload. source_object_id is blank for them; composite_id (below) keeps its existing
# parent-derived or fixed key so the Kafka key and dedupe key are never blank.
_NO_NATIVE_ID_ENTITIES = {"site_policy", "group_policy", "tenant_policy", "system_info"}

for _ent in ENTITIES:
    _name = _ent["entity"]
    _ent["composite_id"] = _COMPOSITE_OVERRIDES.get(_name, _ent["object_id"])
    if _name in _NO_NATIVE_ID_ENTITIES:
        _ent["object_id"] = "${literal('')}"

ENTITIES_BY_NAME = {e["entity"]: e for e in ENTITIES}

# Families raw.md asks for that this token cannot reach. Rendered as labels only, the same
# way build_fortisiem_maximum_useful.py handles SCAFFOLD_ENTITIES.
SCAFFOLD_ENTITIES = [
    ("xdr_asset", "GET /application-management/inventory -> HTTP 403 insufficient permissions"),
    ("ranger_discovered_device", "GET /ranger/table-view -> HTTP 403 insufficient permissions"),
    ("ranger_network", "GET /ranger/networks -> HTTP 404 not present on this tenant"),
    ("rogue_device", "GET /rogues/table-view -> requires an account-scope filter; token is site-scoped"),
    ("endpoint_vulnerability_finding", "GET /application-management/risks/endpoints -> HTTP 403"),
    ("application_risk", "GET /application-management/risks/applications -> HTTP 403"),
    ("cloud_resource", "cloud inventory routes -> not exposed to this token/SKU"),
    ("cloud_posture_finding", "cloud posture routes -> not exposed to this token/SKU"),
    ("kubernetes_resource", "no inventory route; threat records carry kubernetesInfo only"),
    ("container_workload", "no inventory route; threat records carry containerInfo only"),
    ("endpoint_process", "GET /agents/processes?ids= -> per-agent call, online agents only, volatile"),
    ("deep_visibility_event", "POST /dv/init-query then poll query-status then page events [gated]"),
    ("deep_visibility_process", "POST /dv/init-query lifecycle [gated]"),
    ("unified_alert", "GET /unified-alerts -> HTTP 404 not present on this tenant"),
]


# ---------------------------------------------------------------------------
# Groovy scripts
# ---------------------------------------------------------------------------

# Strips enrolment tokens / API tokens / licence keys out of the payload before anything
# else touches it. Keys are removed rather than blanked so they never reach the inferred
# Avro schema, and this runs before the hash so redaction cannot itself cause a republish.
REDACT_SCRIPT = r'''
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.io.OutputStreamCallback

def flowFile = session.get()
if (!flowFile) return

// Exact key names known to carry secrets in SentinelOne payloads, plus a suffix rule for
// anything future that is named <something>Token / ApiKey / Credential / Passphrase.
//
// The suffix rule deliberately excludes bare "password" and "key": SentinelOne policy
// objects contain boolean toggles such as dvEventTypeOpenDirectoryModifyPassword, and a
// contains-match would silently delete real configuration data.
def SECRET_KEYS = ['registrationtoken', 'apitoken', 'licensekey', 'passphrase',
                   'password', 'secretkey', 'clientsecret'] as Set
def SECRET_PATTERN = ~/(?i).*(token|apikey|api_key|credential|passphrase)$/

def redacted = [count: 0]
def scrub
scrub = { value ->
    if (value instanceof Map) {
        def out = new LinkedHashMap()
        value.each { k, v ->
            def key = k.toString()
            def lower = key.toLowerCase()
            if (SECRET_KEYS.contains(lower) || SECRET_PATTERN.matcher(key).matches()) {
                redacted.count = redacted.count + 1
                return
            }
            out.put(key, scrub(v))
        }
        return out
    }
    if (value instanceof List) return value.collect { scrub(it) }
    return value
}

try {
    def textHolder = [value: '']
    session.read(flowFile, { inputStream -> textHolder.value = inputStream.getText('UTF-8') } as InputStreamCallback)
    def parsed = new JsonSlurper().parseText(textHolder.value)

    // Single-object endpoints return {"data": {...}}. Unwrapping here keeps the Bronze row
    // shape consistent with the split-based entities, so policy settings become real
    // columns in Iceberg instead of one nested blob.
    def unwrapProp = context.getProperty('UNWRAP_ROOT')
    def unwrap = unwrapProp == null ? null : unwrapProp.evaluateAttributeExpressions(flowFile).getValue()
    if (unwrap && unwrap.trim() && parsed instanceof Map && parsed.containsKey(unwrap)) {
        def inner = parsed.get(unwrap)
        if (inner instanceof Map) parsed = inner
    }

    def cleaned = scrub(parsed)
    flowFile = session.write(flowFile, { os -> os.write(JsonOutput.toJson(cleaned).getBytes('UTF-8')) } as OutputStreamCallback)
    flowFile = session.putAttribute(flowFile, 'redacted_field_count', redacted.count.toString())
    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    flowFile = session.putAttribute(flowFile, 'redact.error', e.message ?: e.toString())
    log.error('sentinelone secret redaction failed: ' + e.message, e)
    session.transfer(flowFile, REL_FAILURE)
}
'''


# Adapted from sentinelone.agent__dedupe__hash. Adds DEFAULT_CUSTOMER fallback and an
# EXCLUDE_FIELDS list so volatile fields can be kept in the payload but left out of the
# content hash.
DEDUPE_HASH_SCRIPT = r'''
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import java.security.MessageDigest
import java.time.Instant
import org.apache.nifi.processor.io.InputStreamCallback

def flowFile = session.get()
if (!flowFile) return

try {
    def prop = { name ->
        def p = context.getProperty(name)
        return p == null ? null : p.evaluateAttributeExpressions(flowFile).getValue()
    }

    def textHolder = [value: '']
    session.read(flowFile, { inputStream ->
        textHolder.value = inputStream.getText('UTF-8')
    } as InputStreamCallback)

    def parsed = new JsonSlurper().parseText(textHolder.value)
    def rec = (parsed instanceof List) ? parsed[0] : parsed
    if (!(rec instanceof Map)) {
        throw new IllegalArgumentException('Expected JSON object after split')
    }

    def sourcePlatform = prop('SOURCE_PLATFORM') ?: 'sentinelone'
    def entity = prop('SOURCE_OBJECT_TYPE')

    // source_object_id: the vendor's NATIVE id, per raw.md. Blank is a valid, intentional
    // value for the handful of endpoints that return a settings object with no id field --
    // it is not defaulted to a generated value.
    def nativeObjectId = prop('OBJECT_ID')
    if (nativeObjectId == null || nativeObjectId.contains('${')) nativeObjectId = ''
    nativeObjectId = nativeObjectId.trim().length() == 0 ? '' : nativeObjectId.replaceAll('[^A-Za-z0-9_.:-]', '_')

    // object_id: the natural/composite key. Always non-blank -- it is the Kafka message key
    // and the dedupe key, so a blank value here would break both. Falls back to the FlowFile
    // uuid only as a last resort if the composite expression itself failed to resolve.
    def objectId = prop('COMPOSITE_OBJECT_ID')
    if (!objectId || objectId.trim().length() == 0 || objectId.contains('${')) {
        objectId = flowFile.getAttribute('uuid')
    }
    objectId = objectId.replaceAll('[^A-Za-z0-9_.:-]', '_')

    // Content hash excludes ingestion-generated fields (they are attributes, not payload)
    // plus any explicitly volatile source fields named in EXCLUDE_FIELDS.
    def excludeRaw = prop('EXCLUDE_FIELDS') ?: ''
    def excludes = excludeRaw.split(',').collect { it.trim() }.findAll { it }
    def hashable = rec
    if (excludes) {
        hashable = new LinkedHashMap()
        rec.each { k, v -> if (!excludes.contains(k.toString())) hashable.put(k.toString(), v) }
    }
    def canonical
    canonical = { obj ->
        if (obj instanceof Map) {
            def out = new TreeMap()
            obj.each { k, v -> out[k.toString()] = canonical(v) }
            return out
        }
        if (obj instanceof List) return obj.collect { canonical(it) }
        return obj
    }
    def canonicalJson = JsonOutput.toJson(canonical(hashable))
    def hash = MessageDigest.getInstance('SHA-256').digest(canonicalJson.getBytes('UTF-8')).encodeHex().toString()

    def customerOrg = prop('CUSTOMER_TENANT_ORGANIZATION')
    if (!customerOrg || customerOrg.trim().length() == 0 || customerOrg.contains('${')) {
        customerOrg = rec.get('siteName') ?: rec.get('accountName') ?: flowFile.getAttribute('s1_site_name')
    }
    // No DEFAULT_CUSTOMER parameter fallback here on purpose: when neither the per-entity
    // expression nor the record itself carries a tenant, the field is genuinely unresolvable,
    // so it defaults straight to the literal 'NA' rather than a guessed account name.
    if (!customerOrg || customerOrg.toString().trim().length() == 0) {
        customerOrg = 'NA'
    }

    // raw.md section 5B: Source + Tenant + Object Type + Source Object ID + Content Hash.
    // Tenant must be resolved before the key is built.
    def tenantKey = (customerOrg ?: '').toString().trim() ?: '_'
    def dedupeKey = "${sourcePlatform}:${tenantKey}:${entity}:${objectId}:${hash}"
    // Public object identity: the same composite MINUS the content hash, so it stays stable for a
    // given record across content changes. dedupeKey keeps the hash and drives change detection.
    def objectIdPublic = "${sourcePlatform}:${tenantKey}:${entity}:${objectId}"

    def sourceUpdateTs = prop('SOURCE_EVENT_UPDATE_TIMESTAMP')
    if (!sourceUpdateTs || sourceUpdateTs.contains('${')) {
        sourceUpdateTs = rec.get('updatedAt') ?: ''
    }

    def runId = flowFile.getAttribute('ingestion_run_batch_identity')
    if (!runId || runId.trim().length() == 0) runId = flowFile.getAttribute('uuid')

    def cursorWindow = prop('CURSOR_WINDOW') ?: ''

    def attrPairs = [
        'source_platform': sourcePlatform,
        'customer_tenant_organization': customerOrg,
        'source_object_type': entity,
        'source_object_id': nativeObjectId,
        'object_id': objectIdPublic,
        'source_event_update_timestamp': sourceUpdateTs,
        'api_endpoint_export_query_identity': prop('API_ENDPOINT_EXPORT_QUERY_IDENTITY') ?: '',
        'cursor_window': cursorWindow,
        'payload_hash_fingerprint': hash,
        'ingestion_run_batch_identity': runId,
        // Per-record stamp, epoch millis, matching the fileshare flow's /ingest_ts.
        // Set AFTER the hash above, so it can never enter the content fingerprint.
        'ingest_ts': System.currentTimeMillis().toString(),
        'dedupe.key': dedupeKey,
        'kafka_topic': "bronze.sentinelone.${entity}__raw",
        'avro_topic': "bronze.sentinelone.${entity}__raw.avro",
        'avro_subject': "bronze.sentinelone.${entity}__raw.avro-value"
    ]
    attrPairs.each { k, v -> flowFile = session.putAttribute(flowFile, k, v == null ? '' : v.toString()) }

    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    flowFile = session.putAttribute(flowFile, 'dedupe.error', e.message ?: e.toString())
    log.error('sentinelone raw hash/enrich failed: ' + e.message, e)
    session.transfer(flowFile, REL_FAILURE)
}
'''


# ---------------------------------------------------------------------------
# NiFi helpers
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# NiFi transport override
#
# The shared library shells out to curl.exe for every NiFi call. At this flow's size (200+
# processors, so 400+ calls per build) curl.exe intermittently dies with 0xC0000005, taking
# the build down mid-way. Routing the same calls through `requests` in-process removes the
# crash and keeps a session alive. Local override only -- the shared module is untouched.
# ---------------------------------------------------------------------------

_SESSION = requests.Session()
_SESSION.verify = False
_TOKEN = {"value": None}


def _nifi_login():
    if _TOKEN["value"]:
        return _TOKEN["value"]
    if os.environ.get("NIFI_TOKEN"):
        _TOKEN["value"] = os.environ["NIFI_TOKEN"]
        return _TOKEN["value"]
    user, pwd = os.environ.get("NIFI_USER"), os.environ.get("NIFI_PASSWORD")
    if not user or not pwd:
        raise RuntimeError("Set NIFI_TOKEN or NIFI_USER/NIFI_PASSWORD")
    r = _SESSION.post(f"{n.NIFI_BASE}/nifi-api/access/token",
                      data={"username": user, "password": pwd}, timeout=60)
    r.raise_for_status()
    _TOKEN["value"] = r.text.strip()
    return _TOKEN["value"]


def nifi_requests(method, path, body=None, timeout=120):
    last = None
    for attempt in range(4):
        try:
            headers = {"Authorization": f"Bearer {_nifi_login()}"}
            if body is not None:
                headers["Content-Type"] = "application/json"
            r = _SESSION.request(method, f"{n.NIFI_BASE}{path}", headers=headers,
                                 data=json.dumps(body) if body is not None else None,
                                 timeout=timeout)
            if r.status_code == 401:
                _TOKEN["value"] = None
                last = RuntimeError(f"{method} {path} HTTP 401")
                continue
            if r.status_code < 200 or r.status_code > 299:
                raise RuntimeError(f"{method} {path} HTTP {r.status_code}: {r.text[:1500]}")
            txt = (r.text or "").strip()
            if not txt:
                return {}
            try:
                return json.loads(txt)
            except json.JSONDecodeError:
                return txt
        except requests.exceptions.RequestException as exc:
            last = exc
            time.sleep(1 + attempt)
    raise RuntimeError(f"{method} {path} failed after retries: {last}")


# The reverse proxy in front of NiFi rejects some PUT/POST paths that do not come from curl
# (parameter-context updates, controller-service run-status, processor entity updates all return
# 403), so curl stays the transport. Two problems show up at this flow's size (318 processors):
# curl.exe intermittently dies with 0xC0000005, and against this specific process group even
# simple calls (GET .../controller-services, PUT .../run-status) can take longer than the shared
# library's 90s default -- NiFi's own response time, not a client-side issue. Both are retried;
# subprocess.TimeoutExpired is NOT a RuntimeError, so it must be caught separately or it crashes
# straight through the RuntimeError-only handler that was here before.
# nifi_requests() above is kept for read-only helpers that benefit from a keep-alive session.

_orig_run_curl = n.run_curl
_MIN_TIMEOUT = 240


def _run_curl_retrying(args, input_text=None, timeout=90):
    timeout = max(timeout, _MIN_TIMEOUT)
    last = None
    for attempt in range(5):
        try:
            return _orig_run_curl(args, input_text, timeout)
        except subprocess.TimeoutExpired as exc:
            last = exc
            timeout = min(timeout + 120, 600)
            time.sleep(1 + attempt * 2)
        except RuntimeError as exc:
            msg = str(exc)
            # 3221225477 == 0xC0000005 access violation; also retry empty/aborted responses.
            if "3221225477" not in msg and "exit 52" not in msg and "exit 56" not in msg:
                raise
            last = exc
            time.sleep(1 + attempt * 2)
    raise RuntimeError(f"curl kept crashing after retries: {last}")


n.run_curl = _run_curl_retrying


def ensure_controller_service(name, service_type, properties):
    """Reuse an existing enabled service; only create when missing.

    n.create_controller_service() DISABLEs an existing service so it can rewrite properties,
    then re-enables it. Through this deployment's reverse proxy that PUT returns 403, and the
    teardown is unnecessary anyway -- these services are created once and their properties do
    not change between builds.
    """
    data = n.nifi("GET", f"/nifi-api/flow/process-groups/{n.pg_id()}/controller-services")
    for svc in data.get("controllerServices", []):
        if svc["component"]["name"] == name:
            sid = svc["id"]
            if svc["component"].get("state") != "ENABLED":
                try:
                    n.enable_controller_service(sid)
                except Exception as exc:
                    print(f"  warn: could not enable {name}: {str(exc)[:120]}", file=sys.stderr)
            return sid
    payload = {"revision": {"clientId": n.CLIENT_ID, "version": 0},
               "component": {"name": name, "type": service_type, "properties": properties}}
    sid = n.nifi("POST", f"/nifi-api/process-groups/{n.pg_id()}/controller-services", payload)["id"]
    n.enable_controller_service(sid)
    return sid


def set_sensitive_dynamic(proc_id, names, properties=None):
    """Mark dynamic properties sensitive so they may reference a sensitive parameter.

    NiFi returns already-set sensitive properties as the literal string '********'. Writing
    that value back stores the mask as the real secret -- the processor stays VALID while the
    credential is silently destroyed. Masked values are therefore dropped rather than echoed,
    unless the caller explicitly supplies a replacement.
    """
    ent = n.nifi("GET", f"/nifi-api/processors/{proc_id}")
    comp = ent["component"]
    cfg = dict(comp.get("config") or {})
    props = {k: v for k, v in (cfg.get("properties") or {}).items() if v != "********"}
    if properties:
        props.update(properties)
    cfg["properties"] = props
    cfg["sensitiveDynamicPropertyNames"] = names
    payload = {
        "revision": {"clientId": n.CLIENT_ID, "version": ent["revision"]["version"]},
        "component": {"id": proc_id, "name": comp["name"], "config": cfg},
    }
    return n.nifi("PUT", f"/nifi-api/processors/{proc_id}", payload)


def ensure_param_context():
    """Create or update the sentinelone.maximum_useful parameter context."""
    global PARAM_CONTEXT_ID
    if PARAM_CONTEXT_ID:
        return PARAM_CONTEXT_ID

    params = [
        {"parameter": {"name": "SOURCE_API_BASE", "value": SOURCE_API_BASE, "sensitive": False,
                       "description": "SentinelOne v2.1 API base"}},
        {"parameter": {"name": "CURSOR_LIMIT", "value": CURSOR_LIMIT, "sensitive": False,
                       "description": "Page size for cursor pagination"}},
        {"parameter": {"name": "SOURCE_INSTANCE", "value": SOURCE_INSTANCE, "sensitive": False}},
        {"parameter": {"name": "DEFAULT_CUSTOMER", "value": DEFAULT_CUSTOMER, "sensitive": False,
                       "description": "Fallback customer_tenant_organization for tenant-wide entities"}},
        {"parameter": {"name": "BLOCKED_SITES", "value": "", "sensitive": False}},
    ]
    if AUTH_TOKEN:
        params.append({"parameter": {"name": "AUTH_TOKEN", "value": AUTH_TOKEN, "sensitive": True,
                                     "description": "Full 'ApiToken <jwt>' Authorization header value"}})

    existing = None
    for pc in n.nifi("GET", "/nifi-api/flow/parameter-contexts")["parameterContexts"]:
        if pc["component"]["name"] == PG_NAME:
            existing = pc
            break

    if existing:
        pcid = existing["id"]
        if not AUTH_TOKEN:
            # Nothing to change: the context already holds SOURCE_API_BASE / CURSOR_LIMIT /
            # DEFAULT_CUSTOMER and the sensitive AUTH_TOKEN. Skipping the update-request avoids
            # a needless state change (and the reverse proxy rejects POSTs to this path).
            PARAM_CONTEXT_ID = pcid
            return pcid
        if False:
            params = [p for p in params if p["parameter"]["name"] != "AUTH_TOKEN"]
        payload = {
            "revision": {"clientId": n.CLIENT_ID, "version": existing["revision"]["version"]},
            "id": pcid,
            "component": {"id": pcid, "name": PG_NAME, "parameters": params},
        }
        req = n.nifi("POST", f"/nifi-api/parameter-contexts/{pcid}/update-requests", payload)
        rid = req["request"]["requestId"]
        for _ in range(30):
            st = n.nifi("GET", f"/nifi-api/parameter-contexts/{pcid}/update-requests/{rid}")
            if st["request"].get("complete"):
                break
            time.sleep(1)
        n.nifi("DELETE", f"/nifi-api/parameter-contexts/{pcid}/update-requests/{rid}")
        PARAM_CONTEXT_ID = pcid
        return pcid

    if not AUTH_TOKEN:
        raise RuntimeError("Set S1_AUTH_TOKEN on first build so AUTH_TOKEN can be stored as a sensitive parameter")

    payload = {
        "revision": {"clientId": n.CLIENT_ID, "version": 0},
        "component": {
            "name": PG_NAME,
            "description": "SentinelOne maximum-useful ingestion",
            "parameters": params,
            # NiFi will not resolve a bare {"id": ...}; the nested component is required.
            "inheritedParameterContexts": [
                {"id": GLOBAL_INFRA_PC_ID,
                 "component": {"id": GLOBAL_INFRA_PC_ID, "name": "global-infra"}}
            ],
        },
    }
    PARAM_CONTEXT_ID = n.nifi("POST", "/nifi-api/parameter-contexts", payload)["id"]
    return PARAM_CONTEXT_ID


def invoke_props(url):
    """InvokeHTTP config for SentinelOne: bearer-style ApiToken header, no basic auth."""
    return {
        "HTTP Method": "GET",
        "HTTP URL": url,
        "HTTP/2 Disabled": "True",
        "SSL Context Service": None,
        "Connection Timeout": "10 secs",
        "Socket Read Timeout": "60 secs",
        "Socket Write Timeout": "15 secs",
        "Socket Idle Timeout": "5 mins",
        "Socket Idle Connections": "5",
        "Proxy Configuration Service": None,
        "Request Digest Authentication Enabled": "false",
        "Request Failure Penalization Enabled": "true",
        "Request Body Enabled": "false",
        "Request Content-Encoding": "DISABLED",
        "Request Content-Type": "${mime.type}",
        "Request Date Header Enabled": "True",
        "Response Body Ignored": "false",
        "Response Cache Enabled": "false",
        "Response Cookie Strategy": "DISABLED",
        "Response Generation Required": "false",
        "Response FlowFile Naming Strategy": "RANDOM",
        "Response Header Request Attributes Enabled": "false",
        "Response Redirects Enabled": "True",
    }


def create_fetch(name, x, y, url):
    """InvokeHTTP whose Authorization header is a sensitive dynamic property.

    The property must be declared sensitive before it can reference the sensitive
    AUTH_TOKEN parameter, so it is added in a second call rather than at creation.
    """
    pid = n.create_processor(
        name,
        "org.apache.nifi.processors.standard.InvokeHTTP",
        x, y,
        invoke_props(url),
        ["Original", "Failure", "Retry", "No Retry"],
    )
    set_sensitive_dynamic(pid, ["Authorization"], {"Authorization": "#{AUTH_TOKEN}"})
    return pid


def publish_props(topic, avro=False, reader_id=None, writer_id=None):
    """Kafka publish config. Always sets the 11-field header pattern -- raw and Avro alike."""
    props = dict(n.publish_props(topic, avro, reader_id, writer_id))
    props["FlowFile Attribute Header Pattern"] = STANDARD_HEADER_PATTERN
    # object_id (the composite/natural key), not source_object_id, which can be blank for
    # endpoints with no native id -- a blank Kafka key would break partitioning for those.
    props["Kafka Key"] = "${object_id}"
    props["Kafka Key Attribute Encoding"] = "utf-8"
    return props


def processors():
    return n.processors_by_name()


# ---------------------------------------------------------------------------
# Flow construction
# ---------------------------------------------------------------------------

X_TRIGGER, X_INIT, X_FETCH, X_SPLIT, X_EXTRACT = -700, -380, -60, 260, 580
X_REDACT, X_HASH, X_DETECT, X_RAW_PUB = 900, 1220, 1540, 1860
X_META, X_HASMORE, X_NEXT = 260, 580, 900
ROW_H = 340


def build_entity_lane(ent, idx, trigger_id, run_meta_id):
    """Build one entity's lane: cursor loop -> extract -> [redact] -> hash -> dedupe -> raw publish."""
    e = ent["entity"]
    y = idx * ROW_H
    created = {}

    # Where this lane draws its input: the run trigger for a root entity, or the parent
    # entity's extract (optionally via its child gate) for a child entity.
    if ent.get("parent"):
        parent = ent["parent"]
        if ent.get("via_gate"):
            feed_name = f"{SOURCE_INSTANCE}.{parent}__children_gate"
            feed_rel = "recent"
        else:
            feed_name = f"{SOURCE_INSTANCE}.{parent}__extract"
            feed_rel = "matched"
        feed_id = processors()[feed_name]["id"]
    else:
        feed_name, feed_rel, feed_id = f"{SOURCE_INSTANCE}.maximum__run_metadata", "success", run_meta_id

    if ent["paged"]:
        url = f"#{{SOURCE_API_BASE}}{ent['path']}?limit=#{{CURSOR_LIMIT}}&cursor=${{cursor}}{ent['query']}"
        init = n.create_processor(
            f"{SOURCE_INSTANCE}.{e}__list__init_cursor",
            "org.apache.nifi.processors.attributes.UpdateAttribute",
            X_INIT, y,
            {"Store State": "Do not store state", "cursor": ""},
            [],
        )
        fetch = create_fetch(f"{SOURCE_INSTANCE}.{e}__list__fetch", X_FETCH, y, url)
        split = n.create_processor(
            f"{SOURCE_INSTANCE}.{e}__list__split",
            "org.apache.nifi.processors.standard.SplitJson",
            X_SPLIT, y,
            {"JsonPath Expression": ent["split"], "Max String Length": "20 MB",
             "Null Value Representation": "empty string"},
            ["failure"],
        )
        page_meta = n.create_processor(
            f"{SOURCE_INSTANCE}.{e}__list__page_meta",
            "org.apache.nifi.processors.standard.EvaluateJsonPath",
            X_META, y + 150,
            {"Destination": "flowfile-attribute", "Return Type": "auto-detect",
             "Max String Length": "20 MB", "Null Value Representation": "empty string",
             "Path Not Found Behavior": "ignore", "next_cursor": "$.pagination.nextCursor"},
            ["failure"],
        )
        has_more = n.create_processor(
            f"{SOURCE_INSTANCE}.{e}__list__has_more",
            "org.apache.nifi.processors.standard.RouteOnAttribute",
            X_HASMORE, y + 150,
            {"Routing Strategy": "Route to Property name",
             "has_more": "${next_cursor:trim():isEmpty():not()}"},
            ["unmatched"],
        )
        next_cursor = n.create_processor(
            f"{SOURCE_INSTANCE}.{e}__list__next_cursor",
            "org.apache.nifi.processors.attributes.UpdateAttribute",
            X_NEXT, y + 150,
            {"Store State": "Do not store state", "cursor": "${next_cursor}"},
            [],
        )
        n.create_connection(feed_id, feed_name,
                            init, f"{SOURCE_INSTANCE}.{e}__list__init_cursor", [feed_rel])
        n.create_connection(init, f"{SOURCE_INSTANCE}.{e}__list__init_cursor",
                            fetch, f"{SOURCE_INSTANCE}.{e}__list__fetch", ["success"])
        n.create_connection(fetch, f"{SOURCE_INSTANCE}.{e}__list__fetch",
                            split, f"{SOURCE_INSTANCE}.{e}__list__split", ["Response"])
        n.create_connection(split, f"{SOURCE_INSTANCE}.{e}__list__split",
                            page_meta, f"{SOURCE_INSTANCE}.{e}__list__page_meta", ["original"])
        n.create_connection(page_meta, f"{SOURCE_INSTANCE}.{e}__list__page_meta",
                            has_more, f"{SOURCE_INSTANCE}.{e}__list__has_more", ["matched", "unmatched"])
        n.create_connection(has_more, f"{SOURCE_INSTANCE}.{e}__list__has_more",
                            next_cursor, f"{SOURCE_INSTANCE}.{e}__list__next_cursor", ["has_more"])
        n.create_connection(next_cursor, f"{SOURCE_INSTANCE}.{e}__list__next_cursor",
                            fetch, f"{SOURCE_INSTANCE}.{e}__list__fetch", ["success"])
        record_src, record_src_name, record_rel = split, f"{SOURCE_INSTANCE}.{e}__list__split", "split"
        created.update(init=init, fetch=fetch, split=split, page_meta=page_meta,
                       has_more=has_more, next_cursor=next_cursor)
    else:
        # Single-object endpoint rooted from a parent entity's extract step.
        # Single-response endpoint: no cursor block in the payload. Split only if the records
        # arrive as an array (activity_type); site_policy is a lone object and is left whole.
        url = f"#{{SOURCE_API_BASE}}{ent['path']}{('?' + ent['query'].lstrip('&')) if ent['query'] else ''}"
        fetch = create_fetch(f"{SOURCE_INSTANCE}.{e}__fetch", X_FETCH, y, url)
        n.create_connection(feed_id, feed_name, fetch, f"{SOURCE_INSTANCE}.{e}__fetch", [feed_rel])
        record_src, record_src_name, record_rel = fetch, f"{SOURCE_INSTANCE}.{e}__fetch", "Response"
        created.update(fetch=fetch)
        if ent.get("split"):
            split = n.create_processor(
                f"{SOURCE_INSTANCE}.{e}__split",
                "org.apache.nifi.processors.standard.SplitJson",
                X_SPLIT, y,
                {"JsonPath Expression": ent["split"], "Max String Length": "20 MB",
                 "Null Value Representation": "empty string"},
                ["failure", "original"],
            )
            n.create_connection(fetch, f"{SOURCE_INSTANCE}.{e}__fetch",
                                split, f"{SOURCE_INSTANCE}.{e}__split", ["Response"])
            record_src, record_src_name, record_rel = split, f"{SOURCE_INSTANCE}.{e}__split", "split"
            created.update(split=split)

    if ent["extract"]:
        extract_props = {
            "Destination": "flowfile-attribute", "Return Type": "auto-detect",
            "Max String Length": "20 MB", "Null Value Representation": "empty string",
            "Path Not Found Behavior": "ignore",
        }
        extract_props.update(ent["extract"])
        if ent.get("updated_at_path"):
            extract_props["s1_updated_at"] = ent["updated_at_path"]
        extract = n.create_processor(
            f"{SOURCE_INSTANCE}.{e}__extract",
            "org.apache.nifi.processors.standard.EvaluateJsonPath",
            X_EXTRACT, y,
            extract_props,
            ["failure", "unmatched"],
        )
        n.create_connection(record_src, record_src_name, extract, f"{SOURCE_INSTANCE}.{e}__extract", [record_rel])
        upstream, upstream_name, upstream_rel = extract, f"{SOURCE_INSTANCE}.{e}__extract", "matched"
        created.update(extract=extract)

        if ent.get("child_gate"):
            gate = n.create_processor(
                f"{SOURCE_INSTANCE}.{e}__children_gate",
                "org.apache.nifi.processors.standard.RouteOnAttribute",
                X_EXTRACT, y + 150,
                {"Routing Strategy": "Route to Property name", "recent": ent["child_gate"]},
                ["unmatched"],
            )
            n.create_connection(extract, f"{SOURCE_INSTANCE}.{e}__extract",
                                gate, f"{SOURCE_INSTANCE}.{e}__children_gate", ["matched"])
            created.update(gate=gate)
    else:
        # site_policy: single object, no IDs to pull out. It inherits s1_site_id / s1_site_name
        # from the parent site FlowFile, which InvokeHTTP carries onto the Response.
        upstream, upstream_name, upstream_rel = record_src, record_src_name, record_rel

    if ent["redact"]:
        redact = n.create_processor(
            f"{SOURCE_INSTANCE}.{e}__redact_secrets",
            "org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
            X_REDACT, y,
            {"Script Body": REDACT_SCRIPT, "Failure Strategy": "rollback",
             "UNWRAP_ROOT": ent.get("unwrap_root") or "${literal('')}"},
            ["failure"],
        )
        n.create_connection(upstream, upstream_name, redact, f"{SOURCE_INSTANCE}.{e}__redact_secrets", [upstream_rel])
        upstream, upstream_name, upstream_rel = redact, f"{SOURCE_INSTANCE}.{e}__redact_secrets", "success"
        created.update(redact=redact)

    hash_proc = n.create_processor(
        f"{SOURCE_INSTANCE}.{e}__raw__dedupe_hash",
        "org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
        X_HASH, y,
        {
            "Script Body": DEDUPE_HASH_SCRIPT,
            "Failure Strategy": "rollback",
            "SOURCE_PLATFORM": "sentinelone",
            "SOURCE_OBJECT_TYPE": e,
            "OBJECT_ID": ent["object_id"],
            "COMPOSITE_OBJECT_ID": ent["composite_id"],
            # ExecuteGroovyScript rejects empty dynamic properties, so blanks are expressed
            # as ${literal('')} -- the same convention build_fortisiem_maximum_useful.py uses.
            "CUSTOMER_TENANT_ORGANIZATION": ent["customer"] or "${literal('')}",
            "DEFAULT_CUSTOMER": "#{DEFAULT_CUSTOMER}",
            "SOURCE_EVENT_UPDATE_TIMESTAMP": ent["update_ts"] or "${literal('')}",
            "API_ENDPOINT_EXPORT_QUERY_IDENTITY": ent["api_identity"],
            "CURSOR_WINDOW": ent.get("cursor_window") or "${literal('')}",
            "EXCLUDE_FIELDS": ent.get("exclude_fields") or "${literal('')}",
        },
        ["failure"],
    )
    detect = n.create_processor(
        f"{SOURCE_INSTANCE}.{e}__raw__dedupe_detect",
        "org.apache.nifi.processors.standard.DetectDuplicate",
        X_DETECT, y,
        {
            "Cache Entry Identifier": "${dedupe.key}",
            "Cache The Entry Identifier": "true",
            "Age Off Duration": "24 hours",
            "Distributed Cache Service": DMC_ID,
        },
        ["duplicate", "failure"],
    )
    raw_pub = n.create_processor(
        f"{SOURCE_INSTANCE}.{e}__raw__publish",
        "org.apache.nifi.kafka.processors.PublishKafka",
        X_RAW_PUB, y,
        publish_props(f"bronze.{SOURCE_INSTANCE}.{e}__raw"),
        ["success", "failure"],
    )
    n.create_connection(upstream, upstream_name, hash_proc, f"{SOURCE_INSTANCE}.{e}__raw__dedupe_hash", [upstream_rel])
    n.create_connection(hash_proc, f"{SOURCE_INSTANCE}.{e}__raw__dedupe_hash",
                        detect, f"{SOURCE_INSTANCE}.{e}__raw__dedupe_detect", ["success"])
    n.create_connection(detect, f"{SOURCE_INSTANCE}.{e}__raw__dedupe_detect",
                        raw_pub, f"{SOURCE_INSTANCE}.{e}__raw__publish", ["non-duplicate"])
    created.update(hash=hash_proc, detect=detect, raw_pub=raw_pub)
    return created


DMC_ID = None


def build_raw():
    global DMC_ID
    pcid = ensure_param_context()
    n.REFERENCE_PARAM_CONTEXT_ID = pcid
    n.pg_id()
    n.stop_all()

    DMC_ID = ensure_controller_service(
        f"{SOURCE_INSTANCE}.maximum__dedupe__cache",
        "org.apache.nifi.redis.service.RedisDistributedMapCacheClientService",
        {"Redis Connection Pool": REDIS_POOL_ID, "TTL": "24 hours"},
    )

    n.create_label(
        "SentinelOne maximum-useful ingestion. NiFi-native cursor pagination "
        "(init_cursor -> fetch -> split -> page_meta -> has_more -> next_cursor). "
        "Single 2 hour trigger. All 10 standard fields ship as Kafka headers on both the raw "
        "and .avro topics, and are injected into the Avro message value by the normalizer.",
        X_TRIGGER, -260, 1000, 130,
    )

    trigger = n.create_processor(
        f"{SOURCE_INSTANCE}.maximum__trigger",
        "org.apache.nifi.processors.standard.GenerateFlowFile",
        X_TRIGGER, 0,
        {"File Size": "0B", "Batch Size": "1", "Data Format": "Text",
         "Unique FlowFiles": "false", "Custom Text": "sentinelone-maximum-trigger"},
        [],
        scheduling_period="6 hours",
        scheduling_strategy="TIMER_DRIVEN",
    )
    run_meta = n.create_processor(
        f"{SOURCE_INSTANCE}.maximum__run_metadata",
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        X_TRIGGER + 320, 0,
        {
            "Store State": "Do not store state",
            # None removes the property from an existing processor (NiFi merges properties
            # dicts and treats a null value as "delete this dynamic property"). extraction_timestamp
            # is dropped in favor of ingest_ts, which is stamped per record downstream instead.
            "extraction_timestamp": None,
            "ingestion_run_batch_identity": "sentinelone-max-${now():toNumber()}-${uuid}",
            # Bounded incremental windows, wider than the 2h schedule.
            "window_24h": "${now():toNumber():minus(86400000):format(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\", \"GMT\")}",
            "window_4h": "${now():toNumber():minus(14400000):format(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\", \"GMT\")}",
            # Epoch form for the threat child gate: NiFi's gt() is numeric-only, so the
            # ISO strings above cannot be compared directly.
            "window_4h_epoch": "${now():toNumber():minus(14400000)}",
        },
        [],
    )
    n.create_connection(trigger, f"{SOURCE_INSTANCE}.maximum__trigger",
                        run_meta, f"{SOURCE_INSTANCE}.maximum__run_metadata", ["success"])

    # S1_ENTITY_ALLOWLIST scopes a build to a subset of ENTITIES (comma-separated names) --
    # used when deploying into a flow that intentionally holds fewer entities than the full
    # module-level list defines, so build-raw does not silently add the rest.
    _active = active_entities()
    if os.environ.get("S1_ENTITY_ALLOWLIST"):
        names = {x.strip() for x in os.environ["S1_ENTITY_ALLOWLIST"].split(",") if x.strip()}
        missing = names - {e["entity"] for e in _active}
        if missing:
            raise RuntimeError(f"S1_ENTITY_ALLOWLIST names not found in ENTITIES: {sorted(missing)}")

    # Roots before children: a child lane connects to a processor its parent created, so the
    # parent's extract (and child gate) must already exist.
    roots = [e for e in _active if not e.get("parent")]
    children = [e for e in _active if e.get("parent")]
    for idx, ent in enumerate(roots + children):
        build_entity_lane(ent, idx, trigger, run_meta)

    for idx, (entity, reason) in enumerate(SCAFFOLD_ENTITIES):
        n.create_label(
            f"Scaffold only - {entity}: {reason}. Not built in this phase.",
            X_TRIGGER + (idx % 3) * 640, 3200 + (idx // 3) * 110, 600, 80,
        )

    n.stop_all()
    return inspect()


# ---------------------------------------------------------------------------
# Schema / Avro / connectors
# ---------------------------------------------------------------------------


def topic_for(entity):
    return f"bronze.{SOURCE_INSTANCE}.{entity}__raw"


def active_entities():
    """ENTITIES filtered by S1_ENTITY_ALLOWLIST when set -- see build_raw() for why."""
    allowlist = os.environ.get("S1_ENTITY_ALLOWLIST")
    if not allowlist:
        return ENTITIES
    names = {x.strip() for x in allowlist.split(",") if x.strip()}
    return [e for e in ENTITIES if e["entity"] in names]


def infer_register():
    os.makedirs("generated_schemas", exist_ok=True)
    out = {}
    for ent in active_entities():
        e = ent["entity"]
        topic = topic_for(e)
        try:
            vals = n.fetch_topic_values(topic, SCHEMA_SAMPLE_LIMIT)
        except Exception as exc:
            out[e] = {"status": "no_topic_or_samples", "topic": topic, "error": str(exc)[:300]}
            continue
        samples = []
        for val in vals:
            try:
                samples.append(add_standard_value_fields(n.normalize_json(json.loads(val), 0), e))
            except Exception:
                pass
        if not samples:
            out[e] = {"status": "no_samples", "topic": topic}
            continue
        schema = n.schema_from_samples(samples, f"{SOURCE_INSTANCE}_{e}_raw_avro", f"bronze.{SOURCE_INSTANCE}")
        subject = f"{topic}.avro-value"
        path = os.path.join("generated_schemas", f"{subject}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(schema, f, indent=2, ensure_ascii=False)
        reg = n.register_schema(subject, schema)
        out[e] = {
            "status": "registered", "topic": topic, "avro_topic": f"{topic}.avro",
            "subject": subject, "schema_file": path, "samples": len(samples),
            "schema_id": reg.get("id"), "version": reg.get("version"),
            "fields": len(schema.get("fields", [])),
        }
    return out


def add_avro():
    n.stop_all()
    result = {}
    for idx, ent in enumerate(active_entities()):
        e = ent["entity"]
        subject = f"{topic_for(e)}.avro-value"
        source = processors().get(f"{SOURCE_INSTANCE}.{e}__raw__dedupe_hash")
        if not source:
            result[e] = {"status": "missing_source"}
            continue
        if not n.schema_subject_exists(subject):
            result[e] = {"status": "skipped_no_schema", "subject": subject}
            continue
        reader = ensure_controller_service(
            f"{SOURCE_INSTANCE}.{e}__avro_json_reader",
            "org.apache.nifi.json.JsonTreeReader",
            {"Schema Access Strategy": "schema-name", "Schema Registry": n.SCHEMA_REGISTRY_SERVICE_ID,
             "Schema Name": subject, "Schema Version": None, "Schema Branch": None,
             "Schema Text": "${avro.schema}", "Schema Reference Reader": None,
             "Schema Inference Cache": None, "Starting Field Strategy": "ROOT_NODE",
             "Starting Field Name": None, "Schema Application Strategy": "SELECTED_PART"},
        )
        writer = ensure_controller_service(
            f"{SOURCE_INSTANCE}.{e}__avro_writer",
            "org.apache.nifi.avro.AvroRecordSetWriter",
            {"Schema Write Strategy": "schema-reference-writer",
             "Schema Reference Writer": n.SCHEMA_REF_WRITER_SERVICE_ID,
             "Schema Access Strategy": "schema-name", "Schema Registry": n.SCHEMA_REGISTRY_SERVICE_ID,
             "Schema Name": subject, "Schema Version": None, "Schema Branch": None,
             "Schema Text": "${avro.schema}", "Schema Reference Reader": None},
        )
        y = idx * ROW_H
        normalizer = n.create_processor(
            f"{SOURCE_INSTANCE}.{e}__avro__normalize_json",
            "org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
            X_RAW_PUB + 340, y,
            {"Script Body": JSON_NORMALIZE_SCRIPT, "Failure Strategy": "rollback"},
            ["failure"],
        )
        pub = n.create_processor(
            f"{SOURCE_INSTANCE}.{e}__avro__publish",
            "org.apache.nifi.kafka.processors.PublishKafka",
            X_RAW_PUB + 680, y,
            publish_props(f"{topic_for(e)}.avro", True, reader, writer),
            ["success", "failure"],
        )
        # Branch off the dedupe detector so raw and Avro see the identical deduped stream.
        detect = processors()[f"{SOURCE_INSTANCE}.{e}__raw__dedupe_detect"]
        n.create_connection(detect["id"], f"{SOURCE_INSTANCE}.{e}__raw__dedupe_detect",
                            normalizer, f"{SOURCE_INSTANCE}.{e}__avro__normalize_json", ["non-duplicate"])
        n.create_connection(normalizer, f"{SOURCE_INSTANCE}.{e}__avro__normalize_json",
                            pub, f"{SOURCE_INSTANCE}.{e}__avro__publish", ["success"])
        result[e] = {"status": "added", "subject": subject, "avro_topic": f"{topic_for(e)}.avro"}
    n.stop_all()
    return result


def connector_config(entity):
    topic = f"{topic_for(entity)}.avro"
    name = f"{topic}__iceberg"
    safe_group = f"cg-iceberg-bronze-{SOURCE_INSTANCE.replace('_', '-')}-{entity.replace('_', '-')}"
    return name, {
        "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
        "tasks.max": "1",
        "topics": topic,
        "iceberg.tables": f"{SOURCE_INSTANCE}.{entity}",
        "iceberg.tables.auto-create-enabled": "true",
        "iceberg.tables.evolve-schema-enabled": "true",
        "iceberg.tables.schema-force-optional": "true",
        "iceberg.control.topic": "control-iceberg",
        "iceberg.control.group-id-prefix": safe_group,
        "iceberg.control.commit.interval-ms": "60000",
        "iceberg.catalog": "polaris",
        "iceberg.catalog.type": "rest",
        "iceberg.catalog.uri": os.environ.get("POLARIS_URI", "https://polaris.datapasc.com/api/catalog"),
        "iceberg.catalog.warehouse": "bronze",
        "iceberg.catalog.rest.auth.type": "oauth2",
        "iceberg.catalog.credential": os.environ["POLARIS_CREDENTIAL"],
        "iceberg.catalog.scope": "PRINCIPAL_ROLE:ALL",
        "iceberg.catalog.oauth2-server-uri": os.environ.get(
            "POLARIS_OAUTH_URI", "https://polaris.datapasc.com/api/catalog/v1/oauth/tokens"),
        "iceberg.catalog.token-refresh-enabled": "true",
        "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "iceberg.catalog.s3.endpoint": os.environ.get("OZONE_S3_ENDPOINT", "https://ozones3g.datapasc.com"),
        "iceberg.catalog.s3.access-key-id": os.environ["OZONE_ACCESS_KEY"],
        "iceberg.catalog.s3.secret-access-key": os.environ["OZONE_SECRET_KEY"],
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
        # 'none', not 'all'. The Connect DLQ only captures converter/transform errors, not sink
        # write errors, so errors.tolerance=all silently swallows Iceberg write failures while
        # offsets advance -- the suspected cause of the FortiSIEM "one run only" defect.
        # rapid7_asyad uses 'none' and is the only flow that does not exhibit it.
        "errors.tolerance": "none",
        "errors.log.enable": "true",
        "errors.log.include.messages": "true",
        "errors.deadletterqueue.topic.name": f"dlq.{topic}.iceberg",
        "errors.deadletterqueue.context.headers.enable": "true",
        "errors.deadletterqueue.topic.replication.factor": "1",
    }


def upsert_connectors():
    out = {}
    for ent in active_entities():
        e = ent["entity"]
        subject = f"{topic_for(e)}.avro-value"
        if not n.schema_subject_exists(subject):
            out[e] = {"status": "skipped_missing_schema", "subject": subject}
            continue
        name, cfg = connector_config(e)
        url = f"{KAFKA_CONNECT_BASE}/connectors/{urllib.parse.quote(name, safe='')}/config"
        r = requests.put(url, json=cfg, verify=False, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 404:
            r = requests.post(f"{KAFKA_CONNECT_BASE}/connectors", json={"name": name, "config": cfg},
                              verify=False, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        out[name] = {"status": r.status_code, "body": r.text[:300]}
    return out


# ---------------------------------------------------------------------------
# Run / inspect / verify
# ---------------------------------------------------------------------------


CLEAR_REDIS_SCRIPT = r'''
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
    def result = [pattern: pattern, matched: matched, deleted: deleted]
    log.warn('SENTINELONE_REDIS_CLEAR ' + JsonOutput.toJson(result))
    flowFile = session.write(flowFile, { os -> os.write(JsonOutput.toJson(result).getBytes('UTF-8')) } as OutputStreamCallback)
    session.transfer(flowFile, REL_SUCCESS)
} catch (Throwable t) {
    flowFile = session.putAttribute(flowFile, 'redis.clear.error', t.toString())
    session.transfer(flowFile, REL_FAILURE)
} finally { if (conn != null) conn.close() }
'''


def clear_redis(pattern=None):
    """Clear this source's dedupe keys, never a global flush.

    Defaults to `sentinelone:*`. Pass a narrower pattern (or set S1_REDIS_KEY_PATTERN) to
    reset a single entity -- needed when an Avro branch is added after its raw records were
    already deduped, since a full clear would republish every entity and double the rows
    already committed to Iceberg.
    """
    pattern = pattern or os.environ.get("S1_REDIS_KEY_PATTERN") or f"{SOURCE_INSTANCE}:*"
    trig = n.create_processor(
        f"{SOURCE_INSTANCE}.maximum__admin_clear_redis__trigger",
        "org.apache.nifi.processors.standard.GenerateFlowFile",
        X_TRIGGER, 3000,
        {"File Size": "0B", "Batch Size": "1", "Data Format": "Text", "Unique FlowFiles": "false"},
        [],
        scheduling_period="2 hours",
    )
    proc = n.create_processor(
        f"{SOURCE_INSTANCE}.maximum__admin_clear_redis__run",
        "org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
        X_TRIGGER + 320, 3000,
        {"Script Body": CLEAR_REDIS_SCRIPT, "REDIS_POOL_ID": REDIS_POOL_ID,
         "KEY_PATTERN": pattern},
        ["success", "failure"],
    )
    n.create_connection(trig, f"{SOURCE_INSTANCE}.maximum__admin_clear_redis__trigger",
                        proc, f"{SOURCE_INSTANCE}.maximum__admin_clear_redis__run", ["success"])
    n.set_processor_state(proc, "RUNNING")
    n.set_processor_state(trig, "RUNNING")
    time.sleep(3)
    n.stop_processor(trig)
    time.sleep(6)
    n.stop_processor(proc)
    ent = n.nifi("GET", f"/nifi-api/processors/{proc}")
    bulletins = []
    for item in ent.get("bulletins") or []:
        bb = item.get("bulletin") or item
        if bb and "SENTINELONE_REDIS_CLEAR" in (bb.get("message") or ""):
            bulletins.append({k: bb.get(k) for k in ["level", "message", "timestamp"]})
    return {"pattern": pattern, "bulletins": bulletins[-3:]}


def start_all_except_trigger():
    for name, p in processors().items():
        if name.endswith("maximum__trigger") or "__admin_" in name or "__test_" in name:
            continue
        ent = n.nifi("GET", f"/nifi-api/processors/{p['id']}")
        if ent["component"].get("validationStatus") == "VALID":
            n.set_processor_state(p["id"], "RUNNING")


def run_once(wait_seconds=420):
    start_all_except_trigger()
    trig = processors()[f"{SOURCE_INSTANCE}.maximum__trigger"]
    n.set_processor_state(trig["id"], "RUNNING")
    time.sleep(4)
    n.stop_processor(trig["id"])
    deadline = time.time() + wait_seconds
    last = []
    while time.time() < deadline:
        last = n.queued_summary()
        if not last:
            break
        time.sleep(10)
    n.stop_all()
    return {"queued_remaining": last, "inspect": inspect()}


def inspect():
    out = {"process_group": {"id": n.pg_id(), "name": PG_NAME}, "processors": {}, "topics": []}
    invalid = []
    for name, p in processors().items():
        c = p["component"]
        out["processors"][name] = {
            "id": p["id"], "state": c.get("state"),
            "validation": c.get("validationStatus"), "validation_errors": c.get("validationErrors"),
        }
        if c.get("validationStatus") != "VALID":
            invalid.append({"name": name, "errors": c.get("validationErrors")})
    for ent in active_entities():
        out["topics"].append(topic_for(ent["entity"]))
        out["topics"].append(f"{topic_for(ent['entity'])}.avro")
    out["invalid"] = invalid
    out["processor_count"] = len(out["processors"])
    out["queued"] = n.queued_summary()
    return out


def verify_kafka():
    """Assert per topic: messages exist, 10 headers present, 10 fields inside the Avro value."""
    required = set(STANDARD_VALUE_FIELDS)
    result = {}
    for ent in active_entities():
        e = ent["entity"]
        for topic in [topic_for(e), f"{topic_for(e)}.avro"]:
            try:
                msgs = fetch_topic_messages(topic, 3)
                rec = {"samples_seen": len(msgs), "has_data": bool(msgs)}
                if msgs:
                    hdrs = set((msgs[0].get("headers") or {}).keys())
                    rec["headers_present"] = len(required & hdrs)
                    rec["headers_missing"] = sorted(required - hdrs)
                    if topic.endswith(".avro"):
                        try:
                            val = msgs[0].get("value")
                            obj = json.loads(val) if isinstance(val, str) else val
                            rec["value_fields_present"] = len([k for k in required if isinstance(obj, dict) and k in obj])
                            rec["value_fields_missing"] = sorted([k for k in required if not isinstance(obj, dict) or k not in obj])
                        except Exception as exc:
                            rec["parse_error"] = str(exc)[:200]
                result[topic] = rec
            except Exception as exc:
                result[topic] = {"error": str(exc)[:300]}
    return result


def fetch_topic_messages(topic, limit=5):
    """Like n.fetch_topic_values but keeps headers so the 10-header rule can be asserted."""
    s = n.kafbat_session()
    url = f"{n.KAFBAT_BASE}/api/clusters/local/topics/{urllib.parse.quote(topic, safe='')}/messages/v2"
    r = s.get(url, params={"mode": "LATEST", "limit": str(limit)}, timeout=60)
    r.raise_for_status()
    out = []
    for ev in n.parse_sse(r.text):
        if isinstance(ev, dict) and ev.get("type") == "MESSAGE" and isinstance(ev.get("message"), dict):
            out.append(ev["message"])
    return out


def main():
    requests.packages.urllib3.disable_warnings()
    cmd = sys.argv[1] if len(sys.argv) > 1 else "inspect"
    if cmd == "build-raw":
        print(json.dumps(build_raw(), indent=2))
    elif cmd == "run-once":
        print(json.dumps(run_once(int(os.environ.get("WAIT_SECONDS", "420"))), indent=2))
    elif cmd == "infer-register":
        print(json.dumps(infer_register(), indent=2))
    elif cmd == "add-avro":
        print(json.dumps(add_avro(), indent=2))
    elif cmd == "connectors":
        print(json.dumps(upsert_connectors(), indent=2))
    elif cmd == "verify-kafka":
        print(json.dumps(verify_kafka(), indent=2))
    elif cmd == "clear-redis":
        print(json.dumps(clear_redis(), indent=2))
    elif cmd == "stop":
        n.stop_all()
        print(json.dumps({"stopped": True, "inspect": inspect()}, indent=2))
    else:
        print(json.dumps(inspect(), indent=2))


if __name__ == "__main__":
    main()
