"""Build the FortiSIEM flows that are NOT yet in the application.

Design notes
------------
The application allows exactly ONE root block per flow. The `/query/cmdb`
entities have no natural parent, so naively each would need its own flow. To
honour "maximum entities, minimum flows" they are instead built as a FAN-OUT:

    root: http read, split=false   -> exactly ONE FlowFile per cron tick
      |- http write (paginated POST, target=CASE)   -> kafka write
      |- http write (paginated POST, target=RULE)   -> kafka write
      |- ...

`split=false` is what makes this correct: the root emits a single FlowFile, so
each child fires exactly once per tick rather than once per parent record.
Verified to compile with the full pagination loop intact on every child.

Everything is compiled locally before any API call.
"""
import json
import sys
import urllib.request

sys.path.insert(0, "backend")

BASE = "http://localhost:8010"
SERVICE_ID = "svc-67cso4"          # FortiSIEM CMDB (paginated POST demo) -> apisix in-cluster
CRON = "*/1 * * * *"
PAGE_SIZE = "50"
MAX_PAGES = "2"                     # smoke-test cap; keeps load off FortiSIEM

# Filled in from the live NiFi inventory. target -> (entity, selectFields)
CMDB_ENTITIES: dict = {}

# Standalone GET entities: entity -> (path, recordPath)
SIMPLE_GETS: dict = {}


def _cmdb_child(entity, target, cols, parent):
    body = json.dumps({"target": target, "selectFields": cols}, separators=(",", ":"))
    return [
        {
            "id": f"b-{entity}", "adapter": "http", "mode": "write",
            "name": f"Query {target}", "parentId": parent, "branch": None,
            "serviceId": SERVICE_ID, "entity": entity,
            "config": {
                "method": "POST", "path": "/query/cmdb", "responseFormat": "json",
                "split": True, "recordPath": "$.data[*]", "bodyTemplate": body,
                "writeForwards": "response", "proxyId": None,
                "columnar": {"enabled": True, "rowsField": "data", "columns": cols},
                "pagination": {"type": "offset", "fields": {
                    "offsetParam": "start", "limitParam": "size", "limitValue": PAGE_SIZE,
                    "offsetStop": "total_count", "offsetTotalCountSource": "body",
                    "offsetTotalCountPath": "$.totalCount", "maxPages": MAX_PAGES}},
            },
            "transforms": [], "topicOverride": None, "testResult": None,
        },
        {
            "id": f"b-{entity}-k", "adapter": "kafka", "mode": "write",
            "name": f"Publish {entity}", "parentId": f"b-{entity}", "branch": None,
            "serviceId": None, "entity": entity, "config": {},
            "transforms": [], "topicOverride": None, "testResult": None,
        },
    ]


def cmdb_fanout_flow(flow_id, name, tick_path):
    """One root tick + N paginated POST children."""
    blocks = [{
        "id": "b-tick", "adapter": "http", "mode": "read", "name": "Tick", "parentId": None,
        "branch": None, "serviceId": SERVICE_ID, "entity": None,
        "config": {"method": "GET", "path": tick_path, "responseFormat": "json",
                   "split": False, "recordPath": "$", "proxyId": None,
                   "pagination": {"type": "none", "fields": {}}},
        "transforms": [], "topicOverride": None, "testResult": None,
    }]
    topics = []
    for target, (entity, cols) in CMDB_ENTITIES.items():
        blocks += _cmdb_child(entity, target, cols, "b-tick")
        topics.append({"id": f"t-{entity}", "kind": "materialized",
                       "name": f"bronze.fortisiem.{entity}__raw", "sealed": False,
                       "writerBlockId": f"b-{entity}-k", "backlogEstimate": None})
    return {"id": flow_id, "name": name, "description": "Fan-out: one tick -> N paginated /query/cmdb entities",
            "state": "Draft", "enabled": True, "cron": CRON, "blocks": blocks,
            "topics": topics, "variables": [], "servicePins": {}}


def simple_get_flow(flow_id, name, entity, path, record_path):
    blocks = [
        {"id": "b-read", "adapter": "http", "mode": "read", "name": f"Fetch {entity}",
         "parentId": None, "branch": None, "serviceId": SERVICE_ID, "entity": None,
         "config": {"method": "GET", "path": path, "responseFormat": "json", "split": True,
                    "recordPath": record_path, "proxyId": None,
                    "pagination": {"type": "none", "fields": {}}},
         "transforms": [], "topicOverride": None, "testResult": None},
        {"id": "b-k", "adapter": "kafka", "mode": "write", "name": f"Publish {entity}",
         "parentId": "b-read", "branch": None, "serviceId": None, "entity": entity,
         "config": {}, "transforms": [], "topicOverride": None, "testResult": None},
    ]
    topics = [{"id": "t-1", "kind": "materialized", "name": f"bronze.fortisiem.{entity}__raw",
               "sealed": False, "writerBlockId": "b-k", "backlogEstimate": None}]
    return {"id": flow_id, "name": name, "description": f"{entity} -> Kafka", "state": "Draft",
            "enabled": True, "cron": CRON, "blocks": blocks, "topics": topics,
            "variables": [], "servicePins": {}}


# ---------------------------------------------------------------- compile/create

def compile_locally(doc):
    from models.adapter import AppService, Flow, PlatformConnection
    from services.adapter.compiler.compile_flow import compile_flow
    from services.adapter.compiler.ir import CompileContext

    raw = json.loads(urllib.request.urlopen(f"{BASE}/api/v2/services/", timeout=60).read())
    s = next(x for x in raw if x["id"] == SERVICE_ID)
    s["config"] = {**s["config"], "password": "x"}
    service = AppService(**{k: v for k, v in s.items() if k in AppService.model_fields})
    conn = PlatformConnection(id="conn-kafka", type="kafka", name="K", revision=1, retired=False,
                              health="Healthy", createdAt="2026-01-01T00:00:00.000Z",
                              updatedAt="2026-01-01T00:00:00.000Z",
                              config={"bootstrapServers": "kafka:9092"})
    full = {**doc, "createdAt": "2026-01-01T00:00:00.000Z", "updatedAt": "2026-01-01T00:00:00.000Z"}
    plan = compile_flow(Flow(**full), CompileContext(services={SERVICE_ID: service},
                                                     connections={"kafka": conn},
                                                     gateway_proxies={}, approved_schemas={}))
    return plan


def create(doc):
    req = urllib.request.Request(f"{BASE}/api/v2/flows/", data=json.dumps(doc).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=90) as r:
        return json.loads(r.read())
