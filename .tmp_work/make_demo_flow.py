"""Build the paginated-POST demo flow, compile it locally, then create it via
the application's own v2 API.

Shape is modelled on the existing "FortiSIEM Pagination Test" flow, but points
at the demo service and carries an explicit maxPages cap so the loop cannot
hammer FortiSIEM. Nothing existing is read-modified-written.
"""
import json
import sys
import urllib.request

sys.path.insert(0, "backend")

BASE = "http://localhost:8000"
SERVICE_ID = "svc-67cso4"
FLOW_ID = "flow-pgdemo1"
ENTITY = "cmdb_user"

COLUMNS = ["User_Name", "Group_Name", "Customer_ID", "User_Full_Name", "User_Domain", "Customer_Name"]

BODY = json.dumps({"target": "USER", "selectFields": COLUMNS}, separators=(",", ":"))

flow_doc = {
    "id": FLOW_ID,
    "name": "FortiSIEM POST Pagination Demo",
    "description": "Paginated POST /query/cmdb -> Kafka. Offset pagination, total_count stop, maxPages=3.",
    "state": "Draft",
    "enabled": True,
    "cron": "*/1 * * * *",
    "blocks": [
        {
            "id": "b-cmdb",
            "adapter": "http",
            "mode": "write",
            "name": "Query CMDB (paginated POST)",
            "parentId": None,
            "branch": None,
            "serviceId": SERVICE_ID,
            "entity": ENTITY,
            "config": {
                "method": "POST",
                "path": "/query/cmdb",
                "responseFormat": "json",
                "split": True,
                "recordPath": "$.data[*]",
                "bodyTemplate": BODY,
                "writeForwards": "response",
                "proxyId": None,
                "pagination": {
                    "type": "offset",
                    "fields": {
                        "offsetParam": "start",
                        "limitParam": "size",
                        "limitValue": "50",
                        "offsetStop": "total_count",
                        "offsetTotalCountSource": "body",
                        "offsetTotalCountPath": "$.totalCount",
                        "maxPages": "3",
                    },
                },
                "columnar": {"enabled": True, "rowsField": "data", "columns": COLUMNS},
            },
            "transforms": [],
            "topicOverride": None,
            "testResult": None,
        },
        {
            "id": "b-pub",
            "adapter": "kafka",
            "mode": "write",
            "name": "Publish to Kafka",
            "parentId": "b-cmdb",
            "branch": None,
            "serviceId": None,
            "entity": ENTITY,
            "config": {},
            "transforms": [],
            "topicOverride": None,
            "testResult": None,
        },
    ],
    "topics": [
        {
            "id": "t-pgdemo1",
            "kind": "materialized",
            "name": "raw.fortisiem_post_pagination_demo.cmdb_user",
            "sealed": False,
            "writerBlockId": "b-pub",
            "backlogEstimate": None,
        }
    ],
    "variables": [],
    "servicePins": {},
}


def compile_locally():
    """Fail fast before touching the API."""
    from models.adapter import Flow
    from services.adapter.compiler.compile_flow import compile_flow
    from services.adapter.compiler.ir import CompileContext
    from models.adapter import AppService, PlatformConnection

    svc_raw = json.loads(urllib.request.urlopen(f"{BASE}/api/v2/services/", timeout=30).read())
    svc = next(s for s in svc_raw if s["id"] == SERVICE_ID)
    svc["config"] = {**svc["config"], "password": "x"}
    service = AppService(**{k: v for k, v in svc.items() if k in AppService.model_fields})

    conn = PlatformConnection(
        id="conn-kafka", type="kafka", name="K", revision=1, retired=False, health="Healthy",
        createdAt="2026-01-01T00:00:00.000Z", updatedAt="2026-01-01T00:00:00.000Z",
        config={"bootstrapServers": "kafka:9092"},
    )
    doc = {**flow_doc, "createdAt": "2026-01-01T00:00:00.000Z", "updatedAt": "2026-01-01T00:00:00.000Z"}
    plan = compile_flow(Flow(**doc), CompileContext(services={SERVICE_ID: service},
                                                    connections={"kafka": conn},
                                                    gateway_proxies={}, approved_schemas={}))
    g = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-cmdb")
    print("--- local compile OK: %d processors" % len(g.processors))
    for p in g.processors:
        if p.key in ("init", "render_body", "write", "split", "page_meta", "has_more", "next", "columnar_transform"):
            print("  %s" % p.key)
            for k in ("HTTP URL", "HTTP Method", "Replacement Value", "continue", "JsonPath Expression",
                      "total_count", "offset", "limit", "mime.type", "Jolt Specification"):
                if k in (p.properties or {}):
                    v = str(p.properties[k])
                    print("      %-20s = %s" % (k, v if len(v) < 200 else v[:200] + " ...[truncated]"))


def create():
    req = urllib.request.Request(f"{BASE}/api/v2/flows/", data=json.dumps(flow_doc).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=60) as r:
        out = json.loads(r.read())
    print("--- created flow:", out.get("id"), "| state:", out.get("state"))
    return out


if __name__ == "__main__":
    compile_locally()
    if "--create" in sys.argv:
        create()
