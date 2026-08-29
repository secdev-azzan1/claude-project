"""Flow 1: "FortiSIEM CMDB Catalog" — 8 entities under ONE root.

The app allows one root per flow, and none of these entities has a natural
parent. Rather than eight flows, the root is a deliberately tiny GET with
`split: false`, so it emits exactly ONE FlowFile per cron tick and every child
therefore fires exactly once (not once per parent record).

  root  GET /pub/lookupTable?size=1   split=false        <- tick only, not published
    |- http write POST /query/cmdb  target=CASE     (paginated) -> kafka
    |- ... REPORT, MONITOR, TASK, RULE, USER                    -> kafka
    |- http read  GET /watchlist/all                            -> kafka
    |- http read  GET /pub/lookupTable?size=1000                -> kafka
"""
import json
import sys

sys.path.insert(0, ".tmp_work")
from build_fortisiem_flows import (BASE, CRON, MAX_PAGES, PAGE_SIZE, SERVICE_ID,  # noqa: E402
                                   compile_locally, create)
from fs_specs import CMDB, SIMPLE_GETS  # noqa: E402

FLOW_ID = "flow-fs-cmdb"

blocks = [{
    "id": "b-tick", "adapter": "http", "mode": "read", "name": "Tick", "parentId": None,
    "branch": None, "serviceId": SERVICE_ID, "entity": None,
    "config": {"method": "GET", "path": "/pub/lookupTable?size=1", "responseFormat": "json",
               "split": False, "recordPath": "$", "proxyId": None,
               "pagination": {"type": "none", "fields": {}}},
    "transforms": [], "topicOverride": None, "testResult": None,
}]
topics = []

for target, (entity, cols) in CMDB.items():
    body = json.dumps({"target": target, "selectFields": cols}, separators=(",", ":"))
    blocks.append({
        "id": f"b-{entity}", "adapter": "http", "mode": "write", "name": f"Query {target}",
        "parentId": "b-tick", "branch": None, "serviceId": SERVICE_ID, "entity": entity,
        "config": {"method": "POST", "path": "/query/cmdb", "responseFormat": "json", "split": True,
                   "recordPath": "$.data[*]", "bodyTemplate": body, "writeForwards": "response",
                   "proxyId": None,
                   "columnar": {"enabled": True, "rowsField": "data", "columns": cols},
                   "pagination": {"type": "offset", "fields": {
                       "offsetParam": "start", "limitParam": "size", "limitValue": PAGE_SIZE,
                       "offsetStop": "total_count", "offsetTotalCountSource": "body",
                       "offsetTotalCountPath": "$.totalCount", "maxPages": MAX_PAGES}}},
        "transforms": [], "topicOverride": None, "testResult": None,
    })
    blocks.append({"id": f"b-{entity}-k", "adapter": "kafka", "mode": "write",
                   "name": f"Publish {entity}", "parentId": f"b-{entity}", "branch": None,
                   "serviceId": None, "entity": entity, "config": {},
                   "transforms": [], "topicOverride": None, "testResult": None})
    topics.append({"id": f"t-{entity}", "kind": "materialized",
                   "name": f"bronze.fortisiem.{entity}__raw", "sealed": False,
                   "writerBlockId": f"b-{entity}-k", "backlogEstimate": None})

for entity, (path, rp) in SIMPLE_GETS.items():
    blocks.append({
        "id": f"b-{entity}", "adapter": "http", "mode": "read", "name": f"Fetch {entity}",
        "parentId": "b-tick", "branch": None, "serviceId": SERVICE_ID, "entity": None,
        "config": {"method": "GET", "path": path, "responseFormat": "json", "split": True,
                   "recordPath": rp, "proxyId": None,
                   "pagination": {"type": "none", "fields": {}}},
        "transforms": [], "topicOverride": None, "testResult": None,
    })
    blocks.append({"id": f"b-{entity}-k", "adapter": "kafka", "mode": "write",
                   "name": f"Publish {entity}", "parentId": f"b-{entity}", "branch": None,
                   "serviceId": None, "entity": entity, "config": {},
                   "transforms": [], "topicOverride": None, "testResult": None})
    topics.append({"id": f"t-{entity}", "kind": "materialized",
                   "name": f"bronze.fortisiem.{entity}__raw", "sealed": False,
                   "writerBlockId": f"b-{entity}-k", "backlogEstimate": None})

doc = {"id": FLOW_ID, "name": "FortiSIEM CMDB Catalog",
       "description": "One tick -> 6 paginated /query/cmdb entities + watchlist + lookup_table",
       "state": "Draft", "enabled": True, "cron": CRON, "blocks": blocks, "topics": topics,
       "variables": [], "servicePins": {}}

plan = compile_locally(doc)
print("compiled OK — %d child groups, entities: %s"
      % (len(plan.rootGroup.childGroups), sorted(t["name"].split(".")[-1] for t in topics)))
for g in plan.rootGroup.childGroups:
    w = next((p for p in g.processors if p.key == "write"), None)
    if w:
        print("   %-28s %s" % (g.blockId, w.properties["HTTP URL"]))

if "--create" in sys.argv:
    out = create(doc)
    print("CREATED:", out["id"], "|", out["name"])
