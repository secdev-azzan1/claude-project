"""Flow 2: FortiSIEM Incident — POST /pub/incident, paginated.

Differences from the /query/cmdb family:
  - response is object-shaped, not columnar -> no columnar transform, recordPath `$.data`
  - the body carries a time window. Production builds it from `${page_start}` /
    `${incident_time_from}` attributes; here the window is written inline as NiFi
    EL (`${now():toNumber():minus(86400000)}`). That is deliberate: the
    compiler's `${field}` -> attribute promotion only matches BARE tokens like
    `${foo}`, so a function-call expression passes through untouched and NiFi
    evaluates it per request.
  - `start`/`size` are deliberately ABSENT from the body here — the compiler
    splices them in from the pagination fields (and would raise on a collision).
"""
import json
import sys

sys.path.insert(0, ".tmp_work")
from build_fortisiem_flows import CRON, MAX_PAGES, SERVICE_ID, compile_locally, create  # noqa: E402
from fs_specs import INCIDENT_FIELDS  # noqa: E402

FLOW_ID = "flow-fs-incident"
ENTITY = "incident"

body = (
    '{"filters":{},'
    '"timeFrom":${now():toNumber():minus(86400000)},'
    '"timeTo":${now():toNumber()},'
    '"orderBy":"incidentLastSeen","descending":true,'
    '"fields":' + json.dumps(INCIDENT_FIELDS, separators=(",", ":")) + '}'
)

blocks = [
    {"id": "b-inc", "adapter": "http", "mode": "write", "name": "Query Incidents",
     "parentId": None, "branch": None, "serviceId": SERVICE_ID, "entity": ENTITY,
     "config": {"method": "POST", "path": "/pub/incident", "responseFormat": "json",
                "split": True, "recordPath": "$.data[*]", "bodyTemplate": body,
                "writeForwards": "response", "proxyId": None,
                # Live probe of /pub/incident returned
                #   {"total":1340,"pages":670,"data":[...],"start":0,"sizePerPage":2}
                # so the record path is `$.data[*]` (plain objects, no columnar
                # transform needed) and the page total lives at `$.total`.
                "pagination": {"type": "offset", "fields": {
                    "offsetParam": "start", "limitParam": "size", "limitValue": "50",
                    "offsetStop": "total_count", "offsetTotalCountSource": "body",
                    "offsetTotalCountPath": "$.total", "maxPages": MAX_PAGES}}},
     "transforms": [], "topicOverride": None, "testResult": None},
    {"id": "b-inc-k", "adapter": "kafka", "mode": "write", "name": "Publish incident",
     "parentId": "b-inc", "branch": None, "serviceId": None, "entity": ENTITY,
     "config": {}, "transforms": [], "topicOverride": None, "testResult": None},
]
topics = [{"id": "t-inc", "kind": "materialized", "name": f"bronze.fortisiem.{ENTITY}__raw",
           "sealed": False, "writerBlockId": "b-inc-k", "backlogEstimate": None}]

doc = {"id": FLOW_ID, "name": "FortiSIEM Incident", "description": "Paginated POST /pub/incident -> Kafka",
       "state": "Draft", "enabled": True, "cron": CRON, "blocks": blocks, "topics": topics,
       "variables": [], "servicePins": {}}

plan = compile_locally(doc)
g = next(x for x in plan.rootGroup.childGroups if x.blockId == "b-inc")
print("compiled OK. processors:", [p.key for p in g.processors])
for k in ("write", "render_body"):
    p = next((x for x in g.processors if x.key == k), None)
    if p:
        v = p.properties.get("HTTP URL") or p.properties.get("Replacement Value")
        print("  %-12s %s" % (k, v[:300]))

if "--create" in sys.argv:
    print("CREATED:", create(doc)["id"])
