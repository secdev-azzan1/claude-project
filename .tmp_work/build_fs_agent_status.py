"""Flow 3: FortiSIEM Agent Status — organization -> device -> agent_status.

`/agentStatus/all` REQUIRES `?request=<orgId>,<hostName>` (verified: omitting it
returns a Tomcat error page, and `?request=1` alone returns HTTP 400), so this
entity cannot be a root — it is genuinely a grandchild of the device list.

All three endpoints return XML, not JSON:
  /config/Domain            -> <response><result><domains><domain .../>
  /cmdbDeviceInfo/devices   -> <devices><device><organization id=".." name=".."/>
                                 <accessIp/><name/>...
So responseFormat is `xml` throughout; the compiler runs ConvertRecord
(XMLReader -> JsonRecordSetWriter) first, and every path/record expression below
is written against the POST-conversion JSON shape.

The device record carries BOTH values the grandchild needs — `organization.id`
and `name` — so the chain needs only the immediate parent, not a two-ancestor
join (which the compiler could not express).
"""
import sys

sys.path.insert(0, ".tmp_work")
from build_fortisiem_flows import CRON, SERVICE_ID, compile_locally, create  # noqa: E402

FLOW_ID = "flow-fs-agent-status"

# Each `${...}` a child interpolates must be declared by an `extract` transform
# on its PARENT block — the app's validator rejects an undeclared reference.
# This is also why the attributes are given flat names (`org_id`, not
# `organization.id`): the extract's JSONPath does the nesting.
blocks = [
    {"id": "b-org", "adapter": "http", "mode": "read", "name": "List organizations",
     "parentId": None, "branch": None, "serviceId": SERVICE_ID, "entity": None,
     "config": {"method": "GET", "path": "/config/Domain", "responseFormat": "xml", "split": True,
                "recordPath": "$.result.domains.domain[*]", "proxyId": None,
                "pagination": {"type": "none", "fields": {}}},
     "transforms": [{"id": "t-org-name", "kind": "extract",
                     "config": {"attribute": "org_name", "path": "$.name"}}],
     "topicOverride": None, "testResult": None},

    {"id": "b-dev", "adapter": "http", "mode": "read", "name": "List devices",
     "parentId": "b-org", "branch": None, "serviceId": SERVICE_ID, "entity": None,
     "config": {"method": "GET", "path": "/cmdbDeviceInfo/devices?organization=${org_name}",
                "responseFormat": "xml", "split": True, "recordPath": "$.device[*]",
                "proxyId": None, "pagination": {"type": "none", "fields": {}}},
     "transforms": [
         {"id": "t-dev-orgid", "kind": "extract",
          "config": {"attribute": "org_id", "path": "$.organization.id"}},
         {"id": "t-dev-host", "kind": "extract",
          "config": {"attribute": "host_name", "path": "$.name"}},
     ],
     "topicOverride": None, "testResult": None},

    {"id": "b-agent", "adapter": "http", "mode": "read", "name": "Agent status",
     "parentId": "b-dev", "branch": None, "serviceId": SERVICE_ID, "entity": None,
     "config": {"method": "GET",
                "path": "/agentStatus/all?request=${org_id},${host_name}",
                "responseFormat": "xml", "split": True, "recordPath": "$.agentStatus[*]",
                "proxyId": None, "pagination": {"type": "none", "fields": {}}},
     "transforms": [], "topicOverride": None, "testResult": None},

    {"id": "b-agent-k", "adapter": "kafka", "mode": "write", "name": "Publish agent_status",
     "parentId": "b-agent", "branch": None, "serviceId": None, "entity": "agent_status",
     "config": {}, "transforms": [], "topicOverride": None, "testResult": None},
]
topics = [{"id": "t-agent", "kind": "materialized", "name": "raw.fortisiem_agent_status.agent_status",
           "sealed": False, "writerBlockId": "b-agent-k", "backlogEstimate": None}]

doc = {"id": FLOW_ID, "name": "FortiSIEM Agent Status",
       "description": "organization -> device -> agent_status (XML chain)",
       "state": "Draft", "enabled": True, "cron": CRON, "blocks": blocks, "topics": topics,
       "variables": [], "servicePins": {}}

plan = compile_locally(doc)
print("compiled OK — %d child groups" % len(plan.rootGroup.childGroups))
for g in plan.rootGroup.childGroups:
    f = next((p for p in g.processors if p.key == "fetch"), None)
    if f:
        print("   %-12s %s" % (g.blockId, f.properties.get("HTTP URL")))
    print("        procs:", [p.key for p in g.processors])

if "--create" in sys.argv:
    print("CREATED:", create(doc)["id"])
