"""Add the meta stack + an Avro (kafka_kc) branch to every FortiSIEM entity.

Idempotent: re-running skips blocks that already carry a meta stack or already
have an Avro twin.  `POST /api/v2/flows/` upserts on an existing id, so that is
the update path.
"""
import json
import sys
import urllib.request

sys.path.insert(0, ".tmp_work")
from fs_meta import SINK_SERVICE, avro_twin, extract, meta_stack  # noqa: E402

BASE = "http://localhost:8010"

# entity -> extracts on the PARENT http block, and how to build source_object_id.
# `org` is the expression used for customer_tenant_organization; the /query/cmdb
# family carries Customer_Name per record, so the tenant stays per-record rather
# than being hardcoded.
SPEC = {
    # flow-fs-cmdb — columnar /query/cmdb family
    "case":         dict(ex=[("case_id", "$.Case_ID"), ("org_name", "$.Customer_Name")],
                         sid="${case_id}", api="/query/cmdb"),
    "report":       dict(ex=[("report_name", "$.Report_Name"), ("org_name", "$.Customer_Name")],
                         sid="${report_name}", api="/query/cmdb"),
    "monitor":      dict(ex=[("device_ip", "$.Device_IP"), ("monitor_target", "$.Monitor_Target"),
                             ("org_name", "$.Customer_Name")],
                         sid="${device_ip}_${monitor_target}", api="/query/cmdb"),
    "task":         dict(ex=[("task_collector_id", "$.Task_Collector_ID"), ("task_type", "$.Task_Type"),
                             ("org_name", "$.Customer_Name")],
                         sid="${task_collector_id}_${task_type}", api="/query/cmdb"),
    "rule":         dict(ex=[("rule_natural_id", "$.Rule_Natural_ID"), ("org_name", "$.Customer_Name")],
                         sid="${rule_natural_id}", api="/query/cmdb"),
    "user":         dict(ex=[("user_dn", "$.User_DN"), ("org_name", "$.Customer_Name")],
                         sid="${user_dn}", api="/query/cmdb"),
    # plain JSON GETs — different tenant fields available
    "watchlist":    dict(ex=[("watchlist_id", "$.id"), ("org_name", "$.custId")],
                         sid="${watchlist_id}", api="/watchlist/all"),
    "lookup_table": dict(ex=[("lookup_table_id", "$.id"), ("org_name", "$.organizationName")],
                         sid="${lookup_table_id}", api="/pub/lookupTable"),
    # flow-fs-incident
    "incident":     dict(ex=[("incident_id", "$.incidentId"), ("org_name", "$.customer")],
                         sid="${incident_id}", api="/pub/incident"),
    # flow-fs-agent-status — org_id/host_name already come from the device chain
    "agent_status": dict(ex=[], sid="${org_id}_${host_name}", api="/agentStatus/all",
                         org="${org_name}"),
    # flow-9d7ask
    "event_pulling": dict(ex=[("device_ip", "$.Device_IP"), ("org_name", "$.Customer_Name")],
                          sid="${device_ip}", api="/query/cmdb"),
    # flow-vipjvz — only `interface` lacks a stack; mirrors storage's shape
    "interface":    dict(ex=[("interface_name", "$.name")],
                         sid="${natural_id}_${interface_name}",
                         api="/phoenix/rest/cmdbDeviceInfo/device"),
}


def get(fid):
    return json.loads(urllib.request.urlopen(f"{BASE}/api/v2/flows/{fid}", timeout=90).read())


def put(doc):
    keep = {"id", "name", "description", "state", "enabled", "cron", "blocks", "topics",
            "variables", "servicePins"}
    body = {k: v for k, v in doc.items() if k in keep}
    req = urllib.request.Request(f"{BASE}/api/v2/flows/", data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read())


def patch(fid):
    doc = get(fid)
    blocks = doc["blocks"]
    by_id = {b["id"]: b for b in blocks}
    existing_avro = {b.get("parentId") for b in blocks if b["adapter"] == "kafka_kc"}
    added_meta, added_avro, skipped = [], [], []
    new_blocks = []

    for b in list(blocks):
        if b["adapter"] != "kafka" or b.get("mode") != "write":
            continue
        entity = b.get("entity")
        spec = SPEC.get(entity)

        # 1. meta stack on the kafka block (only if it has none)
        if not (b.get("transforms") or []):
            if not spec:
                skipped.append(f"{entity}: no SPEC entry — meta not added")
            else:
                org = spec.get("org", "${org_name}")
                b["transforms"] = meta_stack(entity, source_object_id=spec["sid"],
                                             api_path=spec["api"], org_expr=org)
                added_meta.append(entity)
                # 2. extracts on the parent http block
                parent = by_id.get(b.get("parentId"))
                if parent is not None and spec["ex"]:
                    have = {t["config"].get("attribute") for t in (parent.get("transforms") or [])
                            if t["kind"] == "extract"}
                    parent.setdefault("transforms", [])
                    for attr, path in spec["ex"]:
                        if attr not in have:
                            parent["transforms"].append(extract(attr, path, parent["id"]))

        # 3. Avro twin (only if the parent has no kafka_kc child yet)
        if b.get("parentId") not in existing_avro:
            new_blocks.append(avro_twin(b))
            added_avro.append(entity)

    doc["blocks"] = blocks + new_blocks
    pins = dict(doc.get("servicePins") or {})
    pins.setdefault(SINK_SERVICE, 1)
    doc["servicePins"] = pins

    put(doc)
    print(f"--- {fid}")
    print("    meta added : %s" % (", ".join(added_meta) or "none (already present)"))
    print("    avro added : %s" % (", ".join(added_avro) or "none (already present)"))
    for s in skipped:
        print("    SKIPPED    : %s" % s)


if __name__ == "__main__":
    for fid in sys.argv[1:]:
        patch(fid)
