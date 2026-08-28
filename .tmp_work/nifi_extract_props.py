import json, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

path = r"C:\Users\kaifm\Desktop\Project\DataPASC-DataMobility\CMDB-DataPush\Ingest(3)_(1).json"
with open(path, encoding='utf-8') as f:
    data = json.load(f)

root = data['flowContents']

def find_pg_by_id(pg, target_id):
    if pg.get('identifier') == target_id:
        return pg
    for child in pg.get('processGroups', []):
        r = find_pg_by_id(child, target_id)
        if r:
            return r
    return None

TARGET_ID = "eaa19eb1-09ee-3d6a-98ac-3d143ad850dd"
pg = find_pg_by_id(root, TARGET_ID)

TARGET_NAMES = {
    "rapid7_securado.site__fetch",
    "rapid7_securado.site__init_page",
    "rapid7_securado.site__next_page",
    "rapid7_securado.site__extract",
    "rapid7_securado.site__filter",
    "rapid7_securado.site__detail_fetch",
    "rapid7_securado.site__page_meta",
    "rapid7_securado.site__has_more",
    "rapid7_securado.asset__init_page",
    "rapid7_securado.asset__fetch",
    "rapid7_securado.asset__next_page",
    "rapid7_securado.asset__extract",
    "rapid7_securado.asset__detail_fetch",
    "rapid7_securado.asset__rate_limit",
    "rapid7_securado.asset__page_meta",
    "rapid7_securado.asset__has_more",
    "rapid7_securado.asset__dedupe_key",
    "rapid7_securado.asset__dedupe",
    "rapid7_securado.asset__hash",
    "rapid7_securado.asset__raw__publish",
    "rapid7_securado.asset__avro__publish",
    "rapid7_securado.site_organization__fetch",
    "rapid7_securado.maximum__trigger",
    "rapid7_securado.maximum__run_metadata",
}

out = []
for p in pg.get('processors', []):
    if p.get('name') in TARGET_NAMES:
        out.append(f"=== {p.get('name')}  [{p.get('type','').rsplit('.',1)[-1]}] ===")
        props = p.get('properties', {}) or {}
        for k, v in props.items():
            if v is None:
                continue
            v_str = str(v)
            if len(v_str) > 500:
                v_str = v_str[:500] + '...[truncated]'
            out.append(f"  {k}: {v_str}")
        sched = p.get('schedulingPeriod')
        if sched:
            out.append(f"  [schedulingPeriod: {sched}]")
        rels = p.get('autoTerminatedRelationships')
        if rels:
            out.append(f"  [autoTerminated: {rels}]")
        out.append("")

text = '\n'.join(out)
with open('graphify-out/nifi_props_dump.txt', 'w', encoding='utf-8') as f:
    f.write(text)
print(f"wrote {len(out)} lines")
