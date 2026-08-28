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
if not pg:
    print("NOT FOUND")
    sys.exit(1)

print(f"=== Process Group: {pg.get('name')} (id={pg.get('identifier')}) ===\n")

def dump_pg(pg, depth=0, out=None):
    indent = '  ' * depth
    out.append(f"{indent}PG: {pg.get('name')}  [id={pg.get('identifier')[:8]}]")
    if pg.get('parameterContextName'):
        out.append(f"{indent}  parameterContext: {pg.get('parameterContextName')}")

    procs = pg.get('processors', [])
    out.append(f"{indent}  -- processors ({len(procs)}) --")
    id_to_name = {}
    for p in procs:
        pid = p.get('identifier')
        pname = p.get('name')
        ptype = p.get('type', '').rsplit('.', 1)[-1]
        sched = p.get('scheduledState', '')
        id_to_name[pid] = pname
        out.append(f"{indent}    [{ptype}] {pname}  ({sched})")

    ports_in = pg.get('inputPorts', [])
    ports_out = pg.get('outputPorts', [])
    for p in ports_in:
        id_to_name[p.get('identifier')] = p.get('name')
        out.append(f"{indent}    [INPUT PORT] {p.get('name')}")
    for p in ports_out:
        id_to_name[p.get('identifier')] = p.get('name')
        out.append(f"{indent}    [OUTPUT PORT] {p.get('name')}")

    funnels = pg.get('funnels', [])
    for f in funnels:
        id_to_name[f.get('identifier')] = 'FUNNEL'

    conns = pg.get('connections', [])
    out.append(f"{indent}  -- connections ({len(conns)}) --")
    for c in conns:
        src = c.get('source', {})
        dst = c.get('destination', {})
        src_name = src.get('name') or id_to_name.get(src.get('id'), src.get('id', '?')[:8])
        dst_name = dst.get('name') or id_to_name.get(dst.get('id'), dst.get('id', '?')[:8])
        rels = ', '.join(c.get('selectedRelationships', []))
        bp = c.get('backPressureObjectThreshold')
        out.append(f"{indent}    {src_name} --[{rels}]--> {dst_name}  (backpressure={bp})")

    css = pg.get('controllerServices', [])
    if css:
        out.append(f"{indent}  -- controller services ({len(css)}) --")
        for cs in css:
            out.append(f"{indent}    [{cs.get('type','').rsplit('.',1)[-1]}] {cs.get('name')}")

    children = pg.get('processGroups', [])
    for child in children:
        dump_pg(child, depth+1, out)

out = []
dump_pg(pg, 0, out)
text = '\n'.join(out)
print(text)
print(f"\n\n[total lines: {len(out)}]")

with open('graphify-out/nifi_pg_dump.txt', 'w', encoding='utf-8') as f:
    f.write(text)
