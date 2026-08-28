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

pg = find_pg_by_id(root, "eaa19eb1-09ee-3d6a-98ac-3d143ad850dd")

for p in pg.get('processors', []):
    if p.get('name') == 'rapid7_securado.asset__fetch':
        # print full structure keys and the config sub-object keys
        print("processor top-level keys:", list(p.keys()))
        cfg = p.get('config', {})
        print("config keys:", list(cfg.keys()))
        print(json.dumps(cfg, indent=2, ensure_ascii=False)[:6000])
        break
