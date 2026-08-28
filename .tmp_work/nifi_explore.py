import json, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

path = r"C:\Users\kaifm\Desktop\Project\DataPASC-DataMobility\CMDB-DataPush\Ingest(3)_(1).json"
with open(path, encoding='utf-8') as f:
    data = json.load(f)

print("top-level keys:", list(data.keys()))

def find_flow_contents(d):
    if isinstance(d, dict):
        if 'processGroups' in d and ('processors' in d or 'name' in d):
            return d
        for k in ('flowContents', 'snapshot', 'flow'):
            if k in d:
                r = find_flow_contents(d[k])
                if r:
                    return r
    return None

root = find_flow_contents(data) or data
print("root keys:", list(root.keys()) if isinstance(root, dict) else type(root))

def walk(pg, depth=0, path_names=()):
    name = pg.get('name', '<unnamed>')
    pgs = pg.get('processGroups', []) or pg.get('flowContents', {}).get('processGroups', []) if isinstance(pg.get('flowContents'), dict) else pg.get('processGroups', [])
    print('  ' * depth + f"- {name}  (id={pg.get('identifier') or pg.get('id','?')[:8]})")
    children = pg.get('processGroups', [])
    for child in children:
        # child may itself have flowContents wrapper
        inner = child.get('flowContents', child) if isinstance(child, dict) else child
        walk(inner if 'processGroups' in inner or 'processors' in inner else child, depth+1, path_names+(name,))

walk(root)
