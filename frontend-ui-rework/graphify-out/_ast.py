import json
from graphify.extract import collect_files, extract
from pathlib import Path

def main():
    d = json.loads(Path('graphify-out/.graphify_detect.json').read_text(encoding='utf-8'))
    files=[]
    for f in d['files']['code']:
        p=Path(f); files.extend(collect_files(p) if p.is_dir() else [p])
    r = extract(files)
    Path('graphify-out/.graphify_extract.json').write_text(json.dumps(r, ensure_ascii=False), encoding='utf-8')
    print('AST:', len(r['nodes']),'nodes', len(r['edges']),'edges')

if __name__=='__main__': main()
