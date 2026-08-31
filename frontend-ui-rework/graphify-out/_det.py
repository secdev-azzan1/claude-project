import json
from graphify.detect import detect
from pathlib import Path
r = detect(Path('src'))
Path('graphify-out/.graphify_detect.json').write_text(json.dumps(r, ensure_ascii=False), encoding='utf-8')
print('files', r['total_files'], 'words', r['total_words'])
for k,v in r['files'].items():
    if v: print(' ', k, len(v))
