import json
from graphify.build import build_from_json
from graphify.cluster import cluster, score_all
from graphify.analyze import god_nodes, surprising_connections, suggest_questions
from graphify.report import generate
from graphify.export import to_json
from pathlib import Path
ex = json.loads(Path('graphify-out/.graphify_extract.json').read_text(encoding='utf-8'))
det= json.loads(Path('graphify-out/.graphify_detect.json').read_text(encoding='utf-8'))
G = build_from_json(ex); comms = cluster(G); coh = score_all(G, comms)
gods = god_nodes(G); sur = surprising_connections(G, comms)
labels = {c: 'Community '+str(c) for c in comms}
q = suggest_questions(G, comms, labels)
Path('graphify-out/GRAPH_REPORT.md').write_text(generate(G,comms,coh,labels,gods,sur,det,{'input':0,'output':0},'src',suggested_questions=q), encoding='utf-8')
to_json(G, comms, 'graphify-out/graph.json')
Path('graphify-out/.graphify_analysis.json').write_text(json.dumps({'communities':{str(k):v for k,v in comms.items()},'cohesion':{str(k):v for k,v in coh.items()},'gods':gods,'surprises':sur}, ensure_ascii=False), encoding='utf-8')
print(f'Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges, {len(comms)} communities')
print('--- GOD NODES ---')
for g in gods[:12]: print(' ', g)
