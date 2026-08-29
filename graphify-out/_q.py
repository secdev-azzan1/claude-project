import json, networkx as nx
from networkx.readwrite import json_graph
from pathlib import Path
G = json_graph.node_link_graph(json.loads(Path('graphify-out/graph.json').read_text(encoding='utf-8')), edges='links')
print('nodes', G.number_of_nodes())
hits = [(n,d) for n,d in G.nodes(data=True) if 'appservice' in d.get('label','').lower().replace(' ','').replace('_','') or 'applicationservice' in d.get('label','').lower().replace(' ','').replace('_','')]
print('--- app-service nodes:', len(hits))
for n,d in hits[:60]:
    print(f"  {d.get('label')}  [{d.get('source_file')}:{d.get('source_location')}]")
