import json, sys

names_of_interest = {"trigger","init","render_body","write","columnar_transform","split","page_meta","has_more","next"}

def parse(path, label):
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    pg = data.get("processGroupFlow", {})
    flow = pg.get("flow", {})
    procs = flow.get("processors", [])
    print(f"=== {label} processors ===")
    for p in procs:
        comp = p.get("component", {})
        name = comp.get("name")
        pid = comp.get("id")
        status = p.get("status", {})
        agg = status.get("aggregateSnapshot", {})
        runStatus = agg.get("runStatus")
        ffIn = agg.get("flowFilesIn")
        ffOut = agg.get("flowFilesOut")
        tasks = agg.get("tasks")
        marker = " <-- OF INTEREST" if name in names_of_interest else ""
        print(f"{name}\tid={pid}\trunStatus={runStatus}\tflowFilesIn={ffIn}\tflowFilesOut={ffOut}\ttasks={tasks}{marker}")
    print(f"=== {label} connections (queued) ===")
    conns = flow.get("connections", [])
    for c in conns:
        comp = c.get("component", {})
        src = comp.get("source", {}).get("name")
        dst = comp.get("destination", {}).get("name")
        status = c.get("status", {})
        agg = status.get("aggregateSnapshot", {})
        queued = agg.get("flowFilesQueued")
        queuedSize = agg.get("queuedCount")
        print(f"{src} -> {dst} : flowFilesQueued={queued} queuedCount={queuedSize}")

if __name__ == "__main__":
    parse(sys.argv[1], sys.argv[2])
