"""
Performance tuning pass for rapid7_securado.maximum_useful.

Nothing reusable existed for this: build_fortisiem_native_lib.update_processor() has no
`scheduling` parameter (properties/auto-terminate/name only), and create_processor()'s
`scheduling` dict is create-only with zero callers. Every builder in tools/ hardcodes
concurrentlySchedulableTaskCount=1 and runDurationMillis=0. So this script does the
GET -> merge into component.config -> PUT dance itself, with the 409 "while the Processor
is running" retry that update_processor() already models.

Stages are independent and ordered by dependency -- raising concurrency before the thread
pool does nothing, and raising DetectDuplicate concurrency before the Redis pool just moves
the block to a 10s pool wait.

  --stage 1   global enablers: maxTimerDrivenThreadCount, global__redis_pool sizing
  --stage 2   ControlRate limiters -> effectively unlimited
  --stage 3   concurrency on I/O-bound processors + HTTP socket pool
  --stage 4   runDurationMillis batching + gzip -> lz4
  --revert    restore everything from .tmp_work/perf_before.json
  --dry-run   print intended changes, mutate nothing

Deliberately NOT touched:
  - ConsumeKafka concurrency (capped by topic partition count; needs a partition check first)
  - ConsumeKafka "Processing Strategy" (stays FLOW_FILE -> 1 record per Kafka message)
  - maximum__trigger schedulingPeriod (stays 6 hours)
"""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import build_fortisiem_native_lib as L

PG_ID = os.environ.get("R7_PG_ID", "1508dfff-01a0-1000-861c-4cbb8f1c946c")
REDIS_POOL_ID = os.environ.get("GLOBAL_REDIS_POOL_ID", "b90bcbdb-d69c-3725-51d1-444dd57b9336")
ROOT_PG = os.environ.get("INGEST_PARENT_PG_ID", "0a00e822-01a0-1000-68b7-f28e69779c95")
SNAPSHOT = ".tmp_work/perf_before.json"

TARGET_THREADS = int(os.environ.get("R7_MAX_THREADS", "32"))
TARGET_REDIS_POOL = os.environ.get("R7_REDIS_POOL", "32")
UNLIMITED_RATE = os.environ.get("R7_RATE", "100000")

# concurrency by processor type -- I/O bound gets more, since blocked threads cost no CPU
CONCURRENCY = {"InvokeHTTP": 8, "DetectDuplicate": 4, "PublishKafka": 4}
# runDuration batching: lightweight non-I/O types only, mirroring which types the fast
# reference flow (Ingest(3).json) sets 25 on
RUNDUR_TYPES = {"UpdateAttribute", "EvaluateJsonPath", "RouteOnAttribute",
                "SplitJson", "UpdateRecord", "DetectDuplicate"}
RUNDUR_MS = 25

# Stage 5 (measurement-driven second pass)
STAGE5_THREADS = int(os.environ.get("R7_MAX_THREADS_2", "64"))
STAGE5_CONCURRENCY = {"EvaluateJsonPath": 4, "ExecuteGroovyScript": 4, "UpdateAttribute": 4,
                      "UpdateRecord": 4, "RouteOnAttribute": 4, "SplitJson": 2, "ControlRate": 4}


def procs(token):
    s, f = L.nifi("GET", f"/nifi-api/flow/process-groups/{PG_ID}", token)
    return f["processGroupFlow"]["flow"]["processors"]


def patch_processor(token, pid, config_patch=None, prop_patch=None, attempts=6):
    """GET -> merge -> PUT. config_patch goes into component.config, prop_patch into config.properties."""
    last = None
    for i in range(attempts):
        s, cur = L.nifi("GET", f"/nifi-api/processors/{pid}", token)
        if s != 200:
            raise RuntimeError(f"GET {pid} HTTP {s}")
        cfg = dict(cur["component"]["config"])
        if prop_patch:
            merged = {k: v for k, v in (cfg.get("properties") or {}).items() if v != "********"}
            merged.update(prop_patch)
            cfg["properties"] = merged
        if config_patch:
            cfg.update(config_patch)
        body = {"revision": cur["revision"], "component": {"id": pid, "config": cfg}}
        s, r = L.nifi("PUT", f"/nifi-api/processors/{pid}", token, body)
        if s == 200:
            return r
        last = f"HTTP {s}: {json.dumps(r)[:300]}"
        if s == 409 or "while the Processor is running" in json.dumps(r):
            time.sleep(2 + i * 2)
            continue
        raise RuntimeError(f"patch {pid} failed: {last}")
    raise RuntimeError(f"patch {pid} failed after retries: {last}")


ALL_PGS = [
    ROOT_PG,
    "11a8ce0c-01a0-1000-66c2-2931dd000cbb",  # fortisiem.maximum_useful
    "14db305d-01a0-1000-11f0-c68b900bbdb5",  # rapid7_asyad.maximum_useful
    "1508dfff-01a0-1000-861c-4cbb8f1c946c",  # rapid7_securado.maximum_useful
    "14ab82fd-01a0-1000-47d6-db7896347cfc",  # sentinelone.maximum_useful
]


def enabled_services(token):
    """Every controller service across root AND all child PGs, with state.

    Must scan the child PGs, not just root: disabling global__redis_pool cascade-disables
    dependent services (readers/writers/dedupe caches) that live in the child groups -- 128 of
    them, including every <flow>.maximum__dedupe__cache, without which the flows cannot run.
    Scanning root alone reports "0 cascade-disabled" and silently leaves the flows broken.
    """
    out = {}
    for pg in ALL_PGS:
        s, f = L.nifi("GET", f"/nifi-api/flow/process-groups/{pg}/controller-services", token)
        for c in f.get("controllerServices", []):
            out[c["id"]] = (c["component"]["name"], c["component"]["state"])
    return out


def set_service_state(token, sid, state, wait=40):
    s, cur = L.nifi("GET", f"/nifi-api/controller-services/{sid}", token)
    if cur["component"]["state"] == state:
        return True
    L.nifi("PUT", f"/nifi-api/controller-services/{sid}/run-status", token,
           {"revision": cur["revision"], "state": state})
    for _ in range(wait):
        time.sleep(1.5)
        s, c = L.nifi("GET", f"/nifi-api/controller-services/{sid}", token)
        if c["component"]["state"] == state:
            return True
    return False


def stage1(token, dry):
    print("== Stage 1: global enablers ==")
    s, c = L.nifi("GET", "/nifi-api/controller/config", token)
    comp = c.get("component", c)
    cur_threads = comp.get("maxTimerDrivenThreadCount")
    print(f"   maxTimerDrivenThreadCount: {cur_threads} -> {TARGET_THREADS}")
    if not dry:
        body = {"revision": c["revision"],
                "component": {"maxTimerDrivenThreadCount": TARGET_THREADS}}
        s, r = L.nifi("PUT", "/nifi-api/controller/config", token, body)
        got = r.get("component", r).get("maxTimerDrivenThreadCount")
        print(f"      -> HTTP{s} now={got}")

    s, rp = L.nifi("GET", f"/nifi-api/controller-services/{REDIS_POOL_ID}", token)
    p = rp["component"]["properties"]
    print(f"   redis pool Max Total/Max Idle: {p.get('Pool - Max Total')}/{p.get('Pool - Max Idle')}"
          f" -> {TARGET_REDIS_POOL}/{TARGET_REDIS_POOL}")
    if dry:
        return
    before = enabled_services(token)
    was_enabled = [sid for sid, (n, st) in before.items() if st == "ENABLED"]
    print(f"   services ENABLED before pool change: {len(was_enabled)}")
    if not set_service_state(token, REDIS_POOL_ID, "DISABLED"):
        print("      WARN: redis pool did not reach DISABLED; aborting pool resize")
        return
    s, cur = L.nifi("GET", f"/nifi-api/controller-services/{REDIS_POOL_ID}", token)
    props = {k: v for k, v in cur["component"]["properties"].items() if v != "********"}
    props["Pool - Max Total"] = TARGET_REDIS_POOL
    props["Pool - Max Idle"] = TARGET_REDIS_POOL
    s, r = L.nifi("PUT", f"/nifi-api/controller-services/{REDIS_POOL_ID}", token,
                  {"revision": cur["revision"],
                   "component": {"id": REDIS_POOL_ID, "properties": props}})
    print(f"      pool update -> HTTP{s}")
    set_service_state(token, REDIS_POOL_ID, "ENABLED")
    # re-enable everything the cascade knocked over
    after = enabled_services(token)
    broke = [sid for sid in was_enabled if after.get(sid, ("", ""))[1] != "ENABLED"]
    print(f"   cascade-disabled by the pool cycle: {len(broke)} -- re-enabling")
    for sid in broke:
        set_service_state(token, sid, "ENABLED", wait=25)
    final = enabled_services(token)
    still = [final[sid][0] for sid in was_enabled if final.get(sid, ("", ""))[1] != "ENABLED"]
    print(f"   still not ENABLED: {still if still else 'NONE'}")


def stage2(token, dry):
    print("== Stage 2: ControlRate limiters ==")
    for p in procs(token):
        c = p["component"]
        if "ControlRate" not in c["type"]:
            continue
        cur = c["config"]["properties"].get("Maximum Rate")
        print(f"   {c['name'][18:][:44]:44s} {cur} -> {UNLIMITED_RATE}")
        if not dry:
            patch_processor(token, p["id"], prop_patch={"Maximum Rate": UNLIMITED_RATE})


def stage3(token, dry):
    print("== Stage 3: concurrency on I/O-bound processors ==")
    from collections import Counter
    done = Counter()
    for p in procs(token):
        c = p["component"]
        t = c["type"].rsplit(".", 1)[-1]
        target = CONCURRENCY.get(t)
        if not target:
            continue
        cfgp = {"concurrentlySchedulableTaskCount": target}
        propp = {"Socket Idle Connections": "20"} if t == "InvokeHTTP" else None
        if not dry:
            patch_processor(token, p["id"], config_patch=cfgp, prop_patch=propp)
        done[t] += 1
    print(f"   {dict(done)}  (ConsumeKafka deliberately left at 1)")


def stage4(token, dry):
    print("== Stage 4: runDuration batching + compression ==")
    from collections import Counter
    rd = Counter(); comp = 0
    for p in procs(token):
        c = p["component"]
        t = c["type"].rsplit(".", 1)[-1]
        if t in RUNDUR_TYPES:
            if not dry:
                patch_processor(token, p["id"], config_patch={"runDurationMillis": RUNDUR_MS})
            rd[t] += 1
        if t == "PublishKafka" and c["config"]["properties"].get("compression.type") == "gzip":
            if not dry:
                patch_processor(token, p["id"], prop_patch={"compression.type": "lz4"})
            comp += 1
    print(f"   runDuration={RUNDUR_MS}ms on {dict(rd)} (total {sum(rd.values())})")
    print(f"   compression gzip->lz4 on {comp} PublishKafka")


def stage5(token, dry):
    """Second-pass tuning, driven by measured queue concentration after stages 1-4.

    After 1-4 the queue moved off the rate limiters and piled up at every processor still
    left at concurrency 1: EvaluateJsonPath 34.8k, ControlRate 14.1k, ExecuteGroovyScript
    13.0k queued. InvokeHTTP still held 59.8k despite concurrency 8, i.e. it is thread-starved
    rather than concurrency-starved -- hence the thread-pool bump as well.

    Note the ControlRate processors are now pure overhead: their rate is effectively unlimited,
    but each is a single-threaded hop every flowfile must still traverse. Raising their
    concurrency is the low-risk fix; deleting them would mean rewiring 11 connections.
    """
    print("== Stage 5: second-pass concurrency (measurement-driven) ==")
    s, c = L.nifi("GET", "/nifi-api/controller/config", token)
    comp = c.get("component", c)
    cur = comp.get("maxTimerDrivenThreadCount")
    print(f"   maxTimerDrivenThreadCount: {cur} -> {STAGE5_THREADS}")
    if not dry:
        L.nifi("PUT", "/nifi-api/controller/config", token,
               {"revision": c["revision"],
                "component": {"maxTimerDrivenThreadCount": STAGE5_THREADS}})
    from collections import Counter
    done = Counter()
    for p in procs(token):
        c_ = p["component"]
        t = c_["type"].rsplit(".", 1)[-1]
        target = STAGE5_CONCURRENCY.get(t)
        if not target:
            continue
        if not dry:
            patch_processor(token, p["id"],
                            config_patch={"concurrentlySchedulableTaskCount": target})
        done[t] += 1
    print(f"   {dict(done)}")


def revert(token, dry):
    print("== REVERT from snapshot ==")
    snap = json.load(open(SNAPSHOT, encoding="utf-8"))
    s, c = L.nifi("GET", "/nifi-api/controller/config", token)
    tgt = snap["controller"]["maxTimerDrivenThreadCount"]
    print(f"   maxTimerDrivenThreadCount -> {tgt}")
    if not dry:
        L.nifi("PUT", "/nifi-api/controller/config", token,
               {"revision": c["revision"], "component": {"maxTimerDrivenThreadCount": tgt}})
    byname = {p["component"]["name"]: p for p in procs(token)}
    n = 0
    for name, was in snap["processors"].items():
        p = byname.get(name)
        if not p:
            continue
        cfgp = {"concurrentlySchedulableTaskCount": was["concurrentlySchedulableTaskCount"],
                "runDurationMillis": was["runDurationMillis"]}
        propp = was.get("props") or None
        if not dry:
            patch_processor(token, p["id"], config_patch=cfgp, prop_patch=propp)
        n += 1
    print(f"   restored {n} processors (redis pool must be reverted manually if needed)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", type=int, choices=[1, 2, 3, 4, 5])
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--revert", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    if not L.NIFI_PASSWORD:
        raise SystemExit("set NIFI_PASSWORD")
    token = L.login()
    if a.revert:
        revert(token, a.dry_run); return
    stages = [1, 2, 3, 4] if a.all else ([a.stage] if a.stage else [])
    if not stages:
        raise SystemExit("pass --stage N, --all, or --revert")
    for st in stages:
        {1: stage1, 2: stage2, 3: stage3, 4: stage4, 5: stage5}[st](token, a.dry_run)


if __name__ == "__main__":
    main()
