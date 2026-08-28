"""
Fix: rapid7_asyad + rapid7_securado every entity's __avro__publish processor has TWO inbound
connections -- a direct '__dedupe --[non-duplicate]--> __avro__publish' bypass (leftover from before
the replay-consume redesign) plus the intended '__replay__consume --[success]--> __avro__publish'
path. NiFi clones the flowfile across both, so every non-duplicate record publishes to the avro topic
twice while raw only gets it once -- exactly the "avro = 2x raw" symptom reported live.

This deletes the 10 orphaned direct dedupe->avro__publish connections (5 entities x 2 flows),
leaving replay__consume as the sole path into avro publish. Re-verifies each connection is still
enabled/empty and both endpoints are stopped immediately before deleting (belt-and-suspenders on top
of the already-confirmed state), refusing to touch anything that looks unexpectedly different.
"""
import json
import os
import subprocess
import sys
import time
import urllib.parse

NIFI_BASE = os.environ.get("NIFI_BASE", "https://nifi.datapasc.com").rstrip("/")
NIFI_USER = os.environ.get("NIFI_USER", "admin")
NIFI_PASSWORD = os.environ.get("NIFI_PASSWORD")

TARGETS = [
    ("asyad", "14e9567f-01a0-1000-954f-9483cffd4e85", "rapid7_asyad.asset_service__dedupe", "rapid7_asyad.asset_service__avro__publish"),
    ("asyad", "14e977c2-01a0-1000-3173-cff250b20259", "rapid7_asyad.asset_vulnerability__dedupe", "rapid7_asyad.asset_vulnerability__avro__publish"),
    ("asyad", "14e90e3f-01a0-1000-e7a9-1502014898b7", "rapid7_asyad.asset__dedupe", "rapid7_asyad.asset__avro__publish"),
    ("asyad", "14e93936-01a0-1000-5390-4b41e4c9fa52", "rapid7_asyad.asset_software__dedupe", "rapid7_asyad.asset_software__avro__publish"),
    ("asyad", "14e8e59d-01a0-1000-365d-1f82b717bd15", "rapid7_asyad.site__dedupe", "rapid7_asyad.site__avro__publish"),
    ("securado", "1511607e-01a0-1000-2bc8-df87ac857851", "rapid7_securado.site__dedupe", "rapid7_securado.site__avro__publish"),
    ("securado", "1511d486-01a0-1000-2084-976c66feb515", "rapid7_securado.asset_vulnerability__dedupe", "rapid7_securado.asset_vulnerability__avro__publish"),
    ("securado", "151199f4-01a0-1000-ea3c-ad02e6569bca", "rapid7_securado.asset_software__dedupe", "rapid7_securado.asset_software__avro__publish"),
    ("securado", "1511b5a6-01a0-1000-e5dc-b5255e6336cc", "rapid7_securado.asset_service__dedupe", "rapid7_securado.asset_service__avro__publish"),
    ("securado", "15117d02-01a0-1000-0b00-e4c153dd4635", "rapid7_securado.asset__dedupe", "rapid7_securado.asset__avro__publish"),
]


def run_curl(args, input_text=None, timeout=30, attempts=3):
    last = None
    for i in range(attempts):
        proc = subprocess.run(["curl.exe", "--http1.1", "-k", "-sS"] + args, input=input_text, text=True, capture_output=True, timeout=timeout)
        if proc.returncode == 0:
            return proc.stdout
        last = f"curl exit {proc.returncode}: {proc.stderr[:400]} {proc.stdout[:400]}"
        time.sleep(1 + i)
    raise RuntimeError(last)


def login():
    if not NIFI_PASSWORD:
        raise RuntimeError("Set NIFI_PASSWORD")
    body = urllib.parse.urlencode({"username": NIFI_USER, "password": NIFI_PASSWORD})
    return run_curl(["-H", "Content-Type: application/x-www-form-urlencoded", "--data-binary", "@-", f"{NIFI_BASE}/nifi-api/access/token"], body).strip()


def nifi(method, path, token, timeout=30):
    args = ["-X", method, "-H", f"Authorization: Bearer {token}", "-w", "\nHTTP_STATUS:%{http_code}", f"{NIFI_BASE}{path}"]
    out = run_curl(args, timeout=timeout)
    raw, status_txt = out.rsplit("\nHTTP_STATUS:", 1)
    status = int(status_txt.strip()[:3])
    return status, (json.loads(raw) if raw.strip() else {})


def main():
    token = login()
    results = []
    for label, conn_id, src_name, dst_name in TARGETS:
        status, conn = nifi("GET", f"/nifi-api/connections/{conn_id}", token)
        if status != 200:
            results.append({"id": conn_id, "action": "SKIPPED", "reason": f"GET connection failed HTTP {status}"})
            continue
        comp = conn["component"]
        actual_src, actual_dst = comp["source"]["name"], comp["destination"]["name"]
        if actual_src != src_name or actual_dst != dst_name:
            results.append({"id": conn_id, "action": "SKIPPED", "reason": f"src/dst mismatch: {actual_src} -> {actual_dst}"})
            continue
        queued = conn.get("status", {}).get("aggregateSnapshot", {}).get("queuedCount")
        if queued not in ("0", 0, None):
            results.append({"id": conn_id, "action": "SKIPPED", "reason": f"queue not empty: {queued}"})
            continue
        for pname, pid_path in ((actual_src, "source"), (actual_dst, "destination")):
            pid = comp[pid_path]["id"]
            pstatus, pdata = nifi("GET", f"/nifi-api/processors/{pid}", token)
            if pstatus != 200:
                results.append({"id": conn_id, "action": "SKIPPED", "reason": f"GET processor {pname} failed HTTP {pstatus}"})
                break
            run_status = pdata["component"].get("state")
            if run_status != "STOPPED":
                results.append({"id": conn_id, "action": "SKIPPED", "reason": f"{pname} not stopped (state={run_status})"})
                break
        else:
            version = conn["revision"]["version"]
            dstatus, dresp = nifi("DELETE", f"/nifi-api/connections/{conn_id}?version={version}", token)
            if dstatus == 200:
                results.append({"id": conn_id, "action": "DELETED", "src": actual_src, "dst": actual_dst})
            else:
                results.append({"id": conn_id, "action": "FAILED", "reason": f"DELETE HTTP {dstatus}: {json.dumps(dresp)[:300]}"})
    print(json.dumps(results, indent=2))
    deleted = sum(1 for r in results if r["action"] == "DELETED")
    print(f"\n{deleted}/{len(TARGETS)} deleted", file=sys.stderr)


if __name__ == "__main__":
    main()
