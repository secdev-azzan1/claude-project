"""
Phase 1 of the FortiSIEM native rebuild: drop device_business_service_membership, software_service,
software_patch entirely -- confirmed with the user that none of the 3 have a real dedicated API
endpoint (all three only ever existed as a byproduct of parsing the full device-detail blob).

Deletes: their 15 NiFi processors + 15 connections in fortisiem.maximum_useful, their Apicurio
schema subjects, and their Kafka Connect Iceberg connectors if any exist -- so they don't keep
polluting the schema registry / connector list.
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
APICURIO_CCOMPAT = os.environ.get("APICURIO_CCOMPAT", "https://apicurio.datapasc.com/apis/ccompat/v7").rstrip("/")
KAFKA_CONNECT_BASE = os.environ.get("KAFKA_CONNECT_BASE", "https://kafkaconnect.datapasc.com").rstrip("/")

DROP_ENTITIES = ("device_business_service_membership", "software_service", "software_patch")
PG_ID = "11a8ce0c-01a0-1000-66c2-2931dd000cbb"


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


def http_delete(url, timeout=20):
    args = ["-X", "DELETE", "-w", "\nHTTP_STATUS:%{http_code}", url]
    out = run_curl(args, timeout=timeout, attempts=2)
    raw, status_txt = out.rsplit("\nHTTP_STATUS:", 1)
    return int(status_txt.strip()[:3]), raw


def main():
    token = login()
    status, flow = nifi("GET", f"/nifi-api/flow/process-groups/{PG_ID}", token)
    if status != 200:
        raise RuntimeError(f"GET flow failed HTTP {status}")
    procs = flow["processGroupFlow"]["flow"]["processors"]
    conns = flow["processGroupFlow"]["flow"]["connections"]

    drop_procs = [p for p in procs if any(p["component"]["name"].startswith(f"fortisiem.{e}__") for e in DROP_ENTITIES)]
    drop_ids = {p["component"]["id"] for p in drop_procs}
    drop_conns = [c for c in conns if c["component"]["source"]["id"] in drop_ids or c["component"]["destination"]["id"] in drop_ids]

    results = {"connections_deleted": [], "connections_failed": [], "processors_deleted": [], "processors_failed": [],
               "schemas_deleted": [], "schemas_failed": [], "connectors_deleted": [], "connectors_not_found": []}

    # 1) connections first (processors can't be deleted while connected)
    for c in drop_conns:
        version = c["revision"]["version"]
        dstatus, dresp = nifi("DELETE", f"/nifi-api/connections/{c['id']}?version={version}", token)
        if dstatus == 200:
            results["connections_deleted"].append(c["id"])
        else:
            results["connections_failed"].append({"id": c["id"], "status": dstatus, "resp": str(dresp)[:200]})

    # 2) processors
    for p in drop_procs:
        pid = p["component"]["id"]
        version = p["revision"]["version"]
        dstatus, dresp = nifi("DELETE", f"/nifi-api/processors/{pid}?version={version}", token)
        if dstatus == 200:
            results["processors_deleted"].append(p["component"]["name"])
        else:
            results["processors_failed"].append({"name": p["component"]["name"], "status": dstatus, "resp": str(dresp)[:200]})

    # 3) Apicurio schema subjects
    for e in DROP_ENTITIES:
        subject = f"bronze.fortisiem.{e}__raw.avro-value"
        status_code, resp = http_delete(f"{APICURIO_CCOMPAT}/subjects/{urllib.parse.quote(subject, safe='')}")
        if status_code == 200:
            results["schemas_deleted"].append(subject)
        else:
            results["schemas_failed"].append({"subject": subject, "status": status_code, "resp": resp[:200]})

    # 4) Kafka Connect connectors, if they exist
    conn_list_raw = run_curl(["-w", "\nHTTP_STATUS:%{http_code}", f"{KAFKA_CONNECT_BASE}/connectors"], attempts=2)
    raw, status_txt = conn_list_raw.rsplit("\nHTTP_STATUS:", 1)
    if int(status_txt.strip()[:3]) == 200:
        existing_connectors = json.loads(raw)
        for e in DROP_ENTITIES:
            connector_name = f"bronze.fortisiem.{e}__raw.avro__iceberg"
            if connector_name in existing_connectors:
                dcode, dresp = http_delete(f"{KAFKA_CONNECT_BASE}/connectors/{urllib.parse.quote(connector_name, safe='')}")
                if dcode in (204, 200):
                    results["connectors_deleted"].append(connector_name)
                else:
                    results["connectors_not_found"].append({"connector": connector_name, "status": dcode, "resp": dresp[:200]})
            else:
                results["connectors_not_found"].append({"connector": connector_name, "reason": "not registered"})
    else:
        results["connectors_not_found"].append({"reason": f"could not list connectors HTTP {status_txt.strip()}"})

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
