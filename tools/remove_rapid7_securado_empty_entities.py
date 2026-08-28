"""
Remove the 4 Tier 1 entities that returned zero records on this tenant, so nothing dangles:
  agent, asset_group, vulnerability_exception, asset_group_asset

Each endpoint answers HTTP 200 with an empty `resources` array -- this tenant genuinely has no
Insight Agents, no asset groups, and no vulnerability exceptions (asset_group_asset is rooted on
asset_group, so it is empty by consequence). With no records to sample, no Avro schema could ever be
inferred, so their `avro__publish` was permanently invalid.

Removes from NiFi (connections -> processors -> per-entity controller services), Kafka (raw + avro
topics), and Kafka Connect (Iceberg sink connectors). Apicurio needs nothing removed -- no schema was
ever registered for these 4.

Order matters: connections must go before processors, and processors before the controller services
they reference, or NiFi refuses with a 409.
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
KAFBAT = "https://kafbat.datapasc.com"
KAFBAT_USER = os.environ.get("KAFBAT_USERNAME", "admin")
KAFBAT_PASS = os.environ.get("KAFBAT_PASSWORD", "Kafbatadmin@123")
KC = "https://kafkaconnect.datapasc.com"
APICURIO = "https://apicurio.datapasc.com/apis/ccompat/v7"

# Defaults target rapid7_securado; override via env for rapid7_asyad (same 4 entities are empty on
# both tenants -- verified independently from Kafka topic counts, not assumed).
#   R7_FLOW=rapid7_asyad R7_PG_ID=14db305d-01a0-1000-11f0-c68b900bbdb5
PG_ID = os.environ.get("R7_PG_ID", "1508dfff-01a0-1000-861c-4cbb8f1c946c")
FLOW = os.environ.get("R7_FLOW", "rapid7_securado")
REMOVE = ["agent", "asset_group", "vulnerability_exception", "asset_group_asset"]
COOKIE = ".tmp_work/kb_rm.txt"

TOKEN = None


def sh(args, input_text=None, timeout=60):
    return subprocess.run(["curl.exe", "--http1.1", "-k", "-sS"] + args, input=input_text,
                          text=True, capture_output=True, timeout=timeout).stdout


def login():
    body = urllib.parse.urlencode({"username": NIFI_USER, "password": NIFI_PASSWORD})
    return sh(["-H", "Content-Type: application/x-www-form-urlencoded", "--data-binary", "@-",
               f"{NIFI_BASE}/nifi-api/access/token"], body).strip()


def nifi(method, path, body=None):
    args = ["-X", method, "-H", f"Authorization: Bearer {TOKEN}"]
    inp = None
    if body is not None:
        args += ["-H", "Content-Type: application/json", "--data-binary", "@-"]
        inp = json.dumps(body)
    args += ["-w", "\nHTTP_STATUS:%{http_code}", f"{NIFI_BASE}{path}"]
    out = sh(args, inp)
    raw, st = out.rsplit("\nHTTP_STATUS:", 1)
    st = int(st.strip()[:3])
    resp = {}
    if raw.strip():
        try:
            resp = json.loads(raw)
        except json.JSONDecodeError:
            resp = {"raw_text": raw[:400]}
    return st, resp


def owned(name):
    """True if this processor belongs to one of the entities being removed."""
    return any(name.startswith(f"{FLOW}.{e}__") for e in REMOVE)


def drop_queue(conn_id):
    st, r = nifi("POST", f"/nifi-api/flowfile-queues/{conn_id}/drop-requests", {})
    if st != 200:
        return
    did = r["dropRequest"]["id"]
    for _ in range(15):
        st, cur = nifi("GET", f"/nifi-api/flowfile-queues/{conn_id}/drop-requests/{did}")
        if cur.get("dropRequest", {}).get("finished"):
            return
        time.sleep(1)


def remove_nifi():
    st, flow = nifi("GET", f"/nifi-api/flow/process-groups/{PG_ID}")
    procs = flow["processGroupFlow"]["flow"]["processors"]
    conns = flow["processGroupFlow"]["flow"]["connections"]

    target_ids = {p["component"]["id"] for p in procs if owned(p["component"]["name"])}
    target_names = sorted(p["component"]["name"] for p in procs if owned(p["component"]["name"]))

    # 1) connections touching any target processor -- includes the inbound trigger links from
    #    maximum__run_metadata and asset_group__extract, which would otherwise dangle.
    conn_done, conn_fail = 0, []
    for c in conns:
        comp = c["component"]
        if comp["source"]["id"] in target_ids or comp["destination"]["id"] in target_ids:
            drop_queue(c["id"])
            st, cur = nifi("GET", f"/nifi-api/connections/{c['id']}")
            if st != 200:
                continue
            st, r = nifi("DELETE", f"/nifi-api/connections/{c['id']}?version={cur['revision']['version']}")
            if st == 200:
                conn_done += 1
            else:
                conn_fail.append((c["id"], st, str(r)[:120]))

    # 2) processors
    proc_done, proc_fail = [], []
    for p in procs:
        name = p["component"]["name"]
        if not owned(name):
            continue
        pid = p["component"]["id"]
        st, cur = nifi("GET", f"/nifi-api/processors/{pid}")
        if st != 200:
            continue
        st, r = nifi("DELETE", f"/nifi-api/processors/{pid}?version={cur['revision']['version']}")
        (proc_done if st == 200 else proc_fail).append(name if st == 200 else (name, st, str(r)[:120]))

    # 3) per-entity controller services (avro reader/writer) -- must be disabled before delete
    st, cs_list = nifi("GET", f"/nifi-api/flow/process-groups/{PG_ID}/controller-services")
    cs_done, cs_fail = [], []
    for cs in cs_list.get("controllerServices", []):
        cname = cs["component"]["name"]
        if not any(cname.startswith(f"{FLOW}.{e}__avro") for e in REMOVE):
            continue
        cid = cs["component"]["id"]
        st, cur = nifi("GET", f"/nifi-api/controller-services/{cid}")
        nifi("PUT", f"/nifi-api/controller-services/{cid}/run-status",
             {"revision": {"version": cur["revision"]["version"]}, "state": "DISABLED"})
        time.sleep(1.2)
        st, cur = nifi("GET", f"/nifi-api/controller-services/{cid}")
        st, r = nifi("DELETE", f"/nifi-api/controller-services/{cid}?version={cur['revision']['version']}")
        (cs_done if st == 200 else cs_fail).append(cname if st == 200 else (cname, st, str(r)[:120]))

    return {"targeted_processors": len(target_names), "connections_deleted": conn_done,
            "connections_failed": conn_fail, "processors_deleted": len(proc_done),
            "processors_failed": proc_fail, "services_deleted": cs_done, "services_failed": cs_fail}


def remove_topics():
    os.makedirs(".tmp_work", exist_ok=True)
    sh(["-c", COOKIE, f"{KAFBAT}/login", "-o", os.devnull])
    sh(["-b", COOKIE, "-c", COOKIE, "-X", "POST", f"{KAFBAT}/login",
        "--data-urlencode", f"username={KAFBAT_USER}", "--data-urlencode", f"password={KAFBAT_PASS}",
        "-o", os.devnull])
    deleted, absent = [], []
    for e in REMOVE:
        for suf in ("__raw", "__raw.avro"):
            t = f"bronze.{FLOW}.{e}{suf}"
            out = sh(["-b", COOKIE, "-X", "DELETE", "-o", os.devnull, "-w", "%{http_code}",
                      f"{KAFBAT}/api/clusters/local/topics/{urllib.parse.quote(t, safe='')}"])
            (deleted if out.strip() == "200" else absent).append(t)
    return {"topics_deleted": deleted, "topics_absent_or_failed": absent}


def remove_connectors():
    deleted, absent = [], []
    for e in REMOVE:
        n = f"bronze.{FLOW}.{e}__raw.avro__iceberg"
        out = sh(["-X", "DELETE", "-o", os.devnull, "-w", "%{http_code}",
                  f"{KC}/connectors/{urllib.parse.quote(n, safe='')}"])
        (deleted if out.strip() in ("204", "200") else absent).append(n)
    return {"connectors_deleted": deleted, "connectors_absent_or_failed": absent}


def check_schemas():
    out = {}
    for e in REMOVE:
        subj = f"bronze.{FLOW}.{e}__raw.avro-value"
        code = sh(["-o", os.devnull, "-w", "%{http_code}",
                   f"{APICURIO}/subjects/{urllib.parse.quote(subj, safe='')}/versions/latest"]).strip()
        out[e] = f"HTTP {code}" + (" (nothing to delete)" if code == "404" else " -- UNEXPECTED, exists")
    return out


def main():
    global TOKEN
    if not NIFI_PASSWORD:
        raise RuntimeError("Set NIFI_PASSWORD")
    TOKEN = login()
    report = {"nifi": remove_nifi(), "kafka": remove_topics(),
              "kafka_connect": remove_connectors(), "apicurio": check_schemas()}
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
