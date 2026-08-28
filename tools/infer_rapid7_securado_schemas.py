"""
Phase 2: infer + register Avro schemas for the 17 new rapid7_securado Tier 1 entities.

Source of truth is the ACTUAL raw-topic message, not the API response -- because that is exactly what
`<entity>__replay__consume` reads back and feeds into `avro__publish`'s Record Reader. Inferring from
anything else risks a schema that doesn't match what the Avro side actually receives (the class of bug
that bit fortisiem's XML double-wrap this session).

Reads via Kafbat's SSE message API (avoids NiFi provenance, which rate-limits hard at 11 outstanding
queries). Samples up to N messages per topic and MERGES the inferred types, so a field that is null in
one record but populated in another still gets its real type rather than defaulting to string.

Everything is nullable-with-default-null: Rapid7 omits keys freely between records, and Iceberg's
schema-force-optional is already set on the sink connectors.
"""
import json
import os
import subprocess
import sys
import urllib.parse

KAFBAT = "https://kafbat.datapasc.com"
KAFBAT_USER = os.environ.get("KAFBAT_USERNAME", "admin")
KAFBAT_PASS = os.environ.get("KAFBAT_PASSWORD", "Kafbatadmin@123")
APICURIO = "https://apicurio.datapasc.com/apis/ccompat/v7"
FLOW = "rapid7_securado"
COOKIE = ".tmp_work/kb_cookies.txt"

ENTITIES = ["agent", "asset_group", "tag", "exploit", "malware_kit", "vulnerability_category",
            "vulnerability_exception", "vulnerability_reference", "site_organization",
            "asset_group_asset", "tag_asset", "tag_site", "asset_vulnerability_solution",
            "operating_system", "software", "vulnerability", "solution"]

# The 12 standard header fields, always present, always these types.
STANDARD = ["source_platform", "customer_tenant_organization", "source_object_type", "source_object_id",
            "extraction_timestamp", "source_event_update_timestamp", "api_endpoint_export_query_identity",
            "cursor_window", "payload_hash_fingerprint", "ingestion_run_batch_identity"]


def sh(args, timeout=90):
    return subprocess.run(["curl.exe", "--http1.1", "-k", "-sS"] + args,
                          capture_output=True, text=True, timeout=timeout).stdout


def kafbat_login():
    os.makedirs(".tmp_work", exist_ok=True)
    sh(["-c", COOKIE, f"{KAFBAT}/login", "-o", os.devnull])
    sh(["-b", COOKIE, "-c", COOKIE, "-X", "POST", f"{KAFBAT}/login",
        "--data-urlencode", f"username={KAFBAT_USER}", "--data-urlencode", f"password={KAFBAT_PASS}",
        "-o", os.devnull])


def read_messages(topic, limit=25):
    """Pull up to `limit` message values off a topic via Kafbat's SSE endpoint."""
    url = f"{KAFBAT}/api/clusters/local/topics/{urllib.parse.quote(topic, safe='')}/messages/v2?limit={limit}&mode=EARLIEST"
    raw = sh(["-b", COOKIE, url])
    out = []
    for line in raw.splitlines():
        line = line.strip()
        if not line.startswith("data:"):
            continue
        try:
            ev = json.loads(line[5:].strip())
        except json.JSONDecodeError:
            continue
        if ev.get("type") != "MESSAGE":
            continue
        val = (ev.get("message") or {}).get("value")
        if not val:
            continue
        try:
            out.append(json.loads(val))
        except json.JSONDecodeError:
            pass
    return out


# ---------------- type inference ----------------

def infer(value):
    """Return an Avro type for one concrete JSON value (never a union -- caller adds null)."""
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "long"
    if isinstance(value, float):
        return "double"
    if isinstance(value, str):
        return "string"
    return None  # null / unknown


def merge_type(a, b):
    """Widen two observed scalar types into one that can hold both."""
    if a is None:
        return b
    if b is None:
        return a
    if a == b:
        return a
    numeric = {"long", "double"}
    if a in numeric and b in numeric:
        return "double"
    return "string"      # anything genuinely mixed degrades to string


import re
_AVRO_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def avro_legal(name):
    """Avro field/record names must be [A-Za-z_][A-Za-z0-9_]* -- no dots, dashes, spaces."""
    return bool(_AVRO_NAME.match(name))


def all_keys_legal(dicts):
    keys = set()
    for d in dicts:
        keys.update(d.keys())
    return all(avro_legal(k) for k in keys)


def walk(records, path_prefix, name_hint):
    """
    Infer an Avro field list from a list of dict samples.
    Nested objects become nested records; arrays become arrays of their merged element type;
    arrays of objects become arrays of a nested record merged across every element seen.
    """
    keys = []
    for r in records:
        for k in r:
            if k not in keys:
                keys.append(k)

    fields = []
    for k in keys:
        # A key that isn't a legal Avro name can't be a field at all. Skipping is the only
        # option that still yields a valid schema -- surfaced loudly rather than silently dropped.
        if not avro_legal(k):
            print(f"  WARN: skipping top-level field with Avro-illegal name: {k!r}", file=sys.stderr)
            continue

        vals = [r[k] for r in records if k in r and r[k] is not None]
        if not vals:
            fields.append({"name": k, "type": ["null", "string"], "default": None})
            continue

        if all(isinstance(v, dict) for v in vals):
            # e.g. Rapid7's `cpe` object has keys "v2.2"/"v2.3" -- dots are illegal in Avro field
            # names, so model the whole object as a map to keep every key's data instead of
            # renaming (which would silently null out those values).
            if not all_keys_legal(vals):
                fields.append({"name": k, "type": ["null", {"type": "map", "values": ["null", "string"]}],
                               "default": None})
                continue
            sub = walk(vals, f"{path_prefix}_{k}", k)
            fields.append({"name": k, "type": ["null", {"type": "record",
                                                        "name": f"{path_prefix}_{k}",
                                                        "namespace": f"bronze.{FLOW}",
                                                        "fields": sub}], "default": None})
            continue

        if all(isinstance(v, list) for v in vals):
            flat = [item for v in vals for item in v if item is not None]
            if not flat:
                fields.append({"name": k, "type": ["null", {"type": "array", "items": ["null", "string"]}],
                               "default": None})
            elif all(isinstance(i, dict) for i in flat):
                if not all_keys_legal(flat):
                    fields.append({"name": k, "type": ["null", {"type": "array", "items":
                                   ["null", {"type": "map", "values": ["null", "string"]}]}],
                                   "default": None})
                    continue
                sub = walk(flat, f"{path_prefix}_{k}_item", k)
                fields.append({"name": k, "type": ["null", {"type": "array", "items": ["null", {
                    "type": "record", "name": f"{path_prefix}_{k}_item",
                    "namespace": f"bronze.{FLOW}", "fields": sub}]}], "default": None})
            else:
                t = None
                for i in flat:
                    t = merge_type(t, infer(i))
                fields.append({"name": k, "type": ["null", {"type": "array", "items": ["null", t or "string"]}],
                               "default": None})
            continue

        t = None
        for v in vals:
            t = merge_type(t, infer(v))
        fields.append({"name": k, "type": ["null", t or "string"], "default": None})
    return fields


def build_schema(entity, records):
    record_name = f"{FLOW}_{entity}_raw_avro"
    inferred = {f["name"]: f for f in walk(records, record_name, entity)}

    fields = []
    for s in STANDARD:
        fields.append({"name": s, "type": ["null", "string"], "default": None})
    fields.append({"name": "object_id", "type": ["null", "string"], "default": None})
    fields.append({"name": "ingest_ts", "type": ["null", "long"], "default": None})
    known = {f["name"] for f in fields}
    for name, f in inferred.items():
        if name not in known:
            fields.append(f)
    return {"type": "record", "name": record_name, "namespace": f"bronze.{FLOW}", "fields": fields}


def register(subject, schema):
    body = json.dumps({"schema": json.dumps(schema)})
    path = f".tmp_work/_reg_{subject.replace('.', '_')}.json"
    with open(path, "w", encoding="utf-8") as f:
        f.write(body)
    out = sh(["-X", "POST", "-H", "Content-Type: application/vnd.schemaregistry.v1+json",
              "--data-binary", f"@{path}", "-w", "\nHTTP:%{http_code}",
              f"{APICURIO}/subjects/{urllib.parse.quote(subject, safe='')}/versions"])
    os.remove(path)
    raw, code = out.rsplit("\nHTTP:", 1)
    return int(code.strip()[:3]), raw.strip()


def main():
    kafbat_login()
    only = sys.argv[1:] or ENTITIES
    report = {}
    for e in only:
        topic = f"bronze.{FLOW}.{e}__raw"
        recs = read_messages(topic)
        if not recs:
            report[e] = {"status": "NO DATA", "messages": 0}
            continue
        schema = build_schema(e, recs)
        subject = f"bronze.{FLOW}.{e}__raw.avro-value"
        code, resp = register(subject, schema)
        report[e] = {"status": "registered" if 200 <= code < 300 else f"FAILED {code}",
                     "messages": len(recs), "fields": len(schema["fields"]), "resp": resp[:120]}
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
