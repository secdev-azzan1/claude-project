"""Backfill `bodySource: "template"` onto every existing `http · write` block.

WHY: `bodySource` is a new, REQUIRED setting on http writes that says what the
request body actually is -- the record flowing through the block (`"record"`,
which is what a kafka write does) or a hand-written body template
(`"template"`). Before it existed, the compiler unconditionally inserted a
`render_body` ReplaceText with "Always Replace", so the record was ALWAYS
discarded and the template always won. Two consequences that this setting
exists to remove:

  - sending a record required rebuilding it by hand, one `extract` transform
    per field to copy values out to attributes plus a template to copy them
    back in -- while `InvokeHTTP` was already configured with
    "Request Body Enabled": "true" and would have sent the record untouched;
  - a BLANK template silently POSTed an EMPTY body. NiFi recorded a
    successful request, the DLQ stayed empty and every counter read green,
    while the receiving system got nothing.

`bodySource` deliberately has NO default -- `validation.py` refuses to deploy
a write that has not chosen. An absent value quietly meaning one thing or the
other is precisely the trap being removed, so it must not be reintroduced as
a fallback. That makes this backfill mandatory: without it, every existing
http write becomes invalid.

WHAT IT DOES: sets `bodySource: "template"` on every http write block that
does not already have one. `"template"` (never `"record"`) is the only correct
value here, because it reproduces the pre-change behaviour byte-for-byte -- the
compiler emitted `render_body` for these blocks and will continue to. Nothing
needs redeploying and no compiled output changes.

Measured before writing this script: 11 http write blocks exist, and ALL 11
carry a non-empty body template (10 FortiSIEM query-POSTs in Draft flows, plus
one Stopped test flow). So no block is being handed a setting that contradicts
its own config, and none would trip the new "a body template is required"
validation rule after the backfill.

A block that somehow has NO template is reported and SKIPPED rather than
guessed at: `"template"` would make it invalid (empty template) and `"record"`
would silently change what it sends. Either way a human should decide.

--dry-run: writes NOTHING (and skips the backup entirely, since nothing is at
risk) and prints exactly which blocks would change.
"""
import sys
from pathlib import Path

# Bootstrap sys.path to allow service imports from standalone execution
BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import argparse
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

from bson import json_util
from pymongo import MongoClient

from services.adapter.common import COLLECTIONS

# Same .env resolution as backend/server.py, so a plain
# `python migrations/backfill_body_source.py` talks to the real database
# rather than the localhost default below. dotenv does not override variables
# already set in the environment, so an explicit env var still wins.
from dotenv import load_dotenv
load_dotenv(BACKEND_DIR / ".env")
load_dotenv(BACKEND_DIR.parent / ".env")

MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "nif_abstractor")

client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=5000)
db = client[DB_NAME]


def _dump_backup(label: str, docs: List[Dict[str, Any]]) -> Path:
    """Dump `docs` raw (`_id` and all) to a timestamped file next to this
    script, then read it back to confirm the dump is usable. Any failure here
    aborts the run before a single write happens."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = Path(__file__).resolve().parent / f"{label}_backup_{ts}.json"
    path.write_text(json_util.dumps(docs, indent=2), encoding="utf-8")
    round_tripped = json_util.loads(path.read_text(encoding="utf-8"))
    if len(round_tripped) != len(docs):
        raise RuntimeError(f"{label} backup verification failed: wrote {len(docs)} docs, read back {len(round_tripped)}")
    return path


def _is_http_write(block: Dict[str, Any]) -> bool:
    return block.get("adapter") == "http" and block.get("mode") == "write"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="Print the plan only; write nothing to Mongo.")
    args = parser.parse_args()

    print(f"Connecting to {MONGO_URL} / db={DB_NAME} (dry_run={args.dry_run})")

    flow_docs = list(db[COLLECTIONS.flows].find({}))
    print(f"Found {len(flow_docs)} flow documents.")

    if not flow_docs:
        print("\nFATAL: 0 flow documents found in this database. That almost certainly means "
              "MONGO_URL/DB_NAME point at the wrong deployment, NOT that the flows collection "
              "is genuinely empty. Refusing to run -- writing nothing.")
        print(f"  MONGO_URL = {MONGO_URL!r}")
        print(f"  DB_NAME   = {DB_NAME!r}")
        return 1

    planned: List[tuple] = []
    skipped_no_template: List[tuple] = []
    already_set = 0
    total_writes = 0

    for flow in flow_docs:
        for block in flow.get("blocks") or []:
            if not _is_http_write(block):
                continue
            total_writes += 1
            config = block.get("config") or {}
            existing = str(config.get("bodySource") or "").strip()
            if existing:
                already_set += 1
                continue
            template = str(config.get("bodyTemplate") or "").strip()
            row = (flow.get("id"), flow.get("name"), flow.get("state"), block.get("id"))
            if not template:
                skipped_no_template.append(row)
            else:
                planned.append(row)

    print(f"\nhttp write blocks: {total_writes} total, {already_set} already set, "
          f"{len(planned)} to backfill, {len(skipped_no_template)} skipped.")

    if planned:
        print("\nWould set bodySource=\"template\":")
        for fid, fname, state, bid in planned:
            print(f'  {str(fname)[:38]:<38} [{state}] block={bid}')

    if skipped_no_template:
        print("\nSKIPPED -- http write with NO body template. Left untouched on purpose: "
              "\"template\" would make it invalid and \"record\" would change what it sends. "
              "Decide per block and set it in the UI:")
        for fid, fname, state, bid in skipped_no_template:
            print(f'  {str(fname)[:38]:<38} [{state}] block={bid}')

    if not planned:
        print("\nNothing to do.")
        return 0

    if args.dry_run:
        print("\nDry run -- nothing written.")
        return 0

    try:
        backup_path = _dump_backup("flows", flow_docs)
    except Exception as exc:
        print(f"FATAL: could not write/verify a backup -- refusing to write anything. {exc}")
        return 1
    print(f"\nBacked up {len(flow_docs)} flow docs to {backup_path}")

    updated_blocks = 0
    updated_flows = 0
    target_flow_ids = {fid for fid, _, _, _ in planned}
    for flow in flow_docs:
        if flow.get("id") not in target_flow_ids:
            continue
        blocks = flow.get("blocks") or []
        changed = False
        for block in blocks:
            if not _is_http_write(block):
                continue
            config = block.get("config") or {}
            if str(config.get("bodySource") or "").strip():
                continue
            if not str(config.get("bodyTemplate") or "").strip():
                continue
            config["bodySource"] = "template"
            block["config"] = config
            changed = True
            updated_blocks += 1
        if changed:
            db[COLLECTIONS.flows].update_one({"id": flow["id"]}, {"$set": {"blocks": blocks}})
            updated_flows += 1

    print(f"Updated {updated_blocks} block(s) across {updated_flows} flow(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
