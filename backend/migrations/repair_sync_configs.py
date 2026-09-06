"""Reconcile drift between `block.config.sinkConfig` and `sync["config"]` for
every Kafka Connect sink linked to a live flow block -- in WHICHEVER
direction actually has the richer config, not a fixed one.

WHY (measured on production data, 2026-09-02): a sink's connector config
exists in two places that can diverge -- `block.config.sinkConfig` and the
linked sync record's `config`. Across 63 linked sinks, both directions of
drift turned out to be real:
  -  4: the BLOCK is richer (the sync holds a stale 2-key copy) -- the block
       is correct, so the SYNC needs enriching.
  -  8: identical except the sync also carries a leftover `name` key --
       harmless once `name` is excluded from the comparison (see below).
  - 51: the SYNC is richer by 12 material keys (`iceberg.catalog`,
       `iceberg.control.topic`, `iceberg.control.group-id-prefix`,
       `iceberg.tables.evolve-schema-enabled` / `schema-case-insensitive` /
       `schema-force-optional`, `errors.tolerance` / `errors.log.enable` /
       `errors.log.include.messages`, `transforms` +
       `transforms.Lowercase.type`) -- these 51 sync records were adopted
       straight off connectors actually running on the cluster and hold the
       observed truth; their blocks were backfilled from the v2 compiler,
       which emits a poorer config. THE BLOCK IS LOSSY here, so the BLOCK
       needs enriching -- the opposite direction from the 4-case above.
  -  0: genuinely conflicting (neither side a subset of the other).

An earlier version of this script only ever copied block -> sync, on the
(then-true) assumption that the block was always the freshly-compiled,
authoritative side. That assumption broke the moment the compiler became a
pass-through and `authoritative_sink_config` (routers/kafka_connect.py)
started making the block win unconditionally for Start/Apply: pushing a
block's config over one of the 51 richer connectors would silently drop
`transforms.Lowercase.type` -- a field-name transform that changes the data
actually written to Iceberg (this is exactly how a prior connector broke
with "Must specify Iceberg catalog properties" once a block missing
`iceberg.catalog*` got pushed live). So the direction can no longer be
assumed; it must be DECIDED, per sink, by comparing the two configs' key
sets and picking whichever side is the strict superset -- that side is
demonstrably not missing anything the other side has, which is the only
safe, general definition of "richer" available from the data itself:

  - sync keys are a STRICT SUPERSET of block keys -> the block is missing
    real settings -> ENRICH THE BLOCK: write the sync's config into
    `block.config.sinkConfig`, and also set the sync's own `config` to that
    same map so both records read identically afterwards.
  - block keys are a STRICT SUPERSET of sync keys -> the sync is the stale
    side -> write the block's `sinkConfig` into the sync's `config`.
  - equal key sets but differing values -> a real conflict (something was
    edited on one side and not the other): leave both alone and report it.
  - neither is a subset of the other -> also a conflict: leave both alone
    and report it.
  - identical (after excluding `name`, see below) -> skip, nothing to do.

`name` is excluded from every comparison AND from whatever gets written to
either side: Kafka Connect injects `name` into a connector's config, but it
is the connector's own identity -- derived by the platform from the flow and
block, and set separately by the compiler via `ConnectorSpec.name`. Storing
it in `sinkConfig` (or copying it back into the sync) would bake in a copy
that goes stale the instant anything is renamed. A pair that differs ONLY by
`name` is therefore treated as identical and skipped rather than churned
(this is the 8-case above).

Because this writes to TWO collections (`flows_v2` and
`kafka_connect_syncs_v2`), BOTH are dumped to timestamped JSON backups next
to this script before any write, each dump is read back and length-checked,
and the whole run aborts writing nothing if either backup fails.

Idempotent: a re-run sees the two sides already agree (name excluded) for
every sink this script touched, so every one of them reports "identical" and
nothing is written.

--dry-run: writes NOTHING (and skips the backup step entirely, since nothing
is at risk) and prints a per-record diff (secret-looking values masked) of
what WOULD change, plus the direction chosen.
"""
import sys
from pathlib import Path

# Bootstrap sys.path to allow service imports from standalone execution
BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import argparse
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from bson import json_util
from pymongo import MongoClient

from services.adapter.common import COLLECTIONS

# Load the same .env files backend/server.py loads, so a plain `python
# migrations/repair_sync_configs.py` resolves MONGO_URL/DB_NAME to the same
# real database the server talks to, instead of falling back to the
# hardcoded localhost/nif_abstractor defaults below. dotenv does NOT override
# variables already present in the environment, so an explicit env var still
# wins over whatever is in the .env files.
from dotenv import load_dotenv
load_dotenv(BACKEND_DIR / ".env")
load_dotenv(BACKEND_DIR.parent / ".env")

MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "nif_abstractor")

client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=5000)
db = client[DB_NAME]

# Case-insensitive: matches password/pass, secret, token, credential,
# api(-|.)key, access(-|.)key, private(-|.)key -- anything that looks like it
# could hold a real credential gets printed as *** in the dry-run diff, never
# the live value.
_SECRET_KEY_RE = re.compile(r"(pass(word)?|secret|token|credential|api.?key|access.?key|private.?key)", re.IGNORECASE)

# Kafka Connect injects this into every config it returns; it is the
# connector's identity, not a tunable setting, and must never be compared or
# copied like the rest of the config (see module docstring).
_IDENTITY_KEY = "name"


def _mask(key: str, value: Any) -> Any:
    return "***" if _SECRET_KEY_RE.search(key) else value


def _without_identity(config: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in config.items() if k != _IDENTITY_KEY}


def _diff(old: Dict[str, Any], new: Dict[str, Any]) -> Tuple[List[str], List[str], List[str]]:
    added = sorted(k for k in new if k not in old)
    changed = sorted(k for k in new if k in old and old[k] != new[k])
    removed = sorted(k for k in old if k not in new)
    return added, changed, removed


def _print_diff(old: Dict[str, Any], new: Dict[str, Any]) -> None:
    added, changed, removed = _diff(old, new)
    for k in added:
        print(f"      + {k} = {_mask(k, new[k])!r}")
    for k in changed:
        print(f"      ~ {k}: {_mask(k, old[k])!r} -> {_mask(k, new[k])!r}")
    for k in removed:
        print(f"      - {k} (was {_mask(k, old[k])!r})")


# ---------------------------------------------------------------------- backup


def _dump_backup(label: str, docs: List[Dict[str, Any]]) -> Path:
    """Dump `docs` (raw, as stored -- `_id` and all) to a timestamped JSON
    file next to this script, then read it back to make sure the dump is
    actually usable. Any failure here must abort the whole run before a
    single write happens."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = Path(__file__).resolve().parent / f"{label}_backup_{ts}.json"
    path.write_text(json_util.dumps(docs, indent=2), encoding="utf-8")
    round_tripped = json_util.loads(path.read_text(encoding="utf-8"))
    if len(round_tripped) != len(docs):
        raise RuntimeError(f"{label} backup verification failed: wrote {len(docs)} docs, read back {len(round_tripped)}")
    return path


# ----------------------------------------------------------------- reconcile


def _find_linked_blocks(flow_docs: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """-> {sync_id: {"flow_id", "block_id", "sink_config"}} for every block
    that names a sync via `config.syncId`, exactly like
    `routers/kafka_connect.py`'s own lookups. A sync id colliding across two
    blocks would be a data bug elsewhere; the last one found simply wins
    here, same as the router's own scans."""
    by_sync_id: Dict[str, Dict[str, Any]] = {}
    for flow in flow_docs:
        flow_id = flow.get("id")
        for block in flow.get("blocks") or []:
            sync_id = (block.get("config") or {}).get("syncId")
            if sync_id:
                by_sync_id[str(sync_id)] = {
                    "flow_id": flow_id,
                    "block_id": block.get("id"),
                    "sink_config": dict((block.get("config") or {}).get("sinkConfig") or {}),
                }
    return by_sync_id


def _decide_direction(sync_config: Dict[str, Any], block_config: Dict[str, Any]) -> str:
    """-> one of "identical", "enrich_block", "enrich_sync",
    "conflict_equal_keys", "conflict_disjoint". Both inputs must already have
    `name` excluded (see `_without_identity`)."""
    if sync_config == block_config:
        return "identical"
    sync_keys, block_keys = set(sync_config), set(block_config)
    if sync_keys == block_keys:
        return "conflict_equal_keys"
    if sync_keys > block_keys:
        return "enrich_block"
    if block_keys > sync_keys:
        return "enrich_sync"
    return "conflict_disjoint"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="Print the plan only; write nothing to Mongo.")
    args = parser.parse_args()

    print(f"Connecting to {MONGO_URL} / db={DB_NAME} (dry_run={args.dry_run})")

    flow_docs = list(db[COLLECTIONS.flows].find({}))
    print(f"Found {len(flow_docs)} flow documents.")

    if not flow_docs:
        print("\nFATAL: 0 flow documents found in this database. This almost certainly means "
              "MONGO_URL/DB_NAME point at the wrong deployment, NOT that the flows collection "
              "is genuinely empty. Refusing to run -- writing nothing and skipping the summary.")
        print(f"  MONGO_URL = {MONGO_URL!r}")
        print(f"  DB_NAME   = {DB_NAME!r}")
        return 1

    sync_docs = list(db[COLLECTIONS.kafka_connect_syncs].find({}))
    print(f"Found {len(sync_docs)} sync documents.")

    if not args.dry_run:
        try:
            flows_backup_path = _dump_backup("flows", flow_docs)
            syncs_backup_path = _dump_backup("kafka_connect_syncs", sync_docs)
        except Exception as exc:
            print(f"FATAL: could not write/verify a backup -- refusing to write anything. {exc}")
            return 1
        print(f"Backed up {len(flow_docs)} flow docs to {flows_backup_path}")
        print(f"Backed up {len(sync_docs)} sync docs to {syncs_backup_path}")

    block_by_sync_id = _find_linked_blocks(flow_docs)

    counts = {
        "identical": 0,
        "enrich_block": 0,
        "enrich_sync": 0,
        "conflict_equal_keys": 0,
        "conflict_disjoint": 0,
        "no_linked_block": 0,
    }
    conflicts: List[Dict[str, str]] = []
    failures: List[Dict[str, str]] = []

    for sync in sync_docs:
        sync_id = sync.get("id")
        name = sync.get("name") or sync_id
        link = block_by_sync_id.get(str(sync_id))
        if link is None:
            # Adopted/unlinked sync -- there is no live block to reconcile
            # against, it must keep using its own stored config, same as the
            # runtime fallback in `authoritative_sink_config`.
            counts["no_linked_block"] += 1
            continue

        sync_config = _without_identity(dict(sync.get("config") or {}))
        block_config = _without_identity(link["sink_config"])
        direction = _decide_direction(sync_config, block_config)
        counts[direction] += 1

        if direction == "identical":
            continue

        if direction in ("conflict_equal_keys", "conflict_disjoint"):
            reason = (
                "same keys, differing values" if direction == "conflict_equal_keys"
                else "neither config's keys are a subset of the other's"
            )
            print(f"\nCONFLICT sync={sync_id!r} ({name!r}): {reason} -- left unchanged.")
            print(f"  sync  keys: {sorted(sync_config)}")
            print(f"  block keys: {sorted(block_config)}")
            conflicts.append({"sync": str(sync_id), "name": str(name), "reason": reason})
            continue

        print(f"\nsync={sync_id!r} ({name!r})  direction={direction}")
        if direction == "enrich_block":
            print("  sync has keys the block is missing -- enriching the block (and the sync's own "
                  "config is set to the same map so both agree afterwards):")
            _print_diff(block_config, sync_config)
        else:
            print("  block has keys the sync is missing -- enriching the sync:")
            _print_diff(sync_config, block_config)

        if args.dry_run:
            continue

        if direction == "enrich_block":
            flow_result = db[COLLECTIONS.flows].update_one(
                {"id": link["flow_id"], "blocks.id": link["block_id"]},
                {"$set": {"blocks.$.config.sinkConfig": sync_config}},
            )
            if flow_result.modified_count != 1:
                failures.append({
                    "sync": str(sync_id),
                    "reason": f"flow block update_one did not modify exactly one document (matched={flow_result.matched_count}, modified={flow_result.modified_count})",
                })
                continue
            sync_result = db[COLLECTIONS.kafka_connect_syncs].update_one(
                {"id": sync_id}, {"$set": {"config": sync_config}},
            )
            if sync_result.modified_count != 1:
                failures.append({
                    "sync": str(sync_id),
                    "reason": f"sync update_one did not modify exactly one document (matched={sync_result.matched_count}, modified={sync_result.modified_count})",
                })
                continue
        else:  # enrich_sync
            sync_result = db[COLLECTIONS.kafka_connect_syncs].update_one(
                {"id": sync_id}, {"$set": {"config": block_config}},
            )
            if sync_result.modified_count != 1:
                failures.append({
                    "sync": str(sync_id),
                    "reason": f"sync update_one did not modify exactly one document (matched={sync_result.matched_count}, modified={sync_result.modified_count})",
                })
                continue

    print("\n=== Summary ===")
    print(f"Database:                              {DB_NAME}")
    print(f"Sync documents scanned:                {len(sync_docs)}")
    print(f"No live linked block (skipped):        {counts['no_linked_block']}")
    print(f"Already identical (name excluded):     {counts['identical']}")
    action_label = "would " if args.dry_run else ""
    print(f"Blocks {action_label}enriched from sync:        {counts['enrich_block']}")
    print(f"Syncs  {action_label}enriched from block:       {counts['enrich_sync']}")
    print(f"Conflicts (equal keys, diff values):   {counts['conflict_equal_keys']}")
    print(f"Conflicts (disjoint, neither subset):  {counts['conflict_disjoint']}")
    print(f"Failures:                              {len(failures)}")

    if conflicts:
        print("\n--- Conflicts (left unchanged; needs a human) ---")
        for c in conflicts:
            print(f"  sync={c['sync']!r} ({c['name']!r}): {c['reason']}")

    if failures:
        print("\n--- Failures ---")
        for f in failures:
            print(f"  sync={f['sync']!r}: {f['reason']}")

    return 1 if failures else 0


if __name__ == "__main__":
    try:
        exit_code = main()
    except Exception as e:
        print(f"ERROR: {e}")
        raise
    sys.exit(exit_code)
