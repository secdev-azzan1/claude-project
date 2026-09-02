"""Backfill `block.config.sinkConfig` with the FULL Kafka Connect connector
config the compiler derives for it today, for every `kc`/`kafka_kc` block.

WHY: today the Connect connector config is built at COMPILE TIME from a
bound AppService plus derived values (`services/adapter/compiler/
connectors.py`); `block.config.sinkConfig` only ever stores the leftover,
user-tunable properties the compiler layers on top. The compiler is about to
change so `sinkConfig` IS the entire connector config, verbatim -- so every
existing block must be backfilled with exactly what `build_kafka_kc_connector`
/ `build_kc_connector` produce for it RIGHT NOW, before that switch flips, or
every production flow silently loses its sink settings the moment the
compiler stops deriving them.

This script calls `build_kafka_kc_connector()` / `build_kc_connector()`
DIRECTLY -- never `compile_flow()`, which raises on an unapproved schema or a
placement violation, i.e. it would skip exactly the flows most in need of
backfilling.

Idempotent migration: a block is skipped once its `sinkConfig` already
contains a `topics` key -- the marker only this backfill (or the future
compiler) ever writes. Re-running this script should result in zero writes.

--dry-run: prints, per affected block, a readable diff (secret-looking values
masked) of what WOULD be written, and writes nothing to Mongo. Without
--dry-run, the entire flows collection is dumped to a timestamped JSON backup
next to this script FIRST, and the script refuses to write anything if that
backup can't be produced and verified.
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

from models.adapter import AppService, Flow, FlowBlock, PlatformConnection
from services.adapter.common import COLLECTIONS
from services.adapter.compiler import CompileContext, CompileError, connectors
from services.adapter.naming import derive_topic_name, tokenize

# Load the same .env files backend/server.py loads, so a plain `python
# migrations/backfill_sink_config.py` resolves MONGO_URL/DB_NAME to the same
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


def _mask(key: str, value: Any) -> Any:
    return "***" if _SECRET_KEY_RE.search(key) else value


# --------------------------------------------------------------------- loaders


def _load_services() -> Dict[str, AppService]:
    """Every AppService, keyed by id, read from the RAW Mongo document --
    NOT via `.redact()` -- so real secret values (see `models/adapter/
    _secrets.py`'s SECRET_CONFIG_KEYS) come across intact into the connector
    config the same way they would at real compile/deploy time. Mirrors
    `services/adapter/deployer/lifecycle.py::_load_services`, which also
    never redacts."""
    services: Dict[str, AppService] = {}
    for doc in db[COLLECTIONS.services].find({}, {"_id": 0}):
        try:
            svc = AppService(**doc)
        except Exception as exc:
            print(f"  WARNING: skipping unparsable service doc {doc.get('id')!r}: {exc}")
            continue
        services[svc.id] = svc
    return services


def _load_connections() -> Dict[str, PlatformConnection]:
    """ACTIVE connections keyed by `type`, exactly like `lifecycle.py`'s
    `_build_compile_context` (`{c.type: c for c in connections if
    c.active}`). This is what makes `ctx.connection_config("apicurio")`
    resolve to the real registry URL the Avro converter needs
    (`connectors.py::_avro_converter_props`)."""
    conns: Dict[str, PlatformConnection] = {}
    for doc in db[COLLECTIONS.connections].find({}, {"_id": 0}):
        try:
            conn = PlatformConnection(**doc)
        except Exception as exc:
            print(f"  WARNING: skipping unparsable connection doc {doc.get('id')!r}: {exc}")
            continue
        if conn.active:
            conns[conn.type] = conn
    return conns


# ----------------------------------------------------------------- topic/entity


def _resolve_kafka_kc(flow: Flow, block: FlowBlock) -> Tuple[str, str]:
    """-> (topic, entity_token) for a `kafka_kc` block, exactly like
    `blocks_kafka_kc.py::compile_publish` (`derive_topic_name(flow,
    block).value`). A block with no entity label derives no real topic
    (`DerivedName.value == "raw.<entity missing>"`) -- that is not something
    to guess at, so it is treated as an unresolvable topic and raised as a
    CompileError-shaped failure instead of being written."""
    derived = derive_topic_name(flow, block)
    if not derived.value or derived.value.startswith("raw.<"):
        raise CompileError(
            f"block {block.id!r} has no usable derived topic "
            f"({derived.warning or 'derive_topic_name returned no value'})"
        )
    entity_token = tokenize(block.entity or block.name)
    return derived.value, entity_token


def _resolve_kc(flow: Flow, block: FlowBlock) -> Tuple[str, str, bool]:
    """-> (topic, entity_token, topic_is_governed) for a `kc` block, ported
    verbatim from `compile_flow.py::_compile_kc`."""
    attach_topic_id = block.config.get("attachTopicId")
    topic = next((t for t in flow.topics if t.id == attach_topic_id), None)
    if topic is None:
        raise CompileError(f'kc block {block.id!r} ("{block.name}") is not attached to a topic')
    owner = next((b for b in flow.blocks if b.id == topic.writerBlockId), None)
    topic_is_governed = bool(owner and owner.adapter == "kafka_kc")
    entity_token = tokenize(block.entity or block.name)
    return topic.name, entity_token, topic_is_governed


# ------------------------------------------------------------------------ diff


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


def _dump_backup(flow_docs: List[Dict[str, Any]]) -> Path:
    """Dump the ENTIRE flows collection (raw docs, as stored -- `_id` and
    all) to a timestamped JSON file next to this script, then read it back
    to make sure the dump is actually usable. Any failure here must abort
    the whole run before a single write happens."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = Path(__file__).resolve().parent / f"flows_backup_{ts}.json"
    path.write_text(json_util.dumps(flow_docs, indent=2), encoding="utf-8")
    round_tripped = json_util.loads(path.read_text(encoding="utf-8"))
    if len(round_tripped) != len(flow_docs):
        raise RuntimeError(f"backup verification failed: wrote {len(flow_docs)} docs, read back {len(round_tripped)}")
    return path


# ------------------------------------------------------------------------ main


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="Print the plan only; write nothing to Mongo.")
    args = parser.parse_args()

    print(f"Connecting to {MONGO_URL} / db={DB_NAME} (dry_run={args.dry_run})")

    services = _load_services()
    connections = _load_connections()
    print(f"Loaded {len(services)} services, {len(connections)} active connections.")
    if "apicurio" not in connections:
        print("  WARNING: no ACTIVE apicurio connection found -- the Avro converter registry URL will come "
              "out empty for any governed sink, exactly as the compiler would produce today.")

    ctx = CompileContext(services=services, connections=connections, gateway_proxies={}, approved_schemas={})

    flow_docs = list(db[COLLECTIONS.flows].find({}))
    print(f"Found {len(flow_docs)} flow documents.")

    if not flow_docs:
        print("\nFATAL: 0 flow documents found in this database. This almost certainly means "
              "MONGO_URL/DB_NAME point at the wrong deployment, NOT that the flows collection "
              "is genuinely empty. Refusing to run -- writing nothing and skipping the summary.")
        print(f"  MONGO_URL = {MONGO_URL!r}")
        print(f"  DB_NAME   = {DB_NAME!r}")
        return 1

    if not args.dry_run:
        try:
            backup_path = _dump_backup(flow_docs)
        except Exception as exc:
            print(f"FATAL: could not write/verify the flows backup -- refusing to write anything. {exc}")
            return 1
        print(f"Backed up {len(flow_docs)} flow docs to {backup_path}")

    flows_scanned = 0
    sink_blocks_found = 0
    blocks_skipped = 0
    blocks_written = 0
    failures: List[Dict[str, str]] = []

    for raw_flow in flow_docs:
        flows_scanned += 1
        flow_doc = dict(raw_flow)
        flow_doc.pop("_id", None)
        try:
            flow = Flow(**flow_doc)
        except Exception as exc:
            failures.append({"flow": str(flow_doc.get("name", "")), "block": "", "reason": f"flow document failed to parse: {exc}"})
            continue

        flow_token = tokenize(flow.name)

        for block in flow.blocks:
            if block.adapter not in ("kc", "kafka_kc"):
                continue
            sink_blocks_found += 1

            existing_sink = block.config.get("sinkConfig") or {}
            if "topics" in existing_sink:
                blocks_skipped += 1
                continue

            try:
                if block.adapter == "kafka_kc":
                    topic, entity_token = _resolve_kafka_kc(flow, block)
                    spec = connectors.build_kafka_kc_connector(
                        flow=flow, block=block, ctx=ctx, flow_token=flow_token, topic=topic, entity_token=entity_token,
                    )
                else:
                    topic, entity_token, topic_is_governed = _resolve_kc(flow, block)
                    spec = connectors.build_kc_connector(
                        flow=flow, block=block, ctx=ctx, flow_token=flow_token, topic=topic,
                        entity_token=entity_token, topic_is_governed=topic_is_governed,
                    )
            except CompileError as exc:
                failures.append({"flow": flow.name, "block": block.id, "reason": str(exc)})
                continue
            except Exception as exc:  # noqa: BLE001 - never guess; record and move on
                failures.append({"flow": flow.name, "block": block.id, "reason": f"unexpected error: {exc!r}"})
                continue

            new_sink = spec.config
            added, changed, removed = _diff(existing_sink, new_sink)

            print(f"\nflow={flow.id!r} ({flow.name!r})  block={block.id!r}  adapter={block.adapter!r}")
            print(f"  keys: {len(new_sink)} total ({len(added)} added, {len(changed)} changed, {len(removed)} removed)")
            _print_diff(existing_sink, new_sink)

            if args.dry_run:
                blocks_written += 1
                continue

            # Targeted update of ONLY this block's config.sinkConfig via the
            # positional operator -- never a whole-document rewrite, never
            # touches serviceId/sinkServiceId or any other field, and
            # deliberately does NOT bump updatedAt: this is a migration
            # backfill, not a user edit.
            result = db[COLLECTIONS.flows].update_one(
                {"id": flow.id, "blocks.id": block.id},
                {"$set": {"blocks.$.config.sinkConfig": new_sink}},
            )
            if result.modified_count != 1:
                failures.append({
                    "flow": flow.name, "block": block.id,
                    "reason": f"update_one did not modify exactly one document (matched={result.matched_count}, modified={result.modified_count})",
                })
                continue
            blocks_written += 1

    print("\n=== Summary ===")
    print(f"Database:                              {DB_NAME}")
    print(f"Flows scanned:                        {flows_scanned}")
    print(f"Sink blocks found:                     {sink_blocks_found}")
    print(f"Blocks skipped (already backfilled):   {blocks_skipped}")
    written_label = "would write" if args.dry_run else "written    "
    print(f"Blocks {written_label}:                 {blocks_written}")
    print(f"Blocks failed:                          {len(failures)}")

    if failures:
        print("\n--- Failures ---")
        for f in failures:
            print(f"  flow={f['flow']!r} block={f['block']!r}: {f['reason']}")

    return 1 if failures else 0


if __name__ == "__main__":
    try:
        exit_code = main()
    except Exception as e:
        print(f"ERROR: {e}")
        raise
    sys.exit(exit_code)
