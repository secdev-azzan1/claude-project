"""Single source of truth for "which Kafka Connect connector names belong
to this flow" -- deliberately NOT derived from `flow_doc["runtimeScopeMap"]`.

The scope map is compiler output, written by `deploy` and nulled by
`undeploy` (see `services/adapter/deployer/lifecycle.py`). Any code that
reads connector names out of it can only ever see connectors belonging to
a flow that is *currently deployed*. That made every other lifecycle verb
that needs to touch a flow's connectors -- `undeploy` itself (which needs
the names *before* it clears the map), `delete` (which may run against an
already-undeployed, map-already-null flow), and runtime status reads for a
flow that's mid-teardown or whose scope map was lost to a crash -- blind to
connectors that very much still exist on the Kafka Connect cluster. That's
how connectors got orphaned: nothing left alive still knew their names.

This module recomputes the same answer two independent ways and unions
them, so a gap in one path doesn't silently drop connectors:

  1. Walk the flow's own blocks. For each kc/kafka_kc block, prefer the
     *recorded* name on its linked `KafkaConnectSync` (some flows -- e.g.
     ones that predate the current naming scheme -- carry legacy names
     like `bronze.fortisiem.device__raw.avro__iceberg` that the current
     deterministic derivation cannot reproduce), and only fall back to
     deriving a name when no sync record exists yet.
  2. Independently, pull every `KafkaConnectSync` whose *stored*
     `linked_flow_id` points at this flow -- including syncs whose block
     was deleted out from under them (e.g. edited while the Connect
     cluster was unreachable, so the connector itself was never cleaned
     up). Path 1 alone would miss these entirely, since it only ever
     walks blocks that still exist.

Order is stable (first-seen wins) so callers that log/display these lists
get deterministic output across calls.
"""

from __future__ import annotations

from typing import Any, Dict, List

from services.adapter.common import COLLECTIONS
from services.adapter.naming import tokenize

_KC_ADAPTERS = ("kc", "kafka_kc")


def connector_names_for_flow(flow_doc: Dict[str, Any], syncs: List[Dict[str, Any]]) -> List[str]:
    """Pure function: derive the union of connector names for `flow_doc`
    given the full list of `KafkaConnectSync` documents (as dicts). See
    module docstring for the two contributing paths.
    """
    flow_id = flow_doc.get("id")
    flow_name = flow_doc.get("name") or ""

    # Index syncs by their own id. `linked_block_id` is a stored convenience
    # field that `list_syncs` recomputes on read and carries no flow scoping
    # -- block ids are only unique *within* a flow, so a dict keyed by block
    # id collides across flows and returns the wrong flow's connector. Each
    # block instead names its sync explicitly via `config.syncId`, which is
    # globally unique (it's the sync's own id), so look up through that.
    sync_by_id: Dict[str, Dict[str, Any]] = {}
    for sync in syncs:
        sync_id = sync.get("id")
        if sync_id:
            sync_by_id[sync_id] = sync

    names: List[str] = []
    seen = set()

    def _add(name: str) -> None:
        if name and name not in seen:
            seen.add(name)
            names.append(name)

    # Path 1: walk the flow's live blocks.
    for block in flow_doc.get("blocks") or []:
        if block.get("adapter") not in _KC_ADAPTERS:
            continue
        block_id = block.get("id")
        sync_id = (block.get("config") or {}).get("syncId")
        sync = sync_by_id.get(sync_id) if sync_id else None
        recorded = (sync or {}).get("connector_name")
        if recorded:
            _add(recorded)
        else:
            derived = f"{tokenize(flow_name)}.{block_id}.{block.get('adapter')}"
            _add(derived)

    # Path 2: every sync stored as belonging to this flow, regardless of
    # whether its block still exists.
    for sync in syncs:
        if sync.get("linked_flow_id") == flow_id:
            recorded = sync.get("connector_name")
            if recorded:
                _add(recorded)

    return names


async def resolve_flow_connector_names(db, flow_doc: Dict[str, Any]) -> List[str]:
    """Async wrapper: load `KafkaConnectSync` docs for `db` and delegate to
    `connector_names_for_flow`. Follows the same "fetch the whole (small)
    collection, filter in Python" pattern already used for services/schemas/
    connections elsewhere in the deployer (see `_load_services` etc. in
    `services/adapter/deployer/lifecycle.py`) -- there is no per-flow index
    on this collection, and it is not expected to be large.

    The `db[COLLECTIONS.kafka_connect_syncs]` access is wrapped narrowly in
    `try/except AttributeError`. A real Motor/MongoDB database's
    `__getitem__` never raises for a collection name that doesn't exist yet
    (Mongo collections are created lazily; querying a nonexistent one just
    yields no documents) -- so this branch can only ever trigger against a
    hand-rolled test double whose `__getitem__` does `getattr(self, name)`
    with no default and simply hasn't been given this attribute. Falling
    back to an empty sync list in that case keeps this resolver usable by
    older test fixtures that predate `kafka_connect_syncs_v2` without
    masking any real lookup failure (the wrap covers only the subscript,
    not the `.find()` call itself).
    """
    try:
        collection = db[COLLECTIONS.kafka_connect_syncs]
    except AttributeError:
        syncs: List[Dict[str, Any]] = []
    else:
        syncs = await collection.find({}, {"_id": 0}).to_list(None)

    return connector_names_for_flow(flow_doc, syncs)
