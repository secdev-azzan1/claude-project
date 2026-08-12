"""Line-by-line port of frontend/src/prototype/legality.ts -- the
placement-rule engine (rules R1-R8) -- plus `hostsTest` (ported from
frontend/src/components/flow-builder/BlockForm.tsx, the only place it is
defined on the frontend) and `validate_placement`, a new whole-flow
structural check described below.

Every "+ add block" menu in the frontend UI is a rendering of
`compute_add_menu()`; `validate_placement()` is this port's server-side
answer to "does this flow's block tree, as persisted, actually obey R1-R8"
-- there is no single TS function that does this (the frontend only ever
*constructs* legal trees, one `canReparent`/`computeAddMenu` call at a time,
so it never needed to re-validate an already-built tree from scratch). The
server does need this, both to catch flows written by a future non-UI client
and as a deploy-time backstop, so it is assembled here from the same rule
predicates (`isTerminal`, `isRawBranch`, `computeTopicMenu`, `flowHasTrigger`)
the rest of this module already exposes.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import List, Optional

from models.adapter import Flow, FlowBlock, FlowTopic

# --------------------------------------------------------------------- entries


@dataclass
class AddMenuEntry:
    key: str
    adapter: Optional[str] = None
    mode: Optional[str] = None
    label: str = ""
    description: str = ""
    disabledReason: Optional[str] = None
    futureScope: bool = False


FUTURE_SCOPE: List[AddMenuEntry] = [
    AddMenuEntry(
        key="future-nosql",
        label="NoSQL family",
        description="MongoDB, graph, key-value",
        disabledReason="Coming later — shelved for the MVP, returns as an adapter package.",
        futureScope=True,
    ),
    AddMenuEntry(
        key="future-files",
        label="File share family",
        description="File upload, S3, SMB",
        disabledReason="Coming later — shelved for the MVP, returns as an adapter package.",
        futureScope=True,
    ),
    AddMenuEntry(
        key="future-jdbc",
        label="More JDBC dialects",
        description="Oracle, SQL Server…",
        disabledReason="Coming later — PostgreSQL, Trino and MySQL ship first.",
        futureScope=True,
    ),
]


def block_by_id(flow: Flow, id_: Optional[str]) -> Optional[FlowBlock]:
    if not id_:
        return None
    return next((b for b in flow.blocks if b.id == id_), None)


def topic_by_id(flow: Flow, id_: Optional[str]) -> Optional[FlowTopic]:
    if not id_:
        return None
    return next((t for t in flow.topics if t.id == id_), None)


def children_of(flow: Flow, node_id: str) -> List[FlowBlock]:
    return [b for b in flow.blocks if b.parentId == node_id]


def is_raw_branch(flow: Flow, block: Optional[FlowBlock]) -> bool:
    """R8 -- true when the branch above (and including) this block carries
    raw bytes."""
    cur = block
    while cur is not None:
        if cur.adapter == "kafka" and cur.mode == "read" and (cur.config or {}).get("parseFormat") == "raw":
            return True
        cur = block_by_id(flow, cur.parentId) if cur.parentId else None
    return False


def is_terminal(block: FlowBlock) -> bool:
    """Terminal blocks (R3/R5): the chain never continues after these."""
    return block.adapter == "kafka_kc" or block.adapter == "kc"


def hosts_transforms(flow: Flow, block: FlowBlock) -> bool:
    """Whether a block hosts the Generic-transformations section."""
    if block.adapter == "kc":
        return False
    return not is_raw_branch(flow, block)


def hosts_test(block: FlowBlock) -> bool:
    """Whether a block gets the Test section (ported from BlockForm.tsx's
    `hostsTest`, the sole place it's defined on the frontend -- it is not in
    legality.ts, but the task brief lists it alongside the R1-R8 predicates
    since it is the same kind of "what may this block do" placement fact).

    No write is test-run, whatever the adapter: jdbc commits rows, http POSTs
    to the destination, kafka and kafka+connect publish. Reads and lookups
    keep Test. kc has no probe surface at all.
    """
    if block.adapter == "http" or block.adapter == "jdbc":
        return block.mode != "write"
    if block.adapter == "kafka":
        return block.mode == "read"
    return False  # kafka_kc (governed terminal write), kc (no test surface)


def compute_root_menu() -> List[AddMenuEntry]:
    """R2 -- the menu shown when a flow has no root yet."""
    return [
        AddMenuEntry(key="http-read", adapter="http", mode="read", label="http · read", description="Pull records from an API"),
        AddMenuEntry(
            key="http-write",
            adapter="http",
            mode="write",
            label="http · write",
            description="A POST whose response is the data to process",
        ),
        AddMenuEntry(key="jdbc-read", adapter="jdbc", mode="read", label="jdbc · read", description="Read tables from a database"),
        AddMenuEntry(
            key="kafka-read",
            adapter="kafka",
            mode="read",
            label="kafka · read (adopt a topic)",
            description="Consume an existing topic — sampled, never renamed",
        ),
        AddMenuEntry(
            key="kafka_kc-root",
            adapter="kafka_kc",
            label="kafka+connect · governed write",
            description="Avro topic + managed sink",
            disabledReason="R2 — kafka+connect is always a destination; it can never start a flow.",
        ),
        *FUTURE_SCOPE,
    ]


def compute_add_menu(flow: Flow, after_block_id: str) -> List[AddMenuEntry]:
    """The + menu at a block's output."""
    parent = block_by_id(flow, after_block_id)
    if not parent:
        return []
    if is_terminal(parent):  # no + on terminal blocks at all
        return []

    raw = is_raw_branch(flow, parent)
    entries: List[AddMenuEntry] = [
        AddMenuEntry(key="http-read", adapter="http", mode="read", label="http · read", description="Chained request per record"),
        AddMenuEntry(key="http-lookup", adapter="http", mode="lookup", label="http · lookup", description="Enrich records from an API"),
        AddMenuEntry(key="http-write", adapter="http", mode="write", label="http · write", description="POST records onward"),
        AddMenuEntry(key="jdbc-read", adapter="jdbc", mode="read", label="jdbc · read", description="Read a table per record"),
        AddMenuEntry(key="jdbc-lookup", adapter="jdbc", mode="lookup", label="jdbc · lookup", description="Enrich records from a table"),
        AddMenuEntry(key="jdbc-write", adapter="jdbc", mode="write", label="jdbc · write", description="Write records to a table"),
        AddMenuEntry(
            key="kafka-write",
            adapter="kafka",
            mode="write",
            label="kafka · write",
            description="Schemaless topic on the platform cluster",
        ),
        AddMenuEntry(
            key="kafka_kc",
            adapter="kafka_kc",
            label="kafka+connect · governed write",
            description="Avro topic + managed sink (schema ceremony)",
        ),
        *FUTURE_SCOPE,
    ]

    result: List[AddMenuEntry] = []
    for e in entries:
        if e.disabledReason:
            result.append(e)
        elif raw and e.adapter == "kafka_kc":
            result.append(
                replace(e, disabledReason="R8 — raw bytes are quarantined: a governed Avro write cannot follow a raw kafka read.")
            )
        elif raw and e.adapter != "kafka":
            result.append(
                replace(e, disabledReason="R8 — raw bytes are quarantined: only byte-preserving delivery (kafka · write) is legal here.")
            )
        else:
            result.append(e)
    return result


AddIntent = str  # "root" | "first" | "parallel" | "attach"


def add_intent(flow: Flow, parent_node_id: Optional[str]) -> AddIntent:
    if parent_node_id is None:
        return "root"
    if topic_by_id(flow, parent_node_id):
        return "attach"
    return "parallel" if len(children_of(flow, parent_node_id)) > 0 else "first"


def add_menu_label(flow: Flow, parent_node_id: Optional[str]) -> str:
    """The one wording for an add menu's heading, shared by every + in the UI."""
    intent = add_intent(flow, parent_node_id)
    if intent == "root":
        return "Choose the root block"
    if intent == "attach":
        topic = topic_by_id(flow, parent_node_id)
        return f"Attach to {topic.name if topic else 'this topic'}"
    block = block_by_id(flow, parent_node_id)
    name = block.name if block else "this block"
    if intent == "parallel":
        return f'Another branch off "{name}" — conditions are set in Routing'
    return f'First branch off "{name}" — it receives every record until you add a condition'


def compute_topic_menu(flow: Flow, topic_id: str) -> List[AddMenuEntry]:
    """The menu on a topic node: kafka read continues the chain, kc subscribes."""
    topic = topic_by_id(flow, topic_id)
    if not topic:
        return []
    if topic.sealed:
        return [
            AddMenuEntry(
                key="sealed",
                label="This topic is sealed",
                description="kafka+connect topics are visible but never attachable.",
                disabledReason="Sealed — the governed topic is managed with its sink as one unit.",
            )
        ]
    return [
        AddMenuEntry(
            key="kafka-read",
            adapter="kafka",
            mode="read",
            label="kafka · read",
            description="Continue the chain from this topic (continuous consumer)",
        ),
        AddMenuEntry(
            key="kc",
            adapter="kc",
            label="kc · sink subscription",
            description="Deliver this topic's bytes untouched into an installed Connect sink",
        ),
    ]


def flow_has_trigger(flow: Flow) -> bool:
    """R1 -- whether the flow has a cron trigger at all. Only http/jdbc roots
    are scheduled; kafka reads (rooted or mid-chain) are continuous consumers,
    and a topic-rooted flow with only kc sinks has no trigger of any kind."""
    root = root_block(flow)
    return root is not None and (root.adapter == "http" or root.adapter == "jdbc")


def root_block(flow: Flow) -> Optional[FlowBlock]:
    """The flow's root block: either a block with no parent, or -- for
    topic-rooted flows -- the kafka read attached to the adopted topic (kc
    subscriptions never count as the root)."""
    bare = next((b for b in flow.blocks if b.parentId is None), None)
    if bare:
        return bare
    for b in flow.blocks:
        if b.adapter == "kafka" and b.mode == "read":
            t = topic_by_id(flow, b.parentId)
            if t is not None and t.kind == "adopted":
                return b
    return None


def scheduled_block(flow: Flow) -> Optional[FlowBlock]:
    """The first runnable block that carries the cron badge."""
    return root_block(flow)


# ------------------------------------------------- subtree walk + re-parent


def subtree_ids(flow: Flow, block_id: str) -> List[str]:
    """Every node id at or below `block_id` -- block ids *and* the ids of the
    topics those blocks materialize (whose own children then join the
    closure). Deliberately iterative with a visited set, matching
    legality.ts's own comment: the flow graph is only a tree by convention,
    and a single bad parent link would make naive recursion hang rather than
    raise. The starting block's own id is included."""
    seen: List[str] = []
    seen_set = set()
    queue: List[str] = [block_id]
    while queue:
        id_ = queue.pop(0)
        if id_ in seen_set:
            continue
        seen_set.add(id_)
        seen.append(id_)
        for b in flow.blocks:
            if b.parentId == id_ and b.id not in seen_set:
                queue.append(b.id)
        for t in flow.topics:
            if t.writerBlockId == id_ and t.id not in seen_set:
                queue.append(t.id)
    return seen


def _matches_block(entry: AddMenuEntry, block: FlowBlock) -> bool:
    if not entry.adapter:
        return False
    return entry.adapter == block.adapter and (entry.mode or None) == (block.mode or None)


def _describe_block(block: FlowBlock) -> str:
    return f"{block.adapter}{f' · {block.mode}' if block.mode else ''}"


def can_reparent(flow: Flow, block_id: str, new_parent_id: str) -> Optional[str]:
    """Whether `block_id` may be moved under `new_parent_id` (a block id or a
    topic id). Returns None when the move is legal, otherwise the refusal --
    in the same voice as the rule refusals, because it is shown to the user
    verbatim."""
    block = block_by_id(flow, block_id)
    if not block:
        return "That block is no longer part of this flow."

    root = root_block(flow)
    if block.parentId is None or (root is not None and root.id == block_id):
        return "The root block cannot be re-parented — R2 fixes where a flow starts."

    if new_parent_id == block_id:
        return "A block cannot be its own parent."

    descendants = subtree_ids(flow, block_id)
    if new_parent_id in descendants:
        return "That target sits inside this block's own branch — moving it there would close the chain into a loop."

    if new_parent_id == block.parentId:
        return None  # already there: a legal no-op

    parent_topic = topic_by_id(flow, new_parent_id)
    if parent_topic:
        if parent_topic.sealed:
            return "Sealed — the governed topic is managed with its sink as one unit; nothing can attach."
        menu = compute_topic_menu(flow, new_parent_id)
        entry = next((e for e in menu if _matches_block(e, block)), None)
        if not entry:
            return f"Only a kafka · read or a kc · sink subscription attaches to a topic (R5) — {_describe_block(block)} cannot."
        if entry.disabledReason:
            return entry.disabledReason
        return None

    parent = block_by_id(flow, new_parent_id)
    if not parent:
        return "That target is no longer part of this flow."

    if block.adapter == "kc":
        return "kc subscribes to a topic, never to a block (R5) — drop it on a topic node instead."

    if is_terminal(parent):
        return f'"{parent.name}" is terminal (R3/R5) — the chain never continues after it.'

    menu = compute_add_menu(flow, new_parent_id)
    entry = next((e for e in menu if _matches_block(e, block)), None)
    if not entry:
        return f'{_describe_block(block)} is not legal after "{parent.name}".'
    if entry.disabledReason:
        return entry.disabledReason

    # R8 travels with the branch: a subtree that is legal on its own can still
    # be illegal once it hangs below a raw kafka read.
    if is_raw_branch(flow, parent):
        stranded: Optional[FlowBlock] = None
        for id_ in descendants:
            b = block_by_id(flow, id_)
            if b is not None and b.id != block_id and not (b.adapter == "kafka" and b.mode == "write"):
                stranded = b
                break
        if stranded:
            return f'R8 — raw bytes are quarantined: "{stranded.name}" ({_describe_block(stranded)}) cannot follow a raw kafka read.'

    return None


# --------------------------------------------------------------- whole-tree check


@dataclass
class PlacementViolation:
    blockId: Optional[str]
    message: str


def validate_placement(flow: Flow) -> List[PlacementViolation]:
    """Whole-flow structural check: does every block's parent linkage in
    `flow.blocks` reflect something a legal `computeAddMenu` / `canReparent`
    call would have produced? See the module docstring for why this exists
    (there is no single TS equivalent).

    Checks, per R1-R8:
      - R2: kafka_kc can never be a bare root.
      - R5: kc can never be a bare root (it only ever subscribes to a topic);
            a block attached to a topic must be a kafka·read or a kc
            subscription, and the topic must not be sealed.
      - R3/R5: a block's block-parent must not be terminal (kafka_kc / kc).
      - R5: kc never has a block parent, only a topic parent.
      - R8: raw-branch quarantine -- once a block's parent chain carries raw
            bytes, every block in that branch must be a byte-preserving
            kafka·write, and no block in a raw branch may carry transforms.
      - R1: `flow.cron` is only legal when the flow's root takes a trigger
            (http/jdbc root).
    """
    violations: List[PlacementViolation] = []

    def add(block_id: Optional[str], message: str) -> None:
        violations.append(PlacementViolation(blockId=block_id, message=message))

    for b in flow.blocks:
        if b.parentId is None:
            if b.adapter == "kafka_kc":
                add(b.id, "R2 — kafka+connect is always a destination; it can never start a flow.")
            elif b.adapter == "kc":
                add(b.id, "kc subscribes to a topic, never to a block (R5) — it cannot be a bare root.")
            continue

        topic = topic_by_id(flow, b.parentId)
        if topic is not None:
            if topic.sealed:
                add(b.id, "Sealed — the governed topic is managed with its sink as one unit; nothing can attach.")
            elif not ((b.adapter == "kafka" and b.mode == "read") or b.adapter == "kc"):
                add(b.id, f"Only a kafka · read or a kc · sink subscription attaches to a topic (R5) — {_describe_block(b)} cannot.")
            continue

        parent = block_by_id(flow, b.parentId)
        if parent is None:
            add(b.id, "This block's parent no longer exists in the flow.")
            continue

        if is_terminal(parent):
            add(b.id, f'"{parent.name}" is terminal (R3/R5) — the chain never continues after it.')
        if b.adapter == "kc":
            add(b.id, "kc subscribes to a topic, never to a block (R5) — drop it on a topic node instead.")
        if is_raw_branch(flow, parent) and not (b.adapter == "kafka" and b.mode == "write"):
            add(b.id, f'R8 — raw bytes are quarantined: "{b.name}" ({_describe_block(b)}) cannot follow a raw kafka read.')

    # R8 -- no transforms inside a raw branch, even on a legal leaf.
    for b in flow.blocks:
        if b.transforms and is_raw_branch(flow, b):
            add(b.id, "R8 — this branch carries raw bytes; transformations are not available here.")

    # R1 -- cron is only legal on an http/jdbc root.
    if flow.cron and not flow_has_trigger(flow):
        add(None, "R1 — cron is only valid on an http or jdbc root; this flow's root does not take a schedule.")

    return violations
