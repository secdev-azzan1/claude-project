"""Line-by-line port of frontend/src/prototype/naming.ts: the tokenizer,
derived topic/table/dlq names, and the simplified deterministic naming walk.

See that file for the prose design notes this port intentionally keeps
verbatim in each docstring/comment.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional

from models.adapter import Flow, FlowBlock

# --------------------------------------------------------------------- tokenize

_NON_ALNUM_RUN_RE = re.compile(r"[^a-z0-9]+")
_EDGE_UNDERSCORE_RE = re.compile(r"^_+|_+$")
_MULTI_UNDERSCORE_RE = re.compile(r"_{2,}")


def tokenize(input: str) -> str:
    """One tokenizer everywhere: trim -> lowercase -> non-alphanumeric -> "_".

    Ported step-for-step from naming.ts's `tokenize`:

        input.trim().toLowerCase()
             .replace(/[^a-z0-9]+/g, "_")
             .replace(/^_+|_+$/g, "")
             .replace(/_{2,}/g, "_");

    Note on the third step: because `[^a-z0-9]+` already matches (and
    collapses) any *run* of non-alphanumeric characters -- including literal
    "_" characters already in the input, since "_" itself is non-alphanumeric
    -- into a single "_", two adjacent underscores can never survive the
    second step. The final `_{2,}` collapse is therefore a no-op in practice;
    it is kept here anyway for line-by-line fidelity with the TS source. This
    is also why there is nothing to "preserve" for consecutive underscores in
    the input: they always collapse to one, by design of the run-matching
    regex, not by an extra dedicated step.
    """
    s = input.strip().lower()
    s = _NON_ALNUM_RUN_RE.sub("_", s)
    s = _EDGE_UNDERSCORE_RE.sub("", s)
    s = _MULTI_UNDERSCORE_RE.sub("_", s)
    return s


def dlq_name(flow_name: str) -> str:
    return f"dlq.{tokenize(flow_name)}"


def table_name(flow_name: str, entity: str) -> str:
    return f"bronze.{tokenize(flow_name)}.{tokenize(entity)}__raw"


def base_topic_name(flow_name: str, entity: str) -> str:
    return f"raw.{tokenize(flow_name)}.{tokenize(entity)}"


# Names that already exist on the (mock) cluster and cannot be re-created.
RESERVED_TOPIC_NAMES: List[str] = [
    "raw.rapid7_assets.asset",
    "raw.fortisiem_events.incident",
    "raw.cmdb_asset_sync.asset",
    "raw.asset_retirement.asset",
    "partner.threatfeed.indicators",
    "audit.raw.mirror",
]


@dataclass
class DerivedName:
    value: str
    overridden: bool
    variant: Optional[str] = None
    warning: Optional[str] = None


def _is_kafka_family_write(b: FlowBlock) -> bool:
    return (b.adapter == "kafka" and b.mode == "write") or b.adapter == "kafka_kc"


def branch_path_labels(flow: Flow, block: FlowBlock) -> List[str]:
    """Walk up to the root collecting branch labels for variant tokens."""
    labels: List[str] = []
    by_id = {b.id: b for b in flow.blocks}
    cur: Optional[FlowBlock] = block
    while cur is not None:
        if cur.branch is not None and cur.branch.name:
            labels.insert(0, cur.branch.name)
        cur = by_id.get(cur.parentId) if cur.parentId else None
    return labels


_TOPIC_OVERRIDE_CLEAN_RE = re.compile(r"[^a-z0-9._-]+")


def clean_topic_override(value: str) -> str:
    """The one cleanup applied to a typed topic override (R7)."""
    s = value.strip().lower()
    return _TOPIC_OVERRIDE_CLEAN_RE.sub("_", s)


def derive_topic_name(flow: Flow, block: FlowBlock) -> DerivedName:
    """Simplified naming walk for one flow:
    - a sole writer for an entity keeps the bare name;
    - multiple same-entity writers take variant tokens pre-filled from branch
      labels (or fork-N defaults);
    - on a bare-name tie between kafka_kc and a plain kafka write, the
      governed (kafka_kc) write holds the bare name.

    An override wins for the whole kafka family -- plain kafka writes *and*
    kafka_kc governed writes (R7).
    """
    if not _is_kafka_family_write(block):
        return DerivedName(value="", overridden=False)

    if block.topicOverride:
        clean = clean_topic_override(block.topicOverride)
        warning = (
            "Custom name strays into the derived raw.* namespace — collisions with derived names are likely."
            if clean.startswith("raw.")
            else None
        )
        return DerivedName(value=clean, overridden=True, warning=warning)

    entity = tokenize(block.entity) if block.entity else ""
    if not entity:
        return DerivedName(
            value="raw.<entity missing>",
            overridden=False,
            warning="Set an entity label to derive the topic name.",
        )
    base = base_topic_name(flow.name, entity)

    same_entity_writers = [
        b
        for b in flow.blocks
        if _is_kafka_family_write(b) and not b.topicOverride and b.entity and tokenize(b.entity) == entity
    ]
    if len(same_entity_writers) <= 1:
        return DerivedName(value=base, overridden=False)

    # Governed write wins the bare name when kinds differ.
    governed = [b for b in same_entity_writers if b.adapter == "kafka_kc"]
    if len(governed) == 1 and block.id == governed[0].id:
        return DerivedName(value=base, overridden=False)

    labels = branch_path_labels(flow, block)
    last_label = labels[-1] if labels else ""
    variant = tokenize(last_label)
    if not variant:
        index = next((i for i, b in enumerate(same_entity_writers) if b.id == block.id), -1)
        variant = f"variant_{index + 1}"
    return DerivedName(value=f"{base}.{variant}", overridden=False, variant=variant)


def derived_topic_default(flow: Flow, block: FlowBlock) -> DerivedName:
    """What the block's topic would be called with no override at all -- the
    name the override input shows as its placeholder."""
    if not block.topicOverride:
        return derive_topic_name(flow, block)
    bare = block.model_copy(update={"topicOverride": None})
    probe_blocks = [bare if b.id == block.id else b for b in flow.blocks]
    probe = flow.model_copy(update={"blocks": probe_blocks})
    return derive_topic_name(probe, bare)


_UNSET = object()


def override_matches_derived(
    flow: Flow,
    block: FlowBlock,
    override: Optional[str] = _UNSET,  # type: ignore[assignment]
) -> bool:
    """True when a typed override is exactly what the platform would have
    derived anyway."""
    if override is _UNSET:
        override = block.topicOverride
    typed = clean_topic_override(override or "")
    if not typed:
        return False
    return typed == derived_topic_default(flow, block).value


def topic_name_collision(name: str, existing: Optional[List[str]] = None) -> Optional[str]:
    if existing is None:
        existing = RESERVED_TOPIC_NAMES
    if not name:
        return None
    return f'"{name}" is already reserved on the platform cluster.' if name in existing else None


# ------------------------------------------------------------------------ cron

_KNOWN_CRON_PREVIEWS = {
    "*/5 * * * *": ["today 12:05 UTC", "today 12:10 UTC", "today 12:15 UTC"],
    "*/15 * * * *": ["today 12:15 UTC", "today 12:30 UTC", "today 12:45 UTC"],
    "0 * * * *": ["today 13:00 UTC", "today 14:00 UTC", "today 15:00 UTC"],
    "0 */6 * * *": ["today 18:00 UTC", "tomorrow 00:00 UTC", "tomorrow 06:00 UTC"],
    "0 2 * * *": ["tomorrow 02:00 UTC", "in 2 days 02:00 UTC", "in 3 days 02:00 UTC"],
    "0 6 * * 1": ["Mon 06:00 UTC", "next Mon 06:00 UTC", "in 2 weeks Mon 06:00 UTC"],
}


def cron_preview(cron: Optional[str]) -> List[str]:
    """Human preview of the next cron occurrences -- canned, not a cron engine."""
    if not cron:
        return []
    known = _KNOWN_CRON_PREVIEWS.get(cron)
    if known:
        return known
    fields = cron.strip().split()
    if len(fields) != 5:
        return []
    return ["next occurrence (preview)", "second occurrence (preview)", "third occurrence (preview)"]


def is_valid_cron(cron: Optional[str]) -> bool:
    if cron is None:
        return True
    return len(cron.strip().split()) == 5


CRON_PRESETS = [
    {"label": "Every 5 minutes", "value": "*/5 * * * *"},
    {"label": "Every 15 minutes", "value": "*/15 * * * *"},
    {"label": "Hourly", "value": "0 * * * *"},
    {"label": "Every 6 hours", "value": "0 */6 * * *"},
    {"label": "Daily at 02:00 UTC", "value": "0 2 * * *"},
    {"label": "Weekly (Mon 06:00 UTC)", "value": "0 6 * * 1"},
]
