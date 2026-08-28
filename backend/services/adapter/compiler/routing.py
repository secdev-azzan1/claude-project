"""Branch fan-out at a parent block's output — compiler-spec.md §4.

Every child of block P attaches at P's finished-record tail. Per spec:
  - unconditional child (no branch, or a branch with no rules): direct
    connection — "NiFi fans out copies" when a relationship has multiple
    outbound connections, so no processor is needed.
  - conditional, match="any": ONE `RouteOnAttribute` (`route__<branchToken>`,
    "Route to 'matched' if any matches" strategy) with one dynamic property
    per rule; the single `matched` relationship feeds the child exactly once
    (see `_wire_any_match` — review M1); `unmatched` auto-terminates.
  - conditional, match="all": a CHAIN of `RouteOnAttribute` processors
    (`route__<branchToken>__rule_<i>`), each testing one rule; `matched`
    advances to the next rule (or the child, on the last one); every
    `unmatched` auto-terminates.

Routing processors live in the PARENT's BlockGroup (they are the parent's
egress decision, not the child's) — `compile_flow` attributes their keys to
the parent's scope-map entry accordingly.

On the `to_dlq(<route>, "failure")` edges below (review C2 claimed they make
every routed flow undeployable): EMPIRICALLY REFUTED — Journey B deployed
this exact wiring against live NiFi, the connections were read back off the
NiFi REST API (`route__males --[failure]--> dlq__meta` et al.), the flow
started and produced exact per-branch counts over three cron firings. Live
NiFi accepts the connection and it is inert at runtime (RouteOnAttribute
never emits `failure`), so the edges are kept as wired-and-verified reality.

Processor keys are derived from the branch's NAME (`route__<branchToken>`),
so two conditional children of the same parent sharing a branch name would
collide on the same key — `BlockBuilder.add_processor` raises `CompileError`
on that (loud failure, never a silent shadow); nothing today forbids
duplicate branch names among siblings, so this is a real, reachable case,
just not one this compiler papers over.

Ambiguity resolved (see the T7.1 report): compiler-spec's IR grammar shows a
single generic `"outputPort"` connection target, but a parent with more than
one child needs to route different record subsets to different children
independently — a single shared port can't do that. Each child therefore
gets ITS OWN dedicated output port, named `outputPort:<childBlockId>`, both
in `ConnectionSpec.to` (inside the parent's group) and in the root-level
`PortLink.viaPort` (which always echoes the same string) — see `ir.py`'s
`ConnectionSpec` docstring.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List

from models.adapter import FlowBlock
from services.adapter.naming import tokenize

from .ir import CompileError, PortLink, ProcessorSpec, branch_rule_el

if TYPE_CHECKING:  # pragma: no cover
    from models.adapter import Flow
    from .ir import BlockBuilder
    from .transforms import Tail


def _is_unconditional(child: FlowBlock) -> bool:
    return child.branch is None or not child.branch.rules


def _branch_token(child: FlowBlock) -> str:
    if child.branch and child.branch.name:
        return tokenize(child.branch.name)
    return tokenize(child.id)


def out_port(child_id: str) -> str:
    return f"outputPort:{child_id}"


def _extract_route_fields_preserving_existing(
    builder: "BlockBuilder", *, fields: List[str], source: "Tail"
) -> "Tail":
    """Promote branch-rule fields without clobbering existing attributes.

    A parent transform may already have extracted a field such as ``name``.
    Re-running one unconditional EvaluateJsonPath for routing can replace
    that attribute with an empty value when the current content is not a
    direct JSON object at that point.  An empty value makes a ``not_equals``
    rule match, which silently sends the excluded record down the branch.

    Each field therefore takes a present path (keep the attribute) or a
    missing path (extract from JSON), then rejoins before RouteOnAttribute.
    """
    working_key, working_rel = source
    for index, field in enumerate(fields):
        gate_key = f"route_fields__check_{index}"
        extract_key = "route_fields" if len(fields) == 1 else f"route_fields__extract_{index}"
        merge_key = f"route_fields__merge_{index}"

        builder.add_processor(
            ProcessorSpec(
                key=gate_key,
                name=gate_key,
                type="org.apache.nifi.processors.standard.RouteOnAttribute",
                properties={
                    "Routing Strategy": "Route to Property name",
                    "present": f"${{{field}:isEmpty():not()}}",
                    "missing": f"${{{field}:isEmpty()}}",
                },
                autoTerminate=["unmatched"],
            )
        )
        builder.link(working_key, gate_key, [working_rel] if working_rel else [])
        builder.to_dlq(gate_key, "failure")

        props: Dict[str, Any] = {
            "Destination": "flowfile-attribute",
            "Path Not Found Behavior": "ignore",
            "Return Type": "scalar",
            field: f"$.{field}",
        }
        builder.add_processor(
            ProcessorSpec(
                key=extract_key,
                name=extract_key,
                type="org.apache.nifi.processors.standard.EvaluateJsonPath",
                properties=props,
                autoTerminate=["unmatched"],
            )
        )
        builder.link(gate_key, extract_key, ["missing"])
        builder.to_dlq(extract_key, "failure")

        builder.add_processor(
            ProcessorSpec(
                key=merge_key,
                name=merge_key,
                type="org.apache.nifi.processors.attributes.UpdateAttribute",
            )
        )
        builder.link(gate_key, merge_key, ["present"])
        builder.link(extract_key, merge_key, ["matched"])
        working_key, working_rel = merge_key, "success"

    return working_key, working_rel


def wire_children(
    builder: "BlockBuilder",
    *,
    flow: "Flow",
    parent: FlowBlock,
    children: List[FlowBlock],
    tail: "Tail",
    port_links: List[PortLink],
) -> List[str]:
    """Wire `parent`'s finished-record `tail` to every child in `children`.
    Returns the keys of every routing processor added (for scope-map
    attribution to the parent)."""
    if not children:
        return []

    added_keys: List[str] = []
    conditional = [c for c in children if not _is_unconditional(c)]
    tail_key, tail_rel = tail

    working_key, working_rel = tail_key, tail_rel
    if conditional:
        fields: List[str] = []
        seen = set()
        for child in conditional:
            for rule in child.branch.rules or []:
                if rule.field and rule.field not in seen:
                    seen.add(rule.field)
                    fields.append(rule.field)
        # Keep the guarded field promotion shape even for one field.  The
        # old direct multi-property EvaluateJsonPath could overwrite an
        # existing value with empty text immediately before routing.
        working_key, working_rel = _extract_route_fields_preserving_existing(
            builder, fields=fields, source=(tail_key, tail_rel)
        )
        added_keys.extend(
            [
                f"route_fields__check_{index}" for index in range(len(fields))
            ]
        )
        added_keys.extend(
            [
                "route_fields" if len(fields) == 1 else f"route_fields__extract_{index}"
                for index in range(len(fields))
            ]
        )
        added_keys.extend([f"route_fields__merge_{index}" for index in range(len(fields))])

    for child in children:
        if _is_unconditional(child):
            builder.link(working_key, out_port(child.id), [working_rel])
        elif (child.branch.match or "all") == "any":
            added_keys.extend(_wire_any_match(builder, child=child, source=(working_key, working_rel)))
        else:
            added_keys.extend(_wire_all_match(builder, child=child, source=(working_key, working_rel)))
        port_links.append(PortLink(fromBlockId=parent.id, toBlockId=child.id, viaPort=out_port(child.id)))

    return added_keys


def _wire_any_match(builder: "BlockBuilder", *, child: FlowBlock, source: "Tail") -> List[str]:
    """match="any": ONE `RouteOnAttribute` with one genuine dynamic property
    per rule (D7's "N real decision expressions on one processor"), but with
    `Routing Strategy = Route to 'matched' if any matches` and a SINGLE
    `matched` connection to the child. The previous "Route to Property name"
    strategy transferred a COPY of the FlowFile to EVERY matching property
    (review finding M1) — a record matching several rules was delivered to
    the child once per matching rule. The 'matched'-if-any strategy collapses
    the relationship set to `matched`/`unmatched`, so a record is forwarded
    exactly once no matter how many rules it satisfies."""
    key = f"route__{_branch_token(child)}"
    rules = child.branch.rules or []
    if not rules:
        raise CompileError(f"Branch on {child.id!r} has match='any' but no rules")
    props = {"Routing Strategy": "Route to 'matched' if any matches"}
    for i, rule in enumerate(rules):
        props[f"rule_{i}"] = branch_rule_el(rule.field, rule.op, rule.value)
    builder.add_processor(
        ProcessorSpec(key=key, name=key, type="org.apache.nifi.processors.standard.RouteOnAttribute",
                      properties=props, autoTerminate=["unmatched"])
    )
    src_key, src_rel = source
    builder.link(src_key, key, [src_rel])
    builder.to_dlq(key, "failure")
    builder.link(key, out_port(child.id), ["matched"])
    return [key]


def _wire_all_match(builder: "BlockBuilder", *, child: FlowBlock, source: "Tail") -> List[str]:
    rules = child.branch.rules or []
    if not rules:
        raise CompileError(f"Branch on {child.id!r} has match='all' but no rules")
    token = _branch_token(child)
    keys: List[str] = []
    src_key, src_rel = source
    for i, rule in enumerate(rules):
        key = f"route__{token}__rule_{i}"
        builder.add_processor(
            ProcessorSpec(
                key=key, name=key, type="org.apache.nifi.processors.standard.RouteOnAttribute",
                properties={"Routing Strategy": "Route to Property name", "matched": branch_rule_el(rule.field, rule.op, rule.value)},
                autoTerminate=["unmatched"],
            )
        )
        builder.link(src_key, key, [src_rel])
        builder.to_dlq(key, "failure")
        keys.append(key)
        src_key, src_rel = key, "matched"
    builder.link(src_key, out_port(child.id), [src_rel])
    return keys
