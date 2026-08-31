"""`compile_flow(flow, ctx) -> DeploymentPlan` — the T7.1 compiler entry
point. Pure function: no network I/O, no database access, no timestamps or
random ids in the produced plan (see `ir.py`'s determinism note).

Orchestration, per block (compiler-spec.md §1-§6):
  1. `validate_placement(flow)` first — fail fast on a structurally illegal
     tree (test 4: a block placed after a terminal `kafka_kc`/`kc` raises).
  2. For each block (declared order — this is what makes the compile
     deterministic): dispatch on adapter to build its "entry" tail (trigger/
     input-port through parsing, or — for kafka_kc — through the envelope
     step), then uniformly run the block's own transform chain
     (`transforms.build_chain`, dedup always last), then either wire it to
     its children (`routing.wire_children`, non-terminal) or publish
     (`blocks_kafka.compile_publish` / `blocks_kafka_kc.compile_publish`,
     terminal) — always finishing with the block's DLQ chain (`dlq.build`).
  3. `kc` blocks get NO BlockGroup at all (spec §3.5: "No NiFi components") —
     just a Connect connector and a `scopeMap` entry.
"""

from __future__ import annotations

from typing import Dict, List

from models.adapter import Flow, FlowBlock
from services.adapter.legality import root_block, validate_placement
from services.adapter.naming import dlq_name, tokenize

from . import blocks_http, blocks_jdbc, blocks_kafka, blocks_kafka_kc, connectors, dlq, routing
from .ir import (
    BlockBuilder,
    BlockGroup,
    CompileContext,
    CompileError,
    ConnectorSpec,
    DeploymentPlan,
    ParameterContextSpec,
    ParameterSpec,
    PortLink,
    RootGroup,
    ScopeMapEntry,
    TopicSpec,
)
from .transforms import build_chain, compile_cleanup


def compile_flow(flow: Flow, ctx: CompileContext) -> DeploymentPlan:
    violations = validate_placement(flow)
    if violations:
        detail = "; ".join(f"[{v.blockId or 'flow'}] {v.message}" for v in violations)
        raise CompileError(f"Flow fails placement validation — {detail}")

    flow_token = tokenize(flow.name)
    root = root_block(flow)

    by_parent: Dict[str, List[FlowBlock]] = {}
    for b in flow.blocks:
        if b.parentId:
            by_parent.setdefault(b.parentId, []).append(b)

    parameters: List[ParameterSpec] = []
    seen_params: set = set()

    def add_param(name: str, value, sensitive: bool) -> None:
        if name in seen_params:
            return
        seen_params.add(name)
        parameters.append(ParameterSpec(name=name, value=(None if value is None else str(value)), sensitive=bool(sensitive)))

    topics: List[TopicSpec] = []
    connectors_out: List[ConnectorSpec] = []
    scope_map: Dict[str, ScopeMapEntry] = {}
    child_groups: List[BlockGroup] = []
    port_links: List[PortLink] = []

    for block in flow.blocks:
        if block.adapter == "kc":
            _compile_kc(flow=flow, block=block, ctx=ctx, flow_token=flow_token,
                       connectors_out=connectors_out, scope_map=scope_map)
            continue

        is_root = root is not None and block.id == root.id
        group = _compile_block(
            flow=flow, block=block, ctx=ctx, flow_token=flow_token, is_root=is_root,
            children=by_parent.get(block.id, []), add_param=add_param, topics_out=topics,
            connectors_out=connectors_out, port_links=port_links,
        )
        child_groups.append(group)
        scope_map[block.id] = ScopeMapEntry(
            adapter=block.adapter, engine="nifi", groupName=group.name,
            components=[p.key for p in group.processors] + [cs.key for cs in group.controllerServices],
        )

    if child_groups:
        topics.append(TopicSpec(name=dlq_name(flow.name), kind="dlq", ownerBlockId=None))

    return DeploymentPlan(
        flowId=flow.id,
        flowToken=flow_token,
        parameterContext=ParameterContextSpec(name=f"{flow_token}__params", parameters=parameters),
        rootGroup=RootGroup(name=flow_token, childGroups=child_groups, connections=port_links),
        topics=topics,
        connectors=connectors_out,
        scopeMap=scope_map,
    )


def _compile_kc(*, flow: Flow, block: FlowBlock, ctx: CompileContext, flow_token: str,
                connectors_out: List[ConnectorSpec], scope_map: Dict[str, ScopeMapEntry]) -> None:
    attach_topic_id = block.config.get("attachTopicId")
    topic = next((t for t in flow.topics if t.id == attach_topic_id), None)
    if topic is None:
        raise CompileError(f'kc block {block.id!r} ("{block.name}") is not attached to a topic')
    owner = next((b for b in flow.blocks if b.id == topic.writerBlockId), None)
    topic_is_governed = bool(owner and owner.adapter == "kafka_kc")
    entity_token = tokenize(block.entity or block.name)

    connector = connectors.build_kc_connector(
        flow=flow, block=block, ctx=ctx, flow_token=flow_token, topic=topic.name,
        entity_token=entity_token, topic_is_governed=topic_is_governed,
    )
    connectors_out.append(connector)
    scope_map[block.id] = ScopeMapEntry(adapter="kc", engine="connect", groupName="", components=[])


def _compile_block(
    *, flow: Flow, block: FlowBlock, ctx: CompileContext, flow_token: str, is_root: bool,
    children: List[FlowBlock], add_param, topics_out: List[TopicSpec], connectors_out: List[ConnectorSpec],
    port_links: List[PortLink],
) -> BlockGroup:
    builder = BlockBuilder()
    terminal = False

    if block.adapter == "http":
        tail = blocks_http.compile_read(builder, flow=flow, block=block, ctx=ctx, flow_token=flow_token,
                                        is_root=is_root, add_param=add_param)
    elif block.adapter == "jdbc":
        tail = blocks_jdbc.compile_entry(builder, flow=flow, block=block, ctx=ctx, flow_token=flow_token,
                                         is_root=is_root, add_param=add_param)
    elif block.adapter == "kafka":
        # NOT terminal: R3 lets the chain continue after a kafka write, and a
        # kafka read is a source — only kafka_kc/kc are terminal. The block's
        # consume chain (read) / publish step (write) is attached below, after
        # the transforms chain, so children can also be wired off the tail.
        tail = blocks_kafka.compile_entry(block, is_root=is_root)
    elif block.adapter == "kafka_kc":
        tail = blocks_kafka_kc.compile_envelope(builder, is_root=is_root)
        terminal = True
    else:  # pragma: no cover - FlowBlock.adapter is a Literal; defensive only
        raise CompileError(f"Unknown adapter {block.adapter!r} on block {block.id}")

    chain = build_chain(builder, flow=flow, block=block, ctx=ctx, flow_token=flow_token, tail=tail, add_param=add_param)
    tail = chain.tail

    output_port_needed = False
    tail_consumed_by_publish = False
    if block.adapter == "kafka":
        # Fulfill the promised consume chain (read) or attach the publish
        # step (write). A write's publish consumes the tail; children (legal
        # per R3) additionally fan out from the same tail below.
        publish_tail = (
            compile_cleanup(builder, cleanup=chain.cleanup, tail=tail, key_prefix="egress__publish")
            if block.mode == "write" else tail
        )
        blocks_kafka.compile_publish(builder, flow=flow, block=block, ctx=ctx, flow_token=flow_token,
                                     add_param=add_param, topics_out=topics_out, tail=publish_tail)
        tail_consumed_by_publish = block.mode == "write"

    if terminal:
        publish_tail = compile_cleanup(builder, cleanup=chain.cleanup, tail=tail, key_prefix="egress__publish")
        blocks_kafka_kc.compile_publish(builder, flow=flow, block=block, ctx=ctx, flow_token=flow_token,
                                        add_param=add_param, topics_out=topics_out,
                                        connectors_out=connectors_out, tail=publish_tail)
    elif children:
        routing.wire_children(builder, flow=flow, parent=block, children=children, tail=tail,
                              cleanup=chain.cleanup, port_links=port_links)
        output_port_needed = True
    elif not tail_consumed_by_publish:
        # Non-terminal, childless: nothing downstream consumes the finished
        # tail (placement-valid but structurally incomplete flow) — the
        # relationship auto-terminates rather than dangling unconnected.
        builder.auto_terminate_tail(tail[0], tail[1])

    dlq.build(builder, flow=flow, block=block, ctx=ctx, flow_token=flow_token, add_param=add_param)

    name = f"{tokenize(block.name)}__{block.adapter}"
    return builder.build_group(block.id, name, input_port=not is_root, output_port=output_port_needed)
