"""Build the disposable V2 deployment used by the schema ceremony.

The production compiler remains the single source of truth.  This module only
selects the target block's ancestor chain, gives that copy a unique identity,
and asks the compiler to replace the target's governed Avro/Connect egress
with a plain-JSON publisher to a temporary topic.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Set

from models.adapter import Flow
from services.adapter.naming import tokenize

from .compile_flow import compile_flow
from .ir import CompileContext, CompileError, DeploymentPlan


def build_inference_plan(
    flow: Flow,
    ctx: CompileContext,
    *,
    target_block_id: str,
    inference_topic: str,
    job_id: str,
) -> DeploymentPlan:
    """Compile only the source-to-target path for one inference job.

    Sibling branches and production sinks must not run during inference.  A
    unique flow id also isolates Redis incremental/dedup state from the real
    flow.  Any intermediate Kafka writes in the selected ancestor path are
    consequently temporary names too and are cleaned up by the runner.
    """
    by_id = {block.id: block for block in flow.blocks}
    target = by_id.get(target_block_id)
    if target is None:
        raise CompileError(f"Schema inference target block {target_block_id!r} was not found.")
    if target.adapter not in ("kafka_kc", "kafka") or (target.adapter == "kafka" and target.mode != "write"):
        raise CompileError(
            "Schema inference must target a Kafka publisher (kafka+connect governed write or kafka write)."
        )
    if not inference_topic.strip():
        raise CompileError("Schema inference topic cannot be empty.")

    included: Set[str] = set()
    current = target
    while current is not None:
        if current.id in included:
            raise CompileError("Schema inference cannot follow a cyclic block parent chain.")
        included.add(current.id)
        if not current.parentId:
            break
        parent = by_id.get(current.parentId)
        if parent is None:
            raise CompileError(f"Block {current.id!r} references missing parent {current.parentId!r}.")
        current = parent

    # Preserve declaration order because it is also the compiler's deterministic
    # ordering.  Flow topics are retained for kafka-read ancestors, while only
    # selected blocks are allowed to become executable components.
    selected_blocks = [block for block in flow.blocks if block.id in included]
    short_job = tokenize(job_id) or "job"
    temporary_flow = flow.model_copy(
        update={
            "id": f"{flow.id}__schema_inference__{short_job}",
            "name": f"{flow.name}__schema_inference__{short_job}",
            # A production cron must not delay a one-off ceremony. The
            # temporary source is allowed to tick once per minute; the runner
            # stops it as soon as the sample window is collected.
            "cron": "* * * * *",
            "blocks": selected_blocks,
        }
    )
    temporary_context = replace(
        ctx,
        inference_target_block_id=target_block_id,
        inference_topic=inference_topic,
    )
    return compile_flow(temporary_flow, temporary_context)
