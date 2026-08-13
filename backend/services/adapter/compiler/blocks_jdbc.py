"""`jdbc` adapter compilation — compiler-spec.md §3.2.

STUB for T7.1: all three modes (read / write / lookup) land in a follow-up
task. Kept as its own module — and this single dispatch function — so that
follow-up work is local: implement `compile_read` / `compile_write` /
`compile_lookup` here and wire them into `compile_flow.py`'s adapter
dispatch, exactly the same shape as `blocks_http.py`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from models.adapter import FlowBlock

if TYPE_CHECKING:  # pragma: no cover
    from models.adapter import Flow
    from .ir import BlockBuilder, CompileContext


def compile_entry(builder: "BlockBuilder", *, flow: "Flow", block: FlowBlock, ctx: "CompileContext",
                  flow_token: str, is_root: bool, add_param):
    raise NotImplementedError(
        f"jdbc adapter compilation (mode={block.mode!r}) is not implemented yet "
        f"— lands in a follow-up task (T7.1 scope: http read + kafka write/kafka_kc/kc only) "
        f"— block {block.id}"
    )
