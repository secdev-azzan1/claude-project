"""Kafka Connect connector configs — compiler-spec.md §5.

Post-migration contract: every kc/kafka_kc block now carries a COMPLETE,
user/migration-authored Kafka Connect connector config at
`block.config.sinkConfig` (connector.class, topics, both converters, the
full iceberg.catalog.* set, credentials — everything). The compiler no
longer derives any of this from the bound sink service; it is pure
pass-through. `build_kafka_kc_connector()` and `build_kc_connector()` both
take `block.config.sinkConfig`, stringify its keys/values, and hand it
straight to the ConnectorSpec. Validation (`validation.py`) is what
guarantees `sinkConfig` is non-empty and carries at least `connector.class`
and `topics` before a flow is allowed to deploy.

Connector naming: `<flowToken>.<blockId>.<kafka_kc|kc>` (live-evidence
convention, compiler-spec §2) — unchanged, since it is the REST path and
`runtimeScopeMap.connectorNames` depends on it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from models.adapter import FlowBlock

from .ir import ConnectorSpec

if TYPE_CHECKING:  # pragma: no cover
    from models.adapter import Flow
    from .ir import CompileContext


def build_kafka_kc_connector(
    *, flow: "Flow", block: FlowBlock, ctx: "CompileContext", flow_token: str, topic: str, entity_token: str
) -> ConnectorSpec:
    # flow, ctx, topic, entity_token: unused now that sinkConfig is passed
    # through verbatim, but kept for call-site compatibility (compile_flow.py
    # passes them positionally/by keyword and is not being edited here).
    config = {str(k): str(v) for k, v in (block.config.get("sinkConfig") or {}).items()}
    name = f"{flow_token}.{block.id}.kafka_kc"
    return ConnectorSpec(name=name, config=config, ownerBlockId=block.id)


def build_kc_connector(
    *, flow: "Flow", block: FlowBlock, ctx: "CompileContext", flow_token: str, topic: str, entity_token: str,
    topic_is_governed: bool,
) -> ConnectorSpec:
    # flow, ctx, topic, entity_token, topic_is_governed: unused now that
    # sinkConfig is passed through verbatim, but kept for call-site
    # compatibility (blocks_kafka_kc.py / compile_flow.py are not being
    # edited here).
    config = {str(k): str(v) for k, v in (block.config.get("sinkConfig") or {}).items()}
    name = f"{flow_token}.{block.id}.kc"
    return ConnectorSpec(name=name, config=config, ownerBlockId=block.id)
