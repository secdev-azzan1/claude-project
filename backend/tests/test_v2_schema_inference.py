from __future__ import annotations

import sys
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from models.adapter import Flow, FlowBlock, PlatformConnection  # noqa: E402
from services.adapter.common import COLLECTIONS  # noqa: E402
from services.adapter.compiler.ir import CompileContext, DeploymentPlan, ParameterContextSpec, RootGroup, TopicSpec  # noqa: E402
from services.adapter.deployer.nifi_apply import AppliedResult  # noqa: E402
from services.adapter import schema_inference as inference  # noqa: E402


class FakeCollection:
    def __init__(self, documents=None):
        self.documents = {document["id"]: dict(document) for document in (documents or []) if "id" in document}
        self.inserted = []

    async def find_one(self, query, projection=None):
        document = self.documents.get(query.get("id"))
        if document is None:
            return None
        return dict(document)

    async def update_one(self, query, update):
        document = self.documents.get(query.get("id"))
        if document is not None:
            document.update(update.get("$set") or {})

    async def insert_one(self, document):
        self.inserted.append(dict(document))
        if "id" in document:
            self.documents[document["id"]] = dict(document)


class FakeDb:
    def __init__(self, job):
        self.collections = {
            COLLECTIONS.schema_inference_jobs: FakeCollection([job]),
            COLLECTIONS.audit: FakeCollection(),
        }

    def __getitem__(self, key):
        return self.collections.setdefault(key, FakeCollection())


def _flow_doc():
    return Flow(
        id="flow-1",
        name="Example flow",
        blocks=[
            FlowBlock(id="read", adapter="http", mode="read", name="Read", parentId=None),
            FlowBlock(id="sink", adapter="kafka_kc", name="Sink", parentId="read", entity="asset"),
        ],
    ).model_dump()


def _job(status="queued"):
    return {
        "id": "schema-inference-job-1",
        "flowId": "flow-1",
        "targetBlockId": "sink",
        "flowName": "Example flow",
        "targetTopic": "raw.example_flow.asset",
        "inferenceTopic": "dmp.schema_inference.example.asset.job-1",
        "status": status,
        "messagesCollected": 0,
        "targetMessages": 1,
        "nifiProcessGroupId": None,
        "generatedSchema": None,
        "schemaStatus": "Needs Verification",
        "error": None,
        "cleanupError": None,
        "cancelRequested": False,
        "createdAt": "2026-01-01T00:00:00.000Z",
        "updatedAt": "2026-01-01T00:00:00.000Z",
    }


def _plan():
    return DeploymentPlan(
        flowId="flow-1__schema_inference__schema_inference_job_1",
        flowToken="example_flow_schema_inference_schema_inference_job_1",
        parameterContext=ParameterContextSpec(name="temporary"),
        rootGroup=RootGroup(name="temporary"),
        topics=[TopicSpec(name="dmp.schema_inference.example.asset.job-1", kind="schema_inference")],
    )


@pytest.mark.asyncio
async def test_v2_inference_runs_through_temporary_runtime_and_cleans_everything(monkeypatch):
    db = FakeDb(_job())
    connections = (
        {"endpoint": "https://nifi", "auth_type": "NONE"},
        {"endpoint": "kafka:9092", "kafka_connection_mode": "kafbat", "kafbat_url": "https://kafbat"},
        CompileContext(),
        PlatformConnection(id="nifi", type="nifi"),
        PlatformConnection(id="kafka", type="kafka"),
    )
    monkeypatch.setattr(inference, "_connections_and_context", lambda _db, _flow: _async_value(connections))
    monkeypatch.setattr(inference, "build_inference_plan", lambda *_args, **_kwargs: _plan())
    ensured = []
    deleted_topics = []
    consumed = []
    monkeypatch.setattr(inference.topics, "ensure_topics", lambda _conn, specs: _async_value(ensured.extend(specs) or [{"ok": True, "name": spec.name} for spec in specs]))
    monkeypatch.setattr(inference.topics, "delete_topic", lambda _conn, name: _async_value(deleted_topics.append(name) or {"ok": True}))
    monkeypatch.setattr(
        inference.nifi_apply,
        "apply_plan",
        lambda _conn, _plan: _async_value(AppliedResult("pg-temp", "pc-temp", "temporary", True)),
    )
    monkeypatch.setattr(inference.nifi_apply, "start_pg", lambda *_args: _async_value({"ok": True}))
    monkeypatch.setattr(inference.nifi_apply, "delete_flow_pg", lambda *_args: _async_value({"ok": True}))
    deleted_pcs = []
    monkeypatch.setattr(inference.nifi_apply, "delete_parameter_context", lambda _conn, pc: _async_value(deleted_pcs.append(pc) or {"ok": True}))
    monkeypatch.setattr(
        inference.kafka_schema_consumer,
        "consume_messages_for_inference",
        lambda **_kwargs: _async_value(([{"id": 7}, {"id": 8, "platform_added": True}], "kafbat", None)),
    )
    monkeypatch.setattr(
        inference,
        "infer_avro_schema",
        lambda samples, name, namespace: consumed.append((samples, name, namespace)) or {"type": "record", "name": name, "namespace": namespace, "fields": [{"name": "id", "type": "long"}, {"name": "platform_added", "type": "boolean"}]},
    )

    await inference.run_inference_background(db, "schema-inference-job-1", _flow_doc())

    final = db[COLLECTIONS.schema_inference_jobs].documents["schema-inference-job-1"]
    assert final["status"] == "complete"
    assert final["messagesCollected"] == 2
    assert final["generatedSchema"]["fields"][-1]["name"] == "platform_added"
    assert final["nifiProcessGroupId"] is None
    assert consumed and len(consumed[0][0]) == 2 and consumed[0][1:] == ("asset", "raw.example_flow")
    assert deleted_pcs == ["pc-temp"]
    assert deleted_topics == ["dmp.schema_inference.example.asset.job-1"]


@pytest.mark.asyncio
async def test_v2_inference_does_not_consume_after_temporary_topic_setup_fails(monkeypatch):
    db = FakeDb(_job())
    connections = (
        {"endpoint": "https://nifi", "auth_type": "NONE"},
        {"endpoint": "kafka:9092", "kafbat_url": "https://kafbat"},
        CompileContext(),
        PlatformConnection(id="nifi", type="nifi"),
        PlatformConnection(id="kafka", type="kafka"),
    )
    monkeypatch.setattr(inference, "_connections_and_context", lambda _db, _flow: _async_value(connections))
    monkeypatch.setattr(inference, "build_inference_plan", lambda *_args, **_kwargs: _plan())
    deleted_topics = []
    monkeypatch.setattr(inference.topics, "ensure_topics", lambda *_args: _async_value([{"ok": False, "name": "temp", "error": "broker unavailable"}]))
    monkeypatch.setattr(inference.topics, "delete_topic", lambda _conn, name: _async_value(deleted_topics.append(name) or {"ok": True}))
    consumed = False

    async def should_not_consume(**_kwargs):
        nonlocal consumed
        consumed = True
        return [], "none", "unexpected"

    monkeypatch.setattr(inference.kafka_schema_consumer, "consume_messages_for_inference", should_not_consume)
    await inference.run_inference_background(db, "schema-inference-job-1", _flow_doc())

    final = db[COLLECTIONS.schema_inference_jobs].documents["schema-inference-job-1"]
    assert final["status"] == "failed"
    assert "temporary Kafka topic" in final["error"]
    assert not consumed
    # Cleanup is idempotent and still attempts the unique temporary topic even
    # when creation reported a failure; this prevents a partially-created topic
    # from leaking if the broker acknowledged creation before the API failed.
    assert deleted_topics == ["dmp.schema_inference.example.asset.job-1"]


async def _async_value(value):
    return value
