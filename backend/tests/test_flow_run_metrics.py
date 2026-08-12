import asyncio
import sys
from datetime import datetime
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from routers import flows


class FakeCollection:
    def __init__(self, docs):
        self.docs = docs

    async def find_one(self, query, projection=None):
        for doc in self.docs:
            if doc.get("id") == query.get("id"):
                return doc
        return None

    async def update_one(self, query, update):
        doc = await self.find_one(query)
        for path, value in update.get("$push", {}).items():
            doc.setdefault(path, []).append(value)
        for path, value in update.get("$set", {}).items():
            parts = path.split(".")
            target = doc
            for part in parts[:-1]:
                target = target[int(part)] if isinstance(target, list) else target[part]
            if isinstance(target, list):
                target[int(parts[-1])] = value
            else:
                target[parts[-1]] = value


class FakeDB:
    def __init__(self, flow):
        self.flows = FakeCollection([flow])


def test_records_processed_is_kafka_delta_not_lifetime_total():
    run = {"kafka_start_count": 54}

    assert flows._records_processed_for_run(run, kafka_total=81) == 27
    assert flows._records_processed_for_run(run, kafka_total=40) == 0


def test_append_run_uses_pre_start_kafka_baseline(monkeypatch):
    flow = {"id": "flow-1", "runs": []}
    db = FakeDB(flow)

    async def fail_if_metrics_are_read_after_start(flow_doc, db_arg):
        raise AssertionError("Kafka baseline should already be captured before NiFi starts")

    async def fake_sync(db_arg, flow_id):
        return None

    monkeypatch.setattr(flows, "_build_kafka_topic_metrics", fail_if_metrics_are_read_after_start)
    monkeypatch.setattr(flows.content_store, "sync_flow", fake_sync)

    asyncio.run(flows._append_flow_run_start(db, "flow-1", kafka_start_count=72))

    assert flow["runs"][0]["kafka_start_count"] == 72


def test_close_run_persists_kafka_delta(monkeypatch):
    flow = {
        "id": "flow-1",
        "total_records": 5,
        "runs": [
            {
                "id": "run-1",
                "started_at": datetime.utcnow(),
                "status": "Running",
                "records_processed": 0,
                "kafka_start_count": 54,
            }
        ],
    }
    db = FakeDB(flow)

    async def fake_metrics(flow_doc, db_arg):
        return {"kafka_topic_total_messages": 81}

    async def fake_sync(db_arg, flow_id):
        return None

    monkeypatch.setattr(flows, "_build_kafka_topic_metrics", fake_metrics)
    monkeypatch.setattr(flows.content_store, "sync_flow", fake_sync)

    asyncio.run(flows._close_latest_running_run(db, "flow-1"))

    assert flow["runs"][0]["records_processed"] == 27
    assert flow["total_records"] == 32
