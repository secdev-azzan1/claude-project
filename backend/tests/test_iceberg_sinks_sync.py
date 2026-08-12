import asyncio
from functools import wraps
from pathlib import Path
import sys

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import iceberg_sinks


def async_test(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


class FakeCursor:
    def __init__(self, docs):
        self._docs = list(docs)

    def __aiter__(self):
        self._iter = iter(self._docs)
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration


class FakeCollection:
    def __init__(self):
        self.docs = []

    def _matches(self, doc, query):
        for key, value in query.items():
            if isinstance(value, dict) and "$nin" in value:
                if doc.get(key) in value["$nin"]:
                    return False
            elif isinstance(value, dict) and "$ne" in value:
                if doc.get(key) == value["$ne"]:
                    return False
            elif isinstance(value, dict) and "$in" in value:
                if doc.get(key) not in value["$in"]:
                    return False
            else:
                if doc.get(key) != value:
                    return False
        return True

    async def find_one(self, query, projection=None):
        for doc in self.docs:
            if self._matches(doc, query):
                return dict(doc)
        return None

    def find(self, query=None, projection=None):
        query = query or {}
        matched = [dict(doc) for doc in self.docs if self._matches(doc, query)]
        return FakeCursor(matched)

    async def insert_one(self, doc):
        self.docs.append(dict(doc))

    async def update_one(self, query, update, upsert=False):
        for doc in self.docs:
            if self._matches(doc, query):
                doc.update(update.get("$set", {}))
                return
        if upsert:
            new_doc = dict(query)
            new_doc.update(update.get("$set", {}))
            self.docs.append(new_doc)

    async def delete_many(self, query):
        self.docs = [doc for doc in self.docs if not self._matches(doc, query)]


class FakeDB:
    def __init__(self):
        self.iceberg_sinks = FakeCollection()
        self.flows = FakeCollection()


def make_stream(stream_id, topic, iceberg_intent):
    return {
        "id": stream_id,
        "name": stream_id,
        "entity": {
            "kafka": {"topic": topic},
            "iceberg": iceberg_intent,
        },
    }


def make_flow(flow_id="flow-1", source_id="source-1", name="CRM"):
    return {"id": flow_id, "source_id": source_id, "name": name}


@async_test
async def test_creates_sink_doc_when_intent_present_and_enabled():
    db = FakeDB()
    flow = make_flow()
    source = {
        "id": "source-1",
        "streams": [
            make_stream("users", "bronze.crm.users", {"enabled": True}),
        ],
    }

    sinks = await iceberg_sinks.sync_sinks_for_flow(db, flow, source)

    assert len(sinks) == 1
    sink = sinks[0]
    assert sink["flow_id"] == "flow-1"
    assert sink["stream_id"] == "users"
    assert sink["topic"] == "bronze.crm.users"
    assert sink["table_name"] == "crm.users"
    assert sink["connector_name"] == "bronze.crm.users__iceberg"
    assert sink["enabled"] is True


@async_test
async def test_creates_doc_even_when_disabled():
    db = FakeDB()
    flow = make_flow()
    source = {
        "id": "source-1",
        "streams": [
            make_stream("users", "bronze.crm.users", {"enabled": False}),
        ],
    }

    sinks = await iceberg_sinks.sync_sinks_for_flow(db, flow, source)

    assert len(sinks) == 1
    assert sinks[0]["enabled"] is False


@async_test
async def test_runtime_disable_survives_unchanged_resave():
    """Key test: runtime disables a sink (enabled=False), overrides stay unchanged
    (designer intent still enabled=True at last sync). Re-syncing with the SAME
    designer intent must NOT flip enabled back to True."""
    db = FakeDB()
    flow = make_flow()
    source = {
        "id": "source-1",
        "streams": [
            make_stream("users", "bronze.crm.users", {"enabled": True}),
        ],
    }

    # First sync creates the doc enabled=True.
    await iceberg_sinks.sync_sinks_for_flow(db, flow, source)

    # Runtime disables it (e.g. via a lifecycle action), but overrides is untouched
    # (still records designer intent enabled=True from last sync).
    await db.iceberg_sinks.update_one(
        {"flow_id": "flow-1", "stream_id": "users"},
        {"$set": {"enabled": False}},
    )

    # Re-sync with the SAME designer intent (enabled still True in the source doc).
    sinks = await iceberg_sinks.sync_sinks_for_flow(db, flow, source)

    assert len(sinks) == 1
    assert sinks[0]["enabled"] is False  # runtime state survives


@async_test
async def test_designer_toggle_change_overrides_runtime_state():
    db = FakeDB()
    flow = make_flow()
    source = {
        "id": "source-1",
        "streams": [
            make_stream("users", "bronze.crm.users", {"enabled": True}),
        ],
    }

    await iceberg_sinks.sync_sinks_for_flow(db, flow, source)

    # Runtime disables it.
    await db.iceberg_sinks.update_one(
        {"flow_id": "flow-1", "stream_id": "users"},
        {"$set": {"enabled": False}},
    )

    # Designer now explicitly disables it too (a real intent change).
    source["streams"][0]["entity"]["iceberg"] = {"enabled": False}
    sinks = await iceberg_sinks.sync_sinks_for_flow(db, flow, source)
    assert sinks[0]["enabled"] is False

    # Designer re-enables it: intent_now (True) != intent_at_sync (False) -> follow intent.
    source["streams"][0]["entity"]["iceberg"] = {"enabled": True}
    sinks = await iceberg_sinks.sync_sinks_for_flow(db, flow, source)
    assert sinks[0]["enabled"] is True


@async_test
async def test_removed_legacy_iceberg_fields_are_ignored():
    db = FakeDB()
    flow = make_flow()
    source = {
        "id": "source-1",
        "streams": [
            make_stream(
                "users",
                "bronze.crm.users",
                {"enabled": True, "removed_legacy_field": "ignored"},
            ),
        ],
    }

    sinks = await iceberg_sinks.sync_sinks_for_flow(db, flow, source)

    assert sinks[0]["table_name"] == "crm.users"
    assert sinks[0]["overrides"] == {"enabled": True}


@async_test
async def test_doc_deleted_when_iceberg_block_removed():
    db = FakeDB()
    flow = make_flow()
    source = {
        "id": "source-1",
        "streams": [
            make_stream("users", "bronze.crm.users", {"enabled": True}),
        ],
    }

    sinks = await iceberg_sinks.sync_sinks_for_flow(db, flow, source)
    assert len(sinks) == 1

    # Designer removes the iceberg block entirely.
    source["streams"][0]["entity"].pop("iceberg", None)
    sinks = await iceberg_sinks.sync_sinks_for_flow(db, flow, source)

    assert sinks == []
    assert db.iceberg_sinks.docs == []


@async_test
async def test_stream_without_topic_is_skipped():
    db = FakeDB()
    flow = make_flow()
    source = {
        "id": "source-1",
        "streams": [
            make_stream("users", "", {"enabled": True}),
        ],
    }

    sinks = await iceberg_sinks.sync_sinks_for_flow(db, flow, source)

    assert sinks == []


@async_test
async def test_legacy_flow_without_entity_iceberg_produces_no_sinks():
    db = FakeDB()
    flow = make_flow()
    source = {
        "id": "source-1",
        "streams": [
            {"id": "users", "name": "users", "entity": {"kafka": {"topic": "bronze.crm.users"}}},
        ],
    }

    sinks = await iceberg_sinks.sync_sinks_for_flow(db, flow, source)

    assert sinks == []


@async_test
async def test_sync_sinks_for_source_updates_all_flows():
    db = FakeDB()
    source = {
        "id": "source-1",
        "streams": [
            make_stream("users", "bronze.crm.users", {"enabled": True}),
        ],
    }
    await db.flows.insert_one(make_flow("flow-1", "source-1"))
    await db.flows.insert_one(make_flow("flow-2", "source-1"))

    await iceberg_sinks.sync_sinks_for_source(db, source)

    sinks_1 = await iceberg_sinks.list_sinks_for_flow(db, "flow-1")
    sinks_2 = await iceberg_sinks.list_sinks_for_flow(db, "flow-2")
    assert len(sinks_1) == 1
    assert len(sinks_2) == 1


@async_test
async def test_find_shared_table_conflicts_warns_across_flows():
    db = FakeDB()
    source = {
        "id": "source-1",
        "streams": [
            make_stream("users", "bronze.crm.users", {"enabled": True}),
        ],
    }

    await iceberg_sinks.sync_sinks_for_flow(db, make_flow("flow-1", "source-1"), source)
    await iceberg_sinks.sync_sinks_for_flow(db, make_flow("flow-2", "source-1"), source)

    conflicts = await iceberg_sinks.find_shared_table_conflicts(db, "flow-1")

    assert len(conflicts) == 1
    assert conflicts[0]["flow_id"] == "flow-2"
    assert conflicts[0]["table_name"] == "crm.users"
