import asyncio
from functools import wraps
from pathlib import Path
import sys

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import routers.flows as flows
from test_content_store import FakeCollection


def async_test(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


class FakeDB:
    def __init__(self, flow_docs, source_docs):
        self.flows = FakeCollection(flow_docs)
        self.sources = FakeCollection(source_docs)
        self.connections = FakeCollection([])


async def _run_delete(db, flow_id):
    """Call delete_flow with content-store + audit side effects stubbed out."""

    async def _noop_async(*args, **kwargs):
        return None

    orig_audit = flows._log_audit
    orig_delete = flows.content_store.delete_flow
    orig_sync = flows.content_store.sync_source
    flows._log_audit = _noop_async
    flows.content_store.delete_flow = lambda *a, **k: None
    flows.content_store.sync_source = _noop_async
    try:
        await flows.delete_flow(flow_id, db=db)
    finally:
        flows._log_audit = orig_audit
        flows.content_store.delete_flow = orig_delete
        flows.content_store.sync_source = orig_sync


@async_test
async def test_deleting_flow_removes_its_orphaned_source():
    db = FakeDB(
        flow_docs=[{"id": "flow-1", "name": "F1", "source_id": "src-1"}],
        source_docs=[{"id": "src-1"}],
    )
    await _run_delete(db, "flow-1")
    assert await db.flows.find_one({"id": "flow-1"}) is None
    assert await db.sources.find_one({"id": "src-1"}) is None, "orphaned source should be deleted with its flow"


@async_test
async def test_deleting_flow_keeps_source_still_used_by_another_flow():
    db = FakeDB(
        flow_docs=[
            {"id": "flow-1", "name": "F1", "source_id": "src-1"},
            {"id": "flow-2", "name": "F2", "source_id": "src-1"},
        ],
        source_docs=[{"id": "src-1"}],
    )
    await _run_delete(db, "flow-1")
    assert await db.flows.find_one({"id": "flow-2"}) is not None
    assert await db.sources.find_one({"id": "src-1"}) is not None, "shared source must survive while another flow uses it"
