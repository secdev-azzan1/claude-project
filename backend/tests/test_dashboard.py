import asyncio
from functools import wraps
from pathlib import Path
import sys


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from routers.dashboard import get_summary
from test_content_store import FakeCollection


def async_test(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


class CountCollection(FakeCollection):
    async def count_documents(self, query):
        from test_content_store import _matches

        return sum(1 for doc in self.docs if _matches(doc, query))

    def aggregate(self, pipeline):
        class Cursor:
            async def to_list(self, length):
                return []

        return Cursor()


class DashboardDB:
    def __init__(self):
        self.sources = CountCollection([{"id": "source-1"}, {"id": "source-2"}])
        self.flows = CountCollection([
            {"id": "flow-1", "state": "Running", "runs": []},
            {"id": "flow-2", "state": "Stopped", "runs": []},
            {"id": "flow-3", "state": "Running", "runs": []},
        ])
        self.connections = CountCollection([{"health": "Healthy"}, {"health": "Failed"}])
        self.schema_artifacts = CountCollection([
            {"artifact_id": "schema-1", "versions": [
                {"version": 1, "status": "Verified"},
                {"version": 2, "status": "Verified"},
            ]},
            {"artifact_id": "schema-2", "versions": [{"version": 1, "status": "Needs Verification"}]},
            {"artifact_id": "schema-3", "versions": [{"version": 1, "status": "Verified"}]},
        ])


@async_test
async def test_dashboard_summary_counts_flows_and_verified_schema_artifacts():
    summary = await get_summary(DashboardDB())

    assert summary["totalFlows"] == 3
    assert summary["runningFlows"] == 2
    assert summary["verifiedSchemas"] == 2
