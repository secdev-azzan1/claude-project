import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services.adapter.deployer import bookmark_store  # noqa: E402


class _FakeRedis:
    deleted = None

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def ping(self):
        return True

    def delete(self, *keys):
        _FakeRedis.deleted = keys
        return len(keys)

    def close(self):
        pass


def test_delete_flow_bookmarks_targets_only_the_flow_keys(monkeypatch):
    _FakeRedis.deleted = None
    monkeypatch.setattr(bookmark_store, "redis_lib", SimpleNamespace(Redis=_FakeRedis))

    result = asyncio.run(
        bookmark_store.delete_flow_bookmarks(
            {"host": "redis.internal", "port": 6379, "bookmarksDb": 7, "password": "secret"},
            "flow-1",
            ["jdbc-read", "jdbc-read-2"],
        )
    )

    assert result == {"ok": True, "deleted": 2}
    assert _FakeRedis.deleted == (
        "dmp:jdbc:bookmark:flow-1:jdbc-read",
        "dmp:jdbc:bookmark:flow-1:jdbc-read-2",
    )

