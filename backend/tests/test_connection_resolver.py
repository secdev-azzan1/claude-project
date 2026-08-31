import asyncio
from functools import wraps
from pathlib import Path
import sys

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services.connection_resolver import resolve_connection


def async_test(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


class FakeCollection:
    def __init__(self, docs):
        self.docs = [dict(doc) for doc in docs]
        self.updates = []

    async def find_one(self, query, projection=None, sort=None):
        docs = list(self.docs)
        if sort:
            for field, direction in reversed(sort):
                docs.sort(key=lambda d: d.get(field), reverse=direction < 0)

        def matches(doc):
            for key, value in query.items():
                if key == "$or":
                    if not any(all(doc.get(k) == v for k, v in clause.items()) for clause in value):
                        return False
                elif doc.get(key) != value:
                    return False
            return True

        for doc in docs:
            if matches(doc):
                return dict(doc)
        return None

    async def update_one(self, query, update):
        self.updates.append((dict(query), dict(update)))
        for doc in self.docs:
            if all(doc.get(k) == v for k, v in query.items()):
                doc.update(update.get("$set") or {})
                return


class FakeDB:
    def __init__(self, docs, v2_docs=None):
        self.connections = FakeCollection(docs)
        self.connections_v2 = FakeCollection(v2_docs or [])

    def __getitem__(self, name):
        return getattr(self, name)


@async_test
async def test_resolve_connection_accepts_v2_active_flag():
    db = FakeDB(
        [
            {
                "id": "kc-1",
                "type": "kafka_connect",
                "name": "Kafka Connect",
                "active": True,
                "updated_at": "2026-08-15T12:00:00.000Z",
            }
        ]
    )

    resolved = await resolve_connection(db, "kafka_connect")

    assert resolved is not None
    assert resolved["id"] == "kc-1"
    assert resolved["active"] is True
    assert resolved["is_active"] is True
    assert db.connections.docs[0]["active"] is True
    assert db.connections.docs[0]["is_active"] is True


@async_test
async def test_resolve_connection_falls_back_to_v2_kafka_connect_url():
    db = FakeDB(
        [],
        [
            {
                "id": "kc-v2",
                "type": "kafka_connect",
                "name": "Kafka Connect v2",
                "active": True,
                "config": {"url": "https://connect.example.test"},
            }
        ],
    )

    resolved = await resolve_connection(db, "kafka_connect")

    assert resolved == {
        "id": "kc-v2",
        "type": "kafka_connect",
        "name": "Kafka Connect v2",
        "active": True,
        "config": {"url": "https://connect.example.test"},
        "endpoint": "https://connect.example.test",
        "auth_type": "NONE",
        "username": None,
        "password": None,
        "token": None,
        "is_active": True,
    }
