import asyncio
from functools import wraps
from pathlib import Path
import sys

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import kafka_client


def async_test(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


def test_ensure_topic_exists_uses_kafbat_for_kafbat_mode(monkeypatch):
    called = {"kafbat": False, "native": False}

    async def fake_kafbat_count(url, username, password, topic):
        called["kafbat"] = True
        assert url == "https://kafbat.example.com"
        assert username == "admin"
        assert password == "secret"
        assert topic == "bronze.dummyjson.user__history"
        return {"ok": True, "total_messages": 3, "source": "kafbat"}

    def fake_native(*args, **kwargs):
        called["native"] = True
        return {"ok": False, "error": "native should not be called"}

    monkeypatch.setattr(kafka_client, "_kafbat_topic_message_count", fake_kafbat_count)
    monkeypatch.setattr(kafka_client, "_ensure_topic_exists_sync", fake_native)

    @async_test
    async def run():
        result = await kafka_client.ensure_topic_exists(
            {
                "endpoint": "https://kafbat.example.com",
                "kafka_connection_mode": "kafbat",
                "kafbat_url": "https://kafbat.example.com",
                "kafbat_username": "admin",
                "kafbat_password": "secret",
            },
            "bronze.dummyjson.user__history",
        )
        assert result["ok"] is True
        assert result["created"] is False
        assert result["source"] == "kafbat"
        assert "verified via Kafbat" in result["message"]

    run()

    assert called == {"kafbat": True, "native": False}
