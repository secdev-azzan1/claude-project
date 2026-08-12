import asyncio
import json
from functools import wraps
from pathlib import Path
import sys

import httpx

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from routers import sources
from test_content_store import FakeDB


def async_test(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


class CapturingAsyncClient:
    requests = []

    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def request(self, method, url, **kwargs):
        self.requests.append({"method": method, "url": url, **kwargs})
        return httpx.Response(
            200,
            content=json.dumps({"ok": True}).encode("utf-8"),
            headers={"content-type": "application/json"},
        )

    async def get(self, url, **kwargs):
        return await self.request("GET", url, **kwargs)

    async def post(self, url, **kwargs):
        return await self.request("POST", url, **kwargs)

    async def put(self, url, **kwargs):
        return await self.request("PUT", url, **kwargs)

    async def patch(self, url, **kwargs):
        return await self.request("PATCH", url, **kwargs)

    async def delete(self, url, **kwargs):
        return await self.request("DELETE", url, **kwargs)


@async_test
async def test_rest_test_stream_sends_confirmed_post_json_body(monkeypatch):
    CapturingAsyncClient.requests = []
    monkeypatch.setattr(sources.httpx, "AsyncClient", CapturingAsyncClient)
    db = FakeDB()
    db.sources.docs.append({
        "id": "source-1",
        "name": "API",
        "type": "REST API",
        "base_url": "https://api.example.com",
        "auth_type": "NONE",
        "streams": [
            {
                "id": "search",
                "name": "Search",
                "method": "POST",
                "endpoint_path": "/search",
                "body_format": "json",
                "body_template": '{"status":"${status}"}',
                "response_format": "json",
            }
        ],
    })

    result = await sources.test_stream(
        "source-1",
        sources.TestStreamRequest(stream_id="search", parent_values={"status": "active"}, confirm_mutation=True),
        db=db,
    )

    assert result["ok"] is True
    assert result["url"] == "https://api.example.com/search"
    sent = CapturingAsyncClient.requests[0]
    assert sent["method"] == "POST"
    assert sent["content"] == b'{"status":"active"}'
    assert sent["headers"]["Content-Type"] == "application/json"
    assert sent["headers"]["Accept"] == "application/json"


@async_test
async def test_rest_test_stream_rejects_mutating_method_without_confirmation():
    db = FakeDB()
    db.sources.docs.append({
        "id": "source-1",
        "name": "API",
        "type": "REST API",
        "base_url": "https://api.example.com",
        "auth_type": "NONE",
        "streams": [
            {
                "id": "delete",
                "name": "Delete",
                "method": "DELETE",
                "endpoint_path": "/records/1",
                "body_format": "empty",
                "response_format": "json",
            }
        ],
    })

    result = await sources.test_stream(
        "source-1",
        sources.TestStreamRequest(stream_id="delete"),
        db=db,
    )

    assert result["ok"] is False
    assert result["status_code"] == 422
    assert "requires explicit confirmation" in result["error"]
