import asyncio
import sys
from functools import wraps
from pathlib import Path

import pytest
from fastapi import HTTPException

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from models.flow import FlowCreate
from routers.flows import _validate_verified_schema_for_flow


def async_test(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


class FakeCollection:
    def __init__(self, docs=None):
        self.docs = list(docs or [])

    async def find_one(self, query, projection=None):
        for doc in self.docs:
            if all(doc.get(key) == value for key, value in query.items()):
                if projection:
                    return {key: doc[key] for key, include in projection.items() if include and key in doc}
                return dict(doc)
        return None


class FakeDB:
    def __init__(self, sources=None, schema_artifacts=None):
        self.sources = FakeCollection(sources)
        self.schema_artifacts = FakeCollection(schema_artifacts)


def test_flow_create_accepts_trino_without_schema_link():
    flow = FlowCreate(
        name="Asset History Snapshot Flow",
        source_id="source-trino",
        source_type="trino",
        primary_stream_id="asset__history",
        kafka_topic="bronze.asset_history.snapshot_rows",
    )

    assert flow.source_type == "Trino"
    assert flow.schema_artifact_id is None
    assert flow.schema_version is None


@async_test
async def test_trino_flow_runtime_schema_validation_is_optional():
    db = FakeDB(
        sources=[
            {
                "id": "source-trino",
                "type": "Trino",
                "streams": [
                    {
                        "id": "asset__history",
                        "name": "asset__history",
                        "entity": {"kafka": {"topic": "bronze.asset_history.snapshot_rows"}},
                    }
                ],
            }
        ]
    )
    flow = {
        "id": "flow-trino",
        "name": "Asset History Snapshot Flow",
        "source_id": "source-trino",
        "source_type": "Trino",
        "primary_stream_id": "asset__history",
        "kafka_topic": "bronze.asset_history.snapshot_rows",
    }

    await _validate_verified_schema_for_flow(flow, db)


@async_test
async def test_non_trino_flow_still_requires_verified_schema():
    db = FakeDB(
        sources=[
            {
                "id": "source-rest",
                "type": "REST API",
                "streams": [
                    {
                        "id": "users",
                        "name": "users",
                        "entity": {"kafka": {"topic": "bronze.users"}},
                    }
                ],
            }
        ]
    )
    flow = {
        "id": "flow-rest",
        "name": "Users Flow",
        "source_id": "source-rest",
        "source_type": "REST API",
        "primary_stream_id": "users",
        "kafka_topic": "bronze.users",
    }

    with pytest.raises(HTTPException) as exc:
        await _validate_verified_schema_for_flow(flow, db)

    assert exc.value.status_code == 400
