import asyncio

import pytest
from pymongo.errors import DuplicateKeyError

from tests.resilience.conftest import (
    FaultInjectingCollection,
    FaultPoint,
    InjectedFailure,
    ResilienceFakeDB,
)


def test_fault_injection_fails_on_configured_call():
    collection = FaultInjectingCollection(
        fault_point=FaultPoint.SCHEMA_INSERT,
        fail_on_call=2,
    )

    asyncio.run(collection.insert_one({"id": "first"}))
    with pytest.raises(InjectedFailure, match="schema_insert"):
        asyncio.run(collection.insert_one({"id": "second"}))


def test_unique_fields_reject_duplicate_values():
    db = ResilienceFakeDB()

    asyncio.run(db.flows.insert_one({"id": "one", "name": "Same"}))
    with pytest.raises(DuplicateKeyError, match="duplicate:name:Same"):
        asyncio.run(db.flows.insert_one({"id": "two", "name": "Same"}))


def test_find_one_and_update_applies_atomic_query():
    collection = FaultInjectingCollection(
        [{"id": "flow-1", "state": "Stopped"}],
    )

    claimed = asyncio.run(
        collection.find_one_and_update(
            {"id": "flow-1", "state": {"$ne": "Deploying"}},
            {"$set": {"state": "Deploying"}},
            return_document=True,
        ),
    )
    rejected = asyncio.run(
        collection.find_one_and_update(
            {"id": "flow-1", "state": {"$ne": "Deploying"}},
            {"$set": {"state": "Deploying"}},
            return_document=True,
        ),
    )

    assert claimed["state"] == "Deploying"
    assert rejected is None
