from __future__ import annotations

import copy
from enum import StrEnum
from typing import Any

from pymongo.errors import DuplicateKeyError

class FaultPoint(StrEnum):
    APPLICATION_SERVICE_INSERT = "application_service_insert"
    SOURCE_INSERT = "source_insert"
    SCHEMA_INSERT = "schema_insert"
    FLOW_INSERT = "flow_insert"
    CONTENT_STORE_SYNC = "content_store_sync"
    NIFI_CREATE = "nifi_create"
    NIFI_PARTIAL_BUILD = "nifi_partial_build"
    FLOW_STATE_PERSIST = "flow_state_persist"


class InjectedFailure(RuntimeError):
    pass


class FakeResult:
    def __init__(self, modified_count: int = 1):
        self.modified_count = modified_count
        self.deleted_count = modified_count
        self.inserted_id = "inserted"


def _get_nested(document: dict[str, Any], dotted_key: str) -> Any:
    value: Any = document
    for part in dotted_key.split("."):
        if isinstance(value, list):
            value = value[int(part)]
        elif isinstance(value, dict):
            value = value.get(part)
        else:
            return None
    return value


def _matches(document: dict[str, Any], query: dict[str, Any]) -> bool:
    for key, expected in query.items():
        actual = _get_nested(document, key)
        if isinstance(expected, dict):
            if "$ne" in expected and actual == expected["$ne"]:
                return False
            if "$in" in expected and actual not in expected["$in"]:
                return False
            if "$nin" in expected and actual in expected["$nin"]:
                return False
        elif actual != expected:
            return False
    return True


def _set_nested(document: dict[str, Any], dotted_key: str, value: Any) -> None:
    parts = dotted_key.split(".")
    target: Any = document
    for part in parts[:-1]:
        if isinstance(target, list):
            target = target[int(part)]
        else:
            target = target.setdefault(part, {})
    if isinstance(target, list):
        target[int(parts[-1])] = value
    else:
        target[parts[-1]] = value


class FakeCursor:
    def __init__(self, documents: list[dict[str, Any]]):
        self.documents = documents

    def sort(self, *_args, **_kwargs):
        return self

    def limit(self, count: int):
        self.documents = self.documents[:count]
        return self

    async def to_list(self, length: int | None):
        if length is None:
            return copy.deepcopy(self.documents)
        return copy.deepcopy(self.documents[:length])


class FaultInjectingCollection:
    def __init__(
        self,
        documents: list[dict[str, Any]] | None = None,
        *,
        fault_point: FaultPoint | None = None,
        fail_on_call: int = 1,
        unique_fields: tuple[str, ...] = (),
    ):
        self.docs = copy.deepcopy(documents or [])
        self.fault_point = fault_point
        self.fail_on_call = fail_on_call
        self.unique_fields = unique_fields
        self.calls = 0

    def _maybe_fail(self) -> None:
        self.calls += 1
        if self.fault_point and self.calls == self.fail_on_call:
            raise InjectedFailure(self.fault_point.value)

    async def find_one(self, query, projection=None, sort=None):
        matches = [doc for doc in self.docs if _matches(doc, query)]
        if sort:
            key, direction = sort[0]
            matches.sort(key=lambda doc: _get_nested(doc, key), reverse=direction < 0)
        return copy.deepcopy(matches[0]) if matches else None

    def find(self, query=None, projection=None):
        return FakeCursor(
            [doc for doc in self.docs if _matches(doc, query or {})],
        )

    async def insert_one(self, document):
        self._maybe_fail()
        for field in self.unique_fields:
            value = _get_nested(document, field)
            if any(_get_nested(existing, field) == value for existing in self.docs):
                raise DuplicateKeyError(f"duplicate:{field}:{value}")
        self.docs.append(copy.deepcopy(document))
        return FakeResult()

    async def update_one(self, query, update):
        self._maybe_fail()
        for document in self.docs:
            if not _matches(document, query):
                continue
            for key, value in update.get("$set", {}).items():
                _set_nested(document, key, copy.deepcopy(value))
            for key in update.get("$unset", {}):
                target = document
                parts = key.split(".")
                for part in parts[:-1]:
                    target = target.get(part, {})
                target.pop(parts[-1], None)
            for key, value in update.get("$push", {}).items():
                current = _get_nested(document, key)
                if current is None:
                    _set_nested(document, key, [])
                    current = _get_nested(document, key)
                current.append(copy.deepcopy(value))
            for key, value in update.get("$inc", {}).items():
                _set_nested(document, key, int(_get_nested(document, key) or 0) + value)
            return FakeResult(1)
        return FakeResult(0)

    async def find_one_and_update(
        self,
        query,
        update,
        return_document=None,
        projection=None,
    ):
        before = await self.find_one(query)
        if before is None:
            return None
        await self.update_one(query, update)
        if return_document is None:
            return before
        identity_query = {"id": before["id"]} if before.get("id") else query
        return await self.find_one(identity_query)

    async def delete_one(self, query):
        self._maybe_fail()
        before = len(self.docs)
        self.docs = [doc for doc in self.docs if not _matches(doc, query)]
        return FakeResult(before - len(self.docs))


class ResilienceFakeDB:
    def __init__(self):
        self.flows = FaultInjectingCollection(unique_fields=("id", "name"))
        self.sources = FaultInjectingCollection(unique_fields=("id",))
        self.schema_artifacts = FaultInjectingCollection(
            unique_fields=("artifact_id",),
        )
        self.schema_inference_jobs = FaultInjectingCollection(unique_fields=("id",))
        self.application_services = FaultInjectingCollection(unique_fields=("id",))
        self.nifi_global_services = FaultInjectingCollection(unique_fields=("id",))
        self.openapi_specs = FaultInjectingCollection(unique_fields=("id",))
        self.connections = FaultInjectingCollection()
        self.audit_events = FaultInjectingCollection()
        self.connection_lifecycle_jobs = FaultInjectingCollection(unique_fields=("id",))


def make_rest_source(
    *,
    source_id: str = "source-1",
    stream_id: str = "stream-1",
) -> dict[str, Any]:
    return {
        "id": source_id,
        "name": "Resilience Source",
        "type": "REST API",
        "state": "Saved",
        "streams": [
            {
                "id": stream_id,
                "name": "records",
                "is_primary": True,
                "endpoint_path": "/records",
                "entity": {
                    "schema_artifact_id": "resilience.records-value",
                    "schema_version": 1,
                    "kafka": {
                        "topic": "resilience.records",
                        "schema_artifact_id": "resilience.records-value",
                        "schema_version": 1,
                    },
                },
            }
        ],
    }


def make_verified_rest_flow(
    *,
    flow_id: str = "flow-1",
    source_id: str = "source-1",
    state: str = "Stopped",
) -> dict[str, Any]:
    return {
        "id": flow_id,
        "name": "Resilience Flow",
        "source_id": source_id,
        "source_type": "REST API",
        "primary_stream_id": "stream-1",
        "state": state,
        "enabled": True,
        "schema_artifact_id": "resilience.records-value",
        "schema_version": 1,
        "runs": [],
        "total_records": 0,
    }


def assert_no_transitional_state(flow: dict[str, Any]) -> None:
    assert flow.get("state") not in {
        "Deploying",
        "Starting",
        "Stopping",
    }
    assert flow.get("schema_inference_active") is not True
