import asyncio
import json

import pytest

from services import content_store, flow_import_finalize
from services.flow_import_finalize import FlowImportFinalizeError, finalize_flow_import
from tests.resilience.conftest import (
    FaultInjectingCollection,
    FaultPoint,
    InjectedFailure,
    ResilienceFakeDB,
)
from test_flow_import_finalize import credential_values
from test_flow_import_preview import make_flowpack


class RacingFlowCollection(FaultInjectingCollection):
    def __init__(self, expected_name):
        super().__init__(unique_fields=("id", "name"))
        self.expected_name = expected_name
        self.arrivals = 0
        self.all_arrived = asyncio.Event()

    async def insert_one(self, document):
        if document.get("name") == self.expected_name:
            self.arrivals += 1
            if self.arrivals == 2:
                self.all_arrived.set()
            await asyncio.wait_for(self.all_arrived.wait(), timeout=2)
        return await super().insert_one(document)


@pytest.mark.parametrize(
    ("fault_point", "collection_name"),
    [
        (FaultPoint.APPLICATION_SERVICE_INSERT, "application_services"),
        (FaultPoint.SOURCE_INSERT, "sources"),
        (FaultPoint.SCHEMA_INSERT, "schema_artifacts"),
        (FaultPoint.FLOW_INSERT, "flows"),
    ],
)
def test_import_rolls_back_every_insert_boundary(
    monkeypatch,
    tmp_path,
    fault_point,
    collection_name,
):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = ResilienceFakeDB()
    unique_fields = ("id", "name") if collection_name == "flows" else ()
    setattr(
        db,
        collection_name,
        FaultInjectingCollection(
            fault_point=fault_point,
            unique_fields=unique_fields,
        ),
    )
    package = make_flowpack()

    with pytest.raises(InjectedFailure, match=fault_point.value):
        asyncio.run(
            finalize_flow_import(
                json.dumps(package).encode("utf-8"),
                credential_values(package),
                {
                    "flow_name": f"resilience_{fault_point.value}",
                    "schema_resolutions": {},
                },
                db,
            ),
        )

    assert db.flows.docs == []
    assert db.sources.docs == []
    assert db.application_services.docs == []
    assert db.schema_artifacts.docs == []
    flows_root = tmp_path / "flows"
    assert not flows_root.exists() or not list(flows_root.glob("resilience_*"))


def test_import_removes_partial_content_store_bundle(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = ResilienceFakeDB()
    package = make_flowpack()

    async def failing_sync(db_arg, flow_id):
        flow = await db_arg.flows.find_one({"id": flow_id})
        bundle = content_store.flow_dir(flow["name"])
        bundle.mkdir(parents=True, exist_ok=True)
        content_store.atomic_write_json(bundle / "flow.json", {"id": flow_id})
        raise InjectedFailure(FaultPoint.CONTENT_STORE_SYNC.value)

    monkeypatch.setattr(flow_import_finalize.content_store, "sync_flow", failing_sync)

    with pytest.raises(InjectedFailure, match="content_store_sync"):
        asyncio.run(
            finalize_flow_import(
                json.dumps(package).encode("utf-8"),
                credential_values(package),
                {
                    "flow_name": "resilience_content_store_failure",
                    "schema_resolutions": {},
                },
                db,
            ),
        )

    assert db.flows.docs == []
    assert not (tmp_path / "flows" / "resilience_content_store_failure").exists()


def test_duplicate_flow_name_is_reported_as_import_conflict(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = ResilienceFakeDB()
    db.flows.docs.append({"id": "existing", "name": "resilience_duplicate"})
    package = make_flowpack()

    with pytest.raises(FlowImportFinalizeError, match="Flow name already exists"):
        asyncio.run(
            finalize_flow_import(
                json.dumps(package).encode("utf-8"),
                credential_values(package),
                {
                    "flow_name": "resilience_duplicate",
                    "schema_resolutions": {},
                },
                db,
            ),
        )


def test_concurrent_imports_with_same_name_leave_one_complete_import(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    flow_name = "resilience_concurrent_import"
    db = ResilienceFakeDB()
    db.flows = RacingFlowCollection(flow_name)
    db.schema_artifacts.docs.append(
        {
            "artifact_id": "bronze.imported_rapid7.sites-value",
            "versions": [
                {
                    "version": 1,
                    "schema": {"type": "record", "name": "Site", "fields": []},
                }
            ],
        }
    )
    package = make_flowpack()
    raw = json.dumps(package).encode("utf-8")
    options = {
        "flow_name": flow_name,
        "schema_resolutions": {
            "bronze.imported_rapid7.sites-value": {"action": "link_existing"},
        },
    }

    async def run_race():
        return await asyncio.gather(
            finalize_flow_import(raw, credential_values(package), options, db),
            finalize_flow_import(raw, credential_values(package), options, db),
            return_exceptions=True,
        )

    results = asyncio.run(run_race())

    successful = [result for result in results if isinstance(result, dict)]
    conflicts = [result for result in results if isinstance(result, FlowImportFinalizeError)]
    assert len(successful) == 1, repr(results)
    assert len(conflicts) == 1
    assert "already exists" in str(conflicts[0]).lower()
    assert len(db.flows.docs) == 1
    assert len(db.sources.docs) == 1
    assert len(db.application_services.docs) == 1
    assert len(db.openapi_specs.docs) == 1
    assert len(db.schema_artifacts.docs) == 1
    assert db.flows.docs[0]["name"] == flow_name
    assert content_store.flow_dir(flow_name).exists()
