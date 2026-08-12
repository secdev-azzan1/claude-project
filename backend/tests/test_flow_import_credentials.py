import asyncio
import json
from functools import wraps
from pathlib import Path
import sys

import pytest
from fastapi import HTTPException

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services.flow_import_credentials import FlowImportCredentialError, validate_import_credentials
from test_content_store import FakeDB
from test_flow_import_preview import Upload, make_flowpack


def async_test(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


@async_test
async def test_validate_import_credentials_accepts_all_required_values():
    package = make_flowpack()
    raw = json.dumps(package).encode("utf-8")

    result = await validate_import_credentials(
        raw,
        {
            "services/application.json": {
                "services.0.config.rest_password": "secret-password",
            }
        },
        db=FakeDB(),
    )

    assert result == {
        "ok": True,
        "credentials_valid": True,
        "missing": [],
        "unexpected": [],
        "provided": [
            {"path": "services/application.json", "field": "services.0.config.rest_password"}
        ],
        "ready_for_import": True,
    }


@async_test
async def test_validate_import_credentials_rejects_missing_values():
    raw = json.dumps(make_flowpack()).encode("utf-8")

    with pytest.raises(FlowImportCredentialError) as exc:
        await validate_import_credentials(raw, {}, db=FakeDB())

    assert exc.value.result["credentials_valid"] is False
    assert exc.value.result["missing"] == [
        {"path": "services/application.json", "field": "services.0.config.rest_password"}
    ]


@async_test
async def test_validate_import_credentials_rejects_blank_values():
    raw = json.dumps(make_flowpack()).encode("utf-8")

    with pytest.raises(FlowImportCredentialError) as exc:
        await validate_import_credentials(
            raw,
            {"services/application.json": {"services.0.config.rest_password": "   "}},
            db=FakeDB(),
        )

    assert exc.value.result["missing"] == [
        {"path": "services/application.json", "field": "services.0.config.rest_password"}
    ]


@async_test
async def test_validate_import_credentials_rejects_unexpected_values():
    raw = json.dumps(make_flowpack()).encode("utf-8")

    with pytest.raises(FlowImportCredentialError) as exc:
        await validate_import_credentials(
            raw,
            {
                "services/application.json": {
                    "services.0.config.rest_password": "secret-password",
                    "services.0.config.api_key": "extra-secret",
                }
            },
            db=FakeDB(),
        )

    assert exc.value.result["unexpected"] == [
        {"path": "services/application.json", "field": "services.0.config.api_key"}
    ]


@async_test
async def test_validate_import_credentials_response_does_not_echo_secret_values():
    raw = json.dumps(make_flowpack()).encode("utf-8")

    result = await validate_import_credentials(
        raw,
        {"services/application.json": {"services.0.config.rest_password": "dont-echo-me"}},
        db=FakeDB(),
    )

    assert "dont-echo-me" not in json.dumps(result)


@async_test
async def test_import_credentials_router_returns_validation_result():
    from routers import flow_import

    result = await flow_import.validate_flowpack_import_credentials(
        file=Upload(json.dumps(make_flowpack()).encode("utf-8")),
        credentials_json=json.dumps(
            {
                "services/application.json": {
                    "services.0.config.rest_password": "secret-password",
                }
            }
        ),
        db=FakeDB(),
    )

    assert result["credentials_valid"] is True
    assert result["ready_for_import"] is True


@async_test
async def test_import_credentials_router_rejects_invalid_credentials_json():
    from routers import flow_import

    with pytest.raises(HTTPException) as exc:
        await flow_import.validate_flowpack_import_credentials(
            file=Upload(json.dumps(make_flowpack()).encode("utf-8")),
            credentials_json="{not-json",
            db=FakeDB(),
        )

    assert exc.value.status_code == 422
    assert "credentials_json" in exc.value.detail
