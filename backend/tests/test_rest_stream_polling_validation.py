import sys
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from models.source import SourceCreate
from services.rest_stream_polling_validation import (
    RestPollingValidationError,
    validate_rest_stream_polling,
)


def test_model_tolerates_legacy_disabled_polling_config():
    source = SourceCreate(
        name="legacy_polling_source",
        type="REST API",
        base_url="https://api.example.com",
        streams=[
            {
                "id": "records",
                "name": "records",
                "endpoint_path": "/records",
                "response_format": "json",
                "polling": {"enabled": False},
            }
        ],
    )

    assert source.streams[0].polling.enabled is False
    validate_rest_stream_polling(source.model_dump())


def test_rejects_enabled_polling_config():
    with pytest.raises(RestPollingValidationError, match="polling has been removed"):
        validate_rest_stream_polling(
            {
                "streams": [
                    {
                        "id": "poll_job",
                        "response_format": "json",
                        "polling": {
                            "enabled": True,
                            "status_path": "$.status",
                            "pending_values": ["running"],
                            "success_values": ["complete"],
                            "interval_seconds": 5,
                            "max_attempts": 3,
                        },
                    }
                ]
            }
        )
