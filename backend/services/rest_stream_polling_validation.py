"""Validation helpers for deprecated REST stream polling configuration."""
from __future__ import annotations

class RestPollingValidationError(ValueError):
    """Raised when REST polling configuration is unsafe or unsupported."""


def _stream_id(stream: dict, fallback_idx: int = 0) -> str:
    return str(stream.get("id") or stream.get("name") or f"stream_{fallback_idx}").strip()


def validate_rest_stream_polling(source: dict) -> None:
    for idx, stream in enumerate([s for s in (source.get("streams") or []) if isinstance(s, dict)]):
        sid = _stream_id(stream, idx)
        polling = stream.get("polling") or {}
        if not isinstance(polling, dict) or not polling.get("enabled"):
            continue
        raise RestPollingValidationError(
            f"Stream {sid} uses polling, but REST polling has been removed from the application."
        )
