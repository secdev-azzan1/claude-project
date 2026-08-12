"""Validation helpers for REST stream pagination configuration."""
from __future__ import annotations

from typing import Any, Optional


class RestPaginationValidationError(ValueError):
    """Raised when REST pagination configuration is unsafe or unsupported."""


def _stream_id(stream: dict, fallback_idx: int = 0) -> str:
    return str(stream.get("id") or stream.get("name") or f"stream_{fallback_idx}").strip()


def _ptype(value: Optional[str]) -> str:
    text = str(value or "none").strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "none": "none",
        "page": "page",
        "page_increment": "page",
        "cursor": "cursor",
        "cursor_pagination": "cursor",
        "offset": "offset",
        "offset_limit": "offset",
        "offset_pagination": "offset",
        "next_url": "next_url",
        "next": "next_url",
        "next_url_pagination": "next_url",
    }
    return aliases.get(text, "unsupported")


def _positive_int(value: Any) -> bool:
    try:
        return int(value) > 0
    except Exception:
        return False


def _has_text(value: Any) -> bool:
    return bool(str(value or "").strip())


def _metadata_source(pag: dict, *keys: str) -> bool:
    return any(_has_text(pag.get(key)) for key in keys)


def validate_rest_stream_pagination(source: dict) -> None:
    for idx, stream in enumerate([s for s in (source.get("streams") or []) if isinstance(s, dict)]):
        sid = _stream_id(stream, idx)
        pag = stream.get("pagination") or {}
        if not isinstance(pag, dict) or not pag.get("enabled"):
            continue

        response_format = str(stream.get("response_format") or "json").strip().lower()
        ptype = _ptype(pag.get("type"))
        if ptype == "unsupported":
            raise RestPaginationValidationError(f"Stream {sid} has unsupported pagination type {pag.get('type')}.")
        if ptype == "none":
            raise RestPaginationValidationError(f"Stream {sid} pagination type cannot be none when enabled.")

        stop_norm = str(pag.get("stop_condition") or "").strip().upper().replace("-", "_").replace(" ", "_")
        has_intrinsic_terminator = (
            ptype in {"cursor", "next_url"}
            or stop_norm in {"EMPTY_ARRAY", "EMPTY", "EMPTY_PAGE"}
        )
        if not has_intrinsic_terminator and not _positive_int(pag.get("max_pages")):
            raise RestPaginationValidationError(
                f"Stream {sid} needs a safety limit (max_pages): a "
                f"{stop_norm.lower() or 'metadata-based'} stop can loop forever if the API omits its "
                f"stop metadata at runtime."
            )

        if not stream.get("split_response_records"):
            raise RestPaginationValidationError(
                f"Stream {sid} pagination requires split response records to be enabled."
            )

        stop_cond = str(pag.get("stop_condition") or "").strip().upper().replace("-", "_").replace(" ", "_")
        if stop_cond == "TOTAL_COUNT" and not _metadata_source(pag, "total_count_path", "total_count_header"):
            raise RestPaginationValidationError(
                f"Stream {sid} total count stop condition requires total_count_path or total_count_header."
            )
        if stop_cond == "HAS_MORE" and not _metadata_source(pag, "has_more_path", "has_more_header"):
            raise RestPaginationValidationError(
                f"Stream {sid} has_more stop condition requires has_more_path or has_more_header."
            )

        location = str(pag.get("location") or "query").strip().lower()
        if location not in {"query", "body"}:
            raise RestPaginationValidationError(f"Stream {sid} pagination location must be query or body.")
        if location == "body":
            method = str(stream.get("method") or "GET").strip().upper()
            if method not in {"POST", "PUT", "PATCH"}:
                raise RestPaginationValidationError(
                    f"Stream {sid} request-body pagination requires method POST, PUT or PATCH."
                )
            if str(stream.get("body_format") or "empty").strip().lower() != "json":
                raise RestPaginationValidationError(
                    f"Stream {sid} request-body pagination requires a JSON body format."
                )
            if ptype == "next_url":
                raise RestPaginationValidationError(
                    f"Stream {sid} next URL pagination cannot send values in the request body."
                )

        header_format = str(pag.get("next_url_header_format") or "").strip().lower()
        if header_format and header_format not in {"raw", "link"}:
            raise RestPaginationValidationError(
                f"Stream {sid} next_url_header_format must be raw or link."
            )

        if ptype == "page":
            if not _has_text(pag.get("page_param")):
                raise RestPaginationValidationError(f"Stream {sid} page pagination requires page_param.")
            if not _has_text(pag.get("page_size_param")):
                raise RestPaginationValidationError(f"Stream {sid} page pagination requires page_size_param.")
            if not _positive_int(pag.get("page_size")):
                raise RestPaginationValidationError(f"Stream {sid} page pagination requires positive page_size.")
            continue

        if ptype == "offset":
            if not _has_text(pag.get("offset_param")):
                raise RestPaginationValidationError(f"Stream {sid} offset pagination requires offset_param.")
            if not _has_text(pag.get("limit_param")):
                raise RestPaginationValidationError(f"Stream {sid} offset pagination requires limit_param.")
            if not _positive_int(pag.get("page_size")):
                raise RestPaginationValidationError(f"Stream {sid} offset pagination requires positive page_size.")
            continue

        if ptype == "cursor":
            if not _has_text(pag.get("cursor_param")):
                raise RestPaginationValidationError(f"Stream {sid} cursor pagination requires cursor_param.")
            if response_format == "csv" and not _metadata_source(pag, "cursor_header"):
                raise RestPaginationValidationError(f"Stream {sid} CSV cursor pagination requires header metadata source.")
            if response_format != "csv" and not _metadata_source(pag, "cursor_path", "cursor_header"):
                raise RestPaginationValidationError(f"Stream {sid} cursor pagination requires cursor_path or cursor_header.")
            continue

        if ptype == "next_url":
            if response_format == "csv" and not _metadata_source(pag, "next_url_header"):
                raise RestPaginationValidationError(f"Stream {sid} CSV next URL pagination requires header metadata source.")
            if response_format != "csv" and not _metadata_source(pag, "next_url_path", "next_url_header"):
                raise RestPaginationValidationError(f"Stream {sid} next URL pagination requires next_url_path or next_url_header.")
            continue
