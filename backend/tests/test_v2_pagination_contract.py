"""Regression tests for the active v2 pagination UI/compiler contract."""

from models.adapter import AppService, Flow, FlowBlock
from services.adapter.validation import validate_block


HTTP_SERVICE = AppService(
    id="svc-http", type="http", name="Public API", retired=False,
    health="Healthy", config={"baseUrl": "https://example.test", "authMode": "none"},
)


def _issues(pagination, *, mode="read", write_forwards="response"):
    block = FlowBlock(
        id="http", adapter="http", mode=mode, name="Read API", parentId=None,
        serviceId="svc-http",
        config={
            "method": "GET" if mode == "read" else "POST",
            "path": "/items", "writeForwards": write_forwards,
            "pagination": pagination,
        },
    )
    flow = Flow(id="flow", name="Flow", blocks=[block], topics=[], variables=[], servicePins={})
    return [issue.message for issue in validate_block(flow, block, [HTTP_SERVICE], [])]


def test_metadata_stops_require_positive_max_pages():
    for ptype, stop_key in (("page", "stop"), ("offset", "offsetStop")):
        for stop in ("total_count", "has_more"):
            messages = _issues({"type": ptype, "fields": {stop_key: stop}})
            assert any("maximum-pages safety limit" in message for message in messages)

            messages = _issues({"type": ptype, "fields": {stop_key: stop, "maxPages": "10"}})
            assert not any("maximum-pages safety limit" in message for message in messages)


def test_intrinsic_stop_modes_allow_blank_max_pages():
    configs = [
        {"type": "page", "fields": {"stop": "empty_response"}},
        {"type": "offset", "fields": {"offsetStop": "empty_response"}},
        {"type": "cursor", "fields": {"cursorSource": "body"}},
        {"type": "next_url", "fields": {"nextUrlSource": "link_header"}},
    ]
    for pagination in configs:
        assert not any("maximum pages" in message.lower() for message in _issues(pagination))


def test_numeric_pagination_fields_are_validated():
    assert any("page size" in message.lower() for message in _issues({
        "type": "page", "fields": {"sizeValue": "zero"},
    }))
    assert any("limit" in message.lower() for message in _issues({
        "type": "offset", "fields": {"limitValue": "0"},
    }))
    assert any("cursor page size" in message.lower() for message in _issues({
        "type": "cursor", "fields": {"cursorSizeValue": "1.5"},
    }))
    assert any("maximum pages" in message.lower() for message in _issues({
        "type": "next_url", "fields": {"maxPages": "-1"},
    }))


def test_write_pagination_requires_counter_style_and_response_forwarding():
    assert any("page or offset" in message for message in _issues(
        {"type": "cursor", "fields": {}}, mode="write",
    ))
    assert any("Continue with" in message for message in _issues(
        {"type": "page", "fields": {}}, mode="write", write_forwards="original",
    ))

