"""`http` adapter compilation — compiler-spec.md §3.1.

FULL scope: all three modes (read/write/lookup), all six auth modes, proxy
egress base-URL swap, all four pagination styles (read only), json/csv/xml
response parsing.

Design used throughout (read mode), per the pokeapi reference wiring
(nifi-reference-flows.md §3.4/§9.2) — review finding C4: NiFi EL is
single-pass, an EL template STORED IN AN ATTRIBUTE is never re-evaluated, so
pagination placeholders must live where EL actually runs per FlowFile — on
the PROCESSOR PROPERTY itself.
  - offset/page/cursor pagination: `fetch`'s "HTTP URL" property carries the
    full URL template directly (e.g.
    `#{svc_x_base_url}/path?offset=${offset}&limit=${limit}`) — InvokeHTTP
    evaluates it fresh against the incoming FlowFile's attributes on every
    request, exactly like the reference `Invoke Page` processor. `init` seeds
    only the literal counter attributes (`offset`/`limit`/`page`/...), and
    `next` recomputes them with EL at the UpdateAttribute
    (`offset = ${offset:toNumber():plus(N)}`), mirroring `Init Offset` /
    `Next Offset` in the reference.
  - `next_url` pagination and no pagination: the URL contains no counter
    placeholders, so `init` stores the concrete URL in `request.url` and
    `fetch` uses `${request.url}` (proven live — Journey C ran this shape);
    for `next_url`, `next` overwrites `request.url` with the server-given
    absolute URL each iteration.

Response parsing runs BEFORE the pagination-continue check (compiler-spec
§3.1 lists parsing at step 5, pagination at step 6): `fetch`'s `Response`
relationship goes to `_parse_response()` (json: `SplitJson` directly;
csv/xml: `ConvertRecord` CSVReader/XMLReader -> JsonRecordSetWriter first,
landing every format on the same per-record JSON shape the rest of the
compiler assumes, THEN `SplitJson` — see that function's docstring for why
`SplitXml` was NOT picked for xml) or directly onward (when `split: false`);
`SplitJson`'s `split` relationship feeds the record chain unconditionally,
while its `original` relationship (or, when not splitting, a second
connection off `fetch`'s own `Response` relationship) feeds `page_meta` —
matching the rapid7-securado/sentinelone reference flows' "decouple
pagination-continue from per-record processing" shape (see
`docs/orchestration/analysis/nifi-reference-flows.md` §4.7/§5.7).

`write`/`lookup` modes (both new): entry point is still the public
`compile_read()` — `compile_flow.py` always calls that one name regardless
of `block.mode` — so it dispatches internally to `_compile_write()` /
`_compile_lookup()`. See those functions' docstrings for the write-mode
"chain continues with" (`writeForwards`) design and the lookup-mode
response-merge design, including the honest limitation the lookup chain
documents (no true two-flowfile-lineage merge — NiFi has no join primitive
in scope here).
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from models.adapter import AppService, FlowBlock
from services.adapter.validation import block_proxy_id
from services.adapter.naming import tokenize

from .ir import CompileError, ControllerServiceSpec, ProcessorSpec, ensure_json_record_services
from .transforms import Tail, cron_or_period

if TYPE_CHECKING:  # pragma: no cover
    from models.adapter import Flow
    from .ir import BlockBuilder, CompileContext

_INVOKE_HTTP_BASELINE: Dict[str, Any] = {
    # compiler-spec §3.1 / nifi-reference-flows.md §9.1 baseline.
    "Connection Timeout": "5 secs",
    "Socket Read Timeout": "15 secs",
    "Socket Write Timeout": "15 secs",
    "Socket Idle Timeout": "5 mins",
    "Socket Idle Connections": "5",
    "Response Cache Enabled": "false",
    "Response Cache Size": "10MB",
    "Response FlowFile Naming Strategy": "RANDOM",
    # MVP explicitly pins this False (unlike the general reference baseline's True).
    "Response Redirects Enabled": "False",
    "Request Date Header Enabled": "True",
    "Request Content-Type": "${mime.type}",
    "Request Content-Encoding": "DISABLED",
    "Request Body Enabled": "false",
    "Request Multipart Form-Data Filename Enabled": "true",
    "Request Chunked Transfer-Encoding Enabled": "false",
    "HTTP/2 Disabled": "True",
}

_INVOKE_HTTP_AUTOTERMINATE = ["No Retry", "Retry", "Original"]


def _service_for(block: FlowBlock, ctx: "CompileContext") -> AppService:
    if not block.serviceId:
        raise CompileError(f"http block {block.id!r} has no service selected")
    svc = ctx.services.get(block.serviceId)
    if svc is None:
        raise CompileError(f"http block {block.id!r} references unknown service {block.serviceId!r}")
    return svc


def _base_url_expr(*, block: FlowBlock, service: AppService, ctx: "CompileContext", add_param) -> str:
    """`#{svc_<id>_base_url}`, or the APISIX proxy swap when a proxy is bound."""
    proxy_id = block_proxy_id(block, list(ctx.services.values()))
    add_param(f"svc_{service.id}_base_url", str(service.config.get("baseUrl", "")), False)
    if not proxy_id:
        return f"#{{svc_{service.id}_base_url}}"
    proxy = ctx.gateway_proxies.get(proxy_id)
    if proxy is None:
        raise CompileError(f"http block {block.id!r} references unknown gateway proxy {proxy_id!r}")
    add_param("apisix_runtime_url", ctx.connection_config("apisix").get("runtimeUrl", ""), False)
    proxy_token = tokenize(proxy.name)
    return f"#{{apisix_runtime_url}}/{proxy_token}"


def _session_header_value(service: AppService) -> str:
    """The header value injected on session_token-authed requests. The
    service's optional `tokenTemplate` (default `${token}`) describes how the
    extracted token is framed — e.g. `Bearer ${token}` — and `${token}` is
    replaced with the EL for the extracted token attribute, so the compiled
    header value is e.g. `Bearer ${session.token}`."""
    template = str(service.config.get("tokenTemplate") or "${token}")
    return template.replace("${token}", "${session.token}")


def _apply_auth(
    builder: "BlockBuilder", *, service: AppService, props: Dict[str, Any], add_param,
    api_key_query_handled: bool = False,
) -> None:
    mode = service.config.get("authMode", "none")
    sid = service.id
    if mode == "none":
        return
    if mode == "basic":
        add_param(f"svc_{sid}_username", str(service.config.get("username", "")), False)
        add_param(f"svc_{sid}_password", service.config.get("password"), True)
        props["Request Username"] = f"#{{svc_{sid}_username}}"
        props["Request Password"] = f"#{{svc_{sid}_password}}"
        return
    if mode == "bearer":
        add_param(f"svc_{sid}_token", service.config.get("token"), True)
        props["Authorization"] = f"Bearer #{{svc_{sid}_token}}"
        return
    if mode == "api_key":
        add_param(f"svc_{sid}_key_value", service.config.get("keyValue"), True)
        key_name = str(service.config.get("keyName", "X-Api-Key")) or "X-Api-Key"
        location = service.config.get("keyLocation", "header")
        if location == "query":
            # M16: a query-location key belongs in the URL's query string,
            # never in a dynamic property (InvokeHTTP emits every dynamic
            # property as a request HEADER — a prose-named one is malformed
            # AND leaks the secret into a second location). Read mode folds
            # the key into the URL itself via `_build_query`
            # (api_key_query_handled=True); write/lookup append it to the
            # concrete "HTTP URL" template here — parameter references are
            # legal inside the URL property, so this stays EL/param-safe.
            if not api_key_query_handled:
                url = str(props.get("HTTP URL", "") or "")
                sep = "&" if "?" in url else "?"
                props["HTTP URL"] = f"{url}{sep}{key_name}=#{{svc_{sid}_key_value}}"
        else:
            props[key_name] = f"#{{svc_{sid}_key_value}}"
        return
    if mode == "oauth2":
        add_param(f"svc_{sid}_token_url", str(service.config.get("tokenUrl", "")), False)
        add_param(f"svc_{sid}_client_id", str(service.config.get("clientId", "")), False)
        add_param(f"svc_{sid}_client_secret", service.config.get("clientSecret"), True)
        cs_key = f"cs_oauth2_{sid}"
        if not builder.has_cs(cs_key):
            builder.add_cs(
                ControllerServiceSpec(
                    key=cs_key, name=f"oauth2_{sid}", type="org.apache.nifi.oauth2.StandardOauth2AccessTokenProvider",
                    properties={
                        "Authorization Server URL": f"#{{svc_{sid}_token_url}}",
                        "Client ID": f"#{{svc_{sid}_client_id}}",
                        "Client Secret": f"#{{svc_{sid}_client_secret}}",
                        "Grant Type": "Client Credentials",
                    },
                )
            )
        props["Request OAuth2 Access Token Provider"] = cs_key
        return
    if mode == "session_token":
        header = str(service.config.get("tokenHeader", "Authorization")) or "Authorization"
        props[header] = _session_header_value(service)
        return
    raise CompileError(f"Unknown http auth mode {mode!r} on service {sid!r}")


def _build_query(pagination: Dict[str, Any], *, key_value_query_param: Optional[Tuple[str, str]]) -> str:
    fields = pagination.get("fields", {}) or {}
    parts = []
    ptype = pagination.get("type", "none")
    if ptype == "offset":
        parts.append(f"{fields.get('offsetParam', 'offset')}=${{offset}}")
        parts.append(f"{fields.get('limitParam', 'limit')}=${{limit}}")
    elif ptype == "page":
        parts.append(f"{fields.get('pageParam', 'page')}=${{page}}")
        size_param = fields.get("sizeParam", "size")
        parts.append(f"{size_param}=${{page_size}}")
    elif ptype == "cursor":
        parts.append(f"{fields.get('cursorParam', 'cursor')}=${{cursor}}")
    if key_value_query_param:
        name, param_ref = key_value_query_param
        parts.append(f"{name}={param_ref}")
    return "&".join(parts)


def _probe_path(record_path: str) -> str:
    rp = (record_path or "$").strip()
    if rp.endswith("[*]"):
        return rp[: -len("[*]")] + "[0]"
    return rp


def _ensure_csv_reader(builder: "BlockBuilder") -> str:
    """Add (once per group) a generic `CSVReader` CS. `Schema Access
    Strategy: infer-schema` (matching `ir.ensure_json_record_services`'s own
    JSON reader) rather than the reference flows' `schema-name` strategy —
    an ad-hoc http response has no registered Avro schema to bind to."""
    key = "cs_csv_reader"
    if not builder.has_cs(key):
        builder.add_cs(
            ControllerServiceSpec(
                key=key, name="csv_reader", type="org.apache.nifi.csv.CSVReader",
                properties={"Schema Access Strategy": "infer-schema", "Treat First Line as Header": "true"},
            )
        )
    return key


def _ensure_xml_reader(builder: "BlockBuilder") -> str:
    """Add (once per group) a generic `XMLReader` CS. Same `infer-schema`
    rationale as `_ensure_csv_reader`; `Expect Records as Array: false`
    matches the task brief's explicit instruction for the analogous kafka
    xml-parse path (blocks_kafka.py)."""
    key = "cs_xml_reader"
    if not builder.has_cs(key):
        builder.add_cs(
            ControllerServiceSpec(
                key=key, name="xml_reader", type="org.apache.nifi.xml.XMLReader",
                properties={"Schema Access Strategy": "infer-schema", "Expect Records as Array": "false"},
            )
        )
    return key


def _parse_response(
    builder: "BlockBuilder", *, source: Tail, response_format: str, split: bool, record_path: str, ptype: str,
) -> "Tuple[Tail, Tail]":
    """`source`'s content -> `(record_tail, original_tail)`.

    json feeds `SplitJson` directly. csv/xml first go through `ConvertRecord`
    (`CSVReader`/`XMLReader` -> `JsonRecordSetWriter`) so every response
    format lands on the same per-record JSON shape the rest of the compiler
    (transforms/dedup/routing, all JSON-record-oriented via
    `ir.ensure_json_record_services`) assumes, THEN `SplitJson` — picked over
    `SplitXml` for xml specifically (compiler-spec §3.1 item 5 allows either;
    this is the documented choice) so csv and xml share one code path instead
    of xml alone needing a structurally different downstream shape.
    `original_tail` feeds pagination's `page_meta` step (read mode only —
    write mode always calls this with `ptype="none"`, so `original` always
    auto-terminates, matching read's own "no pagination configured" case).

    Shared by `compile_read` (its own `fetch` -> `Response`) and
    `_compile_write`'s `writeForwards: "response"` branch (its `write`
    InvokeHTTP's own `Response`) — this is the "route Response through the
    same parse chain builder as read" the task brief asks for.
    """
    src_key, src_rel = source
    if response_format == "json":
        parse_source: Tail = source
    elif response_format in ("csv", "xml"):
        reader_key = _ensure_csv_reader(builder) if response_format == "csv" else _ensure_xml_reader(builder)
        _, writer_key = ensure_json_record_services(builder)
        builder.add_processor(
            ProcessorSpec(key="convert", name="convert", type="org.apache.nifi.processors.standard.ConvertRecord",
                          properties={"Record Reader": reader_key, "Record Writer": writer_key})
        )
        builder.link(src_key, "convert", [src_rel])
        builder.to_dlq("convert", "failure")
        parse_source = ("convert", "success")
    else:
        raise NotImplementedError(
            f"http response format {response_format!r} is not implemented (json/csv/xml only)"
        )

    if not split:
        return parse_source, parse_source

    p_key, p_rel = parse_source
    builder.add_processor(
        ProcessorSpec(key="split", name="split", type="org.apache.nifi.processors.standard.SplitJson",
                      properties={"JsonPath Expression": record_path},
                      autoTerminate=(["original"] if ptype == "none" else []))
    )
    builder.link(p_key, "split", [p_rel])
    builder.to_dlq("split", "failure")
    return ("split", "split"), ("split", "original")


def compile_read(
    builder: "BlockBuilder",
    *,
    flow: "Flow",
    block: FlowBlock,
    ctx: "CompileContext",
    flow_token: str,
    is_root: bool,
    add_param,
) -> Tail:
    if block.mode == "write":
        return _compile_write(builder, flow=flow, block=block, ctx=ctx, flow_token=flow_token,
                              is_root=is_root, add_param=add_param)
    if block.mode == "lookup":
        return _compile_lookup(builder, flow=flow, block=block, ctx=ctx, flow_token=flow_token,
                               is_root=is_root, add_param=add_param)
    if block.mode != "read":
        raise CompileError(f"Unknown http mode {block.mode!r} on block {block.id}")

    response_format = str(block.config.get("responseFormat", "json"))

    service = _service_for(block, ctx)
    pagination = block.config.get("pagination") or {"type": "none", "fields": {}}
    ptype = pagination.get("type", "none")
    split = bool(block.config.get("split", True))
    record_path = str(block.config.get("recordPath", "$"))
    path = str(block.config.get("path", ""))

    # ---- trigger / input port -------------------------------------------------
    if is_root:
        builder.add_processor(_build_trigger(flow))
        entry_key = "trigger"
    else:
        entry_key = "inputPort"

    # ---- init: seed pagination + request.url -----------------------------------
    key_value_query_param = None
    if service.config.get("authMode") == "api_key" and service.config.get("keyLocation") == "query":
        sid = service.id
        key_name = str(service.config.get("keyName", "api_key")) or "api_key"
        key_value_query_param = (key_name, f"#{{svc_{sid}_key_value}}")

    query = _build_query(pagination, key_value_query_param=key_value_query_param)
    base_expr = _base_url_expr(block=block, service=service, ctx=ctx, add_param=add_param)
    initial_url = f"{base_expr}{path}" + (f"?{query}" if query else "")

    # C4: the pagination EL placeholders must sit on the property NiFi
    # actually evaluates per FlowFile — `fetch`'s own "HTTP URL" — because EL
    # stored inside an attribute value is never re-evaluated. Styles whose
    # URL embeds counter placeholders (offset/page/cursor) therefore put the
    # template on `fetch` directly; `none`/`next_url` have a concrete URL, so
    # it is stored in `request.url` (`next` overwrites it for next_url).
    url_on_fetch = ptype in ("offset", "page", "cursor")

    init_props: Dict[str, Any] = {"Accept": "application/json", "mime.type": "application/json"}
    if not url_on_fetch:
        init_props["request.url"] = initial_url
    if ptype == "offset":
        fields = pagination.get("fields", {})
        init_props["offset"] = "0"
        init_props["limit"] = str(fields.get("limitValue", "100"))
        init_props["page_count"] = "0"
    elif ptype == "page":
        fields = pagination.get("fields", {})
        init_props["page"] = str(fields.get("firstPage", "1"))
        init_props["page_size"] = str(fields.get("sizeValue", "100"))
        init_props["page_count"] = "0"
    elif ptype == "cursor":
        init_props["cursor"] = ""
        init_props["page_count"] = "0"
    elif ptype == "next_url":
        init_props["page_count"] = "0"

    builder.add_processor(
        ProcessorSpec(key="init", name="init", type="org.apache.nifi.processors.attributes.UpdateAttribute",
                      properties=init_props)
    )
    builder.link(entry_key, "init", ["success"] if entry_key == "trigger" else [])
    fetch_source: Tail = ("init", "success")

    # ---- session_token login -------------------------------------------------
    if service.config.get("authMode") == "session_token":
        fetch_source = _build_session_login(builder, service=service, add_param=add_param, source_key="init")

    # ---- fetch -----------------------------------------------------------------
    invoke_props: Dict[str, Any] = {**_INVOKE_HTTP_BASELINE, "HTTP Method": "GET",
                                     "HTTP URL": initial_url if url_on_fetch else "${request.url}"}
    if service.config.get("authMode") == "session_token":
        header = str(service.config.get("tokenHeader", "Authorization")) or "Authorization"
        invoke_props[header] = _session_header_value(service)
    else:
        _apply_auth(builder, service=service, props=invoke_props, add_param=add_param,
                    api_key_query_handled=key_value_query_param is not None)
    if ptype == "cursor" and (pagination.get("fields") or {}).get("cursorSource") == "header":
        invoke_props["Response Header Request Attributes Enabled"] = "true"
        invoke_props["Response Header Request Attributes Pattern"] = (pagination["fields"].get("cursorHeaderName", "cursor"))

    builder.add_processor(
        ProcessorSpec(key="fetch", name="fetch", type="org.apache.nifi.processors.standard.InvokeHTTP",
                      properties=invoke_props, autoTerminate=list(_INVOKE_HTTP_AUTOTERMINATE))
    )
    builder.link(fetch_source[0], "fetch", [fetch_source[1]])
    builder.to_dlq("fetch", "Failure")

    # ---- response parsing --------------------------------------------------
    record_tail, original_tail = _parse_response(
        builder, source=("fetch", "Response"), response_format=response_format, split=split,
        record_path=record_path, ptype=ptype,
    )

    # ---- pagination ----------------------------------------------------------
    if ptype != "none":
        _build_pagination(builder, ptype=ptype, pagination=pagination, record_path=record_path,
                          original_tail=original_tail, loop_target="fetch")

    return record_tail


def _build_trigger(flow: "Flow") -> ProcessorSpec:
    period, strategy = cron_or_period(flow.cron)
    return ProcessorSpec(
        key="trigger", name="trigger", type="org.apache.nifi.processors.standard.GenerateFlowFile",
        properties={"File Size": "0B", "Batch Size": "1", "Unique FlowFiles": "false", "Custom Text": "{}",
                    "Character Set": "UTF-8", "Data Format": "Text"},
        schedulingPeriod=period, schedulingStrategy=strategy, runOnPrimary=True,
    )


def _build_session_login(builder: "BlockBuilder", *, service: AppService, add_param, source_key: str) -> Tail:
    """session_token login chain (E5 — rewritten after the live Journey C
    failure): seed -> `login_body` (`ReplaceText`, Always Replace, JSON body
    `{"username":"#{...}","password":"#{...}"}` with both credentials as
    parameters, password sensitive) -> `login` (`InvokeHTTP` POST with
    Request Body Enabled + Content-Type application/json) ->
    `extract_token` (`EvaluateJsonPath` -> `session.token`).

    The previous Basic-Auth-property approach (`Request Username`/`Request
    Password`) was PROVEN BROKEN live: session-token login APIs (dummyjson's
    `/auth/login`, and the pattern generally) require a JSON body and ignore
    Basic-Auth headers — NiFi's bulletin captured the exact 400
    ("Username and password required"). The service's own Test button
    (`_test_http_session_token`) already POSTs a JSON body; the compiled flow
    now matches it.

    M7: `login` auto-terminates ONLY `Original`; `Failure`/`Retry`/`No Retry`
    are connected to `run_failure__log` — exactly one disposition per
    relationship (the builder-level invariant enforces this at compile time).

    Run failures (login_body/login/extract_token) are NOT record failures —
    no DLQ record is fabricated; they surface via the error-level log.
    """
    sid = service.id
    login_path = str(service.config.get("loginPath", "/login"))
    base_expr = f"#{{svc_{sid}_base_url}}"

    add_param(f"svc_{sid}_username", str(service.config.get("username", "")), False)
    add_param(f"svc_{sid}_password", service.config.get("password"), True)
    body = f'{{"username":"#{{svc_{sid}_username}}","password":"#{{svc_{sid}_password}}"}}'
    builder.add_processor(
        ProcessorSpec(key="login_body", name="login_body", type="org.apache.nifi.processors.standard.ReplaceText",
                      properties={"Replacement Strategy": "Always Replace", "Replacement Value": body,
                                  "Evaluation Mode": "Entire text", "Character Set": "UTF-8"})
    )
    builder.link(source_key, "login_body", ["success"] if source_key != "inputPort" else [])

    props: Dict[str, Any] = {**_INVOKE_HTTP_BASELINE, "HTTP Method": "POST",
                              "HTTP URL": f"{base_expr}{login_path}",
                              "Request Body Enabled": "true",
                              "Request Content-Type": "application/json"}
    builder.add_processor(
        ProcessorSpec(key="login", name="login", type="org.apache.nifi.processors.standard.InvokeHTTP",
                      properties=props, autoTerminate=["Original"])
    )
    builder.link("login_body", "login", ["success"])

    token_path = str(service.config.get("tokenPath", "$.token"))
    builder.add_processor(
        ProcessorSpec(key="extract_token", name="extract_token", type="org.apache.nifi.processors.standard.EvaluateJsonPath",
                      properties={"Destination": "flowfile-attribute", "Return Type": "scalar",
                                  "Path Not Found Behavior": "warn", "session.token": token_path})
    )
    builder.link("login", "extract_token", ["Response"])

    builder.add_processor(
        ProcessorSpec(key="run_failure__log", name="run_failure__log",
                      type="org.apache.nifi.processors.standard.LogAttribute",
                      properties={"Log Level": "error", "Log Payload": "false"}, autoTerminate=["success"])
    )
    # Run failures (login chain) are NOT record failures -- no DLQ record.
    builder.link("login_body", "run_failure__log", ["failure"])
    builder.link("login", "run_failure__log", ["Failure", "Retry", "No Retry"])
    builder.link("extract_token", "run_failure__log", ["failure", "unmatched"])
    return "extract_token", "matched"


_EVALUATE_JSON_PATH = "org.apache.nifi.processors.standard.EvaluateJsonPath"
_UPDATE_ATTRIBUTE = "org.apache.nifi.processors.attributes.UpdateAttribute"


def _build_pagination(
    builder: "BlockBuilder", *, ptype: str, pagination: Dict[str, Any], record_path: str,
    original_tail: Tail, loop_target: str,
) -> None:
    fields = pagination.get("fields", {}) or {}
    max_pages = fields.get("maxPages")

    if ptype == "offset" or ptype == "page":
        page_meta_type = _EVALUATE_JSON_PATH
        page_meta_props = {"Destination": "flowfile-attribute", "Return Type": "scalar",
                            "Path Not Found Behavior": "ignore", "probe": _probe_path(record_path)}
        cond = "${probe:isEmpty():not()}"
        next_props = (
            {"offset": "${offset:toNumber():plus(" + str(fields.get("limitValue", 100)) + ")}"}
            if ptype == "offset" else {"page": "${page:toNumber():plus(1)}"}
        )
    elif ptype == "cursor":
        source = fields.get("cursorSource", "body")
        if source == "body":
            page_meta_type = _EVALUATE_JSON_PATH
            page_meta_props = {"Destination": "flowfile-attribute", "Return Type": "scalar",
                                "Path Not Found Behavior": "ignore",
                                "next_cursor": fields.get("cursorPath", "$.nextCursor")}
        else:
            # Best-effort attribute reference for header-sourced cursors:
            # InvokeHTTP's "Response Header Request Attributes Pattern" (set
            # on `fetch`) promotes the matching response header to
            # `invokehttp.response.header.<name>`.
            page_meta_type = _UPDATE_ATTRIBUTE
            header_name = fields.get("cursorHeaderName", "cursor")
            page_meta_props = {"next_cursor": "${invokehttp.response.header." + str(header_name) + "}"}
        cond = "${next_cursor:trim():isEmpty():not()}"
        next_props = {"cursor": "${next_cursor}"}
    elif ptype == "next_url":
        page_meta_type = _EVALUATE_JSON_PATH
        page_meta_props = {"Destination": "flowfile-attribute", "Return Type": "scalar",
                            "Path Not Found Behavior": "ignore", "next_url": fields.get("urlPath", "$.next")}
        cond = "${next_url:trim():isEmpty():not()}"
        next_props = {"request.url": "${next_url}"}
    else:  # pragma: no cover - guarded by caller
        raise CompileError(f"Unknown pagination type {ptype!r}")

    builder.add_processor(ProcessorSpec(key="page_meta", name="page_meta", type=page_meta_type, properties=page_meta_props))
    src_key, src_rel = original_tail
    builder.link(src_key, "page_meta", [src_rel])

    if page_meta_type == _EVALUATE_JSON_PATH:
        # "ignore" means a missing path never fails the record; forward both
        # outcomes into the continuation check rather than guessing which one
        # NiFi would pick for a single-property EvaluateJsonPath.
        builder.link("page_meta", "has_more", ["matched", "unmatched"])
        builder.to_dlq("page_meta", "failure")
    else:
        # UpdateAttribute has no "failure" relationship to route to DLQ.
        builder.link("page_meta", "has_more", ["success"])

    if max_pages:
        cond = "${" + cond[2:-1] + ":and(${page_count:toNumber():lt(" + str(max_pages) + ")})}"
    builder.add_processor(
        ProcessorSpec(key="has_more", name="has_more", type="org.apache.nifi.processors.standard.RouteOnAttribute",
                      properties={"Routing Strategy": "Route to Property name", "continue": cond},
                      autoTerminate=["unmatched"])
    )

    next_props["page_count"] = "${page_count:toNumber():plus(1)}"
    builder.add_processor(
        ProcessorSpec(key="next", name="next", type="org.apache.nifi.processors.attributes.UpdateAttribute",
                      properties=next_props)
    )
    builder.link("has_more", "next", ["continue"])
    builder.link("next", loop_target, ["success"])


# ------------------------------------------------------------- write / lookup


_EL_FIELD_RE = re.compile(r"\$\{([a-zA-Z_][a-zA-Z0-9_.]*)\}")

# Bare `${...}` tokens that are NiFi/compiler built-ins, not record fields —
# excluded from the "promote to attribute via JsonPath" extraction so we
# never try to `$.uuid` a flowfile's own `uuid` attribute out of its JSON
# content. Deliberately small and explicit rather than clever pattern
# matching (mirrors this module's "keep it simple, document it" pattern).
_EL_BUILTIN_DENY = {
    "uuid", "now", "filename", "mime.type", "session.token", "request.url",
    "offset", "limit", "page", "page_size", "cursor", "page_count",
}


def _el_field_refs(template: str) -> List[str]:
    """Every distinct bare `${field}` reference in `template` that is NOT a
    known built-in — the record fields a body/path template needs promoted
    to flowfile attributes before NiFi's own EL evaluates the template."""
    seen: List[str] = []
    for m in _EL_FIELD_RE.finditer(template or ""):
        name = m.group(1)
        if name in _EL_BUILTIN_DENY or name in seen:
            continue
        seen.append(name)
    return seen


def _extract_fields(builder: "BlockBuilder", *, key: str, fields: List[str], source: Tail) -> Tail:
    """One `EvaluateJsonPath` promoting each of `fields` (record JsonPath
    `$.<field>`) to a same-named flowfile attribute — mirrors routing.py's
    `route_fields` exactly (same property shape, same `Path Not Found
    Behavior: ignore` + `autoTerminate=["unmatched"]` + DLQ-on-failure, same
    `(key, "matched")` return convention), reused here so a write's body
    template / a lookup's path template can reference `${field}` over the
    parent record via NiFi EL."""
    props: Dict[str, Any] = {"Destination": "flowfile-attribute", "Path Not Found Behavior": "ignore",
                              "Return Type": "scalar"}
    for f in fields:
        props[f] = f"$.{f}"
    builder.add_processor(
        ProcessorSpec(key=key, name=key, type=_EVALUATE_JSON_PATH, properties=props, autoTerminate=["unmatched"])
    )
    src_key, src_rel = source
    builder.link(src_key, key, [src_rel] if src_rel else [])
    builder.to_dlq(key, "failure")
    return key, "matched"


def _compile_write(
    builder: "BlockBuilder", *, flow: "Flow", block: FlowBlock, ctx: "CompileContext", flow_token: str,
    is_root: bool, add_param,
) -> Tail:
    """compiler-spec §3.1 item 7: request-body materialization ahead of a
    POST/PUT/PATCH `InvokeHTTP`, then "chain continues with" (`writeForwards`
    config, BlockForm.tsx) honored on the outbound connection:
      - `"original"` (default): `write`'s `Original` relationship (the
        pre-POST record, untouched) continues the chain — `Response` is
        unused, auto-terminated.
      - `"response"`: `write`'s `Response` relationship is routed through
        the SAME `_parse_response()` json/csv/xml parse-chain builder read
        mode uses (compiler-spec's literal "route Response through the same
        parse chain builder as read") — `Original` is unused, auto-terminated.
    Root-legal (compute_root_menu lists "http-write" — "a POST whose
    response is the data to process", i.e. root http-write only really makes
    sense with `writeForwards: "response"`, but both values compile either
    way): a trigger seeds it exactly like read mode, same `_build_trigger`.
    """
    service = _service_for(block, ctx)
    method = str(block.config.get("method") or "POST").upper()
    if method not in ("POST", "PUT", "PATCH"):
        raise CompileError(f"http write block {block.id!r} has invalid method {method!r} (POST/PUT/PATCH only)")
    path = str(block.config.get("path", ""))
    body_template = str(block.config.get("bodyTemplate", "") or "")
    write_forwards = str(block.config.get("writeForwards", "original") or "original")

    if is_root:
        builder.add_processor(_build_trigger(flow))
        entry_key = "trigger"
    else:
        entry_key = "inputPort"

    builder.add_processor(
        ProcessorSpec(key="init", name="init", type=_UPDATE_ATTRIBUTE, properties={"mime.type": "application/json"})
    )
    builder.link(entry_key, "init", ["success"] if entry_key == "trigger" else [])
    source: Tail = ("init", "success")

    if service.config.get("authMode") == "session_token":
        source = _build_session_login(builder, service=service, add_param=add_param, source_key="init")

    fields = _el_field_refs(body_template)
    if fields:
        source = _extract_fields(builder, key="extract_body_fields", fields=fields, source=source)

    builder.add_processor(
        ProcessorSpec(key="render_body", name="render_body", type="org.apache.nifi.processors.standard.ReplaceText",
                      properties={"Replacement Strategy": "Always Replace", "Replacement Value": body_template,
                                  "Evaluation Mode": "Entire text", "Character Set": "UTF-8"})
    )
    builder.link(source[0], "render_body", [source[1]])
    builder.to_dlq("render_body", "failure")

    base_expr = _base_url_expr(block=block, service=service, ctx=ctx, add_param=add_param)
    invoke_props: Dict[str, Any] = {**_INVOKE_HTTP_BASELINE, "HTTP Method": method, "HTTP URL": f"{base_expr}{path}",
                                     "Request Body Enabled": "true"}
    if service.config.get("authMode") == "session_token":
        header = str(service.config.get("tokenHeader", "Authorization")) or "Authorization"
        invoke_props[header] = _session_header_value(service)
    else:
        _apply_auth(builder, service=service, props=invoke_props, add_param=add_param)

    unused_relationship = "Response" if write_forwards != "response" else "Original"
    builder.add_processor(
        ProcessorSpec(key="write", name="write", type="org.apache.nifi.processors.standard.InvokeHTTP",
                      properties=invoke_props, autoTerminate=["No Retry", "Retry", unused_relationship])
    )
    builder.link("render_body", "write", ["success"])
    builder.to_dlq("write", "Failure")

    if write_forwards == "response":
        record_path = str(block.config.get("recordPath", "$"))
        split = bool(block.config.get("split", True))
        response_format = str(block.config.get("responseFormat", "json"))
        record_tail, _original_tail = _parse_response(
            builder, source=("write", "Response"), response_format=response_format, split=split,
            record_path=record_path, ptype="none",
        )
        return record_tail
    return "write", "Original"


def _compile_lookup(
    builder: "BlockBuilder", *, flow: "Flow", block: FlowBlock, ctx: "CompileContext", flow_token: str,
    is_root: bool, add_param,
) -> Tail:
    """compiler-spec §3.1 item 8: `InvokeHTTP` GET (path params interpolated
    from the parent record's own fields, same `${field}` -> attribute
    promotion `_compile_write` uses for its body template) -> `EvaluateJsonPath`
    (the whole response body, promoted to one `lookup_value` attribute) ->
    `UpdateRecord` writing `/<joinField>_lookup = ${lookup_value}`.

    HONEST LIMITATION (flagged for live E2E / a follow-up design pass): this
    chain continues on the *lookup response's own* flowfile lineage, not a
    genuine two-lineage merge back onto the calling parent record. NiFi has
    no join primitive in scope here (`MergeContent`/`Wait`+`Notify` are
    neither mentioned in the task brief nor implemented) — a single flowfile
    cannot simultaneously carry "the original record" AND "the response" as
    two independent contents to merge. "Merging under the configured join
    field" is therefore implemented as reshaping the RESPONSE itself under
    `/<joinField>_lookup`, which is the simplest reading of the brief's
    literal 3-step description (InvokeHTTP -> EvaluateJsonPath -> UpdateRecord)
    that NiFi can actually execute without additional processors the task
    didn't ask for.
    """
    if is_root:
        raise CompileError(f"http lookup block {block.id!r} cannot be a flow root")
    service = _service_for(block, ctx)
    path = str(block.config.get("path", ""))
    join_field = str(block.config.get("lookupJoinField") or "id")

    source: Tail = ("inputPort", "")
    if service.config.get("authMode") == "session_token":
        source = _build_session_login(builder, service=service, add_param=add_param, source_key="inputPort")

    fields = _el_field_refs(path)
    if fields:
        source = _extract_fields(builder, key="extract_path_fields", fields=fields, source=source)

    base_expr = _base_url_expr(block=block, service=service, ctx=ctx, add_param=add_param)
    invoke_props: Dict[str, Any] = {**_INVOKE_HTTP_BASELINE, "HTTP Method": "GET", "HTTP URL": f"{base_expr}{path}"}
    if service.config.get("authMode") == "session_token":
        header = str(service.config.get("tokenHeader", "Authorization")) or "Authorization"
        invoke_props[header] = _session_header_value(service)
    else:
        _apply_auth(builder, service=service, props=invoke_props, add_param=add_param)

    builder.add_processor(
        ProcessorSpec(key="lookup_fetch", name="lookup_fetch", type="org.apache.nifi.processors.standard.InvokeHTTP",
                      properties=invoke_props, autoTerminate=["No Retry", "Retry", "Original"])
    )
    builder.link(source[0], "lookup_fetch", [source[1]] if source[1] else [])
    builder.to_dlq("lookup_fetch", "Failure")

    builder.add_processor(
        ProcessorSpec(key="lookup_extract", name="lookup_extract", type=_EVALUATE_JSON_PATH,
                      properties={"Destination": "flowfile-attribute", "Return Type": "json",
                                  "Path Not Found Behavior": "ignore", "lookup_value": "$"},
                      autoTerminate=["unmatched"])
    )
    builder.link("lookup_fetch", "lookup_extract", ["Response"])
    builder.to_dlq("lookup_extract", "failure")

    reader_key, writer_key = ensure_json_record_services(builder)
    builder.add_processor(
        ProcessorSpec(key="lookup_merge", name="lookup_merge", type="org.apache.nifi.processors.standard.UpdateRecord",
                      properties={"Record Reader": reader_key, "Record Writer": writer_key,
                                  "Replacement Value Strategy": "literal-value",
                                  f"/{join_field}_lookup": "${lookup_value}"})
    )
    builder.link("lookup_extract", "lookup_merge", ["matched"])
    builder.to_dlq("lookup_merge", "failure")
    return "lookup_merge", "success"
