"""`http` adapter compilation — compiler-spec.md §3.1.

FULL scope for T7.1: `read` mode only (GET), all six auth modes, proxy
egress base-URL swap, all four pagination styles, JSON response parsing
(split and no-split). `write`/`lookup` modes raise `NotImplementedError` —
they land in a follow-up task (csv/xml/text response parsing likewise).

Design used throughout: `fetch`'s "HTTP URL" property is ALWAYS
`${request.url}` — `init` seeds `request.url` to an EL template containing
the literal (unevaluated) `${offset}`/`${page}`/`${cursor}` placeholders for
the first three pagination styles (NiFi re-evaluates the template fresh each
time the looped flowfile re-enters `fetch`, so `next` only has to update the
underlying counter attribute, not the URL string itself); for `next_url`
pagination, `next` overwrites `request.url` directly with the server-given
absolute URL each iteration, since there is no query template to re-evaluate.

Response parsing runs BEFORE the pagination-continue check (compiler-spec
§3.1 lists parsing at step 5, pagination at step 6): `fetch`'s `Response`
relationship goes to `SplitJson` (when `split: true`) or directly onward
(when `split: false`); `SplitJson`'s `split` relationship feeds the record
chain unconditionally, while its `original` relationship (or, when not
splitting, a second connection off `fetch`'s own `Response` relationship)
feeds `page_meta` — matching the rapid7-securado/sentinelone reference flows'
"decouple pagination-continue from per-record processing" shape (see
`docs/orchestration/analysis/nifi-reference-flows.md` §4.7/§5.7).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

from models.adapter import AppService, FlowBlock
from services.adapter.validation import block_proxy_id
from services.adapter.naming import tokenize

from .ir import CompileError, ControllerServiceSpec, ProcessorSpec
from .transforms import Tail

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


def _apply_auth(
    builder: "BlockBuilder", *, service: AppService, props: Dict[str, Any], add_param
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
            # Query-param api keys are folded into the URL by the caller
            # (see `_build_query`); recorded here too, informationally, so
            # the processor's own properties show where the key went.
            props[f"API Key Query Param (informational): {key_name}"] = f"#{{svc_{sid}_key_value}}"
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
        props[header] = "${session.token}"
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
    if block.mode != "read":
        raise NotImplementedError(
            f"http {block.mode} is not implemented yet (T7.1 scope: http read only) — block {block.id}"
        )
    response_format = block.config.get("responseFormat", "json")
    if response_format != "json":
        raise NotImplementedError(
            f"http response format {response_format!r} is not implemented yet "
            f"(T7.1 scope: json only) — block {block.id}"
        )

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

    init_props: Dict[str, Any] = {"Accept": "application/json", "mime.type": "application/json",
                                   "request.url": initial_url}
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
    invoke_props: Dict[str, Any] = {**_INVOKE_HTTP_BASELINE, "HTTP Method": "GET", "HTTP URL": "${request.url}"}
    if service.config.get("authMode") == "session_token":
        header = str(service.config.get("tokenHeader", "Authorization")) or "Authorization"
        invoke_props[header] = "${session.token}"
    else:
        _apply_auth(builder, service=service, props=invoke_props, add_param=add_param)
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
    if split:
        builder.add_processor(
            ProcessorSpec(key="split", name="split", type="org.apache.nifi.processors.standard.SplitJson",
                          properties={"JsonPath Expression": record_path},
                          autoTerminate=(["original"] if ptype == "none" else []))
        )
        builder.link("fetch", "split", ["Response"])
        builder.to_dlq("split", "failure")
        record_tail: Tail = ("split", "split")
        original_tail: Tail = ("split", "original")
    else:
        record_tail = ("fetch", "Response")
        original_tail = ("fetch", "Response")

    # ---- pagination ----------------------------------------------------------
    if ptype != "none":
        _build_pagination(builder, ptype=ptype, pagination=pagination, record_path=record_path,
                          original_tail=original_tail, loop_target="fetch")

    return record_tail


def _build_trigger(flow: "Flow") -> ProcessorSpec:
    period, strategy = _cron_or_period(flow.cron)
    return ProcessorSpec(
        key="trigger", name="trigger", type="org.apache.nifi.processors.standard.GenerateFlowFile",
        properties={"File Size": "0B", "Batch Size": "1", "Unique FlowFiles": "false", "Custom Text": "{}",
                    "Character Set": "UTF-8", "Data Format": "Text"},
        schedulingPeriod=period, schedulingStrategy=strategy, runOnPrimary=True,
    )


def _cron_or_period(cron: Optional[str]) -> Tuple[str, str]:
    """5-field UTC cron -> NiFi cron (`sec min hour dom mon dow`), per
    compiler-spec §3.1: `0 <min> <hour> <dom> <mon> <dow>`."""
    if not cron:
        return "1 hour", "TIMER_DRIVEN"
    fields = cron.strip().split()
    if len(fields) != 5:
        raise CompileError(f"Cron must be a 5-field expression (UTC), got {cron!r}")
    minute, hour, dom, mon, dow = fields
    return f"0 {minute} {hour} {dom} {mon} {dow}", "CRON_DRIVEN"


def _build_session_login(builder: "BlockBuilder", *, service: AppService, add_param, source_key: str) -> Tail:
    sid = service.id
    login_path = str(service.config.get("loginPath", "/login"))
    base_expr = f"#{{svc_{sid}_base_url}}"
    props: Dict[str, Any] = {**_INVOKE_HTTP_BASELINE, "HTTP Method": "POST",
                              "HTTP URL": f"{base_expr}{login_path}"}
    if service.config.get("username"):
        add_param(f"svc_{sid}_username", str(service.config.get("username", "")), False)
        add_param(f"svc_{sid}_password", service.config.get("password"), True)
        props["Request Username"] = f"#{{svc_{sid}_username}}"
        props["Request Password"] = f"#{{svc_{sid}_password}}"
    builder.add_processor(
        ProcessorSpec(key="login", name="login", type="org.apache.nifi.processors.standard.InvokeHTTP",
                      properties=props, autoTerminate=list(_INVOKE_HTTP_AUTOTERMINATE))
    )
    builder.link(source_key, "login", ["success"])

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
    # Run failures (login/extract_token) are NOT record failures -- no DLQ record.
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
