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

import json
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


def _normalize_path(path: str, service: AppService) -> str:
    """Join-safety for the `{baseUrl}{path}` concatenation (user-reported live
    failure: a full URL — or a missing leading slash — produced an invalid
    host like `dummyjson.comhttps`/`dummyjson.comusers`).

    - full URL matching the service's own base -> stripped to its path part
    - foreign full URL -> CompileError (save/deploy validation also rejects it)
    - missing leading '/' (and not a `${...}` template start) -> '/' prepended
    """
    p = (path or "").strip()
    base = str(service.config.get("baseUrl") or "").strip().rstrip("/")
    if p.lower().startswith(("http://", "https://")):
        if base and p.lower().startswith(base.lower()):
            p = p[len(base):] or "/"
        else:
            raise CompileError(
                f"http path must be a path, not a full URL (the service provides the base URL): {p!r}"
            )
    if p and not p.startswith("/") and not p.startswith("${"):
        p = "/" + p
    return p


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
        if "sizeValue" in fields or "sizeParam" in fields:
            size_param = fields.get("sizeParam", "limit")
            parts.append(f"{size_param}=${{page_size}}")
    if key_value_query_param:
        name, param_ref = key_value_query_param
        parts.append(f"{name}={param_ref}")
    return "&".join(parts)


def _pagination_init_props(ptype: str, pagination: Dict[str, Any]) -> Dict[str, Any]:
    """Seed the pagination counter attributes `init` sets on the entry
    flowfile. Shared by read mode (counters land in `fetch`'s URL template)
    and write mode's pagination extension (same counters, but land in
    `render_body`'s body template instead) — the counters themselves
    (`offset`/`limit`, `page`/`page_size`, `cursor`, `page_count`) are
    mode-agnostic; only *where* the resulting EL is embedded differs."""
    fields = pagination.get("fields", {}) or {}
    props: Dict[str, Any] = {}
    if ptype == "offset":
        props["offset"] = "0"
        props["limit"] = str(fields.get("limitValue", "100"))
        props["page_count"] = "0"
    elif ptype == "page":
        props["page"] = str(fields.get("firstPage", "1"))
        props["page_size"] = str(fields.get("sizeValue", "100"))
        props["page_count"] = "0"
    elif ptype == "cursor":
        props["cursor"] = ""
        props["page_count"] = "0"
        if "sizeValue" in fields or "sizeParam" in fields:
            props["page_size"] = str(fields.get("sizeValue", "100"))
    elif ptype == "next_url":
        props["page_count"] = "0"
    return props


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


_COLUMNAR_NAME_BAD_CHARS = re.compile(r"[^A-Za-z0-9_]")


def _sanitize_columnar_field_name(name: str, index: int) -> str:
    """Column names come from a caller-supplied list, not schema
    introspection (FortiSIEM's column-oriented rows carry no field names of
    their own at runtime) — sanitize to `[A-Za-z_][A-Za-z0-9_]*` so the name
    is safe to use as a Jolt output key, an EvaluateJsonPath property name,
    and an Avro field name, matching `tools/build_fortisiem_native_query_cmdb.py`'s
    convention."""
    cleaned = _COLUMNAR_NAME_BAD_CHARS.sub("_", str(name).strip())
    if not cleaned or not re.match(r"[A-Za-z_]", cleaned[0]):
        cleaned = f"col_{index}_{cleaned}" if cleaned else f"col_{index}"
    return cleaned


def _apply_columnar_transform(builder: "BlockBuilder", *, source: Tail, columnar: Dict[str, Any]) -> Tail:
    """Insert a `JoltTransformJSON` that turns a column-oriented HTTP
    response (`{"<rowsField>": [[v0, v1, ...], ...], ...}` — FortiSIEM's
    `/query/cmdb` family, and any other API that returns rows as bare
    positional arrays instead of objects) into a root-level JSON array of
    named objects, using a caller-supplied, pre-known column name list (there
    is no per-record schema to read the names from at runtime — the columns
    ARE the schema). Mirrors the shift-spec pattern already proven live in
    `tools/build_fortisiem_native_query_cmdb.py` (reference-only script, this
    is its declarative/compiler-native equivalent).

    Without this, `SplitJson` would split `$.<rowsField>[*]` into per-row
    FlowFiles whose content is a bare JSON array (`[v0, v1, ...]`) — every
    downstream `EvaluateJsonPath`/field extraction silently finds nothing
    (`Path Not Found Behavior: ignore`), so records flow all the way through
    dedup/publish looking clean while carrying no usable fields. This
    processor is what makes the rows into objects before the split ever
    happens.
    """
    rows_field = str(columnar.get("rowsField") or "data")
    columns = [str(c) for c in (columnar.get("columns") or [])]
    if not columns:
        raise CompileError("http columnar response transform requires at least one column name")
    sanitized = [_sanitize_columnar_field_name(c, i) for i, c in enumerate(columns)]
    shift_inner = {str(i): f"[&1].{name}" for i, name in enumerate(sanitized)}
    jolt_spec = json.dumps({rows_field: {"*": shift_inner}})
    builder.add_processor(
        ProcessorSpec(key="columnar_transform", name="columnar_transform",
                      type="org.apache.nifi.processors.jolt.JoltTransformJSON",
                      properties={"Jolt Transform": "jolt-transform-shift", "Jolt Specification": jolt_spec})
    )
    src_key, src_rel = source
    builder.link(src_key, "columnar_transform", [src_rel])
    builder.to_dlq("columnar_transform", "failure")
    return "columnar_transform", "success"


def _parse_response(
    builder: "BlockBuilder", *, source: Tail, response_format: str, split: bool, record_path: str, ptype: str,
    columnar: Optional[Dict[str, Any]] = None,
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

    `columnar` (optional): a column-oriented response
    (`{"<rowsField>": [[...], ...]}`) is passed through
    `_apply_columnar_transform` BEFORE `SplitJson`, whose element path is
    then forced to `$.[*]` (the transform's root array) instead of
    `record_path`. `original_tail` is deliberately taken from the PRE-Jolt
    `parse_source`, not the post-transform split's own `original`
    relationship — the Jolt shift only keeps `rowsField`, dropping sibling
    keys like a `totalCount` field, so pagination's total-count/probe check
    (`_build_pagination`, fed by `original_tail`) must see the raw response,
    forked off before the transform rather than after it.
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
        if columnar:
            raise CompileError(
                "http columnar response transform requires \"split into records\" to be enabled"
            )
        return parse_source, parse_source

    if columnar:
        transform_source = _apply_columnar_transform(builder, source=parse_source, columnar=columnar)
        split_path = "$.[*]"
        original_tail: Optional[Tail] = parse_source
        split_autoterm = ["original"]
    else:
        transform_source = parse_source
        split_path = record_path
        original_tail = None
        split_autoterm = ["original"] if ptype == "none" else []

    p_key, p_rel = transform_source
    builder.add_processor(
        ProcessorSpec(key="split", name="split", type="org.apache.nifi.processors.standard.SplitJson",
                      properties={"JsonPath Expression": split_path},
                      autoTerminate=split_autoterm)
    )
    builder.link(p_key, "split", [p_rel])
    builder.to_dlq("split", "failure")
    return ("split", "split"), (original_tail or ("split", "original"))


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
    columnar = block.config.get("columnar") or None
    if columnar and not columnar.get("enabled"):
        columnar = None
    path = _normalize_path(str(block.config.get("path", "")), service)

    # ---- trigger / input port -------------------------------------------------
    if is_root:
        builder.add_processor(_build_trigger(flow))
        entry_key = "trigger"
    else:
        entry_key = "inputPort"

    # ---- promote parent-record fields referenced in the path template --------
    # A non-root read block receives one flowfile per parent record (wired in
    # by routing.wire_children); any ${field} in `path` (e.g.
    # "/sites/${id}/assets") must be pulled out of that record's JSON content
    # into a flowfile attribute — exactly what _compile_write's body_template
    # and _compile_lookup's path already do (same _el_field_refs/
    # _extract_fields helpers). This MUST happen before `init`, not merely
    # before `fetch`: per the C4 note below, `init` itself evaluates and
    # freezes the URL into the `request.url` ATTRIBUTE for pagination types
    # without a fetch-side placeholder (none/next_url) — an attribute value
    # is never re-evaluated, so the field must already be a resolvable
    # attribute by the time `init` runs. A root read has no incoming record
    # to extract from, so a field reference there is a config error, not
    # something to silently compile into an empty value.
    path_fields = _el_field_refs(path)
    entry_source: Tail = (entry_key, "success" if entry_key == "trigger" else "")
    if path_fields:
        if is_root:
            raise CompileError(
                f"http read block {block.id!r} is a flow root — its path cannot reference record "
                f"fields ({', '.join(path_fields)}); there is no parent record to extract them from"
            )
        entry_source = _extract_fields(
            builder, key="extract_path_fields", fields=path_fields, source=entry_source,
            preserve_existing=True,
        )

    # ---- init: seed pagination + request.url -----------------------------------
    key_value_query_param = None
    if service.config.get("authMode") == "api_key" and service.config.get("keyLocation") == "query":
        sid = service.id
        key_name = str(service.config.get("keyName", "api_key")) or "api_key"
        key_value_query_param = (key_name, f"#{{svc_{sid}_key_value}}")

    query = _build_query(pagination, key_value_query_param=key_value_query_param)
    base_expr = _base_url_expr(block=block, service=service, ctx=ctx, add_param=add_param)
    # `path` may already embed a literal "?..." query (e.g. a static date-math
    # filter written directly into the block config); pagination's own query
    # params must then join with "&", not a second "?".
    query_sep = "&" if "?" in path else "?"
    initial_url = f"{base_expr}{path}" + (f"{query_sep}{query}" if query else "")

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
    init_props.update(_pagination_init_props(ptype, pagination))

    builder.add_processor(
        ProcessorSpec(key="init", name="init", type="org.apache.nifi.processors.attributes.UpdateAttribute",
                      properties=init_props)
    )
    builder.link(entry_source[0], "init", [entry_source[1]] if entry_source[1] else [])
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
        record_path=record_path, ptype=ptype, columnar=columnar,
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


# R3 (docs/orchestration/e2e/journey-r-reverify.md): the session-token login
# as ONE ExecuteGroovyScript. Credentials/URL/token-path arrive as DYNAMIC
# properties (PropertyValue bindings, read exactly like the dedup hash
# script's SRC/EXCLUDES/IDENTITY_FIELDS in transforms.py); PASSWORD's value
# is a `#{sensitive param}` reference, which the deployer's
# `_sensitive_dynamic_props` (nifi_apply.py) automatically lists in the
# processor's `sensitiveDynamicPropertyNames` — the one NiFi mechanism that
# legally carries a sensitive parameter into a processor property.
# JSON-escaping of the credential values happens in-script via
# `JsonOutput.toJson`, never by string concatenation. TOKEN_PATH is resolved
# as a simple `$.a.b` dot-path (`tokenize('.')` — literal split, no regex).
# NiFi 2.9 runs Java 21, so java.net.http.HttpClient is available.
GROOVY_SESSION_LOGIN_SCRIPT = """import groovy.json.JsonOutput
import groovy.json.JsonSlurper

def flowFile = session.get()
if (!flowFile) return
try {
    def username = (binding.hasVariable('USERNAME') ? (USERNAME.value ?: '') : '')
    def password = (binding.hasVariable('PASSWORD') ? (PASSWORD.value ?: '') : '')
    def loginUrl = (binding.hasVariable('LOGIN_URL') ? (LOGIN_URL.value ?: '') : '')
    def tokenPath = (binding.hasVariable('TOKEN_PATH') ? (TOKEN_PATH.value ?: '') : '')

    def body = JsonOutput.toJson([username: username, password: password])
    def client = java.net.http.HttpClient.newBuilder()
        .connectTimeout(java.time.Duration.ofSeconds(10))
        .build()
    def request = java.net.http.HttpRequest.newBuilder()
        .uri(java.net.URI.create(loginUrl))
        .timeout(java.time.Duration.ofSeconds(10))
        .header('Content-Type', 'application/json')
        .header('Accept', 'application/json')
        .POST(java.net.http.HttpRequest.BodyPublishers.ofString(body, java.nio.charset.StandardCharsets.UTF_8))
        .build()
    def response = client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() < 200 || response.statusCode() >= 300) {
        log.error('session-token login failed: HTTP ' + response.statusCode() + ' from ' + loginUrl)
        session.transfer(flowFile, REL_FAILURE)
        return
    }
    def parsed = new JsonSlurper().parseText(response.body())
    def path = tokenPath.trim()
    if (path.startsWith('$')) { path = path.substring(1) }
    def cur = parsed
    for (seg in path.tokenize('.')) {
        cur = (cur instanceof Map) ? cur.get(seg) : null
        if (cur == null) { break }
    }
    def token = (cur == null) ? '' : cur.toString()
    if (token.isEmpty()) {
        log.error('session-token login: token path ' + tokenPath + ' resolved to nothing in the login response')
        session.transfer(flowFile, REL_FAILURE)
        return
    }
    flowFile = session.putAttribute(flowFile, 'session.token', token)
    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    log.error('session-token login failed: ' + e.message, e)
    session.transfer(flowFile, REL_FAILURE)
}
"""


def _build_session_login(builder: "BlockBuilder", *, service: AppService, add_param, source_key: str) -> Tail:
    """session_token login step (R3 redesign — journey-r-reverify.md): seed
    -> `login` (ONE `ExecuteGroovyScript`, `GROOVY_SESSION_LOGIN_SCRIPT`
    above) -> `session.token` attribute on REL_SUCCESS.

    Why not the previous ReplaceText-JSON-body + InvokeHTTP + EvaluateJsonPath
    chain (E5): PROVEN UNDEPLOYABLE live. NiFi refuses a `#{sensitive param}`
    reference inside a non-sensitive STATIC property — `login_body`'s
    `Replacement Value` validated as
      "cannot reference Parameter 'svc_..._password' because the Sensitivity
       of the parameter does not match the Sensitivity of the property"
    and `sensitiveDynamicPropertyNames` cannot help a static descriptor. The
    only compiled construct that legally receives a sensitive parameter into
    a processor is a SENSITIVE DYNAMIC property — exactly what
    ExecuteGroovyScript's binding-style dynamic properties are (the dedup
    hash script's mechanism, already applied live-verified by
    `nifi_apply._sensitive_dynamic_props`). So the whole login (body build +
    POST + token extraction) collapses into one Groovy step whose PASSWORD
    dynamic property carries the sensitive reference.

    Wire: trigger/seed -> `login` -> (success) -> fetch (the caller links the
    returned tail; fetch keeps the tokenTemplate-injected header value based
    on `${session.token}`). `failure` (non-2xx login, missing token, script
    error) is a RUN failure, not a record failure — routed to
    `run_failure__log`, never DLQ'd, exactly one disposition per relationship
    (the builder-level invariant checker enforces it).
    """
    sid = service.id
    login_path = str(service.config.get("loginPath", "/login"))
    token_path = str(service.config.get("tokenPath", "$.token"))

    add_param(f"svc_{sid}_base_url", str(service.config.get("baseUrl", "")), False)
    add_param(f"svc_{sid}_username", str(service.config.get("username", "")), False)
    add_param(f"svc_{sid}_password", service.config.get("password"), True)

    builder.add_processor(
        ProcessorSpec(key="login", name="login", type="org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
                      properties={
                          "Script Body": GROOVY_SESSION_LOGIN_SCRIPT,
                          "Failure Strategy": "rollback",
                          "LOGIN_URL": f"#{{svc_{sid}_base_url}}{login_path}",
                          "TOKEN_PATH": token_path,
                          "USERNAME": f"#{{svc_{sid}_username}}",
                          "PASSWORD": f"#{{svc_{sid}_password}}",
                      })
    )
    builder.link(source_key, "login", ["success"] if source_key != "inputPort" else [])

    builder.add_processor(
        ProcessorSpec(key="run_failure__log", name="run_failure__log",
                      type="org.apache.nifi.processors.standard.LogAttribute",
                      properties={"Log Level": "error", "Log Payload": "false"}, autoTerminate=["success"])
    )
    # Run failures (login) are NOT record failures -- no DLQ record.
    builder.link("login", "run_failure__log", ["failure"])
    return "login", "success"


_EVALUATE_JSON_PATH = "org.apache.nifi.processors.standard.EvaluateJsonPath"
_UPDATE_ATTRIBUTE = "org.apache.nifi.processors.attributes.UpdateAttribute"


def _build_pagination(
    builder: "BlockBuilder", *, ptype: str, pagination: Dict[str, Any], record_path: str,
    original_tail: Tail, loop_target: str, extra_next_props: Optional[Dict[str, Any]] = None,
) -> None:
    """`extra_next_props`: additional attributes the `next` UpdateAttribute must
    re-set on every loop iteration. Read mode passes nothing. Write mode uses it
    to restore `mime.type` — see `_compile_write`'s call site for why the loop
    would otherwise poison the next request's Content-Type."""
    fields = pagination.get("fields", {}) or {}
    max_pages = fields.get("maxPages")

    if ptype == "offset" or ptype == "page":
        next_props = (
            {"offset": "${offset:toNumber():plus(" + str(fields.get("limitValue", 100)) + ")}"}
            if ptype == "offset" else {"page": "${page:toNumber():plus(1)}"}
        )
        stop_key = "offsetStop" if ptype == "offset" else "stop"
        if fields.get(stop_key, "empty_response") == "total_count":
            # Total-count stop: continue while (pages fetched so far) * page
            # size hasn't reached the API-reported total yet. Reuses the
            # `page_count` counter every pagination style already increments
            # once per loop (`next_props["page_count"]` below) — no new
            # counter attribute needed. A missing total (path/header not
            # found -> attribute left unset by "Path Not Found Behavior:
            # ignore") fails safe via the standard NiFi isEmpty->ifElse
            # default idiom, which never calls :toNumber() on the possibly-
            # empty raw value, so it stops instead of throwing or looping
            # forever.
            prefix = "offsetTotalCount" if ptype == "offset" else "totalCount"
            source = fields.get(f"{prefix}Source", "body")
            page_size = fields.get("limitValue" if ptype == "offset" else "sizeValue", 100)
            if source == "header":
                page_meta_type = _UPDATE_ATTRIBUTE
                header_name = fields.get(f"{prefix}Header", "X-Total-Count")
                page_meta_props = {"total_count": "${invokehttp.response.header." + str(header_name) + "}"}
            else:
                page_meta_type = _EVALUATE_JSON_PATH
                page_meta_props = {"Destination": "flowfile-attribute", "Return Type": "scalar",
                                    "Path Not Found Behavior": "ignore",
                                    "total_count": fields.get(f"{prefix}Path", "$.totalCount")}
            cond = (
                "${total_count:isEmpty():ifElse('0', ${total_count}):toNumber()"
                ":gt(${page_count:toNumber():plus(1):multiply(" + str(page_size) + ")})}"
            )
        else:
            page_meta_type = _EVALUATE_JSON_PATH
            page_meta_props = {"Destination": "flowfile-attribute", "Return Type": "json",
                                "Path Not Found Behavior": "ignore", "probe": _probe_path(record_path)}
            cond = "${probe:isEmpty():not()}"
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
    if extra_next_props:
        next_props.update(extra_next_props)
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


def _extract_fields(
    builder: "BlockBuilder", *, key: str, fields: List[str], source: Tail,
    preserve_existing: bool = False,
) -> Tail:
    """Ensure each referenced parent field is available as a FlowFile
    attribute without clobbering an attribute that is already present.

    Child HTTP reads normally inherit their parent identifiers as attributes
    from the parent branch.  The old implementation always ran one
    multi-property ``EvaluateJsonPath`` and NiFi could replace an existing
    attribute with an empty value when the JSONPath lookup did not resolve on
    that child FlowFile.  The resulting URL became e.g. ``/sites//assets``.

    Each field now has an exclusive ``present``/``missing`` gate.  The
    present path skips JSON extraction; only the missing path evaluates
    ``$.<field>``.  A no-op ``UpdateAttribute`` rejoins the paths so callers
    still receive one ``(processor, relationship)`` tail.
    """
    if not fields:
        return source

    # Write/lookup body/path promotion historically uses one direct
    # EvaluateJsonPath.  The runtime bug being fixed is specifically the
    # chained HTTP-read path, where the parent identifier is already carried
    # as an attribute.  Keep the existing shape for those other callers and
    # opt into the guarded form only for child reads.
    if not preserve_existing:
        props: Dict[str, Any] = {
            "Destination": "flowfile-attribute",
            "Path Not Found Behavior": "ignore",
            "Return Type": "scalar",
        }
        for field in fields:
            props[field] = f"$.{field}"
        builder.add_processor(
            ProcessorSpec(
                key=key, name=key, type=_EVALUATE_JSON_PATH,
                properties=props, autoTerminate=["unmatched"],
            )
        )
        src_key, src_rel = source
        builder.link(src_key, key, [src_rel] if src_rel else [])
        builder.to_dlq(key, "failure")
        return key, "matched"

    working_key, working_rel = source
    for index, field in enumerate(fields):
        gate_key = f"{key}__check_{index}"
        extract_key = key if len(fields) == 1 else f"{key}__extract_{index}"

        builder.add_processor(
            ProcessorSpec(
                key=gate_key,
                name=gate_key,
                type="org.apache.nifi.processors.standard.RouteOnAttribute",
                properties={
                    "Routing Strategy": "Route to Property name",
                    "present": f"${{{field}:isEmpty():not()}}",
                    "missing": f"${{{field}:isEmpty()}}",
                },
                autoTerminate=["unmatched"],
            )
        )
        builder.link(working_key, gate_key, [working_rel] if working_rel else [])
        builder.to_dlq(gate_key, "failure")

        props: Dict[str, Any] = {
            "Destination": "flowfile-attribute",
            "Path Not Found Behavior": "ignore",
            "Return Type": "scalar",
            field: f"$.{field}",
        }
        builder.add_processor(
            ProcessorSpec(
                key=extract_key,
                name=extract_key,
                type=_EVALUATE_JSON_PATH,
                properties=props,
                autoTerminate=["unmatched"],
            )
        )
        builder.link(gate_key, extract_key, ["missing"])
        builder.to_dlq(extract_key, "failure")

        merge_key = f"{key}__merge_{index}"
        builder.add_processor(
            ProcessorSpec(
                key=merge_key,
                name=merge_key,
                type="org.apache.nifi.processors.attributes.UpdateAttribute",
            )
        )
        builder.link(gate_key, merge_key, ["present"])
        builder.link(extract_key, merge_key, ["matched"])
        working_key, working_rel = merge_key, "success"

    return working_key, working_rel


def _auto_fill_pagination_body(body_template: str, *, ptype: str, fields: Dict[str, Any]) -> str:
    """Splice the offset/page pagination key-value pairs onto a write block's
    own JSON body template automatically, so the person building the flow
    never hand-types `${offset}`/`${limit}` (or `${page}`/`${page_size}`)
    tokens into the free-text Body template box — they only fill in the same
    named Offset/Limit/Page-parameter boxes read-mode pagination already
    uses (`PaginationFields.tsx`), exactly like `_build_query` does for read
    mode's URL. Mechanically a text splice, not a real JSON parse: the
    template legitimately contains non-JSON EL tokens (a bare `${offset}`)
    until NiFi evaluates it at runtime, so a json.loads/dumps round-trip
    would corrupt those tokens.
    """
    if ptype == "offset":
        pairs = [
            (str(fields.get("offsetParam", "offset")), "${offset}"),
            (str(fields.get("limitParam", "limit")), "${limit}"),
        ]
    elif ptype == "page":
        pairs = [
            (str(fields.get("pageParam", "page")), "${page}"),
            (str(fields.get("sizeParam", "size")), "${page_size}"),
        ]
    else:
        return body_template

    trimmed = body_template.rstrip()
    if not trimmed.endswith("}"):
        raise CompileError(
            f"http write block body template must be a JSON object ending in '}}' so {ptype!r} "
            "pagination fields can be added to it automatically"
        )

    for key, _ in pairs:
        # Simple, documented text search for an existing quoted key — same
        # "keep it simple" philosophy as _EL_BUILTIN_DENY above, not a real
        # JSON parser.
        if re.search(r'"' + re.escape(key) + r'"\s*:', body_template):
            raise CompileError(
                f"http write block body template already defines a {key!r} field, which collides with "
                f"the {ptype} pagination parameter of the same name — rename the pagination parameter or "
                "the body field"
            )

    head = trimmed[:-1]
    empty_object = not head.rstrip().rstrip("{")
    pairs_text = ", ".join(f'"{key}": {value}' for key, value in pairs)
    separator = "" if empty_object else ", "
    return f"{head}{separator}{pairs_text}}}"


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

    Pagination (new): a write block may set `config.pagination` exactly like
    a read block (offset/page/cursor/next_url, same `fields` shape) for
    POST-with-body list endpoints (compiler-spec gap this closes — see
    `docs/orchestration/e2e` FortiSIEM plan's "8 deferred CMDB entities").
    Only legal when `writeForwards: "response"`: pagination decides whether
    to continue by inspecting the PARSED RESPONSE (same `_build_pagination`
    "does the probed record path have content" / cursor-in-body-or-header
    check read mode already uses), so there must be a response to parse.
    Mechanically this mirrors read mode's `fetch`-URL-template counters
    (`_pagination_init_props`). The counters are emitted in BOTH places: spliced
    into `body_template` by `_auto_fill_pagination_body`, and appended to the
    request URL's query string via read mode's own `_build_query` (see the call
    site for why body-only was a correctness bug, not just an omission). The
    pagination loop-back target is `render_body` (which re-renders the body from
    the just-updated counters), not `write` directly, since `write` itself has
    no per-page state of its own.

    Only `offset` and `page` are supported here; `cursor`/`next_url` are
    rejected up front (see the guard below).
    """
    service = _service_for(block, ctx)
    method = str(block.config.get("method") or "POST").upper()
    if method not in ("POST", "PUT", "PATCH"):
        raise CompileError(f"http write block {block.id!r} has invalid method {method!r} (POST/PUT/PATCH only)")
    path = _normalize_path(str(block.config.get("path", "")), service)
    body_template = str(block.config.get("bodyTemplate", "") or "")
    write_forwards = str(block.config.get("writeForwards", "original") or "original")
    pagination = block.config.get("pagination") or {"type": "none", "fields": {}}
    ptype = pagination.get("type", "none")
    if ptype in ("cursor", "next_url"):
        # Write mode only supports the two counter styles. Both of the other
        # styles COMPILE today but produce a loop that never advances, so the
        # flow re-POSTs page 1 against the source API forever:
        #   - next_url: `next` sets the `request.url` attribute, but a write
        #     block's "HTTP URL" is the concrete `{base}{path}` (only read
        #     mode's `fetch` reads `${request.url}`), so the URL never changes.
        #   - cursor: `_auto_fill_pagination_body` deliberately splices only
        #     offset/page pairs, so `${cursor}` never reaches the body and the
        #     request is byte-identical every iteration.
        # `PaginationFields.tsx` already hides both for write blocks; this makes
        # the compiler agree rather than silently emitting an API-hammering loop
        # for a config that arrives from an import, a seed, or a direct API call.
        raise CompileError(
            f"http write block {block.id!r} configures {ptype!r} pagination, which is not supported for "
            "write mode (a POST body has no URL or cursor token to advance) — use \"offset\" or \"page\""
        )
    if ptype != "none" and write_forwards != "response":
        raise CompileError(
            f"http write block {block.id!r} configures {ptype!r} pagination but writeForwards is "
            f"{write_forwards!r} — pagination needs the parsed response to decide whether to continue, "
            "so writeForwards must be \"response\""
        )
    if ptype in ("offset", "page"):
        body_template = _auto_fill_pagination_body(body_template, ptype=ptype, fields=pagination.get("fields") or {})

    if is_root:
        builder.add_processor(_build_trigger(flow))
        entry_key = "trigger"
    else:
        entry_key = "inputPort"

    init_props: Dict[str, Any] = {"mime.type": "application/json", **_pagination_init_props(ptype, pagination)}
    builder.add_processor(
        ProcessorSpec(key="init", name="init", type=_UPDATE_ATTRIBUTE, properties=init_props)
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
    # Pagination counters ride in the QUERY STRING as well as the body.
    #
    # The original write-mode design put them in the body alone, on the stated
    # reasoning that "write mode has no query string to inject them into". That
    # is wrong: a POST has a URL like any other request, and a large class of
    # POST-paginated list endpoints reads paging off the URL and ignores it in
    # the body entirely. FortiSIEM's `/query/cmdb` is one — verified live:
    # `{"start": 0}` and `{"start": 3}` in the body return byte-identical rows,
    # while `?start=0` and `?start=3` return different pages. Body-only paging
    # against such an API silently re-fetches page 1 forever (with an
    # empty-response stop it never terminates; with a total-count stop it
    # republishes the same page ceil(total/size) times).
    #
    # Emitting both mirrors how the hand-built reference flow wires the same
    # endpoint, and reuses read mode's `_build_query` so the two modes derive
    # identical parameter names from identical config fields. Sending a paging
    # parameter an API does not read is inert; failing to send one it does read
    # is silent data corruption.
    page_query = _build_query(pagination, key_value_query_param=None) if ptype in ("offset", "page") else ""
    query_sep = "&" if "?" in path else "?"
    write_url = f"{base_expr}{path}" + (f"{query_sep}{page_query}" if page_query else "")
    invoke_props: Dict[str, Any] = {**_INVOKE_HTTP_BASELINE, "HTTP Method": method, "HTTP URL": write_url,
                                     "Request Body Enabled": "true"}
    if service.config.get("authMode") == "session_token":
        header = str(service.config.get("tokenHeader", "Authorization")) or "Authorization"
        invoke_props[header] = _session_header_value(service)
    else:
        _apply_auth(builder, service=service, props=invoke_props, add_param=add_param)
    if ptype == "cursor" and (pagination.get("fields") or {}).get("cursorSource") == "header":
        invoke_props["Response Header Request Attributes Enabled"] = "true"
        invoke_props["Response Header Request Attributes Pattern"] = (pagination["fields"].get("cursorHeaderName", "cursor"))

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
        columnar = block.config.get("columnar") or None
        if columnar and not columnar.get("enabled"):
            columnar = None
        record_tail, original_tail = _parse_response(
            builder, source=("write", "Response"), response_format=response_format, split=split,
            record_path=record_path, ptype=ptype, columnar=columnar,
        )
        if ptype != "none":
            # `mime.type` must be re-set on every loop iteration. `init` seeds it
            # once, but the loop path (page_meta -> has_more -> next ->
            # render_body) carries the flowfile that came off `write`'s Response
            # relationship — and InvokeHTTP overwrites `mime.type` on that
            # flowfile with the RESPONSE's Content-Type. Since the baseline sets
            # "Request Content-Type": "${mime.type}", page 2 onward would POST a
            # JSON body advertised as whatever the API replied with (e.g.
            # application/xml for FortiSIEM), which servers reject. ReplaceText
            # cannot set attributes, so the reset belongs on `next`.
            _build_pagination(builder, ptype=ptype, pagination=pagination, record_path=record_path,
                              original_tail=original_tail, loop_target="render_body",
                              extra_next_props={"mime.type": "application/json"})
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
    path = _normalize_path(str(block.config.get("path", "")), service)
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
