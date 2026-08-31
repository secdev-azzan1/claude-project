// Typed client for the v2 OpenAPI documentation endpoints
// (backend/routers/v2/openapi.py) — upload/parse a spec, then browse it.
//
// This is a deliberate duplicate of api.ts's tiny fetch-wrapper conventions
// (ApiRequestError shape, FastAPI `detail` normalization) rather than an
// import from it: api.ts is owned by another agent right now, and this
// client's needs (multipart upload, query-string GETs) are narrow enough
// that sharing the module isn't worth the coupling.

const BASE = ((import.meta.env.VITE_BACKEND_URL as string | undefined) ?? "").replace(/\/+$/, "");

export class OpenApiRequestError extends Error {
  status: number;
  constructor(message: string, status: number) {
    super(message);
    this.name = "OpenApiRequestError";
    this.status = status;
  }
}

/** FastAPI convention: `{detail: "..."}`, or the stock pydantic validation
 *  shape `{detail: [{loc, msg, type}, ...]}`. */
function normalizeDetail(detail: unknown): string {
  if (typeof detail === "string") return detail;
  if (Array.isArray(detail)) {
    const first = detail[0];
    if (first && typeof first === "object") {
      const rec = first as Record<string, unknown>;
      if (typeof rec.msg === "string") {
        const loc = Array.isArray(rec.loc) ? rec.loc.filter((p) => p !== "body").join(".") : "";
        return loc ? `${loc}: ${rec.msg}` : rec.msg;
      }
    }
    try {
      return detail.map(String).join("; ");
    } catch {
      return JSON.stringify(detail);
    }
  }
  if (detail && typeof detail === "object") {
    const rec = detail as Record<string, unknown>;
    if (typeof rec.message === "string") return rec.message;
    try {
      return JSON.stringify(detail);
    } catch {
      return String(detail);
    }
  }
  return String(detail);
}

interface RequestOpts {
  method?: string;
  form?: FormData;
  query?: Record<string, string | number | undefined>;
}

/** Small fetch wrapper: query strings, multipart form, and FastAPI detail
 *  extraction. Mirrors src/prototype/api.ts's `request` helper. */
async function request<T>(path: string, opts: RequestOpts = {}): Promise<T> {
  const { method = "GET", form, query } = opts;
  const qs = query
    ? Object.entries(query)
        .filter(([, v]) => v !== undefined && v !== "")
        .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`)
        .join("&")
    : "";
  const url = `${BASE}${path}${qs ? `?${qs}` : ""}`;
  const init: RequestInit = { method };
  if (form) init.body = form;

  let res: Response;
  try {
    res = await fetch(url, init);
  } catch (err) {
    throw new OpenApiRequestError(
      `Could not reach the backend at ${url}: ${err instanceof Error ? err.message : String(err)}`,
      0,
    );
  }

  if (res.status === 204) return undefined as T;

  const text = await res.text();

  if (!res.ok) {
    let message = text || res.statusText || `Request failed with status ${res.status}`;
    try {
      const json = JSON.parse(text);
      message = normalizeDetail(json.detail ?? json.message ?? text);
    } catch {
      // non-JSON error body — keep the raw text
    }
    throw new OpenApiRequestError(message, res.status);
  }

  if (!text) return undefined as T;

  try {
    return JSON.parse(text) as T;
  } catch {
    throw new OpenApiRequestError(`Invalid JSON response from ${url}`, 502);
  }
}

// ------------------------------------------------------------------- shapes

export interface OpenApiServer {
  url: string;
  description: string;
}

export interface OpenApiSpecSummary {
  specId: string;
  title: string;
  version: string;
  format: string;
  openapiVersion: string;
  operationsCount: number;
  servers: OpenApiServer[];
  warnings: string[];
}

export interface OpenApiParameterSummary {
  name: string;
  location: string | null;
  required: boolean;
  schemaType: string | null;
  default: unknown;
}

export interface OpenApiOperationSummary {
  operationId: string;
  method: string;
  /** The path template as written in the document, e.g. `/scans/{scan_id}`. */
  path: string;
  summary: string;
  tags: string[];
  parameters: OpenApiParameterSummary[];
}

export interface OpenApiOperationDetail extends OpenApiOperationSummary {
  description: string;
  deprecated: boolean;
  /** Same path with `{param}` rewritten to `${param}` — this platform's runtime template syntax. */
  pathRuntime: string;
  security: unknown[];
  requestContentTypes: string[];
}

export interface OpenApiOperationsPage {
  items: OpenApiOperationSummary[];
  total: number;
  page: number;
  pageSize: number;
}

// ---------------------------------------------------------------- endpoints

/** POST /api/v2/openapi/parse — upload and parse an OpenAPI JSON document. */
export async function parseOpenApiDocument(file: File): Promise<OpenApiSpecSummary> {
  const form = new FormData();
  form.append("file", file);
  return request<OpenApiSpecSummary>("/api/v2/openapi/parse", { method: "POST", form });
}

/** GET /api/v2/openapi/{specId} — the same summary shape parse returns. */
export async function getOpenApiSpec(specId: string): Promise<OpenApiSpecSummary> {
  return request<OpenApiSpecSummary>(`/api/v2/openapi/${encodeURIComponent(specId)}`);
}

/** GET /api/v2/openapi/{specId}/operations?search=&page=&pageSize= */
export async function listOpenApiOperations(
  specId: string,
  opts: { search?: string; page?: number; pageSize?: number } = {},
): Promise<OpenApiOperationsPage> {
  return request<OpenApiOperationsPage>(`/api/v2/openapi/${encodeURIComponent(specId)}/operations`, {
    query: { search: opts.search, page: opts.page, pageSize: opts.pageSize },
  });
}

/** GET /api/v2/openapi/{specId}/operations/{operationId} */
export async function getOpenApiOperationDetail(
  specId: string,
  operationId: string,
): Promise<OpenApiOperationDetail> {
  return request<OpenApiOperationDetail>(
    `/api/v2/openapi/${encodeURIComponent(specId)}/operations/${encodeURIComponent(operationId)}`,
  );
}
