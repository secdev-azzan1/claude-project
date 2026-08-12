/**
 * Centralized API client for Data Mobility Platform backend.
 * All calls use VITE_BACKEND_URL (or fallback host:8010) and /api prefix.
 */

const explicitBase =
  (import.meta.env.VITE_BACKEND_URL as string) ||
  (import.meta.env.REACT_APP_BACKEND_URL as string) ||
  "";
// Fallback: same origin (Kubernetes ingress routes /api/* to backend port 8001)
const inferredBase =
  typeof window !== "undefined"
    ? `${window.location.origin}`
    : "";
const BASE = (explicitBase || inferredBase).replace(/\/+$/, "");

export const getApiBase = () => BASE;

class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
    this.name = 'ApiError';
  }
}

function normalizeErrorDetail(detail: unknown): string {
  if (typeof detail === "string") return detail;
  if (Array.isArray(detail)) {
    const first = detail[0];
    if (first && typeof first === "object") {
      const rec = first as Record<string, unknown>;
      const msg = typeof rec.msg === "string" ? rec.msg : "";
      const loc = Array.isArray(rec.loc) ? rec.loc.map(String).join(".") : "";
      if (msg && loc) return `${loc}: ${msg}`;
      if (msg) return msg;
    }
    try {
      return JSON.stringify(detail);
    } catch {
      return String(detail);
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

/** ADAPTER UI PROTOTYPE: offline-only. All routed pages read the mock store
 * (src/prototype/api.ts); nothing may hit the network. Any call landing here
 * is a stray legacy import — fail loudly instead of fetching. */
const PROTOTYPE_OFFLINE = true;

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  if (PROTOTYPE_OFFLINE) {
    throw new ApiError(
      503,
      `Prototype is offline-only — "${path}" was requested through the legacy HTTP client. Use src/prototype/api.ts instead.`,
    );
  }
  const url = `${BASE}${path}`;
  const isFormData = typeof FormData !== "undefined" && init?.body instanceof FormData;
  const res = await fetch(url, {
    ...init,
    headers: isFormData
      ? { ...init?.headers }
      : { 'Content-Type': 'application/json', ...init?.headers },
  });

  if (res.status === 204) return null as unknown as T;

  const text = await res.text();
  if (!res.ok) {
    let detail = text;
    try {
      const json = JSON.parse(text);
      detail = normalizeErrorDetail(json.detail ?? json.message ?? text);
    } catch {
      // keep raw text
    }
    throw new ApiError(res.status, detail);
  }

  if (!text) return null as unknown as T;

  // Defensive check: when backend base is wrong, frontend dev server returns HTML (index page),
  // which can otherwise surface later as "x.map is not a function" in page components.
  const trimmed = text.trim().toLowerCase();
  if (trimmed.startsWith("<!doctype html") || trimmed.startsWith("<html")) {
    throw new ApiError(
      502,
      `Unexpected HTML response from API at ${url}. Check VITE_BACKEND_URL and backend availability.`,
    );
  }

  try {
    return JSON.parse(text) as T;
  } catch {
    throw new ApiError(502, `Invalid JSON response from API at ${url}`);
  }
}

export const api = {
  get: <T>(path: string) => request<T>(path, { method: 'GET' }),
  post: <T>(path: string, body?: unknown) =>
    request<T>(path, { method: 'POST', body: body !== undefined ? JSON.stringify(body) : undefined }),
  put: <T>(path: string, body?: unknown) =>
    request<T>(path, { method: 'PUT', body: body !== undefined ? JSON.stringify(body) : undefined }),
  patch: <T>(path: string, body?: unknown) =>
    request<T>(path, { method: 'PATCH', body: body !== undefined ? JSON.stringify(body) : undefined }),
  delete: <T>(path: string) => request<T>(path, { method: 'DELETE' }),
  postForm: <T>(path: string, form: FormData) =>
    request<T>(path, { method: 'POST', body: form }),
};

// ─── Helpers ─────────────────────────────────────────────────────────────────

export function timeAgo(iso: string | null | undefined): string {
  if (!iso) return 'never';
  const diff = Date.now() - new Date(iso).getTime();
  const secs = Math.floor(diff / 1000);
  if (secs < 5) return 'just now';
  if (secs < 60) return `${secs}s ago`;
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  return `${days}d ago`;
}

export { ApiError };
