type AttributeExtractionLike = {
  attributeName?: string | null;
};

type RouteSourceLike = {
  parentStreamId?: string | null;
};

export type InferencePreflightStream = {
  id: string;
  name?: string | null;
  endpointPath?: string | null;
  requestBodyTemplate?: string | null;
  additionalHeaders?: string | null;
  additionalQueryParams?: string | null;
  parentStreamId?: string | null;
  pathParamDefaults?: Record<string, string | null | undefined> | null;
  attributeExtractions?: AttributeExtractionLike[] | null;
  routeSource?: RouteSourceLike | null;
};

export type InferencePreflightIssue = {
  streamName: string;
  params: string[];
};

const PLACEHOLDER_PATTERN = /\$\{([^}]+)\}/g;

const toAttributeName = (value: string) =>
  value
    .trim()
    .replace(/[^A-Za-z0-9_]+/g, "_")
    .replace(/^_+|_+$/g, "");

const parentStreamIdFor = (stream: InferencePreflightStream) =>
  stream.parentStreamId || stream.routeSource?.parentStreamId || "";

const upstreamAttributesFor = (
  stream: InferencePreflightStream,
  byId: Map<string, InferencePreflightStream>,
) => {
  const attrs = new Set<string>();
  const seen = new Set<string>();
  let parentId = parentStreamIdFor(stream);
  while (parentId && !seen.has(parentId)) {
    seen.add(parentId);
    const parent = byId.get(parentId);
    if (!parent) break;
    for (const rule of parent.attributeExtractions || []) {
      const name = toAttributeName(rule.attributeName || "").toLowerCase();
      if (name) attrs.add(name);
    }
    parentId = parentStreamIdFor(parent);
  }
  return attrs;
};

const collectBranchStreamIds = (
  streams: InferencePreflightStream[],
  selectedEntityStreamId?: string,
) => {
  if (!selectedEntityStreamId) return new Set(streams.map((stream) => stream.id));

  const byId = new Map(streams.map((stream) => [stream.id, stream]));
  const selected = byId.get(selectedEntityStreamId);
  if (!selected) return new Set(streams.map((stream) => stream.id));

  const selectedIds = new Set<string>();
  let cursor: InferencePreflightStream | undefined = selected;
  while (cursor && !selectedIds.has(cursor.id)) {
    selectedIds.add(cursor.id);
    const parentId = parentStreamIdFor(cursor);
    cursor = parentId ? byId.get(parentId) : undefined;
  }
  return selectedIds;
};

export const collectMissingPathParamResolutionsForInference = ({
  sourceType,
  streams,
  selectedEntityStreamId,
}: {
  sourceType: string;
  streams: InferencePreflightStream[];
  selectedEntityStreamId?: string;
}): InferencePreflightIssue[] => {
  if (sourceType !== "rest") return [];
  const byId = new Map(streams.map((stream) => [stream.id, stream]));
  const streamIdsToValidate = collectBranchStreamIds(streams, selectedEntityStreamId);
  const missing: InferencePreflightIssue[] = [];

  for (const stream of streams) {
    if (!streamIdsToValidate.has(stream.id)) continue;

    const vars = new Set<string>();
    const templates = [
      stream.endpointPath || "",
      stream.requestBodyTemplate || "",
      stream.additionalHeaders || "",
      stream.additionalQueryParams || "",
    ];
    for (const template of templates) {
      PLACEHOLDER_PATTERN.lastIndex = 0;
      let match: RegExpExecArray | null;
      while ((match = PLACEHOLDER_PATTERN.exec(template)) !== null) {
        const name = (match[1] ?? "").trim();
        if (name) vars.add(name);
      }
    }
    if (!vars.size) continue;

    const defaults = stream.pathParamDefaults || {};
    const parentId = parentStreamIdFor(stream);
    const parentAttrs = upstreamAttributesFor(stream, byId);
    const unresolved = Array.from(vars).filter((name) => {
      const hasDefault = Boolean((defaults[name] || "").trim());
      if (hasDefault) return false;
      if (!parentId) return true;
      const normalized = toAttributeName(name).toLowerCase();
      return !parentAttrs.has(normalized);
    });
    if (unresolved.length > 0) {
      missing.push({ streamName: stream.name || "stream", params: unresolved.sort() });
    }
  }

  return missing;
};
