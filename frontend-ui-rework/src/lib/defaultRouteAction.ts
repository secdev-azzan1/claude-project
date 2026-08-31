export type NormalizedDefaultRouteAction = "DROP" | "ROUTE_TO_STREAM";

/**
 * Unmatched records may only be Dropped or Routed. Legacy data used "KEEP";
 * fold it (and empty/unknown values) into DROP. A present default child stream
 * means the intent was to route, so preserve that as ROUTE_TO_STREAM.
 */
export function normalizeDefaultRouteAction(
  rawAction: string,
  hasChildStream: boolean,
): NormalizedDefaultRouteAction {
  const action = (rawAction || "").toLowerCase();
  if (action === "route_to_stream" || hasChildStream) return "ROUTE_TO_STREAM";
  return "DROP";
}
