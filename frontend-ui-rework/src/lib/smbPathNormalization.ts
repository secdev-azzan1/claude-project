export function normalizeSmbClickedPath(pathExpression: string, sanitizedAttributeName: string): string {
  const raw = (pathExpression || "").trim();
  if (!raw) return raw;
  const match = raw.match(/^\$\["(.+)"\]$/);
  if (!match) return raw;
  if (!sanitizedAttributeName.trim()) return raw;
  return `$.${sanitizedAttributeName.trim()}`;
}

