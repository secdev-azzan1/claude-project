// Branches: the one vocabulary for how a record leaves a block.
//
// There is no fork/route distinction. A branch is the edge from a parent block
// to one of its children, and it carries a list of rules:
//
//   no rules      → every record takes this branch
//   rules         → only records passing them do (all of them, or any — the
//                   branch's own `match` says which)
//
// Branches are INDEPENDENT of each other. Each one tests every record on its
// own, so a record that satisfies two branches travels down both, and a branch
// with no rules is a full mirror of the stream rather than a leftovers bin.
// There is no ordering BETWEEN branches, no first-match-wins and no default
// action — those belong to a switch statement, and this is not one.
//
// `match` orders nothing: it only says how one branch's own rules combine.
//
// A record matching nothing simply stops: that is a counted outcome, not an
// error, and the fix is a branch with no rules if a catch-all is wanted.

import type { BranchCondition, BranchInfo, BranchMatch, BranchOp, Flow, FlowBlock } from "./types";

export const BRANCH_OPS: { value: BranchOp; label: string; needsValue: boolean }[] = [
  { value: "equals", label: "equals", needsValue: true },
  { value: "not_equals", label: "does not equal", needsValue: true },
  { value: "contains", label: "contains", needsValue: true },
  { value: "starts_with", label: "starts with", needsValue: true },
  { value: "regex", label: "matches regex", needsValue: true },
  { value: "is_empty", label: "is empty", needsValue: false },
];

export function opNeedsValue(op: BranchOp): boolean {
  return BRANCH_OPS.find((o) => o.value === op)?.needsValue ?? true;
}

/** Children of a block, in insertion order — each one is a branch off it. */
export function branchesOf(flow: Flow, blockId: string): FlowBlock[] {
  return flow.blocks.filter((b) => b.parentId === blockId);
}

/** The rules on a branch, always as a list. */
export function rulesOf(branch: BranchInfo | undefined | null): BranchCondition[] {
  return branch?.rules ?? [];
}

export function matchOf(branch: BranchInfo | undefined | null): BranchMatch {
  return branch?.match === "any" ? "any" : "all";
}

/** Whether a branch filters at all. */
export function isConditional(branch: BranchInfo | undefined | null): boolean {
  return rulesOf(branch).length > 0;
}

/** The next default label, counting the branches that already exist. */
export function defaultBranchName(existing: number): string {
  return `branch-${existing + 1}`;
}

export function emptyRule(): BranchCondition {
  return { field: "", op: "equals", value: "" };
}

/**
 * A rule the user started but did not finish. Worth flagging because a
 * half-written rule matches NOTHING under `all` — which looks identical to
 * "this branch is broken" and nothing else in the UI would say why.
 */
export function ruleIncomplete(rule: BranchCondition): boolean {
  if (!rule.field.trim()) return true;
  return opNeedsValue(rule.op) && !rule.value.trim();
}

export function branchIncomplete(branch: BranchInfo | undefined | null): boolean {
  return rulesOf(branch).some(ruleIncomplete);
}

/** One phrasing of a single rule, used everywhere a rule is shown as text. */
export function describeRule(rule: BranchCondition): string {
  const field = rule.field.trim() || "<field>";
  const label = BRANCH_OPS.find((o) => o.value === rule.op)?.label ?? rule.op;
  if (!opNeedsValue(rule.op)) return `${field} ${label}`;
  const value = rule.value.trim() || "<value>";
  return `${field} ${label} "${value}"`;
}

/**
 * The whole branch as one line — for the map's edge label and the outline.
 * Long rule sets are truncated rather than allowed to swamp the canvas.
 */
export function describeBranch(branch: BranchInfo | undefined | null, maxRules = 2): string {
  const rules = rulesOf(branch);
  if (rules.length === 0) return "all records";
  const joiner = matchOf(branch) === "any" ? " or " : " and ";
  const shown = rules.slice(0, maxRules).map(describeRule).join(joiner);
  return rules.length > maxRules ? `${shown} +${rules.length - maxRules} more` : shown;
}

/**
 * Node ids worth flagging on the map: a branch with a half-written rule, and the
 * block it hangs off. A branch with NO rules is never flagged — that is the
 * catch-all, and treating it as unfinished was the old model's mistake.
 */
export function branchAttentionIds(flow: Flow): Set<string> {
  const ids = new Set<string>();
  for (const block of flow.blocks) {
    if (!branchIncomplete(block.branch)) continue;
    ids.add(block.id);
    if (block.parentId) ids.add(block.parentId);
  }
  return ids;
}

/** Summary for the Routing section header, e.g. "3 branches · 1 unconditional". */
export function branchSummary(flow: Flow, block: FlowBlock): string {
  const children = branchesOf(flow, block.id);
  if (children.length === 0) return "nothing follows yet";
  const conditional = children.filter((c) => isConditional(c.branch)).length;
  const all = children.length - conditional;
  const parts = [`${children.length} branch${children.length === 1 ? "" : "es"}`];
  if (conditional > 0) parts.push(`${conditional} conditional`);
  if (all > 0) parts.push(`${all} unconditional`);
  return parts.join(" · ");
}
