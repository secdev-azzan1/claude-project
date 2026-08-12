// One-way migration of saved flows from the fork/route model to branches.
//
// It runs on load rather than behind a seed bump so nobody's saved flows are
// thrown away for a vocabulary change. Three shapes existed before:
//
//   0. `branch.condition`                   → the same rule, as a one-item list.
//   1. `branch.kind === "fork"`            → a branch with no rules.
//   2. `branch.kind === "route"` + ruleId  → the parent held a `route` transform
//      whose rule fed this child; that rule becomes the branch's condition.
//   3. a `route` transform on the CHILD acting as a filter — a single `forward`
//      rule with `defaultAction: "drop"` (whitelist), or a `drop` rule with
//      `defaultAction: "forward"` (blacklist) — which is exactly "only these
//      records continue down this branch", so it becomes the branch condition
//      too, inverted for the blacklist form.
//
// Anything that cannot be expressed as one condition per branch (several rules
// competing for the same branch, a rule feeding nothing) is dropped and counted,
// because keeping it would mean keeping the ordered-rule engine it needs. The
// count is surfaced once, in the migration notice.

import type { BranchCondition, BranchOp, Flow, FlowBlock, PrototypeState, TransformRule } from "./types";

interface LegacyRule {
  id: string;
  name: string;
  field: string;
  op: BranchOp;
  value: string;
  action: "route" | "drop" | "forward";
}

interface LegacyBranch {
  kind?: "fork" | "route";
  name: string;
  ruleId?: string;
  /** The single-condition shape that briefly preceded the rule list. */
  condition?: BranchCondition | null;
  rules?: BranchCondition[];
  match?: "all" | "any";
}

const isRouteTransform = (t: TransformRule): boolean => (t as { kind: string }).kind === "route";

const rulesOf = (block: FlowBlock): LegacyRule[] =>
  block.transforms
    .filter(isRouteTransform)
    .flatMap((t) => (Array.isArray(t.config?.rules) ? (t.config.rules as LegacyRule[]) : []));

/** The negation of an operator, where one exists — used for blacklist filters. */
function invert(op: BranchOp): BranchOp | null {
  if (op === "equals") return "not_equals";
  if (op === "not_equals") return "equals";
  return null;
}

function conditionFrom(rule: LegacyRule): BranchCondition {
  return { field: rule.field ?? "", op: rule.op ?? "equals", value: rule.value ?? "" };
}

/**
 * A block's own route rules read as a filter on the branch INTO it: "only these
 * records continue". A whitelist (forward + default drop) is the condition as
 * written; a blacklist (drop + default forward) is its negation. Returns the
 * first rule that can be expressed that way, or null when none can.
 */
function filterFromRules(block: FlowBlock, rules: LegacyRule[]): BranchCondition | null {
  if (rules.length === 0) return null;
  const whitelist = block.transforms
    .filter(isRouteTransform)
    .some((t) => t.config?.defaultAction === "drop");

  for (const rule of rules) {
    if (whitelist && rule.action === "forward") return conditionFrom(rule);
    if (!whitelist && rule.action === "drop") {
      const op = invert(rule.op);
      if (op) return { ...conditionFrom(rule), op };
    }
  }
  return null;
}

function migrateFlow(flow: Flow): { changed: boolean; lost: number } {
  let changed = false;
  let lost = 0;

  // Index every rule in the flow BEFORE anything is stripped. The rewrite below
  // removes route transforms as it goes, and a block's rule is looked up from
  // its child — which is visited later, by which time the parent's transforms
  // would already be gone.
  const ruleIndex = new Map<string, LegacyRule>();
  for (const b of flow.blocks) for (const r of rulesOf(b)) ruleIndex.set(r.id, r);

  for (const block of flow.blocks) {
    const legacy = block.branch as LegacyBranch | undefined;

    // 1 + 2 — the branch's own shape, and the single-condition shape that came
    // between it and the rule list.
    if (legacy && (legacy.kind || legacy.ruleId || legacy.condition)) {
      changed = true;
      let condition: BranchCondition | null = legacy.condition ?? null;
      if (legacy.ruleId) {
        const rule = ruleIndex.get(legacy.ruleId);
        if (rule) condition = conditionFrom(rule);
      }
      block.branch = { name: legacy.name, ...(condition ? { rules: [condition] } : {}) };
    }

    // 3 — a filter the child carried itself, or rules that fed other branches.
    if (block.transforms.some(isRouteTransform)) {
      changed = true;
      const fedBranches = new Set(
        flow.blocks.map((b) => (b.branch as LegacyBranch | undefined)?.ruleId).filter((id): id is string => !!id),
      );
      // A rule that fed a branch has already moved onto it. Of what is left,
      // at most one can become this block's own branch condition; the rest
      // needed the ordered engine and are counted as lost.
      const strays = rulesOf(block).filter((r) => !fedBranches.has(r.id));
      const condition = filterFromRules(block, strays);
      lost += strays.length - (condition ? 1 : 0);
      if (condition && block.parentId) {
        const existing = block.branch as LegacyBranch | undefined;
        // A branch condition inherited from the parent's rule wins: it is the
        // one the user actually saw on the branch.
        if (!existing?.rules?.length && !existing?.condition) {
          const siblings = flow.blocks.filter((b) => b.parentId === block.parentId);
          const name = existing?.name ?? `branch-${siblings.indexOf(block) + 1}`;
          block.branch = { name, rules: [condition] };
        }
      }
      block.transforms = block.transforms.filter((t) => !isRouteTransform(t));
    }
  }

  return { changed, lost };
}

/**
 * Migrate every flow in place. Returns a one-time notice when anything could not
 * be carried over, so a silent loss is impossible.
 */
export function migrateBranches(state: PrototypeState): string | null {
  if (!Array.isArray(state.flows)) return null;
  let changed = false;
  let lost = 0;
  for (const flow of state.flows) {
    const result = migrateFlow(flow);
    changed = changed || result.changed;
    lost += result.lost;
  }
  if (!changed) return null;
  return lost > 0
    ? `Branching was simplified: forks and routing rules are now one thing — a branch with an optional condition. ` +
        `${lost} rule(s) needed the old ordered rule engine (several rules competing for one branch) and could not be carried over — ` +
        `re-state them as branch conditions.`
    : null;
}
