// Routing — the block form's view of where its records go.
//
// A block's children ARE its branches. Each card here is one branch: a name, a
// list of rules, and the block it feeds. There is no second kind of branch to
// create and no ordering between branches, because they are independent filters
// over the same stream rather than a switch statement:
//
//   · no rules      → every record takes this branch
//   · rules         → only records passing them do (all of them, or any)
//   · a record can take several branches at once
//   · a record matching none of them stops here, which is a counted outcome
//
// `all / any` combines ONE branch's own rules. It is not precedence: no setting
// here can make one branch win over another, which is what keeps the model flat.
//
// The card only EDITS branches. Creating one is adding a block — the ＋ beside
// the block on the map — so there is one act in one place.

import { AdapterChip } from "@/components/AdapterChip";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import {
  BRANCH_OPS,
  branchesOf,
  emptyRule,
  isConditional,
  matchOf,
  opNeedsValue,
  ruleIncomplete,
  rulesOf,
} from "@/prototype/branches";
import { cn } from "@/lib/utils";
import type { BranchCondition, BranchMatch, BranchOp, Flow, FlowBlock } from "@/prototype/types";
import { AlertCircle, ArrowRight, Filter, Plus, Split, Trash2 } from "lucide-react";

export interface BranchesCardProps {
  flow: Flow;
  block: FlowBlock;
  locked: boolean;
  /** Writes a branch's name, rules and/or match mode through the shared mutation. */
  onSetBranch: (
    blockId: string,
    patch: { name?: string; rules?: BranchCondition[]; match?: BranchMatch },
  ) => void;
  onSelectBlock: (blockId: string) => void;
}

export function BranchesCard({ flow, block, locked, onSetBranch, onSelectBlock }: BranchesCardProps) {
  const children = branchesOf(flow, block.id);

  if (children.length === 0) {
    return (
      <p className="rounded-md border border-dashed px-3 py-2 text-xs text-muted-foreground">
        Nothing follows this block yet. Add the next block with the ＋ beside it — every block you add is a branch, and a
        branch with no rules receives every record.
      </p>
    );
  }

  const unconditional = children.filter((c) => !isConditional(c.branch));
  const conditional = children.filter((c) => isConditional(c.branch));

  const branchCard = (child: FlowBlock, index: number) => {
    const rules = rulesOf(child.branch);
    const match = matchOf(child.branch);
    const label = child.branch?.name ?? (children.length > 1 ? `branch-${index + 1}` : "");
    const broken = rules.some(ruleIncomplete);

    const writeRules = (next: BranchCondition[]) => onSetBranch(child.id, { rules: next });
    const patchRule = (i: number, patch: Partial<BranchCondition>) =>
      writeRules(rules.map((r, j) => (j === i ? { ...r, ...patch } : r)));

    return (
      <div key={child.id} className={cn("rounded-lg border shadow-sm", broken && "border-warning/50")}>
        {/* ---- branch header: name, what it carries, where it goes */}
        <div className="flex flex-wrap items-center gap-2 border-b bg-muted/30 px-2.5 py-2">
          <Input
            className="h-7 w-40 bg-background text-xs"
            value={label}
            placeholder={`branch-${index + 1}`}
            disabled={locked}
            title="Branch names are free text — they pre-fill topic variant tokens in the naming walk."
            onChange={(e) => onSetBranch(child.id, { name: e.target.value })}
          />
          {rules.length > 0 ? (
            <Badge variant="outline" className="gap-1 text-xs">
              <Filter className="h-3 w-3" />
              {rules.length} rule{rules.length === 1 ? "" : "s"}
            </Badge>
          ) : (
            <Badge variant="secondary" className="text-xs">
              all records
            </Badge>
          )}
          <button
            type="button"
            className="ml-auto flex min-w-0 items-center gap-1.5 rounded-md border bg-background px-2 py-1 text-left transition-colors hover:bg-muted"
            onClick={() => onSelectBlock(child.id)}
          >
            <AdapterChip adapter={child.adapter} mode={child.mode} />
            <span className="truncate text-xs font-medium">{child.name}</span>
            <ArrowRight className="h-3 w-3 shrink-0 text-muted-foreground" />
          </button>
        </div>

        {/* ---- the rules themselves */}
        <div className="space-y-2 px-2.5 py-2">
          {rules.length === 0 ? (
            <p className="text-xs text-muted-foreground">
              Every record takes this branch. Add a rule to send only some of them.
            </p>
          ) : (
            <>
              {rules.length > 1 && (
                <div className="flex flex-wrap items-center gap-1.5">
                  <Label className="text-xs font-normal text-muted-foreground">Take a record when</Label>
                  <Select
                    value={match}
                    disabled={locked}
                    onValueChange={(v) => onSetBranch(child.id, { match: v as BranchMatch })}
                  >
                    <SelectTrigger className="h-7 w-44 text-xs">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">all rules match</SelectItem>
                      <SelectItem value="any">any rule matches</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              )}

              {rules.map((rule, i) => (
                <div key={i} className="flex flex-wrap items-center gap-1.5">
                  <span className="w-10 shrink-0 text-xs text-muted-foreground">
                    {i === 0 ? "where" : match === "any" ? "or" : "and"}
                  </span>
                  <Input
                    className="h-7 w-36 font-mono text-xs"
                    value={rule.field}
                    placeholder="field"
                    disabled={locked}
                    onChange={(e) => patchRule(i, { field: e.target.value })}
                  />
                  <Select
                    value={rule.op}
                    disabled={locked}
                    onValueChange={(v) => patchRule(i, { op: v as BranchOp })}
                  >
                    <SelectTrigger className="h-7 w-36 text-xs">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {BRANCH_OPS.map((o) => (
                        <SelectItem key={o.value} value={o.value}>
                          {o.label}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  {opNeedsValue(rule.op) && (
                    <Input
                      className="h-7 w-36 font-mono text-xs"
                      value={rule.value}
                      placeholder="value"
                      disabled={locked}
                      onChange={(e) => patchRule(i, { value: e.target.value })}
                    />
                  )}
                  <Button
                    variant="ghost"
                    size="icon"
                    className="h-7 w-7 text-muted-foreground hover:text-destructive"
                    disabled={locked}
                    title="Remove this rule"
                    onClick={() => writeRules(rules.filter((_, j) => j !== i))}
                  >
                    <Trash2 className="h-3.5 w-3.5" />
                  </Button>
                </div>
              ))}
            </>
          )}

          <div className="flex flex-wrap items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              className="h-7 gap-1 text-xs"
              disabled={locked}
              onClick={() => writeRules([...rules, emptyRule()])}
            >
              <Plus className="h-3 w-3" /> Add rule
            </Button>
            {rules.length > 0 && (
              <Button
                variant="ghost"
                size="sm"
                className="h-7 px-2 text-xs"
                disabled={locked}
                title="Remove every rule — the branch goes back to receiving all records"
                onClick={() => writeRules([])}
              >
                Clear rules
              </Button>
            )}
          </div>

          {broken && (
            <p className="flex items-start gap-1 text-xs text-warning">
              <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
              A rule with no field or value matches nothing, so this branch receives no records until it is finished.
            </p>
          )}
        </div>
      </div>
    );
  };

  return (
    <div className="space-y-3">
      <div className="space-y-2.5">{children.map((child, i) => branchCard(child, i))}</div>

      <div className="space-y-1 rounded-md bg-muted/40 p-2.5 text-xs leading-4 text-muted-foreground">
        <p className="flex items-start gap-1.5">
          <Split className="mt-0.5 h-3.5 w-3.5 shrink-0" />
          <span>
            Branches are independent: every record is tested against each branch on its own, so a record can travel down
            more than one. There is no order and no first-match-wins.
          </span>
        </p>
        {unconditional.length > 0 ? (
          <p>
            {unconditional.length === 1
              ? `"${unconditional[0].branch?.name ?? unconditional[0].name}" has no rules, so it receives every record — including the ones the other branches also take.`
              : `${unconditional.length} branches have no rules, so each receives every record.`}
          </p>
        ) : (
          conditional.length > 0 && (
            <p>
              Every branch here has rules, so a record matching none of them stops at this block — a counted outcome, not
              an error. Add a branch with no rules if you want a catch-all.
            </p>
          )
        )}
        <p>To add another branch, use the ＋ beside this block on the map.</p>
      </div>
    </div>
  );
}
