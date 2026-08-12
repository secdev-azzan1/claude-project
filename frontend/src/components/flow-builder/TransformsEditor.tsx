// The shared Generic-transformations section — identical inside every hosting
// block. Ordered rule list; dedup pinned last; routing rules create named
// branches through the same legality-filtered add menu.

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { AddBlockMenu } from "./AddBlockMenu";
import { computeAddMenu, type AddMenuEntry } from "@/prototype/legality";
import { uid } from "@/prototype/store";
import type { Flow, FlowBlock, TransformKind, TransformRule } from "@/prototype/types";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { ArrowDown, ArrowUp, Fingerprint, GitBranch, Plus, Trash2 } from "lucide-react";

// Routing is NOT a transform any more. A condition belongs to the branch it
// filters — it is edited in Routing, on the branch itself — so this list is
// purely record shaping. Leaving a "route / filter" entry here would have been a
// second, ordered way to express the same thing.
const KIND_LABEL: Record<TransformKind, string> = {
  extract: "Extract / project",
  add_field: "Add field",
  remove_field: "Remove field",
  set_from_attribute: "Set from attribute",
  rename: "Rename",
  coerce: "Coerce",
  dedup: "Dedup",
};

const KIND_HINT: Partial<Record<TransformKind, string>> = {};

const ADDABLE: TransformKind[] = ["extract", "add_field", "remove_field", "set_from_attribute", "rename", "coerce", "dedup"];

function defaultConfig(kind: TransformKind): Record<string, unknown> {
  switch (kind) {
    case "extract":
      return { attribute: "", path: "$.", default: "" };
    case "add_field":
      return { field: "", value: "" };
    case "remove_field":
      return { field: "" };
    case "set_from_attribute":
      return { field: "", attribute: "" };
    case "rename":
      return { from: "", to: "" };
    case "coerce":
      return { field: "", type: "string" };
    case "dedup":
      return { identityFields: [], excludedFields: [], windowHours: 24 };
  }
}

export interface TransformsEditorProps {
  flow: Flow;
  block: FlowBlock;
  locked: boolean;
  onChange: (transforms: TransformRule[]) => void;
  /** Opens Routing — where a record's DESTINATION is decided, not its shape. */
  onGoToBranches?: () => void;
}

export function TransformsEditor({ flow, block, locked, onChange, onGoToBranches }: TransformsEditorProps) {
  const rules = block.transforms;
  const hasDedup = rules.some((r) => r.kind === "dedup");

  const setRule = (id: string, patch: Partial<TransformRule>) =>
    onChange(rules.map((r) => (r.id === id ? { ...r, ...patch, config: { ...r.config, ...(patch.config ?? {}) } } : r)));

  const addRule = (kind: TransformKind) => {
    const rule: TransformRule = { id: uid("t"), kind, config: defaultConfig(kind) };
    // dedup is always last; other rules insert before an existing dedup
    const dedupIdx = rules.findIndex((r) => r.kind === "dedup");
    if (kind === "dedup" || dedupIdx === -1) onChange([...rules, rule]);
    else onChange([...rules.slice(0, dedupIdx), rule, ...rules.slice(dedupIdx)]);
  };

  const move = (idx: number, dir: -1 | 1) => {
    const target = idx + dir;
    if (target < 0 || target >= rules.length) return;
    if (rules[idx].kind === "dedup" || rules[target].kind === "dedup") return;
    const next = [...rules];
    [next[idx], next[target]] = [next[target], next[idx]];
    onChange(next);
  };

  const remove = (id: string) => onChange(rules.filter((r) => r.id !== id));

  const strInput = (rule: TransformRule, key: string, placeholder: string, className = "w-40") => (
    <Input
      className={`h-7 text-xs ${className}`}
      value={(rule.config[key] as string) ?? ""}
      placeholder={placeholder}
      disabled={locked}
      onChange={(e) => setRule(rule.id, { config: { [key]: e.target.value } })}
    />
  );

  return (
    <div className="space-y-2">
      {rules.length === 0 && (
        <p className="rounded-md border border-dashed px-3 py-2 text-xs text-muted-foreground">
          No transformations — records pass through shaped only by the adapter's parsing.
        </p>
      )}

      {rules.map((rule, idx) => (
        <div key={rule.id} className="rounded-md border bg-muted/30 px-3 py-2">
          <div className="flex items-center gap-2">
            {rule.kind === "dedup" && <Fingerprint className="h-3.5 w-3.5 text-muted-foreground" />}
            <span className="text-xs font-semibold">{KIND_LABEL[rule.kind]}</span>
            {rule.kind === "dedup" && (
              <Badge variant="outline" className="text-xs">
                always last
              </Badge>
            )}
            <span className="ml-auto flex items-center gap-0.5">
              <Button size="icon" variant="ghost" className="h-6 w-6" disabled={locked || idx === 0 || rule.kind === "dedup"} onClick={() => move(idx, -1)} title="Move up">
                <ArrowUp className="h-3 w-3" />
              </Button>
              <Button
                size="icon"
                variant="ghost"
                className="h-6 w-6"
                disabled={locked || idx === rules.length - 1 || rule.kind === "dedup" || rules[idx + 1]?.kind === "dedup"}
                onClick={() => move(idx, 1)}
                title="Move down"
              >
                <ArrowDown className="h-3 w-3" />
              </Button>
              <Button size="icon" variant="ghost" className="h-6 w-6 text-destructive" disabled={locked} onClick={() => remove(rule.id)} title="Remove rule">
                <Trash2 className="h-3 w-3" />
              </Button>
            </span>
          </div>

          <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
            {rule.kind === "extract" && (
              <>
                {strInput(rule, "attribute", "attribute name")}
                {strInput(rule, "path", "$.path.to.value", "w-52 font-mono")}
                {strInput(rule, "default", "default (optional)", "w-32")}
              </>
            )}
            {rule.kind === "add_field" && (
              <>
                {strInput(rule, "field", "field")}
                {strInput(rule, "value", "value")}
              </>
            )}
            {rule.kind === "remove_field" && strInput(rule, "field", "field")}
            {rule.kind === "set_from_attribute" && (
              <>
                {strInput(rule, "field", "field")}
                {strInput(rule, "attribute", "attribute")}
              </>
            )}
            {rule.kind === "rename" && (
              <>
                {strInput(rule, "from", "from")}
                {strInput(rule, "to", "to")}
              </>
            )}
            {rule.kind === "coerce" && (
              <>
                {strInput(rule, "field", "field")}
                <Select
                  value={(rule.config.type as string) ?? "string"}
                  disabled={locked}
                  onValueChange={(v) => setRule(rule.id, { config: { type: v } })}
                >
                  <SelectTrigger className="h-7 w-32 text-xs">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {["string", "long", "double", "boolean", "timestamp"].map((t) => (
                      <SelectItem key={t} value={t}>
                        {t}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </>
            )}
            {rule.kind === "dedup" && <DedupFields rule={rule} locked={locked} setRule={setRule} />}
          </div>
        </div>
      ))}

      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button variant="outline" size="sm" className="h-7 gap-1 text-xs" disabled={locked}>
            <Plus className="h-3 w-3" /> Add transformation
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="start">
          {ADDABLE.map((kind) => (
            <DropdownMenuItem
              key={kind}
              disabled={kind === "dedup" && hasDedup}
              onClick={() => addRule(kind)}
              className={KIND_HINT[kind] ? "flex-col items-start gap-0.5 py-2" : undefined}
            >
              <span>
                {KIND_LABEL[kind]}
                {kind === "dedup" && hasDedup ? " (already present)" : ""}
              </span>
              {KIND_HINT[kind] && <span className="text-xs text-muted-foreground">{KIND_HINT[kind]}</span>}
            </DropdownMenuItem>
          ))}
        </DropdownMenuContent>
      </DropdownMenu>
      <p className="text-xs leading-4 text-muted-foreground">
        Applied in order, after the adapter's parsing. Dropped records are intentional outcomes — counted, never errors.
      </p>
    </div>
  );
}

function DedupFields({
  rule,
  locked,
  setRule,
}: {
  rule: TransformRule;
  locked: boolean;
  setRule: (id: string, patch: Partial<TransformRule>) => void;
}) {
  const identity = ((rule.config.identityFields as string[]) ?? []).join(", ");
  const excluded = ((rule.config.excludedFields as string[]) ?? []).join(", ");
  return (
    <div className="w-full space-y-1.5">
      <div className="flex flex-wrap items-center gap-1.5">
        <Input
          className="h-7 w-64 text-xs"
          value={identity}
          placeholder="identity fields (comma-separated)"
          disabled={locked}
          onChange={(e) => setRule(rule.id, { config: { identityFields: e.target.value.split(",").map((s) => s.trim()).filter(Boolean) } })}
        />
        <Input
          className="h-7 w-56 text-xs"
          value={excluded}
          placeholder="excluded fields"
          disabled={locked}
          onChange={(e) => setRule(rule.id, { config: { excludedFields: e.target.value.split(",").map((s) => s.trim()).filter(Boolean) } })}
        />
        <Input
          className="h-7 w-24 text-xs"
          type="number"
          min={1}
          max={8760}
          value={(rule.config.windowHours as number) ?? 24}
          disabled={locked}
          onChange={(e) => setRule(rule.id, { config: { windowHours: Number(e.target.value) } })}
        />
        <span className="text-xs text-muted-foreground">hours</span>
      </div>
      <p className="text-xs leading-4 text-muted-foreground">
        Duplicate suppression, not a delivery guarantee. SHA-256 fingerprints in Redis, one cache per stream — if Redis is down,
        records fail rather than sneak through. A record missing an identity field goes to the DLQ.
      </p>
    </div>
  );
}
