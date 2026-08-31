// The shared Generic-transformations section — identical inside every hosting
// block. Ordered rule list; dedup pinned last; routing rules create named
// branches through the same legality-filtered add menu.

import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { FieldMessage, InfoDot } from "@/components/form/Field";
import { uid } from "@/prototype/store";
import {
  DEDUP_WINDOW_DEFAULT_HOURS,
  dedupIdentityFieldsIssue,
  dedupWindowIssue,
} from "@/prototype/validation";
import type { Flow, FlowBlock, TransformKind, TransformRule } from "@/prototype/types";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { ArrowDown, ArrowUp, Fingerprint, Plus, X } from "lucide-react";

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

// Dedup is not offered here — it has its own always-visible panel below the
// list (see DedupPanel) so it doesn't hide as a terse dropdown item.
const ADDABLE: TransformKind[] = ["extract", "add_field", "remove_field", "set_from_attribute", "rename", "coerce"];

function supportsRetention(kind: TransformKind): boolean {
  return kind === "extract" || kind === "add_field" || kind === "set_from_attribute" || kind === "rename";
}

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
      return { identityFields: [], excludedFields: [], windowHours: DEDUP_WINDOW_DEFAULT_HOURS };
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

export function TransformsEditor({ block, locked, onChange }: TransformsEditorProps) {
  const rules = block.transforms;
  // Dedup is always pinned last (MVP ruling 12) — it never appears in the
  // ordered rule list below; it has its own always-visible panel instead.
  const dedupRule = rules.find((r) => r.kind === "dedup");
  const nonDedupRules = rules.filter((r) => r.kind !== "dedup");

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
      className={`h-8 text-xs ${className}`}
      value={(rule.config[key] as string) ?? ""}
      placeholder={placeholder}
      disabled={locked}
      onChange={(e) => setRule(rule.id, { config: { [key]: e.target.value } })}
    />
  );

  return (
    <div className="space-y-3">
      {nonDedupRules.length === 0 && (
        <p className="rounded-lg bg-muted/40 px-3 py-2.5 text-xs leading-relaxed text-muted-foreground ring-1 ring-inset ring-border/50">
          No transformations — records pass through shaped only by the adapter's parsing.
        </p>
      )}

      {nonDedupRules.length > 0 && (
        <div className="space-y-2">
          {nonDedupRules.map((rule, idx) => (
            // The reorder/remove controls stay hidden until the row is hovered.
            // Three icon buttons on every row turned a list of two rules into a
            // grid of six buttons.
            <div key={rule.id} className="group rounded-lg bg-muted/40 px-3 py-2.5 ring-1 ring-inset ring-border/50">
              <div className="flex items-center gap-2">
                <span className="text-2xs font-semibold uppercase tracking-wider text-muted-foreground">
                  {KIND_LABEL[rule.kind]}
                </span>
                <span className="ml-auto flex items-center gap-0.5 opacity-0 transition-opacity focus-within:opacity-100 group-hover:opacity-100">
                  <Button size="icon-xs" variant="ghost" disabled={locked || idx === 0} onClick={() => move(idx, -1)}>
                    <ArrowUp />
                    <span className="sr-only">Move up</span>
                  </Button>
                  <Button
                    size="icon-xs"
                    variant="ghost"
                    disabled={locked || idx === nonDedupRules.length - 1}
                    onClick={() => move(idx, 1)}
                  >
                    <ArrowDown />
                    <span className="sr-only">Move down</span>
                  </Button>
                  <Button
                    size="icon-xs"
                    variant="ghost"
                    className="text-muted-foreground hover:text-destructive"
                    disabled={locked}
                    onClick={() => remove(rule.id)}
                  >
                    <X />
                    <span className="sr-only">Remove rule</span>
                  </Button>
                </span>
              </div>

              <div className="mt-2 flex flex-wrap items-center gap-2">
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
                      <SelectTrigger className="h-8 w-32 text-xs">
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
                {supportsRetention(rule.kind) && (
                  <div className="ml-auto flex items-center gap-2 text-xs text-muted-foreground">
                    <Label
                      htmlFor={`retention-${rule.id}`}
                      className="cursor-help"
                      title="The key remains available to transforms, deduplication, and routing in this block, then is deleted before output."
                    >
                      Remove after this block
                    </Label>
                    <Switch
                      id={`retention-${rule.id}`}
                      checked={String(rule.config.retention ?? "flow") === "block"}
                      disabled={locked}
                      onCheckedChange={(checked) =>
                        setRule(rule.id, { config: { retention: checked ? "block" : "flow" } })
                      }
                    />
                  </div>
                )}
              </div>
              {supportsRetention(rule.kind) && String(rule.config.retention ?? "flow") === "block" && (
                <p className="mt-2 text-2xs text-muted-foreground">
                  Available through this block, including deduplication and routing; removed before the next block or destination.
                </p>
              )}
            </div>
          ))}
        </div>
      )}

      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button variant="outline" size="xs" disabled={locked}>
            <Plus /> Add transformation
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="start">
          {ADDABLE.map((kind) => (
            <DropdownMenuItem key={kind} onClick={() => addRule(kind)}>
              {KIND_LABEL[kind]}
            </DropdownMenuItem>
          ))}
        </DropdownMenuContent>
      </DropdownMenu>

      <DedupPanel
        rule={dedupRule}
        locked={locked}
        onEnable={() => addRule("dedup")}
        onDisable={() => dedupRule && remove(dedupRule.id)}
        setRule={setRule}
      />
    </div>
  );
}

/**
 * Dedup is a transform like any other (MVP ruling 12 — always pinned last,
 * one per block) but it earns a dedicated, always-visible panel instead of a
 * terse dropdown entry so it's discoverable.
 *
 * The enable/disable action is a Switch rather than a pair of buttons that swap
 * places depending on state — a control that changes identity when you use it is
 * hard to aim at twice.
 */
function DedupPanel({
  rule,
  locked,
  onEnable,
  onDisable,
  setRule,
}: {
  rule: TransformRule | undefined;
  locked: boolean;
  onEnable: () => void;
  onDisable: () => void;
  setRule: (id: string, patch: Partial<TransformRule>) => void;
}) {
  return (
    <div className="rounded-xl bg-muted/40 px-3.5 py-3 ring-1 ring-inset ring-border/60">
      <label className="flex items-center gap-2.5">
        <Switch
          checked={!!rule}
          disabled={locked}
          onCheckedChange={(on) => (on ? onEnable() : onDisable())}
          aria-label="Deduplication"
        />
        <Fingerprint className="h-4 w-4 text-muted-foreground" />
        <span className="text-sm font-medium">Deduplication</span>
        {/* Four consecutive explanatory paragraphs used to live under this
            panel. Three of them were rules that never change; they are here. The
            fourth — the cache-clearing warning — is a consequence of an edit the
            user is making right now, so it stays inline, below. */}
        <InfoDot title="Deduplication">
          Suppresses records already seen within a time window — checked last, after every other transformation.
          The fingerprint excludes exactly the fields you list below; no platform fields are excluded automatically. Suppression is
          best-effort within the window, not a delivery guarantee: SHA-256 fingerprints in Redis, one cache per stream —
          if Redis is down, records fail rather than sneak through. A record missing an identity field goes to the DLQ.
        </InfoDot>
      </label>

      {rule && (
        <div className="mt-3 pl-[3.375rem]">
          <DedupFields rule={rule} locked={locked} setRule={setRule} />
        </div>
      )}
    </div>
  );
}

type WindowUnit = "minutes" | "hours" | "days";

const UNIT_TO_HOURS: Record<WindowUnit, number> = { minutes: 1 / 60, hours: 1, days: 24 };
const UNIT_LABEL: Record<WindowUnit, string> = { minutes: "minutes", hours: "hours", days: "days" };

/** Trim floating-point noise from unit conversion (e.g. 24 hours -> 1 day). */
function roundDisplay(n: number): number {
  return Math.round(n * 1e6) / 1e6;
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
  const [unit, setUnit] = useState<WindowUnit>("hours");
  const identity = ((rule.config.identityFields as string[]) ?? []).join(", ");
  const excluded = ((rule.config.excludedFields as string[]) ?? []).join(", ");
  const windowHours = (rule.config.windowHours as number) ?? DEDUP_WINDOW_DEFAULT_HOURS;
  const displayValue = roundDisplay(windowHours / UNIT_TO_HOURS[unit]);

  const identityIssue = dedupIdentityFieldsIssue(rule.config.identityFields);
  const windowIssue = dedupWindowIssue(rule.config.windowHours);

  return (
    <div className="w-full space-y-3">
      <div className="space-y-1.5">
        <Label className="text-xs text-muted-foreground">Fingerprint</Label>
        <div className="flex flex-wrap items-center gap-2">
          <Input
            className="h-8 w-64 text-xs"
            value={identity}
            placeholder="identity fields (comma-separated)"
            disabled={locked}
            onChange={(e) => setRule(rule.id, { config: { identityFields: e.target.value.split(",").map((s) => s.trim()).filter(Boolean) } })}
          />
          <Input
            className="h-8 w-56 text-xs"
            value={excluded}
            placeholder="excluded fields (optional)"
            disabled={locked}
            onChange={(e) => setRule(rule.id, { config: { excludedFields: e.target.value.split(",").map((s) => s.trim()).filter(Boolean) } })}
          />
        </div>
        {identityIssue && <FieldMessage>{identityIssue}</FieldMessage>}
      </div>

      <div className="space-y-1.5">
        <Label className="flex items-center gap-1.5 text-xs text-muted-foreground">
          Window
          <Tooltip>
            <TooltipTrigger asChild>
              <span className="cursor-help text-muted-foreground/70">(1 minute – 365 days)</span>
            </TooltipTrigger>
            <TooltipContent>Records seen within this window are suppressed.</TooltipContent>
          </Tooltip>
        </Label>
        <div className="flex flex-wrap items-center gap-2">
          <Input
            className="h-8 w-20 text-xs"
            type="number"
            step="any"
            value={displayValue}
            disabled={locked}
            onChange={(e) => {
              const n = Number(e.target.value);
              if (Number.isNaN(n)) return;
              setRule(rule.id, { config: { windowHours: n * UNIT_TO_HOURS[unit] } });
            }}
          />
          <Select value={unit} disabled={locked} onValueChange={(v) => setUnit(v as WindowUnit)}>
            <SelectTrigger className="h-8 w-28 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {(Object.keys(UNIT_LABEL) as WindowUnit[]).map((u) => (
                <SelectItem key={u} value={u}>
                  {UNIT_LABEL[u]}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        {windowIssue && <FieldMessage>{windowIssue}</FieldMessage>}
      </div>

      <FieldMessage tone="warning">
        Changing dedup settings clears this block's cache at the next deploy — previously suppressed records may reappear.
      </FieldMessage>
    </div>
  );
}
