// The shared key/value row editor, lifted out of BlockForm's http
// headers / query-parameter editor so the sink-config editor renders the very
// same rows.
//
// It also owns the "platform-owned key" presentation: locked rows are rendered
// inline, in reading order, as disabled inputs carrying the reason they cannot
// be typed. Their values are always passed in computed — nothing here persists
// them.

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";
import { Lock, Plus, X } from "lucide-react";

export interface KvRow {
  k: string;
  v: string;
}

/** A row the platform owns: shown, explained, never editable, never persisted. */
export interface LockedKvRow {
  k: string;
  v: string;
  /** Why it is locked — rendered under the row, in the refusal voice. */
  reason: string;
  /** Optional trailing affordance (e.g. "Edit the topic name"). */
  action?: React.ReactNode;
}

export interface KvRowsProps {
  rows: KvRow[];
  onChange: (rows: KvRow[]) => void;
  locked: boolean;
  /** Optional section label above the rows. */
  label?: string;
  keyPlaceholder?: string;
  valuePlaceholder?: string;
  /** Text of the add button, e.g. "Add header". */
  addLabel: string;
  /** Rendered above the editable rows, disabled, with their reasons. */
  lockedRows?: LockedKvRow[];
  /** Shown in place of the row list when there is nothing at all. */
  emptyHint?: string;
  /** Width utility for the key column — sink keys are longer than header names. */
  keyClassName?: string;
}

export function KvRows({
  rows,
  onChange,
  locked,
  label,
  keyPlaceholder = "key",
  valuePlaceholder = "value",
  addLabel,
  lockedRows = [],
  emptyHint,
  keyClassName = "w-48",
}: KvRowsProps) {
  const patch = (index: number, patchRow: Partial<KvRow>) =>
    onChange(rows.map((r, j) => (j === index ? { ...r, ...patchRow } : r)));

  return (
    <div className="space-y-2">
      {label && <Label>{label}</Label>}

      {lockedRows.map((row) => (
        <div key={`locked-${row.k}`} className="space-y-1.5 rounded-lg bg-muted/50 px-2.5 py-2 ring-1 ring-inset ring-border/60">
          <div className="flex items-center gap-2">
            <Lock className="h-3.5 w-3.5 shrink-0 text-muted-foreground/70" />
            <Input className={cn("h-8 bg-transparent font-mono text-xs shadow-none", keyClassName)} value={row.k} disabled readOnly />
            <Input className="h-8 flex-1 bg-transparent font-mono text-xs shadow-none" value={row.v} disabled readOnly />
            {row.action}
          </div>
          <p className="pl-[1.375rem] text-xs leading-relaxed text-muted-foreground">{row.reason}</p>
        </div>
      ))}

      {rows.length === 0 && lockedRows.length === 0 && emptyHint && (
        <p className="rounded-lg bg-muted/40 px-3 py-2.5 text-xs leading-relaxed text-muted-foreground ring-1 ring-inset ring-border/50">
          {emptyHint}
        </p>
      )}

      {rows.length > 0 && (
        <div className="space-y-1.5">
          {rows.map((row, i) => (
            // `group` so the remove button can stay invisible until the row is
            // hovered — a column of red trash icons is a lot of alarm for what
            // is usually a list of two headers.
            <div key={i} className="group flex items-center gap-2">
              <Input
                className={cn("h-8 font-mono text-xs", keyClassName)}
                value={row.k}
                placeholder={keyPlaceholder}
                disabled={locked}
                onChange={(e) => patch(i, { k: e.target.value })}
              />
              <Input
                className="h-8 flex-1 font-mono text-xs"
                value={row.v}
                placeholder={valuePlaceholder}
                disabled={locked}
                onChange={(e) => patch(i, { v: e.target.value })}
              />
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button
                    size="icon-xs"
                    variant="ghost"
                    className="shrink-0 text-muted-foreground opacity-0 transition-opacity hover:text-destructive focus-visible:opacity-100 group-hover:opacity-100"
                    disabled={locked}
                    onClick={() => onChange(rows.filter((_, j) => j !== i))}
                  >
                    <X />
                    <span className="sr-only">Remove row</span>
                  </Button>
                </TooltipTrigger>
                <TooltipContent>Remove row</TooltipContent>
              </Tooltip>
            </div>
          ))}
        </div>
      )}

      <Button variant="outline" size="xs" disabled={locked} onClick={() => onChange([...rows, { k: "", v: "" }])}>
        <Plus /> {addLabel}
      </Button>
    </div>
  );
}
