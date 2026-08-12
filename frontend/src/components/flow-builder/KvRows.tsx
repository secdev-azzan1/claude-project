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
import { cn } from "@/lib/utils";
import { Lock, Plus, Trash2 } from "lucide-react";

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
    <div className="grid gap-1.5">
      {label && <Label>{label}</Label>}

      {lockedRows.map((row) => (
        <div key={`locked-${row.k}`} className="rounded-md border border-dashed bg-muted/40 px-2 py-1.5">
          <div className="flex items-center gap-1.5">
            <Lock className="h-3 w-3 shrink-0 text-muted-foreground" />
            <Input className={cn("h-7 font-mono text-xs", keyClassName)} value={row.k} disabled readOnly />
            <Input className="h-7 flex-1 font-mono text-xs" value={row.v} disabled readOnly />
            {row.action}
          </div>
          <p className="mt-1 pl-[1.125rem] text-xs leading-4 text-muted-foreground">{row.reason}</p>
        </div>
      ))}

      {rows.length === 0 && lockedRows.length === 0 && emptyHint && (
        <p className="rounded-md border border-dashed px-3 py-2 text-xs text-muted-foreground">{emptyHint}</p>
      )}

      {rows.map((row, i) => (
        <div key={i} className="flex items-center gap-1.5">
          <Input
            className={cn("h-7 font-mono text-xs", keyClassName)}
            value={row.k}
            placeholder={keyPlaceholder}
            disabled={locked}
            onChange={(e) => patch(i, { k: e.target.value })}
          />
          <Input
            className="h-7 flex-1 font-mono text-xs"
            value={row.v}
            placeholder={valuePlaceholder}
            disabled={locked}
            onChange={(e) => patch(i, { v: e.target.value })}
          />
          <Button
            size="icon"
            variant="ghost"
            className="h-6 w-6 text-destructive"
            disabled={locked}
            title="Remove row"
            onClick={() => onChange(rows.filter((_, j) => j !== i))}
          >
            <Trash2 className="h-3 w-3" />
          </Button>
        </div>
      ))}

      <Button
        variant="outline"
        size="sm"
        className="h-7 w-fit gap-1 text-xs"
        disabled={locked}
        onClick={() => onChange([...rows, { k: "", v: "" }])}
      >
        <Plus className="h-3 w-3" /> {addLabel}
      </Button>
    </div>
  );
}
