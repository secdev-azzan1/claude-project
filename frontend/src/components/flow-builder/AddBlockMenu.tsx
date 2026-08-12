// One menu for every "+ add" affordance: renders the legality engine's entries —
// legal blocks clickable, refusals and future scope greyed with their reasons.
//
// There is nothing here but adapters. Adding a block IS creating a branch off
// its parent; whether that branch carries a condition is decided afterwards, in
// Routing, on the branch itself.
//
// It has two shapes. With `children` it is a normal trigger-anchored dropdown.
// Without them it is *anchorless*: the caller positions a zero-size anchor
// (absolutely, at a drop point on the canvas) and drives `open` itself — that is
// how "drag a handle, release on empty canvas" opens the same legality-filtered
// menu the ＋ buttons open, rather than a second, divergent picker.

import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { ADAPTER_META } from "@/components/AdapterChip";
import type { AddMenuEntry } from "@/prototype/legality";
import { GitBranch, Lock } from "lucide-react";

export function AddBlockMenu({
  entries,
  onSelect,
  children,
  label = "Add a block",
  open,
  onOpenChange,
}: {
  entries: AddMenuEntry[];
  onSelect: (entry: AddMenuEntry) => void;
  /** Omit to get an anchorless menu — the caller then owns `open`/`onOpenChange`. */
  children?: React.ReactNode;
  label?: string;
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
}) {
  const legal = entries.filter((e) => !e.disabledReason);
  const refused = entries.filter((e) => e.disabledReason && !e.futureScope);
  const future = entries.filter((e) => e.futureScope);

  return (
    <DropdownMenu open={open} onOpenChange={onOpenChange}>
      <DropdownMenuTrigger asChild>
        {children ?? <span aria-hidden className="block h-0 w-0" />}
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start" className="w-80 shadow-xl">
        <DropdownMenuLabel className="text-xs font-medium text-muted-foreground">{label}</DropdownMenuLabel>
        {legal.length === 0 && refused.length === 0 && (
          <p className="px-2 py-1.5 text-xs text-muted-foreground">Nothing may be added here.</p>
        )}
        {legal.map((entry) => {
          const Icon = entry.adapter ? ADAPTER_META[entry.adapter].icon : GitBranch;
          return (
            <DropdownMenuItem key={entry.key} onClick={() => onSelect(entry)} className="items-start gap-2 py-2">
              <Icon className="mt-0.5 h-4 w-4 shrink-0 text-muted-foreground" />
              <span className="flex flex-col">
                <span className="text-sm font-medium leading-4">{entry.label}</span>
                <span className="text-xs text-muted-foreground">{entry.description}</span>
              </span>
            </DropdownMenuItem>
          );
        })}
        {refused.length > 0 && <DropdownMenuSeparator />}
        {refused.map((entry) => (
          <DropdownMenuItem key={entry.key} disabled className="items-start gap-2 py-2 opacity-60">
            <Lock className="mt-0.5 h-4 w-4 shrink-0" />
            <span className="flex flex-col">
              <span className="text-sm font-medium leading-4">{entry.label}</span>
              {/* Refusals are quoted verbatim — the spec requires every refusal to explain itself in these words. */}
              <span className="text-xs">{entry.disabledReason}</span>
            </span>
          </DropdownMenuItem>
        ))}
        {future.length > 0 && (
          <>
            <DropdownMenuSeparator />
            <DropdownMenuLabel className="text-xs font-normal uppercase tracking-wide text-muted-foreground">
              Coming later
            </DropdownMenuLabel>
          </>
        )}
        {future.map((entry) => (
          <DropdownMenuItem key={entry.key} disabled className="items-start gap-2 py-1.5 opacity-60">
            <span className="flex flex-col">
              <span className="text-sm leading-4">{entry.label}</span>
              <span className="text-xs">{entry.description}</span>
            </span>
          </DropdownMenuItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
