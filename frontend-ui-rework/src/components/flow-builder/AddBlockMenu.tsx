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
import { cn } from "@/lib/utils";
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
      <DropdownMenuTrigger asChild>{children ?? <span aria-hidden className="block h-0 w-0" />}</DropdownMenuTrigger>
      <DropdownMenuContent align="start" className="w-80">
        <DropdownMenuLabel>{label}</DropdownMenuLabel>
        {legal.length === 0 && refused.length === 0 && (
          <p className="px-2 py-1.5 text-xs text-muted-foreground">Nothing may be added here.</p>
        )}

        {legal.map((entry) => {
          const Icon = entry.adapter ? ADAPTER_META[entry.adapter].icon : GitBranch;
          // The adapter's own tint on the icon tile, so the menu carries the
          // same colour vocabulary as the chips and the map nodes.
          const tint = entry.adapter ? ADAPTER_META[entry.adapter].chipClass : "bg-muted text-muted-foreground";
          return (
            <DropdownMenuItem key={entry.key} onClick={() => onSelect(entry)} className="items-start gap-2.5 py-2">
              <span className={cn("mt-0.5 flex h-6 w-6 shrink-0 items-center justify-center rounded-md", tint)}>
                <Icon className="h-3.5 w-3.5" />
              </span>
              <span className="flex min-w-0 flex-col gap-0.5">
                <span className="text-sm font-medium leading-tight">{entry.label}</span>
                <span className="text-xs leading-relaxed text-muted-foreground">{entry.description}</span>
              </span>
            </DropdownMenuItem>
          );
        })}

        {refused.length > 0 && <DropdownMenuSeparator />}
        {refused.map((entry) => (
          <DropdownMenuItem key={entry.key} disabled className="items-start gap-2.5 py-2 !opacity-100">
            <span className="mt-0.5 flex h-6 w-6 shrink-0 items-center justify-center rounded-md bg-muted text-muted-foreground">
              <Lock className="h-3.5 w-3.5" />
            </span>
            <span className="flex min-w-0 flex-col gap-0.5">
              <span className="text-sm font-medium leading-tight text-muted-foreground">{entry.label}</span>
              {/* Refusals are quoted verbatim — the spec requires every refusal
                  to explain itself in these words. They are also the reason the
                  row overrides the disabled opacity: a refusal the user cannot
                  read is not a refusal that explains itself. */}
              <span className="text-xs leading-relaxed text-muted-foreground">{entry.disabledReason}</span>
            </span>
          </DropdownMenuItem>
        ))}

        {future.length > 0 && (
          <>
            <DropdownMenuSeparator />
            <DropdownMenuLabel>Coming later</DropdownMenuLabel>
          </>
        )}
        {future.map((entry) => (
          <DropdownMenuItem key={entry.key} disabled className="items-start gap-2.5 py-1.5">
            <span className="flex min-w-0 flex-col gap-0.5 pl-[2.125rem]">
              <span className="text-sm leading-tight">{entry.label}</span>
              <span className="text-xs leading-relaxed text-muted-foreground">{entry.description}</span>
            </span>
          </DropdownMenuItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
