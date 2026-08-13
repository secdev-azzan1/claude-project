// The HTTP adapter's Path field once an OpenAPI document is attached: still a
// plain, freely-typeable text input (typing never gets blocked or overwritten
// mid-keystroke), but paired with a server-searched dropdown of the
// document's operations. Picking a row fills the path and reports the whole
// operation back to the caller, which is what sets Method and records
// openapiOperationId — this component only ever reports path text and
// picks, it does not know about the rest of the block's config.
//
// Free typing after a pick is a deliberate escape hatch (flexibility rule):
// the caller clears openapiOperationId as soon as the text no longer matches
// what was picked, so a hand-edited path is never silently misreported as
// "from the doc".

import { useEffect, useState } from "react";
import * as PopoverPrimitive from "@radix-ui/react-popover";
import { useQuery } from "@tanstack/react-query";
import { Loader2, Search } from "lucide-react";
import { Input } from "@/components/ui/input";
import { PopoverContent } from "@/components/ui/popover";
import { cn } from "@/lib/utils";
import { useDebouncedValue } from "@/lib/useDebouncedValue";
import { listOpenApiOperations, type OpenApiOperationSummary } from "@/prototype/openapiClient";

const PAGE_SIZE = 50;
const DEBOUNCE_MS = 250;

export interface OpenApiPathComboboxProps {
  specId: string;
  locked: boolean;
  value: string;
  placeholder?: string;
  className?: string;
  /** Free typing — always fires, including while a dropdown row is being picked. */
  onChange: (path: string) => void;
  /** Fires only when a row is picked from the dropdown. */
  onSelectOperation: (operation: OpenApiOperationSummary) => void;
}

export function OpenApiPathCombobox({
  specId,
  locked,
  value,
  placeholder,
  className,
  onChange,
  onSelectOperation,
}: OpenApiPathComboboxProps) {
  const [open, setOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(0);
  const debouncedSearch = useDebouncedValue(value, DEBOUNCE_MS);

  const opsQuery = useQuery({
    queryKey: ["openapi-operations", specId, debouncedSearch],
    queryFn: () => listOpenApiOperations(specId, { search: debouncedSearch, page: 1, pageSize: PAGE_SIZE }),
    enabled: open && !locked,
    staleTime: 30_000,
  });

  const items = opsQuery.data?.items ?? [];

  useEffect(() => {
    setActiveIndex(0);
  }, [items.length, debouncedSearch]);

  const pick = (op: OpenApiOperationSummary) => {
    onSelectOperation(op);
    setOpen(false);
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (!open) {
      if (e.key === "ArrowDown") setOpen(true);
      return;
    }
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setActiveIndex((i) => Math.min(i + 1, Math.max(items.length - 1, 0)));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActiveIndex((i) => Math.max(i - 1, 0));
    } else if (e.key === "Enter") {
      if (items[activeIndex]) {
        e.preventDefault();
        pick(items[activeIndex]);
      }
    } else if (e.key === "Escape") {
      setOpen(false);
    }
  };

  return (
    <PopoverPrimitive.Root open={open} onOpenChange={setOpen}>
      <PopoverPrimitive.Anchor asChild>
        <Input
          className={cn("font-mono text-xs", className)}
          value={value}
          disabled={locked}
          placeholder={placeholder}
          onFocus={() => setOpen(true)}
          onChange={(e) => {
            onChange(e.target.value);
            setOpen(true);
          }}
          onKeyDown={handleKeyDown}
        />
      </PopoverPrimitive.Anchor>
      <PopoverContent
        align="start"
        sideOffset={4}
        onOpenAutoFocus={(e) => e.preventDefault()}
        onCloseAutoFocus={(e) => e.preventDefault()}
        className="w-[var(--radix-popover-trigger-width)] min-w-[20rem] max-w-[32rem] p-0"
      >
        <div className="max-h-72 overflow-y-auto p-1">
          {opsQuery.isFetching && (
            <div className="flex items-center gap-1.5 px-2 py-2 text-xs text-muted-foreground">
              <Loader2 className="h-3 w-3 animate-spin" /> Searching…
            </div>
          )}
          {!opsQuery.isFetching && items.length === 0 && (
            <div className="flex items-center gap-1.5 px-2 py-3 text-xs text-muted-foreground">
              <Search className="h-3 w-3 shrink-0" />
              {debouncedSearch ? "No matching operations." : "No operations in this document."}
            </div>
          )}
          {items.map((op, i) => (
            <button
              key={op.operationId}
              type="button"
              className={cn(
                "flex w-full flex-col items-start gap-0.5 rounded-sm px-2 py-1.5 text-left text-xs",
                i === activeIndex ? "bg-accent text-accent-foreground" : "hover:bg-accent/60",
              )}
              onMouseEnter={() => setActiveIndex(i)}
              onMouseDown={(e) => e.preventDefault()} // keep focus in the Input, avoid a blur-before-click race
              onClick={() => pick(op)}
            >
              <span className="flex w-full items-center gap-1.5 font-mono">
                <span className="shrink-0 rounded bg-muted px-1 py-0.5 font-sans text-[10px] font-semibold uppercase tracking-wide">
                  {op.method}
                </span>
                <span className="truncate">{op.path}</span>
              </span>
              {op.summary && <span className="truncate pl-0.5 text-muted-foreground">{op.summary}</span>}
            </button>
          ))}
          {opsQuery.data && opsQuery.data.total > items.length && (
            <p className="px-2 py-1 text-[10px] text-muted-foreground">
              Showing {items.length} of {opsQuery.data.total} — narrow the search to see more.
            </p>
          )}
        </div>
      </PopoverContent>
    </PopoverPrimitive.Root>
  );
}
