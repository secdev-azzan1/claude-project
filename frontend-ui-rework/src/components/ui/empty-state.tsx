import * as React from "react";

import { cn } from "@/lib/utils";

/**
 * The shared "there is nothing here yet" surface.
 *
 * Seventeen of these were hand-rolled across the pages, most as
 * `rounded-md border border-dashed px-3 py-2 text-xs text-muted-foreground` —
 * a dashed outline around a sentence. Dashed borders read as "drop target" or
 * "unfinished", and at 17 instances the app looked like a wireframe.
 *
 * This is quieter: a soft filled panel, an optional icon, and the message set as
 * real text. `inline` keeps the compact one-line form for empty states that sit
 * inside an already-small panel.
 */
export function EmptyState({
  icon: Icon,
  title,
  children,
  action,
  inline = false,
  className,
}: {
  icon?: React.ComponentType<{ className?: string }>;
  title?: React.ReactNode;
  children?: React.ReactNode;
  action?: React.ReactNode;
  /** One quiet line inside an existing panel, rather than a centred block. */
  inline?: boolean;
  className?: string;
}) {
  if (inline) {
    return (
      <p
        className={cn(
          "rounded-lg bg-muted/40 px-3 py-2.5 text-xs leading-relaxed text-muted-foreground ring-1 ring-inset ring-border/50",
          className,
        )}
      >
        {title && <span className="font-medium text-foreground">{title} </span>}
        {children}
      </p>
    );
  }

  return (
    <div
      className={cn(
        "flex flex-col items-center justify-center rounded-xl bg-muted/40 px-6 py-10 text-center ring-1 ring-inset ring-border/50",
        className,
      )}
    >
      {Icon && (
        <div className="mb-3 flex h-11 w-11 items-center justify-center rounded-2xl bg-background shadow-sm">
          <Icon className="h-5 w-5 text-muted-foreground/70" />
        </div>
      )}
      {title && <p className="text-sm font-medium">{title}</p>}
      {children && (
        <div className="mt-1 max-w-sm text-xs leading-relaxed text-muted-foreground">{children}</div>
      )}
      {action && <div className="mt-4">{action}</div>}
    </div>
  );
}
