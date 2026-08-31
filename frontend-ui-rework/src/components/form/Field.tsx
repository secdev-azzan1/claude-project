// The form vocabulary for the whole app.
//
// The old build put a `<p className="text-xs text-muted-foreground">` under
// almost every input, and used it for four different jobs at once: stating a
// platform rule, echoing derived state, warning about a problem, and refusing an
// action. All four rendered identically, so none of them could recede — which is
// most of why a form with six fields in it read as a wall of text.
//
// This file separates them and gives each one a home:
//
//   info      → a rule. Moves behind the ⓘ next to the label. Always available,
//               never occupying a line. Text stays verbatim.
//   hint      → derived state the user needs to SEE (a resolved URL, a computed
//               name). Stays inline, but set quiet and small.
//   warning   → something is off but the form still works.
//   error     → a real problem, usually a deploy blocker.
//
// If a sentence does not fit one of those four, it is probably documentation and
// does not belong on the screen at all.

import * as React from "react";
import { AlertCircle, Info } from "lucide-react";

import { Label } from "@/components/ui/label";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { cn } from "@/lib/utils";

/**
 * The ⓘ affordance. Rule jargon lives in here rather than in always-visible
 * helper text — the wording is unchanged, it just stops taking up a line.
 */
export function InfoDot({
  title,
  children,
  className,
}: {
  title: string;
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <Popover>
      <PopoverTrigger asChild>
        <button
          type="button"
          aria-label={`About: ${title}`}
          className={cn(
            "inline-flex h-4 w-4 shrink-0 items-center justify-center rounded-full text-muted-foreground/60 transition-colors hover:text-foreground focus-visible:outline-none focus-visible:ring-[3px] focus-visible:ring-ring/35",
            className,
          )}
        >
          <Info className="h-3.5 w-3.5" />
        </button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-72">
        <p className="text-xs font-semibold">{title}</p>
        <div className="mt-1.5 text-xs leading-relaxed text-muted-foreground">{children}</div>
      </PopoverContent>
    </Popover>
  );
}

/** A warning or error line under a control. Icon + one line, never a paragraph. */
export function FieldMessage({
  tone = "error",
  children,
  className,
}: {
  tone?: "error" | "warning";
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <p
      className={cn(
        "flex items-start gap-1.5 text-xs leading-relaxed",
        tone === "error" ? "text-destructive" : "text-warning",
        className,
      )}
    >
      <AlertCircle className="mt-[0.15rem] h-3.5 w-3.5 shrink-0" />
      <span>{children}</span>
    </p>
  );
}

export interface FieldProps {
  label: React.ReactNode;
  /** Rule text. Goes behind the ⓘ next to the label, never on its own line. */
  info?: React.ReactNode;
  /** Derived state worth showing inline (a resolved URL, a computed name). */
  hint?: React.ReactNode;
  warning?: React.ReactNode;
  error?: React.ReactNode;
  /** Rendered at the end of the label row — a badge, a count, a small action. */
  aside?: React.ReactNode;
  htmlFor?: string;
  className?: string;
  children: React.ReactNode;
}

export function Field({ label, info, hint, warning, error, aside, htmlFor, className, children }: FieldProps) {
  return (
    <div className={cn("space-y-1.5", className)}>
      <div className="flex min-h-[1.25rem] items-center gap-1.5">
        <Label htmlFor={htmlFor}>{label}</Label>
        {info && <InfoDot title={typeof label === "string" ? label : "About this field"}>{info}</InfoDot>}
        {aside && <div className="ml-auto flex items-center gap-1.5">{aside}</div>}
      </div>
      {children}
      {hint && <div className="text-xs leading-relaxed text-muted-foreground">{hint}</div>}
      {warning && <FieldMessage tone="warning">{warning}</FieldMessage>}
      {error && <FieldMessage tone="error">{error}</FieldMessage>}
    </div>
  );
}

/** Consistent vertical rhythm between fields. */
export function FieldGroup({ className, children }: { className?: string; children: React.ReactNode }) {
  return <div className={cn("space-y-4", className)}>{children}</div>;
}

/**
 * An inline computed value — topic names, table names, resolved URLs. These are
 * data, not prose, so they get the mono face and a surface of their own.
 */
export function Mono({ className, children }: { className?: string; children: React.ReactNode }) {
  return (
    <code className={cn("rounded-md bg-muted px-1.5 py-0.5 font-mono text-xs text-foreground", className)}>
      {children}
    </code>
  );
}

/**
 * A row of label → value, for read-only facts. Replaces the very common
 * "<p class=text-xs>rev 3 · Healthy · credentials stored on the service</p>"
 * run-on sentence, which was three separate facts glued together with dots.
 */
export function FactRow({ label, children }: { label: React.ReactNode; children: React.ReactNode }) {
  return (
    <div className="flex items-baseline gap-2 text-xs">
      <span className="shrink-0 text-muted-foreground">{label}</span>
      <span className="min-w-0 text-foreground">{children}</span>
    </div>
  );
}

/** A small caps heading that divides a form into chapters without adding a box. */
export function SectionLabel({ className, children }: { className?: string; children: React.ReactNode }) {
  return (
    <div className={cn("text-2xs font-semibold uppercase tracking-wider text-muted-foreground", className)}>
      {children}
    </div>
  );
}
