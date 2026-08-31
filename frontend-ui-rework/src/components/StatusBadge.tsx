// Status, rendered as a coloured dot next to a plain label.
//
// This used to be a bordered, tinted, icon-bearing pill. One of those on screen
// is fine; the Flows list puts one on every row, the Dashboard puts one on every
// flow AND every activity item, and Apisix puts several per route — at which
// point a column of pills is louder than the data it annotates.
//
// A 6px dot carries the same colour coding at a fraction of the visual weight,
// and the label stays selectable text rather than becoming chrome. `tone="soft"`
// brings the tinted pill back for the handful of places that genuinely need a
// status to be the loudest thing in view (a lock banner, a deploy blocker).

import { cn } from "@/lib/utils";

type Variant = "success" | "warning" | "destructive" | "info" | "muted";

const dotClasses: Record<Variant, string> = {
  success: "bg-success",
  warning: "bg-warning",
  destructive: "bg-destructive",
  info: "bg-info",
  muted: "bg-muted-foreground/45",
};

const softClasses: Record<Variant, string> = {
  success: "bg-success-muted text-success ring-success/15",
  warning: "bg-warning-muted text-warning ring-warning/15",
  destructive: "bg-destructive-muted text-destructive ring-destructive/15",
  info: "bg-info-muted text-info ring-info/15",
  muted: "bg-muted text-muted-foreground ring-border",
};

const variantOf: Record<string, Variant> = {
  Healthy: "success",
  Running: "success",
  Verified: "success",
  Success: "success",
  "Schema Registered": "info",
  "Schema Generated": "info",
  "Pending Verification": "warning",
  "Needs Verification": "warning",
  Degraded: "warning",
  "Schema Outdated": "warning",
  "Not Tested": "muted",
  Draft: "muted",
  Deploying: "info",
  Stopped: "muted",
  Failed: "destructive",
  Error: "destructive",
  // Kafka Connect connector/task states (reported uppercase by the Connect status API)
  RUNNING: "success",
  PAUSED: "muted",
  FAILED: "destructive",
  UNASSIGNED: "warning",
  RESTARTING: "info",
  // Sink states derived by the platform rather than reported by Connect
  Paused: "muted",
  Disabled: "muted",
  "Not Deployed": "muted",
  Unknown: "muted",
  // Adapter-prototype vocabulary
  Approved: "success",
  Active: "info",
  Inactive: "muted",
  Reachable: "success",
  Unreachable: "destructive",
  Retired: "muted",
  "Update available": "info",
  "Action required": "warning",
  "Ceremony required": "warning",
  Sealed: "muted",
  Adopted: "info",
  Reconciled: "success",
  Pending: "warning",
  Valid: "success",
  Invalid: "destructive",
};

/** A live state deserves a pulse; a resting one does not. */
const isLive = (status: string) => status === "Running" || status === "RUNNING" || status === "Deploying";

export function StatusBadge({
  status,
  className,
  compact = false,
  tone = "dot",
}: {
  status: string;
  className?: string;
  /** Dot only — the label goes to screen readers. For dense table cells. */
  compact?: boolean;
  /** "dot" is the quiet default; "soft" is the tinted pill, for emphasis. */
  tone?: "dot" | "soft";
}) {
  const variant = variantOf[status] ?? "muted";

  if (tone === "soft") {
    return (
      <span
        title={status}
        aria-label={status}
        className={cn(
          "inline-flex w-fit shrink-0 items-center gap-1.5 rounded-md px-2 py-0.5 text-2xs font-medium ring-1 ring-inset",
          softClasses[variant],
          className,
        )}
      >
        <span className={cn("h-1.5 w-1.5 shrink-0 rounded-full", dotClasses[variant])} />
        {compact ? <span className="sr-only">{status}</span> : status}
      </span>
    );
  }

  return (
    <span
      title={status}
      aria-label={status}
      className={cn(
        "inline-flex w-fit shrink-0 items-center gap-1.5 text-xs font-medium text-foreground",
        compact && "gap-0",
        className,
      )}
    >
      <span className="relative flex h-1.5 w-1.5 shrink-0">
        {isLive(status) && (
          <span
            className={cn("absolute inline-flex h-full w-full animate-ping rounded-full opacity-60", dotClasses[variant])}
          />
        )}
        <span className={cn("relative inline-flex h-1.5 w-1.5 rounded-full", dotClasses[variant])} />
      </span>
      {compact ? <span className="sr-only">{status}</span> : status}
    </span>
  );
}
