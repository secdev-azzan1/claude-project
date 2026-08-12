import { cn } from "@/lib/utils";
import { CheckCircle2, AlertCircle, Circle, Clock, XCircle, Activity, Pause } from "lucide-react";

type Variant = "success" | "warning" | "destructive" | "info" | "muted";

const variantClasses: Record<Variant, string> = {
  success: "bg-success-muted text-success border-success/20",
  warning: "bg-warning-muted text-warning border-warning/20",
  destructive: "bg-destructive-muted text-destructive border-destructive/20",
  info: "bg-info-muted text-info border-info/20",
  muted: "bg-muted text-muted-foreground border-border",
};

const labelMap: Record<string, { variant: Variant; icon: React.ComponentType<{ className?: string }> }> = {
  Healthy: { variant: "success", icon: CheckCircle2 },
  Running: { variant: "success", icon: Activity },
  Verified: { variant: "success", icon: CheckCircle2 },
  Success: { variant: "success", icon: CheckCircle2 },
  "Schema Registered": { variant: "info", icon: CheckCircle2 },
  "Schema Generated": { variant: "info", icon: CheckCircle2 },
  "Pending Verification": { variant: "warning", icon: Clock },
  "Needs Verification": { variant: "warning", icon: Clock },
  Degraded: { variant: "warning", icon: AlertCircle },
  "Schema Outdated": { variant: "warning", icon: AlertCircle },
  "Not Tested": { variant: "muted", icon: Circle },
  Draft: { variant: "muted", icon: Circle },
  Deploying: { variant: "info", icon: Clock },
  Stopped: { variant: "muted", icon: Pause },
  Failed: { variant: "destructive", icon: XCircle },
  Error: { variant: "destructive", icon: XCircle },
  // Kafka Connect connector/task states (reported uppercase by the Connect status API)
  RUNNING: { variant: "success", icon: Activity },
  PAUSED: { variant: "muted", icon: Pause },
  FAILED: { variant: "destructive", icon: XCircle },
  UNASSIGNED: { variant: "warning", icon: AlertCircle },
  RESTARTING: { variant: "info", icon: Clock },
  // Sink states derived by the platform rather than reported by Connect
  Paused: { variant: "muted", icon: Pause },
  Disabled: { variant: "muted", icon: Circle },
  "Not Deployed": { variant: "muted", icon: Circle },
  Unknown: { variant: "muted", icon: Circle },
  // Adapter-prototype vocabulary
  Approved: { variant: "success", icon: CheckCircle2 },
  Active: { variant: "info", icon: CheckCircle2 },
  Inactive: { variant: "muted", icon: Circle },
  Reachable: { variant: "success", icon: CheckCircle2 },
  Unreachable: { variant: "destructive", icon: XCircle },
  Retired: { variant: "muted", icon: XCircle },
  "Update available": { variant: "info", icon: Clock },
  "Action required": { variant: "warning", icon: AlertCircle },
  "Ceremony required": { variant: "warning", icon: AlertCircle },
  Sealed: { variant: "muted", icon: Circle },
  Adopted: { variant: "info", icon: Circle },
  Reconciled: { variant: "success", icon: CheckCircle2 },
  Pending: { variant: "warning", icon: Clock },
  Valid: { variant: "success", icon: CheckCircle2 },
  Invalid: { variant: "destructive", icon: AlertCircle },
};

export function StatusBadge({
  status,
  className,
  compact = false,
}: {
  status: string;
  className?: string;
  compact?: boolean;
}) {
  const info = labelMap[status] ?? { variant: "muted" as Variant, icon: Circle };
  const Icon = info.icon;
  return (
    <span
      title={status}
      aria-label={status}
      className={cn(
        "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium",
        compact && "h-6 w-6 justify-center gap-0 px-0",
        variantClasses[info.variant],
        className,
      )}
    >
      <Icon className="h-3 w-3" />
      {compact ? <span className="sr-only">{status}</span> : status}
    </span>
  );
}
