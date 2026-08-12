// The single visual vocabulary for the five adapters. Every page renders
// adapters through this — icon, tint, and label stay consistent everywhere.

import { cn } from "@/lib/utils";
import type { AdapterId, BlockMode } from "@/prototype/types";
import { Cable, Database, Globe, Layers, Radio } from "lucide-react";

export const ADAPTER_META: Record<
  AdapterId,
  {
    label: string;
    icon: React.ComponentType<{ className?: string }>;
    chipClass: string;
    nodeClass: string;
    description: string;
  }
> = {
  http: {
    label: "http",
    icon: Globe,
    chipClass: "bg-info-muted text-info border-info/20",
    nodeClass: "border-info/40",
    description: "Read / write / lookup against APIs",
  },
  jdbc: {
    label: "jdbc",
    icon: Database,
    chipClass: "bg-warning-muted text-warning border-warning/20",
    nodeClass: "border-warning/40",
    description: "Read / write / lookup against databases",
  },
  kafka: {
    label: "kafka",
    icon: Radio,
    chipClass: "bg-accent text-accent-foreground border-border",
    nodeClass: "border-primary/30",
    description: "Schemaless topics — read anywhere, write home",
  },
  kafka_kc: {
    label: "kafka+connect",
    icon: Layers,
    chipClass: "bg-success-muted text-success border-success/20",
    nodeClass: "border-success/40",
    description: "Governed Avro topic + managed sink, one unit",
  },
  kc: {
    label: "kc",
    icon: Cable,
    chipClass: "bg-muted text-muted-foreground border-border",
    nodeClass: "border-dashed border-muted-foreground/40",
    description: "Sink subscription over an existing topic",
  },
};

export function AdapterChip({
  adapter,
  mode,
  className,
}: {
  adapter: AdapterId;
  mode?: BlockMode;
  className?: string;
}) {
  const meta = ADAPTER_META[adapter];
  const Icon = meta.icon;
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-xs font-medium leading-4",
        meta.chipClass,
        className,
      )}
    >
      <Icon className="h-3 w-3" />
      {meta.label}
      {mode ? <span className="opacity-70">· {mode}</span> : null}
    </span>
  );
}
