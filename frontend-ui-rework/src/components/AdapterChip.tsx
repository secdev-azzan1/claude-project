// The single visual vocabulary for the five adapters. Every page renders
// adapters through this — icon, tint, and label stay consistent everywhere.
//
// The chip lost its border and its pill shape. An adapter name is a technical
// identifier sitting next to a human-written block name, so it is set small, in
// a soft tint, with the icon carrying most of the recognition — it annotates the
// name rather than competing with it.

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
    chipClass: "bg-info-muted text-info",
    nodeClass: "border-info/40",
    description: "Read / write / lookup against APIs",
  },
  jdbc: {
    label: "jdbc",
    icon: Database,
    chipClass: "bg-warning-muted text-warning",
    nodeClass: "border-warning/40",
    description: "Read / write / lookup against databases",
  },
  kafka: {
    label: "kafka",
    icon: Radio,
    chipClass: "bg-primary-muted text-primary",
    nodeClass: "border-primary/30",
    description: "Schemaless topics — read anywhere, write home",
  },
  kafka_kc: {
    label: "kafka+connect",
    icon: Layers,
    chipClass: "bg-success-muted text-success",
    nodeClass: "border-success/40",
    description: "Governed Avro topic + managed sink, one unit",
  },
  kc: {
    label: "kc",
    icon: Cable,
    chipClass: "bg-muted text-muted-foreground",
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
        "inline-flex w-fit shrink-0 items-center gap-1 rounded-md px-1.5 py-0.5 font-mono text-2xs font-medium leading-4",
        meta.chipClass,
        className,
      )}
    >
      <Icon className="h-3 w-3 shrink-0" />
      {meta.label}
      {mode ? <span className="opacity-65">·{mode}</span> : null}
    </span>
  );
}
