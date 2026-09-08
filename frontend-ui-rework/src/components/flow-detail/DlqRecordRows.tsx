import { useState } from "react";
import { toast } from "sonner";
import { Check, ChevronDown, Copy } from "lucide-react";

import { Button } from "@/components/ui/button";
import { timeAgo } from "@/lib/api";
import type { DlqRecord } from "@/prototype/types";

function DlqRecordRow({ record, expanded, onToggle }: { record: DlqRecord; expanded: boolean; onToggle: () => void }): JSX.Element {
  const [copied, setCopied] = useState(false);
  const copyPayload = async () => {
    try {
      await navigator.clipboard.writeText(record.payloadPreview);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1600);
    } catch {
      toast.error("Could not copy the DLQ payload preview");
    }
  };

  return (
    <div className="overflow-hidden rounded-lg border border-border/70 bg-background/20">
      <button type="button" className="flex w-full min-w-0 items-center gap-3 px-3 py-2.5 text-left transition-colors hover:bg-muted/30" aria-expanded={expanded} onClick={onToggle}>
        <ChevronDown className={`h-4 w-4 shrink-0 text-muted-foreground transition-transform ${expanded ? "rotate-180" : ""}`} />
        <div className="min-w-0 flex-1">
          <div className="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1 text-[11px] text-muted-foreground"><span>{timeAgo(record.ts)}</span><span className="truncate">{record.blockName}</span><code className="rounded bg-muted px-1.5 py-0.5 text-[10px] text-foreground">{record.errorClass}</code></div>
          <div className="mt-1 truncate font-mono text-xs text-foreground/90">{record.payloadPreview}</div>
        </div>
        <span className="hidden shrink-0 text-2xs text-muted-foreground sm:inline">{expanded ? "Collapse" : "View"}</span>
      </button>
      {expanded && <div className="border-t border-border/60 bg-muted/10 px-3 py-3">
        <div className="grid gap-2 text-xs sm:grid-cols-3">
          <div className="min-w-0"><div className="text-2xs uppercase tracking-wide text-muted-foreground">Time</div><code className="break-all text-foreground">{new Date(record.ts).toLocaleString()}</code></div>
          <div className="min-w-0"><div className="text-2xs uppercase tracking-wide text-muted-foreground">Block</div><span className="break-words text-foreground">{record.blockName}</span></div>
          <div className="min-w-0"><div className="text-2xs uppercase tracking-wide text-muted-foreground">Error class</div><code className="break-all text-foreground">{record.errorClass}</code></div>
        </div>
        <div className="mt-3"><div className="mb-1.5 flex items-center justify-between gap-2"><div className="text-2xs uppercase tracking-wide text-muted-foreground">Payload preview</div><Button type="button" size="sm" variant="ghost" className="h-7 px-2 text-xs" onClick={copyPayload}>{copied ? <Check className="mr-1.5 h-3.5 w-3.5 text-emerald-500" /> : <Copy className="mr-1.5 h-3.5 w-3.5" />}{copied ? "Copied" : "Copy"}</Button></div><pre className="max-h-72 overflow-auto whitespace-pre-wrap break-words rounded-md border border-border/60 bg-background/60 p-3 font-mono text-xs leading-relaxed text-foreground">{record.payloadPreview}</pre></div>
      </div>}
    </div>
  );
}

export function DlqRecordRows({ records }: { records: DlqRecord[] }): JSX.Element {
  const [expandedId, setExpandedId] = useState<string | null>(null);
  return <div className="max-h-[min(32rem,52vh)] space-y-1.5 overflow-y-auto pr-1">{records.map((record) => <DlqRecordRow key={record.id} record={record} expanded={expandedId === record.id} onToggle={() => setExpandedId((current) => current === record.id ? null : record.id)} />)}</div>;
}
