import { useState } from "react";
import { toast } from "sonner";
import { Check, ChevronDown, Copy } from "lucide-react";

import { Button } from "@/components/ui/button";
import { timeAgo } from "@/lib/api";
import type { TopicMessage } from "@/prototype/types";

function formatPayload(value: string): string {
  try {
    return JSON.stringify(JSON.parse(value), null, 2);
  } catch {
    return value;
  }
}

function MessageRow({ message, expanded, onToggle }: { message: TopicMessage; expanded: boolean; onToggle: () => void }): JSX.Element {
  const [copied, setCopied] = useState(false);
  const payload = message.value === null ? null : formatPayload(message.value);

  const copyPayload = async () => {
    if (!payload) return;
    try {
      await navigator.clipboard.writeText(payload);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1600);
    } catch {
      toast.error("Could not copy the message payload");
    }
  };

  return (
    <div className="overflow-hidden rounded-lg border border-border/70 bg-background/20">
      <button
        type="button"
        className="flex w-full min-w-0 items-center gap-3 px-3 py-2.5 text-left transition-colors hover:bg-muted/30"
        aria-expanded={expanded}
        onClick={onToggle}
      >
        <ChevronDown className={`h-4 w-4 shrink-0 text-muted-foreground transition-transform ${expanded ? "rotate-180" : ""}`} />
        <div className="min-w-0 flex-1">
          <div className="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1 font-mono text-[11px] text-muted-foreground">
            <span>offset {message.offset}</span>
            <span>{timeAgo(message.ts)}</span>
            <span className="max-w-full truncate">key {message.key ?? "—"}</span>
          </div>
          <div className="mt-1 truncate font-mono text-xs text-foreground/90">
            {message.value !== null ? message.value : <span className="font-sans italic text-muted-foreground">binary payload ({message.bytes} bytes)</span>}
          </div>
        </div>
        <span className="hidden shrink-0 text-2xs text-muted-foreground sm:inline">{expanded ? "Collapse" : "View"}</span>
      </button>

      {expanded && (
        <div className="border-t border-border/60 bg-muted/10 px-3 py-3">
          <div className="grid gap-2 text-xs sm:grid-cols-3">
            <div className="min-w-0"><div className="text-2xs uppercase tracking-wide text-muted-foreground">Offset</div><code className="break-all text-foreground">{message.offset}</code></div>
            <div className="min-w-0"><div className="text-2xs uppercase tracking-wide text-muted-foreground">Timestamp</div><code className="break-all text-foreground">{new Date(message.ts).toLocaleString()}</code></div>
            <div className="min-w-0"><div className="text-2xs uppercase tracking-wide text-muted-foreground">Key</div><code className="break-all text-foreground">{message.key ?? "—"}</code></div>
          </div>
          <div className="mt-3">
            <div className="mb-1.5 flex items-center justify-between gap-2">
              <div className="text-2xs uppercase tracking-wide text-muted-foreground">Payload</div>
              {payload && <Button type="button" size="sm" variant="ghost" className="h-7 px-2 text-xs" onClick={copyPayload}>{copied ? <Check className="mr-1.5 h-3.5 w-3.5 text-emerald-500" /> : <Copy className="mr-1.5 h-3.5 w-3.5" />}{copied ? "Copied" : "Copy"}</Button>}
            </div>
            {payload ? <pre className="max-h-72 overflow-auto whitespace-pre-wrap break-words rounded-md border border-border/60 bg-background/60 p-3 font-mono text-xs leading-relaxed text-foreground">{payload}</pre> : <div className="rounded-md border border-border/60 bg-background/60 p-3 text-xs italic text-muted-foreground">Binary payload ({message.bytes} bytes) is not decoded here.</div>}
          </div>
        </div>
      )}
    </div>
  );
}

export function TopicMessageRows({ messages }: { messages: TopicMessage[] }): JSX.Element {
  const [expandedOffset, setExpandedOffset] = useState<number | null>(null);
  return <div className="max-h-[min(32rem,52vh)] space-y-1.5 overflow-y-auto pr-1">{messages.map((message) => <MessageRow key={`${message.offset}-${message.ts}`} message={message} expanded={expandedOffset === message.offset} onToggle={() => setExpandedOffset((current) => current === message.offset ? null : message.offset)} />)}</div>;
}
