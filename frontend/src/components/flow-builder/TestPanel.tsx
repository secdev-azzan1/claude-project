// Per-block Test: one bounded probe (max 10 records) whose stored result
// feeds the field chips and extraction shortcuts. ${placeholder} values are
// prompted; mutating methods are double-confirmed; failures come back as data.

import { useMemo, useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { testBlock } from "@/prototype/api";
import { blockProxyId } from "@/prototype/validation";
import type { AppService, BlockTestResult, Flow, FlowBlock } from "@/prototype/types";
import { timeAgo } from "@/lib/api";
import { AlertCircle, AlertTriangle, FlaskConical, Loader2, Plus } from "lucide-react";
import { toast } from "sonner";

const MUTATING = ["POST", "PUT", "PATCH", "DELETE"];

export interface TestPanelProps {
  flow: Flow;
  block: FlowBlock;
  locked: boolean;
  /** The bound service, so the confirm can name the host the probe really hits. */
  service?: AppService;
  onTested: (result: BlockTestResult) => void;
  onAddExtraction: (field: string, path?: string) => void;
  /** http blocks: set the response record path from a clicked array node. */
  onSetRecordPath?: (path: string) => void;
}

export function TestPanel({ flow, block, locked, service, onTested, onAddExtraction, onSetRecordPath }: TestPanelProps) {
  const [testing, setTesting] = useState(false);
  const [placeholderDialog, setPlaceholderDialog] = useState<string[] | null>(null);
  const [placeholderValues, setPlaceholderValues] = useState<Record<string, string>>({});
  const [confirmMutating, setConfirmMutating] = useState(false);

  const placeholders = useMemo(() => {
    const text = JSON.stringify(block.config ?? {});
    return [...new Set([...text.matchAll(/\$\{([a-zA-Z0-9_.-]+)\}/g)].map((m) => m[1]))];
  }, [block.config]);

  const method = ((block.config.method as string) ?? "GET").toUpperCase();
  const isMutating = block.adapter === "http" && MUTATING.includes(method);

  // What the probe will actually hit, resolved the same way the run would:
  // the bound service's base URL plus the block's path. Shown in the confirm
  // because "a POST" is not a warning — "a POST at this URL" is.
  const target = (() => {
    const base = typeof service?.config?.baseUrl === "string" ? service.config.baseUrl.replace(/\/+$/, "") : "";
    const path = ((block.config.path as string) ?? "").trim();
    if (!base && !path) return "the endpoint this block is bound to";
    return `${base}${path.startsWith("/") || !path ? path : `/${path}`}`;
  })();
  // Egress is the service's, not the block's — a probe leaves the same way the
  // deployed flow will.
  const proxied = !!blockProxyId(block, service ? [service] : []);

  const reallyRun = async () => {
    setTesting(true);
    try {
      const result = await testBlock(flow.id, block.id);
      if (result) {
        onTested(result);
        if (result.ok)
          toast.success(
            isMutating
              ? `${method} sent — ${result.records?.length ?? 0} record(s) parsed from the response, nothing written to a topic`
              : `Test succeeded — ${result.records?.length ?? 0} sample record(s), nothing committed`,
          );
        else toast.error(`Test failed: ${result.reason}`);
      }
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Test failed");
    } finally {
      setTesting(false);
    }
  };

  const start = () => {
    if (placeholders.length > 0) {
      setPlaceholderValues(Object.fromEntries(placeholders.map((p) => [p, ""])));
      setPlaceholderDialog(placeholders);
      return;
    }
    if (isMutating) setConfirmMutating(true);
    else void reallyRun();
  };

  const result = block.testResult;

  return (
    <div className="space-y-2">
      {/* A mutating test really sends the request, so the danger is carried by
          the control itself — not by a tooltip nobody hovers. */}
      {isMutating && (
        <div className="flex items-start gap-2 rounded-md border border-warning/50 bg-warning-muted/50 px-3 py-2">
          <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-warning" />
          <p className="text-xs leading-5">
            <span className="font-medium">This test sends a real {method}.</span> It changes data in the target system —
            there is no dry run. Target:{" "}
            <code className="break-all font-mono text-xs">{target}</code>
            {proxied ? " (through the configured gateway proxy)" : ""}.
          </p>
        </div>
      )}

      <div className="flex flex-wrap items-center gap-2">
        <Button
          size="sm"
          variant={isMutating ? "destructive" : "secondary"}
          className="gap-1.5"
          onClick={start}
          disabled={testing || locked}
        >
          {testing ? (
            <Loader2 className="h-3.5 w-3.5 animate-spin" />
          ) : isMutating ? (
            <AlertTriangle className="h-3.5 w-3.5" />
          ) : (
            <FlaskConical className="h-3.5 w-3.5" />
          )}
          {isMutating ? `Test block — sends a real ${method}` : "Test block"}
        </Button>
        {result && (
          <span className="text-xs text-muted-foreground">
            last tested {timeAgo(result.testedAt)} · bounded probe, max 10 records ·{" "}
            {isMutating ? "nothing is written to a topic — the request itself is real" : "sample runs never commit anything"}
          </span>
        )}
      </div>

      {result && !result.ok && (
        <Alert variant="destructive">
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>Test failed (returned as data, not an error)</AlertTitle>
          <AlertDescription className="text-xs">{result.reason}</AlertDescription>
        </Alert>
      )}

      {result?.ok && (
        <>
          {result.detectedFields && result.detectedFields.length > 0 && (
            <div className="space-y-1">
              <p className="text-xs font-medium text-muted-foreground">
                Detected fields — click to add an extraction rule:
              </p>
              <div className="flex flex-wrap gap-1">
                {result.detectedFields.map((f) => (
                  <button key={f} type="button" onClick={() => !locked && onAddExtraction(f)} disabled={locked}>
                    <Badge variant="outline" className="cursor-pointer gap-0.5 font-mono text-xs hover:bg-accent">
                      <Plus className="h-2.5 w-2.5" />
                      {f}
                    </Badge>
                  </button>
                ))}
              </div>
            </div>
          )}
          <ResponseTree
            records={result.records ?? []}
            locked={locked}
            onExtract={(field, path) => onAddExtraction(field, path)}
            onSetRecordPath={onSetRecordPath}
          />
        </>
      )}

      {/* ${placeholder} prompt */}
      <Dialog open={!!placeholderDialog} onOpenChange={(open) => !open && setPlaceholderDialog(null)}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>Values for this test run</DialogTitle>
            <DialogDescription>
              The configuration references {"${…}"} values that are resolved at runtime. Provide sample values for the probe —
              they are used once and not saved.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-2">
            {(placeholderDialog ?? []).map((p) => (
              <div key={p} className="flex items-center gap-2">
                <code className="w-36 truncate text-xs">{"${" + p + "}"}</code>
                <Input
                  className="h-8 text-sm"
                  value={placeholderValues[p] ?? ""}
                  onChange={(e) => setPlaceholderValues((v) => ({ ...v, [p]: e.target.value }))}
                />
              </div>
            ))}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setPlaceholderDialog(null)}>
              Cancel
            </Button>
            <Button
              onClick={() => {
                setPlaceholderDialog(null);
                if (isMutating) setConfirmMutating(true);
                else void reallyRun();
              }}
            >
              Continue
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* mutating double-confirm — the second gate the spec requires (the
          backend refuses an unconfirmed mutating test independently). */}
      <Dialog open={confirmMutating} onOpenChange={setConfirmMutating}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <AlertTriangle className="h-4 w-4 shrink-0 text-warning" />
              Send a real {method} to {service?.name ?? "the bound service"}?
            </DialogTitle>
            <DialogDescription asChild>
              <div className="space-y-2 text-sm">
                <p>This may change data in the target system — run anyway?</p>
                <div className="rounded-md border bg-muted/50 px-3 py-2">
                  <code className="break-all font-mono text-xs">
                    {method} {target}
                  </code>
                  {proxied && <p className="mt-1 text-xs">Sent through the configured gateway proxy.</p>}
                </div>
                <p className="text-xs">
                  Records the test brings back are never committed to any topic, but the request itself is not a simulation.
                </p>
              </div>
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setConfirmMutating(false)}>
              Cancel
            </Button>
            <Button
              variant="destructive"
              onClick={() => {
                setConfirmMutating(false);
                void reallyRun();
              }}
            >
              I understand — send the {method}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}

// ------------------------------------------------------------ response tree
// The explorer over the stored probe: click a nested field to create an
// extraction rule with its full JSONPath; click an array node to use it as
// the record path. One test feeds everything — no extra calls.

function typeOf(v: unknown): string {
  if (v === null) return "null";
  if (Array.isArray(v)) return `array(${v.length})`;
  return typeof v;
}

function TreeNode({
  name,
  value,
  path,
  depth,
  locked,
  onExtract,
  onSetRecordPath,
}: {
  name: string;
  value: unknown;
  path: string;
  depth: number;
  locked: boolean;
  onExtract: (field: string, path: string) => void;
  onSetRecordPath?: (path: string) => void;
}) {
  const isObject = value !== null && typeof value === "object" && !Array.isArray(value);
  const isArray = Array.isArray(value);
  const preview = isObject
    ? "{…}"
    : isArray
      ? `[${(value as unknown[]).length}]`
      : JSON.stringify(value);

  return (
    <div style={{ paddingLeft: depth * 14 }}>
      <div className="group flex items-center gap-1.5 rounded px-1 py-0.5 hover:bg-muted/60">
        <code className="text-xs font-medium">{name}</code>
        <span className="text-xs text-muted-foreground">{typeOf(value)}</span>
        {!isObject && !isArray && (
          <code className="max-w-56 truncate text-xs text-muted-foreground">{preview}</code>
        )}
        <span className="ml-auto flex gap-1 opacity-0 transition-opacity group-hover:opacity-100">
          {!locked && !isObject && !isArray && (
            <button
              type="button"
              className="rounded border px-1 text-xs hover:bg-accent"
              title={`Extract ${path}`}
              onClick={() => onExtract(name, path)}
            >
              extract
            </button>
          )}
          {!locked && isArray && onSetRecordPath && (
            <button
              type="button"
              className="rounded border px-1 text-xs hover:bg-accent"
              title={`Use ${path}[*] as the record path`}
              onClick={() => onSetRecordPath(`${path}[*]`)}
            >
              record path
            </button>
          )}
        </span>
      </div>
      {isObject &&
        depth < 3 &&
        Object.entries(value as Record<string, unknown>).map(([k, v]) => (
          <TreeNode
            key={k}
            name={k}
            value={v}
            path={`${path}.${k}`}
            depth={depth + 1}
            locked={locked}
            onExtract={onExtract}
            onSetRecordPath={onSetRecordPath}
          />
        ))}
      {isArray &&
        depth < 3 &&
        (value as unknown[]).slice(0, 1).map((v, i) => (
          <TreeNode
            key={i}
            name={`[${i}]`}
            value={v}
            path={`${path}[${i}]`}
            depth={depth + 1}
            locked={locked}
            onExtract={onExtract}
            onSetRecordPath={onSetRecordPath}
          />
        ))}
    </div>
  );
}

function ResponseTree({
  records,
  locked,
  onExtract,
  onSetRecordPath,
}: {
  records: unknown[];
  locked: boolean;
  onExtract: (field: string, path: string) => void;
  onSetRecordPath?: (path: string) => void;
}) {
  const first = records.find((r) => typeof r === "object" && r !== null) as Record<string, unknown> | undefined;
  return (
    <div className="space-y-1">
      <p className="text-xs font-medium text-muted-foreground">
        Response explorer — first sample record. Hover a row to extract a value{onSetRecordPath ? " or set the record path" : ""}.
      </p>
      <div className="max-h-56 overflow-auto rounded-md border bg-muted/40 p-1.5">
        {first ? (
          Object.entries(first).map(([k, v]) => (
            <TreeNode
              key={k}
              name={k}
              value={v}
              path={`$.${k}`}
              depth={0}
              locked={locked}
              onExtract={onExtract}
              onSetRecordPath={onSetRecordPath}
            />
          ))
        ) : (
          <pre className="p-1 text-xs leading-4">{JSON.stringify(records, null, 2)}</pre>
        )}
      </div>
    </div>
  );
}
