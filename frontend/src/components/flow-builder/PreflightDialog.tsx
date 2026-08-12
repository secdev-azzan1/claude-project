// Deploy preflight — the single named gate. Blocks with specific reasons;
// Deploy is enabled only when every check passes.

import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { getPreflight, type PreflightCheck } from "@/prototype/api";
import type { Flow } from "@/prototype/types";
import { CheckCircle2, Loader2, XCircle } from "lucide-react";

export function PreflightDialog({
  flow,
  open,
  onOpenChange,
  onDeploy,
  deploying,
}: {
  flow: Flow;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onDeploy: () => void;
  deploying: boolean;
}) {
  const [checks, setChecks] = useState<PreflightCheck[] | null>(null);

  useEffect(() => {
    if (open) {
      setChecks(null);
      void getPreflight(flow).then(setChecks);
    }
  }, [open, flow]);

  const allOk = !!checks && checks.every((c) => c.ok);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle>Deploy preflight — {flow.name}</DialogTitle>
          <DialogDescription>Deploy builds the flow stopped. Every check must pass; refusals name their reason.</DialogDescription>
        </DialogHeader>
        {!checks ? (
          <div className="flex items-center gap-2 py-6 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" /> Running preflight checks…
          </div>
        ) : (
          <ul className="space-y-1.5 py-1">
            {checks.map((c, i) => (
              <li key={i} className="flex items-start gap-2 text-sm">
                {c.ok ? (
                  <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0 text-success" />
                ) : (
                  <XCircle className="mt-0.5 h-4 w-4 shrink-0 text-destructive" />
                )}
                <span>
                  <span className="font-medium">{c.label}</span>
                  <span className="block text-xs text-muted-foreground">{c.detail}</span>
                </span>
              </li>
            ))}
          </ul>
        )}
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button onClick={onDeploy} disabled={!allOk || deploying} className="gap-1.5" title={allOk ? undefined : "Resolve the failing checks first"}>
            {deploying && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
            Deploy
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
