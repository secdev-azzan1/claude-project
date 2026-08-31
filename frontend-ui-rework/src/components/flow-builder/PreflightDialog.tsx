// Deploy preflight — the single named diagnostic. Failed checks are recorded
// against this flow when its queue turn arrives; they do not block later jobs.

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
import { cn } from "@/lib/utils";
import type { Flow } from "@/prototype/types";
import { Check, Loader2, X } from "lucide-react";

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

  const failed = checks?.filter((c) => !c.ok).length ?? 0;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle>Deploy preflight</DialogTitle>
          <DialogDescription>
            Deploy builds <span className="font-medium text-foreground">{flow.name}</span> stopped. Every check must
            pass for this flow to deploy. You can still queue it now; a failed check will be recorded on this flow and
            the next queued flow will continue.
          </DialogDescription>
        </DialogHeader>

        {!checks ? (
          <div className="flex items-center gap-2 py-8 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" /> Running preflight checks…
          </div>
        ) : (
          <ul className="space-y-0.5">
            {checks.map((c, i) => (
              <li
                key={i}
                className={cn(
                  "flex items-start gap-2.5 rounded-lg px-2.5 py-2",
                  !c.ok && "bg-destructive-muted",
                )}
              >
                {/* A filled circle rather than a full-size icon: a passing list
                    of eight checks should read as a calm column of ticks, not
                    eight green badges. */}
                <span
                  className={cn(
                    "mt-0.5 flex h-4 w-4 shrink-0 items-center justify-center rounded-full",
                    c.ok ? "bg-success/15 text-success" : "bg-destructive text-destructive-foreground",
                  )}
                >
                  {c.ok ? <Check className="h-3 w-3" strokeWidth={3} /> : <X className="h-3 w-3" strokeWidth={3} />}
                </span>
                <span className="min-w-0">
                  <span className={cn("text-sm font-medium", !c.ok && "text-destructive")}>{c.label}</span>
                  <span className="block text-xs leading-relaxed text-muted-foreground">{c.detail}</span>
                </span>
              </li>
            ))}
          </ul>
        )}

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button
            onClick={onDeploy}
            disabled={!checks || deploying}
            title={failed > 0 ? `${failed} check${failed === 1 ? "" : "s"} will be recorded when this flow runs` : undefined}
          >
            {deploying && <Loader2 className="animate-spin" />}
            Queue deploy
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
