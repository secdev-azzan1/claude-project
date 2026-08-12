// The flow-level form: identity, cron trigger (R1), DLQ preview and the
// validation summary.
//
// Flow variables were removed here as well as globally: two places to define a
// value, neither of them where the value is used, is worse than typing it into
// the field that needs it.

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { cronPreview, CRON_PRESETS, dlqName, isValidCron, tokenize } from "@/prototype/naming";
import { flowHasTrigger, rootBlock } from "@/prototype/legality";
import type { ValidationIssue } from "@/prototype/validation";
import type { Flow } from "@/prototype/types";
import { AlertCircle, Clock } from "lucide-react";

export interface FlowSettingsFormProps {
  flow: Flow;
  locked: boolean;
  issues: ValidationIssue[];
  onPatch: (patch: Partial<Flow>) => void;
  onSelectBlock: (blockId: string) => void;
}

export function FlowSettingsForm({ flow, locked, issues, onPatch, onSelectBlock }: FlowSettingsFormProps) {
  const hasTrigger = flowHasTrigger(flow);
  const root = rootBlock(flow);
  const nameLocked = locked || !!flow.deployedAt;
  const preview = cronPreview(flow.cron);

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Flow identity</CardTitle>
          <CardDescription>
            The name is the source name — the first half of every derived topic, table and DLQ name. It freezes at deploy.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="grid gap-1.5">
            <Label>Name</Label>
            <Input
              value={flow.name}
              disabled={nameLocked}
              onChange={(e) => onPatch({ name: e.target.value })}
              className="max-w-sm"
              title={flow.deployedAt ? "Names freeze at deploy" : undefined}
            />
            <p className="text-xs text-muted-foreground">
              token: <code className="font-mono">{tokenize(flow.name) || "—"}</code> · DLQ:{" "}
              <code className="font-mono">{dlqName(flow.name)}</code> (derived, 3 retries then here, 7-day retention)
            </p>
          </div>
          <div className="grid gap-1.5">
            <Label>Description</Label>
            <Textarea
              value={flow.description ?? ""}
              disabled={locked}
              rows={2}
              onChange={(e) => onPatch({ description: e.target.value })}
            />
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="flex items-center gap-2 text-base">
            <Clock className="h-4 w-4" /> Trigger
          </CardTitle>
          <CardDescription>
            One root, one schedule (R1). Cron is the only trigger type — 5-field, UTC.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          {hasTrigger ? (
            <>
              <div className="flex flex-wrap items-center gap-2">
                <Select
                  value={CRON_PRESETS.find((p) => p.value === flow.cron)?.value ?? "custom"}
                  disabled={locked}
                  onValueChange={(v) => v !== "custom" && onPatch({ cron: v })}
                >
                  <SelectTrigger className="w-56">
                    <SelectValue placeholder="Preset" />
                  </SelectTrigger>
                  <SelectContent>
                    {CRON_PRESETS.map((p) => (
                      <SelectItem key={p.value} value={p.value}>
                        {p.label}
                      </SelectItem>
                    ))}
                    <SelectItem value="custom">Custom…</SelectItem>
                  </SelectContent>
                </Select>
                <Input
                  className="w-44 font-mono"
                  value={flow.cron ?? ""}
                  disabled={locked}
                  placeholder="*/15 * * * *"
                  onChange={(e) => onPatch({ cron: e.target.value })}
                />
                {!isValidCron(flow.cron) && (
                  <span className="flex items-center gap-1 text-xs text-destructive">
                    <AlertCircle className="h-3.5 w-3.5" /> 5 fields required
                  </span>
                )}
              </div>
              {preview.length > 0 && (
                <p className="text-xs text-muted-foreground">
                  Next: {preview.join(" · ")} — overlapping occurrences are skipped and counted.
                </p>
              )}
              {root && (
                <p className="text-xs text-muted-foreground">
                  The trigger lives on the first runnable block: <span className="font-medium">{root.name}</span>.
                </p>
              )}
            </>
          ) : (
            <p className="text-sm text-muted-foreground">
              {root
                ? "This flow is rooted by a Kafka consumer — it runs continuously, there is no schedule."
                : flow.topics.some((t) => t.kind === "adopted")
                  ? "Topic-rooted flow with only sink subscriptions — no trigger of any kind."
                  : "Add a root block first; http and jdbc roots get a cron trigger."}
            </p>
          )}
        </CardContent>
      </Card>

      <Card className={issues.length > 0 ? "border-destructive/40" : "border-success/40"}>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">Validation</CardTitle>
          <CardDescription>
            {issues.length === 0 ? "Everything checks out." : `${issues.length} issue(s) block Deploy.`}
          </CardDescription>
        </CardHeader>
        {issues.length > 0 && (
          <CardContent className="space-y-1">
            {issues.map((issue, i) => (
              <button
                key={i}
                type="button"
                className="flex w-full items-start gap-2 rounded-md px-2 py-1.5 text-left text-xs hover:bg-muted"
                onClick={() => issue.blockId && onSelectBlock(issue.blockId)}
              >
                <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-destructive" />
                <span>
                  <span className="font-medium">{issue.where}: </span>
                  {issue.message}
                </span>
              </button>
            ))}
          </CardContent>
        )}
      </Card>
    </div>
  );
}
