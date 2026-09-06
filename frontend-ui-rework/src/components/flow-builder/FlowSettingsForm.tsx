// The flow-level form: identity, cron trigger (R1) and DLQ preview.
//
// Flow variables were removed here as well as globally: two places to define a
// value, neither of them where the value is used, is worse than typing it into
// the field that needs it.

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { Field, FieldGroup, FactRow, Mono } from "@/components/form/Field";
import { cronPreview, CRON_PRESETS, dlqName, isValidCron, tokenize } from "@/prototype/naming";
import { flowHasTrigger, rootBlock } from "@/prototype/legality";
import type { Flow } from "@/prototype/types";
import { Clock } from "lucide-react";

export interface FlowSettingsFormProps {
  flow: Flow;
  locked: boolean;
  onPatch: (patch: Partial<Flow>) => void;
}

export function FlowSettingsForm({ flow, locked, onPatch }: FlowSettingsFormProps) {
  const hasTrigger = flowHasTrigger(flow);
  const root = rootBlock(flow);
  const nameLocked = locked || !!flow.deployedAt;
  const preview = cronPreview(flow.cron);

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader className="pb-4">
          <CardTitle>Flow identity</CardTitle>
          <CardDescription>
            The name is the source name — the first half of every derived topic, table and DLQ name. It freezes at deploy.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <FieldGroup>
            <Field
              label="Name"
              className="max-w-sm"
              hint={
                <div className="space-y-1">
                  <FactRow label="token">
                    <Mono>{tokenize(flow.name) || "—"}</Mono>
                  </FactRow>
                  <FactRow label="DLQ">
                    <Mono>{dlqName(flow.name)}</Mono>
                  </FactRow>
                </div>
              }
              info="The DLQ name is derived from the flow name: 3 retries, then here, with 7-day retention."
            >
              <Input
                value={flow.name}
                disabled={nameLocked}
                onChange={(e) => onPatch({ name: e.target.value })}
                title={flow.deployedAt ? "Names freeze at deploy" : undefined}
              />
            </Field>

            <Field label="Description">
              <Textarea
                value={flow.description ?? ""}
                disabled={locked}
                rows={2}
                onChange={(e) => onPatch({ description: e.target.value })}
              />
            </Field>
          </FieldGroup>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="pb-4">
          <CardTitle className="flex items-center gap-2">
            <Clock className="h-4 w-4 text-muted-foreground" /> Trigger
          </CardTitle>
          <CardDescription>One root, one schedule (R1). Cron is the only trigger type — 5-field, UTC.</CardDescription>
        </CardHeader>
        <CardContent>
          {hasTrigger ? (
            <FieldGroup>
              <Field
                label="Schedule"
                hint={
                  preview.length > 0 ? (
                    <>Next: {preview.join(" · ")} — overlapping occurrences are skipped and counted.</>
                  ) : undefined
                }
                error={!isValidCron(flow.cron) ? "5 fields required." : undefined}
              >
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
                    className="w-44 font-mono text-xs"
                    value={flow.cron ?? ""}
                    disabled={locked}
                    placeholder="*/15 * * * *"
                    onChange={(e) => onPatch({ cron: e.target.value })}
                  />
                </div>
              </Field>

              {root && (
                <FactRow label="Trigger lives on">
                  <span className="font-medium">{root.name}</span>
                </FactRow>
              )}
            </FieldGroup>
          ) : (
            <p className="text-sm leading-relaxed text-muted-foreground">
              {root
                ? "This flow is rooted by a Kafka consumer — it runs continuously, there is no schedule."
                : flow.topics.some((t) => t.kind === "adopted")
                  ? "Topic-rooted flow with only sink subscriptions — no trigger of any kind."
                  : "Add a root block first; http and jdbc roots get a cron trigger."}
            </p>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
