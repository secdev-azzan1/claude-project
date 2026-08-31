// The Destinations panel — every topic in the flow with its attached sinks:
// "the dashed edges, as a list."

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import type { Flow } from "@/prototype/types";
import { Cable, Lock, Radio } from "lucide-react";

export function DestinationsPanel({ flow, onSelect }: { flow: Flow; onSelect: (id: string) => void }) {
  if (flow.topics.length === 0) return null;
  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle>Destinations</CardTitle>
        <CardDescription>Every topic this flow touches, with its attached sink subscriptions.</CardDescription>
      </CardHeader>
      <CardContent className="space-y-1.5">
        {flow.topics.map((topic) => {
          const writer = flow.blocks.find((b) => b.id === topic.writerBlockId);
          const sinks = flow.blocks.filter((b) => b.adapter === "kc" && b.config.attachTopicId === topic.id);
          return (
            <div key={topic.id} className="rounded-lg bg-muted/40 px-3 py-2.5 ring-1 ring-inset ring-border/50">
              <div className="flex flex-wrap items-center gap-2">
                <button
                  type="button"
                  className="flex min-w-0 items-center gap-1.5 rounded text-left hover:underline"
                  onClick={() => onSelect(topic.id)}
                >
                  {topic.sealed ? (
                    <Lock className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                  ) : (
                    <Radio className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                  )}
                  <span className="truncate font-mono text-xs font-medium">{topic.name}</span>
                </button>

                {/* Was a badge reading "adopted · sampled, never renamed" — the
                    label is "adopted"; the rest is what a tooltip is for. */}
                {topic.kind === "adopted" && (
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <Badge variant="info" className="cursor-help">
                        adopted
                      </Badge>
                    </TooltipTrigger>
                    <TooltipContent>Sampled by the platform, never renamed.</TooltipContent>
                  </Tooltip>
                )}
                {topic.sealed && (
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <Badge variant="muted" className="cursor-help">
                        sealed
                      </Badge>
                    </TooltipTrigger>
                    <TooltipContent>kafka+connect topics are managed with their sink as one unit.</TooltipContent>
                  </Tooltip>
                )}

                <span className="ml-auto flex flex-wrap items-center gap-1.5">
                  {sinks.length === 0 && !topic.sealed && (
                    <span className="text-2xs text-muted-foreground">no subscriptions</span>
                  )}
                  {sinks.map((s) => (
                    <button key={s.id} type="button" onClick={() => onSelect(s.id)}>
                      <Badge variant="outline" className="transition-colors hover:bg-accent hover:text-foreground">
                        <Cable /> {s.name}
                      </Badge>
                    </button>
                  ))}
                </span>
              </div>

              {/* Writer and backlog are metadata about the topic, so they sit on
                  their own quiet line rather than being glued onto the name row. */}
              {(writer || typeof topic.backlogEstimate === "number") && (
                <p className="mt-1 pl-5 text-2xs text-muted-foreground">
                  {writer && <>written by {writer.name}</>}
                  {writer && typeof topic.backlogEstimate === "number" && " · "}
                  {typeof topic.backlogEstimate === "number" && <>~{topic.backlogEstimate.toLocaleString()} messages</>}
                </p>
              )}
            </div>
          );
        })}
      </CardContent>
    </Card>
  );
}
