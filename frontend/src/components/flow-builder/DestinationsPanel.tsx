// The Destinations panel — every topic in the flow with its attached sinks:
// "the dashed edges, as a list."

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import type { Flow } from "@/prototype/types";
import { Cable, Lock, Radio } from "lucide-react";

export function DestinationsPanel({ flow, onSelect }: { flow: Flow; onSelect: (id: string) => void }) {
  if (flow.topics.length === 0) return null;
  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-base">Destinations</CardTitle>
        <CardDescription>Every topic this flow touches, with its attached sink subscriptions.</CardDescription>
      </CardHeader>
      <CardContent className="space-y-2">
        {flow.topics.map((topic) => {
          const writer = flow.blocks.find((b) => b.id === topic.writerBlockId);
          const sinks = flow.blocks.filter((b) => b.adapter === "kc" && b.config.attachTopicId === topic.id);
          return (
            <div key={topic.id} className="flex flex-wrap items-center gap-2 rounded-md border px-3 py-2">
              <button type="button" className="flex items-center gap-1.5 hover:underline" onClick={() => onSelect(topic.id)}>
                {topic.sealed ? <Lock className="h-3.5 w-3.5 text-muted-foreground" /> : <Radio className="h-3.5 w-3.5 text-muted-foreground" />}
                <span className="font-mono text-xs">{topic.name}</span>
              </button>
              {topic.kind === "adopted" && (
                <Badge variant="outline" className="text-xs">
                  adopted · sampled, never renamed
                </Badge>
              )}
              {topic.sealed && (
                <Badge variant="outline" className="text-xs" title="kafka+connect topics are managed with their sink as one unit">
                  sealed
                </Badge>
              )}
              {writer && <span className="text-xs text-muted-foreground">written by {writer.name}</span>}
              {typeof topic.backlogEstimate === "number" && (
                <span className="text-xs text-muted-foreground">~{topic.backlogEstimate.toLocaleString()} messages</span>
              )}
              <span className="ml-auto flex items-center gap-1.5">
                {sinks.length === 0 && !topic.sealed && <span className="text-xs text-muted-foreground">no subscriptions</span>}
                {sinks.map((s) => (
                  <button key={s.id} type="button" onClick={() => onSelect(s.id)}>
                    <Badge variant="outline" className="gap-1 border-dashed text-xs hover:bg-accent">
                      <Cable className="h-3 w-3" /> {s.name}
                    </Badge>
                  </button>
                ))}
              </span>
            </div>
          );
        })}
      </CardContent>
    </Card>
  );
}
