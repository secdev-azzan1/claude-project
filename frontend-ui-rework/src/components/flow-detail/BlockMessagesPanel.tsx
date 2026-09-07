import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { Eraser, RefreshCw } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { EmptyState } from "@/components/ui/empty-state";
import { getTopicMessages, clearFlowTopic } from "@/prototype/api";
import { deriveTopicName } from "@/prototype/naming";
import { timeAgo } from "@/lib/api";
import type { Flow, FlowBlock } from "@/prototype/types";

function topicForBlock(flow: Flow, block: FlowBlock): string | null {
  const configuredTopic = typeof block.config.topicName === "string" ? block.config.topicName.trim() : "";
  const attachedTopicId = typeof block.config.attachTopicId === "string" ? block.config.attachTopicId : null;
  const attachedTopic = attachedTopicId ? flow.topics.find((topic) => topic.id === attachedTopicId) : undefined;
  const parentTopic = flow.topics.find((topic) => topic.id === block.parentId);
  const ownedTopic = flow.topics.find((topic) => topic.writerBlockId === block.id);

  if (attachedTopic?.name) return attachedTopic.name;
  if (parentTopic?.name) return parentTopic.name;
  if (ownedTopic?.name) return ownedTopic.name;
  if (configuredTopic) return configuredTopic;
  if (block.adapter === "kafka" || block.adapter === "kafka_kc") return deriveTopicName(flow, block).value || null;
  return null;
}

export function BlockMessagesPanel({ flow, block }: { flow: Flow; block: FlowBlock }): JSX.Element {
  const queryClient = useQueryClient();
  const topic = topicForBlock(flow, block);
  const [clearOpen, setClearOpen] = useState(false);

  const messagesQuery = useQuery({
    queryKey: ["topic-messages", flow.id, topic],
    queryFn: () => getTopicMessages(flow.id, topic!),
    enabled: Boolean(topic),
  });

  const clearMutation = useMutation({
    mutationFn: () => clearFlowTopic(flow.id, topic!),
    onSuccess: (result) => {
      queryClient.invalidateQueries({ queryKey: ["topic-messages", flow.id, topic] });
      queryClient.invalidateQueries({ queryKey: ["flow-metrics", flow.id] });
      queryClient.invalidateQueries({ queryKey: ["audit"] });
      setClearOpen(false);
      toast.success(`Cleared ${result.before} message(s) from ${result.topic}`);
    },
    onError: (error: Error) => toast.error("Could not clear the topic", { description: error.message }),
  });

  const messages = useMemo(
    () => [...(messagesQuery.data ?? [])].sort((a, b) => b.offset - a.offset).slice(0, 50),
    [messagesQuery.data],
  );

  if (!topic) {
    return <EmptyState inline>This block is not connected to a readable Kafka topic.</EmptyState>;
  }

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <div className="text-sm font-medium">Topic messages</div>
          <p className="mt-0.5 break-all font-mono text-xs text-muted-foreground">{topic}</p>
        </div>
        <Button
          size="sm"
          variant="outline"
          className="text-destructive hover:text-destructive"
          disabled={clearMutation.isPending}
          onClick={() => setClearOpen(true)}
        >
          {clearMutation.isPending ? (
            <RefreshCw className="mr-1.5 h-3.5 w-3.5 animate-spin" />
          ) : (
            <Eraser className="mr-1.5 h-3.5 w-3.5" />
          )}
          Clear topic
        </Button>
      </div>
      <p className="text-xs text-muted-foreground">
        Group-less viewer — nothing is committed. Avro payloads are not decoded here. Newest first, capped at 50.
      </p>
      {messagesQuery.isLoading ? (
        <div className="p-6 text-center text-sm text-muted-foreground">Loading messages…</div>
      ) : messages.length === 0 ? (
        <div className="rounded-md border p-8 text-center text-sm text-muted-foreground">
          No messages readable on <code>{topic}</code>.
        </div>
      ) : (
        <div className="space-y-1.5">
          {messages.map((message) => (
            <div key={message.offset} className="rounded-md border p-2 font-mono text-xs">
              <div className="flex flex-wrap gap-x-3 text-muted-foreground">
                <span>offset {message.offset}</span>
                <span>{timeAgo(message.ts)}</span>
                <span>key {message.key ?? "—"}</span>
              </div>
              <div className="mt-1 break-all">
                {message.value !== null ? message.value : (
                  <span className="font-sans italic text-muted-foreground">binary payload ({message.bytes} bytes)</span>
                )}
              </div>
            </div>
          ))}
        </div>
      )}

      <AlertDialog open={clearOpen} onOpenChange={setClearOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Clear all retained messages from this topic?</AlertDialogTitle>
            <AlertDialogDescription>This cannot be undone. The action is audited.</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              disabled={clearMutation.isPending}
              onClick={() => clearMutation.mutate()}
            >
              Clear topic
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
