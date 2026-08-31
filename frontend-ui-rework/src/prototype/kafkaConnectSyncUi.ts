import type { KafkaConnectSync } from "./api";
import type { Flow } from "./types";

export type SyncConfigurationState = "draft" | "needs_review" | "synced" | "changes_pending";
export type SyncPrimaryAction = "create" | "apply" | null;

/** The primary configuration action is intentionally absent when nothing is pending. */
export function syncPrimaryAction(sync: Pick<KafkaConnectSync, "remotePresent" | "configurationState" | "retired">): SyncPrimaryAction {
  if (sync.retired) return null;
  if (!sync.remotePresent) return "create";
  if (sync.configurationState === "changes_pending") return "apply";
  return null;
}

export function syncConfigurationLabel(
  sync: Pick<KafkaConnectSync, "remotePresent" | "configurationState">,
): string {
  if (!sync.remotePresent || sync.configurationState === "draft") return "Draft — no connector";
  if (sync.configurationState === "changes_pending") return "Changes pending";
  if (sync.configurationState === "needs_review") return "Needs review";
  return "Synced";
}

export function runtimeControlsAvailable(
  sync: Pick<KafkaConnectSync, "enabled" | "remotePresent" | "configurationState" | "retired">,
): boolean {
  return sync.enabled && sync.remotePresent && !sync.retired && sync.configurationState !== "changes_pending";
}

export function kafkaConnectSyncDeleteImpact(
  sync: Pick<KafkaConnectSync, "id">,
  flows: Flow[],
): { deployed: Flow[]; undeployed: Flow[] } {
  const dependents = flows.filter((flow) =>
    flow.blocks.some((block) => block.config?.syncId === sync.id),
  );
  return {
    deployed: dependents.filter((flow) => Boolean(flow.deployedAt)),
    undeployed: dependents.filter((flow) => !flow.deployedAt),
  };
}
