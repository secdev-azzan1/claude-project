import type { KafkaConnectSync } from "./api";
import type { ConnectRunState, Flow } from "./types";

/**
 * Which runtime verbs are legal for one sink, given its LIVE state from
 * `GET /flows/{flowId}/sink-status` — not the sync record's persisted
 * `remotePresent`/`configurationState` flags this used to key off of. Those
 * flags reflect what was last configured/applied, which is exactly the stale
 * signal the live sink-status endpoint replaced: a sink can fail on the
 * cluster and a persisted flag would keep saying it's fine.
 *
 * `state: null` means the cluster could not be reached (see
 * `FlowSinkStatusResponse.reachable`) — offering Start there could create a
 * duplicate of a connector that's simply unreachable right now, so nothing
 * is offered.
 */
export type SinkVerb = "start" | "stop" | "pause" | "resume" | "restart";

export function sinkVerbsAvailable(state: ConnectRunState | null): SinkVerb[] {
  switch (state) {
    case "UNDEPLOYED":
      return ["start"];
    case "RUNNING":
      return ["pause", "stop", "restart"];
    case "PAUSED":
      return ["resume", "stop", "restart"];
    case "STOPPED":
      return ["start", "restart"];
    case "FAILED":
    case "UNASSIGNED":
    case "RESTARTING":
      return ["stop", "restart"];
    case null:
      return [];
    default:
      return [];
  }
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
