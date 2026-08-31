import type { ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

import type { Flow } from "@/prototype/types";
import { TooltipProvider } from "@/components/ui/tooltip";
import { Sheet } from "@/components/ui/sheet";
import Flows, { FlowDetailSheet } from "@/pages/Flows";

vi.mock("@/components/AppLayout", () => ({
  AppLayout: ({
    title,
    description,
    actions,
    children,
  }: {
    title: string;
    description?: string;
    actions?: ReactNode;
    children: ReactNode;
  }) => (
    <div>
      <h1>{title}</h1>
      {description && <p>{description}</p>}
      {actions}
      {children}
    </div>
  ),
}));

const apiMocks = vi.hoisted(() => ({
  clearDedupCache: vi.fn(async () => ({ cleared: true })),
  clearFlowTopic: vi.fn(async (flowId: string, topic: string) => ({ before: 0, topic, flowId })),
  forceRepairRuntime: vi.fn(),
  getDlq: vi.fn(async () => []),
  getFlowRuntime: vi.fn(async () => null),
  getMetrics: vi.fn(async () => null),
  getTopicMessages: vi.fn(async () => []),
  getVerbBlockReason: vi.fn(() => null),
  importConnectorFlow: vi.fn(),
  listConnections: vi.fn(async () => []),
  listConnectors: vi.fn(async () => []),
  listFlows: vi.fn(async () => []),
  listKafkaConnectSyncs: vi.fn(async () => []),
  listSchemas: vi.fn(async () => []),
  listServices: vi.fn(async () => []),
  publishConnector: vi.fn(),
  refreshFlowRuntime: vi.fn(),
  runFlowVerb: vi.fn(),
  serviceUpdateAvailable: vi.fn(() => [] as string[]),
  setFlowEnabled: vi.fn(),
  validateFlowNow: vi.fn(() => []),
  // Bulk background jobs. No job is in flight in these fixtures, so the
  // progress panel never renders; the two pure helpers keep their real
  // behaviour because the component branches on them.
  getActiveBulkJob: vi.fn(async () => null),
  getBulkJob: vi.fn(async () => null),
  getBulkQueue: vi.fn(async () => []),
  startBulkJob: vi.fn(async () => "bulk-test"),
  cancelBulkJob: vi.fn(),
  isBulkJobTerminal: vi.fn((job?: { status?: string } | null) =>
    !!job && ["completed", "failed", "cancelled", "interrupted"].includes(job.status ?? ""),
  ),
  bulkJobPercent: vi.fn((job?: { total?: number; completed?: number } | null) =>
    !job || !job.total ? 0 : Math.round(((job.completed ?? 0) / job.total) * 100),
  ),
  // Cascade delete: no associated resources in these fixtures.
  flowCascadeTargets: vi.fn(() => []),
  deleteCascadeTarget: vi.fn(),
}));

vi.mock("@/prototype/api", () => apiMocks);

const queryClient = () =>
  new QueryClient({
    defaultOptions: {
      queries: { retry: false },
      mutations: { retry: false },
    },
  });

const sampleFlow: Flow = {
  id: "flow-alpha",
  name: "alpha-flow",
  description: "Fixture flow for overview regression",
  state: "Stopped",
  enabled: true,
  cron: null,
  blocks: [
    {
      id: "block-1",
      adapter: "kafka",
      mode: "write",
      name: "publish_asset",
      parentId: null,
      entity: "asset",
      config: {},
      transforms: [],
    },
  ],
  topics: [
    {
      id: "topic-1",
      kind: "materialized",
      name: "bronze.alpha.asset",
      sealed: false,
      writerBlockId: "block-1",
    },
  ],
  variables: [],
  servicePins: {},
  drift: null,
  deployedAt: null,
  lastRunAt: null,
  createdAt: "2026-08-15T00:00:00.000Z",
  updatedAt: "2026-08-15T00:00:00.000Z",
};

function renderPage() {
  const client = queryClient();
  return render(
    <MemoryRouter>
      <QueryClientProvider client={client}>
        <TooltipProvider delayDuration={0}>
          <Flows />
        </TooltipProvider>
      </QueryClientProvider>
    </MemoryRouter>,
  );
}

afterEach(() => {
  vi.clearAllMocks();
});

describe("Flows", () => {
  const activateTab = (name: "Messages" | "DLQ") => {
    const tab = screen.getByRole("tab", { name });
    fireEvent.mouseDown(tab);
    fireEvent.click(tab);
  };

  it("keeps the overview sheet closed until the explicit eye action is used", async () => {
    apiMocks.listFlows.mockResolvedValueOnce([sampleFlow]);

    renderPage();

    await screen.findByRole("button", { name: "Overview" });
    expect(screen.queryByRole("tab", { name: "Overview" })).not.toBeInTheDocument();

    fireEvent.click(screen.getByText("alpha-flow"));
    expect(screen.queryByRole("tab", { name: "Overview" })).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Overview" }));
    expect(await screen.findByRole("tab", { name: "Overview" })).toBeInTheDocument();
  });

  it("shows Clear topic in Messages and Clear DLQ in DLQ", async () => {
    const client = queryClient();
    render(
      <QueryClientProvider client={client}>
        <TooltipProvider delayDuration={0}>
          <Sheet open>
            <FlowDetailSheet
              flow={sampleFlow}
              services={[]}
              schemas={[]}
              connections={[]}
              pendingVerb={undefined}
              onVerb={vi.fn()}
              onToggleEnabled={vi.fn()}
              onEdit={vi.fn()}
              onSaveConnector={vi.fn()}
              enableBusy={false}
            />
          </Sheet>
        </TooltipProvider>
      </QueryClientProvider>,
    );

    await screen.findByRole("tab", { name: "Overview" });

    activateTab("Messages");
    expect(await screen.findByRole("button", { name: "Clear topic" })).toBeInTheDocument();

    activateTab("DLQ");
    expect(await screen.findByRole("button", { name: "Clear DLQ" })).toBeInTheDocument();
  });
});
