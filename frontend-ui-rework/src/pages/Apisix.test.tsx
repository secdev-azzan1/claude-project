import { afterEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";

import { TooltipProvider } from "@/components/ui/tooltip";

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
  deleteGatewayProxy: vi.fn(),
  getGatewayResources: vi.fn(async () => ({ certProfiles: [], allowlist: [] })),
  listConnections: vi.fn(async () => []),
  listFlows: vi.fn(async () => []),
  listGatewayProxies: vi.fn(async () => []),
  proxyDependents: vi.fn(() => []),
  reconcileGatewayProxy: vi.fn(),
  saveGatewayProxy: vi.fn(),
  testGatewayProxy: vi.fn(),
  updateGatewayResources: vi.fn(),
}));

vi.mock("@/prototype/api", () => apiMocks);

import Apisix from "@/pages/Apisix";

const queryClient = () =>
  new QueryClient({
    defaultOptions: {
      queries: { retry: false },
      mutations: { retry: false },
    },
  });

function renderPage() {
  const client = queryClient();
  return render(
    <MemoryRouter>
      <QueryClientProvider client={client}>
        <TooltipProvider delayDuration={0}>
          <Apisix />
        </TooltipProvider>
      </QueryClientProvider>
    </MemoryRouter>,
  );
}

afterEach(() => {
  vi.clearAllMocks();
});

describe("Apisix", () => {
  it("renders the HTTP Proxies page title", async () => {
    renderPage();

    expect(await screen.findByRole("heading", { level: 1, name: "HTTP Proxies" })).toBeInTheDocument();
  });
});
