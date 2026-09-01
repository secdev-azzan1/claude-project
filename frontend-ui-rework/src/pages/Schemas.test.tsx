import { afterEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";

import { TooltipProvider } from "@/components/ui/tooltip";

Object.defineProperty(HTMLElement.prototype, "scrollIntoView", {
  value: vi.fn(),
  writable: true,
});

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

const apiState = vi.hoisted(() => {
  const state = {
    registryVersions: Array.from({ length: 12 }, (_value, index) => ({ version: index + 1 })),
    template: {
      id: "template-1",
      name: "inventory",
      description: "Manual template with missing subject",
      rawAvro: JSON.stringify(
        {
          type: "record",
          name: "inventory_value",
          fields: [{ name: "id", type: "string" }],
        },
        null,
        2,
      ),
      createdAt: "2026-08-15T00:00:00.000Z",
      updatedAt: "2026-08-15T00:00:00.000Z",
      registeredSubject: "inventory-value",
      registeredVersion: 12,
      registeredAt: "2026-08-15T00:00:00.000Z",
    },
    nextGlobalId: 42,
    nextVersion: 13,
    createSchemaTemplate: vi.fn(),
    deleteApprovedSchema: vi.fn(),
    deleteApprovedSchemaVersion: vi.fn(),
    deleteSchemaTemplate: vi.fn(),
    getRegistrySubjectVersion: vi.fn(async (_subject: string, version: number) => ({
      version,
      globalId: 9000 + version,
      avro: {
        type: "record",
        name: "inventory_value",
        fields: [{ name: "id", type: "string" }],
      },
    })),
    listFlows: vi.fn(async () => []),
    listRegistrySubjectVersions: vi.fn(async () => state.registryVersions.map((entry) => ({ ...entry }))),
    listSchemaTemplates: vi.fn(async () => [{ ...state.template }]),
    listSchemas: vi.fn(async () => []),
    registerSchema: vi.fn(async (subject: string, avro: unknown, templateId?: string) => {
      const version = state.nextVersion++;
      const globalId = state.nextGlobalId++;
      state.registryVersions.push({ version });
      if (templateId && state.template.id === templateId) {
        state.template = {
          ...state.template,
          rawAvro: JSON.stringify(avro, null, 2),
          updatedAt: new Date().toISOString(),
          registeredSubject: subject,
          registryGlobalId: globalId,
          registeredVersion: version,
          registeredAt: new Date().toISOString(),
        };
      }
      return { globalId, subject, version };
    }),
    saveApprovedAsTemplate: vi.fn(),
    saveApprovedSchemaDraft: vi.fn(),
    saveSchemaTemplate: vi.fn(async (templateId: string, avro: unknown) => {
      if (state.template.id === templateId) {
        state.template = {
          ...state.template,
          rawAvro: JSON.stringify(avro, null, 2),
          updatedAt: new Date().toISOString(),
        };
      }
      return { ...state.template };
    }),
    stageCeremonyDraft: vi.fn(),
    verifySchema: vi.fn(async () => ({
      issues: [],
      compatibility: { checked: false, compatible: null, message: "" },
    })),
  };
  return state;
});

vi.mock("@/prototype/api", () => apiState);

import Schemas from "@/pages/Schemas";

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
          <Schemas />
        </TooltipProvider>
      </QueryClientProvider>
    </MemoryRouter>,
  );
}

afterEach(() => {
  apiState.registryVersions = Array.from({ length: 12 }, (_value, index) => ({ version: index + 1 }));
  apiState.template = {
    id: "template-1",
    name: "inventory",
    description: "Manual template with missing subject",
    rawAvro: JSON.stringify(
      {
        type: "record",
        name: "inventory_value",
        fields: [{ name: "id", type: "string" }],
      },
      null,
      2,
    ),
    createdAt: "2026-08-15T00:00:00.000Z",
    updatedAt: "2026-08-15T00:00:00.000Z",
    registeredSubject: "inventory-value",
    registeredVersion: 12,
    registeredAt: "2026-08-15T00:00:00.000Z",
  };
  apiState.nextGlobalId = 42;
  apiState.nextVersion = 13;
  vi.clearAllMocks();
});

describe("Schemas", () => {
  it("treats a template with only registeredSubject as registered and loads registry versions", async () => {
    renderPage();

    const templateRow = await screen.findByRole("button", { name: /inventory/i });
    fireEvent.click(templateRow);

    expect(await screen.findByText("inventory-value")).toBeInTheDocument();
    expect(screen.getAllByRole("combobox")[0]).toHaveTextContent("v12");
    expect(apiState.listRegistrySubjectVersions).toHaveBeenCalledWith("inventory-value");
  });

  it("shows a dedicated key and Docs editor for each schema field", async () => {
    renderPage();

    const templateRow = await screen.findByRole("button", { name: /inventory/i });
    fireEvent.click(templateRow);

    expect(await screen.findByText("Schema fields")).toBeInTheDocument();
    expect(screen.getByText("Key 1")).toBeInTheDocument();
    expect(screen.getByText("Key name")).toBeInTheDocument();
    expect(screen.getByText("Docs")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /Docs Add a description/i }));
    expect(screen.getByRole("textbox", { name: "Field documentation" })).toBeInTheDocument();
  });

  it("refreshes the current badge and version picker after editing and re-registering", async () => {
    renderPage();

    const templateRow = await screen.findByRole("button", { name: /inventory/i });
    fireEvent.click(templateRow);

    fireEvent.change(screen.getByDisplayValue("inventory_value"), {
      target: {
        value: "inventory_value_v2",
      },
    });

    fireEvent.click(screen.getByRole("button", { name: "Register…" }));
    expect(screen.getByDisplayValue("inventory-value")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Register" }));

    await waitFor(() =>
      expect(apiState.registerSchema).toHaveBeenCalledWith(
        "inventory-value",
        expect.objectContaining({
          type: "record",
          name: "inventory_value_v2",
          fields: [{ name: "id", type: "string" }],
        }),
        "template-1",
      ),
    );

    expect(
      screen.getAllByText(
        (_, element) =>
          element?.textContent?.includes("Registered") &&
          element?.textContent?.includes("#42") &&
          element?.textContent?.includes("v13"),
      ).length,
    ).toBeGreaterThan(0);
    expect(screen.getAllByRole("combobox")[0]).toHaveTextContent("v13");
    fireEvent.click(screen.getAllByRole("combobox")[0]);
    expect(await screen.findByRole("option", { name: /v13.*current/i })).toBeInTheDocument();
  });
});
