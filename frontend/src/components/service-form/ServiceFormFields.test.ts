import { createElement } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { TooltipProvider } from "@/components/ui/tooltip";
import type { AppService } from "@/prototype/types";
import { ServiceFormFields, buildConfig, emptyForm, formFromService, secretTyped } from "./ServiceFormFields";

vi.mock("@/prototype/api", () => ({
  listGatewayProxies: vi.fn(async () => []),
}));

const queryClient = () =>
  new QueryClient({
    defaultOptions: {
      queries: { retry: false },
      mutations: { retry: false },
    },
  });

afterEach(() => {
  vi.clearAllMocks();
});

describe("ServiceFormFields iceberg catalog helpers", () => {
  it("builds a URL-based Trino database service without requiring a database name", () => {
    const form = {
      ...emptyForm(),
      dialect: "trino" as const,
      dbUrl: "https://trino.datapasc.com",
      dbUsername: "admin",
      driverLocations: "/opt/nifi/trino-jdbc.jar",
    };

    expect(buildConfig("database", form)).toEqual({
      dialect: "trino",
      url: "https://trino.datapasc.com",
      username: "admin",
      driverLocations: "/opt/nifi/trino-jdbc.jar",
      capabilities: ["read"],
    });
  });

  it("renders the S3 access key as a password input", () => {
    const client = queryClient();
    render(
      createElement(
        MemoryRouter,
        null,
        createElement(
          QueryClientProvider,
          { client },
          createElement(
            TooltipProvider,
            { delayDuration: 0 },
            createElement(ServiceFormFields, {
              type: "sink_destination",
              form: { ...emptyForm(), sinkKind: "iceberg_catalog" },
              onChange: vi.fn(),
              editing: true,
            }),
          ),
        ),
      ),
    );

    const label = screen.getByText("S3 access key");
    const input = label.parentElement?.querySelector("input");
    expect(input).not.toBeNull();
    expect(input).toHaveAttribute("type", "password");
  });

  it("round-trips the backend-supported Iceberg service config fields", () => {
    const service: AppService = {
      id: "svc-iceberg",
      type: "sink_destination",
      name: "Iceberg Bronze Catalog",
      revision: 3,
      retired: false,
      health: "Healthy",
      lastTestedAt: null,
      config: {
        kind: "iceberg_catalog",
        catalogUrl: "https://polaris.example/api/catalog",
        warehouse: "bronze",
        oauthClientId: "root",
        s3Endpoint: "https://minio.example",
        s3AccessKey: "ACCESS",
        s3Region: "eu-west-1",
        s3PathStyle: false,
      },
      hasSecret: true,
      createdAt: "2026-08-15T00:00:00.000Z",
      updatedAt: "2026-08-15T00:00:00.000Z",
    };

    const form = formFromService(service);

    expect(form.oauthClientId).toBe("root");
    expect(form.oauthClientSecret).toBe("");
    expect(form.s3Endpoint).toBe("https://minio.example");
    expect(form.s3AccessKey).toBe("ACCESS");
    expect(form.s3SecretKey).toBe("");
    expect(form.s3Region).toBe("eu-west-1");
    expect(form.s3PathStyle).toBe(false);

    expect(buildConfig("sink_destination", form)).toEqual({
      kind: "iceberg_catalog",
      catalogUrl: "https://polaris.example/api/catalog",
      warehouse: "bronze",
      oauthClientId: "root",
      s3Endpoint: "https://minio.example",
      s3AccessKey: "ACCESS",
      s3Region: "eu-west-1",
      s3PathStyle: false,
    });
  });

  it("includes typed Iceberg secrets and preserves the path-style default", () => {
    const form = {
      ...emptyForm(),
      sinkKind: "iceberg_catalog",
      catalogUrl: "https://polaris.example/api/catalog",
      warehouse: "bronze",
      oauthClientId: "root",
      oauthClientSecret: "client-secret",
      s3Endpoint: "https://minio.example",
      s3AccessKey: "ACCESS",
      s3SecretKey: "storage-secret",
      s3Region: "us-east-1",
    };

    expect(form.s3PathStyle).toBe(true);
    expect(secretTyped("sink_destination", form)).toBe(true);
    expect(buildConfig("sink_destination", form)).toEqual({
      kind: "iceberg_catalog",
      catalogUrl: "https://polaris.example/api/catalog",
      warehouse: "bronze",
      oauthClientId: "root",
      oauthClientSecret: "client-secret",
      s3Endpoint: "https://minio.example",
      s3AccessKey: "ACCESS",
      s3SecretKey: "storage-secret",
      s3Region: "us-east-1",
      s3PathStyle: true,
    });
  });
});
