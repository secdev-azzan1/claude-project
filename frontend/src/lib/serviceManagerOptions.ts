import type { ApplicationServiceType } from "@/lib/applicationServicesApi";
import type { NifiControllerServiceType } from "@/lib/nifiServicesApi";

export const HTTP_SERVICE_OPTION_ID = "application:http";

export type ServiceManagerCreateOption =
  | {
      id: string;
      source: "nifi";
      label: string;
      description?: string;
      nifiType: string;
    }
  | {
      id: typeof HTTP_SERVICE_OPTION_ID;
      source: "application";
      label: "HTTP Service";
      description: string;
      applicationType: Extract<ApplicationServiceType, "REST API">;
    };

const serviceTypeName = (type: string) => type.split(".").pop() || type;

export function buildServiceManagerCreateOptions(types: NifiControllerServiceType[]): ServiceManagerCreateOption[] {
  return [
    ...types.map((typeInfo) => ({
      id: `nifi:${typeInfo.type}`,
      source: "nifi" as const,
      label: serviceTypeName(typeInfo.type),
      description: typeInfo.description,
      nifiType: typeInfo.type,
    })),
    {
      id: HTTP_SERVICE_OPTION_ID,
      source: "application",
      label: "HTTP Service",
      description: "Reusable HTTP base URL, authentication, timeout, and rate-limit profile for REST sources.",
      applicationType: "REST API",
    },
  ];
}
