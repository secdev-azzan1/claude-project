import { useMemo, useState, type Dispatch, type SetStateAction } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AppLayout } from "@/components/AppLayout";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { AlertCircle, Check, Database, FileCode2, Globe2, KeyRound, Loader2, Plug, Plus, RefreshCw, Settings2, ShieldCheck, Star, Trash2 } from "lucide-react";
import { toast } from "sonner";
import { StatusBadge } from "@/components/StatusBadge";
import { timeAgo } from "@/lib/api";
import {
  applicationServicesApi,
  type ApplicationService,
  type ApplicationServicePayload,
} from "@/lib/applicationServicesApi";
import {
  nifiServicesApi,
  type NifiServiceConfig,
  type NifiServiceKind,
} from "@/lib/nifiServicesApi";
import {
  buildServiceManagerCreateOptions,
  HTTP_SERVICE_OPTION_ID,
  type ServiceManagerCreateOption,
} from "@/lib/serviceManagerOptions";
import { useDebouncedValue } from "@/lib/useDebouncedValue";

const EMPTY_SELECT_VALUE = "__nifi_empty_value__";

const KIND_SHORT_LABEL: Record<NifiServiceKind, string> = {
  kafka: "Kafka",
  schema_registry: "Schema Registry",
  mongodb: "MongoDB",
  custom: "Custom",
};

const KIND_DESCRIPTION: Record<NifiServiceKind, string> = {
  kafka: "Inherited Kafka controller service used by generated NiFi publish processors.",
  schema_registry: "Inherited schema registry controller service used by generated Avro writers.",
  mongodb: "Reusable MongoDB controller service that Mongo flows can link to.",
  custom: "Reusable NiFi controller service stored in the global inherited scope.",
};

const iconFor = (kind: NifiServiceKind) => {
  switch (kind) {
    case "schema_registry":
      return FileCode2;
    case "kafka":
    case "mongodb":
      return Database;
    case "custom":
    default:
      return Plug;
  }
};

const stateClass = (state?: string) => {
  const normalized = (state || "").toUpperCase();
  if (normalized === "ENABLED") return "bg-success-muted text-success border-success/30";
  if (normalized === "DISABLED") return "bg-muted text-muted-foreground border-border";
  return "bg-warning-muted text-warning border-warning/30";
};

const descriptorLabel = (name: string, descriptor?: NifiServiceConfig["descriptors"][string]) =>
  descriptor?.displayName || descriptor?.name || name;

const serviceTypeName = (type: string) => type.split(".").pop() || type;

const serviceTypeSearchValue = (item: ServiceManagerCreateOption) =>
  [item.id, item.label, optionSubtitle(item), item.description].filter(Boolean).join(" ");

const normalizedKind = (kind?: string): NifiServiceKind =>
  kind === "kafka" || kind === "schema_registry" || kind === "mongodb" || kind === "custom" ? kind : "custom";

function safeAllowedOptions(allowableValues: NonNullable<NifiServiceConfig["descriptors"][string]["allowableValues"]>) {
  const seen = new Set<string>();
  return allowableValues
    .map((item, index) => {
      const rawValue = item.value ?? item.displayName ?? "";
      const selectValue = rawValue === "" ? EMPTY_SELECT_VALUE : String(rawValue);
      const display = item.displayName || item.value || "(empty)";
      return { key: `${selectValue}-${index}`, selectValue, actualValue: String(rawValue), display, description: item.description };
    })
    .filter((item) => {
      if (seen.has(item.selectValue)) return false;
      seen.add(item.selectValue);
      return true;
    });
}

type RestAuthType = "NONE" | "API_KEY" | "BEARER" | "BASIC";
type ApiKeyLocation = "HEADER" | "QUERY";

type HttpServiceFormState = {
  name: string;
  description: string;
  baseUrl: string;
  restAuthType: RestAuthType;
  apiKeyName: string;
  apiKeyValue: string;
  apiKeyLocation: ApiKeyLocation;
  bearerToken: string;
  restUsername: string;
  restPassword: string;
  connectionTimeout: string;
  readTimeout: string;
  writeTimeout: string;
  idleTimeout: string;
  rateLimit: string;
};

const defaultHttpForm = (): HttpServiceFormState => ({
  name: "",
  description: "",
  baseUrl: "",
  restAuthType: "NONE",
  apiKeyName: "",
  apiKeyValue: "",
  apiKeyLocation: "HEADER",
  bearerToken: "",
  restUsername: "",
  restPassword: "",
  connectionTimeout: "30",
  readTimeout: "30",
  writeTimeout: "30",
  idleTimeout: "30",
  rateLimit: "120",
});

const asString = (value: unknown, fallback = ""): string => (typeof value === "string" ? value : fallback);
const asNumberString = (value: unknown, fallback: string): string =>
  typeof value === "number" || typeof value === "string" ? String(value) : fallback;

const applicationServiceToHttpForm = (service: ApplicationService): HttpServiceFormState => {
  const cfg = service.config || {};
  return {
    ...defaultHttpForm(),
    name: service.name,
    description: service.description || "",
    baseUrl: asString(cfg.base_url),
    restAuthType: (["NONE", "API_KEY", "BEARER", "BASIC"].includes(asString(cfg.auth_type))
      ? asString(cfg.auth_type)
      : "NONE") as RestAuthType,
    apiKeyName: asString(cfg.api_key_name),
    apiKeyLocation: (asString(cfg.api_key_location) === "QUERY" ? "QUERY" : "HEADER") as ApiKeyLocation,
    restUsername: asString(cfg.rest_username),
    connectionTimeout: asNumberString(cfg.connection_timeout, "30"),
    readTimeout: asNumberString(cfg.read_timeout, "30"),
    writeTimeout: asNumberString(cfg.write_timeout, "30"),
    idleTimeout: asNumberString(cfg.idle_timeout, "30"),
    rateLimit: asNumberString(cfg.rate_limit, "120"),
  };
};

const buildHttpServicePayload = (form: HttpServiceFormState): ApplicationServicePayload => ({
  name: form.name.trim(),
  service_type: "REST API",
  description: form.description.trim() || null,
  config: {
    base_url: form.baseUrl.trim(),
    auth_type: form.restAuthType,
    api_key_name: form.apiKeyName.trim() || null,
    api_key_value: form.apiKeyValue || null,
    api_key_location: form.apiKeyLocation,
    bearer_token: form.bearerToken || null,
    rest_username: form.restUsername.trim() || null,
    rest_password: form.restPassword || null,
    connection_timeout: Number.parseInt(form.connectionTimeout, 10) || 30,
    read_timeout: Number.parseInt(form.readTimeout, 10) || 30,
    write_timeout: Number.parseInt(form.writeTimeout, 10) || 30,
    idle_timeout: Number.parseInt(form.idleTimeout, 10) || 30,
    rate_limit: Number.parseInt(form.rateLimit, 10) || 120,
  },
});

const optionSubtitle = (option: ServiceManagerCreateOption) =>
  option.source === "application" ? "Source Service" : "NiFi Controller Service";

function DynamicPropertiesForm({
  config,
  values,
  setValues,
}: {
  config: NifiServiceConfig;
  values: Record<string, string | null>;
  setValues: (values: Record<string, string | null>) => void;
}) {
  const entries = Object.entries(config.descriptors || {}).sort(([a], [b]) => a.localeCompare(b));

  if (entries.length === 0) {
    return <div className="rounded-md border border-dashed p-4 text-sm text-muted-foreground">No configurable properties were returned by NiFi.</div>;
  }

  return (
    <div className="grid gap-3">
      {entries.map(([name, descriptor]) => {
        const value = values[name] ?? config.properties?.[name] ?? "";
        const allowable = safeAllowedOptions(descriptor?.allowableValues || []);
        const setValue = (next: string | null) => setValues({ ...values, [name]: next });
        return (
          <div key={name} className="grid gap-1.5 rounded-md border p-3">
            <div className="flex items-center justify-between gap-2">
              <Label>
                {descriptorLabel(name, descriptor)}
                {descriptor?.required && <span className="text-destructive"> *</span>}
              </Label>
              {descriptor?.sensitive && <Badge variant="outline">Sensitive</Badge>}
            </div>
            {allowable.length > 0 ? (
              <Select
                value={String(value ?? "") === "" ? EMPTY_SELECT_VALUE : String(value)}
                onValueChange={(next) => setValue(next === EMPTY_SELECT_VALUE ? "" : next)}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Select value" />
                </SelectTrigger>
                <SelectContent>
                  {allowable.map((item) => (
                    <SelectItem key={`${name}-${item.key}`} value={item.selectValue}>
                      {item.display}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            ) : descriptor?.sensitive ? (
              <Input
                type="password"
                value={String(value || "")}
                placeholder="Leave blank to keep unchanged"
                onChange={(event) => setValue(event.target.value)}
              />
            ) : (
              <Textarea
                value={String(value || "")}
                onChange={(event) => setValue(event.target.value)}
                className="min-h-10"
              />
            )}
            {descriptor?.description && <p className="text-xs text-muted-foreground">{descriptor.description}</p>}
            <code className="text-[11px] text-muted-foreground">{name}</code>
          </div>
        );
      })}
    </div>
  );
}

function HttpServiceFields({
  form,
  setForm,
}: {
  form: HttpServiceFormState;
  setForm: Dispatch<SetStateAction<HttpServiceFormState>>;
}) {
  return (
    <>
      <TextInput label="Base URL" value={form.baseUrl} onChange={(value) => setForm((prev) => ({ ...prev, baseUrl: value }))} />
      <div>
        <Label>Auth Type</Label>
        <Select value={form.restAuthType} onValueChange={(value) => setForm((prev) => ({ ...prev, restAuthType: value as RestAuthType }))}>
          <SelectTrigger>
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="NONE">None</SelectItem>
            <SelectItem value="BEARER">Bearer Token</SelectItem>
            <SelectItem value="BASIC">Basic Auth</SelectItem>
            <SelectItem value="API_KEY">API Key</SelectItem>
          </SelectContent>
        </Select>
      </div>
      {form.restAuthType === "BEARER" && (
        <SecretInput label="Bearer Token" value={form.bearerToken} onChange={(value) => setForm((prev) => ({ ...prev, bearerToken: value }))} />
      )}
      {form.restAuthType === "BASIC" && (
        <>
          <TextInput label="Username" value={form.restUsername} onChange={(value) => setForm((prev) => ({ ...prev, restUsername: value }))} />
          <SecretInput label="Password" value={form.restPassword} onChange={(value) => setForm((prev) => ({ ...prev, restPassword: value }))} />
        </>
      )}
      {form.restAuthType === "API_KEY" && (
        <>
          <TextInput label="API Key Name" value={form.apiKeyName} onChange={(value) => setForm((prev) => ({ ...prev, apiKeyName: value }))} />
          <SecretInput label="API Key Value" value={form.apiKeyValue} onChange={(value) => setForm((prev) => ({ ...prev, apiKeyValue: value }))} />
          <div>
            <Label>API Key Location</Label>
            <Select value={form.apiKeyLocation} onValueChange={(value) => setForm((prev) => ({ ...prev, apiKeyLocation: value as ApiKeyLocation }))}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="HEADER">Header</SelectItem>
                <SelectItem value="QUERY">Query Param</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </>
      )}
      <TextInput label="Connection Timeout (s)" value={form.connectionTimeout} onChange={(value) => setForm((prev) => ({ ...prev, connectionTimeout: value }))} />
      <TextInput label="Socket Read Timeout (s)" value={form.readTimeout} onChange={(value) => setForm((prev) => ({ ...prev, readTimeout: value }))} />
      <TextInput label="Socket Write Timeout (s)" value={form.writeTimeout} onChange={(value) => setForm((prev) => ({ ...prev, writeTimeout: value }))} />
      <TextInput label="Socket Idle Timeout (s)" value={form.idleTimeout} onChange={(value) => setForm((prev) => ({ ...prev, idleTimeout: value }))} />
      <TextInput label="Rate Limit (req/min)" value={form.rateLimit} onChange={(value) => setForm((prev) => ({ ...prev, rateLimit: value }))} />
    </>
  );
}

function TextInput({ label, value, onChange }: { label: string; value: string; onChange: (value: string) => void }) {
  return (
    <div>
      <Label>{label}</Label>
      <Input value={value} onChange={(event) => onChange(event.target.value)} />
    </div>
  );
}

function SecretInput({ label, value, onChange }: { label: string; value: string; onChange: (value: string) => void }) {
  return (
    <div>
      <Label>{label}</Label>
      <Input type="password" placeholder="Leave blank to keep unchanged" value={value} onChange={(event) => onChange(event.target.value)} />
    </div>
  );
}

export default function NifiServices() {
  const qc = useQueryClient();
  const [adding, setAdding] = useState(false);
  const [selectedOptionId, setSelectedOptionId] = useState("");
  const [serviceTypeSearch, setServiceTypeSearch] = useState("");
  const [name, setName] = useState("");
  const [editingId, setEditingId] = useState<string | null>(null);
  const [propertyValues, setPropertyValues] = useState<Record<string, string | null>>({});
  const [httpDialogOpen, setHttpDialogOpen] = useState(false);
  const [editingHttpService, setEditingHttpService] = useState<ApplicationService | null>(null);
  const [httpForm, setHttpForm] = useState<HttpServiceFormState>(defaultHttpForm());
  const [testingHttpId, setTestingHttpId] = useState<string | null>(null);
  const [deletingHttpId, setDeletingHttpId] = useState<string | null>(null);

  const servicesQuery = useQuery({
    queryKey: ["nifi-global-services"],
    queryFn: () => nifiServicesApi.list(),
    refetchInterval: 30_000,
  });
  const typesQuery = useQuery({
    queryKey: ["nifi-service-types"],
    queryFn: () => nifiServicesApi.listTypes(),
    enabled: adding,
  });
  const applicationServicesQuery = useQuery({
    queryKey: ["application-services"],
    queryFn: () => applicationServicesApi.list(),
  });
  const configQuery = useQuery({
    queryKey: ["nifi-global-service-config", editingId],
    queryFn: () => nifiServicesApi.get(editingId!),
    enabled: !!editingId,
  });

  const services = servicesQuery.data?.items || [];
  const httpServices = (applicationServicesQuery.data || []).filter((service) => service.service_type === "REST API");
  const createOptions = useMemo(() => buildServiceManagerCreateOptions(typesQuery.data?.items || []), [typesQuery.data?.items]);
  const debouncedServiceTypeSearch = useDebouncedValue(serviceTypeSearch, 180);
  const filteredCreateOptions = useMemo(() => {
    const query = debouncedServiceTypeSearch.trim().toLowerCase();
    if (!query) return createOptions;
    return createOptions.filter((item) => serviceTypeSearchValue(item).toLowerCase().includes(query));
  }, [createOptions, debouncedServiceTypeSearch]);
  const selectedOption = createOptions.find((item) => item.id === selectedOptionId);
  const selectedTypeInfo =
    selectedOption?.source === "nifi"
      ? typesQuery.data?.items.find((item) => item.type === selectedOption.nifiType)
      : null;

  const invalidate = () => qc.invalidateQueries({ queryKey: ["nifi-global-services"] });
  const invalidateHttpServices = () => qc.invalidateQueries({ queryKey: ["application-services"] });

  const createMut = useMutation({
    mutationFn: () => {
      if (!selectedOption || selectedOption.source !== "nifi") throw new Error("Select a NiFi controller service type");
      return nifiServicesApi.create({ name, nifi_type: selectedOption.nifiType });
    },
    onSuccess: (created) => {
      toast.success("NiFi global service created");
      setAdding(false);
      setPropertyValues({});
      setEditingId(created.id);
      invalidate();
    },
    onError: (err: Error) => toast.error(err.message),
  });

  const saveHttpMut = useMutation({
    mutationFn: (payload: ApplicationServicePayload) =>
      editingHttpService ? applicationServicesApi.update(editingHttpService.id, payload) : applicationServicesApi.create(payload),
    onSuccess: () => {
      toast.success(editingHttpService ? "HTTP Service updated" : "HTTP Service created");
      setHttpDialogOpen(false);
      setEditingHttpService(null);
      invalidateHttpServices();
    },
    onError: (err: Error) => toast.error(err.message),
  });

  const testHttpMut = useMutation({
    mutationFn: (id: string) => applicationServicesApi.test(id),
    onMutate: (id) => setTestingHttpId(id),
    onSuccess: (result) => {
      toast[result.ok ? "success" : "message"](result.message || "HTTP Service test finished");
      invalidateHttpServices();
    },
    onError: (err: Error) => toast.error(err.message),
    onSettled: () => setTestingHttpId(null),
  });

  const deleteHttpMut = useMutation({
    mutationFn: (id: string) => applicationServicesApi.remove(id),
    onMutate: (id) => setDeletingHttpId(id),
    onSuccess: () => {
      toast.success("HTTP Service deleted");
      invalidateHttpServices();
    },
    onError: (err: Error) => toast.error(err.message),
    onSettled: () => setDeletingHttpId(null),
  });

  const updateMut = useMutation({
    mutationFn: ({ id, properties }: { id: string; properties: Record<string, string | null> }) =>
      nifiServicesApi.update(id, { properties }),
    onSuccess: () => {
      toast.success("NiFi service configuration saved");
      invalidate();
      if (editingId) qc.invalidateQueries({ queryKey: ["nifi-global-service-config", editingId] });
    },
    onError: (err: Error) => toast.error(err.message),
  });

  const stateMut = useMutation({
    mutationFn: ({ id, action }: { id: string; action: "enable" | "disable" }) =>
      action === "enable" ? nifiServicesApi.enable(id) : nifiServicesApi.disable(id),
    onSuccess: () => {
      toast.success("NiFi service state updated");
      invalidate();
      if (editingId) qc.invalidateQueries({ queryKey: ["nifi-global-service-config", editingId] });
    },
    onError: (err: Error) => toast.error(err.message),
  });

  const defaultMut = useMutation({
    mutationFn: (id: string) => nifiServicesApi.setDefault(id),
    onSuccess: () => {
      toast.success("Default global service updated");
      invalidate();
    },
    onError: (err: Error) => toast.error(err.message),
  });

  const deleteMut = useMutation({
    mutationFn: (id: string) => nifiServicesApi.remove(id),
    onSuccess: () => {
      toast.success("NiFi global service deleted");
      invalidate();
    },
    onError: (err: Error) => toast.error(err.message),
  });

  const openAdd = () => {
    setName("");
    setSelectedOptionId("");
    setServiceTypeSearch("");
    setAdding(true);
  };

  const selectCreateOption = (next: string) => {
    setSelectedOptionId(next);
    const option = createOptions.find((item) => item.id === next);
    if (option?.source === "nifi") {
      setName((current) => current.trim() ? current : option.label);
    }
  };

  const openConfig = (id: string) => {
    setPropertyValues({});
    setEditingId(id);
  };

  const openHttpCreate = () => {
    setEditingHttpService(null);
    setHttpForm(defaultHttpForm());
    setHttpDialogOpen(true);
  };

  const openHttpEdit = (service: ApplicationService) => {
    setEditingHttpService(service);
    setHttpForm(applicationServiceToHttpForm(service));
    setHttpDialogOpen(true);
  };

  const refreshAll = () => {
    servicesQuery.refetch();
    applicationServicesQuery.refetch();
  };

  const currentConfig = configQuery.data;
  const isMutating =
    createMut.isPending ||
    updateMut.isPending ||
    stateMut.isPending ||
    defaultMut.isPending ||
    deleteMut.isPending ||
    saveHttpMut.isPending ||
    testHttpMut.isPending ||
    deleteHttpMut.isPending;

  return (
    <AppLayout
      title="Service Manager"
      description="Manage live NiFi controller services and reusable HTTP source services"
      actions={
        <div className="flex gap-2">
          <Button
            variant="outline"
            onClick={refreshAll}
            disabled={servicesQuery.isFetching || applicationServicesQuery.isFetching}
          >
            {servicesQuery.isFetching || applicationServicesQuery.isFetching ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <RefreshCw className="h-4 w-4" />
            )} Refresh
          </Button>
          <Button onClick={openAdd}>
            <Plus className="h-4 w-4" /> Add Service
          </Button>
        </div>
      }
    >
      {servicesQuery.isLoading && (
        <div className="flex h-40 items-center justify-center">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      )}

      {applicationServicesQuery.isLoading && (
        <div className="flex h-40 items-center justify-center">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      )}

      {servicesQuery.error && (
        <div className="rounded-md border border-destructive/20 bg-destructive/10 p-4 text-sm text-destructive">
          Failed to load NiFi services: {(servicesQuery.error as Error).message}
        </div>
      )}

      {applicationServicesQuery.error && (
        <div className="rounded-md border border-destructive/20 bg-destructive/10 p-4 text-sm text-destructive">
          Failed to load HTTP services: {(applicationServicesQuery.error as Error).message}
        </div>
      )}

      {!servicesQuery.isLoading && !applicationServicesQuery.isLoading && services.length === 0 && httpServices.length === 0 && (
        <Card>
          <CardContent className="flex flex-col items-center justify-center gap-3 py-12 text-center">
            <div className="flex h-12 w-12 items-center justify-center rounded-md bg-primary-muted text-primary">
              <Plug className="h-6 w-6" />
            </div>
            <div>
              <h3 className="font-medium">No services configured</h3>
              <p className="text-sm text-muted-foreground">Add a NiFi controller service or HTTP source service.</p>
            </div>
            <Button onClick={openAdd}>
              <Plus className="h-4 w-4" /> Add Service
            </Button>
          </CardContent>
        </Card>
      )}

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
        {services.map((service) => {
          const kind = normalizedKind(service.service_kind);
          const Icon = iconFor(kind);
          const state = service.live?.state || "UNKNOWN";
          const normalizedState = state.toUpperCase();
          const validationErrors = service.live?.validation_errors || [];
          const canBeDefault = kind === "kafka" || kind === "schema_registry";
          return (
            <Card key={service.id}>
              <CardHeader className="flex-row items-start justify-between space-y-0">
                <div className="flex items-start gap-3">
                  <div className="flex h-10 w-10 items-center justify-center rounded-md bg-primary-muted text-primary">
                    <Icon className="h-5 w-5" />
                  </div>
                  <div>
                    <div className="flex flex-wrap items-center gap-2">
                      <CardTitle className="text-base">{service.name}</CardTitle>
                      {service.is_default && (
                        <Badge variant="outline">
                          <Star className="mr-1 h-3 w-3" /> Default
                        </Badge>
                      )}
                    </div>
                    <CardDescription>{KIND_DESCRIPTION[kind]}</CardDescription>
                  </div>
                </div>
                <Badge className={stateClass(state)}>{state}</Badge>
              </CardHeader>
              <CardContent>
                <div className="mb-3 rounded-md border bg-muted/40 px-3 py-2">
                  <div className="text-xs text-muted-foreground">NiFi Type</div>
                  <div className="truncate text-sm font-mono">{serviceTypeName(service.nifi_type)}</div>
                </div>
                <div className="mb-3 rounded-md border bg-muted/40 px-3 py-2">
                  <div className="text-xs text-muted-foreground">Controller Service ID</div>
                  <div className="truncate text-sm font-mono">{service.nifi_controller_service_id}</div>
                </div>
                {validationErrors.length > 0 && (
                  <div className="mb-3 flex gap-2 rounded-md border border-warning/20 bg-warning-muted/30 p-3 text-xs text-warning">
                    <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
                    <span>{validationErrors.join(" ")}</span>
                  </div>
                )}
                {service.live?.error && (
                  <div className="mb-3 rounded-md border border-destructive/20 bg-destructive/10 p-3 text-xs text-destructive">{service.live.error}</div>
                )}
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <span className="text-xs text-muted-foreground">Role: {KIND_SHORT_LABEL[kind]}</span>
                  <div className="flex flex-wrap gap-2">
                    {canBeDefault && !service.is_default && (
                      <Button size="sm" variant="outline" onClick={() => defaultMut.mutate(service.id)} disabled={isMutating}>
                        Set Default
                      </Button>
                    )}
                    <Button size="sm" variant="outline" onClick={() => openConfig(service.id)}>
                      <Settings2 className="h-3.5 w-3.5" /> Configure
                    </Button>
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={() =>
                        stateMut.mutate({
                          id: service.id,
                          action: normalizedState === "ENABLED" ? "disable" : "enable",
                        })
                      }
                      disabled={isMutating}
                    >
                      {normalizedState === "ENABLED" ? "Disable" : "Enable"}
                    </Button>
                    <Button
                      size="sm"
                      variant="destructive"
                      onClick={() => {
                        if (confirm(`Delete NiFi global service "${service.name}"?`)) deleteMut.mutate(service.id);
                      }}
                      disabled={isMutating}
                    >
                      <Trash2 className="h-3.5 w-3.5" /> Delete
                    </Button>
                  </div>
                </div>
              </CardContent>
            </Card>
          );
        })}
        {httpServices.map((service) => {
          const isTesting = testingHttpId === service.id;
          const isDeleting = deletingHttpId === service.id;
          return (
            <Card key={service.id}>
              <CardHeader className="flex-row items-start justify-between space-y-0">
                <div className="flex items-start gap-3">
                  <div className="flex h-10 w-10 items-center justify-center rounded-md bg-primary-muted text-primary">
                    <Globe2 className="h-5 w-5" />
                  </div>
                  <div>
                    <CardTitle className="text-base">{service.name}</CardTitle>
                    <CardDescription>Reusable HTTP base URL, authentication, timeout, and rate-limit profile.</CardDescription>
                  </div>
                </div>
                <StatusBadge status={service.health} />
              </CardHeader>
              <CardContent className="space-y-3">
                <div className="rounded-md border bg-muted/40 px-3 py-2">
                  <div className="text-xs text-muted-foreground">Service Type</div>
                  <div className="text-sm font-medium">HTTP Service</div>
                </div>
                <div className="rounded-md border bg-muted/40 px-3 py-2">
                  <div className="text-xs text-muted-foreground">Base URL</div>
                  <div className="truncate text-sm font-mono">{asString(service.config?.base_url, "Not configured")}</div>
                </div>
                {service.description && <p className="text-sm text-muted-foreground">{service.description}</p>}
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <span className="text-xs text-muted-foreground">Last tested: {timeAgo(service.last_tested)}</span>
                  <div className="flex flex-wrap gap-2">
                    <Button variant="outline" size="sm" onClick={() => openHttpEdit(service)}>
                      <KeyRound className="h-3.5 w-3.5" /> Configure
                    </Button>
                    <Button variant="outline" size="sm" onClick={() => testHttpMut.mutate(service.id)} disabled={isTesting}>
                      {isTesting ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <ShieldCheck className="h-3.5 w-3.5" />} Test
                    </Button>
                    <Button
                      variant="destructive"
                      size="sm"
                      onClick={() => {
                        if (confirm(`Delete HTTP Service "${service.name}"?`)) deleteHttpMut.mutate(service.id);
                      }}
                      disabled={isDeleting}
                    >
                      {isDeleting ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Trash2 className="h-3.5 w-3.5" />} Delete
                    </Button>
                  </div>
                </div>
              </CardContent>
            </Card>
          );
        })}
      </div>

      <Dialog open={adding} onOpenChange={setAdding}>
        <DialogContent className="max-h-[88vh] max-w-3xl grid-rows-[auto_minmax(0,1fr)_auto]">
          <DialogHeader>
            <DialogTitle>Add Service</DialogTitle>
            <DialogDescription>Select a live NiFi controller service type or the HTTP Service source profile.</DialogDescription>
          </DialogHeader>
          <div className="grid min-h-0 gap-3 overflow-y-auto pr-1">
            {selectedOption?.source === "nifi" && (
              <div>
                <Label>Name</Label>
                <Input value={name} onChange={(event) => setName(event.target.value)} />
              </div>
            )}
            <div>
              <Label>Service Type</Label>
              <div className="mt-1 overflow-hidden rounded-md border">
                <Command shouldFilter={false}>
                  <CommandInput
                    value={serviceTypeSearch}
                    onValueChange={setServiceTypeSearch}
                    placeholder={typesQuery.isLoading ? "Loading service types..." : "Search service types..."}
                    disabled={typesQuery.isLoading}
                  />
                  <CommandList className="h-[min(420px,45vh)] overflow-y-auto">
                    <CommandEmpty>No service types found.</CommandEmpty>
                    <CommandGroup>
                      {filteredCreateOptions.map((item) => (
                        <CommandItem
                          key={item.id}
                          value={serviceTypeSearchValue(item)}
                          onSelect={() => selectCreateOption(item.id)}
                          className="items-start gap-3 py-3"
                        >
                          <Check className={`mt-1 h-4 w-4 shrink-0 ${selectedOptionId === item.id ? "opacity-100" : "opacity-0"}`} />
                          <span className="flex min-w-0 flex-col">
                            <span className="truncate font-medium">{item.label}</span>
                            <span className="truncate text-xs text-muted-foreground">{optionSubtitle(item)}</span>
                          </span>
                        </CommandItem>
                      ))}
                    </CommandGroup>
                  </CommandList>
                </Command>
              </div>
              {selectedOption?.description && <p className="mt-1 text-xs text-muted-foreground">{selectedOption.description}</p>}
              {typesQuery.error && <p className="mt-1 text-xs text-destructive">Failed to load service types: {(typesQuery.error as Error).message}</p>}
            </div>
            {selectedOption?.source === "application" && (
              <div className="rounded-md border bg-muted/40 p-3 text-sm text-muted-foreground">
                HTTP Service is stored by this application and becomes selectable during REST source setup.
              </div>
            )}
            {selectedOption?.source === "nifi" && selectedTypeInfo?.description && (
              <p className="text-xs text-muted-foreground">{selectedTypeInfo.description}</p>
            )}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setAdding(false)}>
              Cancel
            </Button>
            <Button
              disabled={!selectedOption || (selectedOption.source === "nifi" && !name.trim()) || createMut.isPending}
              onClick={() => {
                if (!selectedOption) return;
                if (selectedOption.id === HTTP_SERVICE_OPTION_ID) {
                  setAdding(false);
                  openHttpCreate();
                  return;
                }
                createMut.mutate();
              }}
            >
              {createMut.isPending && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
              {selectedOption?.id === HTTP_SERVICE_OPTION_ID ? "Configure Service" : "Create Service"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog
        open={httpDialogOpen}
        onOpenChange={(open) => {
          setHttpDialogOpen(open);
          if (!open) setEditingHttpService(null);
        }}
      >
        <DialogContent className="max-h-[85vh] max-w-lg overflow-y-auto">
          <DialogHeader>
            <DialogTitle>{editingHttpService ? "Configure HTTP Service" : "Add HTTP Service"}</DialogTitle>
            <DialogDescription>Configure the reusable HTTP connection profile used by REST sources.</DialogDescription>
          </DialogHeader>
          <div className="grid gap-3">
            <div>
              <Label>Name</Label>
              <Input value={httpForm.name} onChange={(event) => setHttpForm((prev) => ({ ...prev, name: event.target.value }))} />
            </div>
            <div>
              <Label>Description</Label>
              <Textarea value={httpForm.description} onChange={(event) => setHttpForm((prev) => ({ ...prev, description: event.target.value }))} />
            </div>
            <HttpServiceFields form={httpForm} setForm={setHttpForm} />
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setHttpDialogOpen(false)}>
              Cancel
            </Button>
            <Button
              onClick={() => saveHttpMut.mutate(buildHttpServicePayload(httpForm))}
              disabled={!httpForm.name.trim() || !httpForm.baseUrl.trim() || saveHttpMut.isPending}
            >
              {saveHttpMut.isPending && <Loader2 className="h-4 w-4 animate-spin" />} Save
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog
        open={!!editingId}
        onOpenChange={(open) => {
          if (!open) {
            setEditingId(null);
            setPropertyValues({});
          }
        }}
      >
        <DialogContent className="max-h-[85vh] max-w-4xl overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Configure NiFi Service</DialogTitle>
            <DialogDescription>Properties are loaded live from NiFi. Saving may temporarily disable and re-enable the service.</DialogDescription>
          </DialogHeader>
          {configQuery.isLoading && (
            <div className="flex h-32 items-center justify-center">
              <Loader2 className="h-6 w-6 animate-spin" />
            </div>
          )}
          {configQuery.error && (
            <div className="rounded-md border border-destructive/20 bg-destructive/10 p-4 text-sm text-destructive">
              Failed to load NiFi service configuration: {(configQuery.error as Error).message}
            </div>
          )}
          {currentConfig && (
            <div className="grid gap-4">
              <div className="rounded-md border bg-muted/30 p-3 text-sm">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div>
                    <div className="font-medium">{currentConfig.name}</div>
                    <div className="font-mono text-xs text-muted-foreground">{serviceTypeName(currentConfig.type)}</div>
                  </div>
                  <Badge className={stateClass(currentConfig.state)}>{currentConfig.state}</Badge>
                </div>
                {(currentConfig.validation_errors || []).length > 0 && (
                  <p className="mt-2 text-xs text-warning">{currentConfig.validation_errors.join(" ")}</p>
                )}
              </div>
              <DynamicPropertiesForm config={currentConfig} values={propertyValues} setValues={setPropertyValues} />
            </div>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => setEditingId(null)}>
              Close
            </Button>
            {currentConfig && (
              <Button
                disabled={updateMut.isPending}
                onClick={() => updateMut.mutate({ id: currentConfig.global_service.id, properties: { ...currentConfig.properties, ...propertyValues } })}
              >
                {updateMut.isPending && <Loader2 className="h-3.5 w-3.5 animate-spin" />} Save Configuration
              </Button>
            )}
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </AppLayout>
  );
}
