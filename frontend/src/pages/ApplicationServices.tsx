import { useState, type Dispatch, type SetStateAction } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Globe2, HardDrive, KeyRound, Loader2, Plus, RefreshCw, ShieldCheck, Trash2, Webhook } from "lucide-react";
import { toast } from "sonner";

import { AppLayout } from "@/components/AppLayout";
import { StatusBadge } from "@/components/StatusBadge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { timeAgo } from "@/lib/api";
import {
  applicationServicesApi,
  type ApplicationService,
  type ApplicationServicePayload,
  type ApplicationServiceType,
} from "@/lib/applicationServicesApi";

type RestAuthType = "NONE" | "API_KEY" | "BEARER" | "BASIC";
type ApiKeyLocation = "HEADER" | "QUERY";

type FormState = {
  name: string;
  serviceType: ApplicationServiceType;
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
  smbHostname: string;
  smbShare: string;
  smbDomain: string;
  smbUsername: string;
  smbPassword: string;
  webhookSignatureHeader: string;
  webhookSecret: string;
};

const defaultForm = (serviceType: ApplicationServiceType = "REST API"): FormState => ({
  name: "",
  serviceType,
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
  smbHostname: "",
  smbShare: "",
  smbDomain: "",
  smbUsername: "",
  smbPassword: "",
  webhookSignatureHeader: "X-Signature",
  webhookSecret: "",
});

const serviceMeta: Record<ApplicationServiceType, { label: string; description: string; icon: typeof Globe2 }> = {
  "REST API": {
    label: "REST API",
    description: "Reusable base URL, authentication, timeout, and rate-limit profile.",
    icon: Globe2,
  },
  SMB: {
    label: "SMB",
    description: "Reusable SMB server, share, domain, and authentication profile.",
    icon: HardDrive,
  },
  Webhook: {
    label: "Webhook",
    description: "Reusable webhook signature header and shared-secret policy.",
    icon: Webhook,
  },
};

const asString = (value: unknown, fallback = ""): string => (typeof value === "string" ? value : fallback);
const asNumberString = (value: unknown, fallback: string): string =>
  typeof value === "number" || typeof value === "string" ? String(value) : fallback;

const recordToForm = (service: ApplicationService): FormState => {
  const cfg = service.config || {};
  return {
    ...defaultForm(service.service_type),
    name: service.name,
    serviceType: service.service_type,
    description: service.description || "",
    baseUrl: asString(cfg.base_url),
    restAuthType: (["NONE", "API_KEY", "BEARER", "BASIC"].includes(asString(cfg.auth_type))
      ? asString(cfg.auth_type)
      : "NONE") as RestAuthType,
    apiKeyName: asString(cfg.api_key_name),
    apiKeyLocation: (asString(cfg.api_key_location) === "QUERY" ? "QUERY" : "HEADER") as ApiKeyLocation,
    connectionTimeout: asNumberString(cfg.connection_timeout, "30"),
    readTimeout: asNumberString(cfg.read_timeout, "30"),
    writeTimeout: asNumberString(cfg.write_timeout, "30"),
    idleTimeout: asNumberString(cfg.idle_timeout, "30"),
    rateLimit: asNumberString(cfg.rate_limit, "120"),
    smbHostname: asString(cfg.smb_hostname),
    smbShare: asString(cfg.smb_share),
    smbDomain: asString(cfg.smb_domain),
    smbUsername: asString(cfg.smb_username),
    webhookSignatureHeader: asString(cfg.webhook_signature_header, "X-Signature"),
  };
};

const buildPayload = (form: FormState): ApplicationServicePayload => {
  const base = {
    name: form.name.trim(),
    service_type: form.serviceType,
    description: form.description.trim() || null,
  };
  if (form.serviceType === "REST API") {
    return {
      ...base,
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
    };
  }
  if (form.serviceType === "SMB") {
    return {
      ...base,
      config: {
        smb_hostname: form.smbHostname.trim(),
        smb_share: form.smbShare.trim(),
        smb_domain: form.smbDomain.trim() || null,
        smb_username: form.smbUsername.trim(),
        smb_password: form.smbPassword || null,
        connection_timeout: Number.parseInt(form.connectionTimeout, 10) || 30,
      },
    };
  }
  return {
    ...base,
    config: {
      webhook_signature_header: form.webhookSignatureHeader.trim() || "X-Signature",
      webhook_signature_algo: "hmac-sha256",
      webhook_secret: form.webhookSecret || null,
    },
  };
};

export default function ApplicationServices() {
  const qc = useQueryClient();
  const [open, setOpen] = useState(false);
  const [editing, setEditing] = useState<ApplicationService | null>(null);
  const [form, setForm] = useState<FormState>(defaultForm());
  const [testingId, setTestingId] = useState<string | null>(null);
  const [deletingId, setDeletingId] = useState<string | null>(null);

  const servicesQuery = useQuery({
    queryKey: ["application-services"],
    queryFn: () => applicationServicesApi.list(),
  });

  const services = servicesQuery.data || [];

  const invalidate = () => qc.invalidateQueries({ queryKey: ["application-services"] });

  const saveMut = useMutation({
    mutationFn: (payload: ApplicationServicePayload) =>
      editing ? applicationServicesApi.update(editing.id, payload) : applicationServicesApi.create(payload),
    onSuccess: () => {
      toast.success(editing ? "Application Service updated" : "Application Service created");
      setOpen(false);
      setEditing(null);
      invalidate();
    },
    onError: (err: Error) => toast.error(err.message),
  });

  const testMut = useMutation({
    mutationFn: (id: string) => applicationServicesApi.test(id),
    onMutate: (id) => setTestingId(id),
    onSuccess: (result) => {
      toast[result.ok ? "success" : "message"](result.message || "Application Service test finished");
      invalidate();
    },
    onError: (err: Error) => toast.error(err.message),
    onSettled: () => setTestingId(null),
  });

  const deleteMut = useMutation({
    mutationFn: (id: string) => applicationServicesApi.remove(id),
    onMutate: (id) => setDeletingId(id),
    onSuccess: () => {
      toast.success("Application Service deleted");
      invalidate();
    },
    onError: (err: Error) => toast.error(err.message),
    onSettled: () => setDeletingId(null),
  });

  const openCreate = (serviceType: ApplicationServiceType = "REST API") => {
    setEditing(null);
    setForm(defaultForm(serviceType));
    setOpen(true);
  };

  const openEdit = (service: ApplicationService) => {
    setEditing(service);
    setForm(recordToForm(service));
    setOpen(true);
  };

  const renderService = (service: ApplicationService) => {
    const meta = serviceMeta[service.service_type];
    const Icon = meta.icon;
    const isTesting = testingId === service.id;
    const isDeleting = deletingId === service.id;
    return (
      <Card key={service.id}>
        <CardHeader className="flex-row items-start justify-between space-y-0">
          <div className="flex items-start gap-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-md bg-primary-muted text-primary">
              <Icon className="h-5 w-5" />
            </div>
            <div>
              <CardTitle className="text-base">{service.name}</CardTitle>
              <CardDescription>{meta.description}</CardDescription>
            </div>
          </div>
          <StatusBadge status={service.health} />
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="rounded-md border bg-muted/40 px-3 py-2">
            <div className="text-xs text-muted-foreground">Application Service Type</div>
            <div className="text-sm font-medium">{service.service_type}</div>
          </div>
          {service.description && <p className="text-sm text-muted-foreground">{service.description}</p>}
          <div className="flex flex-wrap items-center justify-between gap-2">
            <span className="text-xs text-muted-foreground">Last tested: {timeAgo(service.last_tested)}</span>
            <div className="flex flex-wrap gap-2">
              <Button variant="outline" size="sm" onClick={() => openEdit(service)}>
                <KeyRound className="h-3.5 w-3.5" /> Configure
              </Button>
              <Button variant="outline" size="sm" onClick={() => testMut.mutate(service.id)} disabled={isTesting}>
                {isTesting ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <ShieldCheck className="h-3.5 w-3.5" />} Test
              </Button>
              <Button
                variant="destructive"
                size="sm"
                onClick={() => {
                  if (confirm(`Delete Application Service "${service.name}"?`)) deleteMut.mutate(service.id);
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
  };

  return (
    <AppLayout
      title="Application Services"
      description="Reusable external source connection profiles used during source setup"
      actions={
        <div className="flex gap-2">
          <Button variant="outline" onClick={() => servicesQuery.refetch()} disabled={servicesQuery.isFetching}>
            {servicesQuery.isFetching ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />} Refresh
          </Button>
          <Button onClick={() => openCreate()}>
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

      {!servicesQuery.isLoading && services.length === 0 && (
        <Card>
          <CardContent className="flex flex-col items-center justify-center gap-3 py-12 text-center">
            <div className="flex h-12 w-12 items-center justify-center rounded-md bg-primary-muted text-primary">
              <Globe2 className="h-6 w-6" />
            </div>
            <div>
              <h3 className="font-medium">No Application Services configured</h3>
              <p className="text-sm text-muted-foreground">Create reusable source connection profiles before selecting them in source setup.</p>
            </div>
            <Button onClick={() => openCreate()}>
              <Plus className="h-4 w-4" /> Add Service
            </Button>
          </CardContent>
        </Card>
      )}

      {services.length > 0 && <div className="grid grid-cols-1 gap-4 md:grid-cols-2">{services.map(renderService)}</div>}

      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent className="max-h-[85vh] max-w-lg overflow-y-auto">
          <DialogHeader>
            <DialogTitle>{editing ? "Configure Application Service" : "Add Application Service"}</DialogTitle>
            <DialogDescription>
              Configure a reusable source connection profile. The selected type controls which fields are shown.
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-3">
            <div>
              <Label>Name</Label>
              <Input value={form.name} onChange={(event) => setForm((prev) => ({ ...prev, name: event.target.value }))} />
            </div>
            <div>
              <Label>Service Type</Label>
              <Select
                value={form.serviceType}
                disabled={Boolean(editing)}
                onValueChange={(value) => setForm((prev) => ({ ...prev, serviceType: value as ApplicationServiceType }))}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="REST API">REST API</SelectItem>
                  <SelectItem value="SMB">SMB</SelectItem>
                  <SelectItem value="Webhook">Webhook</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div>
              <Label>Description</Label>
              <Textarea value={form.description} onChange={(event) => setForm((prev) => ({ ...prev, description: event.target.value }))} />
            </div>

            {form.serviceType === "REST API" && <RestFields form={form} setForm={setForm} />}
            {form.serviceType === "SMB" && <SmbFields form={form} setForm={setForm} />}
            {form.serviceType === "Webhook" && <WebhookFields form={form} setForm={setForm} />}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setOpen(false)}>
              Cancel
            </Button>
            <Button onClick={() => saveMut.mutate(buildPayload(form))} disabled={saveMut.isPending}>
              {saveMut.isPending && <Loader2 className="h-4 w-4 animate-spin" />} Save
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </AppLayout>
  );
}

function RestFields({ form, setForm }: { form: FormState; setForm: Dispatch<SetStateAction<FormState>> }) {
  return (
    <>
      <div>
        <Label>Base URL</Label>
        <Input value={form.baseUrl} onChange={(event) => setForm((prev) => ({ ...prev, baseUrl: event.target.value }))} />
      </div>
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

function SmbFields({ form, setForm }: { form: FormState; setForm: Dispatch<SetStateAction<FormState>> }) {
  return (
    <>
      <TextInput label="SMB Server Address" value={form.smbHostname} onChange={(value) => setForm((prev) => ({ ...prev, smbHostname: value }))} />
      <TextInput label="SMB Share" value={form.smbShare} onChange={(value) => setForm((prev) => ({ ...prev, smbShare: value }))} />
      <TextInput label="Domain" value={form.smbDomain} onChange={(value) => setForm((prev) => ({ ...prev, smbDomain: value }))} />
      <TextInput label="Username" value={form.smbUsername} onChange={(value) => setForm((prev) => ({ ...prev, smbUsername: value }))} />
      <SecretInput label="Password" value={form.smbPassword} onChange={(value) => setForm((prev) => ({ ...prev, smbPassword: value }))} />
      <TextInput label="Connection Timeout (s)" value={form.connectionTimeout} onChange={(value) => setForm((prev) => ({ ...prev, connectionTimeout: value }))} />
    </>
  );
}

function WebhookFields({ form, setForm }: { form: FormState; setForm: Dispatch<SetStateAction<FormState>> }) {
  return (
    <>
      <TextInput
        label="Signature Header"
        value={form.webhookSignatureHeader}
        onChange={(value) => setForm((prev) => ({ ...prev, webhookSignatureHeader: value }))}
      />
      <SecretInput label="Shared Secret" value={form.webhookSecret} onChange={(value) => setForm((prev) => ({ ...prev, webhookSecret: value }))} />
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
