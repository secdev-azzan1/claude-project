// Application Services — reusable endpoint + credential profiles.
// Every adapter that needs credentials selects a service; credentials never
// live on a block. Edits create revisions; retirement is logical, never a delete.

import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { AppLayout } from "@/components/AppLayout";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
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
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { StatusBadge } from "@/components/StatusBadge";
import { timeAgo } from "@/lib/api";
import {
  listServices,
  reinstateService,
  retireService,
  saveService,
  serviceDependents,
  testService,
} from "@/prototype/api";
import {
  ServiceFormFields,
  buildConfig,
  emptyForm,
  formFromService,
  saveBlockReason,
  secretTyped,
  type ServiceForm,
} from "@/components/service-form/ServiceFormFields";
import type { AppService, HttpAuthMode, ServiceType, SinkKind } from "@/prototype/types";
import {
  ArchiveRestore,
  ArchiveX,
  Database,
  Globe2,
  HardDriveDownload,
  Loader2,
  Pencil,
  Plus,
  Radio,
  ShieldCheck,
  Workflow,
} from "lucide-react";
import { toast } from "sonner";

// ------------------------------------------------------------------ helpers

const str = (v: unknown, fb = ""): string => (typeof v === "string" ? v : fb);
const numStr = (v: unknown, fb = ""): string =>
  typeof v === "number" || typeof v === "string" ? String(v) : fb;

const AUTH_LABEL: Record<HttpAuthMode, string> = {
  none: "No auth",
  basic: "Basic auth",
  bearer: "Bearer token",
  api_key: "API key",
  oauth2: "OAuth2",
  session_token: "Session token",
};

const SERVICE_TYPE_META: Record<
  ServiceType,
  { label: string; icon: React.ComponentType<{ className?: string }>; blurb: string }
> = {
  http: {
    label: "HTTP service",
    icon: Globe2,
    blurb: "Base URL + authentication profile used by http adapter blocks.",
  },
  database: {
    label: "Database service",
    icon: Database,
    blurb: "Host, credentials and capabilities used by jdbc adapter blocks.",
  },
  external_kafka: {
    label: "External Kafka receiver",
    icon: Radio,
    blurb: "Input only — external clusters are never a destination.",
  },
  sink_destination: {
    label: "Sink destination",
    icon: HardDriveDownload,
    // Where the Connect sink actually writes — an OpenSearch cluster, an
    // Iceberg catalog. It is a service like any other because the reason is the
    // same: the endpoint and its secret belong to one reusable, revisioned
    // profile, not retyped into each connector's properties.
    blurb: "Endpoint + credentials the managed Connect sinks write to — selected by kafka+connect and kc blocks.",
  },
};

const TYPE_ORDER: ServiceType[] = ["http", "database", "external_kafka", "sink_destination"];

function configSummary(svc: AppService): { label: string; value: string }[] {
  const c = svc.config;
  switch (svc.type) {
    case "http": {
      const mode = str(c.authMode, "none") as HttpAuthMode;
      const rows = [
        { label: "Base URL", value: str(c.baseUrl, "—") },
        { label: "Auth", value: AUTH_LABEL[mode] ?? mode },
      ];
      if (mode === "api_key") rows[1].value += ` · ${str(c.keyLocation, "header")} ${str(c.keyName)}`;
      if (mode === "session_token") rows.push({ label: "Login path", value: str(c.loginPath, "—") });
      if (mode === "oauth2") rows.push({ label: "Token URL", value: str(c.tokenUrl, "—") });
      return rows;
    }
    case "database": {
      const caps = Array.isArray(c.capabilities) ? (c.capabilities as string[]).join(" + ") : "read";
      return [
        { label: "Endpoint", value: `${str(c.dialect)} · ${str(c.host)}:${numStr(c.port)}/${str(c.database)}` },
        { label: "Capabilities", value: caps || "—" },
      ];
    }
    case "external_kafka":
      return [
        { label: "Bootstrap servers", value: str(c.bootstrapServers, "—") },
        { label: "Security", value: str(c.securityProtocol, "—") },
      ];
    case "sink_destination": {
      const kind = str(c.kind) as SinkKind;
      if (kind === "opensearch")
        return [
          { label: "OpenSearch URL", value: str(c.url, "—") },
          { label: "Index", value: `prefix ${str(c.indexPrefix, "—")} · ${str(c.writeMode, "upsert")}` },
        ];
      return [
        { label: "Iceberg catalog", value: str(c.catalogUrl, "—") },
        { label: "Warehouse", value: str(c.warehouse, "—") },
      ];
    }
  }
}

// ------------------------------------------------------------------- page

export default function AppServices() {
  const qc = useQueryClient();

  const { data: services = [], isLoading } = useQuery({
    queryKey: ["services"],
    queryFn: listServices,
  });

  const [formOpen, setFormOpen] = useState(false);
  const [editing, setEditing] = useState<AppService | null>(null);
  const [formType, setFormType] = useState<ServiceType | null>(null);
  const [form, setForm] = useState<ServiceForm>(emptyForm());
  const [retireTarget, setRetireTarget] = useState<AppService | null>(null);
  const [testingId, setTestingId] = useState<string | null>(null);

  const invalidate = () => {
    qc.invalidateQueries({ queryKey: ["services"] });
    qc.invalidateQueries({ queryKey: ["audit"] });
    qc.invalidateQueries({ queryKey: ["dashboard"] });
  };

  const saveMut = useMutation({
    mutationFn: (svc: AppService) => saveService(svc),
    onSuccess: (saved, input) => {
      if (input.id) {
        toast.success(`Revision ${saved.revision} created — linked flows adopt it at next deploy`);
      } else {
        toast.success(`Service "${saved.name}" created`);
      }
      setFormOpen(false);
      setEditing(null);
      invalidate();
    },
    onError: (e: Error) => toast.error(e.message),
  });

  const testMut = useMutation({
    mutationFn: (id: string) => testService(id),
    onMutate: (id) => setTestingId(id),
    onSuccess: (svc) => {
      if (svc.health === "Healthy") toast.success(`"${svc.name}" test passed`);
      else toast.error(`"${svc.name}" test failed`);
      invalidate();
    },
    onError: (e: Error) => toast.error(e.message),
    onSettled: () => setTestingId(null),
  });

  const retireMut = useMutation({
    mutationFn: (id: string) => retireService(id),
    onSuccess: (_res, id) => {
      const svc = services.find((s) => s.id === id);
      const deps = serviceDependents(id);
      toast.warning(
        `"${svc?.name ?? "Service"}" retired${deps.length > 0 ? ` — ${deps.length} dependent flow(s) flagged action required` : ""}`,
      );
      setRetireTarget(null);
      invalidate();
      qc.invalidateQueries({ queryKey: ["flows"] });
    },
    onError: (e: Error) => toast.error(e.message),
  });

  const reinstateMut = useMutation({
    mutationFn: (id: string) => reinstateService(id),
    onSuccess: (_res, id) => {
      const svc = services.find((s) => s.id === id);
      toast.success(`"${svc?.name ?? "Service"}" reinstated`);
      invalidate();
      qc.invalidateQueries({ queryKey: ["flows"] });
    },
    onError: (e: Error) => toast.error(e.message),
  });

  const openCreate = () => {
    setEditing(null);
    setFormType(null);
    setForm(emptyForm());
    setFormOpen(true);
  };

  const openEdit = (svc: AppService) => {
    setEditing(svc);
    setFormType(svc.type);
    setForm(formFromService(svc));
    setFormOpen(true);
  };

  const submit = () => {
    if (!formType) return;
    const base: AppService = editing
      ? { ...editing }
      : {
          id: "",
          type: formType,
          name: "",
          revision: 1,
          retired: false,
          health: "Not Tested",
          lastTestedAt: null,
          config: {},
          hasSecret: false,
          createdAt: "",
          updatedAt: "",
        };
    saveMut.mutate({
      ...base,
      name: form.name.trim(),
      config: buildConfig(formType, form),
      hasSecret: secretTyped(formType, form) || (editing?.hasSecret ?? false),
    });
  };

  const blockReason = saveBlockReason(formType, form);
  const retireDeps = retireTarget ? serviceDependents(retireTarget.id) : [];

  return (
    <AppLayout
      title="Application Services"
      description="Reusable endpoint + credential profiles. Every adapter that needs credentials selects a service — secrets never live on a block."
      actions={
        <Button onClick={openCreate}>
          <Plus className="h-4 w-4" /> Add Service
        </Button>
      }
    >
      {isLoading ? (
        <div className="flex h-40 items-center justify-center">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      ) : services.length === 0 ? (
        <Card>
          <CardContent className="flex flex-col items-center justify-center gap-3 py-12 text-center">
            <div className="flex h-12 w-12 items-center justify-center rounded-md bg-primary-muted text-primary">
              <Globe2 className="h-6 w-6" />
            </div>
            <div>
              <h3 className="font-medium">No application services yet</h3>
              <p className="text-sm text-muted-foreground">Create the first endpoint + credential profile.</p>
            </div>
            <Button onClick={openCreate}>
              <Plus className="h-4 w-4" /> Add Service
            </Button>
          </CardContent>
        </Card>
      ) : (
        <div className="space-y-8">
          {TYPE_ORDER.map((type) => {
            const meta = SERVICE_TYPE_META[type];
            const group = services.filter((s) => s.type === type);
            if (group.length === 0) return null;
            const Icon = meta.icon;
            return (
              <section key={type}>
                <div className="mb-3 flex items-center gap-2">
                  <span className="flex h-7 w-7 items-center justify-center rounded-md bg-primary-muted text-primary">
                    <Icon className="h-4 w-4" />
                  </span>
                  <h2 className="text-sm font-semibold">{meta.label}</h2>
                  <span className="text-xs text-muted-foreground">
                    · {group.length} service{group.length === 1 ? "" : "s"} — {meta.blurb}
                  </span>
                </div>
                <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
                  {group.map((svc) => (
                    <ServiceCard
                      key={svc.id}
                      svc={svc}
                      testing={testingId === svc.id}
                      onTest={() => testMut.mutate(svc.id)}
                      onEdit={() => openEdit(svc)}
                      onRetire={() => setRetireTarget(svc)}
                      onReinstate={() => reinstateMut.mutate(svc.id)}
                      reinstating={reinstateMut.isPending}
                    />
                  ))}
                </div>
              </section>
            );
          })}
        </div>
      )}

      {/* ------------------------------------------------ create/edit dialog */}
      <Dialog
        open={formOpen}
        onOpenChange={(open) => {
          setFormOpen(open);
          if (!open) setEditing(null);
        }}
      >
        <DialogContent className="max-h-[85vh] max-w-lg overflow-y-auto">
          <DialogHeader>
            <DialogTitle>{editing ? `Edit ${editing.name}` : "Add Application Service"}</DialogTitle>
            <DialogDescription>
              {editing
                ? `Saving creates revision ${editing.revision + 1}. Linked flows keep their pinned revision until the next deploy.`
                : "Pick a service type, then configure the profile. Secrets are write-only."}
            </DialogDescription>
          </DialogHeader>

          {!editing && (
            <div className="grid gap-2">
              <Label>Service type</Label>
              <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                {TYPE_ORDER.map((type) => {
                  const meta = SERVICE_TYPE_META[type];
                  const Icon = meta.icon;
                  const selected = formType === type;
                  return (
                    <button
                      key={type}
                      type="button"
                      onClick={() => setFormType(type)}
                      className={`rounded-md border p-3 text-left transition hover:bg-muted/50 ${
                        selected ? "border-primary bg-primary-muted" : ""
                      }`}
                    >
                      <div className="flex items-center gap-2 text-sm font-medium">
                        <Icon className="h-4 w-4" /> {meta.label}
                      </div>
                      <p className="mt-1 text-xs text-muted-foreground">{meta.blurb}</p>
                    </button>
                  );
                })}
              </div>
            </div>
          )}

          {formType && (
            <ServiceFormFields type={formType} form={form} onChange={setForm} editing={!!editing} />
          )}

          <DialogFooter>
            <Button variant="outline" onClick={() => setFormOpen(false)}>
              Cancel
            </Button>
            <Button
              onClick={submit}
              disabled={!!blockReason || saveMut.isPending}
              title={blockReason ?? undefined}
            >
              {saveMut.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
              {editing ? `Save as revision ${editing.revision + 1}` : "Create Service"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* --------------------------------------------------- retire dialog */}
      <AlertDialog open={!!retireTarget} onOpenChange={(open) => !open && setRetireTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Retire "{retireTarget?.name}"?</AlertDialogTitle>
            <AlertDialogDescription>
              Retirement is logical — there is no hard delete. The service stays on record and can be
              reinstated, but adapters can no longer select it.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <div className="rounded-md border border-warning/30 bg-warning-muted p-3 text-sm">
            {retireDeps.length > 0 ? (
              <>
                <div className="font-medium">
                  {retireDeps.length} dependent flow{retireDeps.length === 1 ? "" : "s"} will be flagged
                  "action required":
                </div>
                <ul className="mt-1 list-disc pl-5 text-xs text-muted-foreground">
                  {retireDeps.map((f) => (
                    <li key={f.id}>{f.name}</li>
                  ))}
                </ul>
              </>
            ) : (
              <span className="text-muted-foreground">No flows depend on this service.</span>
            )}
          </div>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              onClick={() => retireTarget && retireMut.mutate(retireTarget.id)}
            >
              {retireMut.isPending && <Loader2 className="h-4 w-4 animate-spin" />} Retire Service
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </AppLayout>
  );
}

// ------------------------------------------------------------ service card

function ServiceCard({
  svc,
  testing,
  onTest,
  onEdit,
  onRetire,
  onReinstate,
  reinstating,
}: {
  svc: AppService;
  testing: boolean;
  onTest: () => void;
  onEdit: () => void;
  onRetire: () => void;
  onReinstate: () => void;
  reinstating: boolean;
}) {
  const meta = SERVICE_TYPE_META[svc.type];
  const Icon = meta.icon;
  const deps = serviceDependents(svc.id);
  const summary = configSummary(svc);

  return (
    <Card className={svc.retired ? "opacity-70 bg-muted/30" : undefined}>
      <CardHeader className="flex-row items-start justify-between space-y-0 gap-2">
        <div className="flex items-start gap-3 min-w-0">
          <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-md bg-primary-muted text-primary">
            <Icon className="h-5 w-5" />
          </div>
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-2">
              <CardTitle className="text-base truncate">{svc.name}</CardTitle>
              <span className="rounded-full border px-2 py-0.5 text-xs font-mono text-muted-foreground">
                rev {svc.revision}
              </span>
              {svc.retired && <StatusBadge status="Retired" />}
            </div>
            <CardDescription>{meta.label}</CardDescription>
          </div>
        </div>
        <StatusBadge status={svc.health} />
      </CardHeader>
      <CardContent className="space-y-3">
        {summary.map((row) => (
          <div key={row.label} className="rounded-md border bg-muted/40 px-3 py-2">
            <div className="text-xs text-muted-foreground">{row.label}</div>
            <div className="truncate text-sm font-mono">{row.value}</div>
          </div>
        ))}
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div className="flex items-center gap-3 text-xs text-muted-foreground">
            <Popover>
              <PopoverTrigger asChild>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-7 px-2 text-xs"
                  disabled={deps.length === 0}
                  title={deps.length === 0 ? "No flows use this service" : "Show dependent flows"}
                >
                  <Workflow className="h-3.5 w-3.5" />
                  {deps.length} dependent flow{deps.length === 1 ? "" : "s"}
                </Button>
              </PopoverTrigger>
              <PopoverContent className="w-72 p-3" align="start">
                <div className="text-xs font-medium mb-2">Flows using this service</div>
                <div className="space-y-1">
                  {deps.map((f) => (
                    <Link
                      key={f.id}
                      to={`/flow-builder/${f.id}`}
                      className="flex items-center justify-between rounded-md border px-2 py-1.5 text-xs hover:bg-muted/50"
                    >
                      <span className="truncate">{f.name}</span>
                      <StatusBadge status={f.state} compact />
                    </Link>
                  ))}
                </div>
              </PopoverContent>
            </Popover>
            <span>Last tested: {timeAgo(svc.lastTestedAt)}</span>
          </div>
          <div className="flex flex-wrap gap-2">
            <Button variant="outline" size="sm" onClick={onTest} disabled={testing}>
              {testing ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <ShieldCheck className="h-3.5 w-3.5" />} Test
            </Button>
            <Button variant="outline" size="sm" onClick={onEdit}>
              <Pencil className="h-3.5 w-3.5" /> Edit
            </Button>
            {svc.retired ? (
              <Button variant="outline" size="sm" onClick={onReinstate} disabled={reinstating}>
                {reinstating ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <ArchiveRestore className="h-3.5 w-3.5" />} Reinstate
              </Button>
            ) : (
              <Button
                variant="outline"
                size="sm"
                className="text-destructive hover:text-destructive"
                onClick={onRetire}
              >
                <ArchiveX className="h-3.5 w-3.5" /> Retire
              </Button>
            )}
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
