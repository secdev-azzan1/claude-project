// APISIX Gateway — the proxy catalog reconciled onto the active APISIX
// platform connection.
//
// Proxies are self-serve: anyone can define one and reconcile it. The *host
// allowlist* is not — egress hosts are administrator-allowlisted, so every
// allowlist change goes through an explicit admin confirmation and lands in the
// audit trail. A proxy pointing at a host that is not allowlisted looks healthy
// but can never deploy, which is exactly the trap this page has to make loud.
//
// The platform connection itself stays on Platform Connections: that is
// infrastructure identity. This page is the catalog laid on top of it.

import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { AppLayout } from "@/components/AppLayout";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
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
import { Skeleton } from "@/components/ui/skeleton";
import { StatusBadge } from "@/components/StatusBadge";
import { cn } from "@/lib/utils";
import { timeAgo } from "@/lib/api";
import {
  deleteGatewayProxy,
  getGatewayResources,
  listConnections,
  listFlows,
  listGatewayProxies,
  proxyDependents,
  reconcileGatewayProxy,
  saveGatewayProxy,
  testGatewayProxy,
  updateGatewayResources,
  type ProxyTestResult,
} from "@/prototype/api";
import { blockProxyId } from "@/prototype/validation";
import type { Flow, GatewayProxy, GatewayResources } from "@/prototype/types";
import {
  AlertTriangle,
  CheckCircle2,
  Globe,
  KeyRound,
  Loader2,
  Pencil,
  Plus,
  RefreshCw,
  ShieldAlert,
  ShieldCheck,
  Trash2,
  Workflow,
  X,
  XCircle,
} from "lucide-react";
import { toast } from "sonner";

// ------------------------------------------------------------------ helpers

const HTTP_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"] as const;

const gwId = (prefix: string) => `${prefix}-${Date.now().toString(36)}${Math.random().toString(36).slice(2, 7)}`;

/** Bare hostname — no scheme, no port, no path. The port is its own field. */
const HOST_RE = /^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?(\.[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?)*$/;

/** Lowercase-only variant — the allowlist stores hosts verbatim, so casing has to be right at entry. */
const HOST_LOWER_RE = /^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*$/;

/** One reason the allowlist "add host" field is refused, or null. Empty input is not an error yet. */
function hostFieldError(raw: string): string | null {
  const host = raw.trim();
  if (!host) return null;
  if (/^[a-z]+:\/\//i.test(host)) return "Enter a bare hostname — drop the scheme (e.g. “https://”).";
  if (host.includes("/")) return "Enter a bare hostname — no path.";
  if (host.includes(":")) return "Enter a bare hostname — no port.";
  if (/[A-Z]/.test(host)) return "Hostnames are lowercase.";
  if (!HOST_LOWER_RE.test(host)) return "Not a valid hostname.";
  return null;
}

function msLabel(ms: number | undefined): string {
  if (typeof ms !== "number" || !Number.isFinite(ms)) return "default";
  return ms >= 1000 ? `${ms / 1000}s` : `${ms}ms`;
}

// --------------------------------------------------------------- form model

interface ProxyForm {
  name: string;
  description: string;
  targetHost: string;
  port: string;
  sni: string;
  connectTimeoutMs: string;
  readTimeoutMs: string;
  path: string;
  methods: string[];
  certProfileId: string; // "none" sentinel — radix forbids an empty SelectItem value
}

const emptyProxyForm = (): ProxyForm => ({
  name: "",
  description: "",
  targetHost: "",
  port: "443",
  sni: "",
  connectTimeoutMs: "5000",
  readTimeoutMs: "30000",
  path: "/",
  methods: ["GET"],
  certProfileId: "none",
});

function formFromProxy(p: GatewayProxy): ProxyForm {
  return {
    name: p.name,
    description: p.description ?? "",
    targetHost: p.targetHost,
    port: String(p.port),
    sni: p.sni ?? "",
    connectTimeoutMs: p.connectTimeoutMs != null ? String(p.connectTimeoutMs) : "",
    readTimeoutMs: p.readTimeoutMs != null ? String(p.readTimeoutMs) : "",
    path: p.path,
    methods: [...p.methods],
    certProfileId: p.certProfileId ?? "none",
  };
}

function optionalMs(raw: string): number | undefined {
  const trimmed = raw.trim();
  if (!trimmed) return undefined;
  const n = Number.parseInt(trimmed, 10);
  return Number.isFinite(n) ? n : undefined;
}

/** One reason Save is refused, or null. Mirrors the AppServices idiom. */
function proxySaveBlockReason(f: ProxyForm): string | null {
  if (!f.name.trim()) return "Name the proxy — flows reference it by name.";
  const host = f.targetHost.trim();
  if (!host) return "Set the target host.";
  if (/^[a-z]+:\/\//i.test(host)) return "Target host is a bare hostname — drop the scheme.";
  if (host.includes("/")) return "Target host is a bare hostname — the path belongs in the path field.";
  if (host.includes(":")) return "Target host is a bare hostname — the port belongs in the port field.";
  if (!HOST_RE.test(host)) return "Target host is not a valid hostname.";
  const port = Number.parseInt(f.port.trim(), 10);
  if (!Number.isFinite(port) || port < 1 || port > 65535) return "Port must be between 1 and 65535.";
  if (!f.path.trim()) return "Set the route path prefix.";
  if (!f.path.trim().startsWith("/")) return "The route path must start with “/”.";
  if (f.methods.length === 0) return "Allow at least one HTTP method.";
  for (const [label, raw] of [
    ["Connect timeout", f.connectTimeoutMs],
    ["Read timeout", f.readTimeoutMs],
  ] as const) {
    const trimmed = raw.trim();
    if (!trimmed) continue;
    const n = Number.parseInt(trimmed, 10);
    if (!Number.isFinite(n) || n <= 0) return `${label} must be a positive number of milliseconds.`;
  }
  return null;
}

interface CertForm {
  name: string;
  subject: string;
  expiresAt: string;
}

/** One reason per required field, keyed by field — drives inline hints on the create dialog. */
function certFieldErrors(c: CertForm): Partial<Record<"name" | "subject", string>> {
  const errors: Partial<Record<"name" | "subject", string>> = {};
  if (!c.name.trim()) errors.name = "Name the certificate profile.";
  if (!c.subject.trim()) errors.subject = "Set the subject — e.g. CN=api-client.example.corp.";
  return errors;
}

function proxyFromForm(f: ProxyForm, editing: GatewayProxy | null): GatewayProxy {
  const base: GatewayProxy = editing ?? {
    id: "",
    name: "",
    targetHost: "",
    port: 443,
    path: "/",
    methods: [],
    status: "Pending",
    createdAt: "",
    updatedAt: "",
  };
  return {
    ...base,
    name: f.name.trim(),
    description: f.description.trim() || undefined,
    targetHost: f.targetHost.trim(),
    port: Number.parseInt(f.port.trim(), 10),
    sni: f.sni.trim() || undefined,
    connectTimeoutMs: optionalMs(f.connectTimeoutMs),
    readTimeoutMs: optionalMs(f.readTimeoutMs),
    path: f.path.trim(),
    methods: HTTP_METHODS.filter((m) => f.methods.includes(m)),
    certProfileId: f.certProfileId === "none" ? null : f.certProfileId,
  };
}

// ------------------------------------------------------------ small fields

function TextField({
  label,
  value,
  onChange,
  placeholder,
  mono,
  hint,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  mono?: boolean;
  hint?: string;
}) {
  return (
    <div className="grid gap-1.5">
      <Label>{label}</Label>
      <Input
        value={value}
        placeholder={placeholder}
        className={mono ? "font-mono" : undefined}
        onChange={(e) => onChange(e.target.value)}
      />
      {hint && <p className="text-xs text-muted-foreground">{hint}</p>}
    </div>
  );
}

/** The admin action a confirm step is currently gating. */
type AdminAction =
  | { kind: "allow"; host: string; proxyId?: string; proxyName?: string }
  | { kind: "revoke"; host: string };

// -------------------------------------------------------------------- page

export default function Apisix() {
  const qc = useQueryClient();

  const { data: proxies = [], isLoading: proxiesLoading } = useQuery({
    queryKey: ["gateway-proxies"],
    queryFn: listGatewayProxies,
  });
  const { data: gateway, isLoading: gatewayLoading } = useQuery({
    queryKey: ["gateway"],
    queryFn: getGatewayResources,
  });
  const { data: connections = [] } = useQuery({
    queryKey: ["connections"],
    queryFn: listConnections,
  });
  const { data: flows = [] } = useQuery({ queryKey: ["flows"], queryFn: listFlows });

  const [formOpen, setFormOpen] = useState(false);
  const [editing, setEditing] = useState<GatewayProxy | null>(null);
  const [form, setForm] = useState<ProxyForm>(emptyProxyForm());
  const [deleteTarget, setDeleteTarget] = useState<GatewayProxy | null>(null);
  const [busyId, setBusyId] = useState<string | null>(null);
  const [testResults, setTestResults] = useState<Record<string, ProxyTestResult>>({});
  const [adminAction, setAdminAction] = useState<AdminAction | null>(null);
  const [newHost, setNewHost] = useState("");
  const [certFormOpen, setCertFormOpen] = useState(false);
  const [certAttempted, setCertAttempted] = useState(false);
  const [newCert, setNewCert] = useState({ name: "", subject: "", expiresAt: "" });
  const [certDeleteTarget, setCertDeleteTarget] = useState<string | null>(null);

  const allowlist = useMemo(() => gateway?.allowlist ?? [], [gateway]);
  const certProfiles = useMemo(() => gateway?.certProfiles ?? [], [gateway]);

  const apisixConn = connections.find((c) => c.type === "apisix" && c.active) ?? null;
  const inactiveApisix = connections.filter((c) => c.type === "apisix" && !c.active);

  /** Live ref count — the stored `refCount` goes stale the moment a proxy changes. */
  const certRefs = (certId: string) => proxies.filter((p) => p.certProfileId === certId).length;

  const invalidate = () => {
    qc.invalidateQueries({ queryKey: ["gateway-proxies"] });
    qc.invalidateQueries({ queryKey: ["gateway"] });
    qc.invalidateQueries({ queryKey: ["audit"] });
    qc.invalidateQueries({ queryKey: ["dashboard"] });
    qc.invalidateQueries({ queryKey: ["flows"] });
  };

  // ── Proxy mutations ───────────────────────────────────────────────────────

  const saveMut = useMutation({
    mutationFn: (p: GatewayProxy) => saveGatewayProxy(p),
    onSuccess: (saved, input) => {
      toast.success(
        input.id ? `Proxy "${saved.name}" saved` : `Proxy "${saved.name}" created`,
        saved.status === "Pending"
          ? { description: "Reconcile it to push the route and upstream onto the gateway." }
          : undefined,
      );
      setFormOpen(false);
      setEditing(null);
      invalidate();
    },
    onError: (e: Error) => toast.error(e.message),
  });

  const testMut = useMutation({
    mutationFn: (id: string) => testGatewayProxy(id),
    onMutate: (id) => setBusyId(id),
    onSuccess: (result, id) => {
      setTestResults((prev) => ({ ...prev, [id]: result }));
      if (result.ok) toast.success(result.detail);
      else toast.error(result.detail);
      qc.invalidateQueries({ queryKey: ["audit"] });
    },
    onError: (e: Error) => toast.error(e.message),
    onSettled: () => setBusyId(null),
  });

  const reconcileMut = useMutation({
    mutationFn: (id: string) => reconcileGatewayProxy(id),
    onMutate: (id) => setBusyId(id),
    onSuccess: (p) => {
      if (p.status === "Reconciled") toast.success(`"${p.name}" is live on the gateway`);
      else toast.error(p.statusDetail ?? `"${p.name}" failed to reconcile`);
      invalidate();
    },
    onError: (e: Error) => toast.error(e.message),
    onSettled: () => setBusyId(null),
  });

  const deleteMut = useMutation({
    mutationFn: (id: string) => deleteGatewayProxy(id),
    onSuccess: (_r, id) => {
      const name = proxies.find((p) => p.id === id)?.name ?? "Proxy";
      toast.warning(`Proxy "${name}" deleted`);
      setDeleteTarget(null);
      invalidate();
    },
    onError: (e: Error) => toast.error(e.message),
  });

  // ── Gateway-resource mutations (cert profiles + allowlist) ────────────────

  const gwMut = useMutation({
    mutationFn: (vars: { next: GatewayResources; message: string; tone?: "success" | "warning" }) =>
      updateGatewayResources(vars.next),
    onSuccess: (_saved, vars) => {
      if (vars.tone === "warning") toast.warning(vars.message);
      else toast.success(vars.message);
      invalidate();
    },
    onError: (e: Error) => toast.error(e.message),
  });

  /** Every gateway write re-derives refCount from the live catalog. */
  const nextResources = (patch: Partial<GatewayResources>): GatewayResources => {
    const base: GatewayResources = { certProfiles, allowlist, ...patch };
    return {
      ...base,
      certProfiles: base.certProfiles.map((cp) => ({ ...cp, refCount: certRefs(cp.id) })),
    };
  };

  const runAdminAction = (action: AdminAction) => {
    if (action.kind === "allow") {
      if (allowlist.includes(action.host)) {
        setAdminAction(null);
        return;
      }
      gwMut.mutate(
        {
          next: nextResources({ allowlist: [...allowlist, action.host] }),
          message: `Admin action recorded — "${action.host}" added to the gateway allowlist`,
        },
        {
          onSuccess: () => {
            if (!action.proxyId) return;
            toast.info(`"${action.proxyName}" can reconcile now that its host is allowlisted.`, {
              action: { label: "Reconcile", onClick: () => reconcileMut.mutate(action.proxyId!) },
            });
          },
        },
      );
    } else {
      const stranded = proxies.filter((p) => p.targetHost === action.host);
      gwMut.mutate({
        next: nextResources({ allowlist: allowlist.filter((h) => h !== action.host) }),
        message:
          `Admin action recorded — "${action.host}" removed from the gateway allowlist` +
          (stranded.length > 0 ? `; ${stranded.length} proxy/proxies can no longer deploy` : ""),
        tone: "warning",
      });
    }
    setAdminAction(null);
  };

  // ── Form plumbing ─────────────────────────────────────────────────────────

  const openCreate = () => {
    setEditing(null);
    setForm(emptyProxyForm());
    setFormOpen(true);
  };

  const openEdit = (p: GatewayProxy) => {
    setEditing(p);
    setForm(formFromProxy(p));
    setFormOpen(true);
  };

  const openCertCreate = () => {
    setNewCert({ name: "", subject: "", expiresAt: "" });
    setCertAttempted(false);
    setCertFormOpen(true);
  };

  const certErrors = certFieldErrors(newCert);
  const certBlocked = Object.keys(certErrors).length > 0;

  const submitCert = () => {
    setCertAttempted(true);
    if (certBlocked || !gateway) return;
    const expiresAt = newCert.expiresAt
      ? new Date(`${newCert.expiresAt}T00:00:00Z`).toISOString()
      : new Date(Date.now() + 365 * 24 * 3600 * 1000).toISOString();
    gwMut.mutate(
      {
        next: nextResources({
          certProfiles: [
            ...certProfiles,
            {
              id: gwId("gw-cert"),
              name: newCert.name.trim(),
              subject: newCert.subject.trim(),
              expiresAt,
              refCount: 0,
            },
          ],
        }),
        message: `Certificate profile "${newCert.name.trim()}" added`,
      },
      {
        onSuccess: () => {
          setCertFormOpen(false);
          setNewCert({ name: "", subject: "", expiresAt: "" });
          setCertAttempted(false);
        },
      },
    );
  };

  const blockReason = proxySaveBlockReason(form);
  const deleteDeps = deleteTarget ? proxyDependents(deleteTarget.id) : [];
  const certDeleteRefs = certDeleteTarget ? certRefs(certDeleteTarget) : 0;
  const certDeleteName = certProfiles.find((cp) => cp.id === certDeleteTarget)?.name ?? "";

  const hostTrimmed = newHost.trim();
  const hostError = hostFieldError(newHost);
  const hostDuplicate = hostTrimmed !== "" && allowlist.includes(hostTrimmed);
  const submitHost = () => {
    if (!hostTrimmed || hostDuplicate || hostError) return;
    setAdminAction({ kind: "allow", host: hostTrimmed });
  };

  const loading = proxiesLoading || gatewayLoading;

  return (
    <AppLayout
      title="APISIX Gateway"
      description="Named egress proxies an http block routes through. Proxy definitions are self-serve; the host allowlist is administrator-managed."
      actions={
        <Button onClick={openCreate}>
          <Plus className="h-4 w-4" /> Add Proxy
        </Button>
      }
    >
      <div className="space-y-8">
        {/* ── No-connection guard ─────────────────────────────────────────── */}
        {!apisixConn && (
          <Alert variant="destructive" className="py-3">
            <XCircle className="h-4 w-4" />
            <AlertDescription className="flex flex-wrap items-center gap-x-1.5 gap-y-1 text-xs">
              <span>
                No active APISIX connection — configure one under Platform Connections.
                {inactiveApisix.length > 0
                  ? ` ${inactiveApisix.length} APISIX connection${inactiveApisix.length === 1 ? " exists" : "s exist"} but ${inactiveApisix.length === 1 ? "is" : "are"} not active.`
                  : ""}
              </span>
              <Link to="/connections" className="font-medium underline underline-offset-2">
                Go to Platform Connections
              </Link>
            </AlertDescription>
          </Alert>
        )}

        {/* ── Proxies ─────────────────────────────────────────────────────── */}
        <section>
          <SectionHeading
            icon={Globe}
            title="Proxies"
            blurb="One proxy is one egress definition. An http block references it by id — deploy needs it reconciled and its host allowlisted."
            count={proxies.length}
            unit="proxy"
            plural="proxies"
          />
          {loading ? (
            <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
              <Skeleton className="h-56 rounded-lg" />
              <Skeleton className="h-56 rounded-lg" />
            </div>
          ) : proxies.length === 0 ? (
            <Card>
              <CardContent className="flex flex-col items-center justify-center gap-3 py-12 text-center">
                <div className="flex h-12 w-12 items-center justify-center rounded-md bg-primary-muted text-primary">
                  <Globe className="h-6 w-6" />
                </div>
                <div>
                  <h3 className="font-medium">No proxies yet</h3>
                  <p className="text-sm text-muted-foreground">
                    Define the first egress proxy — http blocks pick one from this catalog.
                  </p>
                </div>
                <Button onClick={openCreate}>
                  <Plus className="h-4 w-4" /> Add Proxy
                </Button>
              </CardContent>
            </Card>
          ) : (
            <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
              {proxies.map((p) => (
                <ProxyCard
                  key={p.id}
                  proxy={p}
                  flows={flows}
                  allowlisted={allowlist.includes(p.targetHost)}
                  certName={certProfiles.find((cp) => cp.id === p.certProfileId)?.name ?? null}
                  gatewayReachable={!!apisixConn && apisixConn.health === "Healthy"}
                  busy={busyId === p.id}
                  lastTest={testResults[p.id] ?? null}
                  onTest={() => testMut.mutate(p.id)}
                  onReconcile={() => reconcileMut.mutate(p.id)}
                  onEdit={() => openEdit(p)}
                  onDelete={() => setDeleteTarget(p)}
                  onRequestAllowlist={() =>
                    setAdminAction({ kind: "allow", host: p.targetHost, proxyId: p.id, proxyName: p.name })
                  }
                />
              ))}
            </div>
          )}
        </section>

        {/* ── Certificate profiles ────────────────────────────────────────── */}
        <section>
          <SectionHeading
            icon={KeyRound}
            title="Certificate profiles"
            blurb="Client certificates presented on egress. Shared — a profile may back several proxies."
            count={certProfiles.length}
            unit="profile"
            plural="profiles"
            action={
              <Button size="sm" variant="outline" onClick={openCertCreate}>
                <Plus className="h-3.5 w-3.5" /> Add certificate
              </Button>
            }
          />
          <Card>
            <CardContent className="space-y-2 pt-6">
              {gatewayLoading && <Skeleton className="h-10 w-full" />}
              {!gatewayLoading && certProfiles.length === 0 && (
                <p className="text-xs text-muted-foreground">
                  No certificate profiles — proxies will connect without a client certificate.
                </p>
              )}
              {certProfiles.map((cp) => {
                const refs = certRefs(cp.id);
                const expired = new Date(cp.expiresAt).getTime() < Date.now();
                return (
                  <div key={cp.id} className="flex flex-wrap items-center gap-3 rounded-md border px-3 py-2 text-sm">
                    <span className="min-w-0 truncate font-medium">{cp.name}</span>
                    <span className="min-w-0 truncate font-mono text-xs text-muted-foreground">{cp.subject}</span>
                    <span
                      className={cn(
                        "whitespace-nowrap text-xs",
                        expired ? "font-medium text-destructive" : "text-muted-foreground",
                      )}
                    >
                      {expired ? "expired " : "expires "}
                      {new Date(cp.expiresAt).toLocaleDateString()}
                    </span>
                    <Badge variant="secondary" className="ml-auto whitespace-nowrap">
                      {refs} ref{refs === 1 ? "" : "s"}
                    </Badge>
                    <Button
                      size="sm"
                      variant="ghost"
                      className="text-destructive hover:text-destructive"
                      disabled={refs > 0 || gwMut.isPending}
                      title={
                        refs > 0
                          ? `Referenced by ${refs} prox${refs === 1 ? "y" : "ies"} — re-point those proxies first.`
                          : `Delete ${cp.name}`
                      }
                      onClick={() => setCertDeleteTarget(cp.id)}
                    >
                      <Trash2 className="h-3.5 w-3.5" />
                    </Button>
                  </div>
                );
              })}
            </CardContent>
          </Card>
        </section>

        {/* ── Host allowlist ──────────────────────────────────────────────── */}
        <section>
          <SectionHeading
            icon={ShieldAlert}
            title="Host allowlist"
            blurb="Only these hosts may be reached through the gateway. Administrator-managed — every change is confirmed and audited."
            count={allowlist.length}
            unit="host"
            plural="hosts"
          />
          <Card>
            <CardContent className="space-y-3 pt-6">
              <div className="flex items-start gap-2 rounded-md border border-warning/30 bg-warning-muted px-3 py-2 text-xs">
                <ShieldAlert className="mt-0.5 h-3.5 w-3.5 shrink-0 text-warning" />
                <span>
                  Adding or removing a host is an <strong>administrator action</strong>. Proxy definitions are
                  self-serve, but the set of hosts the platform may egress to is not — each change asks for an
                  explicit confirmation and is written to the audit log.
                </span>
              </div>
              {gatewayLoading && <Skeleton className="h-8 w-2/3" />}
              <div className="flex flex-wrap gap-2">
                {!gatewayLoading && allowlist.length === 0 && (
                  <p className="text-xs text-muted-foreground">
                    The allowlist is empty — all egress is blocked and no proxy can reconcile.
                  </p>
                )}
                {allowlist.map((host) => {
                  const users = proxies.filter((p) => p.targetHost === host).length;
                  return (
                    <Badge key={host} variant="secondary" className="gap-1.5 font-mono font-normal">
                      {host}
                      <span className="text-xs text-muted-foreground">
                        · {users} prox{users === 1 ? "y" : "ies"}
                      </span>
                      <button
                        type="button"
                        aria-label={`Remove ${host} from the allowlist (admin action)`}
                        title={`Remove ${host} — administrator action`}
                        className="ml-0.5 hover:text-destructive"
                        onClick={() => setAdminAction({ kind: "revoke", host })}
                      >
                        <X className="h-3 w-3" />
                      </button>
                    </Badge>
                  );
                })}
              </div>
              <div className="space-y-1.5">
                <div className="flex flex-wrap items-center gap-2 rounded-md border bg-muted/20 p-2">
                  <Input
                    className="w-64 font-mono"
                    placeholder="host.example.corp"
                    aria-label="Host to allowlist"
                    value={newHost}
                    onChange={(e) => setNewHost(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key !== "Enter") return;
                      e.preventDefault();
                      submitHost();
                    }}
                  />
                  <Button
                    size="sm"
                    variant="outline"
                    disabled={!gateway || !hostTrimmed || hostDuplicate || !!hostError || gwMut.isPending}
                    title={
                      !hostTrimmed
                        ? "Enter a host to allowlist."
                        : (hostError ?? (hostDuplicate ? "Host is already on the allowlist." : "Administrator action — you will be asked to confirm."))
                    }
                    onClick={submitHost}
                  >
                    <ShieldCheck className="h-3.5 w-3.5" /> Add host (admin)
                  </Button>
                </div>
                {hostTrimmed && (hostError || hostDuplicate) && (
                  <p className="text-xs text-destructive">{hostError ?? "Host is already on the allowlist."}</p>
                )}
                <p className="text-xs text-muted-foreground">
                  Administrator approval required — additions are audited.
                </p>
              </div>
            </CardContent>
          </Card>
        </section>
      </div>

      {/* ─────────────────────────────────────────── create / edit proxy ── */}
      <Dialog
        open={formOpen}
        onOpenChange={(open) => {
          setFormOpen(open);
          if (!open) setEditing(null);
        }}
      >
        <DialogContent className="max-h-[85vh] max-w-lg overflow-y-auto">
          <DialogHeader>
            <DialogTitle>{editing ? `Edit ${editing.name}` : "Add APISIX Proxy"}</DialogTitle>
            <DialogDescription>
              {editing
                ? "Changing the target, path, methods or certificate puts the proxy back to Pending — reconcile to push it onto the gateway."
                : "A new proxy starts Pending. Reconcile it once the definition is right; deploy also needs its host on the allowlist."}
            </DialogDescription>
          </DialogHeader>

          <div className="grid gap-3">
            <TextField label="Name" value={form.name} onChange={(name) => setForm((p) => ({ ...p, name }))} />
            <TextField
              label="Description"
              value={form.description}
              placeholder="What this egress is for"
              onChange={(description) => setForm((p) => ({ ...p, description }))}
            />
            <div className="grid grid-cols-[1fr_110px] gap-3">
              <TextField
                label="Target host"
                mono
                value={form.targetHost}
                placeholder="api.example.corp"
                onChange={(targetHost) => setForm((p) => ({ ...p, targetHost }))}
              />
              <TextField label="Port" mono value={form.port} onChange={(port) => setForm((p) => ({ ...p, port }))} />
            </div>
            {form.targetHost.trim() && !allowlist.includes(form.targetHost.trim()) && (
              <div className="flex items-start gap-2 rounded-md border border-warning/30 bg-warning-muted px-3 py-2 text-xs">
                <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-warning" />
                <span>
                  <span className="font-mono">{form.targetHost.trim()}</span> is not on the gateway allowlist. You can
                  still save the proxy, but it will not reconcile and no flow using it can deploy until an
                  administrator allowlists the host.
                </span>
              </div>
            )}
            <TextField
              label="SNI"
              mono
              value={form.sni}
              placeholder="Defaults to the target host"
              hint="Server name sent in the TLS handshake when it differs from the target host."
              onChange={(sni) => setForm((p) => ({ ...p, sni }))}
            />
            <div className="grid grid-cols-2 gap-3">
              <TextField
                label="Connect timeout (ms)"
                mono
                value={form.connectTimeoutMs}
                placeholder="5000"
                onChange={(connectTimeoutMs) => setForm((p) => ({ ...p, connectTimeoutMs }))}
              />
              <TextField
                label="Read timeout (ms)"
                mono
                value={form.readTimeoutMs}
                placeholder="30000"
                onChange={(readTimeoutMs) => setForm((p) => ({ ...p, readTimeoutMs }))}
              />
            </div>
            <TextField
              label="Route path prefix"
              mono
              value={form.path}
              placeholder="/v2/indicators"
              hint="Requests from http blocks are matched under this prefix."
              onChange={(path) => setForm((p) => ({ ...p, path }))}
            />
            <div className="grid gap-1.5">
              <Label>Allowed methods</Label>
              <div className="flex flex-wrap gap-2">
                {HTTP_METHODS.map((m) => {
                  const on = form.methods.includes(m);
                  return (
                    <button
                      key={m}
                      type="button"
                      aria-pressed={on}
                      onClick={() =>
                        setForm((p) => ({
                          ...p,
                          methods: on ? p.methods.filter((x) => x !== m) : [...p.methods, m],
                        }))
                      }
                      className={cn(
                        "rounded-md border px-2.5 py-1 font-mono text-xs transition hover:bg-muted/50",
                        on && "border-primary bg-primary-muted text-primary",
                      )}
                    >
                      {m}
                    </button>
                  );
                })}
              </div>
            </div>
            <div className="grid gap-1.5">
              <Label>Client certificate profile</Label>
              <Select
                value={form.certProfileId}
                onValueChange={(certProfileId) => setForm((p) => ({ ...p, certProfileId }))}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="none">No client certificate</SelectItem>
                  {certProfiles.map((cp) => (
                    <SelectItem key={cp.id} value={cp.id}>
                      {cp.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <p className="text-xs text-muted-foreground">
                Gateway mode does not verify the upstream server certificate. Egress only.
              </p>
            </div>
          </div>

          <DialogFooter>
            <Button variant="outline" onClick={() => setFormOpen(false)}>
              Cancel
            </Button>
            <Button
              onClick={() => saveMut.mutate(proxyFromForm(form, editing))}
              disabled={!!blockReason || saveMut.isPending}
              title={blockReason ?? undefined}
            >
              {saveMut.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
              {editing ? "Save proxy" : "Create Proxy"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* ──────────────────────────────────────── create certificate profile ── */}
      <Dialog
        open={certFormOpen}
        onOpenChange={(open) => {
          setCertFormOpen(open);
          if (!open) {
            setNewCert({ name: "", subject: "", expiresAt: "" });
            setCertAttempted(false);
          }
        }}
      >
        <DialogContent className="max-w-lg">
          <DialogHeader>
            <DialogTitle>Add certificate profile</DialogTitle>
            <DialogDescription>
              A client certificate profile is presented on egress for mutual TLS toward upstreams. It is shared —
              any number of proxies may reference the same profile.
            </DialogDescription>
          </DialogHeader>

          <div className="grid gap-3">
            <div className="grid gap-1.5">
              <TextField
                label="Name"
                value={newCert.name}
                placeholder="prod-client-cert"
                onChange={(name) => setNewCert((p) => ({ ...p, name }))}
              />
              {certAttempted && certErrors.name && <p className="text-xs text-destructive">{certErrors.name}</p>}
            </div>
            <div className="grid gap-1.5">
              <TextField
                label="Subject"
                mono
                value={newCert.subject}
                placeholder="CN=api-client.example.corp"
                onChange={(subject) => setNewCert((p) => ({ ...p, subject }))}
              />
              {certAttempted && certErrors.subject && (
                <p className="text-xs text-destructive">{certErrors.subject}</p>
              )}
            </div>
            <div className="grid gap-1.5">
              <Label>Expiry date</Label>
              <Input
                className="w-full"
                type="date"
                aria-label="Expiry date"
                value={newCert.expiresAt}
                onChange={(e) => setNewCert((p) => ({ ...p, expiresAt: e.target.value }))}
              />
              <p className="text-xs text-muted-foreground">Defaults to 365 days from today if left blank.</p>
            </div>
          </div>

          <DialogFooter>
            <Button variant="outline" onClick={() => setCertFormOpen(false)}>
              Cancel
            </Button>
            <Button onClick={submitCert} disabled={gwMut.isPending}>
              {gwMut.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
              Create profile
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* ───────────────────────────────────────────────── delete proxy ── */}
      <AlertDialog open={!!deleteTarget} onOpenChange={(open) => !open && setDeleteTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete "{deleteTarget?.name}"?</AlertDialogTitle>
            <AlertDialogDescription>
              The route and upstream are removed from the gateway. Blocks referencing this proxy keep a dangling id
              and stop validating, so the flows below have to be re-pointed first.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <div
            className={cn(
              "rounded-md border p-3 text-sm",
              deleteDeps.length > 0 ? "border-destructive/20 bg-destructive-muted" : "border-border bg-muted/40",
            )}
          >
            {deleteDeps.length > 0 ? (
              <>
                <div className="font-medium text-destructive">
                  {deleteDeps.length} flow{deleteDeps.length === 1 ? "" : "s"} route through this proxy — delete is
                  refused:
                </div>
                <ul className="mt-1 list-disc pl-5 text-xs text-muted-foreground">
                  {deleteDeps.map((name) => (
                    <li key={name}>{name}</li>
                  ))}
                </ul>
              </>
            ) : (
              <span className="text-muted-foreground">No flows route through this proxy.</span>
            )}
          </div>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              disabled={deleteDeps.length > 0 || deleteMut.isPending}
              onClick={(e) => {
                e.preventDefault();
                if (deleteTarget) deleteMut.mutate(deleteTarget.id);
              }}
            >
              {deleteMut.isPending && <Loader2 className="h-4 w-4 animate-spin" />} Delete Proxy
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* ─────────────────────────────────────────── delete cert profile ── */}
      <AlertDialog open={!!certDeleteTarget} onOpenChange={(open) => !open && setCertDeleteTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete certificate profile "{certDeleteName}"?</AlertDialogTitle>
            <AlertDialogDescription>
              {certDeleteRefs > 0
                ? `${certDeleteRefs} prox${certDeleteRefs === 1 ? "y" : "ies"} still present this certificate — re-point them first.`
                : "No proxy presents this certificate. Removing it is safe."}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              disabled={certDeleteRefs > 0 || gwMut.isPending}
              onClick={(e) => {
                e.preventDefault();
                if (!certDeleteTarget) return;
                gwMut.mutate({
                  next: nextResources({ certProfiles: certProfiles.filter((cp) => cp.id !== certDeleteTarget) }),
                  message: `Certificate profile "${certDeleteName}" deleted`,
                  tone: "warning",
                });
                setCertDeleteTarget(null);
              }}
            >
              {gwMut.isPending && <Loader2 className="h-4 w-4 animate-spin" />} Delete Profile
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* ────────────────────────────────────────── admin allowlist gate ── */}
      <AlertDialog open={!!adminAction} onOpenChange={(open) => !open && setAdminAction(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle className="flex items-center gap-2">
              <ShieldAlert className="h-4 w-4 text-warning" />
              Administrator action
            </AlertDialogTitle>
            <AlertDialogDescription>
              Egress hosts are administrator-allowlisted. You are acting as <strong>admin</strong>; this change is
              written to the audit log.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <div className="rounded-md border border-warning/30 bg-warning-muted p-3 text-sm">
            {adminAction?.kind === "allow" ? (
              <>
                <div className="font-medium">
                  Allow egress to <span className="font-mono">{adminAction.host}</span>
                </div>
                <p className="mt-1 text-xs text-muted-foreground">
                  Every proxy targeting this host becomes reconcilable, and flows using those proxies clear the
                  allowlist deploy check.
                  {adminAction.proxyName ? ` Requested from proxy "${adminAction.proxyName}".` : ""}
                </p>
              </>
            ) : adminAction ? (
              <>
                <div className="font-medium">
                  Revoke egress to <span className="font-mono">{adminAction.host}</span>
                </div>
                <p className="mt-1 text-xs text-muted-foreground">
                  {(() => {
                    const n = proxies.filter((p) => p.targetHost === adminAction.host).length;
                    return n > 0
                      ? `${n} prox${n === 1 ? "y" : "ies"} target this host. They keep their definition but can no longer reconcile, and flows using them stop passing preflight.`
                      : "No proxy targets this host today.";
                  })()}
                </p>
              </>
            ) : null}
          </div>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              disabled={gwMut.isPending}
              onClick={(e) => {
                e.preventDefault();
                if (!adminAction) return;
                if (adminAction.kind === "allow") setNewHost("");
                runAdminAction(adminAction);
              }}
            >
              {gwMut.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
              {adminAction?.kind === "revoke" ? "Confirm as admin — remove host" : "Confirm as admin — add host"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </AppLayout>
  );
}

// ------------------------------------------------------------- subcomponents

function SectionHeading({
  icon: Icon,
  title,
  blurb,
  count,
  unit,
  plural,
  action,
}: {
  icon: React.ComponentType<{ className?: string }>;
  title: string;
  blurb: string;
  count: number;
  unit: string;
  plural: string;
  action?: React.ReactNode;
}) {
  return (
    <div className="mb-3 flex flex-wrap items-center gap-2">
      <span className="flex h-7 w-7 items-center justify-center rounded-md bg-primary-muted text-primary">
        <Icon className="h-4 w-4" />
      </span>
      <h2 className="text-sm font-semibold">{title}</h2>
      <span className="text-xs text-muted-foreground">
        · {count} {count === 1 ? unit : plural} — {blurb}
      </span>
      {action && <span className="ml-auto">{action}</span>}
    </div>
  );
}

function ProxyCard({
  proxy,
  flows,
  allowlisted,
  certName,
  gatewayReachable,
  busy,
  lastTest,
  onTest,
  onReconcile,
  onEdit,
  onDelete,
  onRequestAllowlist,
}: {
  proxy: GatewayProxy;
  flows: Flow[];
  allowlisted: boolean;
  certName: string | null;
  gatewayReachable: boolean;
  busy: boolean;
  lastTest: ProxyTestResult | null;
  onTest: () => void;
  onReconcile: () => void;
  onEdit: () => void;
  onDelete: () => void;
  onRequestAllowlist: () => void;
}) {
  const depNames = proxyDependents(proxy.id);
  const depFlows = flows.filter((f) => f.blocks.some((b) => blockProxyId(b) === proxy.id));
  const deployable = proxy.status === "Reconciled" && allowlisted;

  return (
    <Card>
      <CardHeader className="flex-row items-start justify-between gap-2 space-y-0">
        <div className="flex min-w-0 items-start gap-3">
          <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-md bg-primary-muted text-primary">
            <Globe className="h-5 w-5" />
          </div>
          <div className="min-w-0">
            <CardTitle className="truncate text-base">{proxy.name}</CardTitle>
            <CardDescription className="line-clamp-2">
              {proxy.description ?? "No description."}
            </CardDescription>
          </div>
        </div>
        <StatusBadge status={proxy.status} />
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="space-y-1 rounded-md border bg-muted/40 px-3 py-2">
          <div className="flex items-baseline justify-between gap-3 text-xs">
            <span className="shrink-0 text-muted-foreground">Target</span>
            <span className="truncate font-mono">
              {proxy.targetHost}:{proxy.port}
              {proxy.path}
            </span>
          </div>
          <div className="flex items-baseline justify-between gap-3 text-xs">
            <span className="shrink-0 text-muted-foreground">Methods</span>
            <span className="truncate font-mono">{proxy.methods.join(" · ") || "—"}</span>
          </div>
          <div className="flex items-baseline justify-between gap-3 text-xs">
            <span className="shrink-0 text-muted-foreground">SNI</span>
            <span className="truncate font-mono">{proxy.sni || proxy.targetHost}</span>
          </div>
          <div className="flex items-baseline justify-between gap-3 text-xs">
            <span className="shrink-0 text-muted-foreground">Timeouts</span>
            <span className="truncate font-mono">
              connect {msLabel(proxy.connectTimeoutMs)} · read {msLabel(proxy.readTimeoutMs)}
            </span>
          </div>
          <div className="flex items-baseline justify-between gap-3 text-xs">
            <span className="shrink-0 text-muted-foreground">Client cert</span>
            <span className="truncate font-mono">{certName ?? "none"}</span>
          </div>
        </div>

        {/* Deployability — the two facts that silently block a deploy. */}
        {!allowlisted && (
          <div className="space-y-2 rounded-md border border-warning/30 bg-warning-muted p-3 text-xs">
            <div className="flex items-start gap-2">
              <ShieldAlert className="mt-0.5 h-3.5 w-3.5 shrink-0 text-warning" />
              <span>
                <span className="font-mono">{proxy.targetHost}</span> is <strong>not on the gateway allowlist</strong>.
                This proxy cannot reconcile, and no flow using it can deploy — even while it reads as{" "}
                {proxy.status}.
              </span>
            </div>
            <Button size="sm" variant="outline" className="h-7 text-xs" onClick={onRequestAllowlist}>
              <ShieldCheck className="h-3.5 w-3.5" /> Add to allowlist (admin)
            </Button>
          </div>
        )}
        {allowlisted && proxy.status !== "Reconciled" && (
          <div className="flex items-start gap-2 rounded-md border border-warning/30 bg-warning-muted px-3 py-2 text-xs">
            <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-warning" />
            <span>{proxy.statusDetail ?? `The proxy is ${proxy.status} — reconcile it onto the gateway.`}</span>
          </div>
        )}
        {deployable && (
          <div className="flex items-start gap-2 rounded-md border border-success/20 bg-success-muted px-3 py-2 text-xs">
            <CheckCircle2 className="mt-0.5 h-3.5 w-3.5 shrink-0 text-success" />
            <span>Reconciled and allowlisted — flows routing through this proxy pass the gateway checks.</span>
          </div>
        )}
        {lastTest && (
          <div
            className={cn(
              "flex items-start gap-2 rounded-md border px-3 py-2 text-xs",
              lastTest.ok
                ? "border-success/20 bg-success-muted"
                : "border-destructive/20 bg-destructive-muted",
            )}
          >
            {lastTest.ok ? (
              <CheckCircle2 className="mt-0.5 h-3.5 w-3.5 shrink-0 text-success" />
            ) : (
              <XCircle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-destructive" />
            )}
            <span>
              {lastTest.detail} <span className="text-muted-foreground">({timeAgo(lastTest.testedAt)})</span>
            </span>
          </div>
        )}

        <div className="flex flex-wrap items-center justify-between gap-2">
          <div className="flex items-center gap-3 text-xs text-muted-foreground">
            <Popover>
              <PopoverTrigger asChild>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-7 px-2 text-xs"
                  disabled={depFlows.length === 0}
                  title={depNames.length === 0 ? "No flows use this proxy" : "Show dependent flows"}
                >
                  <Workflow className="h-3.5 w-3.5" />
                  Used by {depNames.length} flow{depNames.length === 1 ? "" : "s"}
                </Button>
              </PopoverTrigger>
              <PopoverContent className="w-72 p-3" align="start">
                <div className="mb-2 text-xs font-medium">Flows routing through this proxy</div>
                <div className="space-y-1">
                  {depFlows.map((f) => (
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
            <span>Updated {timeAgo(proxy.updatedAt)}</span>
          </div>
          <div className="flex flex-wrap gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={onTest}
              disabled={busy}
              title={gatewayReachable ? undefined : "No healthy active APISIX connection — the probe will fail."}
            >
              {busy ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <ShieldCheck className="h-3.5 w-3.5" />} Test
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={onReconcile}
              disabled={busy}
              title="Push this definition onto the gateway"
            >
              {busy ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RefreshCw className="h-3.5 w-3.5" />}{" "}
              Reconcile
            </Button>
            <Button variant="outline" size="sm" onClick={onEdit}>
              <Pencil className="h-3.5 w-3.5" /> Edit
            </Button>
            <Button
              variant="outline"
              size="sm"
              className="text-destructive hover:text-destructive"
              onClick={onDelete}
              title={
                depNames.length > 0
                  ? `${depNames.length} flow(s) route through this proxy — delete is refused.`
                  : `Delete ${proxy.name}`
              }
            >
              <Trash2 className="h-3.5 w-3.5" /> Delete
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
