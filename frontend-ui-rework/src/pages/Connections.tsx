// Platform Connections — adapter-prototype rebuild.
// At most one saved connection per type; health and
// reachability recorded as two separate facts; manual Test only (no polling);
// Activate is guarded by deployed-flow dependents. The APISIX *catalog*
// (proxies, cert profiles, host allowlist) lives on its own /apisix page — this
// page owns the connection, which is infrastructure identity, and links across.

import { useState, type InputHTMLAttributes } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { AppLayout } from "@/components/AppLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { StatusBadge } from "@/components/StatusBadge";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
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
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  ArrowLeft,
  CheckCircle2,
  Database,
  FileCode2,
  Globe,
  Layers,
  Loader2,
  Pencil,
  Plug,
  Plus,
  RefreshCw,
  Trash2,
  Workflow,
} from "lucide-react";
import { toast } from "sonner";
import { timeAgo } from "@/lib/api";
import {
  activateConnection,
  checkNifiPlatformServices,
  connectionDependents,
  deleteConnection,
  listConnections,
  saveConnection,
  testConnection,
  type ConnectionTestResult,
} from "@/prototype/api";
import type {
  ConnectionType,
  PlatformConnection,
} from "@/prototype/types";

// ─── Type metadata ───────────────────────────────────────────────────────────

const TYPE_ORDER: ConnectionType[] = ["nifi", "kafka", "apicurio", "kafka_connect", "redis", "apisix"];

const TYPE_META: Record<
  ConnectionType,
  { label: string; description: string; icon: React.ComponentType<{ className?: string }> }
> = {
  nifi: { label: "Apache NiFi", description: "Flow runtime — deploys and runs the generated process groups.", icon: Workflow },
  kafka: { label: "Apache Kafka", description: "Message backbone for raw and DLQ topics.", icon: Database },
  apicurio: { label: "Apicurio Schema Registry", description: "Schema approvals register subjects here.", icon: FileCode2 },
  kafka_connect: { label: "Kafka Connect", description: "Runs the Iceberg and OpenSearch sink connectors.", icon: Plug },
  redis: { label: "Redis", description: "Dedup windows and jdbc bookmarks (standalone).", icon: Layers },
  apisix: { label: "API Gateway (APISIX)", description: "Egress gateway for proxied HTTP sources.", icon: Globe },
};

// ─── Draft model (dynamic per-type forms) ────────────────────────────────────

type Draft = Record<string, string>;
type DraftErrors = Record<string, string>;

const str = (v: unknown, fallback = ""): string =>
  typeof v === "string" ? v : typeof v === "number" ? String(v) : fallback;

function defaultDraft(type: ConnectionType): Draft {
  switch (type) {
    case "nifi":
      return { name: "", url: "", authMode: "bearer", token: "", username: "", password: "" };
    case "kafka":
      return {
        name: "",
        bootstrapServers: "",
        mode: "native",
        proxyUrl: "",
        kafbatUsername: "",
        kafbatPassword: "",
        securityProtocol: "SASL_SSL",
        saslUsername: "",
        saslPassword: "",
      };
    case "apicurio":
      return { name: "", url: "", authMode: "none", username: "", password: "", token: "" };
    case "kafka_connect":
      return { name: "", url: "" };
    case "redis":
      return { name: "", host: "", port: "6379", dedupDb: "0", bookmarksDb: "1", password: "" };
    case "apisix":
      return { name: "", adminUrl: "", runtimeUrl: "", adminKey: "" };
  }
}

function connectionToDraft(conn: PlatformConnection): Draft {
  const base = defaultDraft(conn.type);
  const c = conn.config;
  switch (conn.type) {
    case "nifi":
      return { ...base, name: conn.name, url: str(c.url), authMode: str(c.authMode, "bearer"), username: str(c.username) };
    case "kafka":
      return {
        ...base,
        name: conn.name,
        bootstrapServers: str(c.bootstrapServers),
        mode: str(c.mode, "native"),
        proxyUrl: str(c.proxyUrl),
        kafbatUsername: str(c.kafbatUsername),
        securityProtocol: str(c.securityProtocol, "PLAINTEXT"),
        saslUsername: str(c.saslUsername),
      };
    case "apicurio":
      return { ...base, name: conn.name, url: str(c.url), authMode: str(c.authMode, "none"), username: str(c.username) };
    case "kafka_connect":
      return { ...base, name: conn.name, url: str(c.url) };
    case "redis":
      return {
        ...base,
        name: conn.name,
        host: str(c.host),
        port: str(c.port, "6379"),
        dedupDb: str(c.dedupDb, "0"),
        bookmarksDb: str(c.bookmarksDb, "1"),
      };
    case "apisix":
      return { ...base, name: conn.name, adminUrl: str(c.adminUrl), runtimeUrl: str(c.runtimeUrl) };
  }
}

function validateDraft(type: ConnectionType, d: Draft, isEdit: boolean): DraftErrors {
  const errors: DraftErrors = {};
  if (!d.name.trim()) errors.name = "Name is required.";
  switch (type) {
    case "nifi":
      if (!d.url.trim()) errors.url = "URL is required.";
      if (d.authMode === "basic" && !d.username.trim()) errors.username = "Username is required for basic auth.";
      if (!isEdit && d.authMode === "bearer" && !d.token.trim()) errors.token = "Bearer token is required when creating this connection.";
      if (!isEdit && d.authMode === "basic" && !d.password.trim()) errors.password = "Password is required when creating this connection.";
      break;
    case "kafka":
      if (!d.bootstrapServers.trim()) errors.bootstrapServers = "Bootstrap servers are required.";
      if (d.mode === "kafbat" && !d.proxyUrl.trim()) errors.proxyUrl = "Proxy URL is required in kafbat mode.";
      if (d.securityProtocol.startsWith("SASL") && !d.saslUsername.trim()) errors.saslUsername = "SASL username is required for SASL protocols.";
      break;
    case "apicurio":
      if (!d.url.trim()) errors.url = "URL is required.";
      if (d.authMode === "basic" && !d.username.trim()) errors.username = "Username is required for basic auth.";
      if (!isEdit && d.authMode === "basic" && !d.password.trim()) errors.password = "Password is required when creating this connection.";
      if (!isEdit && d.authMode === "bearer" && !d.token.trim()) errors.token = "Bearer token is required when creating this connection.";
      break;
    case "kafka_connect":
      if (!d.url.trim()) errors.url = "URL is required.";
      break;
    case "redis": {
      if (!d.host.trim()) errors.host = "Host is required.";
      const port = Number(d.port);
      if (!Number.isInteger(port) || port <= 0 || port > 65535) errors.port = "Port must be 1–65535.";
      const dedup = Number(d.dedupDb);
      const bookmarks = Number(d.bookmarksDb);
      if (!Number.isInteger(dedup) || dedup < 0) errors.dedupDb = "Logical DB must be a non-negative number.";
      if (!Number.isInteger(bookmarks) || bookmarks < 0) errors.bookmarksDb = "Logical DB must be a non-negative number.";
      if (!errors.dedupDb && !errors.bookmarksDb && dedup === bookmarks)
        errors.bookmarksDb = "Dedup and bookmarks must use different logical databases.";
      break;
    }
    case "apisix":
      if (!d.adminUrl.trim()) errors.adminUrl = "Admin URL is required.";
      if (!d.runtimeUrl.trim()) errors.runtimeUrl = "Runtime URL is required.";
      break;
  }
  return errors;
}

/**
 * Build the PlatformConnection to persist. Unlike the old localStorage mock
 * (which never stored secret values, only a `hasSecret` flag), the real
 * backend needs the actual secret text to call out with — so when the user
 * typed something, the value itself is written into `config` under the
 * field name the backend resolves for that type/auth-mode; a blank secret
 * field is simply omitted from `config`, and the backend's own "blank keeps
 * the existing value" merge takes over on update.
 */
function draftToConnection(type: ConnectionType, d: Draft, existing?: PlatformConnection): PlatformConnection {
  let config: Record<string, unknown> = {};
  let secretEntered = false;
  switch (type) {
    case "nifi":
      config = { url: d.url.trim(), authMode: d.authMode };
      if (d.authMode === "basic" && d.username.trim()) config.username = d.username.trim();
      if (d.authMode === "bearer" && d.token.trim()) {
        config.token = d.token.trim();
        secretEntered = true;
      }
      if (d.authMode === "basic" && d.password.trim()) {
        config.password = d.password.trim();
        secretEntered = true;
      }
      break;
    case "kafka":
      config = { bootstrapServers: d.bootstrapServers.trim(), mode: d.mode, securityProtocol: d.securityProtocol };
      if (d.mode === "kafbat") {
        config.proxyUrl = d.proxyUrl.trim();
        if (d.kafbatUsername.trim()) config.kafbatUsername = d.kafbatUsername.trim();
        if (d.kafbatPassword.trim()) {
          config.kafbatPassword = d.kafbatPassword.trim();
          secretEntered = true;
        }
      }
      if (d.saslUsername.trim()) config.saslUsername = d.saslUsername.trim();
      if (d.saslPassword.trim()) {
        config.saslPassword = d.saslPassword.trim();
        secretEntered = true;
      }
      break;
    case "apicurio":
      config = { url: d.url.trim(), authMode: d.authMode };
      if (d.authMode === "basic" && d.username.trim()) config.username = d.username.trim();
      if (d.authMode === "bearer" && d.token.trim()) {
        config.token = d.token.trim();
        secretEntered = true;
      }
      if (d.authMode === "basic" && d.password.trim()) {
        config.password = d.password.trim();
        secretEntered = true;
      }
      break;
    case "kafka_connect":
      config = { url: d.url.trim() };
      break;
    case "redis":
      config = {
        host: d.host.trim(),
        port: Number(d.port),
        dedupDb: Number(d.dedupDb),
        bookmarksDb: Number(d.bookmarksDb),
        mode: "standalone",
      };
      if (d.password.trim()) {
        config.password = d.password.trim();
        secretEntered = true;
      }
      break;
    case "apisix":
      config = { adminUrl: d.adminUrl.trim(), runtimeUrl: d.runtimeUrl.trim() };
      if (d.adminKey.trim()) {
        config.adminKey = d.adminKey.trim();
        secretEntered = true;
      }
      break;
  }
  const noSecretPossible =
    type === "kafka_connect" || (type === "apicurio" && d.authMode === "none");
  return {
    id: existing?.id ?? "",
    type,
    name: d.name.trim(),
    active: existing?.active ?? false,
    health: existing?.health ?? "Not Tested",
    reachability: existing?.reachability ?? "Unknown",
    lastTestedAt: existing?.lastTestedAt ?? null,
    config,
    hasSecret: noSecretPossible ? false : secretEntered || (existing?.hasSecret ?? false),
  };
}

function summaryRows(conn: PlatformConnection): { label: string; value: string }[] {
  const c = conn.config;
  switch (conn.type) {
    case "nifi":
      return [
        { label: "URL", value: str(c.url) },
        { label: "Auth", value: str(c.authMode) === "basic" ? `basic${c.username ? ` (${str(c.username)})` : ""}` : "bearer token" },
      ];
    case "kafka": {
      const rows = [
        { label: "Bootstrap", value: str(c.bootstrapServers) },
        { label: "Mode", value: str(c.mode, "native") === "kafbat" ? "kafbat proxy" : "native" },
      ];
      if (str(c.mode) === "kafbat") rows.push({ label: "Proxy URL", value: str(c.proxyUrl) });
      rows.push({
        label: "Security",
        value: `${str(c.securityProtocol, "PLAINTEXT")}${c.saslUsername ? ` · ${str(c.saslUsername)}` : ""}`,
      });
      return rows;
    }
    case "apicurio":
      return [
        { label: "URL", value: str(c.url) },
        { label: "Auth", value: str(c.authMode, "none") },
      ];
    case "kafka_connect":
      return [{ label: "URL", value: str(c.url) }];
    case "redis":
      return [
        { label: "Host", value: `${str(c.host)}:${str(c.port, "6379")}` },
        { label: "Logical DBs", value: `dedup #${str(c.dedupDb, "0")} · bookmarks #${str(c.bookmarksDb, "1")}` },
      ];
    case "apisix":
      return [
        { label: "Admin URL", value: str(c.adminUrl) },
        { label: "Runtime URL", value: str(c.runtimeUrl) },
      ];
  }
}

// ─── Small form primitives ───────────────────────────────────────────────────

function Field({
  label,
  hint,
  error,
  ...props
}: { label: string; hint?: string; error?: string } & InputHTMLAttributes<HTMLInputElement>) {
  return (
    <div>
      <Label>{label}</Label>
      <Input className="mt-1" {...props} />
      {error && <p className="text-xs text-destructive mt-1">{error}</p>}
      {hint && !error && <p className="text-xs text-muted-foreground mt-1">{hint}</p>}
    </div>
  );
}

const SECRET_PLACEHOLDER = "•••••• (blank keeps existing)";

function SecretField({
  label,
  isEdit,
  ...props
}: { label: string; isEdit: boolean; error?: string } & InputHTMLAttributes<HTMLInputElement>) {
  return <Field label={label} type="password" placeholder={isEdit ? SECRET_PLACEHOLDER : undefined} {...props} />;
}

/** Dynamic per-type form body. Auth-mode switches swap the visible credential fields. */
function ConnectionFormFields({
  type,
  draft,
  errors,
  isEdit,
  onPatch,
}: {
  type: ConnectionType;
  draft: Draft;
  errors: DraftErrors;
  isEdit: boolean;
  onPatch: (patch: Draft) => void;
}) {
  const nameField = (
    <Field
      label="Name"
      value={draft.name}
      onChange={(e) => onPatch({ name: e.target.value })}
      error={errors.name}
    />
  );

  if (type === "nifi") {
    return (
      <div className="grid gap-3">
        {nameField}
        <Field label="URL" value={draft.url} onChange={(e) => onPatch({ url: e.target.value })} error={errors.url} hint="NiFi API base URL, e.g. https://nifi.internal:8443" />
        <div>
          <Label>Auth mode</Label>
          <Select value={draft.authMode} onValueChange={(v) => onPatch({ authMode: v })}>
            <SelectTrigger className="mt-1"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="bearer">Bearer token</SelectItem>
              <SelectItem value="basic">Basic (username + password)</SelectItem>
            </SelectContent>
          </Select>
        </div>
        {draft.authMode === "bearer" ? (
          <SecretField label="Bearer token" isEdit={isEdit} value={draft.token} onChange={(e) => onPatch({ token: e.target.value })} error={errors.token} />
        ) : (
          <>
            <Field label="Username" value={draft.username} onChange={(e) => onPatch({ username: e.target.value })} error={errors.username} />
            <SecretField label="Password" isEdit={isEdit} value={draft.password} onChange={(e) => onPatch({ password: e.target.value })} error={errors.password} />
          </>
        )}
      </div>
    );
  }

  if (type === "kafka") {
    return (
      <div className="grid gap-3">
        {nameField}
        <Field
          label="Bootstrap servers"
          value={draft.bootstrapServers}
          onChange={(e) => onPatch({ bootstrapServers: e.target.value })}
          error={errors.bootstrapServers}
          hint="Comma-separated host:port list"
        />
        <div>
          <Label>Mode</Label>
          <Select value={draft.mode} onValueChange={(v) => onPatch({ mode: v })}>
            <SelectTrigger className="mt-1"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="native">Native Kafka</SelectItem>
              <SelectItem value="kafbat">Kafbat proxy</SelectItem>
            </SelectContent>
          </Select>
        </div>
        {draft.mode === "kafbat" && (
          <>
            <Field
              label="Kafbat proxy URL"
              value={draft.proxyUrl}
              onChange={(e) => onPatch({ proxyUrl: e.target.value })}
              error={errors.proxyUrl}
              hint="HTTP URL used to inspect topics through the Kafbat proxy"
            />
            <Field
              label="Kafbat username"
              value={draft.kafbatUsername}
              onChange={(e) => onPatch({ kafbatUsername: e.target.value })}
            />
            <SecretField
              label="Kafbat password"
              isEdit={isEdit}
              value={draft.kafbatPassword}
              onChange={(e) => onPatch({ kafbatPassword: e.target.value })}
            />
          </>
        )}
        <div>
          <Label>Security protocol</Label>
          <Select value={draft.securityProtocol} onValueChange={(v) => onPatch({ securityProtocol: v })}>
            <SelectTrigger className="mt-1"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="PLAINTEXT">PLAINTEXT</SelectItem>
              <SelectItem value="SSL">SSL</SelectItem>
              <SelectItem value="SASL_SSL">SASL_SSL</SelectItem>
              <SelectItem value="SASL_PLAINTEXT">SASL_PLAINTEXT</SelectItem>
            </SelectContent>
          </Select>
        </div>
        {draft.securityProtocol.startsWith("SASL") && (
          <>
            <Field label="SASL username" value={draft.saslUsername} onChange={(e) => onPatch({ saslUsername: e.target.value })} error={errors.saslUsername} />
            <SecretField label="SASL password" isEdit={isEdit} value={draft.saslPassword} onChange={(e) => onPatch({ saslPassword: e.target.value })} />
          </>
        )}
      </div>
    );
  }

  if (type === "apicurio") {
    return (
      <div className="grid gap-3">
        {nameField}
        <Field label="URL" value={draft.url} onChange={(e) => onPatch({ url: e.target.value })} error={errors.url} />
        <div>
          <Label>Auth mode</Label>
          <Select value={draft.authMode} onValueChange={(v) => onPatch({ authMode: v })}>
            <SelectTrigger className="mt-1"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="none">None</SelectItem>
              <SelectItem value="basic">Basic (username + password)</SelectItem>
              <SelectItem value="bearer">Bearer token</SelectItem>
            </SelectContent>
          </Select>
        </div>
        {draft.authMode === "basic" && (
          <>
            <Field label="Username" value={draft.username} onChange={(e) => onPatch({ username: e.target.value })} error={errors.username} />
            <SecretField label="Password" isEdit={isEdit} value={draft.password} onChange={(e) => onPatch({ password: e.target.value })} error={errors.password} />
          </>
        )}
        {draft.authMode === "bearer" && (
          <SecretField label="Bearer token" isEdit={isEdit} value={draft.token} onChange={(e) => onPatch({ token: e.target.value })} error={errors.token} />
        )}
      </div>
    );
  }

  if (type === "kafka_connect") {
    return (
      <div className="grid gap-3">
        {nameField}
        <Field label="URL" value={draft.url} onChange={(e) => onPatch({ url: e.target.value })} error={errors.url} hint="Kafka Connect REST endpoint" />
      </div>
    );
  }

  if (type === "redis") {
    return (
      <div className="grid gap-3">
        {nameField}
        <div className="grid grid-cols-3 gap-3">
          <div className="col-span-2">
            <Field label="Host" value={draft.host} onChange={(e) => onPatch({ host: e.target.value })} error={errors.host} />
          </div>
          <Field label="Port" value={draft.port} onChange={(e) => onPatch({ port: e.target.value })} error={errors.port} />
        </div>
        <div className="grid grid-cols-2 gap-3">
          <Field label="Dedup logical DB" value={draft.dedupDb} onChange={(e) => onPatch({ dedupDb: e.target.value })} error={errors.dedupDb} />
          <Field label="Bookmarks logical DB" value={draft.bookmarksDb} onChange={(e) => onPatch({ bookmarksDb: e.target.value })} error={errors.bookmarksDb} />
        </div>
        <SecretField label="Password" isEdit={isEdit} value={draft.password} onChange={(e) => onPatch({ password: e.target.value })} />
        <p className="text-xs text-muted-foreground">
          Standalone mode only. Dedup caches and jdbc bookmarks live in separate logical databases.
        </p>
      </div>
    );
  }

  // apisix
  return (
    <div className="grid gap-3">
      {nameField}
      <Field
        label="Admin URL"
        value={draft.adminUrl}
        onChange={(e) => onPatch({ adminUrl: e.target.value })}
        error={errors.adminUrl}
        hint="backend-only — never exposed to flows"
      />
      <Field label="Runtime URL" value={draft.runtimeUrl} onChange={(e) => onPatch({ runtimeUrl: e.target.value })} error={errors.runtimeUrl} />
      <SecretField label="Admin key" isEdit={isEdit} value={draft.adminKey} onChange={(e) => onPatch({ adminKey: e.target.value })} />
    </div>
  );
}

// ─── Main panel ──────────────────────────────────────────────────────────────

type FormState = {
  mode: "add" | "edit";
  type: ConnectionType;
  draft: Draft;
  existing?: PlatformConnection;
};

export function PlatformConnectionsPanel({ showHeading = true }: { showHeading?: boolean }) {
  const queryClient = useQueryClient();

  const { data: connections = [], isLoading } = useQuery({
    queryKey: ["connections"],
    queryFn: listConnections,
  });

  const [testingIds, setTestingIds] = useState<string[]>([]);
  const [testingAll, setTestingAll] = useState(false);
  const [activatingId, setActivatingId] = useState<string | null>(null);
  const [typePickerOpen, setTypePickerOpen] = useState(false);
  const [formState, setFormState] = useState<FormState | null>(null);
  const [formErrors, setFormErrors] = useState<DraftErrors>({});
  const [saving, setSaving] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<PlatformConnection | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [redisConfirm, setRedisConfirm] = useState<PlatformConnection | null>(null);
  const [checkingNifiId, setCheckingNifiId] = useState<string | null>(null);

  const invalidate = () => queryClient.invalidateQueries({ queryKey: ["connections"] });

  // ── Test ──────────────────────────────────────────────────────────────────
  const runTest = async (conn: PlatformConnection) => {
    setTestingIds((prev) => [...prev, conn.id]);
    try {
      const result: ConnectionTestResult = await testConnection(conn.id);
      if (result.health === "Healthy") {
        toast.success(`${conn.name}: Healthy · ${result.reachability}`);
      } else {
        toast.error(`${conn.name}: ${result.health} · ${result.reachability}`, {
          description: result.message || "The service responded, but the connection test did not pass.",
        });
      }
    } catch (err) {
      toast.error(`Test failed: ${(err as Error).message}`);
    } finally {
      setTestingIds((prev) => prev.filter((id) => id !== conn.id));
      invalidate();
    }
  };

  const testAll = async () => {
    setTestingAll(true);
    setTestingIds(connections.map((c) => c.id));
    try {
      const results = await Promise.allSettled(connections.map((c) => testConnection(c.id)));
      const healthy = results.filter((r) => r.status === "fulfilled" && r.value.health === "Healthy").length;
      const failed = connections.length - healthy;
      if (failed > 0) toast.error(`Test all: ${healthy} healthy, ${failed} failed`);
      else toast.success(`Test all: ${healthy}/${connections.length} healthy`);
    } finally {
      setTestingAll(false);
      setTestingIds([]);
      invalidate();
    }
  };

  // ── Activate ──────────────────────────────────────────────────────────────
  const doActivate = async (conn: PlatformConnection) => {
    setActivatingId(conn.id);
    try {
      await activateConnection(conn.id);
      toast.success(`"${conn.name}" is now the active ${TYPE_META[conn.type].label} connection.`);
    } catch (err) {
      toast.error((err as Error).message);
    } finally {
      setActivatingId(null);
      invalidate();
    }
  };

  const requestActivate = (conn: PlatformConnection) => {
    if (conn.type === "redis" && connections.some((c) => c.type === "redis" && c.active && c.id !== conn.id)) {
      setRedisConfirm(conn);
      return;
    }
    void doActivate(conn);
  };

  const checkNifiServices = async (conn: PlatformConnection) => {
    setCheckingNifiId(conn.id);
    try {
      const result = await checkNifiPlatformServices(conn.id);
      const summary = result.summary ?? {};
      if (result.ok) {
        toast.success(`${conn.name}: NiFi platform services are ready`, {
          description: `${summary.created ?? 0} created, ${summary.repaired ?? 0} repaired, ${summary.healthy ?? 0} already healthy. Flow-specific services were left unchanged.`,
        });
      } else {
        const attention = result.services.filter((service) => service.status === "failed" || service.status === "blocked");
        toast.error(`${conn.name}: platform-service readiness needs attention`, {
          description: attention.map((service) => `${service.name}: ${service.message ?? service.status}`).join(" "),
        });
      }
    } catch (err) {
      toast.error(`NiFi service check failed: ${(err as Error).message}`);
    } finally {
      setCheckingNifiId(null);
      invalidate();
    }
  };

  // ── Add / Edit ────────────────────────────────────────────────────────────
  const openAdd = (type: ConnectionType) => {
    setTypePickerOpen(false);
    setFormErrors({});
    setFormState({ mode: "add", type, draft: defaultDraft(type) });
  };

  const openEdit = (conn: PlatformConnection) => {
    setFormErrors({});
    setFormState({ mode: "edit", type: conn.type, draft: connectionToDraft(conn), existing: conn });
  };

  const patchDraft = (patch: Draft) => {
    setFormState((prev) => (prev ? { ...prev, draft: { ...prev.draft, ...patch } } : prev));
    setFormErrors((prev) => {
      const next = { ...prev };
      for (const key of Object.keys(patch)) delete next[key];
      return next;
    });
  };

  const submitForm = async () => {
    if (!formState) return;
    const errors = validateDraft(formState.type, formState.draft, formState.mode === "edit");
    setFormErrors(errors);
    if (Object.keys(errors).length > 0) {
      toast.error(Object.values(errors)[0]);
      return;
    }
    setSaving(true);
    try {
      const conn = draftToConnection(formState.type, formState.draft, formState.existing);
      await saveConnection(conn);
      toast.success(formState.mode === "add" ? "Connection created" : "Connection saved");
      setFormState(null);
    } catch (err) {
      toast.error(`Save failed: ${(err as Error).message}`);
    } finally {
      setSaving(false);
      invalidate();
    }
  };

  // ── Delete ────────────────────────────────────────────────────────────────
  const deleteDeps = deleteTarget ? connectionDependents(deleteTarget) : [];

  const confirmDelete = async () => {
    if (!deleteTarget) return;
    setDeleting(true);
    try {
      await deleteConnection(deleteTarget.id);
      toast.success(`Deleted "${deleteTarget.name}"`);
      setDeleteTarget(null);
    } catch (err) {
      toast.error((err as Error).message);
      setDeleteTarget(null);
    } finally {
      setDeleting(false);
      invalidate();
    }
  };

  // ── Redis confirm ─────────────────────────────────────────────────────────
  const activeRedis = redisConfirm
    ? connections.find((c) => c.type === "redis" && c.active && c.id !== redisConfirm.id)
    : undefined;
  const redisDeps = activeRedis ? connectionDependents(activeRedis) : [];

  const hasAnyConnections = connections.length > 0;
  const availableTypes = TYPE_ORDER.filter((type) => !connections.some((c) => c.type === type));

  return (
    <div className="space-y-8">
      <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
        {showHeading ? (
          <div>
            <h2 className="text-lg font-semibold tracking-tight">Platform Connections</h2>
            <p className="text-sm text-muted-foreground">
              Runtime systems the platform talks to. Health checks are manual — no background polling.
            </p>
          </div>
        ) : (
          <div />
        )}
        <div className="flex gap-2">
          <Button
            variant="outline"
            onClick={testAll}
            disabled={isLoading || !hasAnyConnections || testingAll}
            title={
              isLoading
                ? "Connections are still loading."
                : !hasAnyConnections
                  ? "There are no connections to test."
                  : testingAll
                    ? "A test-all run is in progress."
                    : undefined
            }
          >
            {testingAll ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
            Test All
          </Button>
          <Button
            onClick={() => setTypePickerOpen(true)}
            disabled={isLoading || availableTypes.length === 0}
            title={
              isLoading
                ? "Connections are still loading."
                : availableTypes.length === 0
                  ? "All platform connection types are already configured."
                  : undefined
            }
          >
            <Plus className="h-4 w-4" /> Add Connection
          </Button>
        </div>
      </div>

      {isLoading && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {Array.from({ length: 4 }).map((_, i) => (
            <Skeleton key={i} className="h-44 rounded-lg" />
          ))}
        </div>
      )}

      {!isLoading &&
        TYPE_ORDER.map((type) => {
          const meta = TYPE_META[type];
          const Icon = meta.icon;
          const items = connections.filter((c) => c.type === type);
          return (
            <section key={type} className="space-y-3">
              <div className="flex items-center gap-2">
                <Icon className="h-4 w-4 text-muted-foreground" />
                <h2 className="text-sm font-semibold tracking-tight">{meta.label}</h2>
                <span className="text-xs text-muted-foreground hidden md:inline">— {meta.description}</span>
              </div>
              {items.length === 0 ? (
                <EmptyState
                  title={`No ${meta.label} connections yet`}
                  action={
                    <Button variant="outline" size="sm" onClick={() => openAdd(type)}>
                      <Plus /> Add
                    </Button>
                  }
                />
              ) : (
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  {items.map((c) => {
                    const isTesting = testingIds.includes(c.id);
                    const isActivating = activatingId === c.id;
                    return (
                      <Card key={c.id}>
                        <CardHeader className="flex-row items-start justify-between space-y-0 gap-3 pb-3">
                          <div className="flex items-start gap-3 min-w-0">
                            <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-md bg-primary-muted text-primary">
                              <Icon className="h-5 w-5" />
                            </div>
                            <div className="min-w-0">
                              <div className="flex items-center gap-2 flex-wrap">
                                <CardTitle className="text-base truncate">{c.name}</CardTitle>
                                {c.active && <StatusBadge status="Active" />}
                              </div>
                              <p className="text-xs text-muted-foreground mt-0.5">
                                Last tested: {timeAgo(c.lastTestedAt)}
                              </p>
                            </div>
                          </div>
                          <div className="flex gap-1.5 items-center shrink-0 flex-wrap justify-end">
                            <StatusBadge status={c.health} />
                            <StatusBadge status={c.reachability} />
                          </div>
                        </CardHeader>
                        <CardContent className="space-y-3">
                          <div className="rounded-md bg-muted/40 border px-3 py-2 space-y-1">
                            {summaryRows(c).map((row) => (
                              <div key={row.label} className="flex items-baseline justify-between gap-3 text-xs">
                                <span className="text-muted-foreground shrink-0">{row.label}</span>
                                <span className="font-mono truncate">{row.value}</span>
                              </div>
                            ))}
                            {c.hasSecret && (
                              <div className="flex items-baseline justify-between gap-3 text-xs">
                                <span className="text-muted-foreground shrink-0">Secret</span>
                                <span className="font-mono">stored (write-only)</span>
                              </div>
                            )}
                          </div>
                          <div className="flex flex-wrap gap-2">
                            <Button size="sm" variant="outline" onClick={() => runTest(c)} disabled={isTesting} title={isTesting ? "A test is in progress." : undefined}>
                              {isTesting && <Loader2 className="h-3.5 w-3.5 animate-spin mr-1" />}
                              Test
                            </Button>
                            {!c.active && (
                              <Button size="sm" variant="outline" onClick={() => requestActivate(c)} disabled={isActivating} title={isActivating ? "Activation in progress." : undefined}>
                                {isActivating && <Loader2 className="h-3.5 w-3.5 animate-spin mr-1" />}
                                Activate
                              </Button>
                            )}
                            <Button size="sm" variant="outline" onClick={() => openEdit(c)}>
                              <Pencil className="h-3.5 w-3.5" /> Edit
                            </Button>
                            {c.type === "nifi" && (
                              <Button
                                size="sm"
                                variant="outline"
                                onClick={() => void checkNifiServices(c)}
                                disabled={checkingNifiId === c.id || !c.active}
                                title={!c.active ? "Activate this NiFi connection before checking its platform services." : "Verify and repair Kafka, Apicurio, and Redis services in NiFi."}
                              >
                                {checkingNifiId === c.id ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <CheckCircle2 className="h-3.5 w-3.5" />}
                                {checkingNifiId === c.id ? "Checking..." : "Check NiFi services"}
                              </Button>
                            )}
                            {c.type === "apisix" && (
                              <Button
                                size="sm"
                                variant="outline"
                                asChild
                                title="Proxies, certificate profiles and the host allowlist live on the Proxies page."
                              >
                                <Link to="/apisix">
                                  <Globe className="h-3.5 w-3.5" /> Proxy resources
                                </Link>
                              </Button>
                            )}
                            <Button
                              size="sm"
                              variant="ghost"
                              className="text-destructive ml-auto"
                              title={`Delete ${c.name}`}
                              onClick={() => setDeleteTarget(c)}
                            >
                              <Trash2 className="h-3.5 w-3.5" />
                            </Button>
                          </div>
                        </CardContent>
                      </Card>
                    );
                  })}
                </div>
              )}
            </section>
          );
        })}

      {/* ── Type picker (Add step 1) ─────────────────────────────────────── */}
      <Dialog open={typePickerOpen} onOpenChange={setTypePickerOpen}>
        <DialogContent className="max-w-lg">
          <DialogHeader>
            <DialogTitle>Add Connection</DialogTitle>
            <DialogDescription>Choose the platform system to connect.</DialogDescription>
          </DialogHeader>
          <div className="grid gap-2">
            {availableTypes.map((type) => {
              const meta = TYPE_META[type];
              const Icon = meta.icon;
              return (
                <button
                  key={type}
                  type="button"
                  className="flex items-center gap-3 rounded-md border p-3 text-left hover:bg-muted/40 transition-colors"
                  onClick={() => openAdd(type)}
                >
                  <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-md bg-primary-muted text-primary">
                    <Icon className="h-4 w-4" />
                  </div>
                  <div>
                    <div className="text-sm font-medium">{meta.label}</div>
                    <div className="text-xs text-muted-foreground">{meta.description}</div>
                  </div>
                </button>
              );
            })}
          </div>
          {availableTypes.length === 0 ? (
            <p className="text-xs text-muted-foreground">Each platform type already has one connection. Edit or delete an existing card to change it.</p>
          ) : (
            <p className="text-xs text-muted-foreground">Iceberg moved to Sink destination services.</p>
          )}
        </DialogContent>
      </Dialog>

      {/* ── Add / Edit form (step 2) ─────────────────────────────────────── */}
      <Dialog
        open={!!formState}
        onOpenChange={(open) => {
          if (!open) {
            setFormState(null);
            setFormErrors({});
          }
        }}
      >
        <DialogContent className="max-w-lg max-h-[85vh] overflow-y-auto">
          {formState && (
            <>
              <DialogHeader>
                <DialogTitle>
                  {formState.mode === "add"
                    ? `Add ${TYPE_META[formState.type].label} connection`
                    : `Edit ${formState.existing?.name}`}
                </DialogTitle>
                <DialogDescription>
                  {formState.mode === "add"
                    ? TYPE_META[formState.type].description
                    : "Secrets are write-only — leave secret fields blank to keep the existing values."}
                </DialogDescription>
              </DialogHeader>
              <ConnectionFormFields
                type={formState.type}
                draft={formState.draft}
                errors={formErrors}
                isEdit={formState.mode === "edit"}
                onPatch={patchDraft}
              />
              <DialogFooter className="gap-2 sm:gap-0">
                {formState.mode === "add" && (
                  <Button
                    variant="ghost"
                    className="mr-auto"
                    onClick={() => {
                      setFormState(null);
                      setFormErrors({});
                      setTypePickerOpen(true);
                    }}
                  >
                    <ArrowLeft className="h-3.5 w-3.5" /> Back
                  </Button>
                )}
                <Button
                  variant="outline"
                  onClick={() => {
                    setFormState(null);
                    setFormErrors({});
                  }}
                >
                  Cancel
                </Button>
                <Button onClick={submitForm} disabled={saving} title={saving ? "Saving…" : undefined}>
                  {saving && <Loader2 className="h-3.5 w-3.5 animate-spin mr-1" />}
                  {formState.mode === "add" ? "Create Connection" : "Save Changes"}
                </Button>
              </DialogFooter>
            </>
          )}
        </DialogContent>
      </Dialog>

      {/* ── Delete confirm ───────────────────────────────────────────────── */}
      <AlertDialog open={!!deleteTarget} onOpenChange={(open) => { if (!open) setDeleteTarget(null); }}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete "{deleteTarget?.name}"?</AlertDialogTitle>
            <AlertDialogDescription>
              This removes the {deleteTarget ? TYPE_META[deleteTarget.type].label : ""} connection permanently. Secrets stored for it are discarded.
            </AlertDialogDescription>
          </AlertDialogHeader>
          {deleteDeps.length > 0 && (
            <div className="rounded-md bg-destructive-muted border border-destructive/20 px-3 py-2 text-sm">
              <p className="font-medium text-destructive mb-1">
                {deleteDeps.length} deployed flow(s) depend on this connection:
              </p>
              <ul className="text-xs space-y-0.5">
                {deleteDeps.map((name) => (
                  <li key={name}>{name}</li>
                ))}
              </ul>
            </div>
          )}
          <AlertDialogFooter>
            <AlertDialogCancel disabled={deleting}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              disabled={deleting || deleteDeps.length > 0}
              title={
                deleteDeps.length > 0
                  ? `Blocked: ${deleteDeps.length} deployed flow(s) depend on this connection. Undeploy them first.`
                  : deleting
                    ? "Deleting…"
                    : undefined
              }
              onClick={(e) => {
                e.preventDefault();
                void confirmDelete();
              }}
            >
              {deleting && <Loader2 className="h-3.5 w-3.5 animate-spin mr-1" />}
              Delete Connection
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* ── Redis switch confirm ─────────────────────────────────────────── */}
      <AlertDialog open={!!redisConfirm} onOpenChange={(open) => { if (!open) setRedisConfirm(null); }}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Switch active Redis to "{redisConfirm?.name}"?</AlertDialogTitle>
            <AlertDialogDescription>
              Dedup windows and jdbc bookmarks on the old instance will be lost.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <div className="rounded-md bg-warning-muted border border-warning/20 px-3 py-2 text-sm">
            {redisDeps.length === 0 ? (
              <p className="text-xs text-muted-foreground">
                No deployed flows currently use dedup windows or jdbc bookmarks
                {activeRedis ? ` on "${activeRedis.name}"` : ""}.
              </p>
            ) : (
              <>
                <p className="font-medium mb-1">
                  {redisDeps.length} deployed flow(s) keep dedup windows or jdbc bookmarks on "{activeRedis?.name}":
                </p>
                <ul className="text-xs space-y-0.5">
                  {redisDeps.map((name) => (
                    <li key={name}>{name}</li>
                  ))}
                </ul>
                <p className="text-xs text-muted-foreground mt-2">
                  Their dedup state restarts empty and jdbc reads resume from the initial position.
                </p>
              </>
            )}
          </div>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => {
                const target = redisConfirm;
                setRedisConfirm(null);
                if (target) void doActivate(target);
              }}
            >
              Switch anyway
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

    </div>
  );
}

const Connections = () => (
  <AppLayout
    title="Platform Connections"
    description="Runtime systems the platform talks to — one saved connection per type. Health checks are manual: no background polling."
  >
    <PlatformConnectionsPanel showHeading={false} />
  </AppLayout>
);

export default Connections;
