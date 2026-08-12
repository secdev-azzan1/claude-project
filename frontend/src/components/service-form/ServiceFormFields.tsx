// The Application Service form — ONE definition, two mount points.
//
// It lives here rather than inside the Application Services page because the
// flow builder needs the same thing: "private service" used to collect a name
// and a URL and then send the user to another page to type the credentials,
// which is the one moment they have all the details in front of them. A service
// created mid-flow is a full service; only its `private` flag differs.
//
// Secrets stay write-only everywhere: they are typed here, `hasSecret` records
// that one was given, and no value is ever read back into the form.

import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { listGatewayProxies } from "@/prototype/api";
import type { AppService, HttpAuthMode, JdbcDialect, ServiceType, SinkKind } from "@/prototype/types";

const str = (v: unknown, fb = ""): string => (typeof v === "string" ? v : fb);
const numStr = (v: unknown, fb = ""): string =>
  typeof v === "number" || typeof v === "string" ? String(v) : fb;

// --------------------------------------------------------------- form model

export interface ServiceForm {
  name: string;
  // http
  baseUrl: string;
  authMode: HttpAuthMode;
  username: string;
  password: string;
  token: string;
  keyName: string;
  keyLocation: "header" | "query";
  keyValue: string;
  tokenUrl: string;
  clientId: string;
  clientSecret: string;
  loginPath: string;
  tokenPath: string;
  tokenHeader: string;
  /** APISIX egress for this host — empty means call it directly. */
  proxyId: string;
  // database
  dialect: JdbcDialect;
  host: string;
  port: string;
  database: string;
  dbUsername: string;
  dbPassword: string;
  capRead: boolean;
  capWrite: boolean;
  // external kafka
  bootstrapServers: string;
  securityProtocol: string;
  saslUsername: string;
  saslPassword: string;
  // sink destination
  sinkKind: SinkKind;
  sinkUrl: string;
  indexPrefix: string;
  writeMode: "upsert" | "index";
  catalogUrl: string;
  warehouse: string;
}

export const emptyForm = (): ServiceForm => ({
  name: "",
  baseUrl: "",
  authMode: "none",
  username: "",
  password: "",
  token: "",
  keyName: "",
  keyLocation: "header",
  keyValue: "",
  tokenUrl: "",
  clientId: "",
  clientSecret: "",
  loginPath: "",
  tokenPath: "",
  tokenHeader: "",
  proxyId: "",
  dialect: "postgresql",
  host: "",
  port: "5432",
  database: "",
  dbUsername: "",
  dbPassword: "",
  capRead: true,
  capWrite: false,
  bootstrapServers: "",
  securityProtocol: "SASL_SSL",
  saslUsername: "",
  saslPassword: "",
  sinkKind: "opensearch",
  sinkUrl: "",
  indexPrefix: "",
  writeMode: "upsert",
  catalogUrl: "",
  warehouse: "",
});

export function formFromService(svc: AppService): ServiceForm {
  const c = svc.config;
  const caps = Array.isArray(c.capabilities) ? (c.capabilities as string[]) : ["read"];
  return {
    ...emptyForm(),
    name: svc.name,
    baseUrl: str(c.baseUrl),
    authMode: (str(c.authMode, "none") as HttpAuthMode) || "none",
    username: str(c.username),
    keyName: str(c.keyName),
    keyLocation: str(c.keyLocation) === "query" ? "query" : "header",
    tokenUrl: str(c.tokenUrl),
    clientId: str(c.clientId),
    loginPath: str(c.loginPath),
    tokenPath: str(c.tokenPath),
    tokenHeader: str(c.tokenHeader),
    proxyId: str(c.proxyId),
    dialect: (str(c.dialect, "postgresql") as JdbcDialect) || "postgresql",
    host: str(c.host),
    port: numStr(c.port, "5432"),
    database: str(c.database),
    dbUsername: str(c.username),
    capRead: caps.includes("read"),
    capWrite: caps.includes("write"),
    bootstrapServers: str(c.bootstrapServers),
    securityProtocol: str(c.securityProtocol, "SASL_SSL"),
    saslUsername: str(c.saslUsername),
    sinkKind: (str(c.kind, "opensearch") as SinkKind) || "opensearch",
    sinkUrl: str(c.url),
    indexPrefix: str(c.indexPrefix),
    writeMode: str(c.writeMode) === "index" ? "index" : "upsert",
    catalogUrl: str(c.catalogUrl),
    warehouse: str(c.warehouse),
  };
}

/** Non-secret config only — secret values are write-only and never stored. */
export function buildConfig(type: ServiceType, f: ServiceForm): Record<string, unknown> {
  switch (type) {
    case "http": {
      const cfg: Record<string, unknown> = { baseUrl: f.baseUrl.trim(), authMode: f.authMode };
      if (f.authMode === "basic") cfg.username = f.username.trim();
      if (f.authMode === "api_key") {
        cfg.keyName = f.keyName.trim();
        cfg.keyLocation = f.keyLocation;
      }
      if (f.authMode === "oauth2") {
        cfg.tokenUrl = f.tokenUrl.trim();
        cfg.clientId = f.clientId.trim();
      }
      if (f.authMode === "session_token") {
        cfg.loginPath = f.loginPath.trim();
        cfg.tokenPath = f.tokenPath.trim();
        cfg.tokenHeader = f.tokenHeader.trim();
      }
      // Egress belongs to the host, and the host is this service — so every
      // block using it inherits the same route out.
      cfg.proxyId = f.proxyId || null;
      return cfg;
    }
    case "database":
      return {
        dialect: f.dialect,
        host: f.host.trim(),
        port: Number.parseInt(f.port, 10) || 0,
        database: f.database.trim(),
        username: f.dbUsername.trim(),
        capabilities: [...(f.capRead ? ["read"] : []), ...(f.capWrite ? ["write"] : [])],
      };
    case "external_kafka":
      return {
        bootstrapServers: f.bootstrapServers.trim(),
        securityProtocol: f.securityProtocol,
        saslUsername: f.saslUsername.trim(),
        note: "Input only — never a destination.",
      };
    case "sink_destination":
      return f.sinkKind === "opensearch"
        ? { kind: "opensearch", url: f.sinkUrl.trim(), indexPrefix: f.indexPrefix.trim(), writeMode: f.writeMode }
        : { kind: "iceberg_catalog", catalogUrl: f.catalogUrl.trim(), warehouse: f.warehouse.trim() };
  }
}

export function secretTyped(type: ServiceType, f: ServiceForm): boolean {
  if (type === "http")
    return [f.password, f.token, f.keyValue, f.clientSecret].some((v) => v.length > 0);
  if (type === "database") return f.dbPassword.length > 0;
  if (type === "external_kafka") return f.saslPassword.length > 0;
  return false;
}

export function saveBlockReason(type: ServiceType | null, f: ServiceForm): string | null {
  if (!type) return "Pick a service type first.";
  if (!f.name.trim()) return "Name the service.";
  if (type === "http" && !f.baseUrl.trim()) return "Base URL is required.";
  if (type === "database" && (!f.host.trim() || !f.database.trim())) return "Host and database are required.";
  if (type === "database" && !f.capRead && !f.capWrite) return "Select at least one capability.";
  if (type === "external_kafka" && !f.bootstrapServers.trim()) return "Bootstrap servers are required.";
  if (type === "sink_destination" && f.sinkKind === "opensearch" && !f.sinkUrl.trim()) return "OpenSearch URL is required.";
  if (type === "sink_destination" && f.sinkKind === "iceberg_catalog" && !f.catalogUrl.trim()) return "Catalog URL is required.";
  return null;
}

function TextField({
  label,
  value,
  onChange,
  placeholder,
  mono,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  mono?: boolean;
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
    </div>
  );
}

function SecretField({
  label,
  value,
  onChange,
  editing,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  editing: boolean;
}) {
  return (
    <div className="grid gap-1.5">
      <Label>{label}</Label>
      <Input
        type="password"
        value={value}
        placeholder={editing ? "Leave blank to keep the existing secret" : ""}
        onChange={(e) => onChange(e.target.value)}
      />
      <p className="text-xs text-muted-foreground">Write-only — never displayed again.</p>
    </div>
  );
}


/** Never a real proxy id — the Select needs a non-empty value for "none". */
const NO_PROXY = "__none__";

/**
 * The APISIX egress for this service's host. It lives here rather than on each
 * http block because a proxy answers "how is this host reached?", which every
 * block calling the host must answer the same way.
 */
function ProxyField({ value, onChange }: { value: string; onChange: (v: string) => void }) {
  const { data: proxies = [] } = useQuery({ queryKey: ["gatewayProxies"], queryFn: listGatewayProxies });
  const selected = proxies.find((p) => p.id === value);
  return (
    <div className="grid gap-1.5">
      <Label>API gateway egress</Label>
      <Select value={value || NO_PROXY} onValueChange={(v) => onChange(v === NO_PROXY ? "" : v)}>
        <SelectTrigger>
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value={NO_PROXY}>No proxy — call the host directly</SelectItem>
          {proxies.map((p) => (
            <SelectItem key={p.id} value={p.id}>
              {p.name} · {p.targetHost} · {p.status}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
      <p className="text-xs text-muted-foreground">
        {selected
          ? `Every block using this service calls ${selected.targetHost} through "${selected.name}". Upstream certificates are not verified in this mode.`
          : "For endpoints NiFi refuses (broken or nonstandard TLS) and client-certificate presentation."}{" "}
        <Link to="/apisix" className="underline underline-offset-2">
          Manage proxies
        </Link>
      </p>
    </div>
  );
}

// ------------------------------------------------------------ the fields

export interface ServiceFormFieldsProps {
  type: ServiceType;
  form: ServiceForm;
  onChange: (next: ServiceForm) => void;
  /** Editing an existing service: secret placeholders say "leave blank to keep". */
  editing: boolean;
}

export function ServiceFormFields({ type, form, onChange, editing }: ServiceFormFieldsProps) {
  const setForm = (updater: (prev: ServiceForm) => ServiceForm) => onChange(updater(form));
  const formType = type;
  return (
    <div className="grid gap-3">

              <TextField label="Name" value={form.name} onChange={(name) => setForm((p) => ({ ...p, name }))} />

              {formType === "http" && (
                <>
                  <TextField
                    label="Base URL"
                    mono
                    value={form.baseUrl}
                    placeholder="https://api.example.corp"
                    onChange={(baseUrl) => setForm((p) => ({ ...p, baseUrl }))}
                  />
                  <div className="grid gap-1.5">
                    <Label>Auth mode</Label>
                    <Select
                      value={form.authMode}
                      onValueChange={(v) => setForm((p) => ({ ...p, authMode: v as HttpAuthMode }))}
                    >
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="none">None</SelectItem>
                        <SelectItem value="basic">Basic</SelectItem>
                        <SelectItem value="bearer">Bearer</SelectItem>
                        <SelectItem value="api_key">API key</SelectItem>
                        <SelectItem value="oauth2">OAuth2</SelectItem>
                        <SelectItem value="session_token">Session token</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  {form.authMode === "basic" && (
                    <>
                      <TextField label="Username" value={form.username} onChange={(username) => setForm((p) => ({ ...p, username }))} />
                      <SecretField label="Password" value={form.password} editing={!!editing} onChange={(password) => setForm((p) => ({ ...p, password }))} />
                    </>
                  )}
                  {form.authMode === "bearer" && (
                    <SecretField label="Token" value={form.token} editing={!!editing} onChange={(token) => setForm((p) => ({ ...p, token }))} />
                  )}
                  {form.authMode === "api_key" && (
                    <>
                      <TextField label="Key name" mono value={form.keyName} placeholder="X-Api-Key" onChange={(keyName) => setForm((p) => ({ ...p, keyName }))} />
                      <div className="grid gap-1.5">
                        <Label>Key location</Label>
                        <Select
                          value={form.keyLocation}
                          onValueChange={(v) => setForm((p) => ({ ...p, keyLocation: v as "header" | "query" }))}
                        >
                          <SelectTrigger>
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="header">Header</SelectItem>
                            <SelectItem value="query">Query parameter</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                      <SecretField label="Key value" value={form.keyValue} editing={!!editing} onChange={(keyValue) => setForm((p) => ({ ...p, keyValue }))} />
                    </>
                  )}
                  {form.authMode === "oauth2" && (
                    <>
                      <TextField label="Token URL" mono value={form.tokenUrl} onChange={(tokenUrl) => setForm((p) => ({ ...p, tokenUrl }))} />
                      <TextField label="Client id" value={form.clientId} onChange={(clientId) => setForm((p) => ({ ...p, clientId }))} />
                      <SecretField label="Client secret" value={form.clientSecret} editing={!!editing} onChange={(clientSecret) => setForm((p) => ({ ...p, clientSecret }))} />
                    </>
                  )}
                  <ProxyField value={form.proxyId} onChange={(proxyId) => setForm((p) => ({ ...p, proxyId }))} />
                  {form.authMode === "session_token" && (
                    <>
                      <TextField label="Login path" mono value={form.loginPath} placeholder="/rest/login" onChange={(loginPath) => setForm((p) => ({ ...p, loginPath }))} />
                      <TextField label="Token JSONPath" mono value={form.tokenPath} placeholder="$.sessionToken" onChange={(tokenPath) => setForm((p) => ({ ...p, tokenPath }))} />
                      <TextField label="Token header" mono value={form.tokenHeader} placeholder="Authorization" onChange={(tokenHeader) => setForm((p) => ({ ...p, tokenHeader }))} />
                      <div className="rounded-md border bg-muted/40 p-3 text-xs text-muted-foreground">
                        Logins are never modeled as data streams — session bootstrap lives here now.
                      </div>
                    </>
                  )}
                </>
              )}

              {formType === "database" && (
                <>
                  <div className="grid gap-1.5">
                    <Label>Dialect</Label>
                    <Select
                      value={form.dialect}
                      onValueChange={(v) => setForm((p) => ({ ...p, dialect: v as JdbcDialect }))}
                    >
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="postgresql">PostgreSQL</SelectItem>
                        <SelectItem value="trino">Trino</SelectItem>
                        <SelectItem value="mysql">MySQL / MariaDB</SelectItem>
                        <SelectItem value="oracle" disabled>
                          Oracle — coming later
                        </SelectItem>
                        <SelectItem value="sqlserver" disabled>
                          SQL Server — coming later
                        </SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="grid grid-cols-[1fr_110px] gap-3">
                    <TextField label="Host" mono value={form.host} onChange={(host) => setForm((p) => ({ ...p, host }))} />
                    <TextField label="Port" mono value={form.port} onChange={(port) => setForm((p) => ({ ...p, port }))} />
                  </div>
                  <TextField label="Database" mono value={form.database} onChange={(database) => setForm((p) => ({ ...p, database }))} />
                  <TextField label="Username" value={form.dbUsername} onChange={(dbUsername) => setForm((p) => ({ ...p, dbUsername }))} />
                  <SecretField label="Password" value={form.dbPassword} editing={!!editing} onChange={(dbPassword) => setForm((p) => ({ ...p, dbPassword }))} />
                  <div className="grid gap-1.5">
                    <Label>Capabilities</Label>
                    <div className="flex gap-2">
                      <label className="inline-flex h-8 items-center gap-2 rounded-md border px-2.5 text-xs font-medium text-muted-foreground transition-colors hover:bg-muted/50">
                        <Checkbox checked={form.capRead} onCheckedChange={(v) => setForm((p) => ({ ...p, capRead: v === true }))} aria-label="Read capability" />
                        Read
                      </label>
                      <label className="inline-flex h-8 items-center gap-2 rounded-md border px-2.5 text-xs font-medium text-muted-foreground transition-colors hover:bg-muted/50">
                        <Checkbox checked={form.capWrite} onCheckedChange={(v) => setForm((p) => ({ ...p, capWrite: v === true }))} aria-label="Write capability" />
                        Write
                      </label>
                    </div>
                  </div>
                </>
              )}

              {formType === "external_kafka" && (
                <>
                  <TextField
                    label="Bootstrap servers"
                    mono
                    value={form.bootstrapServers}
                    placeholder="kafka.partner.example:9093"
                    onChange={(bootstrapServers) => setForm((p) => ({ ...p, bootstrapServers }))}
                  />
                  <div className="grid gap-1.5">
                    <Label>Security protocol</Label>
                    <Select
                      value={form.securityProtocol}
                      onValueChange={(securityProtocol) => setForm((p) => ({ ...p, securityProtocol }))}
                    >
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="SASL_SSL">SASL_SSL</SelectItem>
                        <SelectItem value="SASL_PLAINTEXT">SASL_PLAINTEXT</SelectItem>
                        <SelectItem value="SSL">SSL</SelectItem>
                        <SelectItem value="PLAINTEXT">PLAINTEXT</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <TextField label="SASL username" value={form.saslUsername} onChange={(saslUsername) => setForm((p) => ({ ...p, saslUsername }))} />
                  <SecretField label="SASL password" value={form.saslPassword} editing={!!editing} onChange={(saslPassword) => setForm((p) => ({ ...p, saslPassword }))} />
                  <div className="rounded-md border bg-muted/40 p-3 text-xs text-muted-foreground">
                    Input only — external clusters are never a destination (R6).
                  </div>
                </>
              )}

              {formType === "sink_destination" && (
                <>
                  <div className="grid gap-1.5">
                    <Label>Kind</Label>
                    <Select
                      value={form.sinkKind}
                      onValueChange={(v) => setForm((p) => ({ ...p, sinkKind: v as SinkKind }))}
                    >
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="opensearch">OpenSearch</SelectItem>
                        <SelectItem value="iceberg_catalog">Iceberg catalog</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  {form.sinkKind === "opensearch" ? (
                    <>
                      <TextField label="URL" mono value={form.sinkUrl} placeholder="https://opensearch.corp:9200" onChange={(sinkUrl) => setForm((p) => ({ ...p, sinkUrl }))} />
                      <TextField label="Index prefix" mono value={form.indexPrefix} placeholder="dmp-" onChange={(indexPrefix) => setForm((p) => ({ ...p, indexPrefix }))} />
                      <div className="grid gap-1.5">
                        <Label>Write mode</Label>
                        <Select
                          value={form.writeMode}
                          onValueChange={(v) => setForm((p) => ({ ...p, writeMode: v as "upsert" | "index" }))}
                        >
                          <SelectTrigger>
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="upsert">Upsert</SelectItem>
                            <SelectItem value="index">Index</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                    </>
                  ) : (
                    <>
                      <TextField label="Catalog URL" mono value={form.catalogUrl} placeholder="http://polaris.corp:8181/api/catalog" onChange={(catalogUrl) => setForm((p) => ({ ...p, catalogUrl }))} />
                      <TextField label="Warehouse" mono value={form.warehouse} placeholder="bronze" onChange={(warehouse) => setForm((p) => ({ ...p, warehouse }))} />
                    </>
                  )}
                </>
              )}
    </div>
  );
}
