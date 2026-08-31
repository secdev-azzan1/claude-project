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
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { Field, SectionLabel } from "@/components/form/Field";
import { listGatewayProxies } from "@/prototype/api";
import { Lock } from "lucide-react";
import type { AppService, HttpAuthMode, JdbcDialect, ServiceType, SinkKind } from "@/prototype/types";

const str = (v: unknown, fb = ""): string => (typeof v === "string" ? v : fb);
const numStr = (v: unknown, fb = ""): string =>
  typeof v === "number" || typeof v === "string" ? String(v) : fb;
const boolish = (v: unknown, fb = false): boolean =>
  typeof v === "boolean" ? v : typeof v === "string" ? v.toLowerCase() === "true" : fb;

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
  tokenTemplate: string;
  /** APISIX egress for this host — empty means call it directly. */
  proxyId: string;
  // database
  dialect: JdbcDialect;
  dbUrl: string;
  host: string;
  port: string;
  database: string;
  dbUsername: string;
  dbPassword: string;
  driverLocations: string;
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
  oauthClientId: string;
  oauthClientSecret: string;
  s3Endpoint: string;
  s3AccessKey: string;
  s3SecretKey: string;
  s3Region: string;
  s3PathStyle: boolean;
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
  tokenTemplate: "",
  proxyId: "",
  dialect: "postgresql",
  dbUrl: "",
  host: "",
  port: "5432",
  database: "",
  dbUsername: "",
  dbPassword: "",
  driverLocations: "",
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
  oauthClientId: "",
  oauthClientSecret: "",
  s3Endpoint: "",
  s3AccessKey: "",
  s3SecretKey: "",
  s3Region: "",
  s3PathStyle: true,
});

export function formFromService(svc: AppService): ServiceForm {
  const c = svc.config;
  const caps = Array.isArray(c.capabilities) ? (c.capabilities as string[]) : ["read"];
  const form = {
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
    tokenTemplate: str(c.tokenTemplate),
    proxyId: str(c.proxyId),
    dialect: (str(c.dialect, "postgresql") as JdbcDialect) || "postgresql",
    dbUrl: str(c.url || c.endpoint) || (str(c.dialect).toLowerCase() === "trino" && str(c.host)
      ? `http://${str(c.host)}${c.port ? `:${numStr(c.port)}` : ""}`
      : ""),
    host: str(c.host),
    port: numStr(c.port, "5432"),
    database: str(c.database),
    dbUsername: str(c.username),
    driverLocations: str(c.driverLocations),
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
    oauthClientId: str(c.oauthClientId),
    s3Endpoint: str(c.s3Endpoint),
    s3AccessKey: str(c.s3AccessKey),
    s3Region: str(c.s3Region),
    s3PathStyle: boolish(c.s3PathStyle, true),
  };
  if (c.oauthClientSecret != null) form.oauthClientSecret = str(c.oauthClientSecret);
  if (c.s3SecretKey != null) form.s3SecretKey = str(c.s3SecretKey);
  return form;
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
        cfg.tokenTemplate = f.tokenTemplate.trim();
      }
      // Egress belongs to the host, and the host is this service — so every
      // block using it inherits the same route out.
      cfg.proxyId = f.proxyId || null;
      return cfg;
    }
    case "database": {
      const cfg: Record<string, unknown> = {
        dialect: f.dialect,
        username: f.dbUsername.trim(),
        driverLocations: f.driverLocations.trim(),
        capabilities: [...(f.capRead ? ["read"] : []), ...(f.capWrite ? ["write"] : [])],
      };
      if (f.dialect === "trino") {
        cfg.url = f.dbUrl.trim();
      } else {
        cfg.host = f.host.trim();
        cfg.port = Number.parseInt(f.port, 10) || 0;
        cfg.database = f.database.trim();
      }
      if (f.dbPassword.trim()) cfg.password = f.dbPassword.trim();
      return cfg;
    }
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
        : {
            kind: "iceberg_catalog",
            catalogUrl: f.catalogUrl.trim(),
            warehouse: f.warehouse.trim(),
            oauthClientId: f.oauthClientId.trim(),
            s3Endpoint: f.s3Endpoint.trim(),
            s3AccessKey: f.s3AccessKey.trim(),
            s3Region: f.s3Region.trim(),
            s3PathStyle: f.s3PathStyle,
            ...(f.oauthClientSecret.trim() ? { oauthClientSecret: f.oauthClientSecret.trim() } : {}),
            ...(f.s3SecretKey.trim() ? { s3SecretKey: f.s3SecretKey.trim() } : {}),
          };
  }
}

export function secretTyped(type: ServiceType, f: ServiceForm): boolean {
  if (type === "http")
    return [f.password, f.token, f.keyValue, f.clientSecret].some((v) => v.length > 0);
  if (type === "database") return f.dbPassword.length > 0;
  if (type === "external_kafka") return f.saslPassword.length > 0;
  if (type === "sink_destination") return [f.oauthClientSecret, f.s3AccessKey, f.s3SecretKey].some((v) => v.length > 0);
  return false;
}

export function saveBlockReason(type: ServiceType | null, f: ServiceForm): string | null {
  if (!type) return "Pick a service type first.";
  if (!f.name.trim()) return "Name the service.";
  if (type === "http" && !f.baseUrl.trim()) return "Base URL is required.";
  if (type === "database" && f.dialect === "trino" && !f.dbUrl.trim() && !f.host.trim()) return "Trino coordinator URL is required.";
  if (type === "database" && f.dialect !== "trino" && (!f.host.trim() || !f.database.trim())) return "Host and database are required.";
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
  info,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  mono?: boolean;
  info?: React.ReactNode;
}) {
  return (
    <Field label={label} info={info}>
      <Input
        value={value}
        placeholder={placeholder}
        className={mono ? "font-mono text-xs" : undefined}
        onChange={(e) => onChange(e.target.value)}
      />
    </Field>
  );
}

/**
 * A write-only credential field.
 *
 * "Write-only — never displayed again." used to be printed under every one of
 * these. An Iceberg sink has three secrets and an OAuth http service has two, so
 * the same sentence appeared three or four times in a single form. It is a
 * property of the CONTROL, not news about this particular field — so it becomes
 * a lock glyph on the label, with the sentence in its tooltip.
 */
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
    <Field
      label={
        <span className="inline-flex items-center gap-1.5">
          {label}
          <Tooltip>
            <TooltipTrigger asChild>
              <span className="cursor-help text-muted-foreground/60">
                <Lock className="h-3 w-3" />
              </span>
            </TooltipTrigger>
            <TooltipContent>Write-only — stored server-side and never displayed again.</TooltipContent>
          </Tooltip>
        </span>
      }
    >
      <Input
        type="password"
        value={value}
        placeholder={editing ? "Leave blank to keep the existing secret" : ""}
        onChange={(e) => onChange(e.target.value)}
      />
    </Field>
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
    <Field
      label="API gateway egress"
      info={
        <>
          For endpoints NiFi refuses (broken or nonstandard TLS) and client-certificate presentation. A proxy answers
          "how is this host reached?", which every block calling the host must answer the same way —{" "}
          <Link to="/apisix" className="text-primary underline underline-offset-2">
            manage proxies
          </Link>
          .
        </>
      }
      // Only the CONSEQUENCE of the current selection stays inline: which host
      // is routed where, and the fact that certificates go unverified. That
      // changes with the choice, so it is derived state rather than a rule.
      hint={
        selected ? (
          <>
            Calls <span className="font-mono">{selected.targetHost}</span> through "{selected.name}" — upstream
            certificates are not verified in this mode.
          </>
        ) : undefined
      }
    >
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
    </Field>
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
    <div className="grid gap-4">

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
                  <Field label="Auth mode">
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
                  </Field>
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
                      <Field label="Key location">
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
                      </Field>
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
                      <TextField
                        label="Injection template"
                        mono
                        value={form.tokenTemplate}
                        placeholder="Bearer ${token}"
                        info={
                          <>
                            How the token is inserted into the header — use {"${token}"} as the placeholder. Leave empty
                            to send the raw token. Logins are never modelled as data streams; session bootstrap lives
                            here.
                          </>
                        }
                        onChange={(tokenTemplate) => setForm((p) => ({ ...p, tokenTemplate }))}
                      />
                    </>
                  )}
                </>
              )}

              {formType === "database" && (
                <>
                  <Field label="Dialect">
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
                  </Field>
                  {form.dialect === "trino" ? (
                    <>
                      <TextField
                        label="Coordinator URL"
                        mono
                        value={form.dbUrl}
                        placeholder="https://trino.datapasc.com"
                        onChange={(dbUrl) => setForm((p) => ({ ...p, dbUrl }))}
                      />
                      <p className="text-xs text-muted-foreground">
                        Use the URL NiFi can reach. HTTPS is carried into the JDBC connection automatically.
                      </p>
                    </>
                  ) : (
                    <>
                      <div className="grid grid-cols-[1fr_110px] gap-3">
                        <TextField label="Host" mono value={form.host} onChange={(host) => setForm((p) => ({ ...p, host }))} />
                        <TextField label="Port" mono value={form.port} onChange={(port) => setForm((p) => ({ ...p, port }))} />
                      </div>
                      <TextField label="Database" mono value={form.database} onChange={(database) => setForm((p) => ({ ...p, database }))} />
                    </>
                  )}
                  <TextField label="Username" value={form.dbUsername} onChange={(dbUsername) => setForm((p) => ({ ...p, dbUsername }))} />
                  <SecretField label="Password" value={form.dbPassword} editing={!!editing} onChange={(dbPassword) => setForm((p) => ({ ...p, dbPassword }))} />
                  <TextField
                    label="Driver JAR location(s)"
                    mono
                    value={form.driverLocations}
                    placeholder="/opt/nifi/nifi-current/nar_extensions/trino-jdbc-480.jar"
                    info="Path(s) on the NiFi host to the JDBC driver JAR — needed for drivers NiFi doesn't bundle (e.g. Trino). Comma-separated."
                    onChange={(driverLocations) => setForm((p) => ({ ...p, driverLocations }))}
                  />
                  <Field label="Capabilities">
                    <div className="flex gap-2">
                      <label className="inline-flex h-9 cursor-pointer items-center gap-2 rounded-lg bg-card px-3 text-sm font-medium shadow-sm ring-1 ring-inset ring-input transition-colors hover:bg-accent">
                        <Checkbox checked={form.capRead} onCheckedChange={(v) => setForm((p) => ({ ...p, capRead: v === true }))} aria-label="Read capability" />
                        Read
                      </label>
                      <label className="inline-flex h-9 cursor-pointer items-center gap-2 rounded-lg bg-card px-3 text-sm font-medium shadow-sm ring-1 ring-inset ring-input transition-colors hover:bg-accent">
                        <Checkbox checked={form.capWrite} onCheckedChange={(v) => setForm((p) => ({ ...p, capWrite: v === true }))} aria-label="Write capability" />
                        Write
                      </label>
                    </div>
                  </Field>
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
                  <Field
                    label="Security protocol"
                    info="Input only — external clusters are never a destination (R6)."
                  >
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
                  </Field>
                  <TextField label="SASL username" value={form.saslUsername} onChange={(saslUsername) => setForm((p) => ({ ...p, saslUsername }))} />
                  <SecretField label="SASL password" value={form.saslPassword} editing={!!editing} onChange={(saslPassword) => setForm((p) => ({ ...p, saslPassword }))} />
                </>
              )}

              {formType === "sink_destination" && (
                <>
                  <Field label="Kind">
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
                  </Field>
                  {form.sinkKind === "opensearch" ? (
                    <>
                      <TextField label="URL" mono value={form.sinkUrl} placeholder="https://opensearch.corp:9200" onChange={(sinkUrl) => setForm((p) => ({ ...p, sinkUrl }))} />
                      <TextField label="Index prefix" mono value={form.indexPrefix} placeholder="dmp-" onChange={(indexPrefix) => setForm((p) => ({ ...p, indexPrefix }))} />
                      <Field label="Write mode">
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
                      </Field>
                    </>
                  ) : (
                    <div className="grid gap-4">
                      <TextField label="Catalog URL" mono value={form.catalogUrl} placeholder="http://polaris.corp:8181/api/catalog" onChange={(catalogUrl) => setForm((p) => ({ ...p, catalogUrl }))} />
                      <TextField label="Warehouse" mono value={form.warehouse} placeholder="bronze" onChange={(warehouse) => setForm((p) => ({ ...p, warehouse }))} />
                      <div className="grid gap-4 rounded-xl bg-muted/40 p-3.5 ring-1 ring-inset ring-border/50">
                        <SectionLabel>OAuth</SectionLabel>
                        <TextField label="OAuth client id" value={form.oauthClientId} onChange={(oauthClientId) => setForm((p) => ({ ...p, oauthClientId }))} />
                        <SecretField
                          label="OAuth client secret"
                          value={form.oauthClientSecret}
                          editing={!!editing}
                          onChange={(oauthClientSecret) => setForm((p) => ({ ...p, oauthClientSecret }))}
                        />
                      </div>
                      <div className="grid gap-4 rounded-xl bg-muted/40 p-3.5 ring-1 ring-inset ring-border/50">
                        <SectionLabel>S3</SectionLabel>
                        <TextField label="S3 endpoint" mono value={form.s3Endpoint} placeholder="https://ozones3.corp" onChange={(s3Endpoint) => setForm((p) => ({ ...p, s3Endpoint }))} />
                        <SecretField
                          label="S3 access key"
                          value={form.s3AccessKey}
                          editing={!!editing}
                          onChange={(s3AccessKey) => setForm((p) => ({ ...p, s3AccessKey }))}
                        />
                        <SecretField
                          label="S3 secret key"
                          value={form.s3SecretKey}
                          editing={!!editing}
                          onChange={(s3SecretKey) => setForm((p) => ({ ...p, s3SecretKey }))}
                        />
                        <TextField label="S3 region" mono value={form.s3Region} placeholder="us-east-1" onChange={(s3Region) => setForm((p) => ({ ...p, s3Region }))} />
                        <label className="inline-flex w-fit cursor-pointer items-center gap-2 text-sm font-medium">
                          <Checkbox
                            checked={form.s3PathStyle}
                            onCheckedChange={(v) => setForm((p) => ({ ...p, s3PathStyle: v === true }))}
                            aria-label="Path-style access"
                          />
                          Path-style access
                        </label>
                      </div>
                    </div>
                  )}
                </>
              )}
    </div>
  );
}
