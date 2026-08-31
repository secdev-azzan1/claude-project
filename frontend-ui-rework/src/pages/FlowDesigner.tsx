import { useEffect, useMemo, useState } from "react";
import { AppLayout } from "@/components/AppLayout";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from "@/components/ui/accordion";
import { Progress } from "@/components/ui/progress";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Globe,
  Database,
  Leaf,
  Folder,
  Check,
  ArrowRight,
  ArrowLeft,
  ArrowDown,
  Star,
  Plus,
  Trash2,
  Save,
  Play,
  Loader2,
  CheckCircle,
  XCircle,
  ChevronRight as ChevronRightIcon,
  Zap,
  AlertCircle,
  RefreshCw,
  Clock,
  Braces,
  Eye,
  GitBranch,
  KeyRound,
  Link2,
  ListTree,
  Sparkles,
  Wand2,
  Upload,
  Link2Off,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { toast } from "sonner";
import { useNavigate, useSearchParams } from "react-router-dom";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import {
  schemasApi,
  type ApiSchemaArtifact,
  type ApiSchemaVersion,
  type SchemaVersionStatus,
} from "@/lib/schemaApi";
import { flowsApi, sourcesApi, type ApiFlow, type ApiSource } from "@/lib/flowApi";
import { api, ApiError, getApiBase } from "@/lib/api";
import { inferenceApi, type InferenceJob, type InferenceStatus } from "@/lib/inferenceApi";
import { normalizeSmbClickedPath } from "@/lib/smbPathNormalization";
import { openApiApi, type OpenApiServer } from "@/lib/openapiApi";
import { nifiServicesApi, type NifiGlobalService } from "@/lib/nifiServicesApi";
import { applicationServicesApi, type ApplicationService } from "@/lib/applicationServicesApi";
import { useDebouncedValue } from "@/lib/useDebouncedValue";
import { buildStreamGraph, type StreamGraphEdge, type StreamGraphEdges } from "@/lib/streamGraph";
import { normalizeDefaultRouteAction } from "@/lib/defaultRouteAction";
import { StreamFlowMap } from "@/components/flow-map/StreamFlowMap";
import {
  collectMissingPathParamResolutionsForInference as collectInferencePreflightIssues,
} from "@/lib/inferencePreflight";
import {
  mergeSourceConnectionState,
  type SourceConnectionMode,
  type SourceConnectionRecord,
} from "@/lib/flowDesignerConnectionState";
import {
  getEntitySchemaStatusLabel,
  requiresSchemaWorkflow,
  shouldBlockSaveForUnverifiedSchemas,
} from "@/lib/flowDesignerSchemaRequirement";

type SourceType = "rest" | "postgres" | "mongo" | "smb" | "webhook" | "trino";
type MongoConnectionMode = "basic" | "uri" | "nifi_global_service";
type RestAuthType = "NONE" | "API_KEY" | "BEARER" | "BASIC";
type ApiKeyLocation = "HEADER" | "QUERY";
type StreamMethod = "GET" | "POST" | "PUT" | "PATCH" | "DELETE";
type BodyFormat = "EMPTY" | "JSON" | "XML" | "CSV";
type InjectAs = "QUERY_PARAM" | "PATH_PARAM" | "HEADER" | "BODY";
type ScheduleType = "interval" | "cron";
type IntervalUnit = "seconds" | "minutes" | "hours" | "days";
type SchemaMode = "existing" | "auto_generate";
type PaginationType = "NONE" | "PAGE_INCREMENT" | "CURSOR" | "OFFSET_LIMIT" | "NEXT_URL";
type PaginationStopCondition = "EMPTY_ARRAY" | "TOTAL_COUNT" | "HAS_MORE" | "MAX_PAGES";
type MetadataSource = "BODY" | "HEADER";
type NextUrlSource = "BODY" | "HEADER" | "LINK_HEADER";
type PaginationLocation = "QUERY" | "BODY";
type TotalCountStopRule = "FETCHED_RECORDS_GTE_TOTAL" | "PAGE_WINDOW_GTE_TOTAL";
type OffsetTotalCountStopRule = "FETCHED_RECORDS_GTE_TOTAL" | "OFFSET_PLUS_LIMIT_GTE_TOTAL";
type OffsetStopCondition = "EMPTY_ARRAY" | "TOTAL_COUNT" | "HAS_MORE";
type TransformationAction = "ADD_FIELD" | "REMOVE_FIELD" | "SET_FROM_ATTRIBUTE";
type RoutingCondition = "equals" | "contains" | "starts_with" | "ends_with" | "regex" | "is_empty";
type RoutingDestination = "INCLUDE" | "EXCLUDE" | "ROUTE_TO_STREAM";
type DefaultRouteAction = "DROP" | "KEEP" | "ROUTE_TO_STREAM";
type KafkaKeyStrategy = "none" | "primary_key";
type KafkaValueFormat = "avro";
type ServiceType = "nifi" | "kafka" | "apicurio";
type BranchingMode = "none" | "conditional" | "parallel";
type FanOutCompletionMode = "per_record" | "after_all";
type ConnectionRecord = {
  id: string;
  type: ServiceType;
};

const RUNTIME_SERVICE_TYPES: ServiceType[] = ["nifi", "kafka", "apicurio"];
const RUNTIME_SERVICE_LABELS: Record<ServiceType, string> = {
  nifi: "NiFi",
  kafka: "Kafka",
  apicurio: "Apicurio",
};

type AttributeExtractionRule = {
  id: string;
  attributeName: string;
  pathExpression: string;
  defaultValue: string;
};

type TransformationRule = {
  id: string;
  action: TransformationAction;
  fieldName: string;
  staticValue: string;
  attributeName: string;
};

type RoutingRule = {
  id: string;
  attributeName: string;
  condition: RoutingCondition;
  value: string;
  destination: RoutingDestination;
  targetStreamId: string;
};

// New routing model types (Phase 4)
type FilterRule = {
  id: string;
  attribute: string;
  condition: RoutingCondition;
  value: string;
  action: "include" | "exclude";
};

type RouteConditionType = {
  attribute: string;
  condition: RoutingCondition;
  value: string;
};

type Route = {
  id: string;
  condition: RouteConditionType;
  childStreamId: string;
  ordinal: number;
};

type RouteSource = {
  parentStreamId: string;
  routeId: string;
};

type EntityKafkaConfig = {
  topic: string;
  partitions: number;
  replicationFactor: number;
  keyStrategy: KafkaKeyStrategy;
  valueFormat: string;
  schemaMode: string;
  schemaArtifactId: string;
  schemaVersion: string;
};

type EntityIcebergConfig = {
  enabled: boolean;
};

type EntityConfig = {
  kafka: EntityKafkaConfig;
  schemaArtifactId: string;
  schemaVersion: string;
  iceberg?: EntityIcebergConfig | null;
};

type ParamValueSource = "parent_field" | "static" | "global_var" | "manual_runtime";

type StreamParamBinding = {
  id: string;
  paramName: string;
  paramIn: "path" | "query" | "header" | "body";
  required: boolean;
  enabled: boolean;
  valueSource: ParamValueSource;
  parentStreamId: string;
  parentField: string;
  staticValue: string;
  globalVarName: string;
  runtimeVarName: string;
  confidence: "high" | "medium" | "low";
  description: string;
  schemaType: string;
  defaultValue: string;
};

type Stream = {
  id: string;
  name: string;
  branchingMode: BranchingMode;
  method: StreamMethod;
  endpointPath: string;
  requestEnabled: boolean;
  openApiOperationId: string;
  requestBodyTemplate: string;
  bodyFormat: BodyFormat;
  additionalHeaders: string;
  additionalQueryParams: string;
  responseDataPath: string;
  responseFormat: "JSON" | "XML" | "CSV";
  splitResponseIntoRecords: boolean;
  waitForAllRecordsBeforeDownstream: boolean;
  xmlSplitDepth: string;
  xmlRecordElement: string;
  csvDelimiter: string;
  csvHasHeaderRow: boolean;
  csvQuoteChar: string;
  csvEscapeChar: string;
  mongoQuery: string;
  mongoProjection: string;
  mongoSort: string;
  mongoLimit: string;
  primaryKeyFields: string;
  isPrimary: boolean;
  attributeExtractions: AttributeExtractionRule[];
  paginationEnabled: boolean;
  paginationType: PaginationType;
  pageParamName: string;
  pageSizeParamName: string;
  pageSizeValue: string;
  firstPageNumber: string;
  pageStopCondition: PaginationStopCondition;
  totalCountPath: string;
  totalCountStopRule: TotalCountStopRule;
  maxPages: string;
  cursorParamName: string;
  cursorValuePath: string;
  cursorStopPath: string;
  cursorPageSizeParamName: string;
  cursorPageSizeValue: string;
  nextUrlPath: string;
  offsetParamName: string;
  limitParamName: string;
  limitValue: string;
  offsetStopCondition: OffsetStopCondition;
  offsetTotalCountPath: string;
  offsetTotalCountStopRule: OffsetTotalCountStopRule;
  paginationLocation: PaginationLocation;
  cursorSource: MetadataSource;
  cursorHeader: string;
  nextUrlSource: NextUrlSource;
  nextUrlHeader: string;
  totalCountSource: MetadataSource;
  totalCountHeader: string;
  offsetTotalCountSource: MetadataSource;
  offsetTotalCountHeader: string;
  hasMoreSource: MetadataSource;
  hasMorePath: string;
  hasMoreHeader: string;
  parentStreamId: string;
  fanOutCompletionMode: FanOutCompletionMode;
  parentField: string;
  injectAs: InjectAs;
  injectFieldName: string;
  injectTemplate: string;
  pathParamDefaults: Record<string, string>;
  transformations: TransformationRule[];
  routingRules: RoutingRule[];
  defaultRouteAction: DefaultRouteAction;
  defaultRouteTargetStreamId: string;
  openApiParamBindings: StreamParamBinding[];
  // New routing model fields (Phase 4)
  filters: FilterRule[];
  routes: Route[];
  defaultRouteChildStreamId: string;
  routeSource: RouteSource | null;
  entity: EntityConfig | null;
};

type EntityDestinationDraft = {
  partitions: string;
  replicationFactor: string;
  keyStrategy: KafkaKeyStrategy;
  valueFormat: KafkaValueFormat;
  schemaMode: SchemaMode;
  existingSchemaArtifactId: string;
  existingSchemaVersion: string;
  iceberg: EntityIcebergConfig;
};

type PendingStreamTest = {
  streamId: string;
  values: Record<string, string>;
  variables: Array<{ name: string; required: boolean }>;
  confirmMutation?: boolean;
};

type InferenceRuntimeParam = {
  key: string;
  streamName: string;
  paramName: string;
  description: string;
};

type PendingInferenceStart = {
  flowId: string;
  entityStreamId?: string;
  params: InferenceRuntimeParam[];
  values: Record<string, string>;
};

type RestSourceConfig = {
  sourceName: string;
  baseUrl: string;
  authType: RestAuthType;
  apiKeyName: string;
  apiKeyValue: string;
  apiKeyLocation: ApiKeyLocation;
  bearerToken: string;
  username: string;
  password: string;
  connectionTimeout: string;
  socketReadTimeout: string;
  socketWriteTimeout: string;
  socketIdleTimeout: string;
  rateLimitPerMin: string;
  openApiSpecId: string;
  openApiFileName: string;
  openApiTitle: string;
  openApiVersion: string;
  openApiFormat: string;
  openApiWarnings: string[];
  openApiServers: OpenApiServer[];
  openApiSelectedServerUrl: string;
  openApiOperationsCount: number;
};

type DatabaseSourceConfig = {
  sourceName: string;
  connectionMode: MongoConnectionMode;
  nifiGlobalServiceId: string;
  host: string;
  port: string;
  databaseName: string;
  username: string;
  password: string;
  sslMode: string;
  connectionTimeout: string;
  connectionUri: string;
};

type SmbSourceConfig = {
  sourceName: string;
  smbConnection: string;
  shareName: string;
  username: string;
  password: string;
  domain: string;
  baseDirectoryPath: string;
  fileFilterPattern: string;
  fileFormat: "CSV" | "JSON" | "JSONL" | "XLSX" | "EXCEL" | "BINARY";
  csvDelimiter: string;
  hasHeaderRow: boolean;
};

type WebhookSourceConfig = {
  sourceName: string;
  endpointId: string;
  enabled: boolean;
  contentType: "auto" | "json" | "xml" | "text";
  signatureHeader: string;
  signatureSecret: string;
  signatureAlgo: "hmac-sha256";
};

type TrinoSourceConfig = {
  sourceName: string;
  endpoint: string;
  user: string;
  catalog: string;
  schema: string;
  table: string;
  checkpointCatalog: string;
  checkpointSchema: string;
  checkpointTable: string;
  autoCreateCheckpointTable: boolean;
  columnMode: "all" | "manual";
  columns: string;
};

type KafkaOutputConfig = {
  topic: string;
  partitions: string;
  replicationFactor: string;
  keyStrategy: KafkaKeyStrategy;
  valueFormat: KafkaValueFormat;
  schemaMode: SchemaMode;
  existingSchemaArtifactId: string;
  existingSchemaVersion: string;
};

type SourceSchedule = {
  type: ScheduleType;
  interval: {
    value: number | "";
    unit: IntervalUnit;
  };
  cron: {
    expression: string;
  };
};

type FlowDesignerPayload = {
  step: number;
  sourceType: SourceType;
  restSource: RestSourceConfig;
  postgresSource: DatabaseSourceConfig;
  mongoSource: DatabaseSourceConfig;
  smbSource: SmbSourceConfig;
  webhookSource: WebhookSourceConfig;
  trinoSource: TrinoSourceConfig;
  restConnectionMode: SourceConnectionMode;
  restApplicationServiceId: string;
  smbConnectionMode: SourceConnectionMode;
  smbApplicationServiceId: string;
  webhookConnectionMode: SourceConnectionMode;
  webhookApplicationServiceId: string;
  streams: Stream[];
  editingStream: string;
  kafkaOutput: KafkaOutputConfig;
  entityDestinations?: Record<string, EntityDestinationDraft>;
  schedule: SourceSchedule;
  savedAt: string;
};

type ResponseNodeKind = "object" | "array" | "string" | "number" | "boolean" | "null";

type ResponseExplorerNode = {
  path: string;
  label: string;
  kind: ResponseNodeKind;
  summary: string;
  value: unknown;
  children: ResponseExplorerNode[];
  depth: number;
  childCount: number;
  hiddenChildrenCount: number;
  truncatedByDepth: boolean;
};

type ResponseArraySuggestion = {
  path: string;
  label: string;
  itemCount: number;
  sampleKeys: string[];
};

type ResponseFieldSuggestion = {
  label: string;
  relativePath: string;
  fullPath: string;
  sample: string;
};

type ResponseInsights = {
  rootNode: ResponseExplorerNode | null;
  recordRootNode: ResponseExplorerNode | null;
  arraySuggestions: ResponseArraySuggestion[];
  fieldSuggestions: ResponseFieldSuggestion[];
  identifierSuggestions: ResponseFieldSuggestion[];
  recordPreview: unknown[];
  activeRecordPath: string;
  parsed: unknown;
  format: "JSON" | "XML";
  xmlPathMap?: Map<string, string>;
};

type XmlSplitSuggestion = {
  depth: number;
  recordElement: string;
};

const STEP_SOURCE_TYPE = 0;
const STEP_CONFIGURE_SOURCE = 1;
const STEP_STREAMS = 2;
const STEP_DESTINATION = 3;
const STEP_SCHEDULE = 4;
const STEP_REVIEW = 5;

const steps = ["Source Type", "Configure Source", "Streams", "Destination", "Flow Schedule", "Review"];
const FLOW_DESIGNER_DRAFT_KEY = "nif-flow-designer-draft-v2";

const sourceTypes: { id: SourceType; name: string; icon: typeof Globe; desc: string }[] = [
  { id: "rest", name: "REST API", icon: Globe, desc: "Pull JSON data from HTTP endpoints with pagination" },
  { id: "postgres", name: "PostgreSQL", icon: Database, desc: "Ingest tables via batch or incremental query" },
  { id: "mongo", name: "MongoDB", icon: Leaf, desc: "Ingest collections via query or incremental field" },
  { id: "smb", name: "SMB", icon: Folder, desc: "Ingest files from SMB shares with pattern filters" },
  { id: "webhook", name: "Webhook", icon: Link2, desc: "Receive pushed events at a dedicated endpoint" },
  { id: "trino", name: "Trino", icon: Database, desc: "Read Iceberg table snapshots through Trino and publish rows" },
];
const DISABLED_SOURCE_TYPES: SourceType[] = ["postgres"];

const sourceLabelToType = (value: string): SourceType => {
  if (value === "PostgreSQL") return "postgres";
  if (value === "MongoDB") return "mongo";
  if (value === "SMB") return "smb";
  if (value === "Webhook") return "webhook";
  if (value === "Trino") return "trino";
  return "rest";
};

const sourceTypeToApiType = (t: SourceType): string => {
  if (t === "postgres") return "PostgreSQL";
  if (t === "mongo") return "MongoDB";
  if (t === "smb") return "SMB";
  if (t === "webhook") return "Webhook";
  if (t === "trino") return "Trino";
  return "REST API";
};

const makeId = (prefix: string) => `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

const asRecord = (value: unknown): Record<string, unknown> | null =>
  value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : null;

const asString = (value: unknown, fallback = ""): string => (typeof value === "string" ? value : fallback);

const isCompleteDesignerPayload = (value: unknown): value is Partial<FlowDesignerPayload> => {
  const payload = asRecord(value);
  if (!payload) return false;
  if (!Array.isArray(payload.streams) || payload.streams.length === 0) return false;
  if (!asRecord(payload.kafkaOutput)) return false;
  if (!asRecord(payload.schedule)) return false;
  const sourceType = asString(payload.sourceType);
  return sourceType === "rest" || sourceType === "postgres" || sourceType === "mongo" || sourceType === "smb" || sourceType === "webhook" || sourceType === "trino";
};

const toInt = (value: unknown, fallback: number): number => {
  if (typeof value === "number" && Number.isFinite(value)) return Math.trunc(value);
  if (typeof value === "string") {
    const parsed = Number.parseInt(value, 10);
    if (Number.isFinite(parsed)) return Math.trunc(parsed);
  }
  return fallback;
};

const toOptionalPositiveInt = (value: unknown): number | "" => {
  if (value === "") return "";
  const parsed = toInt(value, 0);
  return parsed > 0 ? parsed : "";
};

const toRestAuthType = (value: unknown): RestAuthType => {
  const v = asString(value).toUpperCase();
  if (v === "API_KEY") return "API_KEY";
  if (v === "BEARER") return "BEARER";
  if (v === "BASIC") return "BASIC";
  return "NONE";
};

const normalizeKafkaKeyStrategy = (value: string): KafkaKeyStrategy =>
  value === "primary_key" ? "primary_key" : "none";

const normalizeKafkaValueFormat = (_value: string): KafkaValueFormat => "avro";

const toApiKeyLocation = (value: unknown): ApiKeyLocation => {
  const v = asString(value).toUpperCase();
  return v === "QUERY" ? "QUERY" : "HEADER";
};

const toTopicToken = (value: string, fallback: string): string => {
  const normalized = (value || "").trim().toLowerCase();
  if (!normalized) return fallback;
  const token = normalized.replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
  return token || fallback;
};

const buildKafkaTopicName = (sourceName: string, primaryStreamName: string): string => {
  const sourceToken = toTopicToken(sourceName, "source");
  const streamToken = toTopicToken(primaryStreamName, "stream");
  return `bronze.${sourceToken}.${streamToken}__history`;
};

const defaultEntityIcebergConfig = (): EntityIcebergConfig => ({
  enabled: false,
});

const parseEntityIcebergConfig = (raw: unknown): EntityIcebergConfig => {
  const rec = asRecord(raw);
  if (!rec) return defaultEntityIcebergConfig();
  return {
    enabled: Boolean(rec.enabled),
  };
};

const createEmptyEntityConfig = (): EntityConfig => ({
  kafka: {
    topic: "",
    partitions: 6,
    replicationFactor: 3,
    keyStrategy: "none",
    valueFormat: "avro",
    schemaMode: "auto_generate",
    schemaArtifactId: "",
    schemaVersion: "",
  },
  schemaArtifactId: "",
  schemaVersion: "",
});

const defaultEntityDestinationDraft = (kafkaOutput: KafkaOutputConfig): EntityDestinationDraft => ({
  partitions: kafkaOutput.partitions || "6",
  replicationFactor: kafkaOutput.replicationFactor || "3",
  keyStrategy: normalizeKafkaKeyStrategy(kafkaOutput.keyStrategy),
  valueFormat: "avro",
  schemaMode: kafkaOutput.schemaMode || "auto_generate",
  existingSchemaArtifactId: "",
  existingSchemaVersion: "",
  iceberg: defaultEntityIcebergConfig(),
});

const isConfiguredRoutingRule = (rule: RoutingRule): boolean =>
  Boolean(
    (rule.attributeName || "").trim() ||
      (rule.value || "").trim() ||
      (rule.targetStreamId || "").trim() ||
      rule.destination === "ROUTE_TO_STREAM",
  );

const routeRuleHasComparisonValue = (rule: RoutingRule): boolean =>
  rule.condition === "is_empty" || Boolean((rule.value || "").trim());

const getConfiguredBranchRouteRules = (stream: Stream): RoutingRule[] =>
  (stream.routingRules || []).filter(
    (rule) =>
      rule.destination === "ROUTE_TO_STREAM" &&
      isConfiguredRoutingRule(rule) &&
      Boolean((rule.attributeName || "").trim()) &&
      routeRuleHasComparisonValue(rule) &&
      Boolean((rule.targetStreamId || "").trim()),
  );

const getBranchingMode = (
  stream: Stream,
  graph: StreamGraphEdges,
): BranchingMode => {
  if (stream.branchingMode === "conditional" || stream.branchingMode === "parallel") {
    return stream.branchingMode;
  }
  const outgoing = graph.childrenByParent.get(stream.id) || [];
  const hasParallelChildren = outgoing.some((edge) => edge.kind === "fanout");
  const hasConditionalChildren =
    outgoing.some((edge) => edge.kind === "route" || edge.kind === "default_route") ||
    getConfiguredBranchRouteRules(stream).length > 0;
  if (hasParallelChildren) return "parallel";
  if (hasConditionalChildren) return "conditional";
  return "none";
};

const getEntityStreams = (streams: Stream[]): Stream[] => streams.filter((stream) => Boolean(stream.entity));

const toInjectAs = (value: unknown): InjectAs => {
  const v = asString(value).toUpperCase();
  if (v === "PATH_PARAM") return "PATH_PARAM";
  if (v === "HEADER") return "HEADER";
  if (v === "BODY") return "BODY";
  return "QUERY_PARAM";
};

const toPaginationType = (value: unknown): PaginationType => {
  const v = asString(value).toLowerCase();
  if (v === "page") return "PAGE_INCREMENT";
  if (v === "cursor") return "CURSOR";
  if (v === "offset") return "OFFSET_LIMIT";
  if (v === "next_url" || v === "next-url" || v === "next") return "NEXT_URL";
  return "NONE";
};

const toTransformationAction = (value: unknown): TransformationAction => {
  const v = asString(value).toLowerCase();
  if (v === "remove_field") return "REMOVE_FIELD";
  if (v === "set_from_attribute") return "SET_FROM_ATTRIBUTE";
  return "ADD_FIELD";
};

const toRoutingCondition = (value: unknown): RoutingCondition => {
  const v = asString(value).toLowerCase();
  if (v === "contains") return "contains";
  if (v === "starts_with") return "starts_with";
  if (v === "ends_with") return "ends_with";
  if (v === "regex") return "regex";
  if (v === "is_empty") return "is_empty";
  return "equals";
};

const toRoutingDestination = (value: unknown): RoutingDestination => {
  const v = asString(value).toLowerCase();
  if (v === "exclude" || v === "drop" || v === "blocked") return "EXCLUDE";
  if (v === "route_to_stream" || v === "stream" || v === "child_stream") return "ROUTE_TO_STREAM";
  if (v === "separate_topic" || v === "topic" || v === "route_to_topic") return "ROUTE_TO_STREAM";
  return "INCLUDE";
};

const tryPrettyJson = (text: string): string | null => {
  try {
    const parsed = JSON.parse(text);
    return JSON.stringify(parsed, null, 2);
  } catch {
    return null;
  }
};

const prettyXml = (xml: string): string => {
  const compact = xml.replace(/>\s*</g, "><").trim();
  const withBreaks = compact.replace(/(>)(<)(\/*)/g, "$1\n$2$3");
  const lines = withBreaks.split("\n");
  let pad = 0;

  return lines
    .map((line) => {
      const trimmed = line.trim();
      if (!trimmed) return "";
      if (/^<\//.test(trimmed)) {
        pad = Math.max(pad - 1, 0);
      }
      const out = `${"  ".repeat(pad)}${trimmed}`;
      if (/^<[^!?/][^>]*[^/]>\s*$/.test(trimmed)) {
        pad += 1;
      }
      return out;
    })
    .filter(Boolean)
    .join("\n");
};

const formatResponsePreview = (result?: {
  response_data?: unknown;
  response_data_type?: "json" | "text" | null;
}): string => {
  if (!result || result.response_data === undefined || result.response_data === null) return "";

  if (result.response_data_type === "json") {
    if (typeof result.response_data === "string") {
      return tryPrettyJson(result.response_data) ?? result.response_data;
    }
    return JSON.stringify(result.response_data, null, 2);
  }

  if (typeof result.response_data === "string") {
    const trimmed = result.response_data.trim();
    const jsonPretty = tryPrettyJson(trimmed);
    if (jsonPretty) return jsonPretty;
    if ((trimmed.startsWith("<") && trimmed.endsWith(">")) || trimmed.startsWith("<?xml")) {
      return prettyXml(trimmed);
    }
    return result.response_data;
  }

  return JSON.stringify(result.response_data, null, 2);
};

const KEY_VALUE_SEPARATOR = /[:=]/;
const JSON_PATH_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;
const SAFE_ATTRIBUTE_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;
const RESPONSE_TREE_MAX_DEPTH = 8;
const RESPONSE_TREE_MAX_ARRAY_CHILDREN = 25;
const RESPONSE_TREE_MAX_OBJECT_CHILDREN = 40;
const RESPONSE_FIELD_SUGGESTION_RECORD_SAMPLE_SIZE = 5;

const parseKeyValueLines = (value: string): Record<string, string> => {
  const out: Record<string, string> = {};

  value
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .forEach((line) => {
      const separatorIndex = line.search(KEY_VALUE_SEPARATOR);
      if (separatorIndex < 0) return;
      const key = line.slice(0, separatorIndex).trim();
      const rawValue = line.slice(separatorIndex + 1).trim();
      if (!key) return;
      out[key] = rawValue;
    });

  return out;
};

const parseTrinoColumns = (value: string): string[] =>
  value
    .split(/[\n,]/)
    .map((item) => item.trim())
    .filter(Boolean);

const splitTrinoTableName = (value: string): { catalog: string; schema: string; table: string } | null => {
  const parts = value.split(".").map((item) => item.trim()).filter(Boolean);
  if (parts.length !== 3) return null;
  return { catalog: parts[0], schema: parts[1], table: parts[2] };
};

const findInvalidKeyValueLines = (value: string): string[] => {
  return value
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .filter((line) => line.search(KEY_VALUE_SEPARATOR) < 0);
};

const parsePositiveInteger = (value: string): number | null => {
  const trimmed = value.trim();
  if (!trimmed) return null;
  if (!/^\d+$/.test(trimmed)) return null;
  const parsed = Number.parseInt(trimmed, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return Math.trunc(parsed);
};

const parseNonNegativeInteger = (value: string): number | null => {
  const trimmed = value.trim();
  if (!trimmed) return null;
  if (!/^\d+$/.test(trimmed)) return null;
  const parsed = Number.parseInt(trimmed, 10);
  if (!Number.isFinite(parsed) || parsed < 0) return null;
  return Math.trunc(parsed);
};

const parsePort = (value: string): number | null => {
  const parsed = parsePositiveInteger(value);
  if (!parsed) return null;
  return parsed >= 1 && parsed <= 65535 ? parsed : null;
};

const isLikelyHttpUrl = (value: string): boolean => {
  try {
    const parsed = new URL(value.trim());
    return parsed.protocol === "http:" || parsed.protocol === "https:";
  } catch {
    return false;
  }
};

const isLikelyCronExpression = (value: string): boolean => {
  const chunks = value.trim().split(/\s+/).filter(Boolean);
  return chunks.length >= 5;
};

const isValidJson = (value: string): boolean => {
  try {
    JSON.parse(value);
    return true;
  } catch {
    return false;
  }
};

const formatKeyValueLines = (value: unknown): string => {
  const record = asRecord(value);
  if (!record) return "";
  return Object.entries(record)
    .map(([key, item]) => `${key}=${typeof item === "string" ? item : String(item ?? "")}`)
    .join("\n");
};

const toResponseNodeKind = (value: unknown): ResponseNodeKind => {
  if (Array.isArray(value)) return "array";
  if (value === null) return "null";
  if (typeof value === "object") return "object";
  if (typeof value === "number") return "number";
  if (typeof value === "boolean") return "boolean";
  return "string";
};

const formatSampleValue = (value: unknown): string => {
  if (value === null || value === undefined) return "null";
  if (typeof value === "string") return value.length > 48 ? `${value.slice(0, 45)}...` : value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  if (Array.isArray(value)) return `${value.length} item${value.length === 1 ? "" : "s"}`;
  if (typeof value === "object") return `${Object.keys(value as Record<string, unknown>).length} field(s)`;
  return String(value);
};

const summarizeNodeValue = (value: unknown): string => {
  if (Array.isArray(value)) {
    return `${value.length} item${value.length === 1 ? "" : "s"}`;
  }
  if (value && typeof value === "object") {
    return `${Object.keys(value as Record<string, unknown>).length} field(s)`;
  }
  return formatSampleValue(value);
};

const appendJsonPathSegment = (basePath: string, segment: string | number): string => {
  if (typeof segment === "number") return `${basePath}[${segment}]`;
  if (JSON_PATH_IDENTIFIER.test(segment)) return `${basePath}.${segment}`;
  return `${basePath}[${JSON.stringify(segment)}]`;
};

const parseJsonPath = (path: string): Array<string | number> => {
  if (!path || path === "$") return [];

  const tokens: Array<string | number> = [];
  let index = path.startsWith("$") ? 1 : 0;

  while (index < path.length) {
    const char = path[index];

    if (char === ".") {
      index += 1;
      let next = index;
      while (next < path.length && /[A-Za-z0-9_]/.test(path[next])) next += 1;
      const token = path.slice(index, next);
      if (token) tokens.push(token);
      index = next;
      continue;
    }

    if (char === "[") {
      const end = path.indexOf("]", index);
      if (end === -1) break;
      const token = path.slice(index + 1, end).trim();
      if ((token.startsWith("\"") && token.endsWith("\"")) || (token.startsWith("'") && token.endsWith("'"))) {
        tokens.push(token.slice(1, -1));
      } else {
        const numeric = Number.parseInt(token, 10);
        tokens.push(Number.isFinite(numeric) ? numeric : token);
      }
      index = end + 1;
      continue;
    }

    index += 1;
  }

  return tokens;
};

const getValueAtJsonPath = (value: unknown, path: string): unknown => {
  const tokens = parseJsonPath(path);
  let current: unknown = value;

  for (const token of tokens) {
    if (typeof token === "number") {
      if (!Array.isArray(current)) return undefined;
      current = current[token];
      continue;
    }

    const record = asRecord(current);
    if (!record) return undefined;
    current = record[token];
  }

  return current;
};

type StreamTestResultPayload = {
  response_data?: unknown;
  response_data_type?: "json" | "text" | null;
};

type ParsedPreviewPayload = {
  parsed: unknown | null;
  format: "JSON" | "XML";
  xmlPathMap?: Map<string, string>;
};

const parseXmlScalar = (value: string): unknown => {
  const trimmed = value.trim();
  if (!trimmed) return "";
  if (/^(true|false)$/i.test(trimmed)) return trimmed.toLowerCase() === "true";
  if (/^-?\d+(\.\d+)?$/.test(trimmed)) {
    const numeric = Number(trimmed);
    if (Number.isFinite(numeric)) return numeric;
  }
  return trimmed;
};

const xmlLocalName = (value: string): string => value.split(":").pop() || value;

const parseXmlElementValue = (
  element: Element,
  jsonPath: string,
  xPath: string,
  pathMap: Map<string, string>,
): unknown => {
  pathMap.set(jsonPath, xPath);

  const attributes = Array.from(element.attributes ?? []);
  const childElements = Array.from(element.children ?? []);
  const textContent = Array.from(element.childNodes)
    .filter((node) => node.nodeType === Node.TEXT_NODE)
    .map((node) => node.textContent?.trim() ?? "")
    .filter(Boolean)
    .join(" ")
    .trim();

  if (childElements.length === 0) {
    if (attributes.length === 0) {
      return parseXmlScalar(textContent);
    }

    const out: Record<string, unknown> = {};
    for (const attr of attributes) {
      const key = `@${xmlLocalName(attr.name)}`;
      const attrPath = appendJsonPathSegment(jsonPath, key);
      pathMap.set(attrPath, `${xPath}/@${xmlLocalName(attr.name)}`);
      out[key] = parseXmlScalar(attr.value);
    }
    if (textContent) {
      const textPath = appendJsonPathSegment(jsonPath, "#text");
      pathMap.set(textPath, `${xPath}/text()`);
      out["#text"] = parseXmlScalar(textContent);
    }
    return out;
  }

  const out: Record<string, unknown> = {};
  for (const attr of attributes) {
    const key = `@${xmlLocalName(attr.name)}`;
    const attrPath = appendJsonPathSegment(jsonPath, key);
    pathMap.set(attrPath, `${xPath}/@${xmlLocalName(attr.name)}`);
    out[key] = parseXmlScalar(attr.value);
  }

  const groupedChildren: Record<string, Element[]> = {};
  for (const child of childElements) {
    const key = xmlLocalName(child.tagName);
    if (!groupedChildren[key]) groupedChildren[key] = [];
    groupedChildren[key].push(child);
  }

  for (const [key, nodes] of Object.entries(groupedChildren)) {
    const keyPath = appendJsonPathSegment(jsonPath, key);
    if (nodes.length === 1) {
      out[key] = parseXmlElementValue(nodes[0], keyPath, `${xPath}/${key}`, pathMap);
      continue;
    }

    pathMap.set(keyPath, `${xPath}/${key}`);
    out[key] = nodes.map((child, index) =>
      parseXmlElementValue(
        child,
        appendJsonPathSegment(keyPath, index),
        `${xPath}/${key}[${index + 1}]`,
        pathMap,
      ),
    );
  }

  if (textContent) {
    const textPath = appendJsonPathSegment(jsonPath, "#text");
    pathMap.set(textPath, `${xPath}/text()`);
    out["#text"] = parseXmlScalar(textContent);
  }

  return out;
};

const parseXmlPreview = (xml: string): { parsed: unknown; pathMap: Map<string, string> } | null => {
  const trimmed = xml.trim();
  if (!trimmed) return null;
  if (typeof DOMParser === "undefined") return null;

  const parser = new DOMParser();
  const doc = parser.parseFromString(trimmed, "application/xml");
  if (doc.querySelector("parsererror")) return null;
  const root = doc.documentElement;
  if (!root) return null;

  const rootName = xmlLocalName(root.tagName);
  const pathMap = new Map<string, string>();
  const parsedRoot = parseXmlElementValue(root, appendJsonPathSegment("$", rootName), `/${rootName}`, pathMap);
  const parsed = { [rootName]: parsedRoot };
  pathMap.set("$", `/${rootName}`);
  return { parsed, pathMap };
};

const parsePreviewPayload = (
  result: StreamTestResultPayload | undefined,
  preferredFormat: "JSON" | "XML" | "CSV",
): ParsedPreviewPayload => {
  const fallbackFormat: "JSON" | "XML" = preferredFormat === "XML" ? "XML" : "JSON";
  if (!result || result.response_data === undefined || result.response_data === null) {
    return { parsed: null, format: fallbackFormat };
  }
  if (preferredFormat === "XML" && typeof result.response_data === "string") {
    const xmlParsed = parseXmlPreview(result.response_data);
    if (xmlParsed) {
      return {
        parsed: xmlParsed.parsed,
        format: "XML",
        xmlPathMap: xmlParsed.pathMap,
      };
    }
  }

  if (result.response_data_type === "json") {
    if (typeof result.response_data === "string") {
      try {
        return {
          parsed: JSON.parse(result.response_data),
          format: "JSON",
        };
      } catch {
        return { parsed: null, format: fallbackFormat };
      }
    }
    return {
      parsed: result.response_data,
      format: "JSON",
    };
  }

  if (typeof result.response_data === "string") {
    if (preferredFormat === "XML") {
      const xmlParsed = parseXmlPreview(result.response_data);
      if (xmlParsed) {
        return {
          parsed: xmlParsed.parsed,
          format: "XML",
          xmlPathMap: xmlParsed.pathMap,
        };
      }
    }
    try {
      return {
        parsed: JSON.parse(result.response_data),
        format: "JSON",
      };
    } catch {
      return { parsed: null, format: fallbackFormat };
    }
  }

  return { parsed: null, format: fallbackFormat };
};

const buildResponseExplorerNode = (
  value: unknown,
  path = "$",
  label = "$",
  depth = 0,
  maxDepth = RESPONSE_TREE_MAX_DEPTH,
): ResponseExplorerNode => {
  const kind = toResponseNodeKind(value);
  const valueObject = value && typeof value === "object" ? (value as Record<string, unknown>) : null;
  const totalChildCount = Array.isArray(value)
    ? value.length
    : valueObject
    ? Object.keys(valueObject).length
    : 0;

  if (depth >= maxDepth) {
    return {
      path,
      label,
      kind,
      summary: summarizeNodeValue(value),
      value,
      children: [],
      depth,
      childCount: totalChildCount,
      hiddenChildrenCount: totalChildCount,
      truncatedByDepth: totalChildCount > 0,
    };
  }

  if (Array.isArray(value)) {
    const visibleItems = value.slice(0, RESPONSE_TREE_MAX_ARRAY_CHILDREN);
    const hiddenChildrenCount = Math.max(0, value.length - visibleItems.length);
    const children = visibleItems.map((item, index) =>
      buildResponseExplorerNode(item, appendJsonPathSegment(path, index), `[${index}]`, depth + 1, maxDepth),
    );
    return {
      path,
      label,
      kind,
      summary: summarizeNodeValue(value),
      value,
      children,
      depth,
      childCount: value.length,
      hiddenChildrenCount,
      truncatedByDepth: false,
    };
  }

  const record = asRecord(value);
  if (record) {
    const entries = Object.entries(record);
    const visibleEntries = entries.slice(0, RESPONSE_TREE_MAX_OBJECT_CHILDREN);
    const hiddenChildrenCount = Math.max(0, entries.length - visibleEntries.length);
    const children = visibleEntries.map(([key, item]) =>
      buildResponseExplorerNode(item, appendJsonPathSegment(path, key), key, depth + 1, maxDepth),
    );
    return {
      path,
      label,
      kind,
      summary: summarizeNodeValue(value),
      value,
      children,
      depth,
      childCount: Object.keys(record).length,
      hiddenChildrenCount,
      truncatedByDepth: false,
    };
  }

  return {
    path,
    label,
    kind,
    summary: summarizeNodeValue(value),
    value,
    children: [],
    depth,
    childCount: 0,
    hiddenChildrenCount: 0,
    truncatedByDepth: false,
  };
};

const collectArraySuggestions = (
  value: unknown,
  path = "$",
  label = "$",
  depth = 0,
  suggestions: ResponseArraySuggestion[] = [],
): ResponseArraySuggestion[] => {
  if (depth > 4 || value === null || value === undefined) return suggestions;

  if (Array.isArray(value)) {
    if (value.length > 0) {
      const sample = value.find((item) => item !== null && item !== undefined) ?? value[0];
      suggestions.push({
        path,
        label,
        itemCount: value.length,
        sampleKeys: asRecord(sample) ? Object.keys(sample as Record<string, unknown>).slice(0, 4) : [],
      });
      if (depth < 3 && sample && typeof sample === "object") {
        collectArraySuggestions(sample, appendJsonPathSegment(path, 0), `${label}[0]`, depth + 1, suggestions);
      }
    }
    return suggestions;
  }

  const record = asRecord(value);
  if (!record) return suggestions;

  for (const [key, item] of Object.entries(record).slice(0, 12)) {
    collectArraySuggestions(item, appendJsonPathSegment(path, key), key, depth + 1, suggestions);
  }

  return suggestions;
};

const collectFieldSuggestions = (
  value: unknown,
  basePath = "$",
  suggestions: ResponseFieldSuggestion[] = [],
  depth = 0,
): ResponseFieldSuggestion[] => {
  if (depth > 3 || value === null || value === undefined) return suggestions;

  if (Array.isArray(value)) {
    const sample = value[0];
    if (sample !== undefined) {
      collectFieldSuggestions(sample, appendJsonPathSegment(basePath, 0), suggestions, depth + 1);
    }
    return suggestions;
  }

  const record = asRecord(value);
  if (!record) return suggestions;

  for (const [key, item] of Object.entries(record).slice(0, 20)) {
    const path = appendJsonPathSegment(basePath, key);
    if (item === null || ["string", "number", "boolean"].includes(typeof item)) {
      suggestions.push({
        label: key,
        relativePath: path,
        fullPath: path,
        sample: formatSampleValue(item),
      });
      continue;
    }

    collectFieldSuggestions(item, path, suggestions, depth + 1);
  }

  return suggestions;
};

const dedupeFieldSuggestions = (suggestions: ResponseFieldSuggestion[]): ResponseFieldSuggestion[] => {
  const seen = new Map<string, ResponseFieldSuggestion>();
  for (const item of suggestions) {
    const existing = seen.get(item.relativePath);
    if (!existing || (existing.sample === "null" && item.sample !== "null")) {
      seen.set(item.relativePath, item);
    }
  }
  return Array.from(seen.values());
};

const isLikelyIdentifierField = (label: string): boolean => /(^id$|_id$|Id$|ID$|uuid$|key$|code$)/.test(label);

const buildResponseInsights = (
  parsed: unknown,
  configuredRecordPath: string,
  splitResponseIntoRecords: boolean,
  format: "JSON" | "XML",
  xmlPathMap?: Map<string, string>,
): ResponseInsights => {
  const buildMergedRecordSample = (rows: unknown[]): unknown => {
    const samples = rows.filter((item) => item !== null && item !== undefined).slice(0, 25);
    const objects = samples.map((item) => asRecord(item)).filter((item): item is Record<string, unknown> => Boolean(item));
    if (objects.length === 0) return samples[0] ?? null;

    const mergeObjects = (list: Record<string, unknown>[], depth = 0): Record<string, unknown> => {
      if (depth > 6) return {};
      const merged: Record<string, unknown> = {};
      const keys = new Set<string>();
      for (const obj of list) {
        for (const key of Object.keys(obj)) keys.add(key);
      }
      for (const key of keys) {
        const values = list.map((obj) => obj[key]).filter((value) => value !== undefined);
        if (values.length === 0) continue;
        const firstNonNull = values.find((value) => value !== null) ?? values[0];
        if (Array.isArray(firstNonNull)) {
          const arrSamples = values.filter(Array.isArray) as unknown[][];
          const firstArray = arrSamples[0] ?? [];
          const firstElement = firstArray.find((value) => value !== null && value !== undefined);
          if (firstElement && asRecord(firstElement)) {
            const objectElements = arrSamples
              .map((arr) => arr.find((value) => value !== null && value !== undefined))
              .map((value) => asRecord(value))
              .filter((value): value is Record<string, unknown> => Boolean(value));
            merged[key] = [mergeObjects(objectElements, depth + 1)];
          } else {
            merged[key] = firstArray;
          }
          continue;
        }
        const nestedObjects = values
          .map((value) => asRecord(value))
          .filter((value): value is Record<string, unknown> => Boolean(value));
        if (nestedObjects.length > 0) {
          merged[key] = mergeObjects(nestedObjects, depth + 1);
        } else {
          merged[key] = firstNonNull;
        }
      }
      return merged;
    };

    return mergeObjects(objects);
  };

  const rootNode = buildResponseExplorerNode(parsed);
  const arraySuggestions = collectArraySuggestions(parsed);
  const supportsRecordPath = format === "JSON";
  const suggestedRecordPath = supportsRecordPath
    ? configuredRecordPath ||
      (Array.isArray(parsed) ? "$" : arraySuggestions.find((item) => item.path !== "$[0]")?.path ?? "")
    : "";
  const activeValue =
    supportsRecordPath && splitResponseIntoRecords && suggestedRecordPath
      ? getValueAtJsonPath(parsed, suggestedRecordPath)
      : parsed;
  const previewRecords = Array.isArray(activeValue) ? activeValue.slice(0, 3) : activeValue === undefined ? [] : [activeValue];
  const sampleRecordForTree = Array.isArray(activeValue)
    ? buildMergedRecordSample(activeValue)
    : previewRecords.find((item) => item !== null && item !== undefined) ?? previewRecords[0] ?? null;
  const recordRootNode =
    supportsRecordPath && splitResponseIntoRecords && sampleRecordForTree !== null
      ? buildResponseExplorerNode(sampleRecordForTree, "$", "$record")
      : null;
  const fieldSuggestionCandidates = Array.isArray(activeValue)
    ? activeValue
        .filter((item) => item !== null && item !== undefined)
        .slice(0, RESPONSE_FIELD_SUGGESTION_RECORD_SAMPLE_SIZE)
    : activeValue === undefined || activeValue === null
    ? []
    : [activeValue];
  const fieldSuggestions = dedupeFieldSuggestions(
    fieldSuggestionCandidates.flatMap((item) => collectFieldSuggestions(item)),
  );
  const identifierSuggestions = fieldSuggestions.filter((item) => isLikelyIdentifierField(item.label));

  return {
    rootNode,
    recordRootNode,
    arraySuggestions,
    fieldSuggestions,
    identifierSuggestions,
    recordPreview: previewRecords,
    activeRecordPath: suggestedRecordPath,
    parsed,
    format,
    xmlPathMap,
  };
};

const findResponseNodeByPath = (node: ResponseExplorerNode | null, path: string): ResponseExplorerNode | null => {
  if (!node) return null;
  if (node.path === path) return node;
  for (const child of node.children) {
    const found = findResponseNodeByPath(child, path);
    if (found) return found;
  }
  return null;
};

const toRelativeExtractionPath = (fullPath: string, recordPath: string): string => {
  if (!recordPath) return fullPath;
  const sampleRecordPrefix = recordPath === "$" ? "$[0]" : `${recordPath}[0]`;
  if (fullPath === sampleRecordPrefix) return "$";
  if (fullPath.startsWith(`${sampleRecordPrefix}.`) || fullPath.startsWith(`${sampleRecordPrefix}[`)) {
    return `$${fullPath.slice(sampleRecordPrefix.length)}`;
  }
  return fullPath;
};

const parseXPathSegments = (path: string): string[] =>
  path
    .split("/")
    .map((segment) => segment.trim())
    .filter(Boolean);

const stripXPathIndex = (segment: string): string => segment.replace(/\[\d+\]$/, "");

const toNamespaceSafeXPath = (segments: string[]): string => {
  if (segments.length === 0) return "/";
  return segments
    .map((segment) => {
      if (segment === "text()") return "/text()";
      if (segment.startsWith("@")) return `/@*[local-name()='${segment.slice(1)}']`;
      const local = stripXPathIndex(segment);
      return `/*[local-name()='${local}']`;
    })
    .join("");
};

const toXmlExtractionPath = (
  node: ResponseExplorerNode,
  xmlPathMap?: Map<string, string>,
  xmlSplitDepth?: string,
  xmlRecordElement?: string,
  splitResponseIntoRecords?: boolean,
): string => {
  if (!xmlPathMap) return "";
  const basePathRaw = xmlPathMap.get(node.path);
  if (!basePathRaw) return "";

  const baseSegments = parseXPathSegments(basePathRaw);
  if (baseSegments.length === 0) return "";
  const lastBase = baseSegments[baseSegments.length - 1];

  const fullSegments = [...baseSegments];
  const isScalar = ["string", "number", "boolean", "null"].includes(node.kind);
  const isAttr = lastBase.startsWith("@");
  const isText = lastBase === "text()";
  if (isScalar && !isAttr && !isText) {
    fullSegments.push("text()");
  }

  if (!splitResponseIntoRecords) {
    return toNamespaceSafeXPath(fullSegments);
  }

  const elementSegments = baseSegments.filter((segment) => !segment.startsWith("@") && segment !== "text()");
  if (elementSegments.length === 0) {
    return toNamespaceSafeXPath(fullSegments);
  }

  const parsedDepth = Number.parseInt(xmlSplitDepth || "1", 10);
  const splitDepth = Number.isFinite(parsedDepth) ? Math.max(parsedDepth, 1) : 1;
  const splitRootElementCount = Math.min(splitDepth + 1, elementSegments.length);
  let seenElements = 0;
  let splitStartIndex = 0;
  for (let index = 0; index < fullSegments.length; index += 1) {
    const segment = fullSegments[index];
    if (!segment.startsWith("@") && segment !== "text()") {
      seenElements += 1;
      if (seenElements === splitRootElementCount) {
        splitStartIndex = index + 1;
        break;
      }
    }
  }

  const relativeSegments = fullSegments.slice(splitStartIndex);
  return toNamespaceSafeXPath(relativeSegments);
};

const suggestXmlSplitFromNode = (
  node: ResponseExplorerNode | null,
  xmlPathMap?: Map<string, string>,
): XmlSplitSuggestion | null => {
  if (!node || !xmlPathMap) return null;
  if (node.kind !== "array" && node.kind !== "object") return null;

  const rawPath = xmlPathMap.get(node.path);
  if (!rawPath) return null;

  const elementSegments = parseXPathSegments(rawPath).filter(
    (segment) => !segment.startsWith("@") && segment !== "text()",
  );
  if (elementSegments.length === 0) return null;

  const recordElement = stripXPathIndex(elementSegments[elementSegments.length - 1]).trim();
  if (!recordElement) return null;

  // NiFi SplitXml depth 1 means "split children of root".
  // If selected node path is /root/child, split depth should be 1 (not 2).
  const depth = Math.max(elementSegments.length - 1, 1);

  return {
    depth,
    recordElement,
  };
};

const toAttributeName = (value: string): string => {
  const normalized = value
    .trim()
    .replace(/[^A-Za-z0-9_]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return normalized || "field";
};

const mapApiStreamToDesignerStream = (raw: unknown, index: number): Stream | null => {
  const rec = asRecord(raw);
  if (!rec) return null;

  const streamName = asString(rec.name).trim() || `stream_${index + 1}`;
  const stream = createDefaultStream(streamName);
  stream.id = asString(rec.id).trim() || stream.id;
  const routeSourceRec = asRecord(rec.route_source);
  const hasRouteSource = Boolean(routeSourceRec && asString(routeSourceRec.parent_stream_id));
  stream.requestEnabled = rec.request_enabled == null ? !hasRouteSource : Boolean(rec.request_enabled);
  const method = asString(rec.method).toUpperCase();
  stream.method =
    method === "POST" || method === "PUT" || method === "PATCH" || method === "DELETE"
      ? method
      : "GET";
  stream.endpointPath = asString(rec.endpoint_path, stream.endpointPath);
  const pathParamDefaultsRaw = asRecord(rec.path_param_defaults);
  if (pathParamDefaultsRaw) {
    const nextDefaults: Record<string, string> = {};
    for (const [k, v] of Object.entries(pathParamDefaultsRaw)) {
      const key = (k || "").trim();
      if (!key) continue;
      nextDefaults[key] = asString(v).trim();
    }
    stream.pathParamDefaults = nextDefaults;
  }
  stream.openApiOperationId = asString(rec.openapi_operation_id);
  stream.requestBodyTemplate = asString(rec.body_template);
  const bf = asString(rec.body_format).toUpperCase();
  stream.bodyFormat =
    bf === "JSON" || bf === "XML" || bf === "CSV"
      ? bf
      : stream.requestBodyTemplate.trim()
      ? "JSON"
      : "EMPTY";
  stream.additionalHeaders = formatKeyValueLines(rec.headers);
  stream.additionalQueryParams = formatKeyValueLines(rec.query_params);
  stream.responseDataPath = asString(rec.response_data_path);

  // XML response format fields
  const rf = asString(rec.response_format).toUpperCase();
  stream.responseFormat = rf === "XML" ? "XML" : rf === "CSV" ? "CSV" : "JSON";
  stream.csvDelimiter = asString(rec.csv_delimiter, stream.csvDelimiter || ",");
  stream.csvHasHeaderRow = rec.csv_has_header_row == null ? stream.csvHasHeaderRow : Boolean(rec.csv_has_header_row);
  stream.csvQuoteChar = asString(rec.csv_quote_char, stream.csvQuoteChar || "\"");
  stream.csvEscapeChar = asString(rec.csv_escape_char, stream.csvEscapeChar || "\\");
  stream.mongoQuery = asString(rec.mongo_query, stream.mongoQuery || "{}");
  stream.mongoProjection = asString(rec.mongo_projection, stream.mongoProjection);
  stream.mongoSort = asString(rec.mongo_sort, stream.mongoSort);
  stream.mongoLimit = rec.mongo_limit != null ? String(toInt(rec.mongo_limit, toInt(stream.mongoLimit, 0))) : stream.mongoLimit;
  stream.splitResponseIntoRecords =
    rec.split_response_records == null
      ? stream.responseFormat === "XML" || stream.responseFormat === "CSV" || Boolean(stream.responseDataPath)
      : Boolean(rec.split_response_records);
  stream.waitForAllRecordsBeforeDownstream = Boolean(rec.wait_for_all_records_before_downstream);
  stream.xmlSplitDepth = rec.xml_split_depth != null ? String(rec.xml_split_depth) : "1";
  stream.xmlRecordElement = asString(rec.xml_record_element);

  const pk = rec.primary_key_fields;
  if (Array.isArray(pk)) {
    stream.primaryKeyFields = pk.filter((v): v is string => typeof v === "string").join(", ");
  }
  stream.isPrimary = Boolean(rec.is_primary);

  const fanOut = asRecord(rec.fan_out);
  if (fanOut) {
    const parentId = asString(fanOut.parent_stream_id);
    stream.parentStreamId = parentId;
    stream.fanOutCompletionMode = asString(fanOut.completion_mode).toLowerCase() === "after_all" ? "after_all" : "per_record";
    stream.parentField = asString(fanOut.parent_field) || asString(fanOut.inject_field_name);
    stream.injectAs = toInjectAs(fanOut.inject_as);
    stream.injectFieldName = asString(fanOut.inject_field_name);
    stream.injectTemplate = asString(fanOut.inject_template);
  }

  const pagination = asRecord(rec.pagination);
  if (pagination) {
    stream.paginationEnabled = Boolean(pagination.enabled);
    stream.paginationType = toPaginationType(pagination.type);
    stream.paginationLocation = asString(pagination.location).toLowerCase() === "body" ? "BODY" : "QUERY";
    if (stream.paginationType === "PAGE_INCREMENT") {
      stream.pageParamName = asString(pagination.page_param, stream.pageParamName);
      stream.pageSizeParamName = asString(pagination.page_size_param, stream.pageSizeParamName);
      stream.pageSizeValue = String(toInt(pagination.page_size, toInt(stream.pageSizeValue, 100)));
      stream.firstPageNumber = pagination.first_page_number != null ? String(toInt(pagination.first_page_number, 0)) : "";
      stream.totalCountPath = asString(pagination.total_count_path);
      stream.totalCountStopRule =
        asString(pagination.total_count_stop_rule).toUpperCase() === "PAGE_WINDOW_GTE_TOTAL"
          ? "PAGE_WINDOW_GTE_TOTAL"
          : "FETCHED_RECORDS_GTE_TOTAL";
      stream.maxPages = pagination.max_pages != null ? String(toInt(pagination.max_pages, 0)) : "";
      const pageStop = asString(pagination.stop_condition).toUpperCase();
      // Legacy "MAX_PAGES" stop condition now hydrates as EMPTY_ARRAY. The saved maxPages value is
      // preserved separately (above), so this is a safe behavioural improvement: the page bound is kept
      // and empty-page detection becomes an additional, earlier stop — never a later one.
      stream.pageStopCondition =
        pageStop === "TOTAL_COUNT" ? "TOTAL_COUNT"
        : pageStop === "HAS_MORE" ? "HAS_MORE"
        : "EMPTY_ARRAY";
      stream.totalCountHeader = asString(pagination.total_count_header);
      stream.totalCountSource = stream.totalCountHeader ? "HEADER" : "BODY";
      stream.hasMorePath = asString(pagination.has_more_path);
      stream.hasMoreHeader = asString(pagination.has_more_header);
      stream.hasMoreSource = stream.hasMoreHeader ? "HEADER" : "BODY";
    } else if (stream.paginationType === "CURSOR") {
      stream.cursorParamName = asString(pagination.cursor_param, stream.cursorParamName);
      stream.cursorValuePath = asString(pagination.cursor_path, stream.cursorValuePath);
      stream.cursorStopPath = asString(pagination.cursor_stop_path, stream.cursorStopPath);
      stream.cursorHeader = asString(pagination.cursor_header);
      stream.cursorSource = stream.cursorHeader ? "HEADER" : "BODY";
      stream.cursorPageSizeParamName = asString(pagination.page_size_param, stream.cursorPageSizeParamName);
      stream.cursorPageSizeValue = String(toInt(pagination.page_size, toInt(stream.cursorPageSizeValue, 100)));
      stream.maxPages = pagination.max_pages != null ? String(toInt(pagination.max_pages, 0)) : "";
    } else if (stream.paginationType === "NEXT_URL") {
      stream.nextUrlPath = asString(pagination.next_url_path, stream.nextUrlPath);
      stream.nextUrlHeader = asString(pagination.next_url_header);
      stream.nextUrlSource = !stream.nextUrlHeader
        ? "BODY"
        : asString(pagination.next_url_header_format).toLowerCase() === "link"
        ? "LINK_HEADER"
        : "HEADER";
      stream.maxPages = pagination.max_pages != null ? String(toInt(pagination.max_pages, 0)) : "";
    } else if (stream.paginationType === "OFFSET_LIMIT") {
      stream.offsetParamName = asString(pagination.offset_param, stream.offsetParamName);
      stream.limitParamName = asString(pagination.limit_param, stream.limitParamName);
      stream.limitValue = String(toInt(pagination.page_size, toInt(stream.limitValue, 100)));
      stream.offsetTotalCountPath = asString(pagination.total_count_path);
      stream.offsetTotalCountStopRule =
        asString(pagination.offset_total_count_stop_rule).toUpperCase() === "OFFSET_PLUS_LIMIT_GTE_TOTAL"
          ? "OFFSET_PLUS_LIMIT_GTE_TOTAL"
          : "FETCHED_RECORDS_GTE_TOTAL";
      const offsetStop = asString(pagination.stop_condition).toUpperCase();
      stream.offsetStopCondition =
        offsetStop === "TOTAL_COUNT" ? "TOTAL_COUNT"
        : offsetStop === "HAS_MORE" ? "HAS_MORE"
        : "EMPTY_ARRAY";
      stream.offsetTotalCountHeader = asString(pagination.total_count_header);
      stream.offsetTotalCountSource = stream.offsetTotalCountHeader ? "HEADER" : "BODY";
      stream.hasMorePath = asString(pagination.has_more_path);
      stream.hasMoreHeader = asString(pagination.has_more_header);
      stream.hasMoreSource = stream.hasMoreHeader ? "HEADER" : "BODY";
      stream.maxPages = pagination.max_pages != null ? String(toInt(pagination.max_pages, 0)) : "";
    }
  }

  const extractionRules = (Array.isArray(rec.extraction_rules) ? rec.extraction_rules : [])
    .map((rule) => {
      const r = asRecord(rule);
      if (!r) return null;
      const attributeName = asString(r.attribute_name).trim();
      const pathExpression = asString(r.path).trim();
      if (!attributeName || !pathExpression) return null;
      return {
        ...createExtractionRule(),
        attributeName,
        pathExpression,
        defaultValue: asString(r.default_value),
      } as AttributeExtractionRule;
    })
    .filter((rule): rule is AttributeExtractionRule => Boolean(rule));
  stream.attributeExtractions = extractionRules.length > 0 ? extractionRules : [createExtractionRule()];

  const transformationRules = (Array.isArray(rec.transformation_rules) ? rec.transformation_rules : [])
    .map((rule) => {
      const r = asRecord(rule);
      if (!r) return null;
      const fieldName = asString(r.field_name).trim();
      if (!fieldName) return null;
      return {
        ...createTransformationRule(),
        action: toTransformationAction(r.type),
        fieldName,
        staticValue: asString(r.value),
        attributeName: asString(r.source_attribute),
      } as TransformationRule;
    })
    .filter((rule): rule is TransformationRule => Boolean(rule));
  stream.transformations = transformationRules.length > 0 ? transformationRules : [createTransformationRule()];

  const legacyRoutingRules = (Array.isArray(rec.routing_rules) ? rec.routing_rules : [])
    .map((rule) => {
      const r = asRecord(rule);
      if (!r) return null;
      const attributeName = asString(r.attribute).trim();
      if (!attributeName) return null;
      return {
        ...createRoutingRule(),
        attributeName,
        condition: toRoutingCondition(r.condition),
        value: asString(r.value),
        destination: toRoutingDestination(r.destination),
        targetStreamId: asString(r.target_stream_id || r.child_stream_id),
      } as RoutingRule;
    })
    .filter((rule): rule is RoutingRule => Boolean(rule));

  // New routing model fields deserialization
  const filterRoutingRules = (Array.isArray(rec.filters) ? rec.filters : []).map((f) => {
    const fr = asRecord(f);
    if (!fr) return null;
    return {
      ...createRoutingRule(),
      id: asString(fr.id) || makeId("filter"),
      attributeName: asString(fr.attribute),
      condition: toRoutingCondition(fr.condition),
      value: asString(fr.value),
      destination: asString(fr.action) === "exclude" ? "EXCLUDE" : "INCLUDE",
    } as RoutingRule;
  }).filter((f): f is RoutingRule => Boolean(f));

  const streamRoutes = (Array.isArray(rec.routes) ? rec.routes : []).map((r) => {
    const rr = asRecord(r);
    if (!rr) return null;
    const cond = asRecord(rr.condition);
    return {
      id: asString(rr.id) || makeId("route"),
      condition: {
        attribute: cond ? asString(cond.attribute) : "",
        condition: cond ? toRoutingCondition(cond.condition) : "equals",
        value: cond ? asString(cond.value) : "",
      },
      childStreamId: asString(rr.child_stream_id),
      ordinal: typeof rr.ordinal === "number" ? rr.ordinal : 0,
    } as Route;
  }).filter((r): r is Route => Boolean(r));

  const routeRoutingRules = streamRoutes.map((route) => ({
    ...createRoutingRule(),
    id: route.id,
    attributeName: route.condition.attribute,
    condition: route.condition.condition,
    value: route.condition.value,
    destination: "ROUTE_TO_STREAM" as RoutingDestination,
    targetStreamId: route.childStreamId,
  }));

  const routingRules = [...legacyRoutingRules, ...filterRoutingRules, ...routeRoutingRules];
  stream.routingRules = routingRules.length > 0 ? routingRules : [createRoutingRule()];

  stream.filters = (Array.isArray(rec.filters) ? rec.filters : []).map((f) => {
    const fr = asRecord(f);
    if (!fr) return null;
    return {
      id: asString(fr.id) || makeId("filter"),
      attribute: asString(fr.attribute),
      condition: toRoutingCondition(fr.condition),
      value: asString(fr.value),
      action: (asString(fr.action) === "exclude" ? "exclude" : "include") as "include" | "exclude",
    } as FilterRule;
  }).filter((f): f is FilterRule => Boolean(f));

  stream.routes = streamRoutes;

  stream.defaultRouteChildStreamId = asString(rec.default_route_child_stream_id);
  stream.defaultRouteTargetStreamId = stream.defaultRouteChildStreamId;
  stream.defaultRouteAction = normalizeDefaultRouteAction(
    asString(rec.default_route_action),
    Boolean(stream.defaultRouteChildStreamId),
  );
  stream.branchingMode =
    streamRoutes.length > 0 || stream.defaultRouteChildStreamId
      ? "conditional"
      : stream.parentStreamId
      ? "none"
      : "none";

  stream.routeSource = routeSourceRec ? {
    parentStreamId: asString(routeSourceRec.parent_stream_id),
    routeId: asString(routeSourceRec.route_id),
  } : null;

  const entityRec = asRecord(rec.entity);
  if (entityRec) {
    const ekRec = asRecord(entityRec.kafka);
    stream.entity = {
      kafka: {
        topic: ekRec ? asString(ekRec.topic) : "",
        partitions: ekRec ? toInt(ekRec.partitions, 6) : 6,
        replicationFactor: ekRec ? toInt(ekRec.replication_factor, 3) : 3,
        keyStrategy: ekRec ? normalizeKafkaKeyStrategy(asString(ekRec.key_strategy)) : "none",
        valueFormat: ekRec ? asString(ekRec.value_format, "avro") : "avro",
        schemaMode: ekRec ? asString(ekRec.schema_mode, "existing") : "existing",
        schemaArtifactId: ekRec ? asString(ekRec.schema_artifact_id) : "",
        schemaVersion: ekRec && ekRec.schema_version != null ? String(ekRec.schema_version) : "",
      },
      schemaArtifactId: asString(entityRec.schema_artifact_id),
      schemaVersion: entityRec.schema_version != null ? String(entityRec.schema_version) : "",
      iceberg: entityRec.iceberg != null ? parseEntityIcebergConfig(entityRec.iceberg) : null,
    };
  } else {
    stream.entity = null;
    // Entity config is set explicitly by the user and persisted from the Destination step.
  }

  const paramBindings = (Array.isArray(rec.parameter_bindings) ? rec.parameter_bindings : [])
    .map((rawBinding) => {
      const binding = asRecord(rawBinding);
      if (!binding) return null;
      const paramName = asString(binding.param_name).trim();
      const paramIn = asString(binding.param_in).trim().toLowerCase();
      if (!paramName || !["path", "query", "header", "body"].includes(paramIn)) return null;
      const valueSourceRaw = asString(binding.value_source).trim().toLowerCase();
      const valueSource: ParamValueSource =
        valueSourceRaw === "parent_field" ||
        valueSourceRaw === "static" ||
        valueSourceRaw === "global_var" ||
        valueSourceRaw === "manual_runtime"
          ? (valueSourceRaw as ParamValueSource)
          : "manual_runtime";
      const confidenceRaw = asString(binding.confidence).trim().toLowerCase();
      const confidence: "high" | "medium" | "low" =
        confidenceRaw === "high" || confidenceRaw === "medium" || confidenceRaw === "low"
          ? (confidenceRaw as "high" | "medium" | "low")
          : "low";
      return createParamBinding({
        id: asString(binding.id) || makeId("parambind"),
        paramName,
        paramIn: paramIn as "path" | "query" | "header" | "body",
        required: Boolean(binding.required),
        enabled: binding.enabled == null ? Boolean(binding.required) : Boolean(binding.enabled),
        valueSource,
        parentStreamId: asString(binding.parent_stream_id),
        parentField: asString(binding.parent_field),
        staticValue: asString(binding.static_value),
        globalVarName: asString(binding.global_var_name),
        runtimeVarName: asString(binding.runtime_var_name),
        confidence,
        description: asString(binding.description),
        schemaType: asString(binding.schema_type),
        defaultValue: asString(binding.default_value),
      });
    })
    .filter((binding): binding is StreamParamBinding => Boolean(binding));
  stream.openApiParamBindings = paramBindings;

  return stream;
};

const createExtractionRule = (): AttributeExtractionRule => ({
  id: makeId("extract"),
  attributeName: "",
  pathExpression: "",
  defaultValue: "",
});

const createTransformationRule = (): TransformationRule => ({
  id: makeId("transform"),
  action: "ADD_FIELD",
  fieldName: "",
  staticValue: "",
  attributeName: "",
});

const createRoutingRule = (): RoutingRule => ({
  id: makeId("route"),
  attributeName: "",
  condition: "equals",
  value: "",
  destination: "INCLUDE",
  targetStreamId: "",
});

const createParamBinding = (
  input: Partial<StreamParamBinding> & Pick<StreamParamBinding, "paramName" | "paramIn">,
): StreamParamBinding => ({
  id: input.id || makeId("parambind"),
  paramName: input.paramName,
  paramIn: input.paramIn,
  required: Boolean(input.required),
  enabled: input.enabled ?? Boolean(input.required),
  valueSource: input.valueSource || "manual_runtime",
  parentStreamId: input.parentStreamId || "",
  fanOutCompletionMode: input.fanOutCompletionMode || "per_record",
  parentField: input.parentField || "",
  staticValue: input.staticValue || "",
  globalVarName: input.globalVarName || "",
  runtimeVarName: input.runtimeVarName || "",
  confidence: input.confidence || "low",
  description: input.description || "",
  schemaType: input.schemaType || "",
  defaultValue: input.defaultValue || "",
});

const PAGINATION_TYPE_MAP: Record<string, string> = {
  NONE: "none",
  PAGE_INCREMENT: "page",
  CURSOR: "cursor",
  OFFSET_LIMIT: "offset",
  NEXT_URL: "next_url",
};

const buildPaginationCfg = (s: Stream, isPassiveStream: boolean): Record<string, unknown> => {
  const paginationType = isPassiveStream ? "none" : (PAGINATION_TYPE_MAP[s.paginationType] || "none");
  const cfg: Record<string, unknown> = {
    enabled: isPassiveStream ? false : s.paginationEnabled,
    type: paginationType,
  };
  if (isPassiveStream || paginationType === "none") return cfg;

  const num = (v: string) => (v ? Number.parseInt(v, 10) : null);
  cfg.max_pages = num(s.maxPages);
  cfg.location = s.paginationLocation === "BODY" ? "body" : "query";

  if (paginationType === "page") {
    cfg.page_param = s.pageParamName || null;
    cfg.page_size_param = s.pageSizeParamName || null;
    cfg.page_size = num(s.pageSizeValue);
    cfg.first_page_number = s.firstPageNumber !== "" ? Number.parseInt(s.firstPageNumber, 10) : null;
    cfg.stop_condition = s.pageStopCondition || null;
    if (s.pageStopCondition === "TOTAL_COUNT") {
      cfg.total_count_path = s.totalCountSource === "BODY" ? (s.totalCountPath || null) : null;
      cfg.total_count_header = s.totalCountSource === "HEADER" ? (s.totalCountHeader || null) : null;
    }
    if (s.pageStopCondition === "HAS_MORE") {
      cfg.has_more_path = s.hasMoreSource === "BODY" ? (s.hasMorePath || null) : null;
      cfg.has_more_header = s.hasMoreSource === "HEADER" ? (s.hasMoreHeader || null) : null;
    }
  } else if (paginationType === "cursor") {
    cfg.cursor_param = s.cursorParamName || null;
    cfg.cursor_path = s.cursorSource === "BODY" ? (s.cursorValuePath || null) : null;
    cfg.cursor_header = s.cursorSource === "HEADER" ? (s.cursorHeader || null) : null;
    cfg.page_size_param = s.cursorPageSizeParamName || null;
    cfg.page_size = num(s.cursorPageSizeValue);
  } else if (paginationType === "next_url") {
    cfg.next_url_path = s.nextUrlSource === "BODY" ? (s.nextUrlPath || null) : null;
    cfg.next_url_header = s.nextUrlSource === "BODY" ? null : (s.nextUrlHeader || null);
    cfg.next_url_header_format =
      s.nextUrlSource === "LINK_HEADER" ? "link" : s.nextUrlSource === "HEADER" ? "raw" : null;
  } else if (paginationType === "offset") {
    cfg.offset_param = s.offsetParamName || null;
    cfg.limit_param = s.limitParamName || null;
    cfg.page_size = num(s.limitValue);
    cfg.stop_condition = s.offsetStopCondition || null;
    if (s.offsetStopCondition === "TOTAL_COUNT") {
      cfg.total_count_path = s.offsetTotalCountSource === "BODY" ? (s.offsetTotalCountPath || null) : null;
      cfg.total_count_header = s.offsetTotalCountSource === "HEADER" ? (s.offsetTotalCountHeader || null) : null;
    }
    if (s.offsetStopCondition === "HAS_MORE") {
      cfg.has_more_path = s.hasMoreSource === "BODY" ? (s.hasMorePath || null) : null;
      cfg.has_more_header = s.hasMoreSource === "HEADER" ? (s.hasMoreHeader || null) : null;
    }
  }
  return cfg;
};

const createDefaultStream = (name: string): Stream => ({
  id: makeId("stream"),
  name,
  branchingMode: "none",
  method: "GET",
  endpointPath: "",
  requestEnabled: false,
  openApiOperationId: "",
  requestBodyTemplate: "",
  bodyFormat: "EMPTY",
  additionalHeaders: "",
  additionalQueryParams: "",
  responseDataPath: "",
  responseFormat: "JSON",
  splitResponseIntoRecords: true,
  waitForAllRecordsBeforeDownstream: false,
  xmlSplitDepth: "1",
  xmlRecordElement: "",
  csvDelimiter: ",",
  csvHasHeaderRow: true,
  csvQuoteChar: "\"",
  csvEscapeChar: "\\",
  mongoQuery: "{}",
  mongoProjection: "",
  mongoSort: "",
  mongoLimit: "",
  primaryKeyFields: "",
  isPrimary: false,
  attributeExtractions: [createExtractionRule()],
  paginationEnabled: false,
  paginationType: "NONE",
  pageParamName: "",
  pageSizeParamName: "",
  pageSizeValue: "",
  firstPageNumber: "",
  pageStopCondition: "EMPTY_ARRAY",
  totalCountPath: "",
  totalCountStopRule: "FETCHED_RECORDS_GTE_TOTAL",
  maxPages: "",
  cursorParamName: "",
  cursorValuePath: "",
  cursorStopPath: "",
  cursorPageSizeParamName: "",
  cursorPageSizeValue: "",
  nextUrlPath: "",
  offsetParamName: "",
  limitParamName: "",
  limitValue: "",
  offsetStopCondition: "EMPTY_ARRAY",
  offsetTotalCountPath: "",
  offsetTotalCountStopRule: "FETCHED_RECORDS_GTE_TOTAL",
  paginationLocation: "QUERY",
  cursorSource: "BODY",
  cursorHeader: "",
  nextUrlSource: "BODY",
  nextUrlHeader: "",
  totalCountSource: "BODY",
  totalCountHeader: "",
  offsetTotalCountSource: "BODY",
  offsetTotalCountHeader: "",
  hasMoreSource: "BODY",
  hasMorePath: "",
  hasMoreHeader: "",
  parentStreamId: "",
  fanOutCompletionMode: "per_record",
  parentField: "",
  injectAs: "QUERY_PARAM",
  injectFieldName: "",
  injectTemplate: "",
  pathParamDefaults: {},
  transformations: [createTransformationRule()],
  routingRules: [createRoutingRule()],
  defaultRouteAction: "DROP",
  defaultRouteTargetStreamId: "",
  openApiParamBindings: [],
  // New routing model fields
  filters: [],
  routes: [],
  defaultRouteChildStreamId: "",
  routeSource: null,
  entity: null,
});

const normalizeDesignerStream = (raw: Partial<Stream>, index: number): Stream => {
  const name = asString(raw.name).trim() || `stream_${index + 1}`;
  const base = createDefaultStream(name);
  const responseFormat =
    raw.responseFormat === "XML" || raw.responseFormat === "CSV" || raw.responseFormat === "JSON"
      ? raw.responseFormat
      : "JSON";
  const bodyFormat =
    raw.bodyFormat === "JSON" || raw.bodyFormat === "XML" || raw.bodyFormat === "CSV" || raw.bodyFormat === "EMPTY"
      ? raw.bodyFormat
      : "EMPTY";

  const routingRules = Array.isArray(raw.routingRules) && raw.routingRules.length > 0
    ? raw.routingRules.map((rule) => {
        const item = asRecord(rule) || {};
        const fallback = createRoutingRule();
        const destination = asString(item.destination, fallback.destination);
        const condition = asString(item.condition, fallback.condition);
        return {
          ...fallback,
          id: asString(item.id, fallback.id) || fallback.id,
          attributeName: asString(item.attributeName),
          condition: (
            ["equals", "contains", "starts_with", "ends_with", "regex", "is_empty"].includes(condition)
              ? condition
              : fallback.condition
          ) as RoutingCondition,
          value: asString(item.value),
          destination: (
            ["INCLUDE", "EXCLUDE", "ROUTE_TO_STREAM"].includes(destination)
              ? destination
              : fallback.destination
          ) as RoutingDestination,
          targetStreamId: asString(item.targetStreamId),
        };
      })
    : [createRoutingRule()];

  return {
    ...base,
    ...raw,
    id: asString(raw.id, base.id) || base.id,
    name,
    branchingMode:
      raw.branchingMode === "conditional" || raw.branchingMode === "parallel" || raw.branchingMode === "none"
        ? raw.branchingMode
        : base.branchingMode,
    method: ["GET", "POST", "PUT", "PATCH", "DELETE"].includes(asString(raw.method)) ? (raw.method as StreamMethod) : "GET",
    endpointPath: asString(raw.endpointPath),
    requestEnabled: typeof raw.requestEnabled === "boolean" ? raw.requestEnabled : false,
    openApiOperationId: asString(raw.openApiOperationId),
    requestBodyTemplate: asString(raw.requestBodyTemplate),
    bodyFormat,
    additionalHeaders: asString(raw.additionalHeaders),
    additionalQueryParams: asString(raw.additionalQueryParams),
    responseDataPath: asString(raw.responseDataPath),
    responseFormat,
    splitResponseIntoRecords:
      typeof raw.splitResponseIntoRecords === "boolean"
        ? raw.splitResponseIntoRecords
        : responseFormat === "XML" || responseFormat === "CSV" || Boolean(raw.responseDataPath),
    waitForAllRecordsBeforeDownstream: Boolean(raw.waitForAllRecordsBeforeDownstream),
    xmlSplitDepth: asString(raw.xmlSplitDepth, "1"),
    xmlRecordElement: asString(raw.xmlRecordElement),
    csvDelimiter: asString(raw.csvDelimiter, ",") || ",",
    csvHasHeaderRow: raw.csvHasHeaderRow == null ? true : Boolean(raw.csvHasHeaderRow),
    csvQuoteChar: asString(raw.csvQuoteChar, "\"") || "\"",
    csvEscapeChar: asString(raw.csvEscapeChar, "\\") || "\\",
    mongoQuery: asString(raw.mongoQuery, "{}") || "{}",
    mongoProjection: asString(raw.mongoProjection),
    mongoSort: asString(raw.mongoSort),
    mongoLimit: asString(raw.mongoLimit),
    primaryKeyFields: asString(raw.primaryKeyFields),
    isPrimary: Boolean(raw.isPrimary),
    attributeExtractions: Array.isArray(raw.attributeExtractions) ? raw.attributeExtractions : [createExtractionRule()],
    transformations: Array.isArray(raw.transformations) ? raw.transformations : [createTransformationRule()],
    routingRules,
    pathParamDefaults: (asRecord(raw.pathParamDefaults) as Record<string, string> | null) || {},
    openApiParamBindings: Array.isArray(raw.openApiParamBindings)
      ? raw.openApiParamBindings.map((binding) => {
          const item = asRecord(binding) || {};
          const paramIn = asString(item.paramIn);
          return createParamBinding({
            ...(item as Partial<StreamParamBinding>),
            paramName: asString(item.paramName),
            paramIn: ["path", "query", "header", "body"].includes(paramIn) ? (paramIn as StreamParamBinding["paramIn"]) : "query",
          });
        })
      : [],
    filters: Array.isArray(raw.filters) ? raw.filters : [],
    routes: Array.isArray(raw.routes) ? raw.routes : [],
    defaultRouteChildStreamId: asString(raw.defaultRouteChildStreamId),
    routeSource: raw.routeSource || null,
    entity: raw.entity || null,
  };
};

const tailorStreamForSmb = (stream: Stream): Stream => {
  const shouldChange =
    stream.method !== "GET" ||
    stream.endpointPath !== "" ||
    stream.requestBodyTemplate !== "" ||
    stream.bodyFormat !== "EMPTY" ||
    stream.additionalHeaders !== "" ||
    stream.additionalQueryParams !== "" ||
    stream.parentStreamId !== "" ||
    stream.parentField !== "" ||
    stream.injectFieldName !== "" ||
    stream.injectTemplate !== "" ||
    stream.paginationEnabled ||
    stream.paginationType !== "NONE" ||
    stream.responseFormat !== "JSON" ||
    stream.xmlSplitDepth !== "1" ||
    stream.xmlRecordElement !== "" ||
    stream.csvDelimiter !== "," ||
    stream.csvHasHeaderRow !== true ||
    stream.csvQuoteChar !== "\"" ||
    stream.csvEscapeChar !== "\\";

  if (!shouldChange) return stream;

  return {
    ...stream,
    method: "GET",
    endpointPath: "",
    requestBodyTemplate: "",
    bodyFormat: "EMPTY",
    additionalHeaders: "",
    additionalQueryParams: "",
    parentStreamId: "",
    parentField: "",
    injectFieldName: "",
    injectTemplate: "",
    paginationEnabled: false,
    paginationType: "NONE",
    responseFormat: "JSON",
    xmlSplitDepth: "1",
    xmlRecordElement: "",
    csvDelimiter: ",",
    csvHasHeaderRow: true,
    csvQuoteChar: "\"",
    csvEscapeChar: "\\",
  };
};

const tailorStreamForWebhook = (stream: Stream): Stream => {
  const shouldChange =
    stream.method !== "GET" ||
    stream.endpointPath !== "" ||
    stream.requestBodyTemplate !== "" ||
    stream.bodyFormat !== "EMPTY" ||
    stream.additionalHeaders !== "" ||
    stream.additionalQueryParams !== "" ||
    stream.parentStreamId !== "" ||
    stream.parentField !== "" ||
    stream.injectFieldName !== "" ||
    stream.injectTemplate !== "" ||
    stream.paginationEnabled ||
    stream.paginationType !== "NONE";

  if (!shouldChange) return stream;

  return {
    ...stream,
    method: "GET",
    endpointPath: "",
    requestBodyTemplate: "",
    bodyFormat: "EMPTY",
    additionalHeaders: "",
    additionalQueryParams: "",
    parentStreamId: "",
    parentField: "",
    injectFieldName: "",
    injectTemplate: "",
    paginationEnabled: false,
    paginationType: "NONE",
  };
};

const nextGeneratedStreamName = (streams: Stream[]): string => {
  const existing = new Set(streams.map((stream) => (stream.name || "").trim().toLowerCase()).filter(Boolean));
  let index = streams.length + 1;
  while (existing.has(`stream_${index}`)) index += 1;
  return `stream_${index}`;
};

const PLACEHOLDER_PATTERN = /\$\{([^}]+)\}/g;
const MONGO_PLACEHOLDER_PATTERN = /\$\{([^}]+)\}/g;
const MONGO_PLACEHOLDER_KEY_PATTERN = /(?:^|[,{]\s*)(?:"\s*\$\{[^}]+\}\s*"|\$\{[^}]+\})\s*:/m;
const MONGO_QUOTED_PLACEHOLDER_PATTERN = /"[^"\n\r]*\$\{[^}]+\}[^"\n\r]*"/m;

const extractTemplateVariables = (stream: Stream): Array<{ name: string; required: boolean }> => {
  const required = new Set<string>();
  const optional = new Set<string>();
  const extractFrom = (value: string, target: Set<string>) => {
    if (!value) return;
    PLACEHOLDER_PATTERN.lastIndex = 0;
    let match: RegExpExecArray | null;
    while ((match = PLACEHOLDER_PATTERN.exec(value)) !== null) {
      const variable = (match[1] ?? "").trim();
      if (variable) target.add(variable);
    }
  };

  // Path placeholders are always required.
  extractFrom(stream.endpointPath, required);

  // Body/header/query placeholders are optional at test time.
  const optionalTemplateValues: string[] = [stream.requestBodyTemplate, stream.injectTemplate];
  for (const value of optionalTemplateValues) extractFrom(value, optional);
  for (const [, value] of Object.entries(parseKeyValueLines(stream.additionalHeaders))) extractFrom(value, optional);
  for (const [, value] of Object.entries(parseKeyValueLines(stream.additionalQueryParams))) extractFrom(value, optional);

  for (const binding of stream.openApiParamBindings || []) {
    if (!binding.enabled && !binding.required) continue;
    if (binding.paramIn === "path") {
      const key = (binding.runtimeVarName || binding.paramName).trim();
      if (key) required.add(key);
      continue;
    }
    if (binding.paramIn === "query" && binding.enabled) {
      if (binding.staticValue.trim()) {
        extractFrom(binding.staticValue, optional);
      } else {
        const key = (binding.runtimeVarName || binding.paramName).trim();
        if (key) optional.add(key);
      }
    }
  }

  const ordered: Array<{ name: string; required: boolean }> = [];
  for (const name of required) ordered.push({ name, required: true });
  for (const name of optional) {
    if (!required.has(name)) ordered.push({ name, required: false });
  }
  return ordered;
};

const extractMongoTemplateVariables = (stream: Stream): string[] => {
  const values: string[] = [stream.mongoQuery, stream.mongoProjection, stream.mongoSort];
  const found = new Set<string>();
  for (const value of values) {
    if (!value) continue;
    MONGO_PLACEHOLDER_PATTERN.lastIndex = 0;
    let match: RegExpExecArray | null;
    while ((match = MONGO_PLACEHOLDER_PATTERN.exec(value)) !== null) {
      const variable = (match[1] ?? "").trim();
      if (variable) found.add(variable);
    }
  }
  return Array.from(found);
};

const validateMongoJsonTemplate = (
  fieldLabel: string,
  rawValue: string,
): string[] => {
  const text = rawValue.trim();
  if (!text) return [];

  const errors: string[] = [];
  if (MONGO_PLACEHOLDER_KEY_PATTERN.test(text)) {
    errors.push(
      `${fieldLabel}: placeholders cannot be used as JSON keys. Use a normal key with a placeholder value (example {"asset_id":\${asset_id}}).`,
    );
  }
  if (MONGO_QUOTED_PLACEHOLDER_PATTERN.test(text)) {
    errors.push(
      `${fieldLabel}: placeholders must be unquoted raw values (use {"asset_id":\${asset_id}}, not {"asset_id":"\${asset_id}"}).`,
    );
  }

  const normalized = text.replace(MONGO_PLACEHOLDER_PATTERN, "0");
  try {
    const parsed = JSON.parse(normalized);
    if (!parsed || Array.isArray(parsed) || typeof parsed !== "object") {
      errors.push(`${fieldLabel}: template must resolve to a JSON object.`);
    }
  } catch {
    errors.push(`${fieldLabel}: template is not valid JSON.`);
  }

  return errors;
};

const initialPrimaryStream = (() => {
  const stream = createDefaultStream("");
  return {
    ...stream,
    isPrimary: true,
  };
})();

const initialStreams: Stream[] = [initialPrimaryStream];

const FlowDesigner = () => {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [searchParams] = useSearchParams();
  const editFlowId = (searchParams.get("editFlowId") || "").trim();
  const forceNewFlow = searchParams.get("new") === "1";
  const [step, setStep] = useState(0);
  const [sourceType, setSourceType] = useState<SourceType>("rest");
  const [editingFlow, setEditingFlow] = useState<ApiFlow | null>(null);
  const [editingSourceId, setEditingSourceId] = useState<string | null>(null);
  const [isSaving, setIsSaving] = useState(false);
  const [restConnectionMode, setRestConnectionMode] = useState<SourceConnectionMode>("manual");
  const [restApplicationServiceId, setRestApplicationServiceId] = useState("");
  const [smbConnectionMode, setSmbConnectionMode] = useState<SourceConnectionMode>("manual");
  const [smbApplicationServiceId, setSmbApplicationServiceId] = useState("");
  const [webhookConnectionMode, setWebhookConnectionMode] = useState<SourceConnectionMode>("manual");
  const [webhookApplicationServiceId, setWebhookApplicationServiceId] = useState("");

  const [restSource, setRestSource] = useState<RestSourceConfig>({
    sourceName: "",
    baseUrl: "",
    authType: "NONE",
    apiKeyName: "",
    apiKeyValue: "",
    apiKeyLocation: "HEADER",
    bearerToken: "",
    username: "",
    password: "",
    connectionTimeout: "",
    socketReadTimeout: "",
    socketWriteTimeout: "",
    socketIdleTimeout: "",
    rateLimitPerMin: "",
    openApiSpecId: "",
    openApiFileName: "",
    openApiTitle: "",
    openApiVersion: "",
    openApiFormat: "",
    openApiWarnings: [],
    openApiServers: [],
    openApiSelectedServerUrl: "",
    openApiOperationsCount: 0,
  });
  const [openApiUploadStatus, setOpenApiUploadStatus] = useState<"idle" | "uploading" | "error">("idle");
  const [openApiUploadError, setOpenApiUploadError] = useState("");
  const [openApiOperationSearch, setOpenApiOperationSearch] = useState("");
  const debouncedOpenApiOperationSearch = useDebouncedValue(openApiOperationSearch, 250);

  const [postgresSource, setPostgresSource] = useState<DatabaseSourceConfig>({
    sourceName: "",
    connectionMode: "basic",
    nifiGlobalServiceId: "",
    host: "",
    port: "",
    databaseName: "",
    username: "",
    password: "",
    sslMode: "require",
    connectionTimeout: "",
    connectionUri: "",
  });

  const [mongoSource, setMongoSource] = useState<DatabaseSourceConfig>({
    sourceName: "",
    connectionMode: "basic",
    nifiGlobalServiceId: "",
    host: "",
    port: "",
    databaseName: "",
    username: "",
    password: "",
    sslMode: "disable",
    connectionTimeout: "",
    connectionUri: "",
  });

  const [smbSource, setSmbSource] = useState<SmbSourceConfig>({
    sourceName: "",
    smbConnection: "",
    shareName: "",
    username: "",
    password: "",
    domain: "",
    baseDirectoryPath: "",
    fileFilterPattern: "",
    fileFormat: "CSV",
    csvDelimiter: ",",
    hasHeaderRow: true,
  });

  const [webhookSource, setWebhookSource] = useState<WebhookSourceConfig>({
    sourceName: "",
    endpointId: "",
    enabled: true,
    contentType: "auto",
    signatureHeader: "X-Signature",
    signatureSecret: "",
    signatureAlgo: "hmac-sha256",
  });
  const [trinoSource, setTrinoSource] = useState<TrinoSourceConfig>({
    sourceName: "",
    endpoint: "",
    user: "admin",
    catalog: "",
    schema: "",
    table: "",
    checkpointCatalog: "bronze",
    checkpointSchema: "checkpoints",
    checkpointTable: "trino_snapshot_checkpoints",
    autoCreateCheckpointTable: true,
    columnMode: "all",
    columns: "",
  });

  const [streams, setStreams] = useState<Stream[]>(initialStreams);
  const [editingStream, setEditingStream] = useState<string>(initialStreams[0].id);
  const [flowMapOpen, setFlowMapOpen] = useState(true);
  const streamGraph = useMemo(() => buildStreamGraph(streams), [streams]);
  const entityStreams = useMemo(() => getEntityStreams(streams), [streams]);
  const entityStreamIdsKey = useMemo(() => entityStreams.map((stream) => stream.id).join("|"), [entityStreams]);
  const primaryStream = useMemo(() => streams.find((stream) => stream.isPrimary) || streams[0], [streams]);

  // Stream testing state: map of streamId → test result
  type StreamTestState = {
    status: "idle" | "running" | "success" | "error";
    result?: {
      ok: boolean;
      status_code?: number;
      url?: string;
      error?: string;
      response_data?: unknown;
      response_data_type?: "json" | "text" | null;
      response_data_truncated?: boolean;
      response_headers?: Record<string, string>;
    };
  };
  const [streamTestStates, setStreamTestStates] = useState<Record<string, StreamTestState>>({});
  const [pendingStreamTest, setPendingStreamTest] = useState<PendingStreamTest | null>(null);
  const [selectedResponseNodeByStream, setSelectedResponseNodeByStream] = useState<Record<string, string>>({});
  const [selectedRecordResponseNodeByStream, setSelectedRecordResponseNodeByStream] = useState<Record<string, string>>({});

  // Schema Inference state
  const [inferenceJobId, setInferenceJobId] = useState<string | null>(null);
  const [inferenceJob, setInferenceJob] = useState<InferenceJob | null>(null);
  const [inferenceJobsByEntity, setInferenceJobsByEntity] = useState<Record<string, InferenceJob>>({});
  const [inferenceStartingByEntity, setInferenceStartingByEntity] = useState<Record<string, boolean>>({});
  const [inferenceStarting, setInferenceStarting] = useState(false);
  const [pendingInferenceStart, setPendingInferenceStart] = useState<PendingInferenceStart | null>(null);
  const [inferenceRuntimeValues, setInferenceRuntimeValues] = useState<Record<string, string>>({});
  const [savedFlowId, setSavedFlowId] = useState<string | null>(null);

  const [kafkaOutput, setKafkaOutput] = useState<KafkaOutputConfig>({
    topic: "",
    partitions: "",
    replicationFactor: "",
    keyStrategy: "none",
    valueFormat: "avro",
    schemaMode: "auto_generate",
    existingSchemaArtifactId: "",
    existingSchemaVersion: "",
  });
  const [entityDestinations, setEntityDestinations] = useState<Record<string, EntityDestinationDraft>>({});
  const { data: availableSchemaArtifacts = [], refetch: refetchSchemas } = useQuery<ApiSchemaArtifact[]>({
    queryKey: ["schemas"],
    queryFn: () => schemasApi.list(),
  });
  const { data: serviceConnections = [] } = useQuery<ConnectionRecord[]>({
    queryKey: ["connections"],
    queryFn: () => api.get<ConnectionRecord[]>("/api/connections/"),
  });
  const { data: nifiGlobalServicesPage } = useQuery({
    queryKey: ["nifi-global-services"],
    queryFn: () => nifiServicesApi.list(),
  });
  const { data: applicationServices = [] } = useQuery<ApplicationService[]>({
    queryKey: ["application-services"],
    queryFn: () => applicationServicesApi.list(),
  });
  const restApplicationServices = useMemo(
    () => applicationServices.filter((service) => service.service_type === "REST API"),
    [applicationServices],
  );
  const smbApplicationServices = useMemo(
    () => applicationServices.filter((service) => service.service_type === "SMB"),
    [applicationServices],
  );
  const webhookApplicationServices = useMemo(
    () => applicationServices.filter((service) => service.service_type === "Webhook"),
    [applicationServices],
  );
  const mongoGlobalServices: NifiGlobalService[] = useMemo(
    () => (nifiGlobalServicesPage?.items || []).filter((service) => service.service_kind === "mongodb"),
    [nifiGlobalServicesPage],
  );
  const configuredRuntimeServices = useMemo(
    () => new Set(serviceConnections.map((connection) => connection.type)),
    [serviceConnections],
  );
  const missingInferenceServices = useMemo(
    () => RUNTIME_SERVICE_TYPES.filter((type) => !configuredRuntimeServices.has(type)),
    [configuredRuntimeServices],
  );
  const inferenceBlockReason = missingInferenceServices.length
    ? `Schema inference requires NiFi, Kafka, and Apicurio platform connections. Missing: ${missingInferenceServices
        .map((type) => RUNTIME_SERVICE_LABELS[type])
        .join(", ")}. Configure them in Settings > Platform Connections.`
    : null;
  const openApiSpecId = restSource.openApiSpecId.trim();
  const { data: openApiOperationsPage, isFetching: openApiOperationsLoading } = useQuery({
    queryKey: ["openapi-operations", openApiSpecId, debouncedOpenApiOperationSearch],
    queryFn: () =>
      openApiApi.listOperations(openApiSpecId, {
        search: debouncedOpenApiOperationSearch,
        page: 1,
        pageSize: 200,
      }),
    enabled: sourceType === "rest" && openApiSpecId.length > 0,
  });
  const [schedule, setSchedule] = useState<SourceSchedule>({
    type: "interval",
    interval: {
      value: "",
      unit: "minutes",
    },
    cron: {
      expression: "",
    },
  });

  const applyDesignerPayload = (
    payload: Partial<FlowDesignerPayload>,
    sourceConnection?: SourceConnectionRecord | null,
  ) => {
    if (typeof payload.step === "number") {
      setStep(Math.max(0, Math.min(steps.length - 1, Math.trunc(payload.step))));
    }

    if (
      typeof payload.sourceType === "string" &&
      ["rest", "postgres", "mongo", "smb", "webhook", "trino"].includes(payload.sourceType)
    ) {
      setSourceType(payload.sourceType as SourceType);
    }

    if (payload.restSource && typeof payload.restSource === "object") {
      const restored = payload.restSource as Partial<RestSourceConfig>;
      setRestSource((prev) => ({
        ...prev,
        ...restored,
        openApiWarnings: Array.isArray(restored.openApiWarnings)
          ? restored.openApiWarnings.filter((item): item is string => typeof item === "string")
          : prev.openApiWarnings,
        openApiServers: Array.isArray(restored.openApiServers)
          ? restored.openApiServers.filter(
              (item): item is OpenApiServer =>
                Boolean(item && typeof item === "object" && typeof item.url === "string"),
            )
          : prev.openApiServers,
        openApiOperationsCount:
          typeof restored.openApiOperationsCount === "number"
            ? restored.openApiOperationsCount
            : prev.openApiOperationsCount,
      }));
    }
    if (payload.postgresSource && typeof payload.postgresSource === "object") {
      setPostgresSource((prev) => ({ ...prev, ...(payload.postgresSource as Partial<DatabaseSourceConfig>) }));
    }
    if (payload.mongoSource && typeof payload.mongoSource === "object") {
      setMongoSource((prev) => ({ ...prev, ...(payload.mongoSource as Partial<DatabaseSourceConfig>) }));
    }
    if (payload.smbSource && typeof payload.smbSource === "object") {
      setSmbSource((prev) => ({ ...prev, ...(payload.smbSource as Partial<SmbSourceConfig>) }));
    }
    if (payload.webhookSource && typeof payload.webhookSource === "object") {
      setWebhookSource((prev) => ({ ...prev, ...(payload.webhookSource as Partial<WebhookSourceConfig>) }));
    }
    if (payload.trinoSource && typeof payload.trinoSource === "object") {
      const restored = payload.trinoSource as Partial<TrinoSourceConfig> & { checkpointTable?: string };
      const legacyCheckpoint = restored.checkpointTable ? splitTrinoTableName(restored.checkpointTable) : null;
      setTrinoSource((prev) => ({
        ...prev,
        ...restored,
        checkpointCatalog: restored.checkpointCatalog || legacyCheckpoint?.catalog || prev.checkpointCatalog,
        checkpointSchema: restored.checkpointSchema || legacyCheckpoint?.schema || prev.checkpointSchema,
        checkpointTable: legacyCheckpoint?.table || restored.checkpointTable || prev.checkpointTable,
        autoCreateCheckpointTable:
          typeof restored.autoCreateCheckpointTable === "boolean"
            ? restored.autoCreateCheckpointTable
            : prev.autoCreateCheckpointTable,
        columnMode: restored.columnMode === "manual" ? "manual" : "all",
      }));
    }
    if (payload.kafkaOutput && typeof payload.kafkaOutput === "object") {
      const restoredKafka = payload.kafkaOutput as Partial<KafkaOutputConfig>;
      setKafkaOutput((prev) => ({
        ...prev,
        ...restoredKafka,
        keyStrategy: normalizeKafkaKeyStrategy(asString(restoredKafka.keyStrategy, prev.keyStrategy)),
        valueFormat: normalizeKafkaValueFormat(asString(restoredKafka.valueFormat, prev.valueFormat)),
        schemaMode: restoredKafka.schemaMode === "existing" ? "existing" : "auto_generate",
        existingSchemaArtifactId:
          typeof restoredKafka.existingSchemaArtifactId === "string"
            ? restoredKafka.existingSchemaArtifactId
            : "",
        existingSchemaVersion:
          typeof restoredKafka.existingSchemaVersion === "string" ? restoredKafka.existingSchemaVersion : "",
      }));
    }
    if (payload.entityDestinations && typeof payload.entityDestinations === "object") {
      const restored: Record<string, EntityDestinationDraft> = {};
      Object.entries(payload.entityDestinations).forEach(([streamId, raw]) => {
        const item = asRecord(raw);
        if (!item) return;
        restored[streamId] = {
          partitions: asString(item.partitions, "6"),
          replicationFactor: asString(item.replicationFactor, "3"),
          keyStrategy: normalizeKafkaKeyStrategy(asString(item.keyStrategy, "none")),
          valueFormat: "avro",
          schemaMode: asString(item.schemaMode) === "existing" ? "existing" : "auto_generate",
          existingSchemaArtifactId: asString(item.existingSchemaArtifactId),
          existingSchemaVersion: asString(item.existingSchemaVersion),
          iceberg: parseEntityIcebergConfig(item.iceberg),
        };
      });
      setEntityDestinations(restored);
    }

    if (payload.schedule && typeof payload.schedule === "object") {
      const scheduleDraft = payload.schedule as Partial<SourceSchedule>;
      const type = scheduleDraft.type === "cron" ? "cron" : "interval";
      const intervalValue = toOptionalPositiveInt(scheduleDraft.interval?.value);
      const intervalUnit =
        scheduleDraft.interval?.unit === "seconds" ||
        scheduleDraft.interval?.unit === "minutes" ||
        scheduleDraft.interval?.unit === "hours" ||
        scheduleDraft.interval?.unit === "days"
          ? scheduleDraft.interval.unit
          : "minutes";
      const cronExpression =
        typeof scheduleDraft.cron?.expression === "string" ? scheduleDraft.cron.expression : "";

      setSchedule({
        type,
        interval: {
          value: intervalValue,
          unit: intervalUnit,
        },
        cron: {
          expression: cronExpression,
        },
      });
    }

    if (Array.isArray(payload.streams) && payload.streams.length > 0) {
      let restoredStreams = payload.streams.map((stream, index) =>
        normalizeDesignerStream((asRecord(stream) || {}) as Partial<Stream>, index),
      );
      const restoredGraph = buildStreamGraph(restoredStreams);
      restoredStreams = restoredStreams.map((stream) => ({
        ...stream,
        branchingMode: getBranchingMode(stream, restoredGraph),
      }));
      if (!restoredStreams.some((stream) => Boolean(stream.entity))) {
        const graph = buildStreamGraph(restoredStreams);
        restoredStreams = restoredStreams.map((stream) =>
          (graph.childrenByParent.get(stream.id) || []).length === 0
            ? { ...stream, entity: createEmptyEntityConfig() }
            : stream,
        );
      }
      setStreams(restoredStreams);
      if (
        typeof payload.editingStream === "string" &&
        restoredStreams.some((stream) => stream.id === payload.editingStream)
      ) {
        setEditingStream(payload.editingStream);
      } else {
        setEditingStream(restoredStreams[0].id);
      }
    }

    const connectionState = mergeSourceConnectionState(
      {
        restConnectionMode,
        restApplicationServiceId,
        smbConnectionMode,
        smbApplicationServiceId,
        webhookConnectionMode,
        webhookApplicationServiceId,
      },
      sourceConnection,
      payload,
    );
    setRestConnectionMode(connectionState.restConnectionMode);
    setRestApplicationServiceId(connectionState.restApplicationServiceId);
    setSmbConnectionMode(connectionState.smbConnectionMode);
    setSmbApplicationServiceId(connectionState.smbApplicationServiceId);
    setWebhookConnectionMode(connectionState.webhookConnectionMode);
    setWebhookApplicationServiceId(connectionState.webhookApplicationServiceId);
  };

  const buildDesignerPayload = (savedStep: number): FlowDesignerPayload => ({
    step: savedStep,
    sourceType,
    restSource,
    postgresSource,
    mongoSource,
    smbSource,
    webhookSource,
    trinoSource,
    restConnectionMode,
    restApplicationServiceId,
    smbConnectionMode,
    smbApplicationServiceId,
    webhookConnectionMode,
    webhookApplicationServiceId,
    streams,
    editingStream,
    kafkaOutput,
    entityDestinations,
    schedule,
    savedAt: new Date().toISOString(),
  });

  useEffect(() => {
    if (typeof window === "undefined") return;
    // Editing an existing flow must hydrate from backend data, not local draft.
    if (editFlowId) return;
    // Explicit "new flow" navigation should start clean and skip local draft hydration.
    if (forceNewFlow) return;

    try {
      const rawDraft = localStorage.getItem(FLOW_DESIGNER_DRAFT_KEY);
      if (!rawDraft) return;

      const draft = JSON.parse(rawDraft) as Partial<FlowDesignerPayload>;
      applyDesignerPayload(draft);
    } catch {
      // Ignore malformed local draft payloads.
    }
  }, [editFlowId, forceNewFlow]);

  useEffect(() => {
    if (!editFlowId) {
      setEditingFlow(null);
      setEditingSourceId(null);
      return;
    }

    let cancelled = false;
    (async () => {
      try {
        const flow = await flowsApi.get(editFlowId);
        if (cancelled) return;

        setEditingFlow(flow);
        setEditingSourceId(flow.source_id);
        setSavedFlowId(flow.id);

        // Restore schema inference job if one was started for this flow
        if (flow.schema_inference_job_id) {
          try {
            const existingJob = await inferenceApi.getJobForFlow(flow.id);
            if (existingJob) {
              setInferenceJobId(existingJob.id);
              setInferenceJob(existingJob);
            }
          } catch {
            // ignore — inference may have been cleaned up
          }
        }

        // Load the linked source for designer payload + fallback fields
        let source: ApiSource | null = null;
        try {
          source = await sourcesApi.get(flow.source_id);
        } catch {
          // ignore — fallback to flow-only data
        }
        if (cancelled) return;

        const sourceSchedule = asRecord(source?.schedule);
        const sourceKafkaOutput = asRecord(source?.kafka_output);

        const hasCompleteDesignerPayload = isCompleteDesignerPayload(source?.designer_payload);
        if (hasCompleteDesignerPayload) {
          const restoredPayload = source.designer_payload as Partial<FlowDesignerPayload>;
          const linkedSchemaArtifactId = flow.schema_artifact_id || asString(sourceKafkaOutput?.schema_artifact_id);
          const linkedSchemaVersion =
            flow.schema_version != null
              ? String(flow.schema_version)
              : sourceKafkaOutput?.schema_version != null
              ? String(toInt(sourceKafkaOutput.schema_version, 1))
              : "";

          const sourceConnectionType = sourceLabelToType(source?.type || flow.source_type);
          applyDesignerPayload(
            {
              ...restoredPayload,
              kafkaOutput: {
                ...(restoredPayload.kafkaOutput ?? {}),
                ...(linkedSchemaArtifactId
                  ? {
                      schemaMode: "existing",
                      existingSchemaArtifactId: linkedSchemaArtifactId,
                      existingSchemaVersion: linkedSchemaVersion,
                    }
                  : {}),
              } as KafkaOutputConfig,
            },
            {
              sourceType: sourceConnectionType,
              connection_setup_mode: source?.connection_setup_mode,
              application_service_id: source?.application_service_id,
            },
          );
        }

        // Fallback: reconstruct designer state from persisted source + flow fields
        const fallbackSourceType = sourceLabelToType(flow.source_type);
        setSourceType(fallbackSourceType);
        if (!hasCompleteDesignerPayload) {
          setStep(STEP_CONFIGURE_SOURCE);
        }

        const scheduleType = asString(sourceSchedule?.type).toLowerCase() === "cron" ? "cron" : "interval";
        const intervalUnitRaw = asString(sourceSchedule?.interval_unit).toLowerCase();
        const intervalUnit: IntervalUnit =
          intervalUnitRaw === "seconds" || intervalUnitRaw === "minutes" || intervalUnitRaw === "hours" || intervalUnitRaw === "days"
            ? (intervalUnitRaw as IntervalUnit)
            : "minutes";
        setSchedule({
          type: scheduleType,
          interval: {
            value: Math.max(1, toInt(sourceSchedule?.interval_value, 15)),
            unit: intervalUnit,
          },
          cron: {
            expression: asString(sourceSchedule?.cron_expression),
          },
        });

        setKafkaOutput((prev) => {
          const fallbackPartitions = toInt(prev.partitions, 6);
          const fallbackReplication = toInt(prev.replicationFactor, 3);
          const schemaModeRaw = asString(sourceKafkaOutput?.schema_mode).toLowerCase();
          return {
            ...prev,
            topic: flow.kafka_topic || asString(sourceKafkaOutput?.topic, prev.topic),
            partitions: String(toInt(sourceKafkaOutput?.partitions, fallbackPartitions)),
            replicationFactor: String(toInt(sourceKafkaOutput?.replication_factor, fallbackReplication)),
            keyStrategy: normalizeKafkaKeyStrategy(asString(sourceKafkaOutput?.key_strategy, prev.keyStrategy)),
            valueFormat: normalizeKafkaValueFormat(asString(sourceKafkaOutput?.value_format, prev.valueFormat)),
            schemaMode: flow.schema_artifact_id ? "existing" : schemaModeRaw === "existing" ? "existing" : "auto_generate",
            existingSchemaArtifactId: flow.schema_artifact_id || asString(sourceKafkaOutput?.schema_artifact_id),
            existingSchemaVersion:
              flow.schema_version != null
                ? String(flow.schema_version)
                : sourceKafkaOutput?.schema_version != null
                ? String(toInt(sourceKafkaOutput.schema_version, 1))
                : "",
          };
        });

        if (fallbackSourceType === "rest") {
          const sourceConnectionMode = asString((source as Record<string, unknown> | null)?.connection_setup_mode, "manual");
          setRestConnectionMode(sourceConnectionMode === "application_service" ? "application_service" : "manual");
          setRestApplicationServiceId(asString((source as Record<string, unknown> | null)?.application_service_id));
          setRestSource((prev) => ({
            ...prev,
            sourceName: source?.name || flow.name,
            baseUrl: asString(source?.base_url, prev.baseUrl),
            authType: toRestAuthType(source?.auth_type),
            apiKeyName: asString(source?.api_key_name, prev.apiKeyName),
            apiKeyValue: asString(source?.api_key_value, prev.apiKeyValue),
            apiKeyLocation: toApiKeyLocation(source?.api_key_location),
            bearerToken: asString(source?.bearer_token, prev.bearerToken),
            username: asString(source?.rest_username, prev.username),
            password: asString(source?.rest_password, prev.password),
            connectionTimeout: String(toInt(source?.connection_timeout, toInt(prev.connectionTimeout, 30))),
            socketReadTimeout: String(toInt(source?.read_timeout, toInt(prev.socketReadTimeout, 30))),
            socketWriteTimeout: String(toInt(source?.write_timeout, toInt(prev.socketWriteTimeout, 30))),
            socketIdleTimeout: String(toInt(source?.idle_timeout, toInt(prev.socketIdleTimeout, 30))),
            rateLimitPerMin: String(toInt(source?.rate_limit, toInt(prev.rateLimitPerMin, 120))),
            openApiSpecId: asString((source as Record<string, unknown> | null)?.openapi_spec_id, prev.openApiSpecId),
            openApiSelectedServerUrl: asString(
              (source as Record<string, unknown> | null)?.openapi_selected_server_url,
              prev.openApiSelectedServerUrl,
            ),
          }));
        } else if (fallbackSourceType === "postgres") {
          setPostgresSource((prev) => ({
            ...prev,
            sourceName: source?.name || flow.name,
            host: asString(source?.db_host, prev.host),
            port: String(toInt(source?.db_port, toInt(prev.port, 5432))),
            databaseName: asString(source?.db_database, prev.databaseName),
            username: asString(source?.db_username, prev.username),
            password: asString(source?.db_password, prev.password),
            sslMode: asString(source?.db_ssl_mode, prev.sslMode),
            connectionTimeout: String(toInt(source?.db_connection_timeout, toInt(prev.connectionTimeout, 30))),
            connectionUri: asString(source?.db_connection_uri, prev.connectionUri),
            query: asString(source?.db_query, prev.query || "{}"),
            projection: asString(source?.db_projection, prev.projection),
            sort: asString(source?.db_sort, prev.sort),
            limit: source?.db_limit != null ? String(toInt(source.db_limit, toInt(prev.limit, 0))) : prev.limit,
          }));
        } else if (fallbackSourceType === "mongo") {
          const sourceRefs = ((source as Record<string, unknown> | null)?.nifi_service_refs || {}) as Record<string, unknown>;
          const sourceMongoMode = asString(source?.mongo_connection_mode, "basic");
          const normalizedMongoMode: MongoConnectionMode =
            sourceMongoMode === "nifi_global_service"
              ? "nifi_global_service"
              : sourceMongoMode === "uri" || asString(source?.db_connection_uri).trim()
              ? "uri"
              : "basic";
          setMongoSource((prev) => ({
            ...prev,
            sourceName: source?.name || flow.name,
            connectionMode: normalizedMongoMode,
            nifiGlobalServiceId: asString(sourceRefs.mongodb, prev.nifiGlobalServiceId),
            host: asString(source?.db_host, prev.host),
            port: String(toInt(source?.db_port, toInt(prev.port, 27017))),
            databaseName: asString(source?.db_database, prev.databaseName),
            username: asString(source?.db_username, prev.username),
            password: asString(source?.db_password, prev.password),
            sslMode: asString(source?.db_ssl_mode, prev.sslMode),
            connectionTimeout: String(toInt(source?.db_connection_timeout, toInt(prev.connectionTimeout, 30))),
            connectionUri: asString(source?.db_connection_uri, prev.connectionUri),
          }));
        } else if (fallbackSourceType === "webhook") {
          const sourceConnectionMode = asString((source as Record<string, unknown> | null)?.connection_setup_mode, "manual");
          setWebhookConnectionMode(sourceConnectionMode === "application_service" ? "application_service" : "manual");
          setWebhookApplicationServiceId(asString((source as Record<string, unknown> | null)?.application_service_id));
          setWebhookSource((prev) => ({
            ...prev,
            sourceName: source?.name || flow.name,
            endpointId: asString((source as Record<string, unknown> | null)?.webhook_endpoint_id, prev.endpointId),
            enabled:
              (source as Record<string, unknown> | null)?.webhook_enabled == null
                ? prev.enabled
                : Boolean((source as Record<string, unknown>).webhook_enabled),
            contentType: (
              ["auto", "json", "xml", "text"].includes(
                asString((source as Record<string, unknown> | null)?.webhook_content_type).toLowerCase(),
              )
                ? asString((source as Record<string, unknown> | null)?.webhook_content_type).toLowerCase()
                : prev.contentType
            ) as WebhookSourceConfig["contentType"],
            signatureHeader: asString(
              (source as Record<string, unknown> | null)?.webhook_signature_header,
              prev.signatureHeader,
            ),
            signatureSecret: asString(
              (source as Record<string, unknown> | null)?.webhook_secret,
              prev.signatureSecret,
            ),
            signatureAlgo: "hmac-sha256",
          }));
        } else if (fallbackSourceType === "trino") {
          const sourceRecord = (source as Record<string, unknown> | null) || {};
          const rawColumns = sourceRecord.trino_columns;
          const legacyCheckpoint = splitTrinoTableName(asString(sourceRecord.trino_checkpoint_table));
          setTrinoSource((prev) => ({
            ...prev,
            sourceName: source?.name || flow.name,
            endpoint: asString(sourceRecord.trino_endpoint, prev.endpoint),
            user: asString(sourceRecord.trino_user, prev.user || "admin"),
            catalog: asString(sourceRecord.trino_catalog, prev.catalog),
            schema: asString(sourceRecord.trino_schema, prev.schema),
            table: asString(sourceRecord.trino_table, prev.table),
            checkpointCatalog: asString(sourceRecord.trino_checkpoint_catalog, legacyCheckpoint?.catalog || prev.checkpointCatalog),
            checkpointSchema: asString(sourceRecord.trino_checkpoint_schema, legacyCheckpoint?.schema || prev.checkpointSchema),
            checkpointTable: asString(sourceRecord.trino_checkpoint_table_name, legacyCheckpoint?.table || prev.checkpointTable),
            autoCreateCheckpointTable:
              typeof sourceRecord.trino_auto_create_checkpoint_table === "boolean"
                ? sourceRecord.trino_auto_create_checkpoint_table
                : prev.autoCreateCheckpointTable,
            columnMode: asString(sourceRecord.trino_column_mode, prev.columnMode) === "manual" ? "manual" : "all",
            columns: Array.isArray(rawColumns)
              ? rawColumns.map((item) => asString(item)).filter(Boolean).join(", ")
              : asString(rawColumns, prev.columns),
          }));
        } else {
          const sourceConnectionMode = asString((source as Record<string, unknown> | null)?.connection_setup_mode, "manual");
          setSmbConnectionMode(sourceConnectionMode === "application_service" ? "application_service" : "manual");
          setSmbApplicationServiceId(asString((source as Record<string, unknown> | null)?.application_service_id));
          setSmbSource((prev) => ({
            ...prev,
            sourceName: source?.name || flow.name,
            smbConnection: source?.smb_hostname || prev.smbConnection,
            username: source?.smb_username || prev.username,
            domain: source?.smb_domain || prev.domain,
            password: source?.smb_password || prev.password,
            shareName: source?.smb_share || prev.shareName,
            baseDirectoryPath: source?.smb_base_directory || prev.baseDirectoryPath,
            fileFilterPattern: source?.smb_file_filter || prev.fileFilterPattern,
            fileFormat: (source?.smb_file_format as SmbSourceConfig["fileFormat"]) || prev.fileFormat,
            csvDelimiter: asString(source?.smb_csv_delimiter, prev.csvDelimiter),
            hasHeaderRow: source?.smb_has_header_row == null ? prev.hasHeaderRow : Boolean(source.smb_has_header_row),
          }));
        }

        const hydratedStreams = (Array.isArray(source?.streams) ? source.streams : [])
          .map((item, index) => mapApiStreamToDesignerStream(item, index))
          .filter((item): item is Stream => Boolean(item));
        const hydratedGraph = buildStreamGraph(hydratedStreams);
        hydratedStreams.forEach((stream) => {
          stream.branchingMode = getBranchingMode(stream, hydratedGraph);
        });
        const restoredEntityJobs = await Promise.all(
          hydratedStreams
            .filter((stream) => Boolean(stream.entity))
            .map(async (stream) => {
              try {
                const job = await inferenceApi.getJobForFlow(flow.id, stream.id);
                return job ? ([stream.id, job] as const) : null;
              } catch {
                return null;
              }
            }),
        );
        if (cancelled) return;
        const entityJobEntries = restoredEntityJobs.filter(
          (entry): entry is readonly [string, InferenceJob] => Boolean(entry),
        );
        if (entityJobEntries.length > 0) {
          setInferenceJobsByEntity((prev) => ({
            ...prev,
            ...Object.fromEntries(entityJobEntries),
          }));
        }

        if (fallbackSourceType === "mongo" && hydratedStreams.length > 0) {
          const legacyQuery = asString(source?.db_query);
          const legacyProjection = asString(source?.db_projection);
          const legacySort = asString(source?.db_sort);
          const legacyLimit = source?.db_limit != null ? String(toInt(source.db_limit, 0)) : "";
          hydratedStreams.forEach((stream) => {
            if (!stream.mongoQuery.trim()) stream.mongoQuery = legacyQuery || "{}";
            if (!stream.mongoProjection.trim()) stream.mongoProjection = legacyProjection;
            if (!stream.mongoSort.trim()) stream.mongoSort = legacySort;
            if (!stream.mongoLimit.trim()) stream.mongoLimit = legacyLimit;
          });
        }

        if (hydratedStreams.length > 0) {
          if (flow.primary_stream_id && hydratedStreams.some((stream) => stream.id === flow.primary_stream_id)) {
            hydratedStreams.forEach((stream) => {
              stream.isPrimary = stream.id === flow.primary_stream_id;
            });
          } else if (!hydratedStreams.some((stream) => stream.isPrimary) && !hydratedStreams.some((stream) => stream.entity !== null)) {
            hydratedStreams[0].isPrimary = true;
          }
          setStreams(hydratedStreams);
          const preferred =
            (flow.primary_stream_id && hydratedStreams.find((stream) => stream.id === flow.primary_stream_id)) ||
            hydratedStreams.find((stream) => stream.isPrimary) ||
            hydratedStreams[0];
          setEditingStream(preferred.id);
        } else {
          const fallbackPrimary = createDefaultStream("stream");
          fallbackPrimary.isPrimary = true;
          setStreams([fallbackPrimary]);
          setEditingStream(fallbackPrimary.id);
        }
      } catch (err) {
        if (cancelled) return;
        toast.error(`Flow not found: ${(err as Error).message}`);
        navigate("/flows");
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [editFlowId, navigate]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    // React Query handles schema list refetch; refetch on focus to keep in sync with Schemas page
    const refresh = () => refetchSchemas();
    window.addEventListener("focus", refresh);
    return () => {
      window.removeEventListener("focus", refresh);
    };
  }, [refetchSchemas]);

  // Poll inference job status while active
  useEffect(() => {
    const TERMINAL_STATES: InferenceStatus[] = ["complete", "failed", "stopped"];
    if (!inferenceJobId) return;
    if (inferenceJob && TERMINAL_STATES.includes(inferenceJob.status)) return;

    const id = setInterval(async () => {
      try {
        const job = await inferenceApi.getStatus(inferenceJobId);
        setInferenceJob(job);
        if (TERMINAL_STATES.includes(job.status)) {
          clearInterval(id);
          if (job.status === "complete") {
            toast.success(`Schema inference complete! ${job.messages_collected} messages sampled.`);
            // Reload schemas list so the new artifact appears
            refetchSchemas();
          } else if (job.status === "failed") {
            toast.error(`Schema inference failed: ${job.error || "Unknown error"}`);
          }
        }
      } catch {
        // ignore transient errors
      }
    }, 5000);

    return () => clearInterval(id);
  }, [inferenceJobId, inferenceJob?.status, refetchSchemas]);

  useEffect(() => {
    const TERMINAL_STATES: InferenceStatus[] = ["complete", "failed", "stopped"];
    const activeEntries = Object.entries(inferenceJobsByEntity).filter(
      ([, job]) => job && !TERMINAL_STATES.includes(job.status),
    );
    if (activeEntries.length === 0) return;

    const id = setInterval(async () => {
      await Promise.all(
        activeEntries.map(async ([streamId, job]) => {
          try {
            const latest = await inferenceApi.getStatus(job.id);
            setInferenceJobsByEntity((prev) => ({ ...prev, [streamId]: latest }));
            if (latest.status === "complete") {
              refetchSchemas();
            }
          } catch {
            // ignore transient status refresh failures
          }
        }),
      );
    }, 5000);

    return () => clearInterval(id);
  }, [inferenceJobsByEntity, refetchSchemas]);

  const compileRestStreamFromOpenApiBindings = (stream: Stream) => {
    const bindings = stream.openApiParamBindings || [];
    const endpointPath = stream.endpointPath || "";
    const queryParams = parseKeyValueLines(stream.additionalQueryParams);
    const firstPathBinding = bindings.find((binding) => binding.paramIn === "path" && binding.enabled);
    const inferredParentStreamId = stream.parentStreamId || firstPathBinding?.parentStreamId || "";
    const parentStreamForBinding = inferredParentStreamId
      ? streams.find((candidate) => candidate.id === inferredParentStreamId)
      : null;
    const parentAttrs = new Map(
      [
        ...(parentStreamForBinding?.attributeExtractions || []).map((rule) => rule.attributeName),
        ...((parentStreamForBinding?.primaryKeyFields || "").split(",")),
      ]
        .map((value) => (value || "").trim())
        .filter(Boolean)
        .map((value) => [toAttributeName(value).toLowerCase(), value] as const),
    );
    const endpointPlaceholders: string[] = [];
    PLACEHOLDER_PATTERN.lastIndex = 0;
    let placeholderMatch: RegExpExecArray | null;
    while ((placeholderMatch = PLACEHOLDER_PATTERN.exec(endpointPath)) !== null) {
      const variable = (placeholderMatch[1] ?? "").trim();
      if (variable && !endpointPlaceholders.includes(variable)) endpointPlaceholders.push(variable);
    }
    const parentMatchedPlaceholder = endpointPlaceholders.find((variable) =>
      parentAttrs.has(toAttributeName(variable).toLowerCase()),
    );
    const inferredInjectField = stream.injectFieldName || parentMatchedPlaceholder || firstPathBinding?.paramName || "";
    const inferredParentField =
      stream.parentField ||
      (parentMatchedPlaceholder ? parentAttrs.get(toAttributeName(parentMatchedPlaceholder).toLowerCase()) || "" : "") ||
      (firstPathBinding?.parentStreamId === inferredParentStreamId ? firstPathBinding?.parentField || "" : "") ||
      firstPathBinding?.paramName ||
      "";
    const inferredInjectAs: InjectAs = inferredInjectField ? "PATH_PARAM" : stream.injectAs;

    for (const binding of bindings) {
      if (binding.paramIn === "query") {
        if (!binding.enabled) {
          delete queryParams[binding.paramName];
          continue;
        }
        const value = binding.staticValue.trim();
        if (value) {
          queryParams[binding.paramName] = value;
        } else {
          delete queryParams[binding.paramName];
        }
      }
    }

    const parameterBindingsPayload = bindings.map((binding) => ({
      id: binding.id,
      param_name: binding.paramName,
      param_in: binding.paramIn,
      required: binding.required,
      enabled: binding.enabled,
      value_source: binding.paramIn === "query" ? "static" : binding.valueSource,
      parent_stream_id:
        binding.paramIn === "query"
          ? null
          : binding.valueSource === "parent_field"
          ? inferredParentStreamId || binding.parentStreamId || null
          : binding.parentStreamId || null,
      parent_field:
        binding.paramIn === "query"
          ? null
          : binding.valueSource === "parent_field"
          ? inferredParentField || binding.parentField || null
          : binding.parentField || null,
      static_value: binding.staticValue || null,
      global_var_name: binding.paramIn === "query" ? null : binding.globalVarName || null,
      runtime_var_name: binding.paramIn === "query" ? null : binding.runtimeVarName || null,
      confidence: binding.confidence,
      description: binding.description || null,
      schema_type: binding.schemaType || null,
      default_value: binding.defaultValue || null,
    }));

    return {
      endpointPath,
      queryParams,
      inferredParentStreamId,
      inferredParentField,
      inferredInjectField,
      inferredInjectAs,
      parameterBindingsPayload,
    };
  };

  const buildRuntimeRoutingForStream = (stream: Stream) => {
    const configuredRules = (stream.routingRules || []).filter(
      (rule) => isConfiguredRoutingRule(rule) && (rule.attributeName || "").trim() && routeRuleHasComparisonValue(rule),
    );
    const filterRules = configuredRules.filter((rule) => rule.destination === "INCLUDE" || rule.destination === "EXCLUDE");
    const routeRules = configuredRules.filter((rule) => rule.destination === "ROUTE_TO_STREAM" && rule.targetStreamId);
    const incomingRoute = (streamGraph.incomingByChild.get(stream.id) || []).find(
      (edge) => edge.kind === "route" || edge.kind === "default_route",
    );
    const entitySummary = entityDestinationSummaries.find((item) => item.stream.id === stream.id);

    return {
      legacyRoutingRules:
        sourceType === "webhook"
          ? filterRules.map((rule) => ({
              attribute: rule.attributeName,
              condition: rule.condition,
              value: rule.value,
              destination: rule.destination.toLowerCase(),
              topic_name: null,
            }))
          : [],
      filters: filterRules.map((rule) => ({
        id: rule.id,
        attribute: rule.attributeName || "",
        condition: rule.condition,
        value: rule.value || "",
        action: rule.destination === "EXCLUDE" ? "exclude" : "include",
      })),
      routes: routeRules.map((rule, ordinal) => ({
        id: rule.id,
        condition: {
          attribute: rule.attributeName || "",
          condition: rule.condition,
          value: rule.value || "",
        },
        child_stream_id: rule.targetStreamId || "",
        ordinal,
      })),
      defaultRouteChildStreamId:
        stream.defaultRouteAction === "ROUTE_TO_STREAM" ? stream.defaultRouteTargetStreamId || null : null,
      defaultRouteAction:
        routeRules.length > 0
          ? stream.defaultRouteAction.toLowerCase()
          : null,
      routeSource: incomingRoute
        ? {
            parent_stream_id: incomingRoute.parentId,
            route_id: incomingRoute.routeId || "default",
          }
        : null,
      entity: entitySummary
        ? {
            kafka: {
              topic: entitySummary.topic,
              partitions: Number.parseInt(entitySummary.draft.partitions, 10) || 6,
              replication_factor: Number.parseInt(entitySummary.draft.replicationFactor, 10) || 3,
              key_strategy: entitySummary.draft.keyStrategy,
              value_format: "avro",
              schema_mode: entitySummary.draft.schemaMode,
              schema_artifact_id: entitySummary.schemaArtifactId,
              schema_version: entitySummary.effectiveVersion?.version ?? null,
            },
            schema_artifact_id: entitySummary.schemaArtifactId,
            schema_version: entitySummary.effectiveVersion?.version ?? null,
            iceberg: entitySummary.draft.iceberg,
          }
        : null,
    };
  };

  const persistDraftToBackend = async (): Promise<string | undefined> => {
    const flowName = sourceName.trim() || "untitled-flow";
    const selectedPrimaryStream = streams.find((stream) => stream.isPrimary) || streams[0];
    const primaryStreamName = selectedPrimaryStream?.name?.trim() || "stream";
    const primaryStreamId = selectedPrimaryStream?.id || "";
    const apiType = sourceTypeToApiType(sourceType);
    const designerPayload = buildDesignerPayload(step) as unknown as Record<string, unknown>;

    const apiStreams = streams.map((s) => {
      const isPassiveStream = sourceType === "smb" || sourceType === "webhook" || sourceType === "trino";
      const supportsResponseFormatForStream = sourceType === "rest" || sourceType === "webhook";
      const compiledRest = sourceType === "rest" ? compileRestStreamFromOpenApiBindings(s) : null;
      const isRouteChildStream = Boolean(
        (streamGraph.incomingByChild.get(s.id) || []).some(
          (edge) => edge.kind === "route" || edge.kind === "default_route",
        ),
      );
      const paginationCfg = buildPaginationCfg(s, isPassiveStream);
      const runtimeRouting = buildRuntimeRoutingForStream(s);
      return {
        id: s.id,
        name: s.name,
        endpoint_path:
          sourceType === "mongo"
            ? s.endpointPath
            : sourceType === "rest"
            ? compiledRest?.endpointPath || s.endpointPath
            : "",
        request_enabled: sourceType === "rest" ? (!isRouteChildStream || s.requestEnabled) : false,
        method: sourceType === "rest" ? s.method : "GET",
        openapi_operation_id: sourceType === "rest" ? (s.openApiOperationId || null) : null,
        response_data_path: s.responseDataPath || null,
        response_format: supportsResponseFormatForStream ? (s.responseFormat || "JSON").toLowerCase() : "json",
        mongo_query: sourceType === "mongo" ? (s.mongoQuery || "{}") : null,
        mongo_projection: sourceType === "mongo" ? (s.mongoProjection || null) : null,
        mongo_sort: sourceType === "mongo" ? (s.mongoSort || null) : null,
        mongo_limit: sourceType === "mongo" ? (s.mongoLimit ? Number.parseInt(s.mongoLimit, 10) || null : null) : null,
        split_response_records: Boolean(s.splitResponseIntoRecords),
        wait_for_all_records_before_downstream: sourceType === "rest" ? Boolean(s.waitForAllRecordsBeforeDownstream) : false,
        xml_split_depth: supportsResponseFormatForStream && s.responseFormat === "XML" ? (parseInt(s.xmlSplitDepth, 10) || 1) : null,
        xml_record_element: supportsResponseFormatForStream && s.responseFormat === "XML" ? (s.xmlRecordElement || null) : null,
        csv_delimiter: supportsResponseFormatForStream && s.responseFormat === "CSV" ? (s.csvDelimiter || ",") : null,
        csv_has_header_row: supportsResponseFormatForStream && s.responseFormat === "CSV" ? Boolean(s.csvHasHeaderRow) : null,
        csv_quote_char: supportsResponseFormatForStream && s.responseFormat === "CSV" ? (s.csvQuoteChar || "\"") : null,
        csv_escape_char: supportsResponseFormatForStream && s.responseFormat === "CSV" ? (s.csvEscapeChar || "\\") : null,
        csv_encoding: supportsResponseFormatForStream && s.responseFormat === "CSV" ? "utf-8" : null,
        csv_test_record_limit: null,
        primary_key_fields: s.primaryKeyFields ? s.primaryKeyFields.split(",").map((x) => x.trim()).filter(Boolean) : [],
        is_primary: s.isPrimary,
        headers: sourceType === "rest" ? parseKeyValueLines(s.additionalHeaders) : {},
        query_params: sourceType === "rest" ? compiledRest?.queryParams || parseKeyValueLines(s.additionalQueryParams) : {},
        body_format: sourceType === "rest" ? (s.bodyFormat || "EMPTY").toLowerCase() : "empty",
        body_template: sourceType === "rest" ? (s.requestBodyTemplate || null) : null,
        path_param_defaults: sourceType === "rest" ? (s.pathParamDefaults || {}) : {},
        pagination: paginationCfg,
        fan_out: {
          enabled:
            sourceType === "rest"
              ? Boolean(compiledRest?.inferredParentStreamId || s.parentStreamId)
              : sourceType === "mongo"
              ? Boolean(s.parentStreamId)
              : false,
          parent_stream_id:
            sourceType === "rest"
              ? (compiledRest?.inferredParentStreamId || s.parentStreamId || null)
              : sourceType === "mongo"
              ? (s.parentStreamId || null)
              : null,
          completion_mode:
            sourceType === "rest" || sourceType === "mongo"
              ? "per_record"
              : "per_record",
          parent_field:
            sourceType === "rest"
              ? (compiledRest?.inferredParentField || s.parentField || null)
              : sourceType === "mongo"
              ? (s.parentField || s.injectFieldName || null)
              : null,
          inject_as:
            sourceType === "rest"
              ? ((compiledRest?.inferredInjectField || s.injectFieldName)
                  ? (compiledRest?.inferredInjectAs || s.injectAs || "PATH_PARAM").toLowerCase()
                  : null)
              : null,
          inject_field_name:
            sourceType === "rest"
              ? (compiledRest?.inferredInjectField || s.injectFieldName || null)
              : null,
          inject_template: sourceType === "rest" ? (s.injectTemplate || null) : null,
        },
        parameter_bindings: sourceType === "rest" ? compiledRest?.parameterBindingsPayload || [] : [],
        extraction_rules: (s.attributeExtractions || []).filter((r) => r.attributeName && r.pathExpression).map((r) => ({
          attribute_name: r.attributeName,
          path: r.pathExpression,
          default_value: r.defaultValue || null,
        })),
        transformation_rules: (s.transformations || []).filter((r) => r.fieldName).map((r) => ({
          type: r.action === "ADD_FIELD" ? "add_field" : r.action === "REMOVE_FIELD" ? "remove_field" : "set_from_attribute",
          field_name: r.fieldName,
          value: r.staticValue || null,
          source_attribute: r.attributeName || null,
        })),
        routing_rules: runtimeRouting.legacyRoutingRules,
        filters: runtimeRouting.filters,
        routes: runtimeRouting.routes,
        default_route_child_stream_id: runtimeRouting.defaultRouteChildStreamId,
        default_route_action: runtimeRouting.defaultRouteAction,
        route_source: runtimeRouting.routeSource,
        entity: runtimeRouting.entity,
      };
    });

    const sourcePayload: Record<string, unknown> = {
      name: flowName,
      type: apiType,
      designer_payload: designerPayload,
      streams: apiStreams,
      schedule: {
        type: schedule.type,
        interval_value: schedule.interval.value === "" ? 15 : schedule.interval.value,
        interval_unit: schedule.interval.unit,
        cron_expression: schedule.cron.expression,
      },
      kafka_output: {
        topic: entityDestinationSummaries[0]?.topic || derivedKafkaTopic,
        partitions: Number.parseInt(kafkaOutput.partitions, 10) || 6,
        replication_factor: Number.parseInt(kafkaOutput.replicationFactor, 10) || 3,
        key_strategy: normalizeKafkaKeyStrategy(kafkaOutput.keyStrategy),
        value_format: "avro",
        schema_mode: kafkaOutput.schemaMode || "existing",
        schema_artifact_id: entityDestinationSummaries[0]?.schemaArtifactId || (kafkaOutput.schemaMode === "existing" ? derivedSchemaArtifactId : null),
        schema_version: entityDestinationSummaries[0]?.effectiveVersion?.version || (kafkaOutput.existingSchemaVersion ? Number.parseInt(kafkaOutput.existingSchemaVersion, 10) || null : null),
      },
    };
    if (sourceType === "rest") {
      Object.assign(sourcePayload, {
        connection_setup_mode: restConnectionMode,
        application_service_id: restConnectionMode === "application_service" ? restApplicationServiceId : null,
        openapi_spec_id: restSource.openApiSpecId || null,
        openapi_selected_server_url: restSource.openApiSelectedServerUrl || null,
        ...(restConnectionMode === "manual"
          ? {
              base_url: restSource.baseUrl,
              auth_type: restSource.authType,
              api_key_name: restSource.apiKeyName || null,
              api_key_value: restSource.apiKeyValue || null,
              api_key_location: restSource.apiKeyLocation,
              bearer_token: restSource.bearerToken || null,
              rest_username: restSource.username || null,
              rest_password: restSource.password || null,
              connection_timeout: Number.parseInt(restSource.connectionTimeout, 10) || 30,
              read_timeout: Number.parseInt(restSource.socketReadTimeout, 10) || 30,
              write_timeout: Number.parseInt(restSource.socketWriteTimeout, 10) || 30,
              idle_timeout: Number.parseInt(restSource.socketIdleTimeout, 10) || 30,
              rate_limit: Number.parseInt(restSource.rateLimitPerMin, 10) || 120,
            }
          : {}),
      });
    } else if (sourceType === "postgres" || sourceType === "mongo") {
      const dbCfg = sourceType === "postgres" ? postgresSource : mongoSource;
      const mongoPrimary = sourceType === "mongo" ? (streams.find((stream) => stream.isPrimary) || streams[0]) : null;
      Object.assign(sourcePayload, {
        db_host: sourceType === "mongo" && dbCfg.connectionMode !== "basic" ? null : dbCfg.host,
        db_port:
          sourceType === "mongo" && dbCfg.connectionMode !== "basic"
            ? null
            : Number.parseInt(dbCfg.port, 10) || (sourceType === "postgres" ? 5432 : 27017),
        db_database: dbCfg.databaseName,
        db_username: dbCfg.username,
        db_password: dbCfg.password || null,
        db_ssl_mode: dbCfg.sslMode,
        db_connection_timeout: Number.parseInt(dbCfg.connectionTimeout, 10) || 30,
      });
      if (sourceType === "mongo") {
        Object.assign(sourcePayload, {
          mongo_connection_mode: dbCfg.connectionMode || "basic",
          db_connection_uri: dbCfg.connectionMode === "uri" ? dbCfg.connectionUri || null : null,
          nifi_service_refs: dbCfg.connectionMode === "nifi_global_service" ? { mongodb: dbCfg.nifiGlobalServiceId } : {},
          // Legacy fallback fields retained for compatibility with older flow readers.
          db_query: mongoPrimary?.mongoQuery || "{}",
          db_projection: mongoPrimary?.mongoProjection || null,
          db_sort: mongoPrimary?.mongoSort || null,
          db_limit: mongoPrimary?.mongoLimit ? Number.parseInt(mongoPrimary.mongoLimit, 10) || null : null,
        });
      }
    } else if (sourceType === "smb") {
      Object.assign(sourcePayload, {
        connection_setup_mode: smbConnectionMode,
        application_service_id: smbConnectionMode === "application_service" ? smbApplicationServiceId : null,
        ...(smbConnectionMode === "manual"
          ? {
              smb_hostname: smbSource.smbConnection,
              smb_username: smbSource.username || null,
              smb_domain: smbSource.domain || null,
              smb_password: smbSource.password || null,
              smb_share: smbSource.shareName || null,
            }
          : {}),
        smb_base_directory: smbSource.baseDirectoryPath,
        smb_file_filter: smbSource.fileFilterPattern,
        smb_file_format: smbSource.fileFormat,
        smb_csv_delimiter: smbSource.fileFormat === "CSV" ? smbSource.csvDelimiter || "," : null,
        smb_has_header_row: smbSource.fileFormat === "CSV" ? smbSource.hasHeaderRow : null,
        smb_inject_org_from_filename: false,
        smb_org_filename_delimiter: null,
      });
    } else if (sourceType === "webhook") {
      Object.assign(sourcePayload, {
        connection_setup_mode: webhookConnectionMode,
        application_service_id: webhookConnectionMode === "application_service" ? webhookApplicationServiceId : null,
        webhook_endpoint_id: webhookSource.endpointId.trim().toLowerCase(),
        ...(webhookConnectionMode === "manual"
          ? {
              webhook_secret: webhookSource.signatureSecret || null,
              webhook_signature_header: webhookSource.signatureHeader || "X-Signature",
              webhook_signature_algo: "hmac-sha256",
            }
          : {}),
        webhook_enabled: webhookSource.enabled,
        webhook_content_type: webhookSource.contentType,
      });
    } else if (sourceType === "trino") {
      const checkpointFullName = [
        trinoSource.checkpointCatalog.trim(),
        trinoSource.checkpointSchema.trim(),
        trinoSource.checkpointTable.trim(),
      ].filter(Boolean).join(".");
      Object.assign(sourcePayload, {
        trino_endpoint: trinoSource.endpoint.trim(),
        trino_user: trinoSource.user.trim() || "admin",
        trino_catalog: trinoSource.catalog.trim(),
        trino_schema: trinoSource.schema.trim(),
        trino_table: trinoSource.table.trim(),
        trino_checkpoint_table: checkpointFullName,
        trino_checkpoint_catalog: trinoSource.checkpointCatalog.trim(),
        trino_checkpoint_schema: trinoSource.checkpointSchema.trim(),
        trino_checkpoint_table_name: trinoSource.checkpointTable.trim(),
        trino_auto_create_checkpoint_table: trinoSource.autoCreateCheckpointTable,
        trino_column_mode: trinoSource.columnMode,
        trino_columns: trinoSource.columnMode === "manual" ? parseTrinoColumns(trinoSource.columns) : [],
        trino_bootstrap_mode: "latest_snapshot",
        trino_output_mode: "one_message_per_row",
        trino_include_snapshot_metadata: true,
      });
    }

    let savedSource: ApiSource;
    if (editingSourceId) savedSource = await sourcesApi.update(editingSourceId, sourcePayload as never);
    else {
      savedSource = await sourcesApi.create(sourcePayload as never);
      setEditingSourceId(savedSource.id);
    }

    const draftSchemaArtifactId = entityDestinationSummaries[0]?.schemaArtifactId || null;
    const draftSchemaVersion = entityDestinationSummaries[0]?.effectiveVersion?.version ?? null;
    const draftHasCompleteSchemaLink = Boolean(draftSchemaArtifactId && draftSchemaVersion);
    const flowPayload = {
      name: flowName,
      source_id: savedSource.id,
      source_type: apiType,
      primary_stream_id: primaryStreamId,
      kafka_topic: entityDestinationSummaries[0]?.topic || derivedKafkaTopic,
      enabled: editingFlow?.enabled ?? true,
      schema_artifact_id: draftHasCompleteSchemaLink ? draftSchemaArtifactId : null,
      schema_version: draftHasCompleteSchemaLink ? draftSchemaVersion : null,
    };
    let savedFlow: ApiFlow;
    if (editingFlow) savedFlow = await flowsApi.update(editingFlow.id, flowPayload);
    else {
      savedFlow = await flowsApi.create(flowPayload);
      setEditingFlow(savedFlow);
      navigate(`/flow-designer?editFlowId=${encodeURIComponent(savedFlow.id)}`, { replace: true });
    }
    savedFlow = await flowsApi.patch(savedFlow.id, { state: "Draft" });
    setEditingFlow(savedFlow);
    setSavedFlowId(savedFlow.id);
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ["flows"] }),
      queryClient.invalidateQueries({ queryKey: ["dashboard"] }),
    ]);
    return savedFlow.id;
  };

  const handleSaveDraft = async () => {
    if (typeof window !== "undefined") {
      const payload = buildDesignerPayload(step);
      localStorage.setItem(FLOW_DESIGNER_DRAFT_KEY, JSON.stringify(payload));
    }
    try {
      await persistDraftToBackend();
      toast.success("Flow draft saved");
    } catch (err) {
      toast.error(`Failed to save draft: ${(err as Error).message}`);
    }
  };

  const handleGoToSchemaVerification = async () => {
    try {
      await persistDraftToBackend();
      toast.success("Draft saved");
      navigate("/schemas");
    } catch (err) {
      toast.error(`Failed to save draft before navigation: ${(err as Error).message}`);
    }
  };

  const sourceTypeMeta = sourceTypes.find((s) => s.id === sourceType)!;
  const isRestSource = sourceType === "rest";
  const isMongoSource = sourceType === "mongo";
  const isSmbSource = sourceType === "smb";
  const isWebhookSource = sourceType === "webhook";
  const supportsResponseFormat = isRestSource || isWebhookSource;

  useEffect(() => {
    const specId = restSource.openApiSpecId.trim();
    if (!specId) return;
    let cancelled = false;
    (async () => {
      try {
        const spec = await openApiApi.getSpec(specId);
        if (cancelled) return;
        setRestSource((prev) => {
          if (prev.openApiSpecId.trim() !== specId) return prev;
          return {
            ...prev,
            openApiTitle: spec.title || prev.openApiTitle,
            openApiVersion: spec.version || prev.openApiVersion,
            openApiFormat: spec.format || prev.openApiFormat,
            openApiWarnings: Array.isArray(spec.warnings) ? spec.warnings : [],
            openApiServers: Array.isArray(spec.servers) ? spec.servers : [],
            openApiOperationsCount: typeof spec.operations_count === "number" ? spec.operations_count : 0,
            openApiSelectedServerUrl:
              prev.openApiSelectedServerUrl ||
              (Array.isArray(spec.servers) && spec.servers.length > 0 ? spec.servers[0].url : ""),
          };
        });
      } catch {
        // Ignore stale/missing spec metadata failures in hydration mode.
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [restSource.openApiSpecId]);

  useEffect(() => {
    if (!isSmbSource) return;
    setStreams((prev) => {
      const next = prev.map((stream) => tailorStreamForSmb(stream));
      const changed = next.some((stream, index) => stream !== prev[index]);
      return changed ? next : prev;
    });
  }, [isSmbSource]);

  useEffect(() => {
    if (!isWebhookSource) return;
    setStreams((prev) => {
      const base = prev.length > 0 ? [prev[0]] : [createDefaultStream("events")];
      const normalized = base.map((stream, index) => ({
        ...tailorStreamForWebhook(stream),
        isPrimary: index === 0,
      }));
      const changed =
        prev.length !== normalized.length ||
        normalized.some((stream, index) => stream !== prev[index]);
      return changed ? normalized : prev;
    });
  }, [isWebhookSource]);

  const editing = streams.find((s) => s.id === editingStream) ?? streams[0];
  const selectedOpenApiOperationId = editing.openApiOperationId || "";
  const { data: selectedOpenApiOperation, isFetching: selectedOpenApiOperationLoading } = useQuery({
    queryKey: ["openapi-operation-detail", openApiSpecId, selectedOpenApiOperationId],
    queryFn: () => openApiApi.getOperation(openApiSpecId, selectedOpenApiOperationId),
    enabled: isRestSource && openApiSpecId.length > 0 && selectedOpenApiOperationId.length > 0,
  });
  const editingTestState = streamTestStates[editing.id];
  const editingHeaderEntries = useMemo(
    () => Object.entries(parseKeyValueLines(editing.additionalHeaders)),
    [editing.additionalHeaders],
  );
  const editingQueryEntries = useMemo(
    () => Object.entries(parseKeyValueLines(editing.additionalQueryParams)),
    [editing.additionalQueryParams],
  );
  const editingTemplateVariables = useMemo(
    () => (isRestSource ? extractTemplateVariables(editing) : []),
    [editing, isRestSource],
  );
  const editingPathVariables = useMemo(() => {
    if (!isRestSource) return [] as string[];
    const path = editing.endpointPath || "";
    const found: string[] = [];
    const seen = new Set<string>();
    PLACEHOLDER_PATTERN.lastIndex = 0;
    let match: RegExpExecArray | null;
    while ((match = PLACEHOLDER_PATTERN.exec(path)) !== null) {
      const variable = (match[1] ?? "").trim();
      if (!variable || seen.has(variable)) continue;
      seen.add(variable);
      found.push(variable);
    }
    return found;
  }, [editing.endpointPath, isRestSource]);
  const editingRouteParentEdge = (streamGraph.incomingByChild.get(editing.id) || []).find(
    (edge) => edge.kind === "route" || edge.kind === "default_route",
  );
  const editingRouteParent = editingRouteParentEdge ? streams.find((stream) => stream.id === editingRouteParentEdge.parentId) : null;
  const editingIsRouteChild = Boolean(editingRouteParent);
  const editingMakesRequest = !isRestSource || !editingIsRouteChild || editing.requestEnabled;
  const showRequestConfiguration = !isSmbSource && !isWebhookSource && (!editingIsRouteChild || editingMakesRequest);
  const parentStream = useMemo(
    () =>
      isSmbSource || isWebhookSource
        ? null
        : streams.find((stream) => stream.id === editing.parentStreamId) ?? editingRouteParent ?? null,
    [editing.parentStreamId, editingRouteParent, isSmbSource, isWebhookSource, streams],
  );
  const parentTestState = parentStream ? streamTestStates[parentStream.id] : undefined;
  const parentPreviewPayload = useMemo(
    () => parsePreviewPayload(parentTestState?.result, isRestSource ? (parentStream?.responseFormat ?? "JSON") : "JSON"),
    [isRestSource, parentStream?.responseFormat, parentTestState],
  );
  const parentResponseInsights = useMemo(
    () =>
      parentStream && parentPreviewPayload.parsed
        ? buildResponseInsights(
            parentPreviewPayload.parsed,
            parentStream.responseFormat === "CSV" ? "$" : parentStream.responseDataPath,
            parentStream.splitResponseIntoRecords,
            parentPreviewPayload.format,
            parentPreviewPayload.xmlPathMap,
          )
        : null,
    [parentPreviewPayload.format, parentPreviewPayload.parsed, parentPreviewPayload.xmlPathMap, parentStream],
  );
  const parentFieldSuggestions = useMemo(() => {
    const extractedAttributes =
      parentStream?.attributeExtractions
        .map((rule) => (rule.attributeName || "").trim())
        .filter(Boolean) ?? [];
    const previewSuggestions = (
      parentResponseInsights?.identifierSuggestions.length
        ? parentResponseInsights.identifierSuggestions
        : parentResponseInsights?.fieldSuggestions.slice(0, 8) ?? []
    ).map((item) => toAttributeName(item.label));

    return Array.from(new Set([...extractedAttributes, ...previewSuggestions])).slice(0, 10);
  }, [parentResponseInsights, parentStream]);
  const normalizedParentExtractedFieldSet = useMemo(
    () =>
      new Set(
        (parentStream?.attributeExtractions || [])
          .map((rule) => toAttributeName(rule.attributeName || "").toLowerCase())
          .filter(Boolean),
      ),
    [parentStream],
  );
  const childFallbackPathVariables = useMemo(() => {
    if (!isRestSource || !parentStream || !editingMakesRequest) return [] as string[];
    return editingPathVariables.filter((variable) => {
      const normalized = toAttributeName(variable).toLowerCase();
      return !normalizedParentExtractedFieldSet.has(normalized);
    });
  }, [editingMakesRequest, editingPathVariables, isRestSource, normalizedParentExtractedFieldSet, parentStream]);
  const editingPreviewPayload = useMemo(
    () =>
      parsePreviewPayload(
        editingTestState?.result,
        supportsResponseFormat ? editing.responseFormat : "JSON",
      ),
    [editing.responseFormat, editingTestState, supportsResponseFormat],
  );
  const editingResponseInsights = useMemo(
    () =>
      editingPreviewPayload.parsed
        ? buildResponseInsights(
            editingPreviewPayload.parsed,
            editing.responseFormat === "CSV" ? "$" : editing.responseDataPath,
            editing.splitResponseIntoRecords,
            editingPreviewPayload.format,
            editingPreviewPayload.xmlPathMap,
          )
        : null,
    [
      editing.responseFormat,
      editing.responseDataPath,
      editing.splitResponseIntoRecords,
      editingPreviewPayload.format,
      editingPreviewPayload.parsed,
      editingPreviewPayload.xmlPathMap,
    ],
  );
  const selectedResponsePath = selectedResponseNodeByStream[editing.id] || "";
  const selectedRecordResponsePath = selectedRecordResponseNodeByStream[editing.id] || "";
  const selectedResponseNode = useMemo(
    () =>
      editingResponseInsights
        ? findResponseNodeByPath(editingResponseInsights.rootNode, selectedResponsePath)
        : null,
    [editingResponseInsights, selectedResponsePath],
  );
  const selectedRecordResponseNode = useMemo(
    () =>
      editingResponseInsights
        ? findResponseNodeByPath(editingResponseInsights.recordRootNode, selectedRecordResponsePath)
        : null,
    [editingResponseInsights, selectedRecordResponsePath],
  );
  const selectedResponseActionPath = useMemo(() => {
    if (!selectedResponseNode || !editingResponseInsights) return "";
    if (editingResponseInsights.format === "XML") {
      return toXmlExtractionPath(
        selectedResponseNode,
        editingResponseInsights.xmlPathMap,
        editing.xmlSplitDepth,
        editing.xmlRecordElement,
        editing.splitResponseIntoRecords,
      );
    }
    return toRelativeExtractionPath(
      selectedResponseNode.path,
      editingResponseInsights.activeRecordPath || editing.responseDataPath,
    );
  }, [
    editing.responseDataPath,
    editingResponseInsights,
    editingResponseInsights?.activeRecordPath,
    editingResponseInsights?.format,
    editingResponseInsights?.xmlPathMap,
    editing.splitResponseIntoRecords,
    editing.xmlRecordElement,
    editing.xmlSplitDepth,
    selectedResponseNode,
  ]);
  const selectedRecordResponseActionPath = useMemo(
    () => (selectedRecordResponseNode ? selectedRecordResponseNode.path : ""),
    [selectedRecordResponseNode],
  );
  const selectedResponseDisplayPath = useMemo(() => {
    if (!selectedResponseNode) return "";
    if (editing.responseFormat === "XML") {
      return selectedResponseActionPath || selectedResponseNode.path;
    }
    return selectedResponseNode.path;
  }, [editing.responseFormat, selectedResponseActionPath, selectedResponseNode]);
  const activeSelectedResponseNode = selectedRecordResponseNode || selectedResponseNode;
  const activeSelectedResponseActionPath = selectedRecordResponseNode
    ? selectedRecordResponseActionPath
    : selectedResponseActionPath;
  const activeSelectedResponseDisplayPath = selectedRecordResponseNode
    ? selectedRecordResponseActionPath || selectedRecordResponseNode.path
    : selectedResponseDisplayPath;
  const xmlSplitSuggestion = useMemo(
    () =>
      editing.responseFormat === "XML"
        ? suggestXmlSplitFromNode(selectedResponseNode, editingResponseInsights?.xmlPathMap)
        : null,
    [editing.responseFormat, editingResponseInsights?.xmlPathMap, selectedResponseNode],
  );
  const supportsRecordPathPicker =
    sourceType === "mongo" || editing.responseFormat === "JSON";
  const supportsStructuredRecordView =
    sourceType === "mongo" || editing.responseFormat === "JSON" || editing.responseFormat === "CSV";
  const showVisualResponseExplorer =
    sourceType === "mongo" || editing.responseFormat === "JSON" || editing.responseFormat === "XML" || editing.responseFormat === "CSV";
  const childStreamCount = useMemo(
    () => (streamGraph.childrenByParent.get(editing.id) || []).length,
    [editing.id, streamGraph],
  );
  const editingBranchingMode = useMemo(
    () => getBranchingMode(editing, streamGraph),
    [editing, streamGraph],
  );
  const editingParallelBranchCount = useMemo(
    () => (streamGraph.childrenByParent.get(editing.id) || []).filter((edge) => edge.kind === "fanout").length,
    [editing.id, streamGraph],
  );
  const editingConditionalBranchCount = useMemo(
    () =>
      (streamGraph.childrenByParent.get(editing.id) || []).filter(
        (edge) => edge.kind === "route" || edge.kind === "default_route",
      ).length,
    [editing.id, streamGraph],
  );
  const editingIsEntity = entityStreams.some((stream) => stream.id === editing.id);
  const editingEntityDestinationDraft = editingIsEntity
    ? entityDestinations[editing.id] || defaultEntityDestinationDraft(kafkaOutput)
    : null;
  const parentSampleValue = useMemo(() => {
    if (!editing.parentField || !parentResponseInsights) return "";
    const match = parentResponseInsights.fieldSuggestions.find(
      (item) =>
        item.label === editing.parentField ||
        toAttributeName(item.label) === toAttributeName(editing.parentField),
    );
    return match?.sample ?? "";
  }, [editing.parentField, parentResponseInsights]);
  const openApiOperationOptions = useMemo(
    () => openApiOperationsPage?.items || [],
    [openApiOperationsPage?.items],
  );
  const suggestedMethodsForPath = useMemo(() => {
    if (!isRestSource || !restSource.openApiSpecId.trim()) return ["GET", "POST", "PUT", "PATCH", "DELETE"];
    const path = editing.endpointPath.trim();
    if (!path) return ["GET", "POST", "PUT", "PATCH", "DELETE"];
    const methods = Array.from(
      new Set(
        openApiOperationOptions
          .filter((item) => (item.path_runtime || item.path_original || "").trim() === path)
          .map((item) => (item.method || "").toUpperCase())
          .filter((item) => ["GET", "POST", "PUT", "PATCH", "DELETE"].includes(item)),
      ),
    );
    return methods.length > 0 ? methods : ["GET", "POST", "PUT", "PATCH", "DELETE"];
  }, [editing.endpointPath, isRestSource, openApiOperationOptions, restSource.openApiSpecId]);

  useEffect(() => {
    if (!isRestSource || !selectedOpenApiOperation) return;
    const params = (selectedOpenApiOperation.parameters || []).filter(
      (p) => p && (p.in === "path" || p.in === "query"),
    );
    if (params.length === 0) return;

    setStreams((prev) =>
      prev.map((stream) => {
        if (stream.id !== editing.id) return stream;
        const nextKeySet = params
          .map((p) => `${p.in}:${p.name}`)
          .sort()
          .join("|");
        const existingKeySet = (stream.openApiParamBindings || [])
          .map((p) => `${p.paramIn}:${p.paramName}`)
          .sort()
          .join("|");
        if (existingKeySet === nextKeySet && stream.openApiParamBindings.length > 0) {
          return stream;
        }

        const siblingStreams = prev.filter((item) => item.id !== stream.id);
        const fallbackParentId =
          stream.parentStreamId || siblingStreams.find((item) => item.isPrimary)?.id || siblingStreams[0]?.id || "";
        const parentStreamCandidate = siblingStreams.find((item) => item.id === fallbackParentId);

        const parentCandidates = Array.from(
          new Set([
            ...(parentStreamCandidate?.attributeExtractions || [])
              .map((rule) => (rule.attributeName || "").trim())
              .filter(Boolean),
            ...((parentStreamCandidate?.primaryKeyFields || "")
              .split(",")
              .map((item) => item.trim())
              .filter(Boolean)),
            "id",
          ]),
        );
        const normalize = (value: string) => toAttributeName(value).toLowerCase();

        const bindings = params.map((param) => {
          const paramName = String(param.name || "").trim();
          const paramIn = param.in as "path" | "query";
          const required = Boolean(param.required || paramIn === "path");
          const normalizedParam = normalize(paramName);

          let bestField = "";
          let bestScore = 0;
          for (const candidate of parentCandidates) {
            const normalizedCandidate = normalize(candidate);
            let score = 0;
            if (normalizedCandidate === normalizedParam) score = 100;
            else if (
              normalizedCandidate === normalizedParam.replace(/_id$/, "") ||
              normalizedParam === normalizedCandidate.replace(/_id$/, "")
            ) {
              score = 85;
            } else if (
              normalizedCandidate.includes(normalizedParam) ||
              normalizedParam.includes(normalizedCandidate)
            ) {
              score = 65;
            }
            if (score > bestScore) {
              bestScore = score;
              bestField = candidate;
            }
          }

          let valueSource: ParamValueSource = "manual_runtime";
          let parentField = "";
          let confidence: "high" | "medium" | "low" = "low";
          if (paramIn === "path" && bestField && fallbackParentId) {
            valueSource = "parent_field";
            parentField = bestField;
            confidence = bestScore >= 90 ? "high" : bestScore >= 70 ? "medium" : "low";
          }

          const defaultValue =
            param.default == null ? "" : typeof param.default === "string" ? param.default : JSON.stringify(param.default);
          if (defaultValue && paramIn === "query") {
            valueSource = "static";
          }

          return createParamBinding({
            paramName,
            paramIn,
            required,
            enabled: required,
            valueSource,
            parentStreamId: valueSource === "parent_field" ? fallbackParentId : "",
            parentField,
            staticValue: valueSource === "static" ? defaultValue : "",
            runtimeVarName: paramName,
            confidence,
            description: param.description || "",
            schemaType: param.schema_type || "",
            defaultValue,
          });
        });

        const firstParentBinding = bindings.find((binding) => binding.valueSource === "parent_field");
        return {
          ...stream,
          parentStreamId: stream.parentStreamId || fallbackParentId,
          parentField: stream.parentField || firstParentBinding?.parentField || stream.parentField,
          injectFieldName: stream.injectFieldName || firstParentBinding?.paramName || stream.injectFieldName,
          injectAs:
            stream.injectFieldName && stream.injectAs
              ? stream.injectAs
              : firstParentBinding?.paramIn === "query"
              ? "QUERY_PARAM"
              : "PATH_PARAM",
          openApiParamBindings: bindings,
        };
      }),
    );
  }, [editing.id, isRestSource, selectedOpenApiOperation]);

  const setPrimaryStream = (id: string) => {
    setStreams((prev) => prev.map((stream) => ({ ...stream, isPrimary: stream.id === id })));
  };

  const resetOpenApiSelection = () => {
    setRestSource((prev) => ({
      ...prev,
      openApiSpecId: "",
      openApiFileName: "",
      openApiTitle: "",
      openApiVersion: "",
      openApiFormat: "",
      openApiWarnings: [],
      openApiServers: [],
      openApiSelectedServerUrl: "",
      openApiOperationsCount: 0,
    }));
    setOpenApiUploadStatus("idle");
    setOpenApiUploadError("");
  };

  const handleOpenApiUpload = async (file: File) => {
    setOpenApiUploadStatus("uploading");
    setOpenApiUploadError("");
    try {
      const parsed = await openApiApi.parse(file);
      setRestSource((prev) => ({
        ...prev,
        openApiSpecId: parsed.spec_id,
        openApiFileName: file.name,
        openApiTitle: parsed.title || "",
        openApiVersion: parsed.version || "",
        openApiFormat: parsed.format || "",
        openApiWarnings: Array.isArray(parsed.warnings) ? parsed.warnings : [],
        openApiServers: Array.isArray(parsed.servers) ? parsed.servers : [],
        openApiSelectedServerUrl:
          prev.openApiSelectedServerUrl ||
          (Array.isArray(parsed.servers) && parsed.servers.length > 0 ? parsed.servers[0].url : ""),
        openApiOperationsCount: typeof parsed.operations_count === "number" ? parsed.operations_count : 0,
      }));
      setOpenApiUploadStatus("idle");
      toast.success("OpenAPI document parsed successfully.");
    } catch (err) {
      setOpenApiUploadStatus("error");
      const message = (err as Error).message || "Failed to parse OpenAPI document";
      setOpenApiUploadError(message);
      toast.error(message);
    }
  };

  const updateStream = (id: string, patch: Partial<Stream>) => {
    setStreams((prev) => prev.map((stream) => (stream.id === id ? { ...stream, ...patch } : stream)));
  };

  const applyOpenApiOperationToStream = (streamId: string, operationId: string) => {
    const op = openApiOperationOptions.find((item) => item.operation_id === operationId);
    if (!op) return;
    const method = (op.method || "GET").toUpperCase();
    const normalizedMethod: StreamMethod =
      method === "POST" || method === "PUT" || method === "PATCH" || method === "DELETE" ? method : "GET";
    const normalizeEndpointAgainstBaseUrl = (baseUrl: string, endpointPath: string): string => {
      const endpoint = (endpointPath || "").trim();
      if (!endpoint.startsWith("/")) return endpoint;
      try {
        const parsed = new URL(baseUrl);
        const basePath = (parsed.pathname || "").replace(/\/+$/, "");
        if (!basePath || basePath === "/") return endpoint;
        const baseParts = basePath.split("/").filter(Boolean);
        const endpointParts = endpoint.split("/").filter(Boolean);
        let overlap = 0;
        const maxLen = Math.min(baseParts.length, endpointParts.length);
        for (let len = maxLen; len > 0; len -= 1) {
          const baseSuffix = baseParts.slice(baseParts.length - len).join("/");
          const endpointPrefix = endpointParts.slice(0, len).join("/");
          if (baseSuffix === endpointPrefix) {
            overlap = len;
            break;
          }
        }
        if (overlap > 0) {
          const trimmed = endpointParts.slice(overlap);
          return trimmed.length > 0 ? `/${trimmed.join("/")}` : "/";
        }
      } catch {
        // Keep original endpoint when base URL isn't a parseable absolute URL.
      }
      return endpoint;
    };
    const operationPath = op.path_runtime || op.path_original || "";
    const normalizedEndpointPath = normalizeEndpointAgainstBaseUrl(restSource.baseUrl, operationPath);
    updateStream(streamId, {
      openApiOperationId: op.operation_id,
      endpointPath: normalizedEndpointPath || operationPath,
      method: normalizedMethod,
    });
  };

  const updateOpenApiParamBinding = (
    streamId: string,
    bindingId: string,
    patch: Partial<StreamParamBinding>,
  ) => {
    setStreams((prev) =>
      prev.map((stream) => {
        if (stream.id !== streamId) return stream;
        const nextBindings = stream.openApiParamBindings.map((binding) =>
          binding.id === bindingId ? { ...binding, ...patch } : binding,
        );
        const updated = nextBindings.find((binding) => binding.id === bindingId);
        const nextParentStreamId =
          updated && updated.valueSource === "parent_field"
            ? updated.parentStreamId || stream.parentStreamId
            : stream.parentStreamId;
        return {
          ...stream,
          parentStreamId: nextParentStreamId,
          openApiParamBindings: nextBindings,
        };
      }),
    );
  };

  const applySuggestedRecordPath = (streamId: string, path: string) => {
    updateStream(streamId, {
      splitResponseIntoRecords: true,
      responseDataPath: path,
    });
    setSelectedResponseNodeByStream((prev) => ({ ...prev, [streamId]: "" }));
    setSelectedRecordResponseNodeByStream((prev) => ({ ...prev, [streamId]: "" }));
    toast.success(`Using ${path} as the record path`);
  };

  const toggleKafkaKeyAttribute = (streamId: string, attributeName: string) => {
    const normalized = attributeName.trim();
    if (!normalized) return;

    setStreams((prev) =>
      prev.map((stream) => {
        if (stream.id !== streamId) return stream;
        const current = stream.primaryKeyFields
          .split(",")
          .map((item) => item.trim())
          .filter(Boolean)[0] || "";
        return {
          ...stream,
          primaryKeyFields: current === normalized ? "" : normalized,
        };
      }),
    );
    toast.success("Kafka key updated");
  };

  const applySuggestedXmlSplitRoot = (streamId: string, suggestion: XmlSplitSuggestion) => {
    updateStream(streamId, {
      splitResponseIntoRecords: true,
      xmlSplitDepth: String(suggestion.depth),
      xmlRecordElement: suggestion.recordElement,
    });
    toast.success(`Using XML split depth ${suggestion.depth} (${suggestion.recordElement})`);
  };

  const applySuggestedExtraction = (streamId: string, attributeName: string, pathExpression: string) => {
    const normalizedAttribute = toAttributeName(attributeName);
    const normalizedPath =
      sourceType === "smb"
        ? normalizeSmbClickedPath(pathExpression, normalizedAttribute)
        : pathExpression.trim();
    if (!normalizedPath) return;

    setStreams((prev) =>
      prev.map((stream) => {
        if (stream.id !== streamId) return stream;

        const existingIndex = stream.attributeExtractions.findIndex(
          (rule) => rule.pathExpression === normalizedPath || rule.attributeName === normalizedAttribute,
        );
        if (existingIndex >= 0) {
          return {
            ...stream,
            attributeExtractions: stream.attributeExtractions.map((rule, index) =>
              index === existingIndex
                ? { ...rule, attributeName: normalizedAttribute, pathExpression: normalizedPath }
                : rule,
            ),
          };
        }

        const blankIndex = stream.attributeExtractions.findIndex((rule) => !rule.attributeName && !rule.pathExpression);
        if (blankIndex >= 0) {
          return {
            ...stream,
            attributeExtractions: stream.attributeExtractions.map((rule, index) =>
              index === blankIndex
                ? { ...rule, attributeName: normalizedAttribute, pathExpression: normalizedPath }
                : rule,
            ),
          };
        }

        return {
          ...stream,
          attributeExtractions: [
            ...stream.attributeExtractions,
            {
              ...createExtractionRule(),
              attributeName: normalizedAttribute,
              pathExpression: normalizedPath,
            },
          ],
        };
      }),
    );

    toast.success(`Added extraction for ${normalizedAttribute}`);
  };

  const addExtractionRule = (streamId: string) => {
    setStreams((prev) =>
      prev.map((stream) =>
        stream.id === streamId
          ? { ...stream, attributeExtractions: [...stream.attributeExtractions, createExtractionRule()] }
          : stream,
      ),
    );
  };

  const updateExtractionRule = (streamId: string, ruleId: string, patch: Partial<AttributeExtractionRule>) => {
    setStreams((prev) =>
      prev.map((stream) =>
        stream.id === streamId
          ? {
              ...stream,
              attributeExtractions: stream.attributeExtractions.map((rule) =>
                rule.id === ruleId ? { ...rule, ...patch } : rule,
              ),
            }
          : stream,
      ),
    );
  };

  const removeExtractionRule = (streamId: string, ruleId: string) => {
    setStreams((prev) =>
      prev.map((stream) =>
        stream.id === streamId
          ? {
              ...stream,
              attributeExtractions: stream.attributeExtractions.filter((rule) => rule.id !== ruleId),
            }
          : stream,
      ),
    );
  };

  const addTransformationRule = (streamId: string) => {
    setStreams((prev) =>
      prev.map((stream) =>
        stream.id === streamId
          ? { ...stream, transformations: [...stream.transformations, createTransformationRule()] }
          : stream,
      ),
    );
  };

  const updateTransformationRule = (streamId: string, ruleId: string, patch: Partial<TransformationRule>) => {
    setStreams((prev) =>
      prev.map((stream) =>
        stream.id === streamId
          ? {
              ...stream,
              transformations: stream.transformations.map((rule) =>
                rule.id === ruleId ? { ...rule, ...patch } : rule,
              ),
            }
          : stream,
      ),
    );
  };

  const removeTransformationRule = (streamId: string, ruleId: string) => {
    setStreams((prev) =>
      prev.map((stream) =>
        stream.id === streamId
          ? {
              ...stream,
              transformations: stream.transformations.filter((rule) => rule.id !== ruleId),
            }
          : stream,
      ),
    );
  };

  const addRoutingRule = (streamId: string) => {
    setStreams((prev) =>
      prev.map((stream) =>
        stream.id === streamId
          ? { ...stream, routingRules: [...stream.routingRules, createRoutingRule()] }
          : stream,
      ),
    );
  };

  const updateRoutingRule = (streamId: string, ruleId: string, patch: Partial<RoutingRule>) => {
    setStreams((prev) =>
      prev.map((stream) =>
        stream.id === streamId
          ? {
              ...stream,
              routingRules: stream.routingRules.map((rule) =>
                rule.id === ruleId ? { ...rule, ...patch } : rule,
              ),
            }
          : stream,
      ),
    );
  };

  const removeRoutingRule = (streamId: string, ruleId: string) => {
    setStreams((prev) =>
      prev.map((stream) =>
        stream.id === streamId
          ? {
              ...stream,
              routingRules: stream.routingRules.filter((rule) => rule.id !== ruleId),
            }
          : stream,
      ),
    );
  };

  const clearRouteTargetsForStream = (stream: Stream): Stream => ({
    ...stream,
    routingRules: (stream.routingRules || []).map((rule) =>
      rule.destination === "ROUTE_TO_STREAM"
        ? { ...rule, destination: "INCLUDE" as RoutingDestination, targetStreamId: "" }
        : { ...rule, targetStreamId: "" },
    ),
    defaultRouteAction: "DROP",
    defaultRouteTargetStreamId: "",
  });

  const detachParallelChildren = (parentId: string, nextEditingId?: string) => {
    setStreams((prev) =>
      prev.map((stream) =>
        stream.parentStreamId === parentId
          ? {
              ...stream,
              parentStreamId: "",
              fanOutCompletionMode: "per_record",
              parentField: "",
              injectFieldName: "",
              injectTemplate: "",
              requestEnabled: true,
            }
          : stream,
      ),
    );
    if (nextEditingId) {
      setEditingStream(nextEditingId);
    }
  };

  const detachParallelChild = (childId: string) => {
    setStreams((prev) =>
      prev.map((stream) =>
        stream.id === childId
          ? {
              ...stream,
              parentStreamId: "",
              fanOutCompletionMode: "per_record",
              parentField: "",
              injectFieldName: "",
              injectTemplate: "",
            }
          : stream,
      ),
    );
  };

  const attachParallelChild = (parentId: string, childId: string) => {
    setStreams((prev) =>
      prev.map((stream) => {
        if (stream.id === childId) {
          return {
            ...stream,
            parentStreamId: parentId,
            fanOutCompletionMode: stream.fanOutCompletionMode || "per_record",
            requestEnabled: true,
            defaultRouteAction: stream.defaultRouteAction === "ROUTE_TO_STREAM" ? "DROP" : stream.defaultRouteAction,
            defaultRouteTargetStreamId:
              stream.defaultRouteAction === "ROUTE_TO_STREAM" ? "" : stream.defaultRouteTargetStreamId,
          };
        }
        const nextRules = (stream.routingRules || []).map((rule) =>
          rule.targetStreamId === childId
            ? {
                ...rule,
                destination: "INCLUDE" as RoutingDestination,
                targetStreamId: "",
              }
            : rule,
        );
        return {
          ...stream,
          routingRules: nextRules,
          defaultRouteTargetStreamId: stream.defaultRouteTargetStreamId === childId ? "" : stream.defaultRouteTargetStreamId,
          defaultRouteAction:
            stream.defaultRouteTargetStreamId === childId && stream.defaultRouteAction === "ROUTE_TO_STREAM"
              ? "DROP"
              : stream.defaultRouteAction,
        };
      }),
    );
    setEditingStream(childId);
  };

  const handleCreateParallelBranch = (parentId: string) => {
    if (isWebhookSource) {
      toast.error("Webhook sources support exactly one stream.");
      return;
    }
    const name = nextGeneratedStreamName(streams);
    const stream = createDefaultStream(name);
    if (sourceType === "mongo") {
      stream.endpointPath = name;
    } else if (sourceType === "rest" || sourceType === "postgres") {
      stream.endpointPath = sourceType === "rest" ? "" : `/${name}`;
      stream.method = "GET";
    } else {
      stream.endpointPath = "";
      stream.method = "GET";
    }
    stream.parentStreamId = parentId;
    stream.fanOutCompletionMode = "per_record";
    stream.isPrimary = false;
    stream.branchingMode = "none";
    stream.requestEnabled = true;

    setStreams((prev) => [...prev, stream]);
    setEditingStream(stream.id);
    toast.success(`Branch stream "${name}" added.`);
  };

  const handleCreateStream = () => {
    if (isWebhookSource) {
      toast.error("Webhook sources support exactly one stream.");
      return;
    }
    const name = nextGeneratedStreamName(streams);
    const stream = createDefaultStream(name);
    if (sourceType === "mongo") {
      stream.endpointPath = name;
    } else if (sourceType === "rest" || sourceType === "postgres") {
      stream.endpointPath = sourceType === "rest" ? "" : `/${name}`;
      stream.method = "GET";
    } else {
      stream.endpointPath = "";
      stream.method = "GET";
    }

    const shouldBePrimary = streams.every((item) => !item.isPrimary);
    stream.isPrimary = shouldBePrimary;

    setStreams((prev) => {
      const base = shouldBePrimary ? prev.map((item) => ({ ...item, isPrimary: false })) : prev;
      return [...base, stream];
    });

    setEditingStream(stream.id);
    toast.success(`Stream "${name}" added. Configure it in the stream editor.`);
  };

  const handleDeleteStream = (streamId: string) => {
    if (streams.length <= 1) {
      toast.error("At least one stream is required.");
      return;
    }

    const streamToDelete = streams.find((stream) => stream.id === streamId);
    if (!streamToDelete) return;

    const confirmed = confirm(`Delete stream "${streamToDelete.name}"?`);
    if (!confirmed) return;

    const remaining = streams
      .filter((stream) => stream.id !== streamId)
      .map((stream) => ({
        ...stream,
        // Remove broken parent references when parent stream is deleted.
        ...(stream.parentStreamId === streamId
          ? {
              parentStreamId: "",
              fanOutCompletionMode: "per_record",
              parentField: "",
              injectFieldName: "",
              injectTemplate: "",
            }
          : {}),
        openApiParamBindings: (stream.openApiParamBindings || []).map((binding) =>
          binding.parentStreamId === streamId
            ? { ...binding, parentStreamId: "", parentField: "", valueSource: "manual_runtime" }
            : binding,
        ),
        routingRules: (stream.routingRules || []).map((rule) =>
          rule.targetStreamId === streamId ? { ...rule, targetStreamId: "" } : rule,
        ),
        defaultRouteTargetStreamId:
          stream.defaultRouteTargetStreamId === streamId ? "" : stream.defaultRouteTargetStreamId,
        defaultRouteAction:
          stream.defaultRouteTargetStreamId === streamId ? "DROP" : stream.defaultRouteAction,
      }));

    let nextStreams = remaining;
    if (!remaining.some((stream) => stream.isPrimary) && remaining.length > 0) {
      nextStreams = remaining.map((stream, index) => ({ ...stream, isPrimary: index === 0 }));
    }

    setStreams(nextStreams);
    setSelectedResponseNodeByStream((prev) => {
      const next = { ...prev };
      delete next[streamId];
      return next;
    });
    setEntityDestinations((prev) => {
      const next = { ...prev };
      delete next[streamId];
      return next;
    });

    if (editingStream === streamId && nextStreams.length > 0) {
      const preferred = nextStreams.find((stream) => stream.isPrimary) ?? nextStreams[0];
      setEditingStream(preferred.id);
    }

    toast.success(`Stream "${streamToDelete.name}" deleted`);
  };

  const executeStreamTest = async (
    streamId: string,
    variableValues: Record<string, string> = {},
    confirmMutation = false,
  ) => {
    if (!editingSourceId) {
      toast.error("Please save the source first before testing streams.");
      return;
    }
    const stream = streams.find((s) => s.id === streamId);
    if (!stream) return;

    // For child streams, collect parent values from last successful parent test
    const parentValues: Record<string, unknown> = variableValues;

    setStreamTestStates((prev) => ({ ...prev, [streamId]: { status: "running" } }));

    try {
      const response = await fetch(`${getApiBase()}/api/sources/${editingSourceId}/test-stream`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          stream_id: streamId,
          parent_values: parentValues,
          confirm_mutation: confirmMutation,
        }),
      });
      const result = await response.json();
      setStreamTestStates((prev) => ({
        ...prev,
        [streamId]: {
          status: result.ok ? "success" : "error",
          result,
        },
      }));
    } catch (e) {
      setStreamTestStates((prev) => ({
        ...prev,
        [streamId]: {
          status: "error",
          result: { ok: false, error: (e as Error).message },
        },
      }));
    }
  };

  const testStream = (streamId: string) => {
    const stream = streams.find((s) => s.id === streamId);
    if (!stream) return;

    if (isMongoSource) {
      const mongoTemplateErrors = [
        ...validateMongoJsonTemplate("Mongo query JSON", stream.mongoQuery || "{}"),
        ...validateMongoJsonTemplate("Mongo projection JSON", stream.mongoProjection || "{}"),
        ...validateMongoJsonTemplate("Mongo sort JSON", stream.mongoSort || "{}"),
      ];
      if (mongoTemplateErrors.length > 0) {
        toast.error(mongoTemplateErrors[0]);
        return;
      }
      const variables = extractMongoTemplateVariables(stream);
      if (variables.length === 0) {
        void executeStreamTest(streamId, {});
        return;
      }
      const values: Record<string, string> = {};
      for (const variable of variables) values[variable] = "";
      setPendingStreamTest({ streamId, variables, values });
      return;
    }

    if (!isRestSource) {
      void executeStreamTest(streamId, {});
      return;
    }
    const isMutatingRestMethod = ["POST", "PUT", "PATCH", "DELETE"].includes(stream.method);
    const confirmMutation =
      isMutatingRestMethod
        ? confirm(`Run ${stream.method} test for stream "${stream.name}"? This may change data in the target API.`)
        : false;
    if (isMutatingRestMethod && !confirmMutation) return;
    const variables = extractTemplateVariables(stream);
    if (variables.length === 0) {
      void executeStreamTest(streamId, {}, confirmMutation);
      return;
    }
    const values: Record<string, string> = {};
    for (const variable of variables) values[variable.name] = "";
    setPendingStreamTest({ streamId, variables, values, confirmMutation });
  };

  const sourceName = useMemo(() => {
    if (sourceType === "rest") return restSource.sourceName;
    if (sourceType === "postgres") return postgresSource.sourceName;
    if (sourceType === "mongo") return mongoSource.sourceName;
    if (sourceType === "webhook") return webhookSource.sourceName;
    if (sourceType === "trino") return trinoSource.sourceName;
    return smbSource.sourceName;
  }, [
    sourceType,
    restSource.sourceName,
    postgresSource.sourceName,
    mongoSource.sourceName,
    smbSource.sourceName,
    webhookSource.sourceName,
    trinoSource.sourceName,
  ]);

  const sourceConnectionSummary = useMemo(() => {
    if (sourceType === "rest") {
      if (restConnectionMode === "application_service") {
        const selected = restApplicationServices.find((service) => service.id === restApplicationServiceId);
        return selected ? `HTTP Service: ${selected.name}` : "HTTP Service";
      }
      return restSource.baseUrl;
    }
    if (sourceType === "postgres") {
      if (!postgresSource.host && !postgresSource.port && !postgresSource.databaseName) return "";
      return `${postgresSource.host}:${postgresSource.port}/${postgresSource.databaseName}`;
    }
    if (sourceType === "mongo") {
      if (mongoSource.connectionMode === "nifi_global_service") {
        const selected = mongoGlobalServices.find((service) => service.id === mongoSource.nifiGlobalServiceId);
        return selected ? `NiFi global service: ${selected.name}` : "NiFi global service";
      }
      if (mongoSource.connectionMode === "uri") return mongoSource.connectionUri;
      if (!mongoSource.host && !mongoSource.port && !mongoSource.databaseName) return "";
      return `${mongoSource.host}:${mongoSource.port}/${mongoSource.databaseName}`;
    }
    if (sourceType === "webhook") {
      if (webhookConnectionMode === "application_service") {
        const selected = webhookApplicationServices.find((service) => service.id === webhookApplicationServiceId);
        return selected ? `Service: ${selected.name}` : "Service";
      }
      if (!webhookSource.endpointId.trim()) return "";
      return `/api/webhooks/${webhookSource.endpointId.trim()}`;
    }
    if (sourceType === "trino") {
      const tableRef = [trinoSource.catalog, trinoSource.schema, trinoSource.table].map((item) => item.trim()).filter(Boolean).join(".");
      return trinoSource.endpoint.trim() && tableRef ? `${trinoSource.endpoint.trim()} :: ${tableRef}` : trinoSource.endpoint.trim() || tableRef;
    }
    if (smbConnectionMode === "application_service") {
      const selected = smbApplicationServices.find((service) => service.id === smbApplicationServiceId);
      const serviceLabel = selected ? `Service: ${selected.name}` : "Service";
      return smbSource.baseDirectoryPath ? `${serviceLabel} :: ${smbSource.baseDirectoryPath}` : serviceLabel;
    }
    if (!smbSource.smbConnection && !smbSource.baseDirectoryPath) return "";
    return `${smbSource.smbConnection} :: ${smbSource.baseDirectoryPath}`;
  }, [
    sourceType,
    restConnectionMode,
    restApplicationServiceId,
    restApplicationServices,
    restSource.baseUrl,
    postgresSource.host,
    postgresSource.port,
    postgresSource.databaseName,
    mongoSource.host,
    mongoSource.port,
    mongoSource.databaseName,
    mongoSource.connectionMode,
    mongoSource.connectionUri,
    mongoSource.nifiGlobalServiceId,
    mongoGlobalServices,
    webhookConnectionMode,
    webhookApplicationServiceId,
    webhookApplicationServices,
    webhookSource.endpointId,
    trinoSource.endpoint,
    trinoSource.catalog,
    trinoSource.schema,
    trinoSource.table,
    smbConnectionMode,
    smbApplicationServiceId,
    smbApplicationServices,
    smbSource.smbConnection,
    smbSource.baseDirectoryPath,
  ]);

  const streamSummary = useMemo(() => {
    const entityIds = new Set(getEntityStreams(streams).map((stream) => stream.id));
    return streams
      .filter((stream) => stream.name.trim())
      .map((stream) => `${stream.name}${entityIds.has(stream.id) ? " (entity)" : " (routing)"}`)
      .join(", ");
  }, [streams]);

  const primaryEntityStream = useMemo(() => entityStreams[0] || primaryStream, [entityStreams, primaryStream]);
  const derivedKafkaTopic = useMemo(() => {
    const flowName = sourceName.trim() || "source";
    const primaryStreamName = primaryEntityStream?.name?.trim() || "stream";
    return buildKafkaTopicName(flowName, primaryStreamName);
  }, [sourceName, primaryEntityStream?.name]);
  const derivedSchemaArtifactId = useMemo(() => `${derivedKafkaTopic}-value`, [derivedKafkaTopic]);

  useEffect(() => {
    setEntityDestinations((prev) => {
      const next: Record<string, EntityDestinationDraft> = {};
      for (const stream of entityStreams) {
        const existing = prev[stream.id];
        const entityKafka = stream.entity?.kafka;
        const linkedSchemaVersion =
          stream.entity?.schemaVersion ||
          entityKafka?.schemaVersion ||
          "";
        const linkedSchemaArtifactId =
          stream.entity?.schemaArtifactId ||
          entityKafka?.schemaArtifactId ||
          "";
        const hydrated: EntityDestinationDraft = {
          ...defaultEntityDestinationDraft(kafkaOutput),
          partitions: entityKafka?.partitions ? String(entityKafka.partitions) : defaultEntityDestinationDraft(kafkaOutput).partitions,
          replicationFactor: entityKafka?.replicationFactor ? String(entityKafka.replicationFactor) : defaultEntityDestinationDraft(kafkaOutput).replicationFactor,
          keyStrategy: entityKafka?.keyStrategy ? normalizeKafkaKeyStrategy(entityKafka.keyStrategy) : normalizeKafkaKeyStrategy(kafkaOutput.keyStrategy),
          schemaMode:
            entityKafka?.schemaMode === "existing" || linkedSchemaArtifactId || linkedSchemaVersion
              ? "existing"
              : kafkaOutput.schemaMode || "auto_generate",
          existingSchemaArtifactId: linkedSchemaArtifactId,
          existingSchemaVersion: linkedSchemaVersion,
          iceberg: stream.entity?.iceberg ?? defaultEntityIcebergConfig(),
        };
        next[stream.id] = existing
          ? {
              ...hydrated,
              ...existing,
              existingSchemaArtifactId: existing.existingSchemaArtifactId || linkedSchemaArtifactId,
              existingSchemaVersion: existing.existingSchemaVersion || linkedSchemaVersion,
              schemaMode: existing.existingSchemaVersion || linkedSchemaVersion ? "existing" : existing.schemaMode,
              iceberg: existing.iceberg ?? hydrated.iceberg,
            }
          : hydrated;
      }
      return next;
    });
  }, [
    entityStreamIdsKey,
    entityStreams,
    kafkaOutput.keyStrategy,
    kafkaOutput.partitions,
    kafkaOutput.replicationFactor,
    kafkaOutput.schemaMode,
  ]);

  const paginationSummary = useMemo(() => {
    if (sourceType !== "rest") return "";
    if (!primaryStream) return "";
    if (!primaryStream.paginationEnabled || primaryStream.paginationType === "NONE") return "";
    const where = primaryStream.paginationLocation === "BODY" ? ", sent in request body" : "";
    if (primaryStream.paginationType === "PAGE_INCREMENT") {
      return `Page Increment (${primaryStream.pageParamName}, size ${primaryStream.pageSizeValue})${where}`;
    }
    if (primaryStream.paginationType === "CURSOR") {
      const from =
        primaryStream.cursorSource === "HEADER"
          ? `${primaryStream.cursorHeader} header`
          : primaryStream.cursorValuePath;
      return `Cursor (${primaryStream.cursorParamName} <- ${from})${where}`;
    }
    if (primaryStream.paginationType === "NEXT_URL") {
      const from =
        primaryStream.nextUrlSource === "LINK_HEADER"
          ? `${primaryStream.nextUrlHeader} header, rel="next"`
          : primaryStream.nextUrlSource === "HEADER"
          ? `${primaryStream.nextUrlHeader} header`
          : primaryStream.nextUrlPath;
      return `Next URL (${from})`;
    }
    return `Offset/Limit (${primaryStream.offsetParamName}/${primaryStream.limitParamName})${where}`;
  }, [primaryStream, sourceType]);

  const mongoPublishMode = useMemo(() => {
    if (sourceType !== "mongo" || !primaryStream) return "";
    return primaryStream.splitResponseIntoRecords
      ? "One Kafka message per Mongo document"
      : "One Kafka message per Mongo query batch";
  }, [sourceType, primaryStream]);

  const fanOutSummary = useMemo(() => {
    if (sourceType === "smb" || sourceType === "webhook" || sourceType === "trino") return "";
    const fanOutStreams = streams.filter((stream) => stream.parentStreamId);
    if (fanOutStreams.length === 0) return "";

    return fanOutStreams
      .map((stream) => {
        const parent = streams.find((item) => item.id === stream.parentStreamId);
        const parentName = parent?.name || "unknown";
        const injectLabel =
          stream.injectAs === "QUERY_PARAM"
            ? "query"
            : stream.injectAs === "PATH_PARAM"
            ? "path"
            : stream.injectAs === "HEADER"
            ? "header"
            : "body";
        return `${parentName}.${stream.parentField || "value"} -> ${stream.name} (${injectLabel}:${stream.injectFieldName || "field"})`;
      })
      .join("; ");
  }, [sourceType, streams]);

  const scheduleSummary = useMemo(() => {
    if (schedule.type === "interval") {
      if (schedule.interval.value === "") return "";
      const value = Math.max(1, Math.trunc(schedule.interval.value));
      const unit = value === 1 ? schedule.interval.unit.replace(/s$/, "") : schedule.interval.unit;
      return `Runs every ${value} ${unit}`;
    }

    const expression = schedule.cron.expression.trim();
    return expression ? `Runs using cron expression: ${expression}` : "";
  }, [schedule]);

  const autoGeneratedSchemaArtifactId = useMemo(() => {
    if (!derivedKafkaTopic) return "";
    return `${derivedKafkaTopic}-value`;
  }, [derivedKafkaTopic]);

  const selectedExistingArtifact = useMemo(
    () =>
      availableSchemaArtifacts.find(
        (artifact) => artifact.artifact_id === derivedSchemaArtifactId,
      ) ?? null,
    [availableSchemaArtifacts, derivedSchemaArtifactId],
  );

  const selectedExistingVersionNumber = useMemo(() => {
    const parsed = Number.parseInt(kafkaOutput.existingSchemaVersion, 10);
    if (!Number.isFinite(parsed) || parsed <= 0) return null;
    return Math.trunc(parsed);
  }, [kafkaOutput.existingSchemaVersion]);

  const selectedExistingVersion = useMemo<ApiSchemaVersion | null>(() => {
    if (!selectedExistingArtifact || !selectedExistingVersionNumber) return null;
    return (
      selectedExistingArtifact.versions.find((version) => version.version === selectedExistingVersionNumber) ??
      null
    );
  }, [selectedExistingArtifact, selectedExistingVersionNumber]);

  const autoGeneratedArtifact = useMemo<ApiSchemaArtifact | null>(
    () =>
      availableSchemaArtifacts.find((a) => a.artifact_id === autoGeneratedSchemaArtifactId) ?? null,
    [availableSchemaArtifacts, autoGeneratedSchemaArtifactId],
  );

  const autoGeneratedLatestVersion = useMemo<ApiSchemaVersion | null>(() => {
    if (!autoGeneratedArtifact || autoGeneratedArtifact.versions.length === 0) return null;
    return autoGeneratedArtifact.versions[autoGeneratedArtifact.versions.length - 1];
  }, [autoGeneratedArtifact]);

  const schemaWorkflowRequired = requiresSchemaWorkflow(sourceType);

  const entityDestinationSummaries = useMemo(() => {
    return entityStreams.map((stream) => {
      const draft = entityDestinations[stream.id] || defaultEntityDestinationDraft(kafkaOutput);
      const topic = buildKafkaTopicName(sourceName.trim() || "source", stream.name.trim() || "stream");
      const generatedSchemaArtifactId = `${topic}-value`;
      const schemaArtifactId =
        draft.schemaMode === "existing"
          ? draft.existingSchemaArtifactId || generatedSchemaArtifactId
          : generatedSchemaArtifactId;
      const artifact = availableSchemaArtifacts.find((item) => item.artifact_id === schemaArtifactId) ?? null;
      const selectedVersionNumber = Number.parseInt(draft.existingSchemaVersion, 10);
      const selectedVersion =
        artifact?.versions.find((version) => version.version === selectedVersionNumber) ?? null;
      const latestVersion =
        artifact && artifact.versions.length > 0 ? artifact.versions[artifact.versions.length - 1] : null;
      const effectiveVersion = draft.schemaMode === "existing" ? selectedVersion : latestVersion;
      const effectiveStatus: SchemaVersionStatus =
        draft.schemaMode === "existing"
          ? selectedVersion?.status ?? "Draft"
          : latestVersion?.status ?? "Needs Verification";
      return {
        stream,
        draft,
        topic,
        schemaArtifactId,
        artifact,
        selectedVersion,
        latestVersion,
        effectiveVersion,
        effectiveStatus,
        inferenceJob: inferenceJobsByEntity[stream.id] || null,
        inferenceStarting: Boolean(inferenceStartingByEntity[stream.id]),
      };
    });
  }, [
    availableSchemaArtifacts,
    entityDestinations,
    entityStreams,
    inferenceJobsByEntity,
    inferenceStartingByEntity,
    kafkaOutput,
    sourceName,
  ]);

  const allEntitySchemasVerified = useMemo(
    () =>
      entityDestinationSummaries.length > 0 &&
      !shouldBlockSaveForUnverifiedSchemas(
        sourceType,
        entityDestinationSummaries.map((item) => item.effectiveStatus),
      ),
    [entityDestinationSummaries, sourceType],
  );

  const effectiveSchemaArtifactId = useMemo(() => {
    if (kafkaOutput.schemaMode === "existing") {
      return derivedSchemaArtifactId;
    }
    return autoGeneratedSchemaArtifactId;
  }, [kafkaOutput.schemaMode, derivedSchemaArtifactId, autoGeneratedSchemaArtifactId]);

  const effectiveSchemaVersion = useMemo(() => {
    if (kafkaOutput.schemaMode === "existing") {
      return selectedExistingVersion?.version ?? null;
    }
    return autoGeneratedLatestVersion?.version ?? 1;
  }, [kafkaOutput.schemaMode, selectedExistingVersion, autoGeneratedLatestVersion]);

  const effectiveSchemaVersionStatus = useMemo((): SchemaVersionStatus => {
    if (kafkaOutput.schemaMode === "existing") {
      return selectedExistingVersion?.status ?? "Draft";
    }
    return autoGeneratedLatestVersion?.status ?? "Needs Verification";
  }, [kafkaOutput.schemaMode, selectedExistingVersion, autoGeneratedLatestVersion]);

  const finalSaveButtonLabel = useMemo(() => {
    if (isSaving || inferenceStarting) return "Saving...";
    if (!schemaWorkflowRequired) {
      return editingFlow ? "Save Changes" : "Save";
    }
    if (kafkaOutput.schemaMode === "auto_generate") {
      return editingFlow ? "Save Changes" : "Save";
    }
    if (editingFlow) {
      return allEntitySchemasVerified ? "Save Changes" : "Save Changes & Continue to Schema";
    }
    return allEntitySchemasVerified ? "Save" : "Save & Continue to Schema";
  }, [allEntitySchemasVerified, editingFlow, isSaving, inferenceStarting, kafkaOutput.schemaMode, schemaWorkflowRequired]);

  type DesignerValidationIssue = { step: number; message: string };

  const formatValidationToast = (issues: DesignerValidationIssue[]): string => {
    if (issues.length === 0) return "Validation failed.";
    if (issues.length === 1) return issues[0].message;
    return `${issues[0].message} (+${issues.length - 1} more)`;
  };

  const validateDesignerState = (onlyStep?: number): DesignerValidationIssue[] => {
    const issues: DesignerValidationIssue[] = [];
    const shouldCheck = (stepIndex: number) => onlyStep === undefined || onlyStep === stepIndex;
    const add = (stepIndex: number, message: string) => {
      issues.push({ step: stepIndex, message });
    };

    if (shouldCheck(STEP_CONFIGURE_SOURCE)) {
      if (!sourceName.trim()) {
        add(STEP_CONFIGURE_SOURCE, "Source name is required.");
      }

      if (sourceType === "rest") {
        if (restConnectionMode === "application_service") {
          if (!restApplicationServiceId.trim()) {
            add(STEP_CONFIGURE_SOURCE, "Select an HTTP Service.");
          }
        } else if (!restSource.baseUrl.trim()) {
          add(STEP_CONFIGURE_SOURCE, "Base URL is required for REST sources.");
        } else if (!isLikelyHttpUrl(restSource.baseUrl)) {
          add(STEP_CONFIGURE_SOURCE, "Base URL must be a valid http/https URL.");
        }
        if (restConnectionMode === "manual" && restSource.authType === "API_KEY") {
          if (!restSource.apiKeyName.trim()) add(STEP_CONFIGURE_SOURCE, "API key name is required.");
          if (!restSource.apiKeyValue.trim()) add(STEP_CONFIGURE_SOURCE, "API key value is required.");
        }
        if (restConnectionMode === "manual" && restSource.authType === "BEARER" && !restSource.bearerToken.trim()) {
          add(STEP_CONFIGURE_SOURCE, "Bearer token is required.");
        }
        if (restConnectionMode === "manual" && restSource.authType === "BASIC") {
          if (!restSource.username.trim()) add(STEP_CONFIGURE_SOURCE, "Username is required for basic auth.");
          if (!restSource.password.trim()) add(STEP_CONFIGURE_SOURCE, "Password is required for basic auth.");
        }
        const restNumericChecks: Array<{ label: string; value: string }> = [
          { label: "Connection timeout", value: restSource.connectionTimeout },
          { label: "Socket read timeout", value: restSource.socketReadTimeout },
          { label: "Socket write timeout", value: restSource.socketWriteTimeout },
          { label: "Socket idle timeout", value: restSource.socketIdleTimeout },
          { label: "Rate limit", value: restSource.rateLimitPerMin },
        ];
        if (restConnectionMode === "manual") {
          restNumericChecks.forEach((field) => {
            if (field.value.trim() && !parsePositiveInteger(field.value)) {
              add(STEP_CONFIGURE_SOURCE, `${field.label} must be a positive integer.`);
            }
          });
        }
      }

      if (sourceType === "postgres") {
        if (!postgresSource.host.trim()) add(STEP_CONFIGURE_SOURCE, "PostgreSQL host is required.");
        if (!postgresSource.port.trim()) {
          add(STEP_CONFIGURE_SOURCE, "PostgreSQL port is required.");
        } else if (!parsePort(postgresSource.port)) {
          add(STEP_CONFIGURE_SOURCE, "PostgreSQL port must be between 1 and 65535.");
        }
        if (!postgresSource.databaseName.trim()) add(STEP_CONFIGURE_SOURCE, "PostgreSQL database name is required.");
        if (!postgresSource.connectionTimeout.trim() || !parsePositiveInteger(postgresSource.connectionTimeout)) {
          add(STEP_CONFIGURE_SOURCE, "PostgreSQL connection timeout must be a positive integer.");
        }
      }

      if (sourceType === "mongo") {
        if (mongoSource.connectionMode === "nifi_global_service") {
          if (!mongoSource.nifiGlobalServiceId.trim()) {
            add(STEP_CONFIGURE_SOURCE, "Select a NiFi global MongoDB service.");
          }
        } else if (mongoSource.connectionMode === "uri") {
          if (!mongoSource.connectionUri.trim()) {
            add(STEP_CONFIGURE_SOURCE, "Mongo connection URI is required in URI mode.");
          }
        } else {
          if (!mongoSource.host.trim()) {
            add(STEP_CONFIGURE_SOURCE, "MongoDB host is required in Basic mode.");
          }
          if (!mongoSource.port.trim()) {
            add(STEP_CONFIGURE_SOURCE, "MongoDB port is required in Basic mode.");
          } else if (!parsePort(mongoSource.port)) {
            add(STEP_CONFIGURE_SOURCE, "MongoDB port must be between 1 and 65535.");
          }
        }
        if (!mongoSource.databaseName.trim()) add(STEP_CONFIGURE_SOURCE, "MongoDB database name is required.");
        if (!mongoSource.connectionTimeout.trim() || !parsePositiveInteger(mongoSource.connectionTimeout)) {
          add(STEP_CONFIGURE_SOURCE, "MongoDB connection timeout must be a positive integer.");
        }
      }

      if (sourceType === "smb") {
        if (smbConnectionMode === "application_service") {
          if (!smbApplicationServiceId.trim()) add(STEP_CONFIGURE_SOURCE, "Select an SMB Service.");
        } else if (!smbSource.smbConnection.trim()) {
          add(STEP_CONFIGURE_SOURCE, "SMB server address is required.");
        }
        if (!smbSource.baseDirectoryPath.trim()) add(STEP_CONFIGURE_SOURCE, "SMB base directory path is required.");
        if (smbSource.fileFormat === "CSV" && !smbSource.csvDelimiter.trim()) {
          add(STEP_CONFIGURE_SOURCE, "CSV delimiter is required for CSV format.");
        }
      }

      if (sourceType === "webhook") {
        if (!webhookSource.sourceName.trim()) add(STEP_CONFIGURE_SOURCE, "Webhook source name is required.");
        if (webhookConnectionMode === "application_service" && !webhookApplicationServiceId.trim()) {
          add(STEP_CONFIGURE_SOURCE, "Select a Webhook Service.");
        }
        if (!/^[a-z0-9][a-z0-9._-]{2,127}$/.test(webhookSource.endpointId.trim().toLowerCase())) {
          add(STEP_CONFIGURE_SOURCE, "Webhook endpoint ID must match [a-z0-9][a-z0-9._-]{2,127}.");
        }
        if (webhookConnectionMode === "manual" && !webhookSource.signatureHeader.trim()) {
          add(STEP_CONFIGURE_SOURCE, "Webhook signature header is required.");
        }
      }

      if (sourceType === "trino") {
        if (!trinoSource.endpoint.trim()) {
          add(STEP_CONFIGURE_SOURCE, "Trino endpoint is required.");
        } else if (!isLikelyHttpUrl(trinoSource.endpoint)) {
          add(STEP_CONFIGURE_SOURCE, "Trino endpoint must be a valid http/https URL.");
        }
        if (!trinoSource.catalog.trim()) add(STEP_CONFIGURE_SOURCE, "Trino catalog is required.");
        if (!trinoSource.schema.trim()) add(STEP_CONFIGURE_SOURCE, "Trino schema is required.");
        if (!trinoSource.table.trim()) add(STEP_CONFIGURE_SOURCE, "Trino table is required.");
        if (!trinoSource.checkpointCatalog.trim()) add(STEP_CONFIGURE_SOURCE, "Checkpoint catalog is required.");
        if (!trinoSource.checkpointSchema.trim()) add(STEP_CONFIGURE_SOURCE, "Checkpoint schema is required.");
        if (!trinoSource.checkpointTable.trim()) add(STEP_CONFIGURE_SOURCE, "Checkpoint table is required.");
        const sourceTableName = [trinoSource.catalog, trinoSource.schema, trinoSource.table].map((item) => item.trim()).join(".");
        const checkpointTableName = [trinoSource.checkpointCatalog, trinoSource.checkpointSchema, trinoSource.checkpointTable].map((item) => item.trim()).join(".");
        if (sourceTableName && checkpointTableName && sourceTableName === checkpointTableName) {
          add(STEP_CONFIGURE_SOURCE, "Checkpoint table cannot be the same as the Iceberg source table.");
        }
        if (trinoSource.columnMode === "manual") {
          const columns = parseTrinoColumns(trinoSource.columns);
          if (columns.length === 0) {
            add(STEP_CONFIGURE_SOURCE, "At least one Trino output column is required.");
          }
          columns.forEach((column) => {
            if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(column)) {
              add(STEP_CONFIGURE_SOURCE, `Trino output column "${column}" must be a simple column name.`);
            }
          });
        }
      }
    }

    if (shouldCheck(STEP_SCHEDULE)) {
      if (schedule.type === "interval") {
        if (schedule.interval.value === "") {
          add(STEP_SCHEDULE, "Schedule interval value is required.");
        } else {
          const interval = Number(schedule.interval.value);
          if (!Number.isFinite(interval) || interval <= 0) {
            add(STEP_SCHEDULE, "Schedule interval value must be a positive number.");
          }
        }
      } else if (!schedule.cron.expression.trim()) {
        add(STEP_SCHEDULE, "Cron expression is required.");
      } else if (!isLikelyCronExpression(schedule.cron.expression)) {
        add(STEP_SCHEDULE, "Cron expression looks invalid. Use at least 5 fields.");
      }
    }

    if (shouldCheck(STEP_STREAMS)) {
      if (streams.length === 0) add(STEP_STREAMS, "At least one stream is required.");
      if (sourceType === "webhook" && streams.length !== 1) {
        add(STEP_STREAMS, "Webhook source must contain exactly one stream.");
      }
      const seenStreamNames = new Set<string>();
      streams.forEach((stream) => {
        const streamName = stream.name.trim();
        const streamLabel = streamName || "Unnamed stream";
        if (!streamName) {
          add(STEP_STREAMS, "Each stream must have a name.");
        } else {
          const normalizedName = streamName.toLowerCase();
          if (seenStreamNames.has(normalizedName)) {
            add(STEP_STREAMS, `Stream name "${streamName}" is duplicated.`);
          }
          seenStreamNames.add(normalizedName);
        }

        if (sourceType === "mongo") {
          if (!stream.endpointPath.trim()) {
            add(STEP_STREAMS, `${streamLabel}: collection name is required.`);
          }
          if (!stream.mongoQuery.trim()) {
            add(STEP_STREAMS, `${streamLabel}: query JSON is required.`);
          } else {
            for (const err of validateMongoJsonTemplate(`${streamLabel} query`, stream.mongoQuery)) {
              add(STEP_STREAMS, err);
            }
          }
          if (stream.mongoProjection.trim()) {
            for (const err of validateMongoJsonTemplate(`${streamLabel} projection`, stream.mongoProjection)) {
              add(STEP_STREAMS, err);
            }
          }
          if (stream.mongoSort.trim()) {
            for (const err of validateMongoJsonTemplate(`${streamLabel} sort`, stream.mongoSort)) {
              add(STEP_STREAMS, err);
            }
          }
          if (stream.mongoLimit.trim() && !parsePositiveInteger(stream.mongoLimit)) {
            add(STEP_STREAMS, `${streamLabel}: limit must be a positive integer.`);
          }
        } else if (sourceType === "rest") {
          const routeParentEdge = (streamGraph.incomingByChild.get(stream.id) || []).find(
            (edge) => edge.kind === "route" || edge.kind === "default_route",
          );
          const isRouteChildStream = Boolean(routeParentEdge);
          const streamMakesRequest = !isRouteChildStream || stream.requestEnabled;
          const endpointPath = stream.endpointPath.trim();
          if (streamMakesRequest && !endpointPath) {
            add(STEP_STREAMS, `${streamLabel}: endpoint path is required.`);
          } else if (endpointPath && !endpointPath.startsWith("/")) {
            add(STEP_STREAMS, `${streamLabel}: endpoint path should start with "/".`);
          }
          const parentIdForRequest = stream.parentStreamId || routeParentEdge?.parentId || "";
          if (streamMakesRequest && parentIdForRequest && endpointPath) {
            const parentAttrs = new Set<string>();
            const seenParents = new Set<string>();
            let cursorParentId = parentIdForRequest;
            while (cursorParentId && !seenParents.has(cursorParentId)) {
              seenParents.add(cursorParentId);
              const parent = streams.find((candidate) => candidate.id === cursorParentId);
              if (!parent) break;
              (parent.attributeExtractions || []).forEach((rule) => {
                const name = toAttributeName(rule.attributeName).toLowerCase();
                if (name) parentAttrs.add(name);
              });
              const parentRouteEdge = (streamGraph.incomingByChild.get(parent.id) || []).find(
                (edge) => edge.kind === "route" || edge.kind === "default_route",
              );
              cursorParentId = parent.parentStreamId || parentRouteEdge?.parentId || "";
            }
            const placeholders: string[] = [];
            PLACEHOLDER_PATTERN.lastIndex = 0;
            let match: RegExpExecArray | null;
            while ((match = PLACEHOLDER_PATTERN.exec(endpointPath)) !== null) {
              const variable = (match[1] ?? "").trim();
              if (!variable) continue;
              if (!placeholders.includes(variable)) placeholders.push(variable);
            }
            const unresolved = placeholders.filter((variable) => {
              const normalized = toAttributeName(variable).toLowerCase();
              if (parentAttrs.has(normalized)) return false;
              return !(stream.pathParamDefaults?.[variable] || "").trim();
            });
            if (unresolved.length > 0) {
              add(
                STEP_STREAMS,
                `${streamLabel}: missing child path fallback defaults for ${unresolved.join(", ")} (or extract same-named parent attributes).`,
              );
            }
          }
        }

        if (sourceType === "webhook") {
          if (stream.parentStreamId) add(STEP_STREAMS, `${streamLabel}: webhook stream cannot depend on a parent stream.`);
          if (stream.paginationEnabled || stream.paginationType !== "NONE") {
            add(STEP_STREAMS, `${streamLabel}: webhook stream does not support pagination.`);
          }
          if (stream.responseFormat === "XML" && stream.splitResponseIntoRecords) {
            const splitDepth = parsePositiveInteger(stream.xmlSplitDepth);
            if (!splitDepth) {
              add(STEP_STREAMS, `${streamLabel}: XML split depth must be a positive integer.`);
            } else if (splitDepth > 10) {
              add(STEP_STREAMS, `${streamLabel}: XML split depth cannot exceed 10.`);
            }
            if (!stream.xmlRecordElement.trim()) {
              add(STEP_STREAMS, `${streamLabel}: XML record element name is required when splitting XML.`);
            }
          }
          if (stream.splitResponseIntoRecords && stream.waitForAllRecordsBeforeDownstream) {
            add(
              STEP_STREAMS,
              `${streamLabel}: split response and wait for all records cannot both be enabled.`,
            );
          }
        }

        if (sourceType === "rest") {
          const invalidHeaderLines = findInvalidKeyValueLines(stream.additionalHeaders);
          if (invalidHeaderLines.length > 0) {
            add(STEP_STREAMS, `${streamLabel}: additional headers contain invalid lines (use key=value or key:value).`);
          }
          const invalidQueryLines = findInvalidKeyValueLines(stream.additionalQueryParams);
          if (invalidQueryLines.length > 0) {
            add(STEP_STREAMS, `${streamLabel}: additional query params contain invalid lines (use key=value or key:value).`);
          }
          if (stream.requestBodyTemplate.trim() && stream.bodyFormat === "EMPTY") {
            add(STEP_STREAMS, `${streamLabel}: choose JSON, XML, or CSV as the request body format, or clear the body template.`);
          }
          if (stream.responseFormat === "CSV") {
            if (!stream.csvDelimiter.trim()) {
              add(STEP_STREAMS, `${streamLabel}: CSV delimiter is required.`);
            }
            if (stream.csvQuoteChar.trim() && stream.csvQuoteChar.trim().length !== 1) {
              add(STEP_STREAMS, `${streamLabel}: CSV quote character must be a single character.`);
            }
            if (stream.csvEscapeChar.trim() && stream.csvEscapeChar.trim().length !== 1) {
              add(STEP_STREAMS, `${streamLabel}: CSV escape character must be a single character.`);
            }
          }
          (stream.openApiParamBindings || []).forEach((binding) => {
            if (!binding.enabled && !binding.required) return;
            if (binding.required && !binding.enabled) {
              add(STEP_STREAMS, `${streamLabel}: required parameter "${binding.paramName}" must be enabled.`);
              return;
            }
            if (binding.paramIn === "query" && binding.enabled && !binding.staticValue.trim() && binding.required) {
              add(STEP_STREAMS, `${streamLabel}: query parameter "${binding.paramName}" requires a value.`);
            }
          });
        }

        if (sourceType === "rest" && stream.paginationEnabled) {
          if (stream.paginationType === "NONE") {
            add(STEP_STREAMS, `${streamLabel}: choose a pagination type.`);
          }
          // max_pages is a precaution, not a stop condition. It is REQUIRED only when the stop
          // condition can loop forever if API metadata is missing at runtime (total count / has more).
          // Cursor, next-url and empty-page have their own terminator, so max_pages is optional there.
          const paginationHasIntrinsicTerminator =
            stream.paginationType === "CURSOR" ||
            stream.paginationType === "NEXT_URL" ||
            (stream.paginationType === "PAGE_INCREMENT" && stream.pageStopCondition === "EMPTY_ARRAY") ||
            (stream.paginationType === "OFFSET_LIMIT" && stream.offsetStopCondition === "EMPTY_ARRAY");
          if (stream.maxPages.trim() && !parsePositiveInteger(stream.maxPages)) {
            add(STEP_STREAMS, `${streamLabel}: max pages must be a positive integer.`);
          }
          if (!paginationHasIntrinsicTerminator && !parsePositiveInteger(stream.maxPages)) {
            add(
              STEP_STREAMS,
              `${streamLabel}: a safety limit (max pages) is required for this stop condition, because the API metadata may be missing at runtime.`,
            );
          }

          // A metadata field is required only from the source the user actually picked.
          const requireHasMoreSource = () => {
            if (stream.hasMoreSource === "BODY" && !stream.hasMorePath.trim()) {
              add(STEP_STREAMS, `${streamLabel}: has more path is required when the flag comes from the response body.`);
            }
            if (stream.hasMoreSource === "HEADER" && !stream.hasMoreHeader.trim()) {
              add(STEP_STREAMS, `${streamLabel}: has more header name is required when the flag comes from a response header.`);
            }
          };

          if (stream.paginationType === "PAGE_INCREMENT") {
            if (!stream.pageParamName.trim()) add(STEP_STREAMS, `${streamLabel}: page parameter name is required.`);
            if (!stream.pageSizeParamName.trim()) add(STEP_STREAMS, `${streamLabel}: page size parameter name is required.`);
            if (!parsePositiveInteger(stream.pageSizeValue)) add(STEP_STREAMS, `${streamLabel}: page size value must be a positive integer.`);
            if (stream.firstPageNumber.trim() && parseNonNegativeInteger(stream.firstPageNumber) === null) {
              add(STEP_STREAMS, `${streamLabel}: first page number must be zero or a positive integer.`);
            }
            if (stream.pageStopCondition === "TOTAL_COUNT") {
              if (stream.totalCountSource === "BODY" && !stream.totalCountPath.trim()) {
                add(STEP_STREAMS, `${streamLabel}: total count path is required when the total count comes from the response body.`);
              }
              if (stream.totalCountSource === "HEADER" && !stream.totalCountHeader.trim()) {
                add(STEP_STREAMS, `${streamLabel}: total count header name is required when the total count comes from a response header.`);
              }
            }
            if (stream.pageStopCondition === "HAS_MORE") requireHasMoreSource();
          }
          if (stream.paginationType === "CURSOR") {
            if (!stream.cursorParamName.trim()) add(STEP_STREAMS, `${streamLabel}: cursor parameter name is required.`);
            if (stream.cursorSource === "BODY" && !stream.cursorValuePath.trim()) {
              add(STEP_STREAMS, `${streamLabel}: cursor value path is required when the token comes from the response body.`);
            }
            if (stream.cursorSource === "HEADER" && !stream.cursorHeader.trim()) {
              add(STEP_STREAMS, `${streamLabel}: cursor header name is required when the token comes from a response header.`);
            }
            if (stream.cursorPageSizeValue.trim() && !parsePositiveInteger(stream.cursorPageSizeValue)) {
              add(STEP_STREAMS, `${streamLabel}: cursor page size must be a positive integer.`);
            }
          }
          if (stream.paginationType === "NEXT_URL") {
            if (stream.nextUrlSource === "BODY" && !stream.nextUrlPath.trim()) {
              add(STEP_STREAMS, `${streamLabel}: next URL path is required when the link comes from the response body.`);
            }
            if (stream.nextUrlSource !== "BODY" && !stream.nextUrlHeader.trim()) {
              add(STEP_STREAMS, `${streamLabel}: next URL header name is required when the link comes from a response header.`);
            }
          }
          if (stream.paginationType === "OFFSET_LIMIT") {
            if (!stream.offsetParamName.trim()) add(STEP_STREAMS, `${streamLabel}: offset parameter name is required.`);
            if (!stream.limitParamName.trim()) add(STEP_STREAMS, `${streamLabel}: limit parameter name is required.`);
            if (!parsePositiveInteger(stream.limitValue)) add(STEP_STREAMS, `${streamLabel}: limit value must be a positive integer.`);
            if (stream.offsetStopCondition === "TOTAL_COUNT") {
              if (stream.offsetTotalCountSource === "BODY" && !stream.offsetTotalCountPath.trim()) {
                add(STEP_STREAMS, `${streamLabel}: total count path is required when the total count comes from the response body.`);
              }
              if (stream.offsetTotalCountSource === "HEADER" && !stream.offsetTotalCountHeader.trim()) {
                add(STEP_STREAMS, `${streamLabel}: total count header name is required when the total count comes from a response header.`);
              }
            }
            if (stream.offsetStopCondition === "HAS_MORE") requireHasMoreSource();
          }

          // Request-body pagination — mirrors the backend rules in
          // backend/services/rest_stream_pagination_validation.py so the UI never lets a user
          // build a config the generator will reject.
          if (stream.paginationLocation === "BODY") {
            if (stream.paginationType === "NEXT_URL") {
              add(STEP_STREAMS, `${streamLabel}: next URL pagination cannot send values in the request body.`);
            }
            if (!["POST", "PUT", "PATCH"].includes(stream.method)) {
              add(STEP_STREAMS, `${streamLabel}: request-body pagination requires method POST, PUT or PATCH.`);
            }
            if (stream.bodyFormat !== "JSON") {
              add(STEP_STREAMS, `${streamLabel}: request-body pagination requires a JSON body format.`);
            }
          }
        }

        if (sourceType === "rest" && stream.responseFormat === "XML" && stream.splitResponseIntoRecords) {
          const splitDepth = parsePositiveInteger(stream.xmlSplitDepth);
          if (!splitDepth) {
            add(STEP_STREAMS, `${streamLabel}: XML split depth must be a positive integer.`);
          } else if (splitDepth > 10) {
            add(STEP_STREAMS, `${streamLabel}: XML split depth cannot exceed 10.`);
          }
          if (!stream.xmlRecordElement.trim()) {
            add(STEP_STREAMS, `${streamLabel}: XML record element name is required when splitting XML.`);
          }
        }
        if (sourceType === "rest" && stream.splitResponseIntoRecords && stream.waitForAllRecordsBeforeDownstream) {
          add(STEP_STREAMS, `${streamLabel}: split response and wait for all records cannot both be enabled.`);
        }

        stream.attributeExtractions.forEach((rule, index) => {
          const hasAttribute = Boolean(rule.attributeName.trim());
          const hasPath = Boolean(rule.pathExpression.trim());
          if (!hasAttribute && !hasPath) return;
          const ruleLabel = `${streamLabel}: extraction rule ${index + 1}`;
          if (!hasAttribute || !hasPath) {
            add(STEP_STREAMS, `${ruleLabel} must include both attribute name and path.`);
          }
          if (hasAttribute && !SAFE_ATTRIBUTE_NAME_PATTERN.test(rule.attributeName.trim())) {
            add(STEP_STREAMS, `${ruleLabel} attribute name must be alphanumeric with underscores only.`);
          }
        });

        stream.transformations.forEach((rule, index) => {
          const hasAnyData = Boolean(rule.fieldName.trim() || rule.staticValue.trim() || rule.attributeName.trim());
          if (!hasAnyData) return;
          const ruleLabel = `${streamLabel}: transformation rule ${index + 1}`;
          if (!rule.fieldName.trim()) {
            add(STEP_STREAMS, `${ruleLabel} must set a field name.`);
          }
          if (rule.action === "SET_FROM_ATTRIBUTE" && !rule.attributeName.trim()) {
            add(STEP_STREAMS, `${ruleLabel} must set an extracted attribute.`);
          }
        });

        stream.routingRules.forEach((rule, index) => {
          const hasAnyData = Boolean(
            rule.attributeName.trim() || rule.value.trim() || rule.targetStreamId.trim(),
          );
          if (!hasAnyData) return;
          const ruleLabel = `${streamLabel}: routing rule ${index + 1}`;
          if (!rule.attributeName.trim()) add(STEP_STREAMS, `${ruleLabel} must set an attribute name.`);
          if (rule.condition !== "is_empty" && !rule.value.trim()) {
            add(STEP_STREAMS, `${ruleLabel} must set a comparison value.`);
          }
          if (rule.destination === "ROUTE_TO_STREAM") {
            if (!rule.targetStreamId) {
              add(STEP_STREAMS, `${ruleLabel} must select a target child stream.`);
            } else if (!streams.some((candidate) => candidate.id === rule.targetStreamId)) {
              add(STEP_STREAMS, `${ruleLabel} target stream does not exist.`);
            } else if (rule.targetStreamId === stream.id) {
              add(STEP_STREAMS, `${ruleLabel} cannot route a stream to itself.`);
            }
          }
        });

        const hasRouteToStream = (stream.routingRules || []).some(
          (rule) => rule.destination === "ROUTE_TO_STREAM" && isConfiguredRoutingRule(rule),
        );
        if (hasRouteToStream && stream.defaultRouteAction === "ROUTE_TO_STREAM") {
          if (!stream.defaultRouteTargetStreamId) {
            add(STEP_STREAMS, `${streamLabel}: default route must select a target stream.`);
          } else if (!streams.some((candidate) => candidate.id === stream.defaultRouteTargetStreamId)) {
            add(STEP_STREAMS, `${streamLabel}: default route target stream does not exist.`);
          } else if (stream.defaultRouteTargetStreamId === stream.id) {
            add(STEP_STREAMS, `${streamLabel}: default route cannot target the same stream.`);
          }
        }
      });

      streamGraph.incomingByChild.forEach((incoming, childId) => {
        if (incoming.length <= 1) return;
        const childName = streamGraph.byId.get(childId)?.name || childId;
        add(STEP_STREAMS, `${childName}: a stream cannot have more than one parent or converging route.`);
      });

      for (const stream of streams) {
        if (stream.defaultRouteAction === "KEEP") {
          const routeChildren = (streamGraph.childrenByParent.get(stream.id) || []).filter(
            (edge) => edge.kind === "route" || edge.kind === "default_route",
          );
          if (routeChildren.length > 0) {
            add(
              STEP_STREAMS,
              `${stream.name || "Unnamed stream"}: keep-unmatched would make the routing parent publish while it has child streams. Route to a default child stream or drop unmatched records.`,
            );
          }
        }
      }

      const visiting = new Set<string>();
      const visited = new Set<string>();
      const hasCycleFrom = (streamId: string): boolean => {
        if (visiting.has(streamId)) return true;
        if (visited.has(streamId)) return false;
        visiting.add(streamId);
        for (const edge of streamGraph.childrenByParent.get(streamId) || []) {
          if (hasCycleFrom(edge.childId)) return true;
        }
        visiting.delete(streamId);
        visited.add(streamId);
        return false;
      };
      if (streams.some((stream) => hasCycleFrom(stream.id))) {
        add(STEP_STREAMS, "Stream routing graph contains a cycle. Routing must only diverge.");
      }
    }

    if (shouldCheck(STEP_DESTINATION)) {
      if (entityDestinationSummaries.length === 0) {
        add(STEP_DESTINATION, "Mark at least one stream as an entity output before deployment.");
      }
      for (const entity of entityDestinationSummaries) {
        const label = entity.stream.name || "Unnamed entity";
        const partitions = parsePositiveInteger(entity.draft.partitions);
        if (!partitions) add(STEP_DESTINATION, `${label}: Kafka partitions must be a positive integer.`);
        const replicationFactor = parsePositiveInteger(entity.draft.replicationFactor);
        if (!replicationFactor) add(STEP_DESTINATION, `${label}: replication factor must be a positive integer.`);
        if (partitions && replicationFactor && replicationFactor > partitions) {
          add(STEP_DESTINATION, `${label}: replication factor cannot be greater than partition count.`);
        }
        if (!entity.topic) add(STEP_DESTINATION, `${label}: generated Kafka topic is missing.`);
        if (entity.draft.keyStrategy === "primary_key") {
          const keyField = entity.stream.primaryKeyFields?.split(",").map((value) => value.trim()).find(Boolean);
          if (!keyField) {
            add(STEP_DESTINATION, `${label}: select one extracted attribute as Kafka key, or set key strategy to None.`);
          }
        }
        if (schemaWorkflowRequired && entity.draft.schemaMode === "existing") {
          if (!entity.draft.existingSchemaArtifactId && !entity.artifact) {
            add(STEP_DESTINATION, `${label}: select a schema artifact.`);
          } else if (!entity.artifact) {
            add(STEP_DESTINATION, `${label}: expected schema artifact ${entity.schemaArtifactId} was not found.`);
          } else if (!entity.draft.existingSchemaVersion) {
            add(STEP_DESTINATION, `${label}: select a schema version.`);
          } else if (!entity.selectedVersion) {
            add(STEP_DESTINATION, `${label}: selected schema version does not exist.`);
          } else if (entity.selectedVersion.status !== "Verified") {
            add(STEP_DESTINATION, `${label}: selected schema version is not verified.`);
          }
        }
      }
    }

    return issues;
  };

  const collectRequiredInferenceRuntimeParams = (): InferenceRuntimeParam[] => {
    if (sourceType !== "rest") return [];
    const params: InferenceRuntimeParam[] = [];
    const seen = new Set<string>();
    for (const stream of streams) {
      const streamDefaults = stream.pathParamDefaults || {};
      for (const binding of stream.openApiParamBindings || []) {
        if (!binding.enabled || !binding.required) continue;
        if (binding.valueSource !== "manual_runtime") continue;
        const key = (binding.runtimeVarName || binding.paramName).trim();
        if (!key || seen.has(key)) continue;
        const fromRuntimeName = (streamDefaults[key] || "").trim();
        const fromParamName = (streamDefaults[binding.paramName] || "").trim();
        if (fromRuntimeName || fromParamName) continue;
        seen.add(key);
        params.push({
          key,
          streamName: stream.name || "stream",
          paramName: binding.paramName,
          description: binding.description || "",
        });
      }
    }
    return params;
  };

  const collectMissingPathParamResolutionsForInference = (
    entityStreamId?: string,
  ): Array<{ streamName: string; params: string[] }> =>
    collectInferencePreflightIssues({
      sourceType,
      streams,
      selectedEntityStreamId: entityStreamId,
    });

  const blockedAutoInferenceMethodFor = (entityStreamId?: string): { streamName: string; method: StreamMethod } | null => {
    if (sourceType !== "rest") return null;
    const target =
      (entityStreamId ? streams.find((stream) => stream.id === entityStreamId) : null) ||
      streams.find((stream) => stream.isPrimary || Boolean(stream.entity));
    if (!target) return null;
    if (target.method === "PUT" || target.method === "PATCH" || target.method === "DELETE") {
      return { streamName: target.name || "stream", method: target.method };
    }
    return null;
  };

  const startInferenceWithRuntime = async (
    flowId: string,
    runtimeValues: Record<string, string> = {},
    entityStreamId?: string,
  ) => {
    if (inferenceBlockReason) {
      toast.error(inferenceBlockReason);
      return false;
    }
    if (entityStreamId) {
      setInferenceStartingByEntity((prev) => ({ ...prev, [entityStreamId]: true }));
    } else {
      setInferenceStarting(true);
    }
    try {
      const inferenceTargetStream = entityStreamId ? streams.find((stream) => stream.id === entityStreamId) : null;
      const targetMessages = sourceType === "rest" && inferenceTargetStream?.method === "POST" ? 1 : 10;
      const job = await inferenceApi.start(flowId, targetMessages, runtimeValues, entityStreamId);
      setInferenceJobId(job.id);
      setInferenceJob(job);
      if (entityStreamId) {
        setInferenceJobsByEntity((prev) => ({ ...prev, [entityStreamId]: job }));
      }
      toast.info(`Schema inference started. Topic: ${job.inference_topic}`);
      return true;
    } catch (e) {
      const err = e as Error;
      if ((e as ApiError)?.status === 422 && /Missing required runtime params for inference/i.test(err.message || "")) {
        const params = collectRequiredInferenceRuntimeParams();
        if (params.length > 0) {
          const initialValues: Record<string, string> = {};
          for (const item of params) {
            initialValues[item.key] = (runtimeValues[item.key] || inferenceRuntimeValues[item.key] || "").trim();
          }
          setPendingInferenceStart({ flowId, entityStreamId, params, values: initialValues });
          toast.error("Provide required runtime values to run schema inference.");
        } else {
          toast.error(`Failed to start inference: ${err.message}`);
        }
      } else {
        toast.error(`Failed to start inference: ${err.message}`);
      }
      return false;
    } finally {
      if (entityStreamId) {
        setInferenceStartingByEntity((prev) => ({ ...prev, [entityStreamId]: false }));
      } else {
        setInferenceStarting(false);
      }
    }
  };

  const triggerSchemaInferenceStart = async (flowId: string, entityStreamId?: string) => {
    const blockedMethod = blockedAutoInferenceMethodFor(entityStreamId);
    if (blockedMethod) {
      toast.error(
        `Automatic schema inference is not available for ${blockedMethod.method} stream "${blockedMethod.streamName}". Use an existing schema.`,
      );
      return false;
    }

    const missingDefaults = collectMissingPathParamResolutionsForInference(entityStreamId);
    if (missingDefaults.length > 0) {
      const detail = missingDefaults
        .map((item) => `${item.streamName}: ${item.params.join(", ")}`)
        .join("; ");
      setStep(STEP_STREAMS);
      toast.error(
        `Go back to Streams and resolve path parameters before schema inference. Add defaults or extract same-named parent attributes. Missing: ${detail}`,
      );
      return false;
    }

    const required = collectRequiredInferenceRuntimeParams();
    if (required.length === 0) {
      return startInferenceWithRuntime(flowId, {}, entityStreamId);
    }
    const resolved: Record<string, string> = {};
    const missing: InferenceRuntimeParam[] = [];
    const streamDefaults = streams.reduce<Record<string, string>>((acc, stream) => {
      for (const [k, v] of Object.entries(stream.pathParamDefaults || {})) {
        const key = (k || "").trim();
        const val = String(v || "").trim();
        if (!key || !val) continue;
        if (!(key in acc)) acc[key] = val;
      }
      return acc;
    }, {});
    for (const item of required) {
      const value = (inferenceRuntimeValues[item.key] || streamDefaults[item.key] || "").trim();
      if (!value) {
        missing.push(item);
      } else {
        resolved[item.key] = value;
      }
    }
    if (missing.length > 0) {
      const initialValues: Record<string, string> = {};
      for (const item of required) {
        initialValues[item.key] = (inferenceRuntimeValues[item.key] || "").trim();
      }
      setPendingInferenceStart({ flowId, entityStreamId, params: required, values: initialValues });
      return false;
    }
    return startInferenceWithRuntime(flowId, resolved, entityStreamId);
  };

  const handleSaveAndContinue = async () => {
    if (isSaving) return;
    const validationIssues = validateDesignerState();
    if (validationIssues.length > 0) {
      setStep(validationIssues[0].step);
      toast.error(formatValidationToast(validationIssues));
      return;
    }
    const flowName = editingFlow?.name || sourceName.trim() || "unnamed_source";
    const primaryStreamName = primaryStream?.name || primaryEntityStream?.name || "unknown";
    const primaryStreamId = primaryStream?.id || primaryEntityStream?.id || "";
    let schemaArtifactId = "";
    let schemaVersion: number | null = 1;
    let schemaVersionStatus: SchemaVersionStatus = "Needs Verification";

    if (!schemaWorkflowRequired) {
      schemaArtifactId = "";
      schemaVersion = null;
      schemaVersionStatus = "Verified";
    } else if (kafkaOutput.schemaMode === "existing") {
      const unresolved = entityDestinationSummaries.find(
        (entity) => !entity.selectedVersion || entity.selectedVersion.status !== "Verified",
      );
      if (unresolved) {
        toast.error(`${unresolved.stream.name}: select a verified schema version`);
        return;
      }

      schemaArtifactId = entityDestinationSummaries[0]?.schemaArtifactId || derivedSchemaArtifactId;
      schemaVersion = entityDestinationSummaries[0]?.effectiveVersion?.version || 1;
      schemaVersionStatus = "Verified";
    } else {
      schemaArtifactId = entityDestinationSummaries[0]?.schemaArtifactId || "";
      schemaVersion = entityDestinationSummaries[0]?.effectiveVersion?.version || 0;
      schemaVersionStatus = entityDestinationSummaries[0]?.effectiveStatus || "Needs Verification";
    }

    // Build source payload from current designer state. Persist the resolved
    // schema link in the designer payload so draft flows hydrate with the same
    // artifact/version after the user returns from Schema Manager.
    const apiType = sourceTypeToApiType(sourceType);
    const resolvedKafkaOutput: KafkaOutputConfig = {
      ...kafkaOutput,
      schemaMode: schemaArtifactId ? "existing" : kafkaOutput.schemaMode,
      existingSchemaArtifactId: schemaArtifactId,
      existingSchemaVersion: schemaVersion ? String(schemaVersion) : "",
    };
    const designerPayload = {
      ...buildDesignerPayload(step),
      kafkaOutput: resolvedKafkaOutput,
    } as unknown as Record<string, unknown>;
    const apiStreams = streams.map((s) => {
      const isPassiveStream = sourceType === "smb" || sourceType === "webhook" || sourceType === "trino";
      const supportsResponseFormatForStream = sourceType === "rest" || sourceType === "webhook";
      const compiledRest = sourceType === "rest" ? compileRestStreamFromOpenApiBindings(s) : null;
      const isRouteChildStream = Boolean(
        (streamGraph.incomingByChild.get(s.id) || []).some(
          (edge) => edge.kind === "route" || edge.kind === "default_route",
        ),
      );
      const paginationCfg = buildPaginationCfg(s, isPassiveStream);
      const runtimeRouting = buildRuntimeRoutingForStream(s);
      const extractionRules = (s.attributeExtractions || [])
        .filter((r) => r.attributeName && r.pathExpression)
        .map((r) => ({
          attribute_name: r.attributeName,
          path: r.pathExpression,
          default_value: r.defaultValue || null,
        }));
      const transformationRules = (s.transformations || [])
        .filter((r) => r.fieldName)
        .map((r) => {
          const t = r.action === "ADD_FIELD" ? "add_field" : r.action === "REMOVE_FIELD" ? "remove_field" : "set_from_attribute";
          return {
            type: t,
            field_name: r.fieldName,
            value: r.staticValue || null,
            source_attribute: r.attributeName || null,
          };
        });
      return {
        id: s.id,
        name: s.name,
        endpoint_path:
          sourceType === "mongo"
            ? s.endpointPath
            : sourceType === "rest"
            ? compiledRest?.endpointPath || s.endpointPath
            : "",
        request_enabled: sourceType === "rest" ? (!isRouteChildStream || s.requestEnabled) : false,
        method: sourceType === "rest" ? s.method : "GET",
        openapi_operation_id: sourceType === "rest" ? (s.openApiOperationId || null) : null,
        response_data_path: s.responseDataPath || null,
        response_format: supportsResponseFormatForStream ? (s.responseFormat || "JSON").toLowerCase() : "json",
        mongo_query: sourceType === "mongo" ? (s.mongoQuery || "{}") : null,
        mongo_projection: sourceType === "mongo" ? (s.mongoProjection || null) : null,
        mongo_sort: sourceType === "mongo" ? (s.mongoSort || null) : null,
        mongo_limit: sourceType === "mongo" ? (s.mongoLimit ? Number.parseInt(s.mongoLimit, 10) || null : null) : null,
        split_response_records: Boolean(s.splitResponseIntoRecords),
        wait_for_all_records_before_downstream: sourceType === "rest" ? Boolean(s.waitForAllRecordsBeforeDownstream) : false,
        xml_split_depth: supportsResponseFormatForStream && s.responseFormat === "XML" ? (parseInt(s.xmlSplitDepth, 10) || 1) : null,
        xml_record_element: supportsResponseFormatForStream && s.responseFormat === "XML" ? (s.xmlRecordElement || null) : null,
        csv_delimiter: supportsResponseFormatForStream && s.responseFormat === "CSV" ? (s.csvDelimiter || ",") : null,
        csv_has_header_row: supportsResponseFormatForStream && s.responseFormat === "CSV" ? Boolean(s.csvHasHeaderRow) : null,
        csv_quote_char: supportsResponseFormatForStream && s.responseFormat === "CSV" ? (s.csvQuoteChar || "\"") : null,
        csv_escape_char: supportsResponseFormatForStream && s.responseFormat === "CSV" ? (s.csvEscapeChar || "\\") : null,
        csv_encoding: supportsResponseFormatForStream && s.responseFormat === "CSV" ? "utf-8" : null,
        csv_test_record_limit: null,
        primary_key_fields: s.primaryKeyFields ? s.primaryKeyFields.split(",").map((x) => x.trim()).filter(Boolean) : [],
        is_primary: s.isPrimary,
        headers: sourceType === "rest" ? parseKeyValueLines(s.additionalHeaders) : {},
        query_params: sourceType === "rest" ? compiledRest?.queryParams || parseKeyValueLines(s.additionalQueryParams) : {},
        body_format: sourceType === "rest" ? (s.bodyFormat || "EMPTY").toLowerCase() : "empty",
        body_template: sourceType === "rest" ? (s.requestBodyTemplate || null) : null,
        path_param_defaults: sourceType === "rest" ? (s.pathParamDefaults || {}) : {},
        pagination: paginationCfg,
        fan_out: {
          enabled:
            sourceType === "rest"
              ? Boolean(compiledRest?.inferredParentStreamId || s.parentStreamId)
              : sourceType === "mongo"
              ? Boolean(s.parentStreamId)
              : false,
          parent_stream_id:
            sourceType === "rest"
              ? (compiledRest?.inferredParentStreamId || s.parentStreamId || null)
              : sourceType === "mongo"
              ? (s.parentStreamId || null)
              : null,
          completion_mode:
            sourceType === "rest" || sourceType === "mongo"
              ? "per_record"
              : "per_record",
          parent_field:
            sourceType === "rest"
              ? (compiledRest?.inferredParentField || s.parentField || null)
              : sourceType === "mongo"
              ? (s.parentField || s.injectFieldName || null)
              : null,
          inject_as:
            sourceType === "rest"
              ? ((compiledRest?.inferredInjectField || s.injectFieldName)
                  ? (compiledRest?.inferredInjectAs || s.injectAs || "PATH_PARAM").toLowerCase()
                  : null)
              : null,
          inject_field_name:
            sourceType === "rest"
              ? (compiledRest?.inferredInjectField || s.injectFieldName || null)
              : null,
          inject_template: sourceType === "rest" ? (s.injectTemplate || null) : null,
        },
        parameter_bindings: sourceType === "rest" ? compiledRest?.parameterBindingsPayload || [] : [],
        extraction_rules: extractionRules,
        transformation_rules: transformationRules,
        routing_rules: runtimeRouting.legacyRoutingRules,
        filters: runtimeRouting.filters,
        routes: runtimeRouting.routes,
        default_route_child_stream_id: runtimeRouting.defaultRouteChildStreamId,
        default_route_action: runtimeRouting.defaultRouteAction,
        route_source: runtimeRouting.routeSource,
        entity: runtimeRouting.entity,
      };
    });

    const sourcePayload: Record<string, unknown> = {
      name: flowName,
      type: apiType,
      designer_payload: designerPayload,
      streams: apiStreams,
      schedule: {
        type: schedule.type,
        interval_value: schedule.interval.value === "" ? 15 : schedule.interval.value,
        interval_unit: schedule.interval.unit,
        cron_expression: schedule.cron.expression,
      },
      kafka_output: {
        topic: entityDestinationSummaries[0]?.topic || derivedKafkaTopic,
        partitions: Number.parseInt(kafkaOutput.partitions, 10) || 6,
        replication_factor: Number.parseInt(kafkaOutput.replicationFactor, 10) || 3,
        key_strategy: normalizeKafkaKeyStrategy(kafkaOutput.keyStrategy),
        value_format: "avro",
        schema_mode: resolvedKafkaOutput.schemaMode,
        schema_artifact_id: schemaArtifactId || null,
        schema_version: schemaVersion,
      },
    };

    if (sourceType === "rest") {
      Object.assign(sourcePayload, {
        connection_setup_mode: restConnectionMode,
        application_service_id: restConnectionMode === "application_service" ? restApplicationServiceId : null,
        openapi_spec_id: restSource.openApiSpecId || null,
        openapi_selected_server_url: restSource.openApiSelectedServerUrl || null,
        ...(restConnectionMode === "manual"
          ? {
              base_url: restSource.baseUrl,
              auth_type: restSource.authType,
              api_key_name: restSource.apiKeyName || null,
              api_key_value: restSource.apiKeyValue || null,
              api_key_location: restSource.apiKeyLocation,
              bearer_token: restSource.bearerToken || null,
              rest_username: restSource.username || null,
              rest_password: restSource.password || null,
              connection_timeout: Number.parseInt(restSource.connectionTimeout, 10) || 30,
              read_timeout: Number.parseInt(restSource.socketReadTimeout, 10) || 30,
              write_timeout: Number.parseInt(restSource.socketWriteTimeout, 10) || 30,
              idle_timeout: Number.parseInt(restSource.socketIdleTimeout, 10) || 30,
              rate_limit: Number.parseInt(restSource.rateLimitPerMin, 10) || 120,
            }
          : {}),
      });
    } else if (sourceType === "postgres" || sourceType === "mongo") {
      const dbCfg = sourceType === "postgres" ? postgresSource : mongoSource;
      const mongoPrimary = sourceType === "mongo" ? (streams.find((stream) => stream.isPrimary) || streams[0]) : null;
      Object.assign(sourcePayload, {
        db_host: sourceType === "mongo" && dbCfg.connectionMode !== "basic" ? null : dbCfg.host,
        db_port:
          sourceType === "mongo" && dbCfg.connectionMode !== "basic"
            ? null
            : Number.parseInt(dbCfg.port, 10) || (sourceType === "postgres" ? 5432 : 27017),
        db_database: dbCfg.databaseName,
        db_username: dbCfg.username,
        db_password: dbCfg.password || null,
        db_ssl_mode: dbCfg.sslMode,
        db_connection_timeout: Number.parseInt(dbCfg.connectionTimeout, 10) || 30,
      });
      if (sourceType === "mongo") {
        Object.assign(sourcePayload, {
          mongo_connection_mode: dbCfg.connectionMode || "basic",
          db_connection_uri: dbCfg.connectionMode === "uri" ? dbCfg.connectionUri || null : null,
          nifi_service_refs: dbCfg.connectionMode === "nifi_global_service" ? { mongodb: dbCfg.nifiGlobalServiceId } : {},
          // Legacy fallback fields retained for compatibility with older flow readers.
          db_query: mongoPrimary?.mongoQuery || "{}",
          db_projection: mongoPrimary?.mongoProjection || null,
          db_sort: mongoPrimary?.mongoSort || null,
          db_limit: mongoPrimary?.mongoLimit ? Number.parseInt(mongoPrimary.mongoLimit, 10) || null : null,
        });
      }
    } else if (sourceType === "smb") {
      Object.assign(sourcePayload, {
        connection_setup_mode: smbConnectionMode,
        application_service_id: smbConnectionMode === "application_service" ? smbApplicationServiceId : null,
        ...(smbConnectionMode === "manual"
          ? {
              smb_hostname: smbSource.smbConnection,
              smb_username: smbSource.username || null,
              smb_domain: smbSource.domain || null,
              smb_password: smbSource.password || null,
              smb_share: smbSource.shareName || null,
            }
          : {}),
        smb_base_directory: smbSource.baseDirectoryPath,
        smb_file_filter: smbSource.fileFilterPattern,
        smb_file_format: smbSource.fileFormat,
        smb_csv_delimiter: smbSource.fileFormat === "CSV" ? smbSource.csvDelimiter || "," : null,
        smb_has_header_row: smbSource.fileFormat === "CSV" ? smbSource.hasHeaderRow : null,
        smb_inject_org_from_filename: false,
        smb_org_filename_delimiter: null,
      });
    } else if (sourceType === "webhook") {
      Object.assign(sourcePayload, {
        connection_setup_mode: webhookConnectionMode,
        application_service_id: webhookConnectionMode === "application_service" ? webhookApplicationServiceId : null,
        webhook_endpoint_id: webhookSource.endpointId.trim().toLowerCase(),
        ...(webhookConnectionMode === "manual"
          ? {
              webhook_secret: webhookSource.signatureSecret || null,
              webhook_signature_header: webhookSource.signatureHeader || "X-Signature",
              webhook_signature_algo: "hmac-sha256",
            }
          : {}),
        webhook_enabled: webhookSource.enabled,
        webhook_content_type: webhookSource.contentType,
      });
    } else if (sourceType === "trino") {
      const checkpointFullName = [
        trinoSource.checkpointCatalog.trim(),
        trinoSource.checkpointSchema.trim(),
        trinoSource.checkpointTable.trim(),
      ].filter(Boolean).join(".");
      Object.assign(sourcePayload, {
        trino_endpoint: trinoSource.endpoint.trim(),
        trino_user: trinoSource.user.trim() || "admin",
        trino_catalog: trinoSource.catalog.trim(),
        trino_schema: trinoSource.schema.trim(),
        trino_table: trinoSource.table.trim(),
        trino_checkpoint_table: checkpointFullName,
        trino_checkpoint_catalog: trinoSource.checkpointCatalog.trim(),
        trino_checkpoint_schema: trinoSource.checkpointSchema.trim(),
        trino_checkpoint_table_name: trinoSource.checkpointTable.trim(),
        trino_auto_create_checkpoint_table: trinoSource.autoCreateCheckpointTable,
        trino_column_mode: trinoSource.columnMode,
        trino_columns: trinoSource.columnMode === "manual" ? parseTrinoColumns(trinoSource.columns) : [],
        trino_bootstrap_mode: "latest_snapshot",
        trino_output_mode: "one_message_per_row",
        trino_include_snapshot_metadata: true,
      });
    }

    setIsSaving(true);
    try {
      // Upsert the source
      let savedSource: ApiSource;
      if (editingSourceId) {
        savedSource = await sourcesApi.update(editingSourceId, sourcePayload as never);
      } else {
        savedSource = await sourcesApi.create(sourcePayload as never);
        setEditingSourceId(savedSource.id);
      }

      // Build flow payload
      const flowPayload = {
        name: flowName,
        source_id: savedSource.id,
        source_type: apiType,
        primary_stream_id: primaryStreamId,
        kafka_topic: entityDestinationSummaries[0]?.topic || derivedKafkaTopic,
        enabled: editingFlow?.enabled ?? true,
        schema_artifact_id: schemaArtifactId || null,
        schema_version: schemaVersion,
      };

      let savedFlow: ApiFlow;
      if (editingFlow) {
        savedFlow = await flowsApi.update(editingFlow.id, flowPayload);
      } else {
        savedFlow = await flowsApi.create(flowPayload);
        setEditingFlow(savedFlow);
        navigate(`/flow-designer?editFlowId=${encodeURIComponent(savedFlow.id)}`, { replace: true });
      }
      // Final save should make a draft deployable.
      // Keep non-draft states unchanged; only normalize Draft -> Stopped.
      if (savedFlow.state === "Draft") {
        savedFlow = await flowsApi.patch(savedFlow.id, { state: "Stopped" });
      }
      setEditingFlow(savedFlow);
      setSavedFlowId(savedFlow.id);
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["flows"] }),
        queryClient.invalidateQueries({ queryKey: ["dashboard"] }),
      ]);

      if (!schemaWorkflowRequired) {
        toast.success("Flow saved. Trino schema generation is optional for this source.");
        navigate("/flows");
        return;
      }

      if (kafkaOutput.schemaMode === "auto_generate") {
        toast.success("Flow saved. Run schema inference for each entity in the Destination step.");
        setStep(STEP_DESTINATION);
        return;
      }

      const schemaIsVerified = schemaVersionStatus === "Verified";
      if (schemaIsVerified) {
        toast.success(`Flow saved and linked to verified schema version v${schemaVersion}`);
      } else {
        toast.warning(
          "Flow saved. This schema version is not verified yet — please verify it before starting this flow.",
        );
      }
      navigate(schemaIsVerified ? "/flows" : "/schemas");
    } catch (err) {
      toast.error(`Failed to save flow: ${(err as Error).message}`);
    } finally {
      setIsSaving(false);
    }
  };

  return (
    <AppLayout title="Flow Designer" description="Guided wizard to create a new ingestion flow">
      <Card className="mb-6">
        <CardContent className="p-4 overflow-x-auto">
          <ol className="flex items-center gap-2 min-w-max">
            {steps.map((label, index) => (
              <li key={label} className="flex items-center gap-2">
                <button
                  onClick={() => setStep(index)}
                  className={cn(
                    "flex items-center gap-2 rounded-md px-3 py-1.5 text-sm transition-colors",
                    index === step
                      ? "bg-primary text-primary-foreground"
                      : index < step
                      ? "text-success"
                      : "text-muted-foreground hover:bg-muted",
                  )}
                >
                  <span
                    className={cn(
                      "flex h-5 w-5 items-center justify-center rounded-full text-xs font-medium",
                      index === step ? "bg-primary-foreground/20" : index < step ? "bg-success-muted" : "bg-muted",
                    )}
                  >
                    {index < step ? <Check className="h-3 w-3" /> : index + 1}
                  </span>
                  <span className="font-medium whitespace-nowrap">{label}</span>
                </button>
                {index < steps.length - 1 && <div className="h-px w-6 bg-border" />}
              </li>
            ))}
          </ol>
        </CardContent>
      </Card>

      {editingFlow && (
        <div className="mb-4 rounded-md border border-info/30 bg-info-muted p-3 text-sm">
          Editing existing flow <code>{editingFlow.name}</code>. This flow was opened in edit mode because it is
          currently <strong>Stopped</strong>.
        </div>
      )}

      {step === STEP_SOURCE_TYPE && (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Choose a source type</CardTitle>
            <CardDescription>This determines source-specific configuration fields in the next step.</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
              {sourceTypes.map((source) => (
                (() => {
                  const isDisabled = DISABLED_SOURCE_TYPES.includes(source.id);
                  return (
                <button
                  key={source.id}
                  type="button"
                  disabled={isDisabled}
                  onClick={() => {
                    if (!isDisabled) setSourceType(source.id);
                  }}
                  className={cn(
                    "flex items-start gap-3 rounded-lg border p-4 text-left transition-all hover:border-primary/50",
                    isDisabled && "cursor-not-allowed opacity-50 hover:border-border",
                    sourceType === source.id && "border-primary bg-primary-muted ring-1 ring-primary",
                  )}
                >
                  <div className="flex h-10 w-10 items-center justify-center rounded-md bg-card border">
                    <source.icon className="h-5 w-5 text-primary" />
                  </div>
                  <div className="flex-1">
                    <div className="font-medium">{source.name}</div>
                    <div className="text-xs text-muted-foreground mt-0.5">{source.desc}</div>
                    {isDisabled && <div className="text-xs text-muted-foreground mt-1">Temporarily unavailable</div>}
                  </div>
                  {sourceType === source.id && <Check className="h-4 w-4 text-primary" />}
                </button>
                  );
                })()
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {step === STEP_CONFIGURE_SOURCE && (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Configure {sourceTypeMeta.name} source</CardTitle>
            <CardDescription>Fields are specific to the selected source type.</CardDescription>
          </CardHeader>

          {sourceType === "rest" && (
            <CardContent className="grid gap-3 md:grid-cols-2">
              <div className="md:col-span-2">
                <Label>Source Name</Label>
                <Input
                  value={restSource.sourceName}
                  onChange={(event) => setRestSource((prev) => ({ ...prev, sourceName: event.target.value }))}
                />
              </div>
              <div className="md:col-span-2">
                <Label>Connection Setup</Label>
                <Select value={restConnectionMode} onValueChange={(value) => setRestConnectionMode(value as SourceConnectionMode)}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="manual">Manual Setup</SelectItem>
                    <SelectItem value="application_service">HTTP Service</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              {restConnectionMode === "application_service" ? (
                <div className="md:col-span-2">
                  <Label>HTTP Service</Label>
                  <Select value={restApplicationServiceId || "__none__"} onValueChange={(value) => setRestApplicationServiceId(value === "__none__" ? "" : value)}>
                    <SelectTrigger>
                      <SelectValue placeholder="Select HTTP Service" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="__none__">Select service</SelectItem>
                      {restApplicationServices.map((service) => (
                        <SelectItem key={service.id} value={service.id}>
                          {service.name} ({service.health})
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              ) : (
                <>
              <div className="md:col-span-2">
                <Label>Base URL</Label>
                <Input
                  value={restSource.baseUrl}
                  onChange={(event) => setRestSource((prev) => ({ ...prev, baseUrl: event.target.value }))}
                />
              </div>
              <div>
                <Label>Auth Type</Label>
                <Select
                  value={restSource.authType}
                  onValueChange={(value) => setRestSource((prev) => ({ ...prev, authType: value as RestAuthType }))}
                >
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

              <div className="md:col-span-1" />

              {restSource.authType === "BEARER" && (
                <div className="md:col-span-2">
                  <Label>Bearer Token</Label>
                  <Input
                    type="password"
                    value={restSource.bearerToken}
                    onChange={(event) => setRestSource((prev) => ({ ...prev, bearerToken: event.target.value }))}
                  />
                </div>
              )}

              {restSource.authType === "BASIC" && (
                <>
                  <div>
                    <Label>Username</Label>
                    <Input
                      value={restSource.username}
                      onChange={(event) => setRestSource((prev) => ({ ...prev, username: event.target.value }))}
                    />
                  </div>
                  <div>
                    <Label>Password</Label>
                    <Input
                      type="password"
                      value={restSource.password}
                      onChange={(event) => setRestSource((prev) => ({ ...prev, password: event.target.value }))}
                    />
                  </div>
                </>
              )}

              {restSource.authType === "API_KEY" && (
                <>
                  <div>
                    <Label>API Key Name</Label>
                    <Input
                      value={restSource.apiKeyName}
                      onChange={(event) => setRestSource((prev) => ({ ...prev, apiKeyName: event.target.value }))}
                    />
                  </div>
                  <div>
                    <Label>API Key Value</Label>
                    <Input
                      type="password"
                      value={restSource.apiKeyValue}
                      onChange={(event) => setRestSource((prev) => ({ ...prev, apiKeyValue: event.target.value }))}
                    />
                  </div>
                  <div>
                    <Label>API Key Location</Label>
                    <Select
                      value={restSource.apiKeyLocation}
                      onValueChange={(value) => setRestSource((prev) => ({ ...prev, apiKeyLocation: value as ApiKeyLocation }))}
                    >
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

              <div>
                <Label>Connection Timeout (s)</Label>
                <Input
                  type="number"
                  value={restSource.connectionTimeout}
                  onChange={(event) => setRestSource((prev) => ({ ...prev, connectionTimeout: event.target.value }))}
                />
              </div>
              <div>
                <Label>Socket Read Timeout (s)</Label>
                <Input
                  type="number"
                  value={restSource.socketReadTimeout}
                  onChange={(event) => setRestSource((prev) => ({ ...prev, socketReadTimeout: event.target.value }))}
                />
              </div>
              <div>
                <Label>Socket Write Timeout (s)</Label>
                <Input
                  type="number"
                  value={restSource.socketWriteTimeout}
                  onChange={(event) => setRestSource((prev) => ({ ...prev, socketWriteTimeout: event.target.value }))}
                />
              </div>
              <div>
                <Label>Socket Idle Timeout (s)</Label>
                <Input
                  type="number"
                  value={restSource.socketIdleTimeout}
                  onChange={(event) => setRestSource((prev) => ({ ...prev, socketIdleTimeout: event.target.value }))}
                />
              </div>
              <div>
                <Label>Rate Limit (req/min)</Label>
                <Input
                  type="number"
                  value={restSource.rateLimitPerMin}
                  onChange={(event) => setRestSource((prev) => ({ ...prev, rateLimitPerMin: event.target.value }))}
                />
              </div>
                </>
              )}
              <div className="md:col-span-2 rounded-lg border p-4 space-y-3">
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <Label>OpenAPI Documentation (Optional)</Label>
                    <p className="text-xs text-muted-foreground mt-1">
                      Upload OpenAPI/Swagger JSON or YAML to enable smart endpoint and parameter suggestions in stream setup.
                    </p>
                  </div>
                  <Button
                    type="button"
                    variant="outline"
                    disabled={openApiUploadStatus === "uploading"}
                    onClick={() => {
                      const input = document.getElementById("openapi-upload-input");
                      if (input instanceof HTMLInputElement) input.click();
                    }}
                  >
                    {openApiUploadStatus === "uploading" ? (
                      <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                    ) : (
                      <Upload className="h-4 w-4 mr-2" />
                    )}
                    Upload
                  </Button>
                </div>
                <input
                  id="openapi-upload-input"
                  type="file"
                  accept=".json,.yaml,.yml,application/json,text/yaml,application/yaml"
                  className="hidden"
                  onChange={(event) => {
                    const file = event.target.files?.[0];
                    if (file) {
                      void handleOpenApiUpload(file);
                    }
                    event.currentTarget.value = "";
                  }}
                />
                {restSource.openApiSpecId ? (
                  <div className="rounded-md border bg-muted/30 p-3 space-y-2">
                    <div className="flex items-center justify-between gap-3">
                      <div>
                        <p className="text-sm font-medium">
                          {restSource.openApiTitle || restSource.openApiFileName || "OpenAPI document linked"}
                        </p>
                        <p className="text-xs text-muted-foreground">
                          {restSource.openApiVersion ? `Version ${restSource.openApiVersion}` : "Version unknown"}
                          {" • "}
                          {restSource.openApiFormat || "format"}
                          {" • "}
                          {restSource.openApiOperationsCount} operations
                        </p>
                      </div>
                      <Button type="button" variant="ghost" size="sm" onClick={resetOpenApiSelection}>
                        <Link2Off className="h-4 w-4 mr-1" />
                        Remove
                      </Button>
                    </div>
                    {restSource.openApiServers.length > 0 && (
                      <div>
                        <Label>Preferred Server</Label>
                        <Select
                          value={
                            restSource.openApiSelectedServerUrl ||
                            restSource.openApiServers[0]?.url ||
                            "NONE"
                          }
                          onValueChange={(value) =>
                            setRestSource((prev) => ({
                              ...prev,
                              openApiSelectedServerUrl: value === "NONE" ? "" : value,
                            }))
                          }
                        >
                          <SelectTrigger>
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            {restSource.openApiServers.map((server) => (
                              <SelectItem key={server.url} value={server.url}>
                                {server.url}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                    )}
                    {restSource.openApiWarnings.length > 0 && (
                      <div className="rounded-md border border-amber-300/60 bg-amber-50 px-3 py-2 text-xs text-amber-900">
                        {restSource.openApiWarnings.join(" ")}
                      </div>
                    )}
                  </div>
                ) : (
                  <p className="text-xs text-muted-foreground">
                    No OpenAPI document linked. Stream setup will remain fully manual.
                  </p>
                )}
                {openApiUploadError && (
                  <p className="text-xs text-destructive">{openApiUploadError}</p>
                )}
              </div>
            </CardContent>
          )}

          {(sourceType === "postgres" || sourceType === "mongo") && (
            <CardContent className="grid gap-3 md:grid-cols-2">
              {(() => {
                const cfg = sourceType === "postgres" ? postgresSource : mongoSource;
                const setCfg = sourceType === "postgres" ? setPostgresSource : setMongoSource;
                const isMongoCfg = sourceType === "mongo";
                const useMongoUriMode = isMongoCfg && cfg.connectionMode === "uri";
                const useMongoGlobalService = isMongoCfg && cfg.connectionMode === "nifi_global_service";

                return (
                  <>
                    <div className="md:col-span-2">
                      <Label>Source Name</Label>
                      <Input
                        value={cfg.sourceName}
                        onChange={(event) => setCfg((prev) => ({ ...prev, sourceName: event.target.value }))}
                      />
                    </div>
                    {sourceType === "mongo" && (
                      <div className="md:col-span-2">
                        <Label>Connection Mode</Label>
                        <Select
                          value={cfg.connectionMode}
                          onValueChange={(value) =>
                            setCfg((prev) => ({ ...prev, connectionMode: value as MongoConnectionMode }))
                          }
                        >
                          <SelectTrigger>
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="basic">Basic (Host/Port)</SelectItem>
                            <SelectItem value="uri">Mongo URI</SelectItem>
                            <SelectItem value="nifi_global_service">NiFi Global Service</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                    )}
                    {isMongoCfg && useMongoGlobalService && (
                      <div className="md:col-span-2">
                        <Label>Global MongoDB Service</Label>
                        <Select
                          value={cfg.nifiGlobalServiceId || "__none__"}
                          onValueChange={(value) =>
                            setCfg((prev) => ({ ...prev, nifiGlobalServiceId: value === "__none__" ? "" : value }))
                          }
                        >
                          <SelectTrigger>
                            <SelectValue placeholder="Select MongoDB service" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="__none__">Select service</SelectItem>
                            {mongoGlobalServices.map((service) => (
                              <SelectItem key={service.id} value={service.id}>
                                {service.name} ({service.live?.state || "UNKNOWN"})
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                        <p className="mt-1 text-xs text-muted-foreground">
                          The generated NiFi flow will inherit this MongoDB Controller Service. App-side preview is unavailable in this mode.
                        </p>
                      </div>
                    )}
                    {(!isMongoCfg || (!useMongoUriMode && !useMongoGlobalService)) && (
                      <>
                        <div>
                          <Label>Host</Label>
                          <Input
                            value={cfg.host}
                            onChange={(event) => setCfg((prev) => ({ ...prev, host: event.target.value }))}
                          />
                        </div>
                        <div>
                          <Label>Port</Label>
                          <Input
                            type="number"
                            value={cfg.port}
                            onChange={(event) => setCfg((prev) => ({ ...prev, port: event.target.value }))}
                          />
                        </div>
                      </>
                    )}
                    {isMongoCfg && useMongoUriMode && (
                      <div className="md:col-span-2">
                        <Label>Mongo URI Override</Label>
                        <Input
                          placeholder="mongodb://host:27017"
                          value={cfg.connectionUri}
                          onChange={(event) => setCfg((prev) => ({ ...prev, connectionUri: event.target.value }))}
                        />
                      </div>
                    )}
                    <div className="md:col-span-2">
                      <Label>Database Name</Label>
                      <Input
                        value={cfg.databaseName}
                        onChange={(event) => setCfg((prev) => ({ ...prev, databaseName: event.target.value }))}
                      />
                    </div>
                    {(!isMongoCfg || (!useMongoUriMode && !useMongoGlobalService)) && (
                      <>
                        <div>
                          <Label>Username</Label>
                          <Input
                            value={cfg.username}
                            onChange={(event) => setCfg((prev) => ({ ...prev, username: event.target.value }))}
                          />
                        </div>
                        <div>
                          <Label>Password</Label>
                          <Input
                            type="password"
                            value={cfg.password}
                            onChange={(event) => setCfg((prev) => ({ ...prev, password: event.target.value }))}
                          />
                        </div>
                        <div>
                          <Label>SSL Mode</Label>
                          <Select value={cfg.sslMode} onValueChange={(value) => setCfg((prev) => ({ ...prev, sslMode: value }))}>
                            <SelectTrigger>
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="disable">disable</SelectItem>
                              <SelectItem value="require">require</SelectItem>
                              <SelectItem value="verify-full">verify-full</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>
                      </>
                    )}
                    <div>
                      <Label>Connection Timeout (s)</Label>
                      <Input
                        type="number"
                        value={cfg.connectionTimeout}
                        onChange={(event) => setCfg((prev) => ({ ...prev, connectionTimeout: event.target.value }))}
                      />
                    </div>
                  </>
                );
              })()}
            </CardContent>
          )}

          {sourceType === "smb" && (
            <CardContent className="grid gap-3 md:grid-cols-2">
              <div className="md:col-span-2">
                <Label>Source Name</Label>
                <Input
                  value={smbSource.sourceName}
                  onChange={(event) => setSmbSource((prev) => ({ ...prev, sourceName: event.target.value }))}
                />
              </div>
              <div className="md:col-span-2">
                <Label>Connection Setup</Label>
                <Select value={smbConnectionMode} onValueChange={(value) => setSmbConnectionMode(value as SourceConnectionMode)}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="manual">Manual Setup</SelectItem>
                    <SelectItem value="application_service">Service</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              {smbConnectionMode === "application_service" ? (
                <div className="md:col-span-2">
                  <Label>Service</Label>
                  <Select value={smbApplicationServiceId || "__none__"} onValueChange={(value) => setSmbApplicationServiceId(value === "__none__" ? "" : value)}>
                    <SelectTrigger>
                      <SelectValue placeholder="Select SMB Service" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="__none__">Select service</SelectItem>
                      {smbApplicationServices.map((service) => (
                        <SelectItem key={service.id} value={service.id}>
                          {service.name} ({service.health})
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              ) : (
                <>
              <div className="md:col-span-2">
                <Label>SMB Server Address</Label>
                <Input
                  value={smbSource.smbConnection}
                  onChange={(event) => setSmbSource((prev) => ({ ...prev, smbConnection: event.target.value }))}
                />
              </div>
              <div>
                <Label>SMB Share</Label>
                <Input
                  value={smbSource.shareName}
                  onChange={(event) => setSmbSource((prev) => ({ ...prev, shareName: event.target.value }))}
                />
              </div>
              <div>
                <Label>Domain</Label>
                <Input
                  value={smbSource.domain}
                  onChange={(event) => setSmbSource((prev) => ({ ...prev, domain: event.target.value }))}
                />
              </div>
              <div>
                <Label>Username</Label>
                <Input
                  value={smbSource.username}
                  onChange={(event) => setSmbSource((prev) => ({ ...prev, username: event.target.value }))}
                />
              </div>
              <div>
                <Label>Password</Label>
                <Input
                  type="password"
                  value={smbSource.password}
                  onChange={(event) => setSmbSource((prev) => ({ ...prev, password: event.target.value }))}
                />
              </div>
                </>
              )}
              <div className="md:col-span-2">
                <Label>Base Directory Path</Label>
                <Input
                  value={smbSource.baseDirectoryPath}
                  onChange={(event) => setSmbSource((prev) => ({ ...prev, baseDirectoryPath: event.target.value }))}
                />
              </div>
              <div className="md:col-span-2">
                <Label>File Filter Pattern</Label>
                <Input
                  value={smbSource.fileFilterPattern}
                  onChange={(event) => setSmbSource((prev) => ({ ...prev, fileFilterPattern: event.target.value }))}
                />
              </div>
              <div>
                <Label>File Format</Label>
                <Select
                  value={smbSource.fileFormat}
                  onValueChange={(value) =>
                    setSmbSource((prev) => ({ ...prev, fileFormat: value as SmbSourceConfig["fileFormat"] }))
                  }
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="CSV">CSV</SelectItem>
                    <SelectItem value="JSON">JSON</SelectItem>
                    <SelectItem value="JSONL">JSONL</SelectItem>
                    <SelectItem value="XLSX">XLSX</SelectItem>
                    <SelectItem value="EXCEL">EXCEL</SelectItem>
                    <SelectItem value="BINARY">BINARY</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              {smbSource.fileFormat === "CSV" && (
                <>
                  <div>
                    <Label>CSV Delimiter</Label>
                    <Input
                      value={smbSource.csvDelimiter}
                      onChange={(event) => setSmbSource((prev) => ({ ...prev, csvDelimiter: event.target.value }))}
                    />
                  </div>
                  <div className="flex items-center justify-between rounded-md border p-3 mt-6 md:mt-0">
                    <div>
                      <Label className="text-sm">Has Header Row</Label>
                      <p className="text-xs text-muted-foreground">Toggle for CSV parsing</p>
                    </div>
                    <Switch
                      checked={smbSource.hasHeaderRow}
                      onCheckedChange={(checked) => setSmbSource((prev) => ({ ...prev, hasHeaderRow: checked }))}
                    />
                  </div>
                </>
              )}

            </CardContent>
          )}

          {sourceType === "webhook" && (
            <CardContent className="grid gap-3 md:grid-cols-2">
              <div className="md:col-span-2">
                <Label>Source Name</Label>
                <Input
                  value={webhookSource.sourceName}
                  onChange={(event) => setWebhookSource((prev) => ({ ...prev, sourceName: event.target.value }))}
                />
              </div>
              <div className="md:col-span-2">
                <Label>Connection Setup</Label>
                <Select value={webhookConnectionMode} onValueChange={(value) => setWebhookConnectionMode(value as SourceConnectionMode)}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="manual">Manual Setup</SelectItem>
                    <SelectItem value="application_service">Service</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              {webhookConnectionMode === "application_service" && (
                <div className="md:col-span-2">
                  <Label>Service</Label>
                  <Select value={webhookApplicationServiceId || "__none__"} onValueChange={(value) => setWebhookApplicationServiceId(value === "__none__" ? "" : value)}>
                    <SelectTrigger>
                      <SelectValue placeholder="Select Webhook Service" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="__none__">Select service</SelectItem>
                      {webhookApplicationServices.map((service) => (
                        <SelectItem key={service.id} value={service.id}>
                          {service.name} ({service.health})
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              )}
              <div className="md:col-span-2">
                <Label>Webhook Endpoint ID</Label>
                <Input
                  placeholder="orders-created"
                  value={webhookSource.endpointId}
                  onChange={(event) =>
                    setWebhookSource((prev) => ({
                      ...prev,
                      endpointId: event.target.value.trim().toLowerCase(),
                    }))
                  }
                />
                <p className="mt-1 text-xs text-muted-foreground">
                  Incoming endpoint: <code>/api/webhooks/{webhookSource.endpointId || "your-endpoint-id"}</code>
                </p>
              </div>
              <div>
                <Label>Content Type</Label>
                <Select
                  value={webhookSource.contentType}
                  onValueChange={(value) =>
                    setWebhookSource((prev) => ({
                      ...prev,
                      contentType: value as WebhookSourceConfig["contentType"],
                    }))
                  }
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="auto">Auto-detect</SelectItem>
                    <SelectItem value="json">JSON</SelectItem>
                    <SelectItem value="xml">XML</SelectItem>
                    <SelectItem value="text">Text</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="flex items-center justify-between rounded-md border p-3 mt-6 md:mt-0">
                <div>
                  <Label className="text-sm">Webhook Enabled</Label>
                  <p className="text-xs text-muted-foreground">Disable to reject incoming events without deleting flow</p>
                </div>
                <Switch
                  checked={webhookSource.enabled}
                  onCheckedChange={(checked) => setWebhookSource((prev) => ({ ...prev, enabled: checked }))}
                />
              </div>
              {webhookConnectionMode === "manual" && (
                <>
                  <div>
                    <Label>Signature Header</Label>
                    <Input
                      placeholder="X-Signature"
                      value={webhookSource.signatureHeader}
                      onChange={(event) =>
                        setWebhookSource((prev) => ({ ...prev, signatureHeader: event.target.value }))
                      }
                    />
                  </div>
                  <div>
                    <Label>Signature Secret (optional)</Label>
                    <Input
                      type="password"
                      value={webhookSource.signatureSecret}
                      onChange={(event) =>
                        setWebhookSource((prev) => ({ ...prev, signatureSecret: event.target.value }))
                      }
                    />
                  </div>
                  <div className="md:col-span-2">
                    <Label>Signature Algorithm</Label>
                    <Input value="hmac-sha256" readOnly />
                  </div>
                </>
              )}
            </CardContent>
          )}

          {sourceType === "trino" && (
            <CardContent className="grid gap-3 md:grid-cols-2">
              <div className="md:col-span-2">
                <Label>Source Name</Label>
                <Input
                  value={trinoSource.sourceName}
                  onChange={(event) => setTrinoSource((prev) => ({ ...prev, sourceName: event.target.value }))}
                />
              </div>
              <div className="md:col-span-2">
                <Label>Trino Endpoint</Label>
                <Input
                  placeholder="https://trino.example.com"
                  value={trinoSource.endpoint}
                  onChange={(event) => setTrinoSource((prev) => ({ ...prev, endpoint: event.target.value }))}
                />
              </div>
              <div className="md:col-span-2">
                <Label>Trino User</Label>
                <Input
                  placeholder="admin"
                  value={trinoSource.user}
                  onChange={(event) => setTrinoSource((prev) => ({ ...prev, user: event.target.value }))}
                />
              </div>
              <div className="md:col-span-2 border-t pt-4">
                <h3 className="text-sm font-semibold">Iceberg Source Table</h3>
                <p className="text-xs text-muted-foreground">This is the data table to read through Trino.</p>
              </div>
              <div>
                <Label>Source Catalog</Label>
                <Input
                  placeholder="bronze"
                  value={trinoSource.catalog}
                  onChange={(event) => setTrinoSource((prev) => ({ ...prev, catalog: event.target.value }))}
                />
              </div>
              <div>
                <Label>Source Schema / Namespace</Label>
                <Input
                  placeholder="fortisiem"
                  value={trinoSource.schema}
                  onChange={(event) => setTrinoSource((prev) => ({ ...prev, schema: event.target.value }))}
                />
              </div>
              <div className="md:col-span-2">
                <Label>Source Table</Label>
                <Input
                  placeholder="device__history"
                  value={trinoSource.table}
                  onChange={(event) => setTrinoSource((prev) => ({ ...prev, table: event.target.value }))}
                />
              </div>
              <div className="md:col-span-2 border-t pt-4">
                <h3 className="text-sm font-semibold">Checkpoint Tracking Table</h3>
                <p className="text-xs text-muted-foreground">
                  This is not the source table. It stores the last processed Iceberg snapshot ID for each source table.
                </p>
              </div>
              <div>
                <Label>Checkpoint Catalog</Label>
                <Input
                  placeholder="bronze"
                  value={trinoSource.checkpointCatalog}
                  onChange={(event) => setTrinoSource((prev) => ({ ...prev, checkpointCatalog: event.target.value }))}
                />
              </div>
              <div>
                <Label>Checkpoint Schema</Label>
                <Input
                  placeholder="checkpoints"
                  value={trinoSource.checkpointSchema}
                  onChange={(event) => setTrinoSource((prev) => ({ ...prev, checkpointSchema: event.target.value }))}
                />
              </div>
              <div className="md:col-span-2">
                <Label>Checkpoint Table</Label>
                <Input
                  placeholder="trino_snapshot_checkpoints"
                  value={trinoSource.checkpointTable}
                  onChange={(event) => setTrinoSource((prev) => ({ ...prev, checkpointTable: event.target.value }))}
                />
              </div>
              <div className="md:col-span-2 flex items-center justify-between rounded-md border p-3">
                <div>
                  <Label className="text-sm">Auto-create checkpoint table</Label>
                  <p className="text-xs text-muted-foreground">Create the tracking table through Trino if it does not already exist.</p>
                </div>
                <Switch
                  checked={trinoSource.autoCreateCheckpointTable}
                  onCheckedChange={(checked) => setTrinoSource((prev) => ({ ...prev, autoCreateCheckpointTable: checked }))}
                />
              </div>
              <div className="md:col-span-2">
                <Label>Column Selection</Label>
                <Select
                  value={trinoSource.columnMode}
                  onValueChange={(value) =>
                    setTrinoSource((prev) => ({ ...prev, columnMode: value === "manual" ? "manual" : "all" }))
                  }
                >
                  <SelectTrigger>
                    <SelectValue placeholder="Select column mode" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All columns</SelectItem>
                    <SelectItem value="manual">Manual columns</SelectItem>
                  </SelectContent>
                </Select>
                <p className="mt-1 text-xs text-muted-foreground">
                  All columns are resolved inside the NiFi flow from Trino metadata on each run.
                </p>
              </div>
              {trinoSource.columnMode === "manual" && (
                <div className="md:col-span-2">
                  <Label>Manual Output Columns</Label>
                  <Textarea
                    className="min-h-24"
                    placeholder={"accessip, object_id, name, naturalid, organization, ingest_id, ingest_ts"}
                    value={trinoSource.columns}
                    onChange={(event) => setTrinoSource((prev) => ({ ...prev, columns: event.target.value }))}
                  />
                </div>
              )}
            </CardContent>
          )}
        </Card>
      )}

      {step === STEP_SCHEDULE && (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Flow Schedule</CardTitle>
            <CardDescription>Define how often this source flow should run.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid gap-3 md:grid-cols-2">
              <div>
                <Label>Schedule Type</Label>
                <Select
                  value={schedule.type}
                  onValueChange={(value) =>
                    setSchedule((prev) => ({
                      ...prev,
                      type: value as ScheduleType,
                    }))
                  }
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="interval">Interval</SelectItem>
                    <SelectItem value="cron">Cron</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>

            {schedule.type === "interval" && (
              <div className="grid gap-3 md:grid-cols-2">
                <div>
                  <Label>Run Every</Label>
                  <Input
                    type="number"
                    min={1}
                    value={schedule.interval.value}
                    onChange={(event) => {
                      setSchedule((prev) => ({
                        ...prev,
                        interval: {
                          ...prev.interval,
                          value: event.target.value === "" ? "" : Math.max(1, Number.parseInt(event.target.value, 10) || 1),
                        },
                      }));
                    }}
                  />
                </div>
                <div>
                  <Label>Unit</Label>
                  <Select
                    value={schedule.interval.unit}
                    onValueChange={(value) =>
                      setSchedule((prev) => ({
                        ...prev,
                        interval: {
                          ...prev.interval,
                          unit: value as IntervalUnit,
                        },
                      }))
                    }
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="seconds">seconds</SelectItem>
                      <SelectItem value="minutes">minutes</SelectItem>
                      <SelectItem value="hours">hours</SelectItem>
                      <SelectItem value="days">days</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
            )}

            {schedule.type === "cron" && (
              <div className="space-y-1">
                <Label>Cron Expression</Label>
                <Input
                  value={schedule.cron.expression}
                  onChange={(event) =>
                    setSchedule((prev) => ({
                      ...prev,
                      cron: {
                        expression: event.target.value,
                      },
                    }))
                  }
                />
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {step === STEP_STREAMS && editing && (
        <div className="space-y-4">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 py-3">
              <div>
                <CardTitle className="text-base">Flow Map</CardTitle>
                <CardDescription>
                  Visual overview of streams, routes, and branches. Click a node to edit it; hover a node and
                  press + to add a parallel branch.
                </CardDescription>
              </div>
              <div className="flex items-center gap-2">
                <Button size="sm" variant="outline" disabled={isWebhookSource} onClick={handleCreateStream}>
                  <Plus className="h-4 w-4" /> Add Root
                </Button>
                <Button size="sm" variant="ghost" onClick={() => setFlowMapOpen((open) => !open)}>
                  {flowMapOpen ? "Hide" : "Show"}
                </Button>
              </div>
            </CardHeader>
            {flowMapOpen && (
              <CardContent className="p-0">
                <div className="h-[320px] w-full border-t">
                  <StreamFlowMap
                    streams={streams}
                    graph={streamGraph}
                    editingStream={editingStream}
                    setEditingStream={setEditingStream}
                    onCreateParallelBranch={handleCreateParallelBranch}
                  />
                </div>
              </CardContent>
            )}
          </Card>
          <div className="grid grid-cols-1 gap-4 lg:grid-cols-12">
          <div className="space-y-4 lg:col-span-12">
            <Card className="border-primary/20 bg-gradient-to-r from-primary/5 via-background to-background">
              <CardContent className="flex flex-col gap-4 p-5 lg:flex-row lg:items-start lg:justify-between">
                <div className="space-y-3">
                  <div className="flex flex-wrap items-center gap-2">
                    <Badge variant="secondary" className="border-0 bg-primary/10 text-primary">
                      {editingRouteParent ? "Route child" : parentStream ? "Child stream" : "Root stream"}
                    </Badge>
                    {editingRouteParent && (
                      <Badge variant="outline" className="gap-1">
                        <GitBranch className="h-3 w-3" />
                        {editingRouteParent.name}
                      </Badge>
                    )}
                    {editingIsEntity && (
                      <Badge variant="secondary" className="border-0 bg-warning-muted text-warning">
                        <Star className="mr-1 h-3 w-3 fill-current" /> Entity output
                      </Badge>
                    )}
                    {childStreamCount > 0 && (
                      <Badge variant="outline" className="gap-1">
                        <GitBranch className="h-3 w-3" />
                        {childStreamCount} child{childStreamCount === 1 ? "" : "ren"}
                      </Badge>
                    )}
                    {editingBranchingMode === "conditional" && editingConditionalBranchCount > 0 && (
                      <Badge variant="outline" className="gap-1 text-purple-600">
                        <GitBranch className="h-3 w-3" />
                        Conditional routes
                      </Badge>
                    )}
                    {editingBranchingMode === "parallel" && editingParallelBranchCount > 0 && (
                      <Badge variant="outline" className="gap-1 text-blue-600">
                        <GitBranch className="h-3 w-3" />
                        Parallel branches
                      </Badge>
                    )}
                    {editingTestState?.status === "success" && (
                      <Badge variant="outline" className="gap-1 text-green-700">
                        <CheckCircle className="h-3 w-3" />
                        Tested
                      </Badge>
                    )}
                  </div>
                  <div>
                    <h3 className="text-xl font-semibold">{editing.name || "Untitled stream"}</h3>
                    <p className="mt-1 text-sm text-muted-foreground">
                      {isMongoSource
                        ? "Configure the collection request, inspect sample documents, then choose how records should be emitted."
                        : isSmbSource
                        ? "SMB streams read records from matched files. Configure parsing and shaping controls without HTTP request mapping."
                        : isWebhookSource
                        ? "Webhook streams receive pushed events. Test by sending events to the webhook endpoint, then shape records visually."
                        : "Set up the request, test it, then pick records and fields directly from the returned payload."}
                    </p>
                  </div>
                  <div className="flex flex-wrap gap-2 text-xs">
                    {isRestSource && (
                      <Badge variant="outline" className="font-mono">
                        {editing.method}
                      </Badge>
                    )}
                    <Badge variant="outline" className="font-mono">
                      {editingIsRouteChild && !editingMakesRequest
                        ? "Routed record"
                        : isSmbSource
                        ? "SMB stream"
                        : isWebhookSource
                        ? "Webhook stream"
                        : editing.endpointPath || (isMongoSource ? "collection" : "/path")}
                    </Badge>
                    {parentStream && editing.parentField && (
                      <Badge variant="outline" className="gap-1">
                        <Link2 className="h-3 w-3" />
                        {parentStream.name}.{editing.parentField}
                      </Badge>
                    )}
                  </div>
                </div>

                <div className="flex flex-col gap-3 sm:min-w-[260px]">
                  <div className="flex items-center justify-between rounded-md border bg-background/70 px-3 py-2">
                    <div>
                      <Label htmlFor={`entity-toggle-${editing.id}`} className="text-sm">Entity output</Label>
                      <p className="text-xs text-muted-foreground">Publish this stream to Kafka.</p>
                    </div>
                    <Switch
                      id={`entity-toggle-${editing.id}`}
                      checked={editingIsEntity}
                      onCheckedChange={(checked) => {
                        updateStream(editing.id, { entity: checked ? editing.entity || createEmptyEntityConfig() : null });
                        if (!checked) {
                          setEntityDestinations((prev) => {
                            const next = { ...prev };
                            delete next[editing.id];
                            return next;
                          });
                        }
                      }}
                    />
                  </div>
                  <div className="flex items-center justify-between rounded-md border bg-background/70 px-3 py-2">
                    <div>
                      <Label htmlFor={`iceberg-toggle-${editing.id}`} className="text-sm">Sync to Iceberg</Label>
                      <p className="text-xs text-muted-foreground">Stream this entity output through Kafka Connect.</p>
                    </div>
                    <Switch
                      id={`iceberg-toggle-${editing.id}`}
                      checked={Boolean(editingEntityDestinationDraft?.iceberg.enabled)}
                      disabled={!editingIsEntity}
                      onCheckedChange={(checked) => {
                        if (!editingIsEntity) return;
                        setEntityDestinations((prev) => {
                          const draft = prev[editing.id] || defaultEntityDestinationDraft(kafkaOutput);
                          return {
                            ...prev,
                            [editing.id]: {
                              ...draft,
                              iceberg: { enabled: checked },
                            },
                          };
                        });
                      }}
                    />
                  </div>
                  <Button
                    size="sm"
                    variant="outline"
                    className="justify-center text-destructive hover:text-destructive"
                    onClick={() => handleDeleteStream(editing.id)}
                  >
                    <Trash2 className="h-4 w-4" /> Delete Stream
                  </Button>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="text-base">
                  {isMongoSource ? "Collection & Dependency" : isSmbSource ? "SMB Stream Setup" : isWebhookSource ? "Webhook Stream Setup" : "Request & Dependency"}
                </CardTitle>
                <CardDescription>
                  {isSmbSource
                    ? "SMB streams do not use HTTP request mapping. Configure stream identity and use response shaping below."
                    : isWebhookSource
                    ? "Webhook streams are event-driven and independent. Configure response shaping and downstream behavior."
                    : "Set request basics first. Parent stream injection lives here because it changes the URL, params, headers, or body."}
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid gap-3 md:grid-cols-2">
                  <div>
                    <Label>{isMongoSource ? "Collection Alias" : "Stream Name"}</Label>
                    <Input value={editing.name} onChange={(event) => updateStream(editing.id, { name: event.target.value })} />
                  </div>
                  {isRestSource && (!editingIsRouteChild || editingMakesRequest) && (
                    <div>
                      <Label>HTTP Method</Label>
                      <Select
                        value={editing.method}
                        onValueChange={(value) => updateStream(editing.id, { method: value as StreamMethod })}
                      >
                        <SelectTrigger>
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          {suggestedMethodsForPath.map((method) => (
                            <SelectItem key={method} value={method}>
                              {method}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  )}
                  {editingRouteParent && (
                    <div className="md:col-span-2 rounded-md border bg-muted/30 p-3 text-sm">
                      <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
                        <div>
                          <p>
                            This stream receives records from route rules on <strong>{editingRouteParent.name}</strong>.
                          </p>
                          <p className="mt-1 text-xs text-muted-foreground">
                            Keep it passive to publish/process the routed record directly, or enable a child HTTP request that uses routed-record attributes in the path, query, headers, or body.
                          </p>
                        </div>
                        {isRestSource && (
                          <div className="flex items-center gap-2 rounded-md border bg-background/60 px-3 py-2">
                            <Label htmlFor={`request-enabled-${editing.id}`} className="text-xs whitespace-nowrap">
                              Make HTTP request
                            </Label>
                            <Switch
                              id={`request-enabled-${editing.id}`}
                              checked={editing.requestEnabled}
                              onCheckedChange={(checked) => updateStream(editing.id, { requestEnabled: checked })}
                            />
                          </div>
                        )}
                      </div>
                    </div>
                  )}
                  {showRequestConfiguration && (
                    <div className="md:col-span-2">
                      <Label>{isMongoSource ? "Collection Name" : "Endpoint Path"}</Label>
                      {isRestSource && restSource.openApiSpecId.trim() ? (
                        <div className="space-y-2">
                          <Input
                            value={editing.endpointPath}
                            onChange={(event) =>
                              updateStream(editing.id, {
                                endpointPath: event.target.value,
                                openApiOperationId: "",
                              })
                            }
                          />
                          <div className="rounded-lg border p-2">
                            <Input
                              placeholder="Search endpoints from OpenAPI"
                              value={openApiOperationSearch}
                              onChange={(event) => setOpenApiOperationSearch(event.target.value)}
                              className="mb-2"
                            />
                            <div className="max-h-52 overflow-y-auto space-y-1 pr-1">
                              {openApiOperationsLoading ? (
                                <div className="text-xs text-muted-foreground px-2 py-1">Loading endpoints…</div>
                              ) : openApiOperationOptions.length === 0 ? (
                                <div className="text-xs text-muted-foreground px-2 py-1">No endpoint matches your search.</div>
                              ) : (
                                openApiOperationOptions.map((op) => {
                                  const active = editing.openApiOperationId === op.operation_id;
                                  return (
                                    <button
                                      key={op.operation_id}
                                      type="button"
                                      onClick={() => applyOpenApiOperationToStream(editing.id, op.operation_id)}
                                      className={cn(
                                        "w-full text-left rounded-md border px-2 py-1.5 text-xs transition",
                                        active
                                          ? "border-primary bg-primary/10"
                                          : "border-transparent hover:border-border hover:bg-muted/40",
                                      )}
                                    >
                                      <div className="font-mono">
                                        {op.method} {op.path_runtime || op.path_original}
                                      </div>
                                      {op.summary ? (
                                        <div className="text-muted-foreground mt-0.5 truncate">{op.summary}</div>
                                      ) : null}
                                    </button>
                                  );
                                })
                              )}
                            </div>
                          </div>
                          {selectedOpenApiOperationLoading ? (
                            <p className="text-xs text-muted-foreground">Loading operation details…</p>
                          ) : selectedOpenApiOperation ? (
                            <p className="text-xs text-muted-foreground">
                              {selectedOpenApiOperation.summary || selectedOpenApiOperation.description || "Selected from OpenAPI"}
                            </p>
                          ) : null}
                        </div>
                      ) : (
                        <Input
                          value={editing.endpointPath}
                          onChange={(event) => updateStream(editing.id, { endpointPath: event.target.value })}
                        />
                      )}
                    </div>
                  )}
                </div>

                {showRequestConfiguration && isRestSource && restSource.openApiSpecId.trim() && editing.openApiParamBindings.length > 0 && (
                  <div className="rounded-xl border p-4 space-y-3">
                    <div>
                      <h3 className="text-sm font-semibold">OpenAPI Parameter Mapping</h3>
                      <p className="text-xs text-muted-foreground">
                        Required path parameters are enabled by default. Query parameters stay optional unless enabled.
                      </p>
                    </div>
                    <div className="space-y-3">
                      {editing.openApiParamBindings.map((binding) => {
                        return (
                          <div key={binding.id} className="rounded-lg border bg-muted/20 p-3 space-y-3">
                            <div className="flex flex-wrap items-center justify-between gap-2">
                              <div className="flex items-center gap-2">
                                <Badge variant="outline">{binding.paramIn}</Badge>
                                <span className="font-mono text-sm">{binding.paramName}</span>
                                {binding.required && <Badge className="bg-red-100 text-red-800 hover:bg-red-100">required</Badge>}
                              </div>
                              <div className="flex items-center gap-2">
                                <Label className="text-xs text-muted-foreground">Enable</Label>
                                <Switch
                                  checked={binding.enabled}
                                  disabled={binding.required}
                                  onCheckedChange={(checked) =>
                                    updateOpenApiParamBinding(editing.id, binding.id, { enabled: checked })
                                  }
                                />
                              </div>
                            </div>
                            {binding.description && (
                              <p className="text-xs text-muted-foreground">{binding.description}</p>
                            )}
                            {binding.paramIn === "path" ? (
                              <div className="rounded-md border bg-background/60 p-3 text-xs text-muted-foreground">
                                Required path parameter. Use parent-child mapping if this value should come from a parent stream.
                              </div>
                            ) : (
                              <div>
                                <Label>Query Value</Label>
                                <Input
                                  value={binding.staticValue}
                                  disabled={!binding.enabled}
                                  onChange={(event) =>
                                    updateOpenApiParamBinding(editing.id, binding.id, {
                                      staticValue: event.target.value,
                                    })
                                  }
                                  placeholder={binding.defaultValue || "Amazon or ${site.id}"}
                                />
                                <p className="mt-1 text-[11px] text-muted-foreground">
                                  Enter a static value (for example <code>Amazon</code>) or a template token (for example <code>${"{site.id}"}</code>).
                                </p>
                              </div>
                            )}
                            <div className="text-xs text-muted-foreground">
                              Confidence: <strong>{binding.confidence}</strong>
                              {binding.schemaType ? <> • Type: <code>{binding.schemaType}</code></> : null}
                              {binding.defaultValue ? <> • Default: <code>{binding.defaultValue}</code></> : null}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                )}

                {isMongoSource && (
                  <div className="rounded-xl border p-4 space-y-3">
                    <div>
                      <h3 className="text-sm font-semibold">Collection Query Options</h3>
                      <p className="text-xs text-muted-foreground">
                        Configure query/filtering per collection stream.
                      </p>
                    </div>
                    <div className="space-y-3">
                      <div>
                        <Label>Query JSON</Label>
                        <Textarea
                          rows={3}
                          placeholder='{"status":"active"}'
                          value={editing.mongoQuery}
                          onChange={(event) => updateStream(editing.id, { mongoQuery: event.target.value })}
                        />
                        {editing.parentStreamId && (
                          <p className="mt-1 text-xs text-muted-foreground">
                            Parent-linked stream: use extracted parent attributes in query (example{" "}
                            <code>{"{\"asset_id\":${asset_id}}"}</code>).
                          </p>
                        )}
                      </div>
                      <div>
                        <Label>Projection JSON</Label>
                        <Textarea
                          rows={2}
                          placeholder='{"field": 1, "_id": 0}'
                          value={editing.mongoProjection}
                          onChange={(event) => updateStream(editing.id, { mongoProjection: event.target.value })}
                        />
                      </div>
                      <div className="grid gap-3 md:grid-cols-2">
                        <div>
                          <Label>Sort JSON</Label>
                          <Input
                            placeholder='{"asset_id": 1}'
                            value={editing.mongoSort}
                            onChange={(event) => updateStream(editing.id, { mongoSort: event.target.value })}
                          />
                        </div>
                        <div>
                          <Label>Limit</Label>
                          <Input
                            type="number"
                            min={1}
                            value={editing.mongoLimit}
                            onChange={(event) => updateStream(editing.id, { mongoLimit: event.target.value })}
                          />
                        </div>
                      </div>
                    </div>
                  </div>
                )}

                {showRequestConfiguration && (
                <div className="rounded-xl border border-dashed p-4">
                  <div className="flex items-start gap-3">
                    <div className="flex h-9 w-9 items-center justify-center rounded-full bg-primary/10 text-primary">
                      <GitBranch className="h-4 w-4" />
                    </div>
                    <div className="flex-1 space-y-4">
                      <div>
                        <h3 className="text-sm font-semibold">
                          {isMongoSource ? "Parent-child stream mapping" : "Parent-child request mapping"}
                        </h3>
                        <p className="text-xs text-muted-foreground">
                          {isMongoSource
                            ? "Use this when a child collection query depends on parent stream attributes (for example {\"asset_id\":${asset_id}})."
                            : "Use this only when the stream depends on a value from another stream. Test the parent stream first to get better field suggestions."}
                        </p>
                      </div>

                      <div className="grid gap-3 md:grid-cols-2">
                        {isRestSource && parentStream && (
                          <div className="md:col-span-2 rounded-lg border bg-muted/20 p-3">
                            <p className="text-xs font-medium">Path parameters detected in endpoint</p>
                            {editingPathVariables.length === 0 ? (
                              <p className="mt-1 text-xs text-muted-foreground">
                                No path placeholders detected. You can still reference parent attributes in query/header/body templates.
                              </p>
                            ) : (
                              <div className="mt-2 space-y-2">
                                {editingPathVariables.map((variable) => {
                                  const normalized = toAttributeName(variable).toLowerCase();
                                  const matchedFromParent = normalizedParentExtractedFieldSet.has(normalized);
                                  const hasFallbackDefault = Boolean((editing.pathParamDefaults?.[variable] || "").trim());
                                  return (
                                    <div
                                      key={variable}
                                      className="flex flex-wrap items-center gap-2 rounded-md border bg-background/70 px-2 py-1.5 text-xs"
                                    >
                                      <Badge variant="outline">path</Badge>
                                      <span className="font-mono">{variable}</span>
                                      <ArrowRight className="h-3.5 w-3.5 text-muted-foreground" />
                                      <span className="text-muted-foreground">
                                        {matchedFromParent
                                          ? "Auto-matched by name from parent attributes"
                                          : hasFallbackDefault
                                          ? "Uses child fallback default when parent value is missing"
                                          : "Needs same-named parent attribute or child fallback default"}
                                      </span>
                                    </div>
                                  );
                                })}
                              </div>
                            )}
                          </div>
                        )}
                      </div>

                      {parentStream ? (
                        <div className="rounded-lg border bg-muted/30 p-3 text-sm">
                          {isMongoSource ? (
                            <>
                              <div className="flex flex-wrap items-center gap-2">
                                <Badge variant="outline">{parentStream?.name || "Parent"}</Badge>
                                <ArrowRight className="h-3.5 w-3.5 text-muted-foreground" />
                                <Badge variant="outline">{editing.name || "Child"}</Badge>
                              </div>
                              <p className="mt-2 text-xs text-muted-foreground">
                                Put parent attributes directly in child Query JSON using NiFi EL, for example
                                <code className="ml-1 rounded bg-background px-1 py-0.5">{"{\"asset_id\":${asset_id}}"}</code>.
                              </p>
                              {parentFieldSuggestions.length > 0 && (
                                <p className="mt-2 text-xs text-muted-foreground">
                                  Available parent attributes:{" "}
                                  <span className="font-mono">{parentFieldSuggestions.join(", ")}</span>
                                </p>
                              )}
                            </>
                          ) : (
                            <>
                              <div className="flex flex-wrap items-center gap-2">
                                <Badge variant="outline">{parentStream?.name || "Parent"}</Badge>
                                <ArrowRight className="h-3.5 w-3.5 text-muted-foreground" />
                                <Badge variant="outline">{editing.name || "Child"}</Badge>
                              </div>
                              <p className="mt-2 text-xs text-muted-foreground">
                                {editingIsRouteChild
                                  ? "The routed record is the parent context. Keep path placeholders in endpoint URL "
                                  : "Parent-child linking is automatic. Keep path placeholders in endpoint URL "}
                                (for example <code>${"{site_id}"}</code>) and use template values in optional request details (for example <code>status=${"{status_id}"}</code>).
                              </p>
                              {editingPathVariables.length > 0 && (
                                <p className="mt-2 text-xs text-muted-foreground">
                                  Path placeholders in this stream:{" "}
                                  <span className="font-mono">{editingPathVariables.join(", ")}</span>
                                </p>
                              )}
                              {parentSampleValue && (
                                <p className="mt-2 text-xs text-muted-foreground">
                                  Latest parent sample: <code className="rounded bg-background px-1 py-0.5">{parentSampleValue}</code>
                                </p>
                              )}
                              {parentFieldSuggestions.length > 0 && (
                                <p className="mt-2 text-xs text-muted-foreground">
                                  Available parent attributes:{" "}
                                  <span className="font-mono">{parentFieldSuggestions.join(", ")}</span>
                                </p>
                              )}
                            </>
                          )}
                        </div>
                      ) : (
                        <div className="rounded-lg border bg-muted/20 p-3 text-xs text-muted-foreground">
                          Parent stream is set to <strong>None</strong>. This stream is currently independent.
                        </div>
                      )}

                      {isRestSource && parentStream && childFallbackPathVariables.length > 0 && (
                        <div className="rounded-lg border p-4 space-y-3">
                          <div>
                            <h3 className="text-sm font-semibold">Child Path Fallback Defaults</h3>
                            <p className="text-xs text-muted-foreground">
                              Used only when a same-named parent attribute is not available at runtime.
                            </p>
                          </div>
                          <div className="grid gap-3 md:grid-cols-2">
                            {childFallbackPathVariables.map((variable) => (
                              <div key={variable}>
                                <Label className="font-mono">{variable}</Label>
                                <Input
                                  value={editing.pathParamDefaults?.[variable] || ""}
                                  placeholder={`Fallback for ${variable}`}
                                  onChange={(event) =>
                                    setStreams((prev) =>
                                      prev.map((stream) =>
                                        stream.id === editing.id
                                          ? {
                                              ...stream,
                                              pathParamDefaults: {
                                                ...(stream.pathParamDefaults || {}),
                                                [variable]: event.target.value,
                                              },
                                            }
                                          : stream,
                                      ),
                                    )
                                  }
                                />
                              </div>
                            ))}
                          </div>
                        </div>
                      )}

                      {isRestSource && !parentStream && editingPathVariables.length > 0 && (
                        <div className="rounded-lg border p-4 space-y-3">
                          <div>
                            <h3 className="text-sm font-semibold">Path Parameter Defaults</h3>
                            <p className="text-xs text-muted-foreground">
                              Used for deployed/runtime flow when this stream has no parent. Test Stream can still override these values.
                            </p>
                          </div>
                          <div className="grid gap-3 md:grid-cols-2">
                            {editingPathVariables.map((variable) => (
                              <div key={variable}>
                                <Label className="font-mono">{variable}</Label>
                                <Input
                                  value={editing.pathParamDefaults?.[variable] || ""}
                                  placeholder={`Default for ${variable}`}
                                  onChange={(event) =>
                                    setStreams((prev) =>
                                      prev.map((stream) =>
                                        stream.id === editing.id
                                          ? {
                                              ...stream,
                                              pathParamDefaults: {
                                                ...(stream.pathParamDefaults || {}),
                                                [variable]: event.target.value,
                                              },
                                            }
                                          : stream,
                                      ),
                                    )
                                  }
                                />
                              </div>
                            ))}
                          </div>
                        </div>
                      )}

                      {isRestSource && (
                        <Accordion type="multiple" className="rounded-lg border px-4">
                          <AccordionItem value="request-details" className="border-b-0">
                            <AccordionTrigger className="py-3 text-sm">Optional request details</AccordionTrigger>
                            <AccordionContent className="space-y-4">
                              <div className="grid gap-4 md:grid-cols-2">
                                <div>
                                  <Label>Request Body Format</Label>
                                  <Select
                                    value={editing.bodyFormat}
                                    onValueChange={(value) =>
                                      updateStream(editing.id, {
                                        bodyFormat: value as BodyFormat,
                                        requestBodyTemplate: value === "EMPTY" ? "" : editing.requestBodyTemplate,
                                      })
                                    }
                                  >
                                    <SelectTrigger>
                                      <SelectValue />
                                    </SelectTrigger>
                                    <SelectContent>
                                      <SelectItem value="EMPTY">Empty</SelectItem>
                                      <SelectItem value="JSON">JSON</SelectItem>
                                      <SelectItem value="XML">XML</SelectItem>
                                      <SelectItem value="CSV">CSV</SelectItem>
                                    </SelectContent>
                                  </Select>
                                </div>
                                {editing.bodyFormat !== "EMPTY" && (
                                  <div>
                                    <Label>Request Body Template</Label>
                                    <Textarea
                                      rows={4}
                                      value={editing.requestBodyTemplate}
                                      onChange={(event) => updateStream(editing.id, { requestBodyTemplate: event.target.value })}
                                    />
                                  </div>
                                )}
                              </div>
                              <div className="grid gap-4 md:grid-cols-2">
                                <div>
                                  <Label>Additional Headers</Label>
                                  <Textarea
                                    rows={4}
                                    placeholder="X-Api-Version=v2"
                                    value={editing.additionalHeaders}
                                    onChange={(event) => updateStream(editing.id, { additionalHeaders: event.target.value })}
                                  />
                                </div>
                                <div>
                                  <Label>Additional Query Params</Label>
                                  <Textarea
                                    rows={4}
                                    placeholder="include=details"
                                    value={editing.additionalQueryParams}
                                    onChange={(event) => updateStream(editing.id, { additionalQueryParams: event.target.value })}
                                  />
                                </div>
                              </div>
                              {(editingTemplateVariables.length > 0 || editingHeaderEntries.length > 0 || editingQueryEntries.length > 0) && (
                                <div className="grid gap-3 md:grid-cols-3">
                                  {editingTemplateVariables.length > 0 && (
                                    <div className="rounded-md border bg-muted/20 p-3">
                                      <p className="text-xs font-medium">Detected variables</p>
                                      <div className="mt-2 flex flex-wrap gap-2">
                                        {editingTemplateVariables.map((variable) => (
                                          <Badge key={variable.name} variant="outline" className="font-mono text-[11px]">
                                            {variable.name}
                                            {!variable.required && " (optional)"}
                                          </Badge>
                                        ))}
                                      </div>
                                    </div>
                                  )}
                                  {editingHeaderEntries.length > 0 && (
                                    <div className="rounded-md border bg-muted/20 p-3">
                                      <p className="text-xs font-medium">Headers</p>
                                      <div className="mt-2 space-y-1 text-[11px] text-muted-foreground">
                                        {editingHeaderEntries.map(([key, value]) => (
                                          <div key={key} className="font-mono">{key}={value}</div>
                                        ))}
                                      </div>
                                    </div>
                                  )}
                                  {editingQueryEntries.length > 0 && (
                                    <div className="rounded-md border bg-muted/20 p-3">
                                      <p className="text-xs font-medium">Query params</p>
                                      <div className="mt-2 space-y-1 text-[11px] text-muted-foreground">
                                        {editingQueryEntries.map(([key, value]) => (
                                          <div key={key} className="font-mono">{key}={value}</div>
                                        ))}
                                      </div>
                                    </div>
                                  )}
                                </div>
                              )}
                            </AccordionContent>
                          </AccordionItem>
                        </Accordion>
                      )}
                    </div>
                  </div>
                </div>
                )}
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="flex-row items-start justify-between gap-3 space-y-0">
                <div>
                  <CardTitle className="text-base">Test & Shape Response</CardTitle>
                  <CardDescription>
                    Run the stream, inspect the payload, then apply record paths and field mappings without manual path typing.
                  </CardDescription>
                </div>
                <Button
                  size="sm"
                  variant="outline"
                  disabled={!editingSourceId || editingTestState?.status === "running" || (isRestSource && editingIsRouteChild && !editingMakesRequest)}
                  onClick={() => testStream(editing.id)}
                  title={
                    !editingSourceId
                      ? "Save the flow first to test streams"
                      : isRestSource && editingIsRouteChild && !editingMakesRequest
                      ? "Enable HTTP request on this routed stream before testing it"
                      : "Test this stream"
                  }
                >
                  {editingTestState?.status === "running" ? (
                    <Loader2 className="h-4 w-4 animate-spin" />
                  ) : (
                    <Play className="h-4 w-4" />
                  )}
                  {editingTestState?.status === "running" ? "Testing..." : "Test Stream"}
                </Button>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid gap-3 xl:grid-cols-3">
                  {supportsResponseFormat && (
                    <div className="rounded-lg border p-3 xl:col-span-1">
                      <Label>Response Format</Label>
                      <div className="mt-2 flex flex-wrap gap-2">
                        {((isRestSource ? ["JSON", "XML", "CSV"] : ["JSON", "XML"]) as const).map((fmt) => (
                          <button
                            key={fmt}
                            type="button"
                            onClick={() => updateStream(editing.id, { responseFormat: fmt })}
                            className={cn(
                              "rounded-md border px-3 py-1.5 text-sm transition-colors",
                              editing.responseFormat === fmt
                                ? "border-primary bg-primary text-primary-foreground"
                                : "bg-background hover:bg-muted",
                            )}
                          >
                            {fmt}
                          </button>
                        ))}
                      </div>
                      <p className="mt-2 text-xs text-muted-foreground">
                        {editing.responseFormat === "XML"
                          ? "Use XML mode when the endpoint returns XML and you need split depth / XPath controls."
                          : editing.responseFormat === "CSV"
                          ? "Use CSV mode when the endpoint returns direct CSV. Configure delimiter/header and parsing options below."
                          : "JSON mode unlocks record-path picking. JSON, XML, and CSV all support visual structure and key suggestions."}
                      </p>
                    </div>
                  )}

                  <div className="rounded-lg border p-3 xl:col-span-1">
                    {isSmbSource ? (
                      <div>
                        <Label className="text-sm">Record emission mode</Label>
                        <p className="mt-1 text-xs text-muted-foreground">
                          For SMB streams, message splitting is determined by file format processing (for example CSV rows or JSONL lines).
                        </p>
                      </div>
                    ) : (
                      <div className="flex items-center justify-between gap-3">
                        <div>
                          <Label className="text-sm">
                            {isMongoSource
                              ? "Publish each Mongo document separately"
                              : "Split response into records"}
                          </Label>
                          <p className="mt-1 text-xs text-muted-foreground">
                            {isMongoSource
                              ? "Enable one Kafka message per document. Disable to publish the query result as a batch."
                              : isWebhookSource
                              ? "Enable to split incoming webhook payloads into multiple records based on selected JSON path or XML split settings."
                              : "Enable for list endpoints. Disable for detail endpoints where the whole response is one record."}
                          </p>
                        </div>
                        <Switch
                          checked={editing.splitResponseIntoRecords}
                          onCheckedChange={(checked) => updateStream(editing.id, { splitResponseIntoRecords: checked })}
                        />
                      </div>
                    )}
                  </div>

                  {isRestSource && (
                    <div className="rounded-lg border p-3 xl:col-span-1">
                      <div className="flex items-center justify-between gap-3">
                        <div>
                          <Label className="text-sm">Wait for all records before downstream streams</Label>
                          <p className="mt-1 text-xs text-muted-foreground">
                            Enable when downstream streams should start only after all records entering this stream have completed it.
                          </p>
                        </div>
                        <Switch
                          checked={editing.waitForAllRecordsBeforeDownstream}
                          onCheckedChange={(checked) =>
                            updateStream(editing.id, { waitForAllRecordsBeforeDownstream: checked })
                          }
                        />
                      </div>
                      {editing.splitResponseIntoRecords && editing.waitForAllRecordsBeforeDownstream && (
                        <p className="mt-2 text-xs text-destructive">
                          Split and wait cannot both be enabled on the same stream yet.
                        </p>
                      )}
                    </div>
                  )}

                </div>

                {supportsResponseFormat && editing.responseFormat === "JSON" && editing.splitResponseIntoRecords && (
                  <div className="rounded-lg border p-3">
                    <div className="flex items-center justify-between gap-3">
                      <div>
                        <Label>Record Path</Label>
                        <p className="mt-1 text-xs text-muted-foreground">
                          Pick the collection that should become one record per output message. You can still edit the path manually.
                        </p>
                      </div>
                      {editingResponseInsights?.activeRecordPath && (
                        <Badge variant="outline" className="font-mono">
                          {editingResponseInsights.activeRecordPath}
                        </Badge>
                      )}
                    </div>
                    <Input
                      className="mt-3"
                      value={editing.responseDataPath}
                      onChange={(event) => updateStream(editing.id, { responseDataPath: event.target.value })}
                      placeholder="$.data"
                    />
                    {editingResponseInsights?.arraySuggestions.length ? (
                      <div className="mt-3 flex flex-wrap gap-2">
                        {editingResponseInsights.arraySuggestions.slice(0, 6).map((item) => (
                          <button
                            key={item.path}
                            type="button"
                            className={cn(
                              "rounded-full border px-2.5 py-1 text-[11px] transition",
                              editing.responseDataPath === item.path
                                ? "border-primary bg-primary/10 text-primary"
                                : "text-muted-foreground hover:border-primary hover:text-foreground",
                            )}
                            onClick={() => applySuggestedRecordPath(editing.id, item.path)}
                          >
                            {item.path} {item.sampleKeys.length > 0 ? `(${item.sampleKeys.join(", ")})` : ""}
                          </button>
                        ))}
                      </div>
                    ) : null}
                  </div>
                )}

                {supportsResponseFormat && editing.responseFormat === "XML" && editing.splitResponseIntoRecords && (
                  <div className="grid gap-3 md:grid-cols-2">
                    <div className="rounded-lg border p-3">
                      <Label>XML Split Depth</Label>
                      <Input
                        className="mt-2"
                        type="number"
                        min={1}
                        max={10}
                        value={editing.xmlSplitDepth}
                        onChange={(event) => updateStream(editing.id, { xmlSplitDepth: event.target.value })}
                      />
                    </div>
                    <div className="rounded-lg border p-3">
                      <Label>XML Record Element Name</Label>
                      <Input
                        className="mt-2"
                        placeholder="device"
                        value={editing.xmlRecordElement}
                        onChange={(event) => updateStream(editing.id, { xmlRecordElement: event.target.value })}
                      />
                    </div>
                  </div>
                )}

                {supportsResponseFormat && isRestSource && editing.responseFormat === "CSV" && (
                  <div className="rounded-lg border p-3">
                    <div className="grid gap-3 md:grid-cols-2">
                      <div>
                        <Label>CSV Delimiter</Label>
                        <Input
                          className="mt-2"
                          value={editing.csvDelimiter}
                          onChange={(event) => updateStream(editing.id, { csvDelimiter: event.target.value })}
                          placeholder=","
                        />
                      </div>
                      <div className="flex items-center justify-between rounded-md border px-3 py-2">
                        <div>
                          <Label className="text-sm">First row has headers</Label>
                          <p className="text-xs text-muted-foreground">Use column names from the first CSV row.</p>
                        </div>
                        <Switch
                          checked={editing.csvHasHeaderRow}
                          onCheckedChange={(checked) => updateStream(editing.id, { csvHasHeaderRow: checked })}
                        />
                      </div>
                      <div>
                        <Label>Quote Character</Label>
                        <Input
                          className="mt-2"
                          value={editing.csvQuoteChar}
                          onChange={(event) => updateStream(editing.id, { csvQuoteChar: event.target.value })}
                          placeholder={"\""}
                        />
                      </div>
                      <div>
                        <Label>Escape Character</Label>
                        <Input
                          className="mt-2"
                          value={editing.csvEscapeChar}
                          onChange={(event) => updateStream(editing.id, { csvEscapeChar: event.target.value })}
                          placeholder="\\"
                        />
                      </div>
                    </div>
                    <p className="mt-3 text-xs text-muted-foreground">
                      CSV responses are parsed into records during stream test and NiFi runtime. Keep split enabled for one Kafka message per row.
                    </p>
                  </div>
                )}

                {parentStream && !parentTestState?.result && (
                  <div className="rounded-lg border border-dashed p-3 text-xs text-muted-foreground">
                    Test the parent stream to surface likely parent values here before configuring this child stream further.
                  </div>
                )}

                {!editingTestState?.result ? (
                  <div className="rounded-xl border border-dashed p-8 text-center">
                    <Sparkles className="mx-auto h-8 w-8 text-primary/60" />
                    <h3 className="mt-3 text-sm font-semibold">No response captured yet</h3>
                    <p className="mt-1 text-sm text-muted-foreground">
                      Save the flow, run a stream test, and this section will turn into a response explorer with record-path and field suggestions.
                    </p>
                  </div>
                ) : (
                  <>
                    <div className="rounded-lg border bg-muted/20 p-3">
                      <div className="flex flex-wrap items-center gap-2 text-xs">
                        {editingTestState.result.ok ? (
                          <Badge variant="outline" className="gap-1 text-green-700">
                            <CheckCircle className="h-3 w-3" />
                            HTTP {editingTestState.result.status_code}
                          </Badge>
                        ) : (
                          <Badge variant="outline" className="gap-1 text-destructive">
                            <XCircle className="h-3 w-3" />
                            Failed
                          </Badge>
                        )}
                        {editingTestState.result.url && (
                          <span className="truncate font-mono text-muted-foreground">{editingTestState.result.url}</span>
                        )}
                      </div>
                      {editingTestState.result.error && (
                        <p className="mt-2 text-xs text-destructive">{editingTestState.result.error}</p>
                      )}
                    </div>

                    {editingResponseInsights && showVisualResponseExplorer ? (
                      <>
                        <div className="grid gap-4 xl:grid-cols-5">
                          <div className="rounded-xl border xl:col-span-3">
                            <div className="border-b px-4 py-3">
                              <div className="flex items-center gap-2">
                                <ListTree className="h-4 w-4 text-primary" />
                                <div>
                                  <p className="text-sm font-semibold">Response Structure</p>
                                  <p className="text-xs text-muted-foreground">Click a node to reuse it for record selection or field extraction.</p>
                                </div>
                              </div>
                            </div>
                            <div className="space-y-3 p-3">
                              <div className="space-y-2">
                                <p className="text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
                                  Full Response
                                </p>
                                <div className="max-h-[260px] overflow-auto rounded-md border bg-background p-2">
                                  <ResponseTreeViewer
                                    node={editingResponseInsights.rootNode}
                                    selectedPath={selectedResponsePath}
                                    activeRecordPath={editingResponseInsights.activeRecordPath}
                                    onSelect={(path) => {
                                      setSelectedResponseNodeByStream((prev) => ({
                                        ...prev,
                                        [editing.id]: path,
                                      }));
                                      setSelectedRecordResponseNodeByStream((prev) => ({
                                        ...prev,
                                        [editing.id]: "",
                                      }));
                                    }}
                                  />
                                </div>
                              </div>
                              {editingResponseInsights.recordRootNode && supportsStructuredRecordView && (
                                <div className="space-y-2">
                                  <p className="text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
                                    Record Item Structure
                                  </p>
                                  <div className="max-h-[260px] overflow-auto rounded-md border bg-muted/20 p-2">
                                    <ResponseTreeViewer
                                      node={editingResponseInsights.recordRootNode}
                                      selectedPath={selectedRecordResponsePath}
                                      activeRecordPath="$"
                                      onSelect={(path) => {
                                        setSelectedRecordResponseNodeByStream((prev) => ({
                                          ...prev,
                                          [editing.id]: path,
                                        }));
                                        setSelectedResponseNodeByStream((prev) => ({
                                          ...prev,
                                          [editing.id]: "",
                                        }));
                                      }}
                                    />
                                  </div>
                                  <p className="text-[11px] text-muted-foreground">
                                    Record tree paths are already relative (for example <code>$.type</code>).
                                  </p>
                                </div>
                              )}
                            </div>
                          </div>

                          <div className="space-y-4 xl:col-span-2">
                            <div className="rounded-xl border p-4">
                              <div className="flex items-center gap-2">
                                <Wand2 className="h-4 w-4 text-primary" />
                                <div>
                                  <p className="text-sm font-semibold">Smart Suggestions</p>
                                  <p className="text-xs text-muted-foreground">Apply likely paths and IDs with one click.</p>
                                </div>
                              </div>

                              {activeSelectedResponseNode ? (
                                <div className="mt-4 space-y-3">
                                  <div className="rounded-lg border bg-muted/20 p-3">
                                    <p className="text-xs font-medium">{activeSelectedResponseNode.label}</p>
                                    <p className="mt-1 break-all font-mono text-[11px] text-muted-foreground">
                                      {activeSelectedResponseDisplayPath}
                                    </p>
                                    <p className="mt-2 text-xs text-muted-foreground">
                                      {activeSelectedResponseNode.kind} · {activeSelectedResponseNode.summary}
                                    </p>
                                  </div>
                                  <div className="flex flex-wrap gap-2">
                                    {supportsRecordPathPicker && !selectedRecordResponseNode && activeSelectedResponseNode.kind === "array" && (
                                      <Button size="sm" variant="outline" onClick={() => applySuggestedRecordPath(editing.id, activeSelectedResponseNode.path)}>
                                        Use as record list
                                      </Button>
                                    )}
                                    {editing.responseFormat === "XML" && xmlSplitSuggestion && (
                                      <Button
                                        size="sm"
                                        variant="outline"
                                        onClick={() => applySuggestedXmlSplitRoot(editing.id, xmlSplitSuggestion)}
                                      >
                                        Use as XML record root
                                      </Button>
                                    )}
                                    {["string", "number", "boolean", "null"].includes(activeSelectedResponseNode.kind) && (
                                      <>
                                        <Button
                                          size="sm"
                                          variant="outline"
                                          onClick={() =>
                                            applySuggestedExtraction(
                                              editing.id,
                                              activeSelectedResponseNode.label.replace(/^\[.*\]$/, "value"),
                                              activeSelectedResponseActionPath,
                                            )
                                          }
                                        >
                                          Extract field
                                        </Button>
                                      </>
                                    )}
                                  </div>
                                </div>
                              ) : (
                                <div className="mt-4 space-y-4">
                                  {supportsRecordPathPicker && (
                                    <div>
                                      <p className="text-xs font-medium text-muted-foreground">Likely record collections</p>
                                      <div className="mt-2 flex flex-wrap gap-2">
                                        {editingResponseInsights.arraySuggestions.slice(0, 5).map((item) => (
                                          <button
                                            key={item.path}
                                            type="button"
                                            className="rounded-full border px-2.5 py-1 text-[11px] text-muted-foreground transition hover:border-primary hover:text-foreground"
                                            onClick={() => applySuggestedRecordPath(editing.id, item.path)}
                                          >
                                            {item.path}
                                          </button>
                                        ))}
                                      </div>
                                    </div>
                                  )}
                                </div>
                              )}
                              {supportsStructuredRecordView && editingResponseInsights.fieldSuggestions.length > 0 && (
                                <div className="mt-4 space-y-2">
                                  <p className="text-xs font-medium text-muted-foreground">
                                    Suggested fields from {editing.splitResponseIntoRecords ? "current record" : "response sample"}
                                  </p>
                                  <div className="max-h-56 space-y-2 overflow-auto rounded-md border bg-muted/20 p-2">
                                    {editingResponseInsights.fieldSuggestions.slice(0, 20).map((item) => (
                                      <div
                                        key={`${item.relativePath}:${item.label}`}
                                        className="rounded border bg-background px-2 py-1.5"
                                      >
                                        <div className="flex items-center justify-between gap-2">
                                          <p className="text-xs font-medium">{item.label}</p>
                                          <Button
                                            size="sm"
                                            variant="outline"
                                            className="h-6 px-2 text-[11px]"
                                            onClick={() => applySuggestedExtraction(editing.id, item.label, item.relativePath)}
                                          >
                                            Extract
                                          </Button>
                                        </div>
                                        <p className="mt-1 break-all font-mono text-[11px] text-muted-foreground">
                                          {item.relativePath}
                                        </p>
                                        <p className="mt-1 truncate text-[11px] text-muted-foreground">
                                          sample: {item.sample}
                                        </p>
                                      </div>
                                    ))}
                                  </div>
                                </div>
                              )}
                            </div>

                            <div className="rounded-xl border p-4">
                              <div className="flex items-center gap-2">
                                <KeyRound className="h-4 w-4 text-primary" />
                                <div>
                                  <p className="text-sm font-semibold">Selected Fields</p>
                                  <p className="text-xs text-muted-foreground">Fields picked here still use the same backend extraction-rule contract.</p>
                                </div>
                              </div>
                              <div className="mt-3 flex flex-wrap gap-2">
                                {editing.attributeExtractions.filter((rule) => rule.attributeName || rule.pathExpression).length > 0 ? (
                                  editing.attributeExtractions
                                    .filter((rule) => rule.attributeName || rule.pathExpression)
                                    .map((rule) => (
                                      <Badge key={rule.id} variant="outline" className="gap-1 font-normal">
                                        <span className="font-medium">{rule.attributeName || "field"}</span>
                                        <span className="font-mono text-[11px] text-muted-foreground">{rule.pathExpression || "manual"}</span>
                                      </Badge>
                                    ))
                                ) : (
                                  <p className="text-xs text-muted-foreground">
                                    Click a leaf value in the response tree and use <strong>Extract field</strong> to populate this list.
                                  </p>
                                )}
                              </div>
                            </div>
                          </div>
                        </div>

                        {supportsStructuredRecordView && editingResponseInsights.recordPreview.length > 0 && (
                          <div className="rounded-xl border">
                            <div className="border-b px-4 py-3">
                              <div className="flex items-center gap-2">
                                <Eye className="h-4 w-4 text-primary" />
                                <div>
                                  <p className="text-sm font-semibold">Record Preview</p>
                                  <p className="text-xs text-muted-foreground">
                                    Preview of the records that would flow downstream from the current record path.
                                  </p>
                                </div>
                              </div>
                            </div>
                            <div className="p-4">
                              <pre className="max-h-64 overflow-auto rounded-md bg-muted p-3 text-xs text-muted-foreground">
                                {JSON.stringify(editingResponseInsights.recordPreview, null, 2)}
                              </pre>
                            </div>
                          </div>
                        )}

                        {editing.responseFormat === "XML" && (
                          <div className="rounded-xl border">
                            <div className="border-b px-4 py-3">
                              <div className="flex items-center gap-2">
                                <Braces className="h-4 w-4 text-primary" />
                                <div>
                                  <p className="text-sm font-semibold">Raw XML Preview</p>
                                  <p className="text-xs text-muted-foreground">
                                    Parsed structure is shown above for selection. This is the exact XML response body.
                                  </p>
                                </div>
                              </div>
                            </div>
                            <div className="p-4">
                              <pre className="max-h-80 overflow-auto rounded-md bg-muted p-3 text-xs text-muted-foreground">
                                {formatResponsePreview(editingTestState.result)}
                              </pre>
                              {editingTestState.result.response_data_truncated && (
                                <p className="mt-2 text-[11px] text-muted-foreground">Preview truncated.</p>
                              )}
                            </div>
                          </div>
                        )}
                      </>
                    ) : (
                      <div className="rounded-xl border">
                        <div className="border-b px-4 py-3">
                          <div className="flex items-center gap-2">
                            <Braces className="h-4 w-4 text-primary" />
                            <div>
                              <p className="text-sm font-semibold">Raw Response Preview</p>
                              <p className="text-xs text-muted-foreground">
                                Visual picking is available for JSON, XML, and CSV responses. Use manual controls below when needed.
                              </p>
                            </div>
                          </div>
                        </div>
                        <div className="p-4">
                          <pre className="max-h-80 overflow-auto rounded-md bg-muted p-3 text-xs text-muted-foreground">
                            {formatResponsePreview(editingTestState.result)}
                          </pre>
                          {editingTestState.result.response_data_truncated && (
                            <p className="mt-2 text-[11px] text-muted-foreground">Preview truncated.</p>
                          )}
                        </div>
                      </div>
                    )}
                  </>
                )}
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="text-base">Advanced Behavior</CardTitle>
                <CardDescription>
                  Keep advanced controls available without forcing them on the first pass. Use these after request, test, and record selection are in place.
                </CardDescription>
              </CardHeader>
              <CardContent>
                <Accordion type="multiple" defaultValue={["extraction"]} className="w-full">
                  <AccordionItem value="extraction">
                    <AccordionTrigger>Field Extraction & Request Mapping</AccordionTrigger>
                    <AccordionContent>
                      <StreamExtractionEditor
                        editing={editing}
                        addExtractionRule={addExtractionRule}
                        updateExtractionRule={updateExtractionRule}
                        removeExtractionRule={removeExtractionRule}
                        onToggleKafkaKey={toggleKafkaKeyAttribute}
                      />
                    </AccordionContent>
                  </AccordionItem>
                  {isRestSource && (
                    <AccordionItem value="pagination">
                      <AccordionTrigger>Pagination</AccordionTrigger>
                      <AccordionContent>
                        <StreamPaginationEditor
                          editing={editing}
                          updateStream={updateStream}
                          testResult={streamTestStates[editing.id]?.result}
                        />
                      </AccordionContent>
                    </AccordionItem>
                  )}
                  <AccordionItem value="transformations">
                    <AccordionTrigger>Transformations</AccordionTrigger>
                    <AccordionContent>
                      <StreamTransformationsEditor
                        editing={editing}
                        addTransformationRule={addTransformationRule}
                        updateTransformationRule={updateTransformationRule}
                        removeTransformationRule={removeTransformationRule}
                      />
                    </AccordionContent>
                  </AccordionItem>
                  <AccordionItem value="routing">
                    <AccordionTrigger>Routing</AccordionTrigger>
                    <AccordionContent>
                      <StreamRoutingEditor
                        editing={editing}
                        streams={streams}
                        streamGraph={streamGraph}
                        addRoutingRule={addRoutingRule}
                        updateRoutingRule={updateRoutingRule}
                        removeRoutingRule={removeRoutingRule}
                        updateStream={updateStream}
                        createParallelBranch={handleCreateParallelBranch}
                        attachParallelChild={attachParallelChild}
                        detachParallelChild={detachParallelChild}
                        setEditingStream={setEditingStream}
                      />
                    </AccordionContent>
                  </AccordionItem>
                </Accordion>
              </CardContent>
            </Card>
          </div>
        </div>
      </div>
      )}

      {step === STEP_DESTINATION && (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Destination</CardTitle>
            <CardDescription>
              Streams marked as entity outputs publish to Kafka.
              {schemaWorkflowRequired
                ? " Each entity gets its own generated Kafka topic and schema workflow."
                : " Trino schema generation is optional and does not block saving this flow."}
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            {entityDestinationSummaries.length === 0 && (
              <div className="rounded-md border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">
                No entity streams were selected. Mark at least one stream as an entity output before deployment.
              </div>
            )}

            {schemaWorkflowRequired && inferenceBlockReason && (
              <div className="rounded-md border border-amber-200 bg-amber-50 p-3 text-xs text-amber-800 dark:border-amber-800 dark:bg-amber-950/30 dark:text-amber-200">
                {inferenceBlockReason}
              </div>
            )}

            {entityDestinationSummaries.map((entity, index) => {
              const job = entity.inferenceJob;
              const activeJob = job && ["deploying_nifi", "nifi_running", "collecting", "inferring"].includes(job.status);
              const showInferenceStatus = entity.inferenceStarting || Boolean(job);
              const inferenceProgress = job
                ? Math.max(
                    8,
                    Math.min(100, Math.round((job.messages_collected / Math.max(job.target_messages || 1, 1)) * 100)),
                  )
                : entity.inferenceStarting
                ? 8
                : 0;
              return (
                <div key={entity.stream.id} className="rounded-xl border bg-background/80 p-4 space-y-4">
                  <div className="flex flex-wrap items-start justify-between gap-3">
                    <div>
                      <div className="flex items-center gap-2">
                        <Badge variant="secondary">Entity {index + 1}</Badge>
                        <h3 className="text-sm font-semibold">{entity.stream.name || entity.stream.id}</h3>
                      </div>
                      <p className="mt-1 text-xs text-muted-foreground">
                        This stream is marked as an entity output and publishes its processed records to Kafka.
                      </p>
                    </div>
                    <Badge
                      variant="outline"
                      className={cn(
                        !schemaWorkflowRequired && "border-blue-500 text-blue-700",
                        schemaWorkflowRequired && entity.effectiveStatus === "Verified" && "border-green-500 text-green-700",
                        schemaWorkflowRequired && entity.effectiveStatus !== "Verified" && "border-amber-500 text-amber-700",
                      )}
                    >
                      {getEntitySchemaStatusLabel(sourceType, entity.effectiveStatus)}
                    </Badge>
                  </div>

                  <div className="grid gap-3 md:grid-cols-2">
                    <div className="md:col-span-2">
                      <Label>Generated Kafka Topic</Label>
                      <Input value={entity.topic} readOnly className="font-mono" />
                      <p className="text-xs text-muted-foreground mt-1">
                        Generated using <code>bronze.&lt;source_name&gt;.&lt;entity_stream&gt;__history</code>.
                      </p>
                    </div>
                    <div>
                      <Label>Partitions</Label>
                      <Input
                        type="number"
                        value={entity.draft.partitions}
                        onChange={(event) =>
                          setEntityDestinations((prev) => ({
                            ...prev,
                            [entity.stream.id]: { ...entity.draft, partitions: event.target.value },
                          }))
                        }
                      />
                    </div>
                    <div>
                      <Label>Replication Factor</Label>
                      <Input
                        type="number"
                        value={entity.draft.replicationFactor}
                        onChange={(event) =>
                          setEntityDestinations((prev) => ({
                            ...prev,
                            [entity.stream.id]: { ...entity.draft, replicationFactor: event.target.value },
                          }))
                        }
                      />
                    </div>
                    <div>
                      <Label>Key Strategy</Label>
                      <Select
                        value={entity.draft.keyStrategy}
                        onValueChange={(value) =>
                          setEntityDestinations((prev) => ({
                            ...prev,
                            [entity.stream.id]: { ...entity.draft, keyStrategy: normalizeKafkaKeyStrategy(value) },
                          }))
                        }
                      >
                        <SelectTrigger>
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="none">None (round-robin)</SelectItem>
                          <SelectItem value="primary_key">Primary Key</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                    <div>
                      <Label>Value Format</Label>
                      <Input value="Avro (Apicurio)" readOnly />
                    </div>
                    {!schemaWorkflowRequired ? (
                      <div className="md:col-span-2 rounded-md border border-blue-200 bg-blue-50 p-3 text-xs text-blue-700 dark:border-blue-800 dark:bg-blue-950/30 dark:text-blue-300">
                        Trino flows can be saved without schema inference. Kafka output settings still apply.
                      </div>
                    ) : (
                      <>
                        <div>
                          <Label>Schema Option</Label>
                          <Select
                            value={entity.draft.schemaMode}
                            onValueChange={(value) =>
                              setEntityDestinations((prev) => ({
                                ...prev,
                                [entity.stream.id]: {
                                  ...entity.draft,
                                  schemaMode: value as SchemaMode,
                                  existingSchemaArtifactId:
                                    value === "existing"
                                      ? entity.draft.existingSchemaArtifactId || entity.schemaArtifactId
                                      : entity.draft.existingSchemaArtifactId,
                                  existingSchemaVersion: "",
                                },
                              }))
                            }
                          >
                            <SelectTrigger>
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="existing">Use Existing Schema</SelectItem>
                              <SelectItem value="auto_generate">Auto Schema Inference</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>
                        <div>
                          <Label>Schema Artifact</Label>
                          {entity.draft.schemaMode === "existing" ? (
                        <Select
                          value={entity.draft.existingSchemaArtifactId || entity.schemaArtifactId}
                          onValueChange={(value) =>
                            setEntityDestinations((prev) => ({
                              ...prev,
                              [entity.stream.id]: {
                                ...entity.draft,
                                existingSchemaArtifactId: value,
                                existingSchemaVersion: "",
                              },
                            }))
                          }
                        >
                          <SelectTrigger className="font-mono">
                            <SelectValue placeholder="Select schema artifact" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value={entity.schemaArtifactId}>
                              {entity.schemaArtifactId}
                            </SelectItem>
                            {availableSchemaArtifacts
                              .filter((artifact) => artifact.artifact_id !== entity.schemaArtifactId)
                              .map((artifact) => (
                                <SelectItem key={artifact.artifact_id} value={artifact.artifact_id}>
                                  {artifact.artifact_id}
                                </SelectItem>
                              ))}
                          </SelectContent>
                        </Select>
                      ) : (
                        <Input value="" readOnly placeholder="Created after schema inference" className="font-mono" />
                      )}
                        </div>
                      </>
                    )}
                  </div>

                  {schemaWorkflowRequired && entity.draft.schemaMode === "existing" ? (
                    <div className="space-y-2">
                      {!entity.artifact && (
                        <p className="text-xs text-amber-600">
                          Selected schema artifact <code>{entity.schemaArtifactId}</code> was not found. Choose another artifact or run schema inference.
                        </p>
                      )}
                      <div>
                        <Label>Schema Version</Label>
                        <Select
                          value={entity.draft.existingSchemaVersion || "NONE"}
                          onValueChange={(value) =>
                            setEntityDestinations((prev) => ({
                              ...prev,
                              [entity.stream.id]: {
                                ...entity.draft,
                                existingSchemaVersion: value === "NONE" ? "" : value,
                              },
                            }))
                          }
                        >
                          <SelectTrigger>
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="NONE">Select version</SelectItem>
                            {(entity.artifact?.versions ?? []).map((version) => (
                              <SelectItem key={version.version} value={String(version.version)}>
                                v{version.version} - {version.status}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                    </div>
                  ) : schemaWorkflowRequired ? (
                    <div className="space-y-3 rounded-md border border-blue-200 bg-blue-50 p-3 dark:border-blue-800 dark:bg-blue-950/30">
                      <div className="flex items-center justify-between gap-3">
                        <div className="text-xs text-blue-700 dark:text-blue-300">
                          <div>
                            <span className="font-medium">Inference topic: </span>
                            <code>{entity.topic}-schema-inference</code>
                          </div>
                          <div>Runs a temporary NiFi inference flow and samples only this entity branch.</div>
                        </div>
                        <Button
                          size="sm"
                          className="gap-1"
                          disabled={entity.inferenceStarting || Boolean(inferenceBlockReason) || Boolean(activeJob)}
                          onClick={async () => {
                            let fid: string | undefined;
                            // Persist current entity-output/topic settings before inference.
                            // Existing flows can have unsaved Destination changes, and the backend
                            // starts inference from the saved source/flow state.
                            fid = await persistDraftToBackend();
                            if (!fid) {
                              fid = savedFlowId || editingFlow?.id || undefined;
                            }
                            if (!fid) {
                              toast.error("Save the flow before starting schema inference.");
                              return;
                            }
                            await triggerSchemaInferenceStart(fid, entity.stream.id);
                          }}
                        >
                          {entity.inferenceStarting ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Zap className="h-3.5 w-3.5" />}
                          {entity.inferenceStarting ? "Starting..." : job ? "Retry Inference" : "Start Inference"}
                        </Button>
                      </div>

                      {showInferenceStatus && (
                        <div className={cn(
                          "rounded-md border bg-background/80 p-3 space-y-2",
                          job?.status === "complete" && "border-green-200",
                          job?.status === "failed" && "border-red-200",
                        )}>
                          <div className="flex items-center justify-between gap-2 text-sm">
                            <span className="font-medium capitalize">
                              {entity.inferenceStarting && !job ? "Starting schema inference" : job?.status.replace(/_/g, " ")}
                            </span>
                            {activeJob && job && (
                              <Button
                                size="sm"
                                variant="outline"
                                onClick={async () => {
                                  await inferenceApi.stop(job.id);
                                  setInferenceJobsByEntity((prev) => ({
                                    ...prev,
                                    [entity.stream.id]: { ...job, status: "stopped" },
                                  }));
                                }}
                              >
                                Stop
                              </Button>
                            )}
                          </div>
                          <Progress value={inferenceProgress} />
                          <div className="text-xs text-muted-foreground">
                            {job ? (
                              <>
                                {job.messages_collected}/{job.target_messages} messages sampled from <code>{job.inference_topic}</code>
                              </>
                            ) : (
                              <>Creating temporary NiFi inference flow for this entity branch...</>
                            )}
                          </div>
                          {job?.error && <div className="text-xs text-red-600">{job.error}</div>}
                          {job?.status === "complete" && job.generated_schema && (
                            <div className="space-y-2">
                              <pre className="max-h-40 overflow-auto rounded bg-muted p-2 text-xs">
                                {JSON.stringify(job.generated_schema, null, 2)}
                              </pre>
                              <div className="flex flex-wrap gap-2">
                                <Button
                                  size="sm"
                                  onClick={async () => {
                                    const result = await inferenceApi.accept(job.id);
                                    toast.success(`Schema accepted for ${entity.stream.name}: ${result.schema_artifact_id} v${result.schema_version}`);
                                    setEntityDestinations((prev) => ({
                                      ...prev,
                                        [entity.stream.id]: {
                                          ...entity.draft,
                                          schemaMode: "existing",
                                          existingSchemaArtifactId: result.schema_artifact_id,
                                          existingSchemaVersion: String(result.schema_version),
                                        },
                                    }));
                                    await refetchSchemas();
                                  }}
                                >
                                  Accept Schema
                                </Button>
                                <Button size="sm" variant="outline" onClick={() => navigate("/schemas")}>
                                  Verify in Schema Manager
                                </Button>
                              </div>
                            </div>
                          )}
                        </div>
                      )}
                    </div>
                  ) : null}

                </div>
              );
            })}
          </CardContent>
        </Card>
      )}
      {step === STEP_REVIEW && (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Review Flow</CardTitle>
            <CardDescription>
              {schemaWorkflowRequired
                ? "Confirm configuration. Schema must be verified before run."
                : "Confirm configuration. Trino schema generation is optional for this flow."}
            </CardDescription>
          </CardHeader>
          <CardContent>
            <dl className="grid grid-cols-1 md:grid-cols-2 gap-x-6 gap-y-3 text-sm">
              {[
                ["Source Type", sourceTypeMeta.name],
                ["Source Name", sourceName || ""],
                ["Connection", sourceConnectionSummary || ""],
                ["Flow Schedule", scheduleSummary],
                ["Streams", streamSummary || ""],
                ...(sourceType === "mongo" ? ([["Mongo Publish Mode", mongoPublishMode]] as [string, string][]) : []),
                ...(sourceType === "rest" ? ([["Pagination", paginationSummary]] as [string, string][]) : []),
                ...(sourceType === "smb" || sourceType === "webhook" || sourceType === "trino" ? [] : ([["Fan-out", fanOutSummary]] as [string, string][])),
                ["Entity Outputs", String(entityDestinationSummaries.length)],
              ].map(([key, value]) => (
                <div key={key} className="flex justify-between border-b py-2">
                  <dt className="text-muted-foreground">{key}</dt>
                  <dd className="font-medium text-right">{value}</dd>
                </div>
              ))}
            </dl>
            <div className="mt-4 space-y-2">
              {entityDestinationSummaries.map((entity) => (
                <div key={entity.stream.id} className="rounded-md border p-3 text-sm">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div>
                      <div className="font-medium">{entity.stream.name}</div>
                      <div className="text-xs text-muted-foreground font-mono">{entity.topic}</div>
                      <div className="text-xs text-muted-foreground font-mono">
                        {schemaWorkflowRequired
                          ? `${entity.schemaArtifactId}${entity.effectiveVersion ? ` v${entity.effectiveVersion.version}` : ""}`
                          : "Schema optional"}
                      </div>
                    </div>
                    <Badge
                      variant="outline"
                      className={cn(
                        !schemaWorkflowRequired && "border-blue-500 text-blue-700",
                        schemaWorkflowRequired && entity.effectiveStatus === "Verified" && "border-green-500 text-green-700",
                        schemaWorkflowRequired && entity.effectiveStatus !== "Verified" && "border-amber-500 text-amber-700",
                      )}
                    >
                      {getEntitySchemaStatusLabel(sourceType, entity.effectiveStatus)}
                    </Badge>
                  </div>
                </div>
              ))}
            </div>
            {schemaWorkflowRequired && !allEntitySchemasVerified ? (
              <div className="mt-4 rounded-md border border-warning/30 bg-warning-muted p-3 text-sm">
                <p>One or more entity schemas are not verified. Deployment/start will remain blocked until every entity schema is verified.</p>
                <Button className="mt-2" size="sm" variant="outline" onClick={() => setStep(STEP_DESTINATION)}>
                  Back to Destination
                </Button>
              </div>
            ) : schemaWorkflowRequired ? (
              <div className="mt-4 rounded-md border border-success/30 bg-success-muted p-3 text-sm text-success">
                All entity schemas are verified and eligible for deployment.
              </div>
            ) : (
              <div className="mt-4 rounded-md border border-blue-200 bg-blue-50 p-3 text-sm text-blue-700">
                Trino flows can be saved without schema inference.
              </div>
            )}
          </CardContent>
        </Card>
      )}

      <div className="flex items-center justify-between mt-6">
        <div className="flex items-center gap-2">
          <Button variant="outline" disabled={step === STEP_SOURCE_TYPE} onClick={() => setStep((prev) => prev - 1)}>
            <ArrowLeft className="h-4 w-4" /> Back
          </Button>
          <Button variant="outline" onClick={handleSaveDraft}>
            <Save className="h-4 w-4" /> Save Draft
          </Button>
        </div>
        {step < steps.length - 1 ? (
          <Button
            onClick={() => {
              const validationIssues = validateDesignerState(step);
              if (validationIssues.length > 0) {
                toast.error(formatValidationToast(validationIssues));
                return;
              }
              setStep((prev) => prev + 1);
            }}
          >
            Next <ArrowRight className="h-4 w-4" />
          </Button>
        ) : (
          <Button onClick={handleSaveAndContinue} disabled={isSaving} data-testid="flow-save-continue-btn">
            {finalSaveButtonLabel}{" "}
            <ArrowRight className="h-4 w-4" />
          </Button>
        )}
      </div>

      <Dialog
        open={Boolean(pendingStreamTest)}
        onOpenChange={(open) => {
          if (!open) setPendingStreamTest(null);
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Enter Test Variables</DialogTitle>
            <DialogDescription>Provide values for required variables before running this stream test.</DialogDescription>
          </DialogHeader>
          {pendingStreamTest && (
            <div className="space-y-3">
              {pendingStreamTest.variables.map((variable) => (
                <div key={variable.name} className="space-y-1.5">
                  <Label htmlFor={`test-var-${variable.name}`}>
                    {variable.name}
                    {!variable.required && <span className="ml-2 text-xs text-muted-foreground">(optional)</span>}
                  </Label>
                  <Input
                    id={`test-var-${variable.name}`}
                    value={pendingStreamTest.values[variable.name] ?? ""}
                    onChange={(event) =>
                      setPendingStreamTest((prev) =>
                        prev
                          ? {
                              ...prev,
                              values: {
                                ...prev.values,
                                [variable.name]: event.target.value,
                              },
                            }
                          : prev,
                      )
                    }
                  />
                </div>
              ))}
            </div>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => setPendingStreamTest(null)}>
              Cancel
            </Button>
            <Button
              onClick={() => {
                if (!pendingStreamTest) return;
                const hasMissing = pendingStreamTest.variables.some(
                  (variable) => variable.required && !(pendingStreamTest.values[variable.name] ?? "").trim(),
                );
                if (hasMissing) {
                  toast.error("Please fill all required variables.");
                  return;
                }
                const payload: Record<string, string> = {};
                for (const variable of pendingStreamTest.variables) {
                  const value = (pendingStreamTest.values[variable.name] ?? "").trim();
                  if (value) payload[variable.name] = value;
                }
                const streamId = pendingStreamTest.streamId;
                const confirmMutation = Boolean(pendingStreamTest.confirmMutation);
                setPendingStreamTest(null);
                void executeStreamTest(streamId, payload, confirmMutation);
              }}
            >
              Run Test
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      <Dialog
        open={Boolean(pendingInferenceStart)}
        onOpenChange={(open) => {
          if (!open) setPendingInferenceStart(null);
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Inference Runtime Values</DialogTitle>
            <DialogDescription>
              This source requires runtime variables before schema inference can call the API.
            </DialogDescription>
          </DialogHeader>
          {pendingInferenceStart && (
            <div className="max-h-[420px] space-y-3 overflow-auto pr-1">
              {pendingInferenceStart.params.map((param) => (
                <div key={param.key} className="space-y-1.5">
                  <Label htmlFor={`infer-var-${param.key}`}>{param.key}</Label>
                  <Input
                    id={`infer-var-${param.key}`}
                    value={pendingInferenceStart.values[param.key] ?? ""}
                    onChange={(event) =>
                      setPendingInferenceStart((prev) =>
                        prev
                          ? {
                              ...prev,
                              values: {
                                ...prev.values,
                                [param.key]: event.target.value,
                              },
                            }
                          : prev,
                      )
                    }
                    placeholder={param.paramName || param.key}
                  />
                  <p className="text-[11px] text-muted-foreground">
                    Stream: <strong>{param.streamName}</strong>
                    {param.description ? ` · ${param.description}` : ""}
                  </p>
                </div>
              ))}
            </div>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => setPendingInferenceStart(null)}>
              Cancel
            </Button>
            <Button
              disabled={Boolean(inferenceBlockReason)}
              title={inferenceBlockReason ?? "Start inference"}
              onClick={async () => {
                if (!pendingInferenceStart) return;
                const hasMissing = pendingInferenceStart.params.some(
                  (param) => !(pendingInferenceStart.values[param.key] ?? "").trim(),
                );
                if (hasMissing) {
                  toast.error("Please fill all required inference values.");
                  return;
                }
                const values: Record<string, string> = {};
                for (const param of pendingInferenceStart.params) {
                  values[param.key] = (pendingInferenceStart.values[param.key] ?? "").trim();
                }
                setInferenceRuntimeValues((prev) => ({ ...prev, ...values }));
                const flowId = pendingInferenceStart.flowId;
                const entityStreamId = pendingInferenceStart.entityStreamId;
                setPendingInferenceStart(null);
                await startInferenceWithRuntime(flowId, values, entityStreamId);
              }}
            >
              Start Inference
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </AppLayout>
  );
};

export default FlowDesigner;

function ResponseTreeViewer({
  node,
  selectedPath,
  activeRecordPath,
  onSelect,
}: {
  node: ResponseExplorerNode | null;
  selectedPath: string;
  activeRecordPath: string;
  onSelect: (path: string) => void;
}) {
  if (!node) {
    return <p className="text-xs text-muted-foreground">No JSON structure available for this response yet.</p>;
  }

  return (
    <div className="space-y-1">
      <ResponseTreeNode
        node={node}
        selectedPath={selectedPath}
        activeRecordPath={activeRecordPath}
        onSelect={onSelect}
      />
    </div>
  );
}

function ResponseTreeNode({
  node,
  selectedPath,
  activeRecordPath,
  onSelect,
}: {
  node: ResponseExplorerNode;
  selectedPath: string;
  activeRecordPath: string;
  onSelect: (path: string) => void;
}) {
  const isSelected = selectedPath === node.path;
  const isRecordPath = activeRecordPath && activeRecordPath === node.path;

  return (
    <div className="space-y-1">
      <button
        type="button"
        onClick={() => onSelect(node.path)}
        className={cn(
          "flex w-full items-center gap-2 rounded-md border px-3 py-2 text-left text-xs transition-colors",
          isSelected ? "border-primary bg-primary/10" : "hover:bg-muted/50",
        )}
        style={{ paddingLeft: `${12 + node.depth * 14}px` }}
      >
        <span className="font-mono text-[11px]">{node.label}</span>
        <Badge variant="outline" className="h-5 px-1.5 text-[10px] capitalize">
          {node.kind}
        </Badge>
        <span className="truncate text-muted-foreground">{node.summary}</span>
        {isRecordPath && (
          <Badge variant="secondary" className="ml-auto border-0 bg-primary/10 text-primary">
            record
          </Badge>
        )}
      </button>

      {node.children.length > 0 && (
        <div className="space-y-1">
          {node.children.map((child) => (
            <ResponseTreeNode
              key={child.path}
              node={child}
              selectedPath={selectedPath}
              activeRecordPath={activeRecordPath}
              onSelect={onSelect}
            />
          ))}
        </div>
      )}
      {node.hiddenChildrenCount > 0 && (
        <p
          className="px-3 text-[11px] text-muted-foreground"
          style={{ paddingLeft: `${24 + node.depth * 14}px` }}
        >
          {node.truncatedByDepth
            ? `+${node.hiddenChildrenCount} nested node${node.hiddenChildrenCount === 1 ? "" : "s"} hidden (depth limit)`
            : `+${node.hiddenChildrenCount} more node${node.hiddenChildrenCount === 1 ? "" : "s"} hidden`}
        </p>
      )}
    </div>
  );
}

function StreamExtractionEditor({
  editing,
  addExtractionRule,
  updateExtractionRule,
  removeExtractionRule,
  onToggleKafkaKey,
}: {
  editing: Stream;
  addExtractionRule: (streamId: string) => void;
  updateExtractionRule: (streamId: string, ruleId: string, patch: Partial<AttributeExtractionRule>) => void;
  removeExtractionRule: (streamId: string, ruleId: string) => void;
  onToggleKafkaKey: (streamId: string, attributeName: string) => void;
}) {
  const selectedKafkaKey =
    editing.primaryKeyFields
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean)[0] || "";
  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <div>
          <h3 className="text-sm font-semibold">Attribute Extraction</h3>
          <p className="text-xs text-muted-foreground">
            Manual rule editor for attribute extraction. Select at most one extracted attribute as Kafka key.
          </p>
        </div>
        <Button size="sm" variant="outline" onClick={() => addExtractionRule(editing.id)}>
          <Plus className="h-4 w-4" /> Add Rule
        </Button>
      </div>

      {editing.attributeExtractions.length === 0 ? (
        <p className="text-xs text-muted-foreground">No extraction rules yet.</p>
      ) : (
        <div className="space-y-3">
          {editing.attributeExtractions.map((rule) => (
            <div key={rule.id} className="rounded-md border p-3 space-y-3">
              <div className="flex items-center justify-between">
                <p className="text-sm font-medium">Extraction Rule</p>
                <Button
                  size="icon"
                  variant="ghost"
                  onClick={() => removeExtractionRule(editing.id, rule.id)}
                  aria-label="Remove extraction rule"
                >
                  <Trash2 className="h-4 w-4" />
                </Button>
              </div>
              <div className="flex items-center justify-between rounded-md border p-3">
                <div>
                  <Label className="text-sm">Kafka Key</Label>
                  <p className="text-xs text-muted-foreground">
                    {rule.attributeName.trim()
                      ? "Use this extracted attribute as the Kafka message key."
                      : "Set Attribute Name first to enable key selection."}
                  </p>
                </div>
                <Switch
                  checked={Boolean(rule.attributeName.trim()) && selectedKafkaKey === rule.attributeName}
                  disabled={!rule.attributeName.trim()}
                  onCheckedChange={() => onToggleKafkaKey(editing.id, rule.attributeName)}
                />
              </div>
              <div className="grid gap-3 md:grid-cols-2">
                <div>
                  <Label>Attribute Name</Label>
                  <Input
                    value={rule.attributeName}
                    onChange={(event) =>
                      updateExtractionRule(editing.id, rule.id, { attributeName: event.target.value })
                    }
                  />
                </div>
                <div>
                  <Label>{editing.responseFormat === "XML" ? "XPath Expression" : "JSONPath / Path Expression"}</Label>
                  <Input
                    placeholder={editing.responseFormat === "XML" ? "/device/naturalId/text()" : "$.id"}
                    value={rule.pathExpression}
                    onChange={(event) =>
                      updateExtractionRule(editing.id, rule.id, { pathExpression: event.target.value })
                    }
                  />
                </div>
                <div>
                  <Label>Default Value</Label>
                  <Input
                    value={rule.defaultValue}
                    onChange={(event) =>
                      updateExtractionRule(editing.id, rule.id, { defaultValue: event.target.value })
                    }
                  />
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

type PaginationGuess = {
  patch: Partial<Stream>;
  summary: string[];
};

const _findHeader = (headers: Record<string, string>, candidates: string[]): string | null => {
  const keys = Object.keys(headers || {});
  for (const wanted of candidates) {
    const hit = keys.find((k) => k.toLowerCase() === wanted.toLowerCase());
    if (hit) return hit; // return the server's ACTUAL casing, not ours
  }
  return null;
};

const _findBodyKey = (body: unknown, candidates: string[]): string | null => {
  if (!body || typeof body !== "object" || Array.isArray(body)) return null;
  const keys = Object.keys(body as Record<string, unknown>);
  for (const wanted of candidates) {
    const hit = keys.find((k) => k.toLowerCase() === wanted.toLowerCase());
    if (hit) return hit;
  }
  return null;
};

const detectPagination = (
  responseData: unknown,
  responseHeaders: Record<string, string>,
  method: string,
  bodyFormat: string,
): PaginationGuess | null => {
  const headers = responseHeaders || {};
  const body = responseData && typeof responseData === "object" && !Array.isArray(responseData)
    ? (responseData as Record<string, unknown>)
    : null;
  const patch: Partial<Stream> = { paginationEnabled: true, maxPages: "50" };
  const summary: string[] = [];

  // Where do we send values back? Only a real choice for a JSON-bodied write.
  const canBody = ["POST", "PUT", "PATCH"].includes((method || "GET").toUpperCase()) && bodyFormat === "JSON";
  patch.paginationLocation = canBody ? "BODY" : "QUERY";

  // --- 1. Link header (RFC 5988) wins: most explicit next-link signal there is.
  const linkHeader = _findHeader(headers, ["Link"]);
  if (linkHeader && /rel\s*=\s*"?next"?/i.test(headers[linkHeader] || "")) {
    patch.paginationType = "NEXT_URL";
    patch.nextUrlSource = "LINK_HEADER";
    patch.nextUrlHeader = linkHeader;
    summary.push(`Next link: ${linkHeader} header (rel="next")`);
    return { patch, summary };
  }

  // --- 2. Next-URL header
  const nextUrlHeader = _findHeader(headers, ["X-Next-Url", "X-Next-Link", "X-Next-Page-Url"]);
  if (nextUrlHeader) {
    patch.paginationType = "NEXT_URL";
    patch.nextUrlSource = "HEADER";
    patch.nextUrlHeader = nextUrlHeader;
    summary.push(`Next link: ${nextUrlHeader} header`);
    return { patch, summary };
  }

  // --- 3. Next-URL in the body
  const nextUrlKey = _findBodyKey(body, ["next_url", "nextUrl", "next", "next_page_url"]);
  if (nextUrlKey) {
    patch.paginationType = "NEXT_URL";
    patch.nextUrlSource = "BODY";
    patch.nextUrlPath = `$.${nextUrlKey}`;
    summary.push(`Next link: $.${nextUrlKey} in the response body`);
    return { patch, summary };
  }

  // --- 4. Cursor header
  const cursorHeader = _findHeader(headers, ["X-Next-Cursor", "X-Cursor", "X-Next-Page-Token"]);
  if (cursorHeader) {
    patch.paginationType = "CURSOR";
    patch.cursorSource = "HEADER";
    patch.cursorHeader = cursorHeader;
    patch.cursorParamName = "cursor";
    summary.push(`Token: ${cursorHeader} header, sent back as ?cursor=…`);
    return { patch, summary };
  }

  // --- 5. Cursor in the body
  const cursorKey = _findBodyKey(body, ["next_cursor", "nextCursor", "next_page_token", "cursor"]);
  if (cursorKey) {
    patch.paginationType = "CURSOR";
    patch.cursorSource = "BODY";
    patch.cursorValuePath = `$.${cursorKey}`;
    patch.cursorParamName = "cursor";
    summary.push(`Token: $.${cursorKey} in the response body, sent back as ?cursor=…`);
    return { patch, summary };
  }

  // --- 6/7. Counting styles. Pick the stop signal first: has_more > total count > empty page.
  const hasMoreKey = _findBodyKey(body, ["has_more", "hasMore", "more", "has_next"]);
  const totalHeader = _findHeader(headers, ["X-Total-Count", "X-Total"]);
  const totalKey = _findBodyKey(body, ["total_count", "totalCount", "total", "totalElements", "count"]);

  const offsetKey = _findBodyKey(body, ["offset", "skip", "start"]);
  const pageKey = _findBodyKey(body, ["page", "page_number", "pageNumber"]);
  const sizeKey = _findBodyKey(body, ["page_size", "pageSize", "per_page", "perPage", "limit"]);
  const pageSize = sizeKey && typeof (body as Record<string, unknown>)[sizeKey] === "number"
    ? String((body as Record<string, unknown>)[sizeKey])
    : "10";

  const applyStop = (setter: (v: PaginationStopCondition) => void) => {
    if (hasMoreKey) {
      setter("HAS_MORE");
      patch.hasMoreSource = "BODY";
      patch.hasMorePath = `$.${hasMoreKey}`;
      summary.push(`Stops when: $.${hasMoreKey} is false`);
    } else if (totalHeader) {
      setter("TOTAL_COUNT");
      summary.push(`Stops when: total count from ${totalHeader} header is reached`);
    } else if (totalKey) {
      setter("TOTAL_COUNT");
      summary.push(`Stops when: $.${totalKey} is reached`);
    } else {
      setter("EMPTY_ARRAY");
      summary.push("Stops when: a page comes back empty");
    }
  };

  if (offsetKey) {
    patch.paginationType = "OFFSET_LIMIT";
    patch.offsetParamName = offsetKey;
    patch.limitParamName = sizeKey && sizeKey.toLowerCase() === "limit" ? sizeKey : "limit";
    patch.limitValue = pageSize;
    applyStop((v) => { patch.offsetStopCondition = v as OffsetStopCondition; });
    if (patch.offsetStopCondition === "TOTAL_COUNT") {
      patch.offsetTotalCountSource = totalHeader ? "HEADER" : "BODY";
      if (totalHeader) patch.offsetTotalCountHeader = totalHeader;
      else if (totalKey) patch.offsetTotalCountPath = `$.${totalKey}`;
    }
    summary.unshift(`Offset & limit: ?${offsetKey}=…&${patch.limitParamName}=${pageSize}`);
    return { patch, summary };
  }

  if (pageKey) {
    patch.paginationType = "PAGE_INCREMENT";
    patch.pageParamName = pageKey;
    patch.pageSizeParamName = sizeKey || "page_size";
    patch.pageSizeValue = pageSize;
    const firstPage = typeof (body as Record<string, unknown>)[pageKey] === "number"
      ? String((body as Record<string, unknown>)[pageKey])
      : "1";
    patch.firstPageNumber = firstPage;
    applyStop((v) => { patch.pageStopCondition = v; });
    if (patch.pageStopCondition === "TOTAL_COUNT") {
      patch.totalCountSource = totalHeader ? "HEADER" : "BODY";
      if (totalHeader) patch.totalCountHeader = totalHeader;
      else if (totalKey) patch.totalCountPath = `$.${totalKey}`;
    }
    summary.unshift(`Page number: ?${pageKey}=…&${patch.pageSizeParamName}=${pageSize}`);
    return { patch, summary };
  }

  return null;
};

const isRestDetectAvailable = (s: Stream): boolean => s.paginationType !== "NONE" || !s.paginationEnabled;

function MetadataSourceField({
  label,
  source,
  onSourceChange,
  pathLabel,
  pathValue,
  onPathChange,
  headerValue,
  onHeaderChange,
  headerPlaceholder,
}: {
  label: string;
  source: "BODY" | "HEADER";
  onSourceChange: (v: "BODY" | "HEADER") => void;
  pathLabel: string;
  pathValue: string;
  onPathChange: (v: string) => void;
  headerValue: string;
  onHeaderChange: (v: string) => void;
  headerPlaceholder: string;
}) {
  return (
    <div className="md:col-span-2 rounded-md border p-3 space-y-3">
      <Label className="text-sm">{label}</Label>
      <Select value={source} onValueChange={(v) => onSourceChange(v as "BODY" | "HEADER")}>
        <SelectTrigger>
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="BODY">Response body</SelectItem>
          <SelectItem value="HEADER">Response header</SelectItem>
        </SelectContent>
      </Select>
      {source === "BODY" ? (
        <div>
          <Label>{pathLabel}</Label>
          <Input value={pathValue} onChange={(event) => onPathChange(event.target.value)} />
        </div>
      ) : (
        <div>
          <Label>Response header name</Label>
          <Input
            value={headerValue}
            placeholder={headerPlaceholder}
            onChange={(event) => onHeaderChange(event.target.value)}
          />
          <p className="text-xs text-muted-foreground mt-1">
            Capitalisation must match the API exactly — <code>x-next-cursor</code> will not match{" "}
            <code>X-Next-Cursor</code>.
          </p>
        </div>
      )}
    </div>
  );
}

function StreamPaginationEditor({
  editing,
  updateStream,
  testResult,
}: {
  editing: Stream;
  updateStream: (streamId: string, patch: Partial<Stream>) => void;
  testResult?: {
    response_data?: unknown;
    response_headers?: Record<string, string>;
  };
}) {
  const [detectMsg, setDetectMsg] = useState<string[] | null>(null);
  const [detectErr, setDetectErr] = useState<string | null>(null);

  const runDetect = () => {
    setDetectErr(null);
    setDetectMsg(null);
    if (!testResult) {
      setDetectErr("Run Test Stream first — detection reads that response.");
      return;
    }
    const guess = detectPagination(
      testResult.response_data,
      testResult.response_headers || {},
      editing.method,
      editing.bodyFormat,
    );
    if (!guess) {
      setDetectErr("Could not identify a pagination style from that response. Set it up manually below.");
      return;
    }
    updateStream(editing.id, guess.patch);
    setDetectMsg(guess.summary);
  };
  return (
    <div className="space-y-3">
      {isRestDetectAvailable(editing) && (
        <div className="rounded-md border border-dashed p-3 space-y-2">
          <div className="flex items-center justify-between gap-3">
            <div>
              <Label className="text-sm">Detect pagination</Label>
              <p className="text-xs text-muted-foreground">
                Reads your last Test Stream response. No extra request is sent.
              </p>
            </div>
            <Button type="button" size="sm" onClick={runDetect} disabled={!testResult}>
              Detect
            </Button>
          </div>
          {detectMsg && (
            <div className="rounded-md bg-muted p-2 text-xs space-y-1">
              <p className="font-medium">Applied:</p>
              {detectMsg.map((line) => (
                <p key={line} className="text-muted-foreground">{line}</p>
              ))}
            </div>
          )}
          {detectErr && <p className="text-xs text-destructive">{detectErr}</p>}
        </div>
      )}
      <div className="flex items-center justify-between rounded-md border p-3">
        <div>
          <Label className="text-sm">Enable Pagination</Label>
          <p className="text-xs text-muted-foreground">Set pagination behavior for this stream.</p>
        </div>
        <Switch
          checked={editing.paginationEnabled}
          onCheckedChange={(checked) =>
            updateStream(editing.id, {
              paginationEnabled: checked,
              ...(checked && editing.paginationType === "NONE" ? { paginationType: "PAGE_INCREMENT" } : {}),
            })
          }
        />
      </div>

      {editing.paginationEnabled && (
        <div className="grid gap-3 md:grid-cols-2">
          <div className="md:col-span-2">
            <Label>Pagination Type</Label>
            <Select
              value={editing.paginationType === "NONE" ? "PAGE_INCREMENT" : editing.paginationType}
              onValueChange={(value) => updateStream(editing.id, { paginationType: value as PaginationType })}
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="PAGE_INCREMENT">Page Increment</SelectItem>
                <SelectItem value="CURSOR">Cursor-Based</SelectItem>
                <SelectItem value="OFFSET_LIMIT">Offset / Limit</SelectItem>
                <SelectItem value="NEXT_URL">Next URL</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="md:col-span-2">
            <Label>Safety limit — stop after N pages (optional)</Label>
            <Input
              type="number"
              placeholder="Leave blank for no limit"
              value={editing.maxPages}
              onChange={(event) => updateStream(editing.id, { maxPages: event.target.value })}
            />
            <p className="text-xs text-muted-foreground mt-1">
              A precaution, not a stop condition. Required only when the stop condition relies on API
              metadata (total count or &quot;has more&quot;), since that metadata may be missing at runtime.
            </p>
          </div>

          {editing.paginationType !== "NEXT_URL" && (
            <div className="md:col-span-2">
              {["POST", "PUT", "PATCH"].includes(editing.method) && editing.bodyFormat === "JSON" ? (
                <>
                  <Label>Send pagination values in</Label>
                  <Select
                    value={editing.paginationLocation}
                    onValueChange={(value) =>
                      updateStream(editing.id, { paginationLocation: value as PaginationLocation })
                    }
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="QUERY">URL query parameters</SelectItem>
                      <SelectItem value="BODY">Request body</SelectItem>
                    </SelectContent>
                  </Select>
                  <p className="text-xs text-muted-foreground mt-1">
                    Values are added to your JSON body automatically. Leave your body template as-is.
                  </p>
                </>
              ) : (
                <p className="text-xs text-muted-foreground">
                  Pagination values are sent as URL query parameters.
                  {editing.method === "GET"
                    ? " A GET request cannot carry a body."
                    : " Request-body pagination needs a JSON body format."}
                </p>
              )}
            </div>
          )}

          {editing.paginationType === "PAGE_INCREMENT" && (
            <>
              <div>
                <Label>Page Parameter Name</Label>
                <Input
                  value={editing.pageParamName}
                  onChange={(event) => updateStream(editing.id, { pageParamName: event.target.value })}
                />
              </div>
              <div>
                <Label>Page Size Parameter Name</Label>
                <Input
                  value={editing.pageSizeParamName}
                  onChange={(event) => updateStream(editing.id, { pageSizeParamName: event.target.value })}
                />
              </div>
              <div>
                <Label>Page Size Value</Label>
                <Input
                  type="number"
                  value={editing.pageSizeValue}
                  onChange={(event) => updateStream(editing.id, { pageSizeValue: event.target.value })}
                />
              </div>
              <div>
                <Label>First Page Number</Label>
                <Input
                  type="number"
                  value={editing.firstPageNumber}
                  onChange={(event) => updateStream(editing.id, { firstPageNumber: event.target.value })}
                />
              </div>
              <div>
                <Label>Stop Condition</Label>
                <Select
                  value={editing.pageStopCondition}
                  onValueChange={(value) =>
                    updateStream(editing.id, { pageStopCondition: value as PaginationStopCondition })
                  }
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="EMPTY_ARRAY">Empty Array</SelectItem>
                    <SelectItem value="TOTAL_COUNT">Total Count Field</SelectItem>
                    <SelectItem value="HAS_MORE">API "has more" flag</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              {editing.pageStopCondition === "TOTAL_COUNT" && (
                <MetadataSourceField
                  label="Where is the total count?"
                  source={editing.totalCountSource}
                  onSourceChange={(v) => updateStream(editing.id, { totalCountSource: v })}
                  pathLabel="Total Count JSONPath"
                  pathValue={editing.totalCountPath}
                  onPathChange={(v) => updateStream(editing.id, { totalCountPath: v })}
                  headerValue={editing.totalCountHeader}
                  onHeaderChange={(v) => updateStream(editing.id, { totalCountHeader: v })}
                  headerPlaceholder="X-Total-Count"
                />
              )}
              {editing.pageStopCondition === "HAS_MORE" && (
                <MetadataSourceField
                  label='Where is the "has more" flag?'
                  source={editing.hasMoreSource}
                  onSourceChange={(v) => updateStream(editing.id, { hasMoreSource: v })}
                  pathLabel="Has More JSONPath"
                  pathValue={editing.hasMorePath}
                  onPathChange={(v) => updateStream(editing.id, { hasMorePath: v })}
                  headerValue={editing.hasMoreHeader}
                  onHeaderChange={(v) => updateStream(editing.id, { hasMoreHeader: v })}
                  headerPlaceholder="X-Has-More"
                />
              )}

            </>
          )}

          {editing.paginationType === "CURSOR" && (
            <>
              <div>
                <Label>Cursor Parameter Name</Label>
                <Input
                  value={editing.cursorParamName}
                  onChange={(event) => updateStream(editing.id, { cursorParamName: event.target.value })}
                />
              </div>
              <MetadataSourceField
                label="Where does the next-page token come from?"
                source={editing.cursorSource}
                onSourceChange={(v) => updateStream(editing.id, { cursorSource: v })}
                pathLabel="Cursor Value JSONPath"
                pathValue={editing.cursorValuePath}
                onPathChange={(v) => updateStream(editing.id, { cursorValuePath: v })}
                headerValue={editing.cursorHeader}
                onHeaderChange={(v) => updateStream(editing.id, { cursorHeader: v })}
                headerPlaceholder="X-Next-Cursor"
              />
              <div>
                <Label>Page Size Parameter Name (optional)</Label>
                <Input
                  value={editing.cursorPageSizeParamName}
                  onChange={(event) => updateStream(editing.id, { cursorPageSizeParamName: event.target.value })}
                />
              </div>
              <div>
                <Label>Page Size Value (optional)</Label>
                <Input
                  type="number"
                  value={editing.cursorPageSizeValue}
                  onChange={(event) => updateStream(editing.id, { cursorPageSizeValue: event.target.value })}
                />
              </div>
            </>
          )}

          {editing.paginationType === "NEXT_URL" && (
            <div className="md:col-span-2 rounded-md border p-3 space-y-3">
              <Label className="text-sm">Where does the next link come from?</Label>
              <Select
                value={editing.nextUrlSource}
                onValueChange={(value) => updateStream(editing.id, { nextUrlSource: value as NextUrlSource })}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="BODY">Response body</SelectItem>
                  <SelectItem value="HEADER">Response header</SelectItem>
                  <SelectItem value="LINK_HEADER">Link header (RFC 5988)</SelectItem>
                </SelectContent>
              </Select>
              {editing.nextUrlSource === "BODY" && (
                <div>
                  <Label>Next URL JSONPath or XPath</Label>
                  <Input
                    value={editing.nextUrlPath}
                    onChange={(event) => updateStream(editing.id, { nextUrlPath: event.target.value })}
                  />
                </div>
              )}
              {editing.nextUrlSource === "HEADER" && (
                <div>
                  <Label>Response header name</Label>
                  <Input
                    value={editing.nextUrlHeader}
                    placeholder="X-Next-Url"
                    onChange={(event) => updateStream(editing.id, { nextUrlHeader: event.target.value })}
                  />
                  <p className="text-xs text-muted-foreground mt-1">
                    Capitalisation must match the API exactly.
                  </p>
                </div>
              )}
              {editing.nextUrlSource === "LINK_HEADER" && (
                <div>
                  <Label>Response header name</Label>
                  <Input
                    value={editing.nextUrlHeader}
                    placeholder="Link"
                    onChange={(event) => updateStream(editing.id, { nextUrlHeader: event.target.value })}
                  />
                  <p className="text-xs text-muted-foreground mt-1">
                    Standard <code>Link: &lt;…&gt;; rel="next"</code> header, as used by GitHub, Stripe and Twilio.
                  </p>
                </div>
              )}
            </div>
          )}

          {editing.paginationType === "OFFSET_LIMIT" && (
            <>
              <div>
                <Label>Offset Parameter Name</Label>
                <Input
                  value={editing.offsetParamName}
                  onChange={(event) => updateStream(editing.id, { offsetParamName: event.target.value })}
                />
              </div>
              <div>
                <Label>Limit Parameter Name</Label>
                <Input
                  value={editing.limitParamName}
                  onChange={(event) => updateStream(editing.id, { limitParamName: event.target.value })}
                />
              </div>
              <div>
                <Label>Limit Value</Label>
                <Input
                  type="number"
                  value={editing.limitValue}
                  onChange={(event) => updateStream(editing.id, { limitValue: event.target.value })}
                />
              </div>
              <div>
                <Label>Stop Condition</Label>
                <Select
                  value={editing.offsetStopCondition}
                  onValueChange={(value) =>
                    updateStream(editing.id, { offsetStopCondition: value as OffsetStopCondition })
                  }
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="EMPTY_ARRAY">Empty Array</SelectItem>
                    <SelectItem value="TOTAL_COUNT">Total Count Field</SelectItem>
                    <SelectItem value="HAS_MORE">API "has more" flag</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              {editing.offsetStopCondition === "TOTAL_COUNT" && (
                <MetadataSourceField
                  label="Where is the total count?"
                  source={editing.offsetTotalCountSource}
                  onSourceChange={(v) => updateStream(editing.id, { offsetTotalCountSource: v })}
                  pathLabel="Total Count JSONPath"
                  pathValue={editing.offsetTotalCountPath}
                  onPathChange={(v) => updateStream(editing.id, { offsetTotalCountPath: v })}
                  headerValue={editing.offsetTotalCountHeader}
                  onHeaderChange={(v) => updateStream(editing.id, { offsetTotalCountHeader: v })}
                  headerPlaceholder="X-Total-Count"
                />
              )}
              {editing.offsetStopCondition === "HAS_MORE" && (
                <MetadataSourceField
                  label='Where is the "has more" flag?'
                  source={editing.hasMoreSource}
                  onSourceChange={(v) => updateStream(editing.id, { hasMoreSource: v })}
                  pathLabel="Has More JSONPath"
                  pathValue={editing.hasMorePath}
                  onPathChange={(v) => updateStream(editing.id, { hasMorePath: v })}
                  headerValue={editing.hasMoreHeader}
                  onHeaderChange={(v) => updateStream(editing.id, { hasMoreHeader: v })}
                  headerPlaceholder="X-Has-More"
                />
              )}
            </>
          )}
        </div>
      )}
    </div>
  );
}

function StreamTransformationsEditor({
  editing,
  addTransformationRule,
  updateTransformationRule,
  removeTransformationRule,
}: {
  editing: Stream;
  addTransformationRule: (streamId: string) => void;
  updateTransformationRule: (streamId: string, ruleId: string, patch: Partial<TransformationRule>) => void;
  removeTransformationRule: (streamId: string, ruleId: string) => void;
}) {
  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <div>
          <h3 className="text-sm font-semibold">Transformations</h3>
          <p className="text-xs text-muted-foreground">Optional record shaping before Kafka publish.</p>
        </div>
        <Button size="sm" variant="outline" onClick={() => addTransformationRule(editing.id)}>
          <Plus className="h-4 w-4" /> Add Rule
        </Button>
      </div>

      {editing.transformations.map((rule) => (
        <div key={rule.id} className="rounded-md border p-3 space-y-3">
          <div className="flex items-center justify-between">
            <p className="text-sm font-medium">Transformation Rule</p>
            <Button
              size="icon"
              variant="ghost"
              onClick={() => removeTransformationRule(editing.id, rule.id)}
              aria-label="Remove transformation rule"
            >
              <Trash2 className="h-4 w-4" />
            </Button>
          </div>
          <div className="grid gap-3 md:grid-cols-2">
            <div>
              <Label>Action</Label>
              <Select
                value={rule.action}
                onValueChange={(value) =>
                  updateTransformationRule(editing.id, rule.id, { action: value as TransformationAction })
                }
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="ADD_FIELD">Add Field</SelectItem>
                  <SelectItem value="REMOVE_FIELD">Remove Field</SelectItem>
                  <SelectItem value="SET_FROM_ATTRIBUTE">Set from Extracted Attribute</SelectItem>
                </SelectContent>
              </Select>
            </div>

            {(rule.action === "ADD_FIELD" ||
              rule.action === "REMOVE_FIELD" ||
              rule.action === "SET_FROM_ATTRIBUTE") && (
              <div>
                <Label>Field Name</Label>
                <Input
                  value={rule.fieldName}
                  onChange={(event) =>
                    updateTransformationRule(editing.id, rule.id, { fieldName: event.target.value })
                  }
                />
              </div>
            )}

            {rule.action === "ADD_FIELD" && (
              <div>
                <Label>Field Value</Label>
                <Input
                  value={rule.staticValue}
                  onChange={(event) =>
                    updateTransformationRule(editing.id, rule.id, { staticValue: event.target.value })
                  }
                />
              </div>
            )}

            {rule.action === "SET_FROM_ATTRIBUTE" && (
              <div>
                <Label>Extracted Attribute</Label>
                <Input
                  value={rule.attributeName}
                  onChange={(event) =>
                    updateTransformationRule(editing.id, rule.id, { attributeName: event.target.value })
                  }
                />
              </div>
            )}
          </div>
        </div>
      ))}
    </div>
  );
}

function StreamRoutingEditor({
  editing,
  streams,
  streamGraph,
  addRoutingRule,
  updateRoutingRule,
  removeRoutingRule,
  updateStream,
  createParallelBranch,
  attachParallelChild,
  detachParallelChild,
  setEditingStream,
}: {
  editing: Stream;
  streams: Stream[];
  streamGraph: ReturnType<typeof buildStreamGraph>;
  addRoutingRule: (streamId: string) => void;
  updateRoutingRule: (streamId: string, ruleId: string, patch: Partial<RoutingRule>) => void;
  removeRoutingRule: (streamId: string, ruleId: string) => void;
  updateStream: (id: string, patch: Partial<Stream>) => void;
  createParallelBranch: (parentId: string) => void;
  attachParallelChild: (parentId: string, childId: string) => void;
  detachParallelChild: (childId: string) => void;
  setEditingStream: (streamId: string) => void;
}) {
  const [selectedExistingParallelChild, setSelectedExistingParallelChild] = useState("NONE");
  const targetOptions = streams.filter((stream) => stream.id !== editing.id);
  const parallelChildren = (streamGraph.childrenByParent.get(editing.id) || [])
    .filter((edge) => edge.kind === "fanout")
    .map((edge) => streams.find((stream) => stream.id === edge.childId))
    .filter((stream): stream is Stream => Boolean(stream));
  const descendantIds = useMemo(() => {
    const found = new Set<string>();
    const visit = (parentId: string) => {
      for (const edge of streamGraph.childrenByParent.get(parentId) || []) {
        if (found.has(edge.childId)) continue;
        found.add(edge.childId);
        visit(edge.childId);
      }
    };
    visit(editing.id);
    return found;
  }, [editing.id, streamGraph]);
  const attachableParallelTargets = targetOptions.filter((stream) => {
    if (descendantIds.has(stream.id)) return false;
    const incoming = streamGraph.incomingByChild.get(stream.id) || [];
    return incoming.length === 0;
  });
  const hasRouteToStream = editing.routingRules.some(
    (rule) => rule.destination === "ROUTE_TO_STREAM",
  );
  const routeTargetOptions = targetOptions.filter((stream) => !stream.parentStreamId || stream.parentStreamId === editing.id);

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <div>
          <h3 className="text-sm font-semibold">Record Rules</h3>
          <p className="text-xs text-muted-foreground">
            Include, exclude, or route records. Route-to-stream rules send matched records to child streams.
          </p>
        </div>
        <Button size="sm" variant="outline" onClick={() => addRoutingRule(editing.id)}>
          <Plus className="h-4 w-4" /> Add Rule
        </Button>
      </div>

      {editing.routingRules.map((rule) => (
        <div key={rule.id} className="rounded-md border p-3 space-y-3">
          <div className="flex items-center justify-between">
            <p className="text-sm font-medium">Routing Rule</p>
            <Button
              size="icon"
              variant="ghost"
              onClick={() => removeRoutingRule(editing.id, rule.id)}
              aria-label="Remove routing rule"
            >
              <Trash2 className="h-4 w-4" />
            </Button>
          </div>
          <div className="grid gap-3 md:grid-cols-2">
            <div>
              <Label>Attribute Name</Label>
              <Input
                value={rule.attributeName}
                onChange={(event) =>
                  updateRoutingRule(editing.id, rule.id, { attributeName: event.target.value })
                }
              />
            </div>
            <div>
              <Label>Condition</Label>
              <Select
                value={rule.condition}
                onValueChange={(value) =>
                  updateRoutingRule(editing.id, rule.id, { condition: value as RoutingCondition })
                }
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="equals">equals</SelectItem>
                  <SelectItem value="contains">contains</SelectItem>
                  <SelectItem value="starts_with">starts_with</SelectItem>
                  <SelectItem value="ends_with">ends_with</SelectItem>
                  <SelectItem value="regex">regex</SelectItem>
                  <SelectItem value="is_empty">is_empty</SelectItem>
                </SelectContent>
              </Select>
            </div>

            {rule.condition !== "is_empty" && (
              <div>
                <Label>Value</Label>
                <Input
                  value={rule.value}
                  onChange={(event) => updateRoutingRule(editing.id, rule.id, { value: event.target.value })}
                />
              </div>
            )}

            <div>
              <Label>Action</Label>
              <Select
                value={rule.destination}
                onValueChange={(value) =>
                  updateRoutingRule(editing.id, rule.id, {
                    destination: value as RoutingDestination,
                    targetStreamId: value === "ROUTE_TO_STREAM" ? rule.targetStreamId : "",
                  })
                }
              >
                <SelectTrigger>
                <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="INCLUDE">Include</SelectItem>
                  <SelectItem value="EXCLUDE">Exclude</SelectItem>
                  <SelectItem value="ROUTE_TO_STREAM">Route to Stream</SelectItem>
                </SelectContent>
              </Select>
            </div>

            {rule.destination === "ROUTE_TO_STREAM" && (
              <div>
                <Label>Target Child Stream</Label>
                <Select
                  value={rule.targetStreamId || "NONE"}
                  onValueChange={(value) =>
                    updateRoutingRule(editing.id, rule.id, {
                      targetStreamId: value === "NONE" ? "" : value,
                    })
                  }
                  >
                    <SelectTrigger>
                      <SelectValue placeholder="Select stream" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="NONE">Select stream</SelectItem>
                    {routeTargetOptions.map((stream) => (
                      <SelectItem key={stream.id} value={stream.id}>
                        {stream.name || stream.id}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            )}
          </div>
        </div>
      ))}

      {hasRouteToStream && (
        <div className="rounded-md border border-amber-500/40 bg-amber-500/5 p-3 space-y-3">
          <div>
            <p className="text-sm font-medium text-amber-600">Records that match no route</p>
            <p className="text-xs text-muted-foreground">
              Records matching none of the routes above go here. Default: drop them.
            </p>
          </div>
          <div className="grid gap-3 md:grid-cols-2">
            <div>
              <Label>Default Action</Label>
              <Select
                value={editing.defaultRouteAction}
                onValueChange={(value) =>
                  updateStream(editing.id, {
                    defaultRouteAction: value as DefaultRouteAction,
                    defaultRouteTargetStreamId:
                      value === "ROUTE_TO_STREAM" ? editing.defaultRouteTargetStreamId : "",
                  })
                }
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="DROP">Drop unmatched</SelectItem>
                  <SelectItem value="ROUTE_TO_STREAM">Route to default stream</SelectItem>
                </SelectContent>
              </Select>
            </div>
            {editing.defaultRouteAction === "ROUTE_TO_STREAM" && (
              <div>
                <Label>Default Target Stream</Label>
                <Select
                  value={editing.defaultRouteTargetStreamId || "NONE"}
                  onValueChange={(value) =>
                    updateStream(editing.id, {
                      defaultRouteTargetStreamId: value === "NONE" ? "" : value,
                    })
                  }
                >
                  <SelectTrigger>
                    <SelectValue placeholder="Select stream" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="NONE">Select stream</SelectItem>
                    {targetOptions.map((stream) => (
                      <SelectItem key={stream.id} value={stream.id}>
                        {stream.name || stream.id}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            )}
          </div>
        </div>
      )}

      <div className="rounded-md border border-blue-500/30 p-3 space-y-3">
          <div>
            <p className="text-sm font-medium text-blue-600">Parallel Branches</p>
            <p className="text-xs text-muted-foreground">
              A copy of <span className="font-medium">every</span> record is sent to each branch — the record rules above do not apply here. Each child gets a real NiFi branch.
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <Button variant="outline" onClick={() => createParallelBranch(editing.id)}>
              <Plus className="h-4 w-4" /> Create new parallel branch
            </Button>
            <span className="text-xs text-muted-foreground">or attach an existing independent stream below</span>
          </div>
          <div className="space-y-2">
            <div>
              <Label>Attach Existing Stream</Label>
              <Select value={selectedExistingParallelChild} onValueChange={setSelectedExistingParallelChild}>
                <SelectTrigger>
                  <SelectValue placeholder="Select an existing root stream" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="NONE">Select stream</SelectItem>
                  {attachableParallelTargets.map((stream) => (
                    <SelectItem key={stream.id} value={stream.id}>
                      {stream.name || stream.id}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <p className="mt-1 text-[11px] text-muted-foreground">
                Only independent streams can be attached here. Streams already used under another parent are excluded.
              </p>
            </div>
            <div className="flex justify-end">
              <Button
                variant="outline"
                disabled={selectedExistingParallelChild === "NONE"}
                onClick={() => {
                  if (selectedExistingParallelChild === "NONE") return;
                  attachParallelChild(editing.id, selectedExistingParallelChild);
                  setSelectedExistingParallelChild("NONE");
                }}
              >
                Attach Stream
              </Button>
            </div>
          </div>
          {parallelChildren.length > 0 ? (
            <div className="space-y-2">
              {parallelChildren.map((child) => (
                <div
                  key={child.id}
                  className="flex items-center justify-between gap-3 rounded-md border bg-muted/20 px-3 py-2"
                >
                  <button
                    type="button"
                    onClick={() => setEditingStream(child.id)}
                    className="min-w-0 flex-1 text-left transition-colors hover:text-primary"
                  >
                    <div className="text-sm font-medium">{child.name || child.id}</div>
                    <div className="text-xs text-muted-foreground">
                      {child.requestEnabled ? "Makes its own request" : "Uses parent records directly"}
                    </div>
                  </button>
                  <div className="flex items-center gap-2">
                    {child.entity && (
                      <Badge variant="secondary" className="border-0 bg-warning-muted text-warning">
                        Entity
                      </Badge>
                    )}
                    <Badge variant="outline" className="text-blue-600">Branch child</Badge>
                    <Button size="sm" variant="ghost" onClick={() => detachParallelChild(child.id)}>
                      Remove
                    </Button>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="rounded-md border border-dashed p-3 text-xs text-muted-foreground">
              No branch children yet. Add one to create a new downstream stream from this parent.
            </div>
          )}
        </div>
    </div>
  );
}

// ─── StreamHierarchyList Component ──────────────────────────────────────────

type StreamNode = {
  id: string;
  name: string;
  branchingMode?: BranchingMode;
  isPrimary: boolean;
  endpointPath: string;
  parentStreamId: string;
  parentField: string;
  injectAs: string;
  injectFieldName: string;
  injectTemplate: string;
  routeSource?: RouteSource | null;
  entity?: EntityConfig | null;
  routes?: Route[];
  routingRules?: RoutingRule[];
  defaultRouteAction?: DefaultRouteAction;
  defaultRouteTargetStreamId?: string;
};

type StreamTestState = {
  status: "idle" | "running" | "success" | "error";
  result?: {
    ok: boolean;
    status_code?: number;
    url?: string;
    error?: string;
    response_data?: unknown;
    response_data_type?: "json" | "text" | null;
    response_data_truncated?: boolean;
  };
};

function StreamHierarchyList({
  streams,
  editingStream,
  setEditingStream,
  onCreateParallelBranch,
}: {
  streams: StreamNode[];
  editingStream: string;
  setEditingStream: (id: string) => void;
  onCreateParallelBranch: (parentId: string) => void;
}) {
  const extractPathVariables = (value: string): string[] => {
    const found: string[] = [];
    const seen = new Set<string>();
    PLACEHOLDER_PATTERN.lastIndex = 0;
    let match: RegExpExecArray | null;
    while ((match = PLACEHOLDER_PATTERN.exec(value || "")) !== null) {
      const variable = (match[1] ?? "").trim();
      if (!variable || seen.has(variable)) continue;
      seen.add(variable);
      found.push(variable);
    }
    return found;
  };

  const graph = buildStreamGraph(streams as Stream[]);
  const rootStreams = streams.filter((s) => !(graph.incomingByChild.get(s.id) || []).length);
  const childMap = new Map<string, StreamNode[]>();
  for (const edge of graph.edges) {
    const child = streams.find((s) => s.id === edge.childId);
    if (!child) continue;
    if (!childMap.has(edge.parentId)) childMap.set(edge.parentId, []);
    const siblings = childMap.get(edge.parentId)!;
    if (!siblings.some((item) => item.id === child.id)) {
      siblings.push(child);
    }
  }
  const visitedIds = new Set<string>();

  const renderNode = (stream: StreamNode, depth: number, parentLabel?: string) => {
    visitedIds.add(stream.id);
    const children = childMap.get(stream.id) || [];
    const pathVariables = extractPathVariables(stream.endpointPath || "");
    const incomingEdge = graph.incomingByChild.get(stream.id)?.[0];
    const isRouteChild = incomingEdge?.kind === "route" || incomingEdge?.kind === "default_route";
    const isParallelChild = incomingEdge?.kind === "fanout";
    const isEntity = Boolean(stream.entity);
    const branchingMode = getBranchingMode(stream as Stream, graph);

    return (
      <div key={stream.id} className={cn(depth > 0 && "relative pl-5")}>
        {depth > 0 && (
          <>
            <div className="absolute left-1 top-0 bottom-0 border-l border-border/60" />
            <div className="absolute left-1 top-5 w-4 border-t border-border/60" />
          </>
        )}
        <div
          className={cn(
            "rounded-lg border transition-colors",
            editingStream === stream.id ? "border-primary bg-primary/5 shadow-sm" : "border-border hover:bg-muted/40",
          )}
        >
          <button onClick={() => setEditingStream(stream.id)} className="w-full p-3 text-left">
            {parentLabel && (
              <div className="mb-1 flex items-center gap-1 text-[11px] text-muted-foreground">
                <GitBranch className={cn("h-3 w-3", isRouteChild ? "text-purple-500" : "text-blue-500")} />
                <span className="truncate">
                  {parentLabel}
                  <span className={cn("ml-1", isRouteChild ? "text-purple-500/80" : "text-blue-500/80")}>
                    {isRouteChild ? "route" : "branch"}
                  </span>
                </span>
              </div>
            )}
            <div className="flex items-start justify-between gap-2">
              <div className="min-w-0">
                <div className="truncate text-sm font-semibold">{stream.name}</div>
                <div className="mt-0.5 truncate font-mono text-xs text-muted-foreground">
                  {stream.endpointPath || "/path"}
                </div>
              </div>
              <div className="flex flex-wrap items-center justify-end gap-1">
                {isEntity && (
                  <Badge variant="secondary" className="border-0 bg-warning-muted text-warning text-[10px]">
                    <Star className="mr-0.5 h-2.5 w-2.5 fill-current" /> Entity
                  </Badge>
                )}
                {isRouteChild && <Badge variant="outline" className="text-[10px] text-purple-600">Route</Badge>}
                {isParallelChild && <Badge variant="outline" className="text-[10px] text-blue-600">Branch</Badge>}
                {stream.waitForAllRecordsBeforeDownstream && (
                  <Badge variant="outline" className="text-[10px] text-amber-600">
                    Wait before downstream
                  </Badge>
                )}
                {children.length > 0 && (
                  <Badge
                    variant="outline"
                    className={cn(
                      "text-[10px]",
                      branchingMode === "parallel" && "text-blue-600",
                      branchingMode === "conditional" && "text-purple-600",
                    )}
                  >
                    {children.length} {branchingMode === "parallel" ? "branch" : "child"}
                    {children.length === 1 ? "" : "es"}
                  </Badge>
                )}
              </div>
            </div>
            {pathVariables.length > 0 && (
              <div className="mt-1 text-[11px] text-muted-foreground">
                Uses: <span className="font-mono">{pathVariables.join(", ")}</span>
              </div>
            )}
          </button>
          <div className="border-t px-3 py-2">
            <Button size="sm" variant="ghost" className="h-7 px-2 text-xs" onClick={() => onCreateParallelBranch(stream.id)}>
              <GitBranch className="h-3 w-3" /> Add parallel branch
            </Button>
          </div>
        </div>
        {children.length > 0 && (
          <div className="mt-2 space-y-2">
            {children.map((child) => renderNode(child, depth + 1, stream.name))}
          </div>
        )}
      </div>
    );
  };

  return (
    <div className="space-y-2">
      {rootStreams.map((stream) => renderNode(stream, 0))}
      {streams
        .filter((stream) => !visitedIds.has(stream.id))
        .map((stream) => renderNode(stream, 0))}
    </div>
  );
}
