type SearchableSchemaArtifact = {
  artifact_id?: string | null;
  stream?: string | null;
  namespace?: string | null;
  versions?: Array<{
    version?: number | null;
    status?: string | null;
  }>;
};

type SearchableFlowDestination = {
  stream_id?: string | null;
  stream_name?: string | null;
  topic?: string | null;
  schema_artifact_id?: string | null;
};

type SearchableFlow = {
  name?: string | null;
  source_type?: string | null;
  state?: string | null;
  primary_stream_id?: string | null;
  primary_stream_name?: string | null;
  kafka_topic?: string | null;
  schema_artifact_id?: string | null;
  entity_destinations?: SearchableFlowDestination[] | null;
};

type SchemaArtifactStatusFilter = {
  verified: boolean;
  unverified: boolean;
};

const normalizeQuery = (query: string) => query.trim().toLowerCase();

const includesQuery = (values: Array<unknown>, query: string) =>
  values.some((value) => String(value ?? "").toLowerCase().includes(query));

const latestSchemaStatus = (artifact: SearchableSchemaArtifact) =>
  artifact.versions?.[artifact.versions.length - 1]?.status || "";

const matchesSchemaStatusFilter = (
  artifact: SearchableSchemaArtifact,
  filter?: SchemaArtifactStatusFilter,
) => {
  if (!filter || filter.verified === filter.unverified) return true;

  const isVerified = latestSchemaStatus(artifact) === "Verified";
  return filter.verified ? isVerified : !isVerified;
};

export function filterSchemaArtifacts<T extends SearchableSchemaArtifact>(
  artifacts: T[],
  query: string,
  statusFilter?: SchemaArtifactStatusFilter,
): T[] {
  const normalized = normalizeQuery(query);

  return artifacts.filter((artifact) =>
    matchesSchemaStatusFilter(artifact, statusFilter) &&
    (!normalized ||
      includesQuery(
        [
          artifact.artifact_id,
          artifact.stream,
          artifact.namespace,
          ...(artifact.versions || []).flatMap((version) => [version.version, version.status]),
        ],
        normalized,
      )),
  );
}

export function filterFlowRows<T extends SearchableFlow>(flows: T[], query: string): T[] {
  const normalized = normalizeQuery(query);
  if (!normalized) return flows;

  return flows.filter((flow) =>
    includesQuery(
      [
        flow.name,
        flow.source_type,
        flow.state,
        flow.primary_stream_id,
        flow.primary_stream_name,
        flow.kafka_topic,
        flow.schema_artifact_id,
        ...(flow.entity_destinations || []).flatMap((destination) => [
          destination.stream_id,
          destination.stream_name,
          destination.topic,
          destination.schema_artifact_id,
        ]),
      ],
      normalized,
    ),
  );
}
