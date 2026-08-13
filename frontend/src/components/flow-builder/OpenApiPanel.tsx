// The HTTP adapter's OpenAPI documentation attachment — rendered at the top
// of HttpSettings, above Method/Path, because whatever is attached here
// changes what those fields below can offer (the path combobox in
// OpenApiPathCombobox.tsx).
//
// The doc must never lock the user in: attaching one only adds a picker on
// top of the existing manual Method/Path fields and the "Existing service |
// Set up here" toggle in Identity — nothing here disables or requires them,
// and Remove clears exactly the three config keys this panel owns
// (openapiSpecId, openapiSpecTitle, openapiOperationId) and nothing else.

import { useRef } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { AlertTriangle, FileText, Loader2, Upload, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { getOpenApiSpec, parseOpenApiDocument, type OpenApiSpecSummary } from "@/prototype/openapiClient";
import type { FlowBlock } from "@/prototype/types";

export interface OpenApiPanelProps {
  block: FlowBlock;
  locked: boolean;
  onPatchConfig: (blockId: string, patch: Record<string, unknown>) => void;
}

export function OpenApiPanel({ block, locked, onPatchConfig }: OpenApiPanelProps) {
  const queryClient = useQueryClient();
  const fileInputRef = useRef<HTMLInputElement>(null);

  const specId = (block.config.openapiSpecId as string | undefined) || undefined;
  const specTitleCfg = (block.config.openapiSpecTitle as string | undefined) || undefined;

  const specQuery = useQuery({
    queryKey: ["openapi-spec", specId],
    queryFn: () => getOpenApiSpec(specId!),
    enabled: !!specId,
    staleTime: 60_000,
  });

  const uploadMutation = useMutation({
    mutationFn: (file: File) => parseOpenApiDocument(file),
    onSuccess: (summary: OpenApiSpecSummary) => {
      queryClient.setQueryData(["openapi-spec", summary.specId], summary);
      onPatchConfig(block.id, {
        openapiSpecId: summary.specId,
        openapiSpecTitle: summary.title,
        openapiOperationId: undefined,
      });
      toast.success(
        `Imported "${summary.title || "OpenAPI document"}" — ${summary.operationsCount} operation${summary.operationsCount === 1 ? "" : "s"}.`,
      );
      if (summary.warnings.length > 0) {
        toast.warning(summary.warnings[0], summary.warnings.length > 1 ? { description: `+${summary.warnings.length - 1} more warning(s) below.` } : undefined);
      }
    },
    onError: (err: unknown) => {
      toast.error(err instanceof Error ? err.message : "Failed to parse the OpenAPI document.");
    },
  });

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    e.target.value = ""; // allow re-selecting the same file after a failure
    if (!file) return;
    uploadMutation.mutate(file);
  };

  const handleRemove = () => {
    onPatchConfig(block.id, {
      openapiSpecId: undefined,
      openapiSpecTitle: undefined,
      openapiOperationId: undefined,
    });
    toast.success("OpenAPI document removed.");
  };

  const hiddenInput = (
    <input
      ref={fileInputRef}
      type="file"
      accept=".json,application/json"
      className="hidden"
      onChange={handleFileChange}
    />
  );

  if (!specId) {
    return (
      <div className="space-y-2 rounded-md border border-dashed bg-muted/20 p-3">
        {hiddenInput}
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div className="flex items-center gap-2">
            <FileText className="h-4 w-4 shrink-0 text-muted-foreground" />
            <div>
              <p className="text-sm font-semibold">API documentation</p>
              <p className="text-xs text-muted-foreground">
                Upload an OpenAPI 3.0/3.1 JSON document to pick Method and Path from it below — optional.
              </p>
            </div>
          </div>
          <Button
            size="sm"
            variant="outline"
            className="h-8 shrink-0 gap-1.5 text-xs"
            disabled={locked || uploadMutation.isPending}
            onClick={() => fileInputRef.current?.click()}
          >
            {uploadMutation.isPending ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Upload className="h-3.5 w-3.5" />}
            {uploadMutation.isPending ? "Parsing…" : "Upload"}
          </Button>
        </div>
      </div>
    );
  }

  const summary = specQuery.data;
  const title = summary?.title || specTitleCfg || "OpenAPI document";
  const version = summary?.version;
  const opsCount = summary?.operationsCount;
  const warnings = summary?.warnings ?? [];

  return (
    <div className="space-y-2 rounded-md border bg-muted/20 p-3">
      {hiddenInput}
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-2">
          <FileText className="h-4 w-4 shrink-0 text-muted-foreground" />
          <p className="min-w-0 truncate text-sm">
            <span className="font-semibold">{title}</span>
            {specQuery.isLoading ? (
              <span className="text-muted-foreground"> · loading…</span>
            ) : specQuery.isError ? (
              <span className="text-muted-foreground"> · could not refresh document details</span>
            ) : (
              <span className="text-muted-foreground">
                {" "}
                · {version ? `v${version}` : "no version"} · {opsCount ?? 0} operation{opsCount === 1 ? "" : "s"}
              </span>
            )}
          </p>
        </div>
        <Button
          size="sm"
          variant="ghost"
          className="h-8 shrink-0 gap-1.5 text-xs text-destructive hover:bg-destructive/10 hover:text-destructive"
          disabled={locked}
          onClick={handleRemove}
        >
          <X className="h-3.5 w-3.5" /> Remove
        </Button>
      </div>

      {warnings.length > 0 && (
        <ul className="space-y-0.5 rounded-md border border-warning/30 bg-warning/10 px-2.5 py-1.5">
          {warnings.map((w, i) => (
            <li key={i} className="flex items-start gap-1.5 text-xs text-warning">
              <AlertTriangle className="mt-0.5 h-3 w-3 shrink-0" /> {w}
            </li>
          ))}
        </ul>
      )}

      <p className="text-xs text-muted-foreground">
        Endpoints below can be picked from the documentation — you can still use an Application Service or type paths
        manually.
      </p>
    </div>
  );
}
