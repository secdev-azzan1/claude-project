// Pagination for the http adapter, at the alpha UI's depth.
//
// The alpha asked three questions the prototype was skipping, and they are the
// ones that decide whether a paged read terminates at all:
//
//   1. HOW does the next page get addressed? (page / cursor / offset / next URL)
//   2. WHEN does it stop? — and "stop" is not free text. Empty page, a total
//      count, or an explicit "has more" flag are the only three answers, and the
//      last two need a SOURCE.
//   3. WHERE does that metadata live — in the body (a JSONPath) or in a response
//      header? A header name is its own field rather than a path that happens
//      to start with a capital letter; runtime lookup is case-insensitive.
//
// Cursor pagination has no stop condition control on purpose: it stops when the
// response carries no next cursor, which is a property of the response, not a
// choice. Saying so is more honest than offering a dropdown that cannot change
// the behaviour.
//
// Everything is stored under `config.pagination.fields` as strings, matching the
// shape the seeds and the naming walk already read.

import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Field, SectionLabel } from "@/components/form/Field";
import type { FlowBlock } from "@/prototype/types";

export const PAGINATION_TYPES = [
  { value: "none", label: "No pagination" },
  { value: "page", label: "Page increment" },
  { value: "cursor", label: "Cursor-based" },
  { value: "offset", label: "Offset / limit" },
  { value: "next_url", label: "Next-URL" },
];

/** The three answers to "when does it stop?" — shared by page and offset. */
const STOP_CONDITIONS = [
  { value: "empty_response", label: "Empty page", hint: "Stop when a page comes back with no records. Always safe; costs one extra request." },
  { value: "total_count", label: "Total count field", hint: "Stop once the records seen reach a total the API reports." },
  { value: "has_more", label: 'API "has more" flag', hint: "Stop when the API's own boolean says there is nothing after this page." },
];

const METADATA_SOURCES = [
  { value: "body", label: "Response body" },
  { value: "header", label: "Response header" },
];

interface PaginationConfig {
  type?: string;
  fields?: Record<string, string>;
}

export interface PaginationFieldsProps {
  block: FlowBlock;
  locked: boolean;
  onPatchConfig: (blockId: string, patch: Record<string, unknown>) => void;
}

export function PaginationFields({ block, locked, onPatchConfig }: PaginationFieldsProps) {
  const pagination = (block.config.pagination as PaginationConfig | undefined) ?? { type: "none", fields: {} };
  const fields = pagination.fields ?? {};
  const type = pagination.type ?? "none";
  const isWrite = block.mode === "write";
  const paginationTypes = isWrite
    ? PAGINATION_TYPES.filter((option) => ["none", "page", "offset"].includes(option.value))
    : PAGINATION_TYPES;

  const patch = (next: Partial<PaginationConfig>) =>
    onPatchConfig(block.id, {
      pagination: { ...pagination, ...next, fields: { ...fields, ...(next.fields ?? {}) } },
    });

  const field = (key: string, label: string, placeholder: string, opts?: { width?: string; mono?: boolean }) => (
    <div key={key} className="space-y-1.5">
      <Label className="text-xs font-normal text-muted-foreground">{label}</Label>
      <Input
        className={`h-8 ${opts?.width ?? "w-44"} ${opts?.mono === false ? "" : "font-mono"} text-xs`}
        value={fields[key] ?? ""}
        placeholder={placeholder}
        disabled={locked}
        onChange={(e) => patch({ fields: { [key]: e.target.value } })}
      />
    </div>
  );

  /**
   * "Where does this metadata come from?" — the body or a named header. Rendered
   * for whichever stop condition needs it, with its own key prefix so a page
   * total and an offset total never share a field.
   */
  const metadataSource = (prefix: string, label: string, pathPlaceholder: string, headerPlaceholder: string) => {
    const source = fields[`${prefix}Source`] ?? "body";
    return (
      <div className="space-y-2.5 rounded-lg bg-card p-3 ring-1 ring-inset ring-border/60">
        <Field
          label={label}
          info={source === "header" ? "Header lookup is case-insensitive." : undefined}
        >
          <Select value={source} disabled={locked} onValueChange={(v) => patch({ fields: { [`${prefix}Source`]: v } })}>
            <SelectTrigger className="h-8 w-48 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {METADATA_SOURCES.map((s) => (
                <SelectItem key={s.value} value={s.value}>
                  {s.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </Field>
        {source === "body"
          ? field(`${prefix}Path`, "JSONPath", pathPlaceholder, { width: "w-64" })
          : field(`${prefix}Header`, "Header name", headerPlaceholder, { width: "w-64" })}
      </div>
    );
  };

  const stopSelect = (key: string) => {
    const value = fields[key] ?? "empty_response";
    const meta = STOP_CONDITIONS.find((s) => s.value === value);
    return (
      <Field
        label="Stop condition"
        // This hint describes the option currently selected, so it changes as
        // the user chooses — that makes it derived state, and derived state
        // stays inline.
        hint={meta?.hint}
      >
        <Select value={value} disabled={locked} onValueChange={(v) => patch({ fields: { [key]: v } })}>
          <SelectTrigger className="h-8 w-56 text-xs">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {STOP_CONDITIONS.map((s) => (
              <SelectItem key={s.value} value={s.value}>
                {s.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </Field>
    );
  };

  const safetyLimit = (required: boolean) => (
    <Field
      label={required ? "Maximum pages (required safety limit)" : "Maximum pages (optional safety limit)"}
      hint={
        required
          ? "Required for metadata-based stopping so a missing field or header cannot loop forever."
          : "Leave blank to rely on the API's natural end signal."
      }
    >
      <Input
        className="h-8 w-36 font-mono text-xs"
        value={fields.maxPages ?? ""}
        placeholder="100"
        disabled={locked}
        onChange={(event) => patch({ fields: { maxPages: event.target.value } })}
      />
    </Field>
  );

  const pageStop = fields.stop ?? "empty_response";
  const offsetStop = fields.offsetStop ?? "empty_response";
  const nextUrlSource = fields.nextUrlSource ?? "link_header";

  return (
    <div className="space-y-2.5">
      <Field label="Pagination">
        <Select value={type} disabled={locked} onValueChange={(v) => patch({ type: v })}>
          <SelectTrigger className="h-8 w-48 text-xs">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {paginationTypes.map((p) => (
              <SelectItem key={p.value} value={p.value}>
                {p.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </Field>

      {type === "page" && (
        <div className="space-y-3 rounded-xl bg-muted/40 p-3 ring-1 ring-inset ring-border/50">
          <SectionLabel>Addressing</SectionLabel>
          <div className="flex flex-wrap gap-3">
            {field("pageParam", "Page parameter", "page")}
            {field("sizeParam", "Page size parameter", "size")}
            {field("sizeValue", "Page size", "100", { width: "w-24" })}
            {field("firstPage", "First page number", "1", { width: "w-28" })}
          </div>
          {stopSelect("stop")}
          {pageStop === "total_count" &&
            metadataSource("totalCount", "Where is the total count?", "$.meta.total", "X-Total-Count")}
          {pageStop === "has_more" &&
            metadataSource("hasMore", 'Where is the "has more" flag?', "$.meta.hasMore", "X-Has-More")}
          {safetyLimit(pageStop === "total_count" || pageStop === "has_more")}
        </div>
      )}

      {type === "cursor" && (
        <div className="space-y-3 rounded-xl bg-muted/40 p-3 ring-1 ring-inset ring-border/50">
          <SectionLabel>Addressing</SectionLabel>
          <div className="flex flex-wrap gap-3">
            {field("cursorParam", "Cursor parameter", "cursor")}
            {field("cursorSizeParam", "Page size parameter (optional)", "limit")}
            {field("cursorSizeValue", "Page size (optional)", "100", { width: "w-24" })}
          </div>
          {metadataSource("cursor", "Where does the next-page token come from?", "$.nextCursor", "X-Next-Cursor")}
          <p className="text-xs leading-relaxed text-muted-foreground">
            Cursor paging has no stop condition to choose: it ends when a response carries no next token.
          </p>
          {safetyLimit(false)}
        </div>
      )}

      {type === "offset" && (
        <div className="space-y-3 rounded-xl bg-muted/40 p-3 ring-1 ring-inset ring-border/50">
          <SectionLabel>Addressing</SectionLabel>
          <div className="flex flex-wrap gap-3">
            {field("offsetParam", "Offset parameter", "offset")}
            {field("limitParam", "Limit parameter", "limit")}
            {field("limitValue", "Limit", "100", { width: "w-24" })}
          </div>
          {stopSelect("offsetStop")}
          {offsetStop === "total_count" &&
            metadataSource("offsetTotalCount", "Where is the total count?", "$.meta.total", "X-Total-Count")}
          {offsetStop === "has_more" &&
            metadataSource("hasMore", 'Where is the "has more" flag?', "$.meta.hasMore", "X-Has-More")}
          {safetyLimit(offsetStop === "total_count" || offsetStop === "has_more")}
        </div>
      )}

      {type === "next_url" && (
        <div className="space-y-3 rounded-xl bg-muted/40 p-3 ring-1 ring-inset ring-border/50">
          <Field
            label="Where does the next link come from?"
            info={
              nextUrlSource === "link_header" ? (
                <>
                  The standard <code className="font-mono">Link: &lt;…&gt;; rel="next"</code> header, as used by GitHub,
                  Stripe and Twilio.
                </>
              ) : nextUrlSource === "header" ? (
                "Header lookup is case-insensitive."
              ) : undefined
            }
          >
            <Select value={nextUrlSource} disabled={locked} onValueChange={(v) => patch({ fields: { nextUrlSource: v } })}>
              <SelectTrigger className="h-8 w-64 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="link_header">Link header (RFC 5988)</SelectItem>
                <SelectItem value="header">Response header</SelectItem>
                <SelectItem value="body">Response body</SelectItem>
              </SelectContent>
            </Select>
          </Field>

          {nextUrlSource === "link_header" && field("linkRel", "Link rel", "next", { width: "w-32" })}
          {nextUrlSource === "header" && field("nextUrlHeader", "Header name", "X-Next-Url", { width: "w-64" })}
          {nextUrlSource === "body" && field("nextUrlPath", "Next URL JSONPath", "$.next", { width: "w-64" })}

          <p className="text-xs leading-relaxed text-muted-foreground">
            Stops when the response carries no next link — there is nothing else to decide.
          </p>
          {safetyLimit(false)}
        </div>
      )}
    </div>
  );
}
