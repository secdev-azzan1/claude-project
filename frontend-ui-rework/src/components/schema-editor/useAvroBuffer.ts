// The structured⇄raw sync buffer, adopted wholesale from the original app's
// Schemas page (lines 454–596).
//
// The contract that matters:
//   1. an `AvroRecord` is the single source of truth;
//   2. the structured tree is DERIVED every render via `avroToStructuredFields`
//      — it is never a second copy that can drift;
//   3. a raw-JSON parse failure KEEPS the last valid record and only surfaces
//      an error. Typing a stray comma must never destroy the user's work.

import { useCallback, useMemo, useRef, useState } from "react";
import {
  avroToStructuredFields,
  normalizeAvroRecord,
  structuredToAvroFields,
  type AvroRecord,
  type StructuredField,
} from "@/lib/schemaEditor";

interface BufferState {
  record: AvroRecord | null;
  rawText: string;
  rawError: string | null;
  dirty: boolean;
}

export interface AvroBuffer {
  /** Last valid record. Survives every raw-JSON parse failure. */
  record: AvroRecord | null;
  /** Exactly what is in the raw textarea, valid or not. */
  rawText: string;
  rawError: string | null;
  dirty: boolean;
  /** Derived each render from `record` — never stored. */
  fields: StructuredField[];
  /** Replace the record and re-print the raw text. Marks dirty. */
  setRecord: (next: AvroRecord) => void;
  /** Structured-editor edits come back through here. Marks dirty. */
  setFields: (fields: StructuredField[]) => void;
  /** Raw-textarea edits. Parses; on failure keeps `record` and sets `rawError`. */
  setRawText: (value: string) => void;
  /**
   * Load a record without marking dirty (selection change, discard, save).
   * `overrides` exists for the one case where the stored raw JSON does NOT
   * parse: the text is handed back verbatim with its error so the user can
   * repair it, instead of being blanked.
   */
  reset: (record: AvroRecord | null, overrides?: { rawText?: string; rawError?: string | null }) => void;
}

const print = (record: AvroRecord) => JSON.stringify(record, null, 2);

export function useAvroBuffer(fallbackName = "Record"): AvroBuffer {
  const [state, setState] = useState<BufferState>({
    record: null,
    rawText: "",
    rawError: null,
    dirty: false,
  });

  // Read at parse time only, so a changing fallback name never re-runs anything.
  const fallbackRef = useRef(fallbackName);
  fallbackRef.current = fallbackName;

  const reset = useCallback(
    (record: AvroRecord | null, overrides?: { rawText?: string; rawError?: string | null }) => {
      setState({
        record,
        rawText: overrides?.rawText ?? (record ? print(record) : ""),
        rawError: overrides?.rawError ?? null,
        dirty: false,
      });
    },
    [],
  );

  const setRecord = useCallback((next: AvroRecord) => {
    setState({ record: next, rawText: print(next), rawError: null, dirty: true });
  }, []);

  const setFields = useCallback((fields: StructuredField[]) => {
    setState((prev) => {
      if (!prev.record) return prev;
      const next: AvroRecord = { ...prev.record, fields: structuredToAvroFields(fields) };
      return { record: next, rawText: print(next), rawError: null, dirty: true };
    });
  }, []);

  const setRawText = useCallback((value: string) => {
    setState((prev) => {
      try {
        const normalized = normalizeAvroRecord(JSON.parse(value), fallbackRef.current);
        return { record: normalized, rawText: value, rawError: null, dirty: true };
      } catch (error) {
        // Keep the last valid record — only the error surfaces.
        return {
          ...prev,
          rawText: value,
          rawError: error instanceof Error ? error.message : "Invalid JSON",
          dirty: true,
        };
      }
    });
  }, []);

  const fields = useMemo(
    () => (state.record ? avroToStructuredFields(state.record) : []),
    [state.record],
  );

  return {
    record: state.record,
    rawText: state.rawText,
    rawError: state.rawError,
    dirty: state.dirty,
    fields,
    setRecord,
    setFields,
    setRawText,
    reset,
  };
}
