// localStorage-backed mock repository. One document, seed-version stamped.
// All reads/writes in the prototype go through here.

import { migrateBranches } from "./migrate";
import { buildSeedState, SEED_VERSION } from "./seeds";
import type { PrototypeState } from "./types";

const STORAGE_KEY = "dmp-adapter-prototype-v1";
const BACKUP_KEY_PREFIX = "dmp-adapter-prototype-backup-v";

let cache: PrototypeState | null = null;

// Collections every build expects to find. The version stamp alone is not
// enough: a blob written from a half-finished build carries the current
// version but can be missing a collection, and JSON.stringify drops undefined
// keys silently — so that blob would load forever and the UI would read
// `undefined.map`. Checking shape as well as version makes a bad save
// self-healing instead of permanent.
const REQUIRED_COLLECTIONS: (keyof PrototypeState)[] = [
  "flows",
  "schemas",
  "schemaTemplates",
  "connections",
  "gatewayProxies",
  "services",
  "audit",
  "runtimes",
];

function missingCollections(state: PrototypeState): string[] {
  return REQUIRED_COLLECTIONS.filter((key) => !Array.isArray(state[key])).map(String);
}

/** Archive a stale blob so a version bump never silently destroys demo data. */
function archive(raw: string, oldVersion: number | "unreadable" | "incomplete"): string | null {
  const key = `${BACKUP_KEY_PREFIX}${oldVersion}`;
  try {
    window.localStorage.setItem(key, raw);
  } catch {
    // storage full/unavailable — the reset still happens, we just can't archive
    return null;
  }
  return key;
}

function load(): PrototypeState {
  if (cache) return cache;

  let raw: string | null = null;
  try {
    raw = window.localStorage.getItem(STORAGE_KEY);
  } catch {
    raw = null;
  }

  let notice: string | undefined;

  if (raw) {
    let parsed: PrototypeState | null = null;
    try {
      parsed = JSON.parse(raw) as PrototypeState;
    } catch {
      parsed = null;
    }

    const incomplete = parsed ? missingCollections(parsed) : [];

    if (parsed && parsed.seedVersion === SEED_VERSION && incomplete.length === 0) {
      // Shape migrations that must NOT cost the user their saved flows run here
      // rather than behind a version bump: the blob is rewritten in place and
      // persisted, and anything that could not be carried over says so once.
      const branchNotice = migrateBranches(parsed);
      cache = parsed;
      if (branchNotice) {
        parsed.migrationNotice = branchNotice;
        persist();
      }
      return parsed;
    }

    if (parsed && incomplete.length > 0) {
      const backupKey = archive(raw, parsed.seedVersion === SEED_VERSION ? "incomplete" : "unreadable");
      notice =
        `Demo data was reset. The saved dataset was missing ${incomplete.join(", ")}, ` +
        `so it could not be read by this build.` +
        (backupKey ? ` Your previous data was archived in localStorage under "${backupKey}".` : "");
    } else if (parsed) {
      const oldVersion = typeof parsed.seedVersion === "number" ? parsed.seedVersion : 0;
      const backupKey = archive(raw, oldVersion);
      notice =
        `Demo data was reset. The saved dataset was seed version ${oldVersion}; this build ships version ${SEED_VERSION}, ` +
        `which changed the shape of proxies, schemas and sink configuration.` +
        (backupKey ? ` Your previous data was archived in localStorage under "${backupKey}".` : "");
    } else {
      const backupKey = archive(raw, "unreadable");
      notice =
        `Demo data was reset. The saved dataset could not be read.` +
        (backupKey ? ` The unreadable blob was archived in localStorage under "${backupKey}".` : "");
    }
  }

  const seeded = buildSeedState();
  if (notice) seeded.migrationNotice = notice;
  cache = seeded;
  persist();
  return seeded;
}

function persist() {
  if (!cache) return;
  try {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(cache));
  } catch {
    // storage full/unavailable — prototype keeps working in-memory
  }
}

export function getState(): PrototypeState {
  return load();
}

/** Apply a mutation to the state and persist it. */
export function mutate<T>(fn: (state: PrototypeState) => T): T {
  const state = load();
  const result = fn(state);
  persist();
  return result;
}

/** The one-time reset notice, if the last load reseeded over an older version. */
export function getMigrationNotice(): string | null {
  return load().migrationNotice ?? null;
}

/** Read the one-time reset notice and clear it, so it is shown exactly once. */
export function consumeMigrationNotice(): string | null {
  const state = load();
  const notice = state.migrationNotice ?? null;
  if (notice) {
    delete state.migrationNotice;
    persist();
  }
  return notice;
}

/** Wipe saved state and restore the seed dataset. */
export function resetDemoData(): void {
  cache = buildSeedState();
  persist();
}

export function uid(prefix: string): string {
  return `${prefix}-${Math.random().toString(36).slice(2, 8)}`;
}
