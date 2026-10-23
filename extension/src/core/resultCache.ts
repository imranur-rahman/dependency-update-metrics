// The one-day cache of Tool results. This module has no `vscode` import: the Extension gives
// it VS Code global state as the storage, so that the results persist across restarts.

import type { ToolResult } from "./display.ts";
import type { ToolRequest } from "./tool.ts";

// A key-value storage that keeps JSON values. VS Code `Memento` (global state) fits it.
export interface CacheStorage {
  get<T>(key: string): T | undefined;
  update(key: string, value: unknown): PromiseLike<void>;
}

// A stored result and the time (ms since the epoch) when the cache stored it.
export interface CachedResult {
  savedAt: number;
  result: ToolResult;
}

export const FRESH_MS = 24 * 60 * 60 * 1000;
const STORAGE_KEY = "mttx.results";

// The window as the settings define it. An empty date means "not set": without an end date
// the window ends today, and without a start date it starts windowYears before the end.
export interface WindowDefinition {
  windowYears: number;
  startDate: string;
  endDate: string;
}

// The values outside the request fields that change a result:
// - window: the window definition. The key holds it in place of the computed dates, so a
//   window that ends today keeps its value for one day, not until local midnight.
// - osvIndex: the build date of the OSV index, or "none" without an index. So a result that
//   the Tool computed without the index is not fresh after a build.
export interface ResultContext {
  window: WindowDefinition;
  osvIndex: string;
}

// The request fields that do not go into the key: the folder of the Tool cache, the timeout
// (a run that times out fails, and a failed run does not go into the cache), and the dates
// that the window definition of the context replaces.
const IGNORED_FIELDS = new Set<string>(["outputDir", "timeoutSeconds", "startDate", "endDate"]);

// Return the cache key of a request in a context: every field of the request except
// IGNORED_FIELDS, and the context. So a new field of ToolRequest joins the key without a
// change here.
export function cacheKey(request: ToolRequest, context: ResultContext): string {
  const fields = Object.entries({ ...request, context })
    .filter(([name]) => !IGNORED_FIELDS.has(name))
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  return JSON.stringify(fields);
}

export class ResultCache {
  private readonly storage: CacheStorage;
  private readonly now: () => number;

  constructor(storage: CacheStorage, now: () => number = Date.now) {
    this.storage = storage;
    this.now = now;
  }

  // Return the result of the request in the context when it is fresh: younger than one day.
  get(request: ToolRequest, context: ResultContext): CachedResult | undefined {
    const entry = this.entries()[cacheKey(request, context)];
    return entry && this.isFresh(entry) ? entry : undefined;
  }

  // Store the result of the request, and remove the results that are not fresh.
  async set(request: ToolRequest, context: ResultContext, result: ToolResult): Promise<void> {
    const entries: Record<string, CachedResult> = {};
    for (const [key, entry] of Object.entries(this.entries())) {
      if (this.isFresh(entry)) entries[key] = entry;
    }
    entries[cacheKey(request, context)] = { savedAt: this.now(), result };
    await this.storage.update(STORAGE_KEY, entries);
  }

  private entries(): Record<string, CachedResult> {
    return this.storage.get<Record<string, CachedResult>>(STORAGE_KEY) ?? {};
  }

  private isFresh(entry: CachedResult): boolean {
    const age = this.now() - entry.savedAt;
    return age >= 0 && age < FRESH_MS;
  }
}
