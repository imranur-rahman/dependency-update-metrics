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

// The request fields that do not change the result.
const IGNORED_FIELDS = new Set<string>(["outputDir"]);

// Return the cache key of a request: every field of the request except the output folder.
// So the settings that #31 adds to ToolRequest join the key without a change here.
export function cacheKey(request: ToolRequest): string {
  const fields = Object.entries(request)
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

  // Return the result of the request when it is fresh: younger than one day.
  get(request: ToolRequest): CachedResult | undefined {
    const entry = this.entries()[cacheKey(request)];
    return entry && this.isFresh(entry) ? entry : undefined;
  }

  // Store the result of the request, and remove the results that are not fresh.
  async set(request: ToolRequest, result: ToolResult): Promise<void> {
    const entries: Record<string, CachedResult> = {};
    for (const [key, entry] of Object.entries(this.entries())) {
      if (this.isFresh(entry)) entries[key] = entry;
    }
    entries[cacheKey(request)] = { savedAt: this.now(), result };
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
