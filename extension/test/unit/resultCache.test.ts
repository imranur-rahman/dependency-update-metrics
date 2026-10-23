// Tests for the one-day cache of the core module (src/core/resultCache.ts).
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";

import type { ToolResult } from "../../src/core/display.ts";
import { ResultCache, type CacheStorage } from "../../src/core/resultCache.ts";
import type { ToolRequest } from "../../src/core/tool.ts";

const EXAMPLES = join(import.meta.dirname, "..", "..", "..", "contract", "examples");
const RESULT = JSON.parse(readFileSync(join(EXAMPLES, "job_done_window_summary.json"), "utf8"))
  .results[0] as ToolResult;
const HOUR = 60 * 60 * 1000;

// A storage in memory, as VS Code global state is between two sessions.
class MemoryStorage implements CacheStorage {
  readonly values = new Map<string, unknown>();
  get<T>(key: string): T | undefined {
    return structuredClone(this.values.get(key)) as T | undefined;
  }
  async update(key: string, value: unknown): Promise<void> {
    this.values.set(key, structuredClone(value));
  }
}

function request(overrides: Partial<ToolRequest> = {}): ToolRequest {
  return {
    ecosystem: "npm",
    packageName: "express",
    startDate: "2024-10-22",
    outputDir: "/storage/tool-output",
    ...overrides,
  };
}

test("a result is fresh for one day and then the cache forgets it", async () => {
  let now = Date.parse("2026-10-22T09:00:00Z");
  const cache = new ResultCache(new MemoryStorage(), () => now);
  await cache.set(request(), RESULT);

  now += 23 * HOUR;
  assert.deepEqual(cache.get(request()), { result: RESULT, savedAt: now - 23 * HOUR });
  now += 1 * HOUR;
  assert.equal(cache.get(request()), undefined);
});

test("a result persists in the storage for a new cache, as after a VS Code restart", async () => {
  const now = Date.parse("2026-10-22T09:00:00Z");
  const storage = new MemoryStorage();
  await new ResultCache(storage, () => now).set(request(), RESULT);

  assert.deepEqual(new ResultCache(storage, () => now + HOUR).get(request())?.result, RESULT);
});

test("a request with another package, ecosystem, or setting does not get the result", async () => {
  const now = Date.parse("2026-10-22T09:00:00Z");
  const cache = new ResultCache(new MemoryStorage(), () => now);
  await cache.set(request(), RESULT);

  assert.equal(cache.get(request({ packageName: "left-pad" })), undefined);
  assert.equal(cache.get(request({ ecosystem: "pypi" as ToolRequest["ecosystem"] })), undefined);
  assert.equal(cache.get(request({ startDate: "2025-10-22" })), undefined);
  // A field that #31 adds to the request, such as the weighting, joins the key.
  assert.equal(cache.get({ ...request(), weighting: "linear" } as ToolRequest), undefined);
});

test("the output folder of the Tool does not change the result", async () => {
  const now = Date.parse("2026-10-22T09:00:00Z");
  const cache = new ResultCache(new MemoryStorage(), () => now);
  await cache.set(request(), RESULT);

  assert.deepEqual(cache.get(request({ outputDir: "/other/tool-output" }))?.result, RESULT);
});

test("the cache removes the results that are not fresh when it stores a new result", async () => {
  let now = Date.parse("2026-10-22T09:00:00Z");
  const storage = new MemoryStorage();
  const cache = new ResultCache(storage, () => now);
  await cache.set(request(), RESULT);
  now += 25 * HOUR;
  await cache.set(request({ packageName: "left-pad" }), RESULT);

  assert.equal(JSON.stringify([...storage.values]).includes("express"), false);
  assert.deepEqual(cache.get(request({ packageName: "left-pad" }))?.result, RESULT);
});
