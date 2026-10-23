// Tests for the one-day cache of the core module (src/core/resultCache.ts).
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";

import type { ToolResult } from "../../src/core/display.ts";
import { ResultCache, type CacheStorage, type ResultContext } from "../../src/core/resultCache.ts";
import type { ToolRequest } from "../../src/core/tool.ts";

const EXAMPLES = join(import.meta.dirname, "..", "..", "..", "contract", "examples");
const RESULT = JSON.parse(readFileSync(join(EXAMPLES, "job_done_window_summary.json"), "utf8"))
  .results[0] as ToolResult;
const HOUR = 60 * 60 * 1000;
const CONTEXT: ResultContext = { window: { windowYears: 2, startDate: "", endDate: "" }, osvIndex: "2026-10-21" };

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
    dataSource: "depsdev",
    startDate: "2024-10-22",
    endDate: "2026-10-22",
    weightingType: "disable",
    halfLife: null,
    outputDir: "/storage/tool-output",
    timeoutSeconds: 600,
    ...overrides,
  };
}

test("a result is fresh for one day and then the cache forgets it", async () => {
  let now = Date.parse("2026-10-22T09:00:00Z");
  const cache = new ResultCache(new MemoryStorage(), () => now);
  await cache.set(request(), CONTEXT, RESULT);

  now += 23 * HOUR;
  assert.deepEqual(cache.get(request(), CONTEXT), { result: RESULT, savedAt: now - 23 * HOUR });
  now += 1 * HOUR;
  assert.equal(cache.get(request(), CONTEXT), undefined);
});

test("a result persists in the storage for a new cache, as after a VS Code restart", async () => {
  const now = Date.parse("2026-10-22T09:00:00Z");
  const storage = new MemoryStorage();
  await new ResultCache(storage, () => now).set(request(), CONTEXT, RESULT);

  assert.deepEqual(new ResultCache(storage, () => now + HOUR).get(request(), CONTEXT)?.result, RESULT);
});

test("a request with another package, ecosystem, or setting does not get the result", async () => {
  const now = Date.parse("2026-10-22T09:00:00Z");
  const cache = new ResultCache(new MemoryStorage(), () => now);
  await cache.set(request(), CONTEXT, RESULT);

  assert.equal(cache.get(request({ packageName: "left-pad" }), CONTEXT), undefined);
  assert.equal(cache.get(request({ ecosystem: "pypi" as ToolRequest["ecosystem"] }), CONTEXT), undefined);
  for (const overrides of [{ dataSource: "native" as const }, { weightingType: "exponential" as const, halfLife: 180 }]) {
    assert.equal(cache.get(request(overrides), CONTEXT), undefined, JSON.stringify(overrides));
  }
  for (const window of [
    { windowYears: 3, startDate: "", endDate: "" },
    { windowYears: 2, startDate: "2025-10-22", endDate: "" },
    { windowYears: 2, startDate: "", endDate: "2026-10-21" },
  ]) {
    assert.equal(cache.get(request(), { ...CONTEXT, window }), undefined, JSON.stringify(window));
  }
  // A new field of the request joins the key without a change.
  assert.equal(cache.get({ ...request(), perRelease: true } as ToolRequest, CONTEXT), undefined);
});

test("a result that the Tool computed without the OSV index, or with an older index, is not fresh after a build", async () => {
  const now = Date.parse("2026-10-22T09:00:00Z");
  const cache = new ResultCache(new MemoryStorage(), () => now);
  await cache.set(request(), { ...CONTEXT, osvIndex: "none" }, RESULT);
  await cache.set(request({ packageName: "left-pad" }), CONTEXT, RESULT);

  assert.equal(cache.get(request(), CONTEXT), undefined);
  assert.equal(cache.get(request({ packageName: "left-pad" }), { ...CONTEXT, osvIndex: "2026-10-22" }), undefined);
});

test("the output folder, the timeout, and the computed dates of the window do not change the result", async () => {
  const now = Date.parse("2026-10-22T09:00:00Z");
  const cache = new ResultCache(new MemoryStorage(), () => now);
  await cache.set(request(), CONTEXT, RESULT);

  assert.deepEqual(cache.get(request({ outputDir: "/other/tool-output" }), CONTEXT)?.result, RESULT);
  assert.deepEqual(cache.get(request({ timeoutSeconds: 30 }), CONTEXT)?.result, RESULT);
  // A window that ends today: the computed dates change at local midnight, its definition does not.
  assert.deepEqual(cache.get(request({ startDate: "2024-10-23", endDate: "2026-10-23" }), CONTEXT)?.result, RESULT);
});

test("the cache removes the results that are not fresh when it stores a new result", async () => {
  let now = Date.parse("2026-10-22T09:00:00Z");
  const storage = new MemoryStorage();
  const cache = new ResultCache(storage, () => now);
  await cache.set(request(), CONTEXT, RESULT);
  now += 25 * HOUR;
  await cache.set(request({ packageName: "left-pad" }), CONTEXT, RESULT);

  assert.equal(JSON.stringify([...storage.values]).includes("express"), false);
  assert.deepEqual(cache.get(request({ packageName: "left-pad" }), CONTEXT)?.result, RESULT);
});
