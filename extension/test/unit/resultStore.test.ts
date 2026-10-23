// Tests for the run control of the core module (src/core/resultStore.ts): the queue limit,
// the one-day cache, the automatic runs, and the runs that a command starts.
import { test } from "node:test";
import assert from "node:assert/strict";

import type { ToolResult } from "../../src/core/display.ts";
import { ResultCache, type CacheStorage } from "../../src/core/resultCache.ts";
import { ResultStore, type PackageKey } from "../../src/core/resultStore.ts";
import type { ToolOutcome, ToolRequest, ToolRunner } from "../../src/core/tool.ts";

// A Tool runner that finishes a run only when the test says so.
class FakeRunner implements ToolRunner {
  readonly started: string[] = [];
  private readonly pending = new Map<string, (outcome: ToolOutcome) => void>();
  active = 0;
  maxActive = 0;

  run(request: ToolRequest): Promise<ToolOutcome> {
    this.started.push(request.packageName);
    this.active++;
    this.maxActive = Math.max(this.maxActive, this.active);
    return new Promise((resolve) => {
      this.pending.set(request.packageName, (outcome) => {
        this.active--;
        resolve(outcome);
      });
    });
  }

  finish(name: string, outcome: ToolOutcome = { kind: "result", result: result(name) }): void {
    this.pending.get(name)!(outcome);
    this.pending.delete(name);
  }
}

function result(name: string): ToolResult {
  return {
    ecosystem: "npm",
    package: name,
    mode: "window_summary",
    start_date: "2024-10-22",
    end_date: "2026-10-22",
    weighting_type: "disable",
    half_life: null,
    data_source: "depsdev",
    summary: { mttu: 1, mttr: 2, num_dependencies: 3, status: "ok", error: "" },
    releases: [],
    error: "",
  };
}

const key = (name: string): PackageKey => ({ ecosystem: "npm", name });
const makeRequest = (k: PackageKey): ToolRequest => ({
  ecosystem: k.ecosystem,
  packageName: k.name,
  startDate: "2024-10-22",
  outputDir: "/out",
});
class MemoryStorage implements CacheStorage {
  private readonly values = new Map<string, unknown>();
  get<T>(k: string): T | undefined {
    return structuredClone(this.values.get(k)) as T | undefined;
  }
  async update(k: string, value: unknown): Promise<void> {
    this.values.set(k, structuredClone(value));
  }
}

const settle = () => new Promise((resolve) => setImmediate(resolve));

test("the store runs at most the set number of Tool processes at a time", async () => {
  const runner = new FakeRunner();
  const store = new ResultStore({ runner, makeRequest, parallelRuns: 3 });
  for (const name of ["a", "b", "c", "d", "e"]) store.get(key(name));
  await settle();
  assert.deepEqual(runner.started, ["a", "b", "c"]);

  runner.finish("b");
  await settle();
  assert.deepEqual(runner.started, ["a", "b", "c", "d"]);
  for (const name of ["a", "c", "d"]) runner.finish(name);
  await settle();
  runner.finish("e");
  await settle();
  assert.equal(runner.maxActive, 3);
  assert.equal(store.get(key("e"))?.kind, "result");
});

test("a fresh value from the cache shows at once, without a Tool run", async () => {
  const now = Date.parse("2026-10-22T09:00:00Z");
  const cache = new ResultCache(new MemoryStorage(), () => now);
  await cache.set(makeRequest(key("express")), result("express"));
  const runner = new FakeRunner();
  const store = new ResultStore({ runner, makeRequest, cache });

  const state = store.get(key("express"));
  assert.deepEqual(state?.kind === "result" && state.result, result("express"));
  await settle();
  assert.deepEqual(runner.started, []);
});

test("a value from the cache carries the local date of the run that computed it", async () => {
  const savedAt = new Date(2026, 9, 21, 23, 30).getTime();
  const storage = new MemoryStorage();
  await new ResultCache(storage, () => savedAt).set(makeRequest(key("express")), result("express"));
  const now = new Date(2026, 9, 22, 8, 0);
  const store = new ResultStore({
    runner: new FakeRunner(),
    makeRequest,
    cache: new ResultCache(storage, () => now.getTime()),
    now: () => now,
  });

  assert.deepEqual(store.get(key("express")), {
    kind: "result",
    result: result("express"),
    computedOn: "2026-10-21",
  });
});

test("a result of a Tool run goes into the cache, so that the next session shows it at once", async () => {
  let now = Date.parse("2026-10-22T09:00:00Z");
  const storage = new MemoryStorage();
  const runner = new FakeRunner();
  const first = new ResultStore({ runner, makeRequest, cache: new ResultCache(storage, () => now) });
  first.get(key("express"));
  await settle();
  runner.finish("express");
  await settle();
  first.dispose();

  now += 60 * 60 * 1000;
  const second = new ResultStore({ runner, makeRequest, cache: new ResultCache(storage, () => now) });
  assert.equal(second.get(key("express"))?.kind, "result");
  assert.deepEqual(runner.started, ["express"]);
});

test("a failed run does not go into the cache, so that the next session runs the Tool again", async () => {
  const now = Date.parse("2026-10-22T09:00:00Z");
  const storage = new MemoryStorage();
  const runner = new FakeRunner();
  const first = new ResultStore({ runner, makeRequest, cache: new ResultCache(storage, () => now) });
  first.get(key("express"));
  await settle();
  runner.finish("express", { kind: "failed", message: "spawn dependency-metrics ENOENT" });
  await settle();

  const second = new ResultStore({ runner, makeRequest, cache: new ResultCache(storage, () => now) });
  assert.equal(second.get(key("express"))?.kind, "computing");
});

test("with automatic runs off, an unknown package gets no state and no Tool run", async () => {
  const runner = new FakeRunner();
  const store = new ResultStore({ runner, makeRequest, automaticRuns: false });

  assert.equal(store.get(key("express")), undefined);
  await settle();
  assert.deepEqual(runner.started, []);
});

test("with automatic runs off, a fresh value from the cache still shows at once", async () => {
  const now = Date.parse("2026-10-22T09:00:00Z");
  const cache = new ResultCache(new MemoryStorage(), () => now);
  await cache.set(makeRequest(key("express")), result("express"));
  const store = new ResultStore({ runner: new FakeRunner(), makeRequest, cache, automaticRuns: false });

  assert.equal(store.get(key("express"))?.kind, "result");
});

test("analyze runs the Tool for the packages without a fresh value, and gives all outcomes", async () => {
  const now = Date.parse("2026-10-22T09:00:00Z");
  const cache = new ResultCache(new MemoryStorage(), () => now);
  await cache.set(makeRequest(key("a")), result("a"));
  const runner = new FakeRunner();
  const store = new ResultStore({
    runner,
    makeRequest,
    cache,
    automaticRuns: false,
    now: () => new Date(now),
  });
  let updates = 0;
  store.onDidUpdate(() => updates++);

  const outcomes = store.analyze([key("a"), key("b"), key("c")]);
  assert.equal(store.get(key("b"))?.kind, "computing");
  await settle();
  assert.deepEqual(runner.started, ["b", "c"]);
  runner.finish("b");
  runner.finish("c", { kind: "failed", message: "timeout" });

  assert.deepEqual(
    (await outcomes).map((o) => (o.kind === "result" ? o.result.package : o.message)),
    ["a", "b", "timeout"],
  );
  assert.equal(updates, 2);
});

test("analyze runs a failed package again, and joins a run that is in progress", async () => {
  const runner = new FakeRunner();
  const store = new ResultStore({ runner, makeRequest });
  store.get(key("a"));
  store.get(key("b"));
  await settle();
  runner.finish("a", { kind: "failed", message: "spawn dependency-metrics ENOENT" });
  await settle();
  assert.equal(store.get(key("a"))?.kind, "failed");

  const outcomes = store.analyze([key("a"), key("b")]);
  await settle();
  assert.deepEqual(runner.started, ["a", "b", "a"]);
  runner.finish("a");
  runner.finish("b");
  assert.deepEqual(
    (await outcomes).map((o) => o.kind),
    ["result", "result"],
  );
});

test("dispose gives the waiting analyze calls a failed outcome, so that no caller waits forever", async () => {
  const runner = new FakeRunner();
  const store = new ResultStore({ runner, makeRequest, parallelRuns: 1 });
  const outcomes = store.analyze([key("a"), key("b")]);
  await settle();
  store.dispose();
  runner.finish("a", { kind: "failed", message: "aborted" });

  assert.deepEqual(
    (await outcomes).map((o) => o.kind),
    ["failed", "failed"],
  );
  assert.deepEqual(runner.started, ["a"]);
});

test("a value older than one day gets a new Tool run in a long session", async () => {
  let now = Date.parse("2026-10-22T09:00:00Z");
  const runner = new FakeRunner();
  const store = new ResultStore({ runner, makeRequest, now: () => new Date(now) });
  store.get(key("express"));
  await settle();
  runner.finish("express");
  await settle();

  now += 23 * 60 * 60 * 1000;
  assert.equal(store.get(key("express"))?.kind, "result");
  now += 1 * 60 * 60 * 1000;
  assert.equal(store.get(key("express"))?.kind, "computing");
  await settle();
  assert.deepEqual(runner.started, ["express", "express"]);
});
