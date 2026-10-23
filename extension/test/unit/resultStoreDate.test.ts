// Tests for the date of the value (#27): the result store records the local date when a Tool
// run finishes, so that the hover can show it.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";

import type { ToolResult } from "../../src/core/display.ts";
import { ResultStore } from "../../src/core/resultStore.ts";
import type { ToolOutcome, ToolRunner } from "../../src/core/tool.ts";

const EXAMPLES = join(import.meta.dirname, "..", "..", "..", "contract", "examples");
const [okResult]: ToolResult[] = JSON.parse(
  readFileSync(join(EXAMPLES, "job_done_window_summary.json"), "utf8"),
).results;

function storeWith(outcome: ToolOutcome, now: Date): ResultStore {
  const runner: ToolRunner = { run: async () => outcome };
  const request = { ecosystem: "npm" as const, packageName: "mypkg", startDate: "", outputDir: "" };
  return new ResultStore({ runner, makeRequest: () => request, now: () => now });
}

async function finished(store: ResultStore) {
  const key = { ecosystem: "npm" as const, name: "mypkg" };
  store.get(key);
  await new Promise<void>((resolve) => store.onDidUpdate(resolve));
  return store.get(key);
}

test("a finished result carries the local date of the run", async () => {
  const store = storeWith({ kind: "result", result: okResult }, new Date(2026, 0, 5, 23, 59));
  assert.deepEqual(await finished(store), {
    kind: "result",
    result: okResult,
    computedOn: "2026-01-05",
  });
});

test("a failed run carries no date", async () => {
  const store = storeWith({ kind: "failed", message: "boom" }, new Date(2026, 0, 5));
  assert.deepEqual(await finished(store), { kind: "failed", message: "boom" });
});
