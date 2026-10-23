// Tests for the display model of the core module (src/core/display.ts). They read the shared
// API contract examples in contract/examples/, the same files that the Host tests and the
// Webapp tests read, so a change to the contract fails all three.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";

import { hintText, hasNoDependencies, type ToolResult } from "../../src/core/display.ts";
// @ts-expect-error The Webapp module is plain JavaScript without type declarations.
import { resultRows, NO_DEPENDENCIES_ERROR } from "../../../webapp/site/static/js/comparison.js";

const EXAMPLES = join(import.meta.dirname, "..", "..", "..", "contract", "examples");

function exampleResults(name: string): ToolResult[] {
  return JSON.parse(readFileSync(join(EXAMPLES, name), "utf8")).results;
}

const [okResult] = exampleResults("job_done_window_summary.json");
const failedResult = exampleResults("job_done_with_package_error.json")[1];

function withSummary(changes: Partial<NonNullable<ToolResult["summary"]>>): ToolResult {
  return { ...okResult, summary: { ...okResult.summary!, ...changes } };
}

test("a running Tool shows computing…", () => {
  assert.equal(hintText({ kind: "computing" }, "express"), "computing…");
});

test("a Window summary result shows MTTU and MTTR in days", () => {
  assert.equal(hintText({ kind: "result", result: okResult }, "mypkg"), "MTTU 579 d · MTTR 579 d");
});

test("the hint rounds the days to whole days", () => {
  const result = withSummary({ mttu: 12.6, mttr: 0.4 });
  assert.equal(hintText({ kind: "result", result }, "mypkg"), "MTTU 13 d · MTTR 0 d");
});

test("both kinds of a package without dependencies show the no-dependencies text", () => {
  const noDeps = withSummary({ mttu: 0, mttr: 0, num_dependencies: 0, status: "no_deps" });
  const okWithoutDeps = withSummary({ mttu: 0, mttr: 0, num_dependencies: 0, status: "ok" });
  for (const result of [noDeps, okWithoutDeps]) {
    assert.equal(
      hintText({ kind: "result", result }, "left-pad"),
      "could not compute since 'left-pad' has no dependencies",
    );
  }
});

test("a package that the Tool could not analyze shows its error", () => {
  assert.equal(
    hintText({ kind: "result", result: failedResult }, "nope"),
    "error: Package not found in deps.dev",
  );
});

test("a Tool that did not give a result shows a short error", () => {
  const message = "spawn dependency-metrics ENOENT and a long tail that the hint has no room for";
  const text = hintText({ kind: "failed", message }, "express");
  // The message part holds at most 50 characters, with the ellipsis.
  assert.equal(text, "error: spawn dependency-metrics ENOENT and a long tail t…");
});

test("the no-dependencies rule is the rule of the Webapp", () => {
  const entries = [
    { status: "no_deps", num_dependencies: 0, mttu: 0, mttr: 0, error: "" },
    { status: "ok", num_dependencies: 0, mttu: 0, mttr: 0, error: "" },
    { status: "ok", num_dependencies: 3, mttu: 5, mttr: 1, error: "" },
    { status: "error", num_dependencies: 0, mttu: null, mttr: null, error: "" },
  ] as const;
  for (const entry of entries) {
    const job = {
      comparison: { mode: "window_summary" },
      results: [{ ...okResult, summary: entry, error: "" }],
    };
    const webappSaysNoDeps = resultRows(job)[0].error === NO_DEPENDENCIES_ERROR;
    assert.equal(hasNoDependencies(entry), webappSaysNoDeps, JSON.stringify(entry));
  }
});
