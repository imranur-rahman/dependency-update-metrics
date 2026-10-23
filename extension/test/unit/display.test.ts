// Tests for the display model of the core module (src/core/display.ts). They read the shared
// API contract examples in contract/examples/, the same files that the Host tests and the
// Webapp tests read, so a change to the contract fails all three.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";

import { hintText, hasNoDependencies, hoverMarkdown, type ToolResult } from "../../src/core/display.ts";
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

// The hover: the details behind the hint.
function assertHas(text: string, parts: string[]): void {
  for (const part of parts) {
    assert.ok(text.includes(part), `missing ${JSON.stringify(part)} in:\n${text}`);
  }
}

test("the hover of a result shows the window, the dependencies, the source, the date, and the section", () => {
  const text = hoverMarkdown(
    { kind: "result", result: okResult, computedOn: "2026-10-22" },
    { name: "mypkg", section: "devDependencies" },
  );
  assertHas(text, [
    "MTTU 579.0 days · MTTR 579.0 days",
    "Window: 2020-01-01 to 2023-01-01",
    "Dependencies of `mypkg`: 1",
    "Data source: deps.dev",
    "Computed on: 2026-10-22",
    "Section: `devDependencies`",
  ]);
});

test("the hover of a result has one short Explanation line under the values", () => {
  const text = hoverMarkdown({ kind: "result", result: okResult }, { name: "mypkg", section: "dependencies" });
  const line =
    "A direct dependency was behind the newest version for 579.0 days in total (mean over the direct dependencies), " +
    "and on a known vulnerable version after a fix for 579.0 days in total (mean over the direct dependencies).";
  assert.ok(text.includes(`MTTU 579.0 days · MTTR 579.0 days\n\n${line}\n`), text);
});

test("the hover Explanation of MTTR 0.0 does not say that no dependency was vulnerable", () => {
  const result = withSummary({ mttu: 0, mttr: 0 });
  const text = hoverMarkdown({ kind: "result", result }, { name: "mypkg", section: "dependencies" });
  assert.ok(text.includes("none stayed on a known vulnerable version after a fix was available"), text);
  assert.doesNotMatch(text, /no vulnerable|had no vulnerab/i);
});

test("without the OSV index, the hover has no Explanation line", () => {
  const target = { name: "mypkg", section: "dependencies" };
  const text = hoverMarkdown({ kind: "result", result: okResult }, target, { osvIndex: false });
  assert.ok(!text.includes("A direct dependency"), text);
});

test("the hover of a package without dependencies repeats the no-dependencies text", () => {
  const result = withSummary({ mttu: 0, mttr: 0, num_dependencies: 0, status: "no_deps" });
  const text = hoverMarkdown({ kind: "result", result }, { name: "left-pad", section: "dependencies" });
  assertHas(text, [
    "could not compute since 'left-pad' has no dependencies",
    "Window: 2020-01-01 to 2023-01-01",
    "Section: `dependencies`",
  ]);
  assert.ok(!text.includes("MTTU"), text);
});

test("the hover of a package that the Tool could not analyze shows the full Tool error", () => {
  const message = `${"Package nope not found in NPM after 3 retries ".repeat(3)}\nsecond line`;
  const result = { ...failedResult, error: message };
  assert.ok(hintText({ kind: "result", result }, "nope").length < 60);
  const text = hoverMarkdown({ kind: "result", result }, { name: "nope", section: "dependencies" });
  assertHas(text, [message, "Window: 2020-01-01 to 2023-01-01", "Section: `dependencies`"]);
  assert.ok(!text.includes("MTTU"), text);
});

test("the hover of a Tool run without a result shows the full message", () => {
  const message = "spawn dependency-metrics ENOENT\nwith ``` backticks and a long tail";
  const text = hoverMarkdown({ kind: "failed", message }, { name: "express", section: "dependencies" });
  assertHas(text, [message, "Section: `dependencies`"]);
  // The fence is longer than any backtick run in the message, so the message stays literal.
  assert.match(text, /^````$/m);
});

test("the hover of a running Tool says that it computes", () => {
  const text = hoverMarkdown({ kind: "computing" }, { name: "express", section: "dependencies" });
  assertHas(text, ["computing…", "Section: `dependencies`"]);
});
