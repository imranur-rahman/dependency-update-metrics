// Tests for the Comparison model of the core module (src/core/comparison.ts): the input of
// the "MTTX: Compare packages" command, and the Job-shaped object that the webview gives to
// the Webapp comparison.js.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";

import {
  compareLink,
  comparisonJob,
  comparisonRows,
  PACKAGE_COLORS,
  parsePackageNames,
  SERIES_COLORS,
} from "../../src/core/comparison.ts";
import type { ToolResult } from "../../src/core/display.ts";
import { barChartData } from "../../../webapp/site/static/js/comparison.js";

test("the input takes 1 to 5 names, split by commas or spaces", () => {
  assert.deepEqual(parsePackageNames("npm", " express, koa  fastify,"), {
    names: ["express", "koa", "fastify"],
  });
});

test("the input refuses no names and more than 5 names", () => {
  assert.deepEqual(parsePackageNames("npm", " , "), { error: "Enter at least one package." });
  assert.deepEqual(parsePackageNames("npm", "a b c d e f"), { error: "Enter at most 5 packages." });
});

test("the input refuses a name that is not valid in the ecosystem, and a name twice", () => {
  assert.deepEqual(parsePackageNames("cargo", "serde 1tokio"), {
    error: "1tokio is not a valid Cargo package name.",
  });
  assert.deepEqual(parsePackageNames("pypi", "Django django"), {
    error: "django is in the Comparison twice.",
  });
});

const EXAMPLES = join(import.meta.dirname, "..", "..", "..", "contract", "examples");

function exampleJob(name: string) {
  return JSON.parse(readFileSync(join(EXAMPLES, name), "utf8"));
}

test("the Job of the Tool results has the shape of a done Host Job", () => {
  const example = exampleJob("job_done_with_package_error.json");
  const outcomes = example.results.map((result: ToolResult) => ({ kind: "result", result }) as const);
  const job = comparisonJob(
    { ecosystem: "pypi", packages: ["mypkg", "nope"], startDate: "2019-06-01", endDate: "2019-07-01" },
    outcomes,
  );
  assert.deepEqual(job, { ...example, job_id: "mttx-extension" });
});

test("a package without dependencies shows the no-dependencies text and no bar", () => {
  const [ok] = exampleJob("job_done_window_summary.json").results as ToolResult[];
  const noDeps: ToolResult = {
    ...ok,
    package: "left-pad",
    summary: { ...ok.summary!, mttu: 0, mttr: 0, num_dependencies: 0, status: "no_deps" },
  };
  const job = comparisonJob(
    { ecosystem: "pypi", packages: ["mypkg", "left-pad"], startDate: "2020-01-01", endDate: "2023-01-01" },
    [
      { kind: "result", result: ok },
      { kind: "result", result: noDeps },
    ],
  );
  assert.deepEqual(comparisonRows(job), [
    { package: "mypkg", mttu: "579.0", mttr: "579.0", dependencies: 1, error: "" },
    {
      package: "left-pad",
      mttu: "–",
      mttr: "–",
      dependencies: 0,
      error: "could not compute since 'left-pad' has no dependencies",
    },
  ]);
  assert.deepEqual(barChartData(job).datasets.map((dataset: { data: unknown[] }) => dataset.data), [
    [579, null],
    [579, null],
  ]);
});

test("the webview charts use the colors of the Webapp charts", () => {
  const webapp = readFileSync(
    join(import.meta.dirname, "..", "..", "..", "webapp", "site", "static", "js", "webapp.js"),
    "utf8",
  );
  assert.match(webapp, /const SERIES_COLORS = \{ MTTU: "#2a78d6", MTTR: "#eb6834" \};/);
  assert.deepEqual(SERIES_COLORS, { MTTU: "#2a78d6", MTTR: "#eb6834" });
  assert.match(webapp, /const PACKAGE_COLORS = \["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"\];/);
  assert.deepEqual(PACKAGE_COLORS, ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]);
});

test("the hover link starts a Comparison with the package of the dependency", () => {
  const link = compareLink({ ecosystem: "npm", name: "@types/node" });
  const match = /^\[Compare with alternatives\]\(command:mttx\.compare\?(.+)\)$/.exec(link);
  assert.ok(match, link);
  assert.ok(!/[\s()]/.test(match[1]), "the link target has no space and no parenthesis");
  assert.deepEqual(JSON.parse(decodeURIComponent(match[1])), [{ ecosystem: "npm", packages: ["@types/node"] }]);
});

test("a Tool run without a result becomes a package error in the Job", () => {
  const job = comparisonJob(
    { ecosystem: "npm", packages: ["express"], startDate: "2024-10-22", endDate: "2026-10-22" },
    [{ kind: "failed", message: "spawn dependency-metrics ENOENT" }],
  );
  assert.deepEqual(job.comparison, {
    ecosystem: "npm",
    packages: ["express"],
    start_date: "2024-10-22",
    end_date: "2026-10-22",
    mode: "window_summary",
    weighting_type: "disable",
    half_life: null,
  });
  assert.deepEqual(comparisonRows(job), [
    { package: "express", mttu: "–", mttr: "–", dependencies: 0, error: "spawn dependency-metrics ENOENT" },
  ]);
});
