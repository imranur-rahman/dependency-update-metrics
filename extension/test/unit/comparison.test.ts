// Tests for the Comparison model of the core module (src/core/comparison.ts): the input of
// the "MTTX: Compare packages" command, and the Job-shaped object that the webview gives to
// the Webapp comparison.js.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";

import {
  barChartJob,
  compareLink,
  comparisonJob,
  comparisonRows,
  explanationContent,
  noDependenciesNotes,
  PACKAGE_COLORS,
  PACKAGE_POINT_STYLES,
  parsePackageNames,
  SERIES_COLORS,
  type ComparisonRequest,
} from "../../src/core/comparison.ts";
import type { ToolResult } from "../../src/core/display.ts";
import { barChartData } from "../../../webapp/site/static/js/comparison.js";
import { LIMITATIONS } from "../../../webapp/site/static/js/explanation.js";

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

// A Comparison request with the default settings.
function request(changes: Partial<ComparisonRequest>): ComparisonRequest {
  return {
    ecosystem: "npm",
    packages: [],
    startDate: "2020-01-01",
    endDate: "2023-01-01",
    weightingType: "disable",
    halfLife: null,
    dataSource: "depsdev",
    ...changes,
  };
}

test("the Job of the Tool results has the shape of a done Host Job", () => {
  const example = exampleJob("job_done_with_package_error.json");
  const outcomes = example.results.map((result: ToolResult) => ({ kind: "result", result }) as const);
  const job = comparisonJob(
    request({ ecosystem: "pypi", packages: ["mypkg", "nope"], startDate: "2019-06-01", endDate: "2019-07-01" }),
    outcomes,
  );
  assert.deepEqual(job, { ...example, job_id: "mttx-extension" });
});

test("a package without dependencies shows the no-dependencies text and no bar slot", () => {
  const [ok] = exampleJob("job_done_window_summary.json").results as ToolResult[];
  const noDeps: ToolResult = {
    ...ok,
    package: "left-pad",
    summary: { ...ok.summary!, mttu: 0, mttr: 0, num_dependencies: 0, status: "no_deps" },
  };
  const job = comparisonJob(
    request({ ecosystem: "pypi", packages: ["mypkg", "left-pad"] }),
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
  // The bar chart leaves the package out, so the chart has no empty slot for it, and a note
  // next to the chart gives the text.
  const bars = barChartData(barChartJob(job));
  assert.deepEqual(bars.labels, ["mypkg"]);
  assert.deepEqual(bars.datasets.map((dataset: { data: unknown[] }) => dataset.data), [[579], [579]]);
  assert.deepEqual(noDependenciesNotes(job), ["could not compute since 'left-pad' has no dependencies"]);
});

test("the panel shows one Explanation paragraph for each package with a value, then the Limitations", () => {
  const example = exampleJob("job_done_with_package_error.json");
  const [ok] = exampleJob("job_done_window_summary.json").results as ToolResult[];
  const noDeps: ToolResult = {
    ...ok,
    package: "left-pad",
    summary: { ...ok.summary!, mttu: 0, mttr: 0, num_dependencies: 0, status: "no_deps" },
  };
  const failed = example.results[1] as ToolResult;
  const job = comparisonJob(request({ ecosystem: "pypi", packages: ["mypkg", "left-pad", "nope"] }), [
    { kind: "result", result: ok },
    { kind: "result", result: noDeps },
    { kind: "result", result: failed },
  ]);
  assert.deepEqual(explanationContent(job), {
    paragraphs: [
      "mypkg: MTTU is 579.0 days: in this time window, a direct dependency of mypkg was behind the newest " +
        "available version for 579.0 days in total, on average over the direct dependencies. MTTR is 579.0 days: " +
        "in this time window, a direct dependency of mypkg stayed on a version with a known vulnerability after " +
        "a fix was available for 579.0 days in total, on average over the direct dependencies.",
    ],
    limitations: LIMITATIONS,
  });
});

test("the Limitations of the panel name the build date of the OSV index", () => {
  const [ok] = exampleJob("job_done_window_summary.json").results as ToolResult[];
  const job = comparisonJob(request({ packages: ["mypkg"] }), [{ kind: "result", result: ok }], "2026-10-21");
  assert.equal(job.osv_index_date, "2026-10-21");
  assert.match(explanationContent(job).limitations[1], /^The results use the OSV data as of 2026-10-21, /);
  const withoutDate = comparisonJob(request({ packages: ["mypkg"] }), [{ kind: "result", result: ok }]);
  assert.equal("osv_index_date" in withoutDate, false);
});

test("a Comparison where each package failed has no Explanation in the panel", () => {
  const failed = exampleJob("job_done_with_package_error.json").results[1] as ToolResult;
  const job = comparisonJob(request({ packages: ["nope"] }), [{ kind: "result", result: failed }]);
  assert.deepEqual(explanationContent(job), { paragraphs: [], limitations: [] });
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
  assert.match(webapp, /const PACKAGE_POINT_STYLES = \["circle", "rect", "triangle", "rectRot", "crossRot"\];/);
  assert.deepEqual(PACKAGE_POINT_STYLES, ["circle", "rect", "triangle", "rectRot", "crossRot"]);
});

test("the webview chart boxes take the shared size of the Webapp charts", () => {
  const source = (...path: string[]) => readFileSync(join(import.meta.dirname, "..", "..", "src", ...path), "utf8");
  const webview = source("webview", "main.ts");
  assert.match(webview, /Object\.assign\(box\.style, chartBoxStyle\(\)\);/);
  assert.doesNotMatch(webview, /style\.(height|width|maxWidth) =/);
  // The style element of the panel sets no size, so that chartBoxStyle is the one source.
  const boxRule = /\.chart-box \{([^}]*)\}/.exec(source("comparisonPanel.ts"));
  assert.ok(boxRule, "the panel style has no .chart-box rule");
  assert.doesNotMatch(boxRule[1], /width|height/);
});

test("the hover link starts a Comparison with the package of the dependency", () => {
  const link = compareLink({ ecosystem: "npm", name: "@types/node" });
  const match = /^\[Compare with alternatives\]\(command:mttx\.compare\?(.+)\)$/.exec(link);
  assert.ok(match, link);
  assert.ok(!/[\s()]/.test(match[1]), "the link target has no space and no parenthesis");
  assert.deepEqual(JSON.parse(decodeURIComponent(match[1])), [{ ecosystem: "npm", packages: ["@types/node"] }]);
});

test("a Tool run without a result becomes a package error with the settings of the request", () => {
  const job = comparisonJob(
    request({
      ecosystem: "npm",
      packages: ["express"],
      startDate: "2024-10-22",
      endDate: "2026-10-22",
      weightingType: "exponential",
      halfLife: 30,
      dataSource: "native",
    }),
    [{ kind: "failed", message: "spawn dependency-metrics ENOENT" }],
  );
  assert.deepEqual(job.comparison, {
    ecosystem: "npm",
    packages: ["express"],
    start_date: "2024-10-22",
    end_date: "2026-10-22",
    mode: "window_summary",
    weighting_type: "exponential",
    half_life: 30,
  });
  assert.equal(job.results[0].data_source, "native");
  assert.deepEqual(comparisonRows(job), [
    { package: "express", mttu: "–", mttr: "–", dependencies: 0, error: "spawn dependency-metrics ENOENT" },
  ]);
});

test("a Tool run without a result has the shape of the failed result of the Tool contract", () => {
  const example = exampleJob("job_done_with_package_error.json");
  const job = comparisonJob(request({ ecosystem: "pypi", packages: ["mypkg", "nope"] }), [
    { kind: "result", result: example.results[0] },
    { kind: "failed", message: "Package not found in deps.dev" },
  ]);
  assert.deepEqual(job.results[1], example.results[1]);
});
