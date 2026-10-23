// Tests for the report of the Action (src/report.ts): the output rows, the thresholds, the
// shields.io badge JSON, and the Markdown of the PR comment and the job summary.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";

import type { ToolResult } from "../../extension/src/core/display.ts";
import {
  badgeFiles,
  COMMENT_MARKER,
  outputRows,
  reportMarkdown,
  thresholdFailures,
  type PackageOutcome,
} from "../src/report.ts";

const EXAMPLES = join(import.meta.dirname, "..", "..", "contract", "examples");

function example(name: string, index: number): ToolResult {
  return JSON.parse(readFileSync(join(EXAMPLES, name), "utf8")).results[index];
}

const OK = example("job_done_window_summary.json", 0);
const NO_DEPS: ToolResult = {
  ...OK,
  package: "left-pad",
  summary: { mttu: 0, mttr: 0, num_dependencies: 0, status: "no_deps", error: "" },
};
const PACKAGE_ERROR = example("job_done_with_package_error.json", 1);

const outcomes: PackageOutcome[] = [
  { ecosystem: "pypi", name: "mypkg", files: ["pyproject.toml"], outcome: { kind: "result", result: OK } },
  { ecosystem: "pypi", name: "left-pad", files: ["pyproject.toml"], outcome: { kind: "result", result: NO_DEPS } },
  { ecosystem: "pypi", name: "nope", files: ["a/pyproject.toml", "b/pyproject.toml"], outcome: { kind: "result", result: PACKAGE_ERROR } },
  { ecosystem: "npm", name: "gone", files: ["package.json"], outcome: { kind: "failed", message: "the Tool stopped after 600 s\nmore" } },
];

test("the output rows give each package its values, no_deps, or its error", () => {
  assert.deepEqual(outputRows(outcomes), [
    { ecosystem: "pypi", package: "mypkg", mttu: 579, mttr: 579, num_dependencies: 1, status: "ok", error: "" },
    { ecosystem: "pypi", package: "left-pad", mttu: null, mttr: null, num_dependencies: 0, status: "no_deps", error: "" },
    { ecosystem: "pypi", package: "nope", mttu: null, mttr: null, num_dependencies: 0, status: "error", error: "Package not found in deps.dev" },
    { ecosystem: "npm", package: "gone", mttu: null, mttr: null, num_dependencies: 0, status: "error", error: "the Tool stopped after 600 s\nmore" },
  ]);
});

test("a value above a threshold fails; no dependencies and errors never fail", () => {
  const rows = outputRows(outcomes);
  assert.deepEqual(thresholdFailures(rows, { mttu: null, mttr: null }), []);
  assert.deepEqual(thresholdFailures(rows, { mttu: 579, mttr: 579 }), []);
  assert.deepEqual(thresholdFailures(rows, { mttu: 600, mttr: 30 }), [
    "The MTTR of mypkg (pypi) is 579.00 days, more than max-mttr-days (30).",
  ]);
  assert.deepEqual(thresholdFailures(rows, { mttu: 0, mttr: 0 }).length, 2);
});

test("the badge files follow the shields.io endpoint schema, with one file for each metric of each package", () => {
  const files = badgeFiles(outputRows(outcomes.slice(0, 3)), { mttu: null, mttr: 30 });
  assert.deepEqual(files, [
    { fileName: "pypi-mypkg-mttu.json", json: { schemaVersion: 1, label: "MTTU", message: "579.00 days", color: "blue" } },
    { fileName: "pypi-mypkg-mttr.json", json: { schemaVersion: 1, label: "MTTR", message: "579.00 days", color: "red" } },
    { fileName: "pypi-left-pad-mttu.json", json: { schemaVersion: 1, label: "MTTU", message: "no dependencies", color: "lightgrey" } },
    { fileName: "pypi-left-pad-mttr.json", json: { schemaVersion: 1, label: "MTTR", message: "no dependencies", color: "lightgrey" } },
    { fileName: "pypi-nope-mttu.json", json: { schemaVersion: 1, label: "MTTU", message: "error", color: "lightgrey" } },
    { fileName: "pypi-nope-mttr.json", json: { schemaVersion: 1, label: "MTTR", message: "error", color: "lightgrey" } },
  ]);
  const green = badgeFiles(outputRows(outcomes.slice(0, 1)), { mttu: 1000, mttr: null });
  assert.equal(green[0].json.color, "brightgreen");
});

test("a scoped npm name gives a badge file name without a folder", () => {
  const [file] = badgeFiles(
    [{ ecosystem: "npm", package: "@types/node", mttu: 1, mttr: 0, num_dependencies: 1, status: "ok", error: "" }],
    { mttu: null, mttr: null },
  );
  assert.equal(file.fileName, "npm-types__node-mttu.json");
});

test("the report has a table, the no-dependencies text, the error, and the Explanation", () => {
  const markdown = reportMarkdown(outcomes, {
    heading: "MTTU and MTTR of the dependencies that this pull request adds",
    thresholds: { mttu: null, mttr: 30 },
    showFiles: true,
  });
  const lines = markdown.split("\n");
  assert.equal(lines[0], COMMENT_MARKER);
  assert.ok(lines.includes("### MTTU and MTTR of the dependencies that this pull request adds"));
  assert.ok(lines.includes("| Package | Ecosystem | MTTU (days) | MTTR (days) | Dependencies | Manifest | Note |"));
  assert.ok(lines.includes("| `mypkg` | pypi | 579.00 | **579.00** | 1 | `pyproject.toml` | MTTR is more than max-mttr-days (30) |"));
  assert.ok(lines.includes("| `left-pad` | pypi | – | – | 0 | `pyproject.toml` | could not compute since 'left-pad' has no dependencies |"));
  assert.ok(lines.includes("| `nope` | pypi | – | – | 0 | `a/pyproject.toml`, `b/pyproject.toml` | error: Package not found in deps.dev |"));
  assert.ok(lines.includes("| `gone` | npm | – | – | 0 | `package.json` | error: the Tool stopped after 600 s more |"));
  assert.match(markdown, /Window: 2020-01-01 to 2023-01-01 · Data source: deps\.dev · Weighting: disable/);
  assert.match(markdown, /Total days: for each direct dependency/);
  assert.match(markdown, /#### What the results mean/);
  assert.match(markdown, /\*\*mypkg\*\*: MTTU is 579\.00 days: in this time window/);
  assert.match(markdown, /#### Limitations\n\n- Only vulnerabilities with a fix count\./);
});

test("the report names the build date of the OSV index in the Limitations when it is known", () => {
  const markdown = reportMarkdown(outcomes.slice(0, 1), {
    heading: "MTTU and MTTR",
    thresholds: { mttu: null, mttr: null },
    showFiles: false,
    osvIndexDate: "2026-10-21",
  });
  assert.match(markdown, /OSV data as of 2026-10-21, the build date of the OSV index/);
  assert.ok(markdown.includes("| Package | Ecosystem | MTTU (days) | MTTR (days) | Dependencies | Note |"));
});

test("the report escapes the table characters of an error text", () => {
  const markdown = reportMarkdown(
    [{ ecosystem: "npm", name: "x", files: [], outcome: { kind: "failed", message: "bad | value <b>" } }],
    { heading: "MTTU and MTTR", thresholds: { mttu: null, mttr: null }, showFiles: false },
  );
  assert.ok(markdown.includes("| error: bad \\| value &lt;b&gt; |"));
});
