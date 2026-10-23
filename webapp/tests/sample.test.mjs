// Tests for the Sample Comparison (spec #39, ticket #45): the precomputed file that the
// Webapp shows at once, with no Job, and the label that names its compute date.
import { test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  sampleLabel,
  SAMPLE_COMPARISON_PATH,
  resultRows,
  barChartData,
  chartHasValues,
} from "../site/static/js/comparison.js";
import { explainComparison } from "../site/static/js/explanation.js";

const here = dirname(fileURLToPath(import.meta.url));
const siteDir = join(here, "..", "site");
const contractDir = join(here, "..", "..", "contract", "examples");
const readJson = (path) => JSON.parse(readFileSync(path, "utf8"));
const samplePath = join(siteDir, SAMPLE_COMPARISON_PATH);
const sample = existsSync(samplePath) ? readJson(samplePath) : null;
const contract = readJson(join(contractDir, "job_done_window_summary.json"));

// The fields that the Sample Comparison adds to the done-Job body of the Host.
const SAMPLE_FIELDS = ["computed_on", "osv_index_date"];
const keys = (object) => Object.keys(object).sort();

test("the label of the Sample Comparison names the date of the computation", () => {
  assert.equal(sampleLabel({ computed_on: "2026-10-23" }), "Precomputed on 2026-10-23");
});

test("the Sample Comparison file is in the published folder", () => {
  assert.ok(sample !== null, `${samplePath} is missing`);
});

test("the Sample Comparison has the shape of the done-Job contract example", () => {
  const body = Object.fromEntries(
    Object.entries(sample).filter(([key]) => !SAMPLE_FIELDS.includes(key)),
  );
  assert.deepEqual(keys(body), keys(contract));
  assert.equal(body.state, "done");
  assert.deepEqual(keys(body.comparison), keys(contract.comparison));
  const [contractResult] = contract.results;
  assert.equal(body.results.length, body.comparison.packages.length);
  for (const result of body.results) {
    assert.deepEqual(keys(result), keys(contractResult));
    assert.deepEqual(keys(result.summary), keys(contractResult.summary));
    assert.equal(result.data_source, "depsdev");
  }
});

test("the Sample Comparison is the agreed Comparison: npm express, koa, fastify", () => {
  assert.deepEqual(sample.comparison, {
    ecosystem: "npm",
    packages: ["express", "koa", "fastify"],
    start_date: "2024-01-01",
    end_date: "2025-12-31",
    mode: "window_summary",
    weighting_type: "disable",
    half_life: null,
  });
  assert.deepEqual(
    sample.results.map((result) => result.package),
    ["express", "koa", "fastify"],
  );
});

test("the Sample Comparison records its compute date and the date of the OSV index", () => {
  assert.match(sample.computed_on, /^\d{4}-\d{2}-\d{2}$/);
  // A sample without an OSV index would show an MTTR of 0 for every package.
  assert.match(sample.osv_index_date, /^\d{4}-\d{2}-\d{2}$/);
});

test("each package of the Sample Comparison has a value, a chart bar, and an Explanation", () => {
  for (const result of sample.results) {
    assert.equal(result.summary.status, "ok", result.package);
    assert.equal(result.error, "", result.package);
  }
  assert.equal(resultRows(sample).length, 3);
  assert.ok(chartHasValues(barChartData(sample)));
  const explanation = explainComparison(sample);
  assert.equal(explanation.packages.length, 3);
  assert.ok(explanation.limitations.some((text) => text.includes(`as of ${sample.osv_index_date}`)));
});
