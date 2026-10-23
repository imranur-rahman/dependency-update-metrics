// Tests for the Explanation module (webapp/site/static/js/explanation.js) with fixed result
// objects in the shape of the Host API results (contract/examples/).
import { test } from "node:test";
import assert from "node:assert/strict";

import { explainComparison, explainResult, hoverExplanation, LIMITATIONS } from "../site/static/js/explanation.js";

function summaryResult(summary, { pkg = "mypkg", weighting = "disable", halfLife = null } = {}) {
  return {
    ecosystem: "npm",
    package: pkg,
    mode: "window_summary",
    start_date: "2023-01-01",
    end_date: "2025-01-01",
    weighting_type: weighting,
    half_life: halfLife,
    data_source: "depsdev",
    summary: { num_dependencies: 3, status: "ok", error: "", ...summary },
    releases: [],
    error: "",
  };
}

function release(version, date, mttu, mttr, extra = {}) {
  return {
    package_version: version,
    package_release_date: date,
    mttu,
    mttr,
    num_dependencies: 2,
    status: "ok",
    error: "",
    ...extra,
  };
}

function perReleaseResult(releases, pkg = "mypkg") {
  return { ...summaryResult({}, { pkg }), mode: "per_release", summary: null, releases };
}

const MTTU_ZERO = "In this time window, each direct dependency used the newest available version at all times.";
const MTTR_ZERO =
  "In this time window, no direct dependency stayed on a version with a known vulnerability after a fix was available.";

test("MTTU 0.0 and MTTR 0.0 use the agreed sentences", () => {
  const explanation = explainResult(summaryResult({ mttu: 0, mttr: 0 }));
  assert.deepEqual(explanation.sentences, [MTTU_ZERO, MTTR_ZERO]);
});

test("MTTR 0.0 never claims that no dependency was vulnerable", () => {
  const { sentences } = explainResult(summaryResult({ mttu: 0, mttr: 0 }));
  for (const sentence of sentences) assert.doesNotMatch(sentence, /had no vulnerab|no vulnerable/i);
  assert.doesNotMatch(hoverExplanation(summaryResult({ mttu: 0, mttr: 0 })), /had no vulnerab|no vulnerable/i);
});

test("without weighting, each metric gets one sentence that says it is a sum of days", () => {
  const explanation = explainResult(summaryResult({ mttu: 579, mttr: 12.25 }));
  assert.equal(explanation.title, "mypkg");
  assert.deepEqual(explanation.sentences, [
    "MTTU is 579.0 days: in this time window, a direct dependency of mypkg was behind the newest available version for a sum of 579.0 days, as a mean over the direct dependencies.",
    "MTTR is 12.3 days: in this time window, a direct dependency of mypkg stayed on a version with a known vulnerability after a fix was available for a sum of 12.3 days, as a mean over the direct dependencies.",
  ]);
});

test("with exponential weighting, the text says on average and names the half-life", () => {
  const result = summaryResult({ mttu: 4, mttr: 1.5 }, { weighting: "exponential", halfLife: 180 });
  assert.deepEqual(explainResult(result).sentences, [
    "MTTU is 4.0 days: in this time window, a direct dependency of mypkg was behind the newest available version for 4.0 days on average, with exponential weighting and a half-life of 180 days.",
    "MTTR is 1.5 days: in this time window, a direct dependency of mypkg stayed on a version with a known vulnerability after a fix was available for 1.5 days on average, with exponential weighting and a half-life of 180 days.",
  ]);
});

test("with linear weighting, the text says on average and names the weighting", () => {
  const result = summaryResult({ mttu: 4, mttr: 0 }, { weighting: "linear" });
  assert.equal(
    explainResult(result).sentences[0],
    "MTTU is 4.0 days: in this time window, a direct dependency of mypkg was behind the newest available version for 4.0 days on average, with linear weighting.",
  );
});

test("a package without dependencies or with an error gets no Explanation", () => {
  assert.equal(explainResult(summaryResult({ mttu: 0, mttr: 0, status: "no_deps", num_dependencies: 0 })), null);
  assert.equal(explainResult(summaryResult({ mttu: 0, mttr: 0, num_dependencies: 0 })), null);
  assert.equal(explainResult(summaryResult({ mttu: null, mttr: null, status: "error", error: "boom" })), null);
  assert.equal(explainResult({ ...summaryResult({}), summary: null, error: "Package not found" }), null);
  assert.equal(explainResult({ ...summaryResult({ mttu: 1, mttr: 1 }), error: "Package not found" }), null);
});

test("Per-release explains the latest release and the change over the window", () => {
  const result = perReleaseResult([
    release("1.0.0", "2023-02-01", 4, 2),
    release("1.1.0", "2023-06-01", null, null, { status: "error", error: "boom" }),
    release("1.2.0", "2023-09-01", 1, 2),
    release("2.0.0", "2024-03-01", 0, 0),
    release("2.1.0", "2024-06-01", 0, 0, { status: "no_deps", num_dependencies: 0 }),
  ]);
  const explanation = explainResult(result);
  assert.equal(explanation.title, "mypkg 2.0.0, the latest release with a value (2024-03-01)");
  assert.deepEqual(explanation.sentences, [
    MTTU_ZERO,
    MTTR_ZERO,
    "MTTU went from 4.0 to 0.0 days over 3 releases.",
    "MTTR went from 2.0 to 0.0 days over 3 releases.",
  ]);
});

test("Per-release with one release with a value has no change sentence", () => {
  const explanation = explainResult(perReleaseResult([release("1.0.0", "2023-02-01", 3, 0)]));
  assert.equal(explanation.sentences.length, 2);
});

test("Per-release without a release with a value gets no Explanation", () => {
  assert.equal(explainResult(perReleaseResult([])), null);
  const failed = release("1.0.0", "2023-02-01", null, null, { status: "error", error: "boom" });
  assert.equal(explainResult(perReleaseResult([failed])), null);
});

test("a Comparison has one Explanation for each package with a value and one Limitations list", () => {
  const job = {
    comparison: { mode: "window_summary", weighting_type: "disable", half_life: null },
    results: [
      summaryResult({ mttu: 1, mttr: 0 }, { pkg: "a" }),
      summaryResult({ mttu: 0, mttr: 0, status: "no_deps", num_dependencies: 0 }, { pkg: "b" }),
      summaryResult({ mttu: 0, mttr: 0 }, { pkg: "c" }),
    ],
  };
  const explanation = explainComparison(job);
  assert.deepEqual(
    explanation.packages.map((item) => item.title),
    ["a", "c"],
  );
  assert.deepEqual(explanation.limitations, LIMITATIONS);
});

test("a Comparison without a value has no Explanation and no Limitations", () => {
  const job = {
    comparison: { mode: "window_summary", weighting_type: "disable", half_life: null },
    results: [{ ...summaryResult({}), summary: null, error: "Package not found" }],
  };
  assert.deepEqual(explainComparison(job), { packages: [], limitations: [] });
});

test("the Limitations list names each limitation of the data", () => {
  const text = LIMITATIONS.join(" ");
  assert.equal(LIMITATIONS.length, 5);
  for (const words of [/with a fix/, /OSV data as of the date/, /no version constraint/, /direct dependencies/, /release dates/]) {
    assert.match(text, words);
  }
});

test("the hover line is short and follows the values and the weighting", () => {
  assert.equal(
    hoverExplanation(summaryResult({ mttu: 0, mttr: 0 })),
    "Each direct dependency used the newest version at all times, and none stayed on a known vulnerable version after a fix was available.",
  );
  assert.equal(
    hoverExplanation(summaryResult({ mttu: 579, mttr: 12 })),
    "A direct dependency was behind the newest version for a sum of 579.0 days, and on a known vulnerable version after a fix for a sum of 12.0 days.",
  );
  assert.equal(
    hoverExplanation(summaryResult({ mttu: 4, mttr: 0 }, { weighting: "exponential", halfLife: 90 })),
    "A direct dependency was behind the newest version for 4.0 days on average (half-life 90 days), and none stayed on a known vulnerable version after a fix was available.",
  );
  assert.equal(hoverExplanation(summaryResult({ mttu: 0, mttr: 0, status: "no_deps", num_dependencies: 0 })), null);
});
