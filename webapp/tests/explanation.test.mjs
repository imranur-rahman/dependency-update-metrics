// Tests for the Explanation module (webapp/site/static/js/explanation.js) with fixed result
// objects in the shape of the Host API results (contract/examples/).
import { test } from "node:test";
import assert from "node:assert/strict";

import {
  explainComparison,
  explainResult,
  explanationBlocks,
  hoverExplanation,
  LIMITATIONS,
} from "../site/static/js/explanation.js";

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
const MTTU_WHY =
  "MTTU goes up when a dependency publishes a newer version and the package keeps the older one. " +
  "It goes down when a release of the package moves to newer versions.";
const MTTR_WHY =
  "MTTR goes up when a fix is published and the package stays on the vulnerable version. " +
  "It goes down when a release moves to the fixed version.";

test("MTTU 0.00 and MTTR 0.00 use the agreed sentences", () => {
  const explanation = explainResult(summaryResult({ mttu: 0, mttr: 0 }));
  assert.deepEqual(explanation.sentences, [MTTU_ZERO, MTTR_ZERO]);
});

test("a value that shows as 0.00 uses the agreed 0.00 sentences", () => {
  const explanation = explainResult(summaryResult({ mttu: 0.003, mttr: 0.0049 }));
  assert.deepEqual(explanation.sentences, [MTTU_ZERO, MTTR_ZERO]);
  assert.equal(
    hoverExplanation(summaryResult({ mttu: 0.003, mttr: 0.004 })),
    "Each direct dependency used the newest version at all times, and none stayed on a known vulnerable version after a fix was available.",
  );
  assert.match(explainResult(summaryResult({ mttu: 0.006, mttr: 0 })).sentences[0], /^MTTU is 0\.01 days: /);
});

test("MTTR 0.00 never claims that no dependency was vulnerable", () => {
  const { sentences } = explainResult(summaryResult({ mttu: 0, mttr: 0 }));
  for (const sentence of sentences) assert.doesNotMatch(sentence, /had no vulnerab|no vulnerable/i);
  assert.doesNotMatch(hoverExplanation(summaryResult({ mttu: 0, mttr: 0 })), /had no vulnerab|no vulnerable/i);
});

test("without weighting, each metric gets one sentence that says it is a total per dependency, averaged", () => {
  const explanation = explainResult(summaryResult({ mttu: 579, mttr: 12.25 }));
  assert.equal(explanation.title, "mypkg");
  assert.deepEqual(explanation.sentences, [
    "MTTU is 579.00 days: in this time window, a direct dependency of mypkg was behind the newest available version for 579.00 days in total, on average over the direct dependencies.",
    "MTTR is 12.25 days: in this time window, a direct dependency of mypkg stayed on a version with a known vulnerability after a fix was available for 12.25 days in total, on average over the direct dependencies.",
  ]);
});

test("with exponential weighting, the text says on average and names the half-life", () => {
  const result = summaryResult({ mttu: 4, mttr: 1.5 }, { weighting: "exponential", halfLife: 180 });
  assert.deepEqual(explainResult(result).sentences, [
    "MTTU is 4.00 days: in this time window, a direct dependency of mypkg was behind the newest available version for 4.00 days on average, with exponential weighting and a half-life of 180 days.",
    "MTTR is 1.50 days: in this time window, a direct dependency of mypkg stayed on a version with a known vulnerability after a fix was available for 1.50 days on average, with exponential weighting and a half-life of 180 days.",
  ]);
});

test("with linear weighting, the text says on average and names the weighting", () => {
  const result = summaryResult({ mttu: 4, mttr: 0 }, { weighting: "linear" });
  assert.equal(
    explainResult(result).sentences[0],
    "MTTU is 4.00 days: in this time window, a direct dependency of mypkg was behind the newest available version for 4.00 days on average, with linear weighting.",
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
    "MTTU went from 4.00 to 0.00 days over 3 releases.",
    "The largest drop of MTTU was at 1.2.0 (2023-09-01), from 4.00 to 1.00 days.",
    MTTU_WHY,
    "MTTR went from 2.00 to 0.00 days over 3 releases.",
    "The largest drop of MTTR was at 2.0.0 (2024-03-01), from 2.00 to 0.00 days.",
    MTTR_WHY,
  ]);
});

test("Per-release names the largest rise and the largest drop, and the effect of weighting", () => {
  const result = {
    ...perReleaseResult([
      release("1.0.0", "2023-02-01", 1, 0),
      release("1.1.0", "2023-04-01", 3, 5),
      release("1.2.0", "2023-06-01", 10, 4),
      release("1.3.0", "2023-08-01", 2, 4),
    ]),
    weighting_type: "linear",
  };
  const sentences = explainResult(result).sentences.slice(2);
  assert.deepEqual(sentences, [
    "MTTU went from 1.00 to 2.00 days over 4 releases.",
    "The largest rise of MTTU was at 1.2.0 (2023-06-01), from 3.00 to 10.00 days.",
    "The largest drop of MTTU was at 1.3.0 (2023-08-01), from 10.00 to 2.00 days.",
    MTTU_WHY,
    "MTTR went from 0.00 to 4.00 days over 4 releases.",
    "The largest rise of MTTR was at 1.1.0 (2023-04-01), from 0.00 to 5.00 days.",
    "The largest drop of MTTR was at 1.2.0 (2023-06-01), from 5.00 to 4.00 days.",
    MTTR_WHY,
    "With weighting, recent days count more, so a value can also change as time passes.",
  ]);
});

test("Per-release names no rise and no drop when the shown values stay the same", () => {
  const result = perReleaseResult([release("1.0.0", "2023-02-01", 1, 0), release("1.1.0", "2023-04-01", 1.001, 0)]);
  assert.deepEqual(explainResult(result).sentences.slice(2), [
    "MTTU went from 1.00 to 1.00 days over 2 releases.",
    MTTU_WHY,
    "MTTR went from 0.00 to 0.00 days over 2 releases.",
    MTTR_WHY,
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

test("the Limitations name the date of the OSV index when the Job carries one", () => {
  const job = {
    comparison: { mode: "window_summary", weighting_type: "disable", half_life: null },
    results: [summaryResult({ mttu: 1, mttr: 0 })],
    osv_index_date: "2026-10-21",
  };
  const { limitations } = explainComparison(job);
  assert.equal(
    limitations[1],
    "The results use the OSV data as of 2026-10-21, the build date of the OSV index. A vulnerability that OSV added later does not count.",
  );
  assert.deepEqual(
    limitations.filter((_, index) => index !== 1),
    LIMITATIONS.filter((_, index) => index !== 1),
  );
  assert.deepEqual(explainComparison({ ...job, osv_index_date: null }).limitations, LIMITATIONS);
});

test("the blocks of the Explanation give the layout that the Webapp and the Extension draw", () => {
  const job = {
    comparison: { mode: "window_summary", weighting_type: "disable", half_life: null },
    results: [summaryResult({ mttu: 0, mttr: 0 }, { pkg: "a" })],
  };
  assert.deepEqual(explanationBlocks(job), [
    { kind: "heading", text: "What the results mean" },
    { kind: "paragraph", title: "a", text: `${MTTU_ZERO} ${MTTR_ZERO}` },
    { kind: "subheading", text: "Limitations" },
    { kind: "list", items: LIMITATIONS },
  ]);
  const failed = { ...job, results: [{ ...summaryResult({}), summary: null, error: "Package not found" }] };
  assert.deepEqual(explanationBlocks(failed), []);
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
    "A direct dependency was behind the newest version for 579.00 days in total (mean over the direct dependencies), and on a known vulnerable version after a fix for 12.00 days in total (mean over the direct dependencies).",
  );
  assert.equal(
    hoverExplanation(summaryResult({ mttu: 4, mttr: 0 }, { weighting: "exponential", halfLife: 90 })),
    "A direct dependency was behind the newest version for 4.00 days on average (half-life 90 days), and none stayed on a known vulnerable version after a fix was available.",
  );
  assert.equal(hoverExplanation(summaryResult({ mttu: 0, mttr: 0, status: "no_deps", num_dependencies: 0 })), null);
});
