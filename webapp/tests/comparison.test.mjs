// Tests for the Webapp logic module (webapp/site/static/js/comparison.js).
// They read the shared API contract examples in contract/examples/, the same files that
// the Host tests read, so a change to the contract fails both sides.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  requestFromForm,
  describeJob,
  isFinished,
  resultRows,
  formatDays,
  progressOf,
  jobIdFromUrl,
  urlWithJobId,
  JOB_EVENTS,
  packageErrors,
  canAddPackage,
  withComparison,
  comparisonFromSearch,
  formFromComparison,
  barChartData,
  barChartAxisLabel,
  rowsToCsv,
  csvFileName,
  WEIGHTING_TYPES,
  needsHalfLife,
  halfLifeError,
  axisLabel,
  resultsNote,
} from "../site/static/js/comparison.js";

const contractDir = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "contract", "examples");
const example = (name) => JSON.parse(readFileSync(join(contractDir, `${name}.json`), "utf8"));

test("the form gives the request body that the Host accepts", () => {
  const form = {
    ecosystem: "pypi",
    packages: ["  mypkg ", ""],
    startDate: "2020-01-01",
    endDate: "2023-01-01",
  };

  assert.deepEqual(requestFromForm(form, "example-token"), example("create_job_request"));
});

test("the form accepts 1 to 5 different packages", () => {
  assert.deepEqual(packageErrors("npm", ["express"]), []);
  assert.deepEqual(packageErrors("npm", ["a", "b", "c", "d", "e"]), []);
});

test("the form refuses no package, a sixth package, and duplicates", () => {
  assert.deepEqual(packageErrors("npm", ["", "  "]), ["Enter at least one package."]);
  assert.deepEqual(packageErrors("npm", ["a", "b", "c", "d", "e", "f"]), [
    "Enter at most 5 packages.",
  ]);
  assert.deepEqual(packageErrors("npm", ["express", "Express "]), [
    "Express is in the Comparison twice.",
  ]);
  assert.deepEqual(packageErrors("pypi", ["zope.interface", "Zope_Interface"]), [
    "Zope_Interface is in the Comparison twice.",
  ]);
});

test("the form allows a new package field only while it has fewer than 5", () => {
  assert.equal(canAddPackage(["a", "b", "c", "d"]), true);
  assert.equal(canAddPackage(["a", "b", "c", "d", "e"]), false);
});

test("a Comparison survives the round trip through the page URL", () => {
  const comparison = {
    ecosystem: "npm",
    packages: ["express", "@types/node", "a&b=c"],
    start_date: "2023-01-01",
    end_date: "2025-01-01",
    mode: "window_summary",
    weighting_type: "exponential",
    half_life: 90,
  };

  const search = withComparison("", comparison);

  assert.deepEqual(comparisonFromSearch(search), comparison);
});

test("the URL of the contract Comparison is short and readable", () => {
  const comparison = example("job_queued").comparison;

  assert.equal(
    withComparison("", comparison),
    "?ecosystem=pypi&package=mypkg&start_date=2020-01-01&end_date=2023-01-01" +
      "&mode=window_summary&weighting_type=disable",
  );
  assert.deepEqual(comparisonFromSearch(withComparison("", comparison)), comparison);
});

test("the URL keeps the parameters that are not part of the Comparison", () => {
  const search = withComparison("?job=abc&package=old", example("job_queued").comparison);

  assert.equal(new URLSearchParams(search).get("job"), "abc");
  assert.deepEqual(comparisonFromSearch(search).packages, ["mypkg"]);
});

test("a URL without packages holds no Comparison", () => {
  assert.equal(comparisonFromSearch(""), null);
  assert.equal(comparisonFromSearch("?job=abc"), null);
});

test("a shared link fills the form", () => {
  const search = "?ecosystem=cargo&package=serde&package=tokio&start_date=2022-01-01&end_date=2024-06-30";

  assert.deepEqual(formFromComparison(comparisonFromSearch(search)), {
    ecosystem: "cargo",
    packages: ["serde", "tokio"],
    startDate: "2022-01-01",
    endDate: "2024-06-30",
    mode: "window_summary",
    weightingType: "disable",
    halfLife: "",
  });
});

test("a shared link fills the weighting type and the half-life", () => {
  const search = "?ecosystem=npm&package=express&start_date=2023-01-01&end_date=2025-01-01" +
    "&mode=window_summary&weighting_type=exponential&half_life=90";

  const form = formFromComparison(comparisonFromSearch(search));

  assert.equal(form.weightingType, "exponential");
  assert.equal(form.halfLife, "90");
});

test("the form state survives the round trip through the request and the URL", () => {
  const form = {
    ecosystem: "pypi",
    packages: ["requests", "flask"],
    startDate: "2021-01-01",
    endDate: "2024-01-01",
    mode: "window_summary",
    weightingType: "exponential",
    halfLife: "180",
  };
  const { turnstile_token: _token, ...comparison } = requestFromForm(form, "t");

  assert.deepEqual(formFromComparison(comparisonFromSearch(withComparison("", comparison))), form);
});

test("a queued Job shows its queue position", () => {
  assert.equal(describeJob(example("job_queued")), "Queued (position 1)");
  assert.equal(isFinished(example("job_queued")), false);
});

test("a queued Job shows a progress bar with no value", () => {
  assert.deepEqual(progressOf(example("job_queued")), { value: null, max: null });
});

test("a running Job shows which package the Host analyzes", () => {
  const job = example("job_running");

  assert.equal(describeJob(job), "Running (package 2 of 2)");
  assert.equal(isFinished(job), false);
  assert.deepEqual(progressOf(job), { value: 1, max: 2 });
});

test("a finished Job shows no progress bar", () => {
  assert.equal(progressOf(example("job_done_window_summary")), null);
  assert.equal(progressOf(example("job_error")), null);
});

test("a cancelled Job is finished", () => {
  const job = { job_id: "example-job-id", state: "cancelled" };

  assert.equal(describeJob(job), "Cancelled");
  assert.equal(isFinished(job), true);
});

test("the page listens to one event for each Job state", () => {
  assert.deepEqual(JOB_EVENTS, ["queued", "running", "done", "error", "cancelled"]);
});

test("the job ID goes into the page URL and back, and keeps the other parts", () => {
  const url = urlWithJobId("https://example.org/site/?ecosystem=npm#try-it", "abc123");

  assert.equal(url, "https://example.org/site/?ecosystem=npm&job=abc123#try-it");
  assert.equal(jobIdFromUrl(url), "abc123");
});

test("a page URL with no job ID gives null", () => {
  assert.equal(jobIdFromUrl("https://example.org/site/#try-it"), null);
  assert.equal(jobIdFromUrl("https://example.org/site/?job=#try-it"), null);
});

test("a Job that failed shows the message of the Host", () => {
  assert.equal(describeJob(example("job_error")), "Something went wrong (ref: example-job-id)");
  assert.equal(isFinished(example("job_error")), true);
});

test("a done Window summary Job gives one table row for each package", () => {
  const job = example("job_done_window_summary");

  assert.equal(describeJob(job), "Done");
  assert.equal(isFinished(job), true);
  assert.deepEqual(resultRows(job), [
    { package: "mypkg", mttu: "579.0", mttr: "579.0", dependencies: 1, error: "" },
  ]);
});

test("a package that failed gives a row with its error and no values", () => {
  const job = example("job_done_window_summary");
  job.results[0].summary = { mttu: null, mttr: null, num_dependencies: 0, status: "error", error: "boom" };
  job.results[0].error = "boom";

  assert.deepEqual(resultRows(job), [
    { package: "mypkg", mttu: "–", mttr: "–", dependencies: 0, error: "boom" },
  ]);
});

// A done Window summary Job of three packages, built from the contract example:
// "fast" and "slow" have results, and "missing" failed.
function threePackageJob() {
  const job = example("job_done_window_summary");
  const base = job.results[0];
  const result = (name, summary, error = "") => ({
    ...base,
    package: name,
    summary: { ...base.summary, ...summary },
    error,
  });
  job.comparison.packages = ["fast", "missing", "slow"];
  job.results = [
    result("fast", { mttu: 12.25, mttr: 0, num_dependencies: 4 }),
    result(
      "missing",
      { mttu: null, mttr: null, num_dependencies: 0, status: "error", error: "Package not found in deps.dev" },
      "Package not found in deps.dev",
    ),
    result("slow", { mttu: 300.5, mttr: 41, num_dependencies: 20 }),
  ];
  return job;
}

test("the bar chart has one group for each package, with an MTTU bar and an MTTR bar", () => {
  assert.deepEqual(barChartData(threePackageJob()), {
    labels: ["fast", "missing", "slow"],
    datasets: [
      { label: "MTTU", data: [12.25, null, 300.5] },
      { label: "MTTR", data: [0, null, 41] },
    ],
  });
});

test("the bar chart of the contract example has one group", () => {
  assert.deepEqual(barChartData(example("job_done_window_summary")), {
    labels: ["mypkg"],
    datasets: [
      { label: "MTTU", data: [579] },
      { label: "MTTR", data: [579] },
    ],
  });
});

test("the bar chart axis label follows the weighting type of the Job", () => {
  const job = threePackageJob();
  assert.equal(barChartAxisLabel(job), "Days (sum over intervals)");

  job.comparison.weighting_type = "linear";
  assert.equal(barChartAxisLabel(job), "Days (weighted mean)");
});

test("the table shows the values and the error of each package", () => {
  assert.deepEqual(resultRows(threePackageJob()), [
    { package: "fast", mttu: "12.3", mttr: "0.0", dependencies: 4, error: "" },
    { package: "missing", mttu: "–", mttr: "–", dependencies: 0, error: "Package not found in deps.dev" },
    { package: "slow", mttu: "300.5", mttr: "41.0", dependencies: 20, error: "" },
  ]);
});

test("the CSV download has the same columns and values as the table", () => {
  assert.equal(
    rowsToCsv(resultRows(threePackageJob())),
    "Package,MTTU (days),MTTR (days),Dependencies,Error\r\n" +
      "fast,12.3,0.0,4,\r\n" +
      "missing,,,0,Package not found in deps.dev\r\n" +
      "slow,300.5,41.0,20,\r\n",
  );
});

test("the CSV quotes values that hold a comma, a quote, or a line break", () => {
  const rows = [{ package: "@scope/a,b", mttu: "1.0", mttr: "2.0", dependencies: 1, error: 'say "no"\nnow' }];

  assert.equal(
    rowsToCsv(rows),
    "Package,MTTU (days),MTTR (days),Dependencies,Error\r\n" + '"@scope/a,b",1.0,2.0,1,"say ""no""\nnow"\r\n',
  );
});

test("the CSV file name holds the ecosystem, the packages, and the window", () => {
  assert.equal(
    csvFileName(threePackageJob().comparison),
    "mttu-mttr_pypi_fast_missing_slow_2020-01-01_2023-01-01.csv",
  );
  assert.equal(
    csvFileName({ ...threePackageJob().comparison, ecosystem: "npm", packages: ["@types/node"] }),
    "mttu-mttr_npm_types-node_2020-01-01_2023-01-01.csv",
  );
});

test("days show with one decimal", () => {
  assert.equal(formatDays(0), "0.0");
  assert.equal(formatDays(12.345), "12.3");
  assert.equal(formatDays(null), "–");
});

test("the form offers the four weighting types of the Tool", () => {
  assert.deepEqual(WEIGHTING_TYPES, ["disable", "linear", "exponential", "inverse"]);
});

test("only exponential weighting needs the half-life field", () => {
  assert.equal(needsHalfLife("exponential"), true);
  for (const type of ["disable", "linear", "inverse"]) assert.equal(needsHalfLife(type), false);
});

test("exponential weighting accepts a half-life from 1 to 3650 days", () => {
  for (const value of ["1", "180", "3650", "90.5"]) {
    assert.equal(halfLifeError("exponential", value), null, value);
  }
});

test("exponential weighting refuses a missing or out-of-range half-life", () => {
  for (const value of ["", "  ", "0", "0.5", "3651", "-1", "abc", undefined]) {
    assert.equal(
      halfLifeError("exponential", value),
      "Enter a half-life from 1 to 3650 days.",
      String(value),
    );
  }
});

test("the other weighting types ignore the half-life field", () => {
  assert.equal(halfLifeError("linear", "0"), null);
  assert.equal(halfLifeError("disable", ""), null);
});

test("the request body holds the weighting type and the half-life", () => {
  const form = {
    ecosystem: "pypi",
    packages: ["mypkg"],
    startDate: "2020-01-01",
    endDate: "2023-01-01",
    weightingType: "exponential",
    halfLife: "180",
  };

  const body = requestFromForm(form, "example-token");

  assert.equal(body.weighting_type, "exponential");
  assert.equal(body.half_life, 180);
});

test("the request body has no half-life for the other weighting types", () => {
  const form = {
    ecosystem: "pypi",
    packages: ["mypkg"],
    startDate: "2020-01-01",
    endDate: "2023-01-01",
    weightingType: "linear",
    halfLife: "180",
  };

  const body = requestFromForm(form, "example-token");

  assert.equal(body.weighting_type, "linear");
  assert.equal(body.half_life, null);
});

test("the axis label says a sum without weighting and a weighted mean with weighting", () => {
  assert.equal(axisLabel("disable"), "Days (sum over intervals)");
  for (const type of ["linear", "exponential", "inverse"]) {
    assert.equal(axisLabel(type), "Days (weighted mean)");
  }
});

test("the note near the results says what the numbers are", () => {
  assert.match(resultsNote("disable"), /sum/);
  assert.doesNotMatch(resultsNote("disable"), /weighted mean/);
  assert.match(resultsNote("exponential"), /weighted mean/);
  assert.notEqual(resultsNote("disable"), resultsNote("linear"));
});
