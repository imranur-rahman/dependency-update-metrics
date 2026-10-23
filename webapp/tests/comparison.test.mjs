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
  canCancel,
  cancelRequest,
  resultRows,
  formatDays,
  hasValue,
  progressOf,
  jobIdFromUrl,
  urlWithJobId,
  JOB_EVENTS,
  packageErrors,
  dateErrors,
  submitErrorMessage,
  canAddPackage,
  packageLimitNote,
  packageLimitNoteAction,
  withComparison,
  comparisonFromSearch,
  formFromComparison,
  barChartData,
  barChartAxisLabel,
  barChartOptions,
  CHART_SIZE,
  chartBoxStyle,
  lineChartOptions,
  rowsToCsv,
  csvFileName,
  WEIGHTING_TYPES,
  needsHalfLife,
  halfLifeError,
  axisLabel,
  resultsNote,
  botCheckError,
  packageKey,
  packageNameMatchesRules,
  chartHasValues,
  NO_DEPENDENCIES_ERROR,
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

test("the form tells the user when the Comparison holds 5 packages", () => {
  assert.equal(packageLimitNote(["a", "b", "c", "d"]), "");
  assert.equal(packageLimitNote(["a", "b", "c", "d", "e"]), "A Comparison holds at most 5 packages.");
});

test("the package limit note shows only when the count changes to 5", () => {
  const five = ["a", "b", "c", "d", "e"];
  assert.equal(packageLimitNoteAction(4, five), "show");
  assert.equal(packageLimitNoteAction(null, five), "show");
  // A renumber at the same count (for example, after the form is filled) shows it not again.
  assert.equal(packageLimitNoteAction(5, five), "keep");
  assert.equal(packageLimitNoteAction(5, ["a", "b", "c", "d"]), "clear");
  assert.equal(packageLimitNoteAction(null, ["a"]), "clear");
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
  const job = example("job_cancelled");

  assert.equal(describeJob(job), "Cancelled");
  assert.equal(isFinished(job), true);
  assert.equal(progressOf(job), null);
});

test("the user can cancel a Job only while it waits or runs", () => {
  assert.equal(canCancel(example("job_queued")), true);
  assert.equal(canCancel(example("job_running")), true);
  for (const name of ["job_done_window_summary", "job_error", "job_cancelled"]) {
    assert.equal(canCancel(example(name)), false, name);
  }
});

test("the cancel request is a DELETE of the Job", () => {
  assert.deepEqual(cancelRequest("http://localhost:8080", "a b"), {
    url: "http://localhost:8080/api/jobs/a%20b",
    method: "DELETE",
  });
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
    { package: "mypkg", mttu: "579.00", mttr: "579.00", dependencies: 1, error: "" },
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

test("every chart has one shared size: at most 720 px wide, and one height", () => {
  assert.equal(CHART_SIZE.maxWidth, 720);
  assert.ok(CHART_SIZE.height >= 180 && CHART_SIZE.height <= 240, `the charts are ${CHART_SIZE.height} px high`);
});

test("the chart box style centers each chart in one column with the shared size", () => {
  assert.deepEqual(chartBoxStyle(), {
    position: "relative",
    width: "100%",
    maxWidth: "720px",
    height: `${CHART_SIZE.height}px`,
    marginLeft: "auto",
    marginRight: "auto",
  });
  // Each call returns a new object, so that a caller can change it without effect on others.
  assert.notEqual(chartBoxStyle(), chartBoxStyle());
});

test("the bar chart and the line charts share thin lines, small points, and a light grid", () => {
  const bar = barChartOptions(threePackageJob());
  const line = lineChartOptions(threePackageJob());

  for (const options of [bar, line]) {
    assert.ok(options.elements.line.borderWidth <= 1.5, "the lines are thin");
    assert.ok(options.elements.point.radius <= 2.5, "the points are small");
    assert.ok(options.elements.point.hoverRadius > options.elements.point.radius);
    assert.match(options.scales.y.grid.color, /^rgba\(128, 128, 128, 0\.1\d*\)$/, "the grid is light");
  }
  assert.deepEqual(bar.elements, line.elements);
  assert.deepEqual(bar.scales.y.grid, line.scales.y.grid);
  assert.ok(bar.datasets.bar.maxBarThickness <= 32, "the bars stay narrow in a wide chart");
});

test("the bar chart is compact: a small legend and few y-axis ticks", () => {
  const options = barChartOptions(threePackageJob());

  assert.equal(options.maintainAspectRatio, false);
  assert.equal(options.plugins.legend.labels.boxWidth, 12);
  assert.ok(options.plugins.legend.labels.font.size >= 11, "the legend text stays readable");
  assert.equal(options.scales.y.ticks.maxTicksLimit, 4);
  assert.equal(options.scales.y.beginAtZero, true);
});

test("the bar chart options carry the axis label of the weighting type", () => {
  const job = threePackageJob();
  assert.deepEqual(barChartOptions(job).scales.y.title, { display: true, text: "Days (sum over intervals)" });

  job.comparison.weighting_type = "linear";
  assert.equal(barChartOptions(job).scales.y.title.text, "Days (weighted mean)");
});

test("the bar chart tooltip shows the days with two decimals, and no value for a missing bar", () => {
  const { label } = barChartOptions(threePackageJob()).plugins.tooltip.callbacks;

  assert.equal(label({ dataset: { label: "MTTU" }, raw: 300.5 }), "MTTU: 300.50 days");
  assert.equal(label({ dataset: { label: "MTTR" }, raw: null }), "MTTR: no value");
});

test("the table shows the values and the error of each package", () => {
  assert.deepEqual(resultRows(threePackageJob()), [
    { package: "fast", mttu: "12.25", mttr: "0.00", dependencies: 4, error: "" },
    { package: "missing", mttu: "–", mttr: "–", dependencies: 0, error: "Package not found in deps.dev" },
    { package: "slow", mttu: "300.50", mttr: "41.00", dependencies: 20, error: "" },
  ]);
});

test("the CSV download has the same columns and values as the table", () => {
  assert.equal(
    rowsToCsv(resultRows(threePackageJob())),
    "Package,MTTU (days),MTTR (days),Dependencies,Error\r\n" +
      "fast,12.25,0.00,4,\r\n" +
      "missing,,,0,Package not found in deps.dev\r\n" +
      "slow,300.50,41.00,20,\r\n",
  );
});

test("the CSV quotes values that hold a comma, a quote, or a line break", () => {
  const rows = [{ package: "@scope/a,b", mttu: "1.0", mttr: "2.0", dependencies: 1, error: 'say "no"\nnow' }];

  assert.equal(
    rowsToCsv(rows),
    "Package,MTTU (days),MTTR (days),Dependencies,Error\r\n" + '"@scope/a,b",1.0,2.0,1,"say ""no""\nnow"\r\n',
  );
});

test("the CSV file name holds the ecosystem, the packages, the window, the mode, and the weighting", () => {
  assert.equal(
    csvFileName(threePackageJob().comparison),
    "mttu-mttr_pypi_fast_missing_slow_2020-01-01_2023-01-01_window_summary_disable.csv",
  );
  assert.equal(
    csvFileName({ ...threePackageJob().comparison, ecosystem: "npm", packages: ["@types/node"] }),
    "mttu-mttr_npm_types-node_2020-01-01_2023-01-01_window_summary_disable.csv",
  );
});

test("two downloads that differ only in the mode or the weighting get different file names", () => {
  const comparison = threePackageJob().comparison;
  const names = [
    comparison,
    { ...comparison, mode: "per_release" },
    { ...comparison, weighting_type: "linear" },
    { ...comparison, weighting_type: "exponential", half_life: 180 },
    { ...comparison, weighting_type: "exponential", half_life: 30 },
  ].map(csvFileName);

  assert.equal(new Set(names).size, names.length);
  assert.equal(
    names[3],
    "mttu-mttr_pypi_fast_missing_slow_2020-01-01_2023-01-01_window_summary_exponential-180.csv",
  );
});

test("days show with two decimals", () => {
  assert.equal(formatDays(0), "0.00");
  assert.equal(formatDays(12.346), "12.35");
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

// The same names as the Host test (host/tests/test_input_checks.py), so both sides agree.
const VALID_NAMES = {
  npm: ["express", "@babel/core", "lodash.merge", "JSONStream"],
  pypi: ["requests", "zope.interface", "Flask_SQLAlchemy", "a"],
  cargo: ["serde", "tokio-util", "winapi_x86"],
};
const INVALID_NAMES = {
  npm: ["has space", ".hidden", "_private", "@scope", "a/b", "x".repeat(215)],
  pypi: ["-requests", "requests.", "re quests", "@babel/core"],
  cargo: ["1serde", "serde.json", "x".repeat(65)],
};

test("the form accepts the package names that match the rules of the ecosystem", () => {
  for (const [ecosystem, names] of Object.entries(VALID_NAMES)) {
    for (const name of names) assert.deepEqual(packageErrors(ecosystem, [name]), [], name);
  }
});

test("the form refuses the package names that break the rules of the ecosystem", () => {
  for (const [ecosystem, names] of Object.entries(INVALID_NAMES)) {
    for (const name of names) assert.equal(packageErrors(ecosystem, [name]).length, 1, name);
  }
  assert.deepEqual(packageErrors("cargo", ["serde.json"]), [
    "serde.json is not a valid Cargo package name.",
  ]);
});

test("the form accepts a window whose end date is after the start date and not in the future", () => {
  assert.deepEqual(dateErrors("2020-01-01", "2020-01-02", "2026-10-21"), []);
  assert.deepEqual(dateErrors("1990-01-01", "2026-10-21", "2026-10-21"), []);
});

test("the form refuses an end date that is not after the start date or is in the future", () => {
  assert.deepEqual(dateErrors("2023-01-01", "2020-01-01", "2026-10-21"), [
    "The end date must be after the start date.",
  ]);
  assert.deepEqual(dateErrors("2023-01-01", "2023-01-01", "2026-10-21"), [
    "The end date must be after the start date.",
  ]);
  assert.deepEqual(dateErrors("2020-01-01", "2026-10-22", "2026-10-21"), [
    "The end date must not be in the future.",
  ]);
  assert.deepEqual(dateErrors("", "2020-01-01", "2026-10-21"), ["Enter a start date and an end date."]);
});

test("a package that failed shows its message in the table, and the others show results", () => {
  const rows = resultRows(example("job_done_with_package_error"));

  assert.deepEqual(rows, [
    { package: "mypkg", mttu: "579.00", mttr: "579.00", dependencies: 1, error: "" },
    { package: "nope", mttu: "–", mttr: "–", dependencies: 0, error: "Package not found in deps.dev" },
  ]);
});

test("a submit that a limit of the Host refuses shows the message of the Host", () => {
  assert.equal(submitErrorMessage(example("submit_rate_limited")), "Too many requests, try again in 40 min");
  assert.equal(submitErrorMessage(example("submit_server_busy")), "The server is busy, try later");
});

test("the form needs a token from the bot check", () => {
  assert.equal(botCheckError(""), "Wait for the bot check to complete, then click Compute.");
  assert.equal(botCheckError(null), "Wait for the bot check to complete, then click Compute.");
  assert.equal(botCheckError("XXXX.DUMMY.TOKEN.XXXX"), null);
});

test("a refused submit shows the message of the Host, and never internal details", () => {
  assert.equal(
    submitErrorMessage({ detail: "Too many requests, try again in 12 min" }),
    "Too many requests, try again in 12 min",
  );
  assert.equal(submitErrorMessage({ detail: "The server is busy, try later" }), "The server is busy, try later");
  // A 422 body of FastAPI holds a list of internal check results.
  const generic = "Something went wrong. Try again later.";
  assert.equal(submitErrorMessage({ detail: [{ loc: ["body"], msg: "Value error" }] }), generic);
  assert.equal(submitErrorMessage(null), generic);
});

// The Host tests (host/tests/test_input_checks.py) read the same cases, so the JavaScript and
// the Python rules cannot drift apart.
test("the package name rules and the package key match the shared contract cases", () => {
  for (const { ecosystem, name, key, valid } of example("package_names").cases) {
    assert.equal(packageNameMatchesRules(ecosystem, name), valid, `${ecosystem}: ${name}`);
    assert.equal(packageKey(ecosystem, name), key, `${ecosystem}: ${name}`);
  }
});

// A Window summary Job in which mypkg has no dependencies: the Tool gives "no_deps" and 0 days.
const noDependenciesJob = () => {
  const job = example("job_done_window_summary");
  Object.assign(job.results[0].summary, { mttu: 0, mttr: 0, num_dependencies: 0, status: "no_deps" });
  return job;
};

test("an entry has a value only with the status ok, dependencies, and both metrics", () => {
  const entry = { status: "ok", num_dependencies: 2, mttu: 1, mttr: 0, error: "" };
  assert.equal(hasValue(entry), true);
  assert.equal(hasValue({ ...entry, num_dependencies: 0 }), false);
  assert.equal(hasValue({ ...entry, status: "no_deps" }), false);
  assert.equal(hasValue({ ...entry, status: "error", mttu: null, mttr: null }), false);
  assert.equal(hasValue({ ...entry, mttr: null }), false);
  assert.equal(hasValue(null), false);
});

test("a package without dependencies shows an error, not 0 days", () => {
  assert.deepEqual(resultRows(noDependenciesJob()), [
    { package: "mypkg", mttu: "–", mttr: "–", dependencies: 0, error: NO_DEPENDENCIES_ERROR },
  ]);
});

test("a package without dependencies has no bar, and a chart without bars has no values", () => {
  const data = barChartData(noDependenciesJob());

  assert.deepEqual(data.datasets.map((dataset) => dataset.data), [[null], [null]]);
  assert.equal(chartHasValues(data), false);
  assert.equal(chartHasValues(barChartData(example("job_done_window_summary"))), true);
});
