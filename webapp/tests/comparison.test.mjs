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
  packageErrors,
  canAddPackage,
  withComparison,
  comparisonFromSearch,
  formFromComparison,
  barChartData,
  rowsToCsv,
  csvFileName,
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
    halfLife: null,
  });
});

test("a queued Job shows its queue position", () => {
  assert.equal(describeJob(example("job_queued")), "Queued (position 1)");
  assert.equal(isFinished(example("job_queued")), false);
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
