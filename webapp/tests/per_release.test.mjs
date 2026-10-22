// Tests for the Per-release parts of the Webapp logic module (comparison.js): the mode
// field, the two line charts with gaps, and the Per-release table and CSV.
// The Job comes from contract/examples/job_done_per_release.json, the real Tool output
// that the Host tests also read: mypkg release 1.1.0 failed.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  requestFromForm,
  formFromComparison,
  comparisonFromSearch,
  withComparison,
  lineChartData,
  lineChartAxisLabel,
  resultRows,
  tableColumns,
  rowsToCsv,
  TABLE_COLUMNS,
  MODES,
  chartHasValues,
  NO_DEPENDENCIES_ERROR,
} from "../site/static/js/comparison.js";

const contractDir = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "contract", "examples");
const example = (name) => JSON.parse(readFileSync(join(contractDir, `${name}.json`), "utf8"));
const perReleaseJob = () => example("job_done_per_release");
const day = (text) => Date.parse(`${text}T00:00:00Z`);

test("the form offers the two modes, Window summary first", () => {
  assert.deepEqual(MODES, ["window_summary", "per_release"]);
});

test("the request body holds the mode of the form", () => {
  const form = { ecosystem: "pypi", packages: ["mypkg"], startDate: "2020-01-01", endDate: "2023-01-01" };

  assert.equal(requestFromForm({ ...form, mode: "per_release" }, "t").mode, "per_release");
  assert.equal(requestFromForm(form, "t").mode, "window_summary");
});

test("the Per-release mode survives the round trip through the URL and the form", () => {
  const { comparison } = perReleaseJob();

  const fromUrl = comparisonFromSearch(withComparison("", comparison));
  const body = requestFromForm(formFromComparison(fromUrl), "t");

  assert.equal(fromUrl.mode, "per_release");
  assert.equal(body.mode, "per_release");
});

test("the MTTU line chart has one line for each package, with a gap at the failed release", () => {
  assert.deepEqual(lineChartData(perReleaseJob(), "mttu"), {
    datasets: [
      {
        label: "mypkg",
        data: [
          { x: day("2021-01-01"), y: 0, version: "1.0.0" },
          { x: day("2021-09-01"), y: null, version: "1.1.0" },
          { x: day("2022-01-01"), y: 214, version: "1.2.0" },
        ],
      },
      {
        label: "yourpkg",
        data: [
          { x: day("2021-03-01"), y: 0, version: "1.0.0" },
          { x: day("2021-12-01"), y: 183, version: "2.0.0" },
        ],
      },
    ],
  });
});

test("the MTTR line chart reads the MTTR of each release", () => {
  const [mypkg] = lineChartData(perReleaseJob(), "mttr").datasets;

  assert.deepEqual(
    mypkg.data.map((point) => point.y),
    [0, null, 214],
  );
});

test("a release with a status that is not ok is a gap, also when it has a value", () => {
  const job = perReleaseJob();
  Object.assign(job.results[1].releases[0], { status: "no_deps", mttu: 0 });

  const [, yourpkg] = lineChartData(job, "mttu").datasets;

  assert.deepEqual(
    yourpkg.data.map((point) => point.y),
    [null, 183],
  );
});

test("a package that failed has a line with no points", () => {
  const job = perReleaseJob();
  Object.assign(job.results[1], { releases: [], error: "Package not found in deps.dev" });

  const [, yourpkg] = lineChartData(job, "mttu").datasets;

  assert.deepEqual(yourpkg, { label: "yourpkg", data: [] });
});

test("the line chart axis label follows the weighting type of the Job", () => {
  const job = perReleaseJob();
  assert.equal(lineChartAxisLabel(job), "Days (sum over intervals)");

  job.comparison.weighting_type = "inverse";
  assert.equal(lineChartAxisLabel(job), "Days (weighted mean)");
});

test("a Per-release Job gives one table row for each release, with the error of the failed one", () => {
  assert.deepEqual(resultRows(perReleaseJob()), [
    { package: "mypkg", version: "1.0.0", releaseDate: "2021-01-01", mttu: "0.0", mttr: "0.0", dependencies: 1, error: "" },
    {
      package: "mypkg",
      version: "1.1.0",
      releaseDate: "2021-09-01",
      mttu: "–",
      mttr: "–",
      dependencies: 0,
      error: "Something went wrong (ref: example-job-id)",
    },
    { package: "mypkg", version: "1.2.0", releaseDate: "2022-01-01", mttu: "214.0", mttr: "214.0", dependencies: 1, error: "" },
    { package: "yourpkg", version: "1.0.0", releaseDate: "2021-03-01", mttu: "0.0", mttr: "0.0", dependencies: 1, error: "" },
    { package: "yourpkg", version: "2.0.0", releaseDate: "2021-12-01", mttu: "183.0", mttr: "183.0", dependencies: 1, error: "" },
  ]);
});

test("a Per-release package that failed gives one row with its error", () => {
  const job = perReleaseJob();
  Object.assign(job.results[1], { releases: [], error: "Package not found in deps.dev" });

  assert.deepEqual(resultRows(job).at(-1), {
    package: "yourpkg",
    version: "–",
    releaseDate: "–",
    mttu: "–",
    mttr: "–",
    dependencies: 0,
    error: "Package not found in deps.dev",
  });
});

test("the Per-release table has a version and a release date column", () => {
  assert.deepEqual(tableColumns("per_release"), [
    ["package", "Package"],
    ["version", "Version"],
    ["releaseDate", "Release date"],
    ["mttu", "MTTU (days)"],
    ["mttr", "MTTR (days)"],
    ["dependencies", "Dependencies"],
    ["error", "Error"],
  ]);
  assert.deepEqual(tableColumns("window_summary"), TABLE_COLUMNS);
});

test("the Per-release CSV download has the same columns and values as the table", () => {
  const job = perReleaseJob();

  assert.equal(
    rowsToCsv(resultRows(job), tableColumns(job.comparison.mode)),
    "Package,Version,Release date,MTTU (days),MTTR (days),Dependencies,Error\r\n" +
      "mypkg,1.0.0,2021-01-01,0.0,0.0,1,\r\n" +
      "mypkg,1.1.0,2021-09-01,,,0,Something went wrong (ref: example-job-id)\r\n" +
      "mypkg,1.2.0,2022-01-01,214.0,214.0,1,\r\n" +
      "yourpkg,1.0.0,2021-03-01,0.0,0.0,1,\r\n" +
      "yourpkg,2.0.0,2021-12-01,183.0,183.0,1,\r\n",
  );
});

test("a release without dependencies is a gap and shows an error, also with the status ok", () => {
  const job = perReleaseJob();
  Object.assign(job.results[0].releases[0], { status: "ok", mttu: 0, mttr: 0, num_dependencies: 0 });

  const [mypkg] = lineChartData(job, "mttu").datasets;
  const [row] = resultRows(job);

  assert.deepEqual(mypkg.data.map((point) => point.y), [null, null, 214]);
  assert.deepEqual([row.mttu, row.mttr, row.error], ["–", "–", NO_DEPENDENCIES_ERROR]);
});

test("line charts in which no release has a value have no values", () => {
  const job = perReleaseJob();
  for (const result of job.results) {
    for (const release of result.releases) Object.assign(release, { status: "no_deps", num_dependencies: 0 });
  }

  assert.equal(chartHasValues(lineChartData(job, "mttu")), false);
  assert.equal(chartHasValues(lineChartData(perReleaseJob(), "mttu")), true);
});
