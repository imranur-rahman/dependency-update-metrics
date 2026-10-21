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
    package: "  mypkg ",
    startDate: "2020-01-01",
    endDate: "2023-01-01",
  };

  assert.deepEqual(requestFromForm(form, "example-token"), example("create_job_request"));
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
    package: "mypkg",
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
    package: "mypkg",
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
