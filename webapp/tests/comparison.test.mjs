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

test("days show with one decimal", () => {
  assert.equal(formatDays(0), "0.0");
  assert.equal(formatDays(12.345), "12.3");
  assert.equal(formatDays(null), "–");
});
