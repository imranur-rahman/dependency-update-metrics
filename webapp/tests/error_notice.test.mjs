// Tests for the error notices of the Webapp (webapp/site/static/js/errorNotice.js): the
// title, the plain message, and the next step of each error kind. They read the shared API
// contract examples in contract/examples/, so a change to a Host message fails here.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  refusalNotice,
  connectionErrorNotice,
  sampleErrorNotice,
  jobErrorNotice,
  packageErrorNotice,
  doneJobNotice,
  errorCell,
} from "../site/static/js/errorNotice.js";
import { NO_DEPENDENCIES_ERROR, resultRows } from "../site/static/js/comparison.js";

const examples = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "contract", "examples");
const example = (name) => JSON.parse(readFileSync(join(examples, name), "utf8"));

test("a refused submit at the rate limit tells the user how long to wait", () => {
  assert.deepEqual(refusalNotice(example("submit_rate_limited.json")), {
    title: "Too many requests",
    message: "Too many requests, try again in 40 min",
    nextStep: "Wait 40 min, then click Compute again.",
  });
});

test("a refused submit while the server is busy asks the user to try again in a minute", () => {
  assert.deepEqual(refusalNotice(example("submit_server_busy.json")), {
    title: "The server is busy",
    message: "The server is busy, try later",
    nextStep: "Try again in a minute.",
  });
});

test("a failed bot check asks the user to reload the page", () => {
  assert.deepEqual(refusalNotice({ detail: "The bot check failed" }), {
    title: "The bot check failed",
    message: "The bot check failed",
    nextStep: "Reload the page, wait for the bot check, then click Compute again.",
  });
});

test("a refused submit without a user message shows a generic notice", () => {
  for (const body of [null, {}, { detail: [{ loc: ["body"], msg: "internal" }] }]) {
    assert.deepEqual(refusalNotice(body), {
      title: "Something went wrong",
      message: "Something went wrong. Try again later.",
      nextStep: "Try again in a few minutes.",
    });
  }
});

test("a Job that the Host does not know asks the user to compute again", () => {
  assert.deepEqual(refusalNotice({ detail: "Job not found" }), {
    title: "Job not found",
    message: "Job not found",
    nextStep: "The server keeps a Job for 30 days. Click Compute to run the Comparison again.",
  });
});

test("a lost connection to the Host asks the user to check the connection", () => {
  assert.deepEqual(connectionErrorNotice(), {
    title: "No connection to the server",
    message: "The page cannot reach the server.",
    nextStep: "Check your internet connection, then try again in a minute.",
  });
});

test("a Sample Comparison that cannot load asks the user to reload, not to check the server", () => {
  assert.deepEqual(sampleErrorNotice(), {
    title: "The sample could not load",
    message: "The sample could not load. Reload the page.",
  });
});

test("a failed Job shows the message of the Host with its reference", () => {
  assert.deepEqual(jobErrorNotice(example("job_error.json")), {
    title: "The analysis failed",
    message: "Something went wrong (ref: example-job-id)",
    nextStep: "Try again in a few minutes. If it fails again, report the reference.",
  });
});

test("a Job that took too long asks for a smaller Comparison", () => {
  const job = { ...example("job_error.json"), message: "The analysis took too long" };
  assert.deepEqual(jobErrorNotice(job), {
    title: "The analysis took too long",
    message: "The analysis took too long",
    nextStep: "Try fewer packages or a shorter time window.",
  });
});

test("a package error maps to a next step for that package", () => {
  assert.deepEqual(packageErrorNotice("Package not found in deps.dev"), {
    title: "Package not found",
    message: "Package not found in deps.dev",
    nextStep: "Check the package name and the ecosystem.",
  });
  assert.deepEqual(packageErrorNotice("No releases in this window"), {
    title: "No releases in this window",
    message: "No releases in this window",
    nextStep: "Choose a longer or an earlier time window.",
  });
});

test("a package without an error or without dependencies gets no error notice", () => {
  assert.equal(packageErrorNotice(""), null);
  assert.equal(packageErrorNotice(undefined), null);
  assert.equal(packageErrorNotice(NO_DEPENDENCIES_ERROR), null);
});

test("an unknown package error keeps its text and gets a generic next step", () => {
  assert.deepEqual(packageErrorNotice("Some new message"), {
    title: "Something went wrong",
    message: "Some new message",
    nextStep: "Try again in a few minutes.",
  });
});

test("a done Job with a failed package gets a notice that names the package", () => {
  assert.deepEqual(doneJobNotice(example("job_done_with_package_error.json")), {
    title: "1 of 2 packages failed",
    message: "nope: Package not found in deps.dev",
    nextStep: "Check the package name and the ecosystem. The table shows the error next to the package.",
  });
});

test("a done Job without a failed package gets no notice", () => {
  assert.equal(doneJobNotice(example("job_done_window_summary.json")), null);
});

test("a done Per-release Job with one failed release gets no Job notice", () => {
  // The failed release shows in its table row; the package itself has results.
  assert.equal(doneJobNotice(example("job_done_per_release.json")), null);
});

test("a done Per-release Job whose package failed gets a notice", () => {
  const job = example("job_done_per_release.json");
  job.results[0] = { ...job.results[0], releases: [], error: "No releases in this window" };
  const notice = doneJobNotice(job);
  assert.equal(notice.title, `1 of ${job.results.length} packages failed`);
  assert.equal(notice.message, `${job.results[0].package}: No releases in this window`);
});

test("each error row of the result table gets the next step of its error", () => {
  const rows = resultRows(example("job_done_with_package_error.json"));
  assert.deepEqual(rows.map((row) => errorCell(row.error)), [
    null,
    { text: "Package not found in deps.dev", nextStep: "Check the package name and the ecosystem." },
  ]);
});
