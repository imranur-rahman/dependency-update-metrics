// The error notices of the Webapp: for each error kind, a short title, the plain message,
// and the next step for the user. It has no DOM code, so Node's test runner tests it
// (webapp/tests/error_notice.test.mjs). The page code (webapp.js) shows each notice in an
// in-page notification.
//
// The Host gives one of six user messages (host/dmhost/errors.py). This module reads the
// kind of the error from the start of the message, and keeps the message as it is, so
// that the user sees the reference of a failed Job.

import { submitErrorMessage, NO_DEPENDENCIES_ERROR } from "./comparison.js";

// A notice is { title, message, nextStep }: three short texts that the page shows in a red
// notification, or next to a package in the result table.

const GENERIC_TITLE = "Something went wrong";
const GENERIC_NEXT_STEP = "Try again in a few minutes.";

// One rule for each Host message: the start of the message, the title, and the next step.
// nextStep is a text, or a function of the match for a step that repeats a value of the
// message (the minutes of the rate limit).
const RULES = [
  {
    pattern: /^Too many requests, try again in (\d+) min/,
    title: "Too many requests",
    nextStep: (match) => `Wait ${match[1]} min, then click Compute again.`,
  },
  { pattern: /^The server is busy/, title: "The server is busy", nextStep: "Try again in a minute." },
  {
    pattern: /^The bot check failed/,
    title: "The bot check failed",
    nextStep: "Reload the page, wait for the bot check, then click Compute again.",
  },
  {
    // GET /api/jobs/{id} for a job ID in the page URL that the Host deleted or never had.
    pattern: /^Job not found/,
    title: "Job not found",
    nextStep: "The server keeps a Job for 30 days. Click Compute to run the Comparison again.",
  },
  {
    pattern: /^Package not found/,
    title: "Package not found",
    nextStep: "Check the package name and the ecosystem.",
  },
  {
    pattern: /^No releases in this window/,
    title: "No releases in this window",
    nextStep: "Choose a longer or an earlier time window.",
  },
  {
    pattern: /^The analysis took too long/,
    title: "The analysis took too long",
    nextStep: "Try fewer packages or a shorter time window.",
  },
  {
    // The Host message of every other failure holds a reference (the job ID) for its log.
    pattern: /^Something went wrong \(ref: /,
    title: GENERIC_TITLE,
    nextStep: `${GENERIC_NEXT_STEP} If it fails again, report the reference.`,
  },
];

// Return the notice for a Host message. A message that no rule knows keeps its text and
// gets a generic title and next step.
function noticeForMessage(message) {
  for (const rule of RULES) {
    const match = message.match(rule.pattern);
    if (match === null) continue;
    const nextStep = typeof rule.nextStep === "function" ? rule.nextStep(match) : rule.nextStep;
    return { title: rule.title, message, nextStep };
  }
  return { title: GENERIC_TITLE, message, nextStep: GENERIC_NEXT_STEP };
}

// Return the notice for a refused request to the Host: POST /api/jobs, or GET of a Job.
// body is the JSON body of the response, or null. The Host gives a user message for the
// rate limits, the busy queue, the bot check, and an unknown Job; other bodies get the
// generic message of submitErrorMessage.
export function refusalNotice(body) {
  return noticeForMessage(submitErrorMessage(body));
}

// Return the notice for a request to the Host that got no answer (for example, the network
// is down or the event stream closed).
export function connectionErrorNotice() {
  return {
    title: "No connection to the server",
    message: "The page cannot reach the server.",
    nextStep: "Check your internet connection, then try again in a minute.",
  };
}

// Return the notice for a Sample Comparison file that could not load. The sample makes no
// request to the Host, so the notice does not name the server.
export function sampleErrorNotice() {
  return { title: "The sample could not load", message: "The sample could not load. Reload the page." };
}

// Return the notice for a failed Job (state "error"). job.message is a Host user message.
export function jobErrorNotice(job) {
  const notice = noticeForMessage(job.message || "");
  return notice.title === GENERIC_TITLE ? { ...notice, title: "The analysis failed" } : notice;
}

// Return the notice for the error text of a package or a release of a done Job, or null
// when it has no error. A package without dependencies is not a failure: it keeps its own
// text (NO_DEPENDENCIES_ERROR) and gets no error notice.
export function packageErrorNotice(error) {
  if (!error || error === NO_DEPENDENCIES_ERROR) return null;
  return noticeForMessage(error);
}

// Return what the error cell of a result table row (resultRows in comparison.js) shows:
// { text, nextStep } for an error, or null for a row without an error.
export function errorCell(error) {
  const notice = packageErrorNotice(error);
  return notice === null ? null : { text: notice.message, nextStep: notice.nextStep };
}

// Return the notice for a done Job in which at least one package failed, or null. The
// notice names each failed package; the results of the other packages stay. A failed
// release of a package with other results shows only in its table row.
export function doneJobNotice(job) {
  const failed = job.results
    .map((result) => ({ result, notice: packageErrorNotice(result.error || result.summary?.error) }))
    .filter(({ notice }) => notice !== null);
  if (failed.length === 0) return null;
  const total = job.results.length;
  const nextSteps = [...new Set(failed.map(({ notice }) => notice.nextStep))];
  const where = failed.length === 1 ? "next to the package" : "next to each package";
  return {
    title: `${failed.length} of ${total} ${total === 1 ? "package" : "packages"} failed`,
    message: failed.map(({ result, notice }) => `${result.package}: ${notice.message}`).join("; "),
    nextStep: [...nextSteps, `The table shows the error ${where}.`].join(" "),
  };
}
