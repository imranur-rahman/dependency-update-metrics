// The Webapp logic module: the form state of a Comparison and the conversion of Host
// responses into what the page shows. It has no DOM code, so Node's test runner tests it
// (webapp/tests/comparison.test.mjs). The page code (webapp.js) calls it.
//
// The request and response shapes are the Host API contract; contract/examples/ holds
// example bodies.

const NO_VALUE = "–";

// Return the body of POST /api/jobs for the form state and the Turnstile token.
// form: { ecosystem, package, startDate, endDate } with dates as "YYYY-MM-DD".
export function requestFromForm(form, turnstileToken) {
  return {
    ecosystem: form.ecosystem,
    packages: [form.package.trim()],
    start_date: form.startDate,
    end_date: form.endDate,
    mode: "window_summary",
    weighting_type: "disable",
    half_life: null,
    turnstile_token: turnstileToken,
  };
}

// Return the one-line status text of a Job (a body of GET /api/jobs/{id}).
export function describeJob(job) {
  switch (job.state) {
    case "queued":
      return `Queued (position ${job.position})`;
    case "running":
      return "Running";
    case "done":
      return "Done";
    case "error":
      return job.message;
    default:
      return job.state;
  }
}

// Return true when the Job is in a final state and its state cannot change.
export function isFinished(job) {
  return job.state === "done" || job.state === "error";
}

// Return one table row for each package of a done Window summary Job.
export function resultRows(job) {
  return job.results.map((result) => {
    const summary = result.summary ?? {};
    return {
      package: result.package,
      mttu: formatDays(summary.mttu),
      mttr: formatDays(summary.mttr),
      dependencies: summary.num_dependencies ?? 0,
      error: result.error || summary.error || "",
    };
  });
}

// Return a number of days with one decimal, or a dash when there is no value.
export function formatDays(days) {
  return typeof days === "number" ? days.toFixed(1) : NO_VALUE;
}
