// The Webapp logic module: the form state of a Comparison and the conversion of Host
// responses into what the page shows. It has no DOM code, so Node's test runner tests it
// (webapp/tests/comparison.test.mjs). The page code (webapp.js) calls it.
//
// The request and response shapes are the Host API contract; contract/examples/ holds
// example bodies.

const NO_VALUE = "–";

// The weighting types of the Tool (--weighting-type). "disable" is the default.
export const WEIGHTING_TYPES = ["disable", "linear", "exponential", "inverse"];
const NO_WEIGHTING = "disable";
const EXPONENTIAL = "exponential";
// Exponential weighting needs a half-life in this range of days. The Host checks the same rule.
const MIN_HALF_LIFE_DAYS = 1;
const MAX_HALF_LIFE_DAYS = 3650;

// Return the body of POST /api/jobs for the form state and the Turnstile token.
// form: { ecosystem, package, startDate, endDate, weightingType, halfLife } with dates as
// "YYYY-MM-DD" and halfLife as the text of the field. Without weightingType, the body has
// no weighting. The body has a half-life only for exponential weighting.
export function requestFromForm(form, turnstileToken) {
  const weightingType = form.weightingType || NO_WEIGHTING;
  return {
    ecosystem: form.ecosystem,
    packages: [form.package.trim()],
    start_date: form.startDate,
    end_date: form.endDate,
    mode: "window_summary",
    weighting_type: weightingType,
    half_life: needsHalfLife(weightingType) ? Number(form.halfLife) : null,
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

// Return true when the weighting type needs a half-life (the form shows the field only then).
export function needsHalfLife(weightingType) {
  return weightingType === EXPONENTIAL;
}

// Return the error text for the half-life field, or null when the value is valid.
// halfLife is the text of the field. Only exponential weighting checks it.
export function halfLifeError(weightingType, halfLife) {
  if (!needsHalfLife(weightingType)) return null;
  const text = String(halfLife ?? "").trim();
  const days = Number(text);
  if (text === "" || !Number.isFinite(days) || days < MIN_HALF_LIFE_DAYS || days > MAX_HALF_LIFE_DAYS) {
    return `Enter a half-life from ${MIN_HALF_LIFE_DAYS} to ${MAX_HALF_LIFE_DAYS} days.`;
  }
  return null;
}

// Return the y-axis label of the charts for the weighting type. Without weighting, the Tool
// gives a sum of days; with weighting, it gives a weighted mean of days.
export function axisLabel(weightingType) {
  return weightingType === NO_WEIGHTING ? "Days (sum over intervals)" : "Days (weighted mean)";
}

// Return the one-line note near the results that says what the numbers are.
export function resultsNote(weightingType) {
  if (weightingType === NO_WEIGHTING) {
    return "Without weighting, MTTU and MTTR are the sum of the days over all intervals.";
  }
  return (
    `With ${weightingType} weighting, MTTU and MTTR are a weighted mean of days. ` +
    "Do not compare them with values that have no weighting."
  );
}
