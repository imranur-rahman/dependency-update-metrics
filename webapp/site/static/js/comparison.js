// The Webapp logic module: the form state of a Comparison and the conversion of Host
// responses into what the page shows. It has no DOM code, so Node's test runner tests it
// (webapp/tests/comparison.test.mjs). The page code (webapp.js) calls it.
//
// The request and response shapes are the Host API contract; contract/examples/ holds
// example bodies.

const NO_VALUE = "–";

// The most packages in one Comparison. The Host checks the same limit (MAX_PACKAGES).
export const MAX_PACKAGES = 5;

// Return the package names of the form, without spaces and without empty fields.
export function cleanPackages(packages) {
  return packages.map((name) => name.trim()).filter((name) => name !== "");
}

// Return the form of a package name that two names of one package share. Registry
// names do not depend on case, and PyPI treats runs of "-", "_" and "." as one "-"
// (PEP 503). The Host uses the same rule (package_key in host/dmhost/models.py).
export function packageKey(ecosystem, name) {
  const key = name.trim().toLowerCase();
  return ecosystem === "pypi" ? key.replace(/[-_.]+/g, "-") : key;
}

// Return the messages for the package fields of the form, or [] when they are correct.
export function packageErrors(ecosystem, packages) {
  const names = cleanPackages(packages);
  if (names.length === 0) return ["Enter at least one package."];
  if (names.length > MAX_PACKAGES) return [`Enter at most ${MAX_PACKAGES} packages.`];
  const seen = new Set();
  const errors = [];
  for (const name of names) {
    const key = packageKey(ecosystem, name);
    if (seen.has(key)) errors.push(`${name} is in the Comparison twice.`);
    seen.add(key);
  }
  return errors;
}

// Return true when the form can show one more package field.
export function canAddPackage(packages) {
  return packages.length < MAX_PACKAGES;
}

// The weighting types of the Tool (--weighting-type). "disable" is the default.
export const WEIGHTING_TYPES = ["disable", "linear", "exponential", "inverse"];
const NO_WEIGHTING = "disable";
const EXPONENTIAL = "exponential";
// Exponential weighting needs a half-life in this range of days. The Host checks the same rule.
const MIN_HALF_LIFE_DAYS = 1;
const MAX_HALF_LIFE_DAYS = 3650;

// Return the body of POST /api/jobs for the form state and the Turnstile token.
// form: { ecosystem, packages, startDate, endDate, weightingType, halfLife } with dates as
// "YYYY-MM-DD" and halfLife as the text of the field. Without weightingType, the body has
// no weighting. The body has a half-life only for exponential weighting.
export function requestFromForm(form, turnstileToken) {
  const weightingType = form.weightingType || NO_WEIGHTING;
  return {
    ecosystem: form.ecosystem,
    packages: cleanPackages(form.packages),
    start_date: form.startDate,
    end_date: form.endDate,
    mode: "window_summary",
    weighting_type: weightingType,
    half_life: needsHalfLife(weightingType) ? Number(form.halfLife) : null,
    turnstile_token: turnstileToken,
  };
}

// The URL query parameters that hold a Comparison. "package" repeats once per package.
const URL_KEYS = ["ecosystem", "package", "start_date", "end_date", "mode", "weighting_type", "half_life"];

// Return the query string `search` with the Comparison in it, so that a user can share
// the link. Parameters that are not part of the Comparison (for example a job ID) stay.
// comparison: the "comparison" object of the Host API (contract/examples/).
export function withComparison(search, comparison) {
  const params = new URLSearchParams(search);
  for (const key of URL_KEYS) params.delete(key);
  params.set("ecosystem", comparison.ecosystem);
  for (const name of comparison.packages) params.append("package", name);
  params.set("start_date", comparison.start_date);
  params.set("end_date", comparison.end_date);
  params.set("mode", comparison.mode);
  params.set("weighting_type", comparison.weighting_type);
  if (comparison.half_life !== null && comparison.half_life !== undefined) {
    params.set("half_life", String(comparison.half_life));
  }
  return `?${params}`;
}

// Return the Comparison in the query string `search`, or null when it has no package.
export function comparisonFromSearch(search) {
  const params = new URLSearchParams(search);
  const packages = params.getAll("package");
  if (packages.length === 0) return null;
  const halfLife = params.get("half_life");
  return {
    ecosystem: params.get("ecosystem") ?? "npm",
    packages,
    start_date: params.get("start_date") ?? "",
    end_date: params.get("end_date") ?? "",
    mode: params.get("mode") ?? "window_summary",
    weighting_type: params.get("weighting_type") ?? "disable",
    half_life: halfLife === null || halfLife === "" ? null : Number(halfLife),
  };
}

// Return the form state for a Comparison (the reverse of requestFromForm), so that a
// shared link fills the form. halfLife is the text of the field, as in requestFromForm.
export function formFromComparison(comparison) {
  return {
    ecosystem: comparison.ecosystem,
    packages: [...comparison.packages],
    startDate: comparison.start_date,
    endDate: comparison.end_date,
    mode: comparison.mode,
    weightingType: comparison.weighting_type,
    halfLife: comparison.half_life === null ? "" : String(comparison.half_life),
  };
}

// Return the one-line status text of a Job (a body of GET /api/jobs/{id}).
export function describeJob(job) {
  switch (job.state) {
    case "queued":
      return `Queued (position ${job.position})`;
    case "running":
      return `Running (package ${job.package_index} of ${job.package_count})`;
    case "done":
      return "Done";
    case "error":
      return job.message;
    case "cancelled":
      return "Cancelled";
    default:
      return job.state;
  }
}

// Return true when the Job is in a final state and its state cannot change.
export function isFinished(job) {
  return FINAL_STATES.includes(job.state);
}

const FINAL_STATES = ["done", "error", "cancelled"];

// Return true while the user can cancel the Job: it waits or runs.
export function canCancel(job) {
  return job.state === "queued" || job.state === "running";
}

// Return the request that cancels a Job (DELETE /api/jobs/{id}). The event stream of the
// Job then sends "cancelled".
export function cancelRequest(hostUrl, jobId) {
  return { url: `${hostUrl}/api/jobs/${encodeURIComponent(jobId)}`, method: "DELETE" };
}

// The names of the server-sent events of GET /api/jobs/{id}/events: one for each Job state.
// Each event holds the same body as GET /api/jobs/{id}.
export const JOB_EVENTS = ["queued", "running", ...FINAL_STATES];

// Return the progress bar of a Job: { value, max } with the number of packages that the
// Host finished, or { value: null, max: null } (a bar with no value) while the Job waits.
// Return null for a finished Job, which shows no bar.
export function progressOf(job) {
  if (job.state === "queued") return { value: null, max: null };
  if (job.state === "running") return { value: job.package_index - 1, max: job.package_count };
  return null;
}

// The query parameter of the page URL that holds the job ID, so that a reload finds the Job.
const JOB_PARAM = "job";

// Return the job ID in a page URL, or null if the URL has none.
export function jobIdFromUrl(href) {
  return new URL(href).searchParams.get(JOB_PARAM) || null;
}

// Return the page URL with the job ID in it. The other query parameters and the hash stay.
export function urlWithJobId(href, jobId) {
  const url = new URL(href);
  url.searchParams.set(JOB_PARAM, jobId);
  return url.toString();
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

// Return the Chart.js data of the grouped bar chart of a done Window summary Job: one
// group for each package, with an MTTU bar and an MTTR bar. A package without a value
// (for example, a package that failed) has null, so that it shows no bar and not a 0.
// The page code adds the colors.
export function barChartData(job) {
  const value = (result, key) => {
    const days = result.summary?.[key];
    return typeof days === "number" ? days : null;
  };
  return {
    labels: job.results.map((result) => result.package),
    datasets: [
      { label: "MTTU", data: job.results.map((result) => value(result, "mttu")) },
      { label: "MTTR", data: job.results.map((result) => value(result, "mttr")) },
    ],
  };
}

// Return the y-axis label of the bar chart of a done Job. It follows the weighting type,
// because a sum of days and a weighted mean are different quantities (axisLabel).
export function barChartAxisLabel(job) {
  return axisLabel(job.comparison.weighting_type);
}

// The columns of the result table and of the CSV download, in order: [row key, header].
// The page code builds the table from this list, so the table and the CSV always match.
export const TABLE_COLUMNS = [
  ["package", "Package"],
  ["mttu", "MTTU (days)"],
  ["mttr", "MTTR (days)"],
  ["dependencies", "Dependencies"],
  ["error", "Error"],
];

// Return the CSV text of the table rows (resultRows). A cell that shows a dash in the
// table (no value) is empty in the CSV, so that spreadsheet tools read it as missing.
export function rowsToCsv(rows) {
  const cell = (value) => {
    const text = value === NO_VALUE ? "" : String(value);
    return /[",\r\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
  };
  const lines = [TABLE_COLUMNS.map(([, header]) => cell(header))];
  for (const row of rows) lines.push(TABLE_COLUMNS.map(([key]) => cell(row[key])));
  return lines.map((line) => `${line.join(",")}\r\n`).join("");
}

// Return the file name of the CSV download of a Comparison.
export function csvFileName(comparison) {
  const safe = (text) => text.replace(/^@/, "").replace(/[^A-Za-z0-9._-]+/g, "-");
  const parts = [comparison.ecosystem, ...comparison.packages, comparison.start_date, comparison.end_date];
  return `mttu-mttr_${parts.map(safe).join("_")}.csv`;
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
