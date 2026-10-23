// The Webapp logic module: the form state of a Comparison and the conversion of Host
// responses into what the page shows. It has no DOM code, so Node's test runner tests it
// (webapp/tests/comparison.test.mjs). The page code (webapp.js) calls it.
//
// The request and response shapes are the Host API contract; contract/examples/ holds
// example bodies.

const NO_VALUE = "–";

// The error text of a package or a release without dependencies. MTTU and MTTR are means
// over the dependencies, so the Tool cannot compute them; its value of 0 is not a result.
export const NO_DEPENDENCIES_ERROR = "No dependencies, so MTTU and MTTR cannot be computed.";

// Return true when a summary or a release of the Host API has no dependencies: the Tool
// gives the status "no_deps", or "ok" with 0 dependencies (a release in Per-release mode).
function hasNoDependencies(entry) {
  return entry?.status === "no_deps" || (entry?.status === "ok" && entry.num_dependencies === 0);
}

// Return true when a summary or a release has a value: the status is "ok", it has
// dependencies, and both metrics are numbers.
export function hasValue(entry) {
  return metricValue(entry, "mttu") !== null && metricValue(entry, "mttr") !== null;
}

// Return the days of a summary or a release for one metric ("mttu" or "mttr"), or null
// when the entry has no value: it failed, or it has no dependencies.
function metricValue(entry, metric) {
  if (entry?.status !== "ok" || hasNoDependencies(entry)) return null;
  return typeof entry[metric] === "number" ? entry[metric] : null;
}

// Return the values of a summary or a release for a table row: the days of both metrics,
// and the error text.
function rowValues(entry, error) {
  if (hasNoDependencies(entry)) {
    return { mttu: NO_VALUE, mttr: NO_VALUE, error: error || NO_DEPENDENCIES_ERROR };
  }
  return { mttu: formatDays(entry?.mttu), mttr: formatDays(entry?.mttr), error };
}

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

const ECOSYSTEM_LABELS = { npm: "npm", pypi: "PyPI", cargo: "Cargo" };

// The naming rules of each ecosystem: a pattern for the whole name and the most characters.
// The Host checks the same rules (PACKAGE_NAME_RULES in host/dmhost/models.py).
// - npm: an optional "@scope/" and URL-safe characters, not "." or "_" first, at most 214.
//   Old names can have capitals (for example "JSONStream").
// - PyPI: letters, digits, ".", "_", and "-", with a letter or a digit at each end (PEP 508).
// - Cargo: a letter, then letters, digits, "_", and "-", at most 64 characters (crates.io).
const PACKAGE_NAME_RULES = {
  npm: { pattern: /^(@[A-Za-z0-9~-][A-Za-z0-9._~-]*\/)?[A-Za-z0-9~-][A-Za-z0-9._~-]*$/, maxLength: 214 },
  pypi: { pattern: /^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$/, maxLength: Infinity },
  cargo: { pattern: /^[A-Za-z][A-Za-z0-9_-]{0,63}$/, maxLength: 64 },
};

// Return true when the name matches the naming rules of the ecosystem.
export function packageNameMatchesRules(ecosystem, name) {
  const rule = PACKAGE_NAME_RULES[ecosystem];
  if (rule === undefined) return false;
  return name.length <= rule.maxLength && rule.pattern.test(name);
}

// Return the messages for the date fields of the form, or [] when they are correct.
// All dates are "YYYY-MM-DD" text. today is the date of the user, so that the form never
// refuses a date that the user can see on a calendar. The Host checks the same rules.
export function dateErrors(startDate, endDate, today) {
  if (!startDate || !endDate) return ["Enter a start date and an end date."];
  // "YYYY-MM-DD" text sorts in the same order as the dates.
  if (endDate <= startDate) return ["The end date must be after the start date."];
  if (endDate > today) return ["The end date must not be in the future."];
  return [];
}

// Return the message for a submit without a token from the Turnstile widget (the bot
// check), or null when the form has a token. The Host refuses a submit without a token.
export function botCheckError(token) {
  return token ? null : "Wait for the bot check to complete, then click Compute.";
}

// The text that the page shows when a request to the Host fails and the Host gives no
// message that the page can show.
const GENERIC_ERROR = "Something went wrong. Try again later.";

// Return the message for a refused POST /api/jobs. body is the JSON body of the response,
// or null. The Host gives a user message as a text "detail" (for example, for the rate
// limits and the busy queue). Other bodies hold internal details, so the page shows a
// generic text.
export function submitErrorMessage(body) {
  const detail = body?.detail;
  return typeof detail === "string" && detail !== "" ? detail : GENERIC_ERROR;
}

// Return the messages for the package fields of the form, or [] when they are correct.
export function packageErrors(ecosystem, packages) {
  const names = cleanPackages(packages);
  if (names.length === 0) return ["Enter at least one package."];
  if (names.length > MAX_PACKAGES) return [`Enter at most ${MAX_PACKAGES} packages.`];
  const seen = new Set();
  const errors = [];
  for (const name of names) {
    if (!packageNameMatchesRules(ecosystem, name)) {
      errors.push(`${name} is not a valid ${ECOSYSTEM_LABELS[ecosystem] ?? ecosystem} package name.`);
      continue;
    }
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

// Return the note that the form shows when the Comparison is full, or "" below the limit.
export function packageLimitNote(packages) {
  return canAddPackage(packages) ? "" : `A Comparison holds at most ${MAX_PACKAGES} packages.`;
}

// Return what the page does with the package limit note after a change of the package
// fields: "show" when the count changes to the limit, "keep" when the count stays at the
// limit (so that the note does not show again on each renumber), and "clear" below the
// limit. previousCount is the count before the change, or null on the first call.
export function packageLimitNoteAction(previousCount, packages) {
  if (canAddPackage(packages)) return "clear";
  return previousCount === packages.length ? "keep" : "show";
}

// The modes of a Comparison (GLOSSARY.md). The Host checks the same names (Mode).
export const MODES = ["window_summary", "per_release"];
const WINDOW_SUMMARY = "window_summary";
const PER_RELEASE = "per_release";

// The weighting types of the Tool (--weighting-type). "disable" is the default.
export const WEIGHTING_TYPES = ["disable", "linear", "exponential", "inverse"];
export const NO_WEIGHTING = "disable";
export const EXPONENTIAL = "exponential";
// Exponential weighting needs a half-life in this range of days. The Host checks the same rule.
const MIN_HALF_LIFE_DAYS = 1;
const MAX_HALF_LIFE_DAYS = 3650;

// Return the body of POST /api/jobs for the form state and the Turnstile token.
// form: { ecosystem, packages, startDate, endDate, mode, weightingType, halfLife } with
// dates as "YYYY-MM-DD" and halfLife as the text of the field. Without mode, the body asks
// for a Window summary. Without weightingType, the body has no weighting. The body has a half-life only for exponential weighting.
export function requestFromForm(form, turnstileToken) {
  const weightingType = form.weightingType || NO_WEIGHTING;
  return {
    ecosystem: form.ecosystem,
    packages: cleanPackages(form.packages),
    start_date: form.startDate,
    end_date: form.endDate,
    mode: form.mode || WINDOW_SUMMARY,
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

// Return the table rows of a done Job: one row for each package (Window summary), or one
// row for each release of each package (Per-release).
export function resultRows(job) {
  if (job.comparison.mode === PER_RELEASE) return perReleaseRows(job);
  return job.results.map((result) => {
    const summary = result.summary ?? {};
    const values = rowValues(summary, result.error || summary.error || "");
    return {
      package: result.package,
      mttu: values.mttu,
      mttr: values.mttr,
      dependencies: summary.num_dependencies ?? 0,
      error: values.error,
    };
  });
}

// Return one row for each release of each package of a done Per-release Job. A package
// that failed has no releases, so it gets one row with its error and no values.
function perReleaseRows(job) {
  return job.results.flatMap((result) => {
    if (result.releases.length === 0) {
      return [
        {
          package: result.package,
          version: NO_VALUE,
          releaseDate: NO_VALUE,
          mttu: NO_VALUE,
          mttr: NO_VALUE,
          dependencies: 0,
          error: result.error,
        },
      ];
    }
    return result.releases.map((release) => {
      const values = rowValues(release, release.error);
      return {
        package: result.package,
        version: release.package_version,
        releaseDate: release.package_release_date,
        mttu: values.mttu,
        mttr: values.mttr,
        dependencies: release.num_dependencies,
        error: values.error,
      };
    });
  });
}

// Return the Chart.js data of one line chart of a done Per-release Job. metric is "mttu"
// or "mttr"; the page shows each in its own chart, because MTTU values are often much
// larger than MTTR values. Each package has one line. A point has x = the release date
// (milliseconds, UTC) for a linear x-axis, y = the days, and the release version.
// A release with a status that is not "ok", or without dependencies, has y = null, so that
// the line has a gap there and does not show a value of 0. The page code adds the colors.
export function lineChartData(job, metric) {
  return {
    datasets: job.results.map((result) => ({
      label: result.package,
      data: result.releases.map((release) => ({
        x: Date.parse(`${release.package_release_date}T00:00:00Z`),
        y: metricValue(release, metric),
        version: release.package_version,
      })),
    })),
  };
}

// Return the y-axis label of the line charts of a done Job (see axisLabel).
export function lineChartAxisLabel(job) {
  return axisLabel(job.comparison.weighting_type);
}

// Return the Chart.js data of the grouped bar chart of a done Window summary Job: one
// group for each package, with an MTTU bar and an MTTR bar. A package without a value
// (a package that failed, or a package without dependencies) has null, so that it shows
// no bar and not a 0. The page code adds the colors.
export function barChartData(job) {
  const value = (result, key) => metricValue(result.summary, key);
  return {
    labels: job.results.map((result) => result.package),
    datasets: [
      { label: "MTTU", data: job.results.map((result) => value(result, "mttu")) },
      { label: "MTTR", data: job.results.map((result) => value(result, "mttr")) },
    ],
  };
}

// Return true when the Chart.js data of barChartData or lineChartData has at least one
// value. The page hides a chart without values, so that it does not show an empty chart.
export function chartHasValues(data) {
  return data.datasets.some((dataset) =>
    dataset.data.some((point) => (point !== null && typeof point === "object" ? point.y : point) !== null),
  );
}

// Return the y-axis label of the bar chart of a done Job. It follows the weighting type,
// because a sum of days and a weighted mean are different quantities (axisLabel).
export function barChartAxisLabel(job) {
  return axisLabel(job.comparison.weighting_type);
}

// The one size of every chart in CSS pixels, in the Webapp and in the Extension webview.
// The charts sit in one column, so a wide window does not stretch them too far, and they
// stay short, so that a chart and the start of the result table fit on one screen.
export const CHART_SIZE = { maxWidth: 720, height: 220 };

// Return the inline style of a chart box (CSSOM property names). The page code of the
// Webapp and of the webview assigns it to each box, so both places use CHART_SIZE. The box
// is centered and shrinks on a narrow screen. Each call returns a new object.
export function chartBoxStyle() {
  return {
    position: "relative",
    width: "100%",
    maxWidth: `${CHART_SIZE.maxWidth}px`,
    height: `${CHART_SIZE.height}px`,
    marginLeft: "auto",
    marginRight: "auto",
  };
}

// The styles of the chart elements: thin lines, small points, and bars with round corners.
// The dataset colors come from the page code. Each call returns a new object, because
// Chart.js keeps the options of each chart.
function chartElements() {
  return {
    line: { borderWidth: 1.5 },
    point: { radius: 2, hoverRadius: 4 },
    bar: { borderRadius: 3 },
  };
}

// The bar thickness is a dataset option in Chart.js, so it goes in options.datasets. The
// bars stay narrow when the chart is wide and the Job has few packages.
const MAX_BAR_THICKNESS = 32;

// A light y-axis grid. A gray with a low alpha shows on a light and on a dark background,
// so the Webapp and the webview themes use the same value.
function lightGrid() {
  return { color: "rgba(128, 128, 128, 0.15)" };
}

// The legend of each chart: small color boxes and small text, so that the legend takes one
// short line. The text stays at 12 px, so that it stays readable. Each call returns a new
// object, because Chart.js keeps the options of each chart.
function compactLegend(labels = {}) {
  return {
    position: "top",
    align: "end",
    labels: { boxWidth: 12, boxHeight: 12, padding: 8, font: { size: 12 }, ...labels },
  };
}

// The most labels on a y-axis. Three or four values give enough scale for days.
const MAX_Y_TICKS = 4;

// Return the tooltip text of one value: the series name and the days with one decimal.
function tooltipValue(name, days) {
  return `${name}: ${days === null ? "no value" : `${formatDays(days)} days`}`;
}

// Return the Chart.js options of the bar chart of a done Window summary Job (barChartData).
// The options hold no DOM objects, so that the Extension webview uses them unchanged.
export function barChartOptions(job) {
  return {
    maintainAspectRatio: false,
    interaction: { mode: "index", intersect: false },
    elements: chartElements(),
    datasets: { bar: { maxBarThickness: MAX_BAR_THICKNESS } },
    plugins: {
      legend: compactLegend(),
      tooltip: { callbacks: { label: (item) => tooltipValue(item.dataset.label, item.raw) } },
    },
    scales: {
      x: { grid: { display: false } },
      y: {
        beginAtZero: true,
        ticks: { maxTicksLimit: MAX_Y_TICKS },
        grid: lightGrid(),
        title: { display: true, text: barChartAxisLabel(job) },
      },
    },
  };
}

// The most labels on the x-axis of a line chart. Six dates fit in a chart of full width.
const MAX_X_TICKS = 6;

// Return a release date of the x-axis (milliseconds, UTC) as "YYYY-MM-DD".
function formatReleaseDate(ms) {
  return new Date(ms).toISOString().slice(0, 10);
}

// Return the Chart.js options of one line chart of a done Per-release Job (lineChartData).
// The x-axis is linear in milliseconds, so the ticks and the tooltip title show the date.
// The options hold no DOM objects, so that the Extension webview uses them unchanged.
export function lineChartOptions(job) {
  return {
    maintainAspectRatio: false,
    interaction: { mode: "nearest", intersect: false },
    elements: chartElements(),
    plugins: {
      // Chart.js draws a point style with a diameter of about 1.4 times boxHeight.
      legend: compactLegend({ usePointStyle: true, boxHeight: 8 }),
      tooltip: {
        callbacks: {
          title: (items) => formatReleaseDate(items[0].parsed.x),
          label: (item) => tooltipValue(`${item.dataset.label} ${item.raw.version}`, item.raw.y),
        },
      },
    },
    scales: {
      x: {
        type: "linear",
        title: { display: true, text: "Release date" },
        // Horizontal dates are easier to read; Chart.js skips a date that does not fit.
        ticks: { callback: (value) => formatReleaseDate(value), maxTicksLimit: MAX_X_TICKS, maxRotation: 0 },
        grid: { display: false },
      },
      y: {
        beginAtZero: true,
        ticks: { maxTicksLimit: MAX_Y_TICKS },
        grid: lightGrid(),
        title: { display: true, text: lineChartAxisLabel(job) },
      },
    },
  };
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

// The columns of the Per-release table and CSV: TABLE_COLUMNS with the release columns.
export const PER_RELEASE_COLUMNS = [
  TABLE_COLUMNS[0],
  ["version", "Version"],
  ["releaseDate", "Release date"],
  ...TABLE_COLUMNS.slice(1),
];

// Return the columns of the table and the CSV for the mode of a Comparison.
export function tableColumns(mode) {
  return mode === PER_RELEASE ? PER_RELEASE_COLUMNS : TABLE_COLUMNS;
}

// Return the CSV text of the table rows (resultRows), with the given columns
// (tableColumns). A cell that shows a dash in the table (no value) is empty in the CSV,
// so that spreadsheet tools read it as missing.
export function rowsToCsv(rows, columns = TABLE_COLUMNS) {
  const cell = (value) => {
    const text = value === NO_VALUE ? "" : String(value);
    return /[",\r\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
  };
  const lines = [columns.map(([, header]) => cell(header))];
  for (const row of rows) lines.push(columns.map(([key]) => cell(row[key])));
  return lines.map((line) => `${line.join(",")}\r\n`).join("");
}

// Return the file name of the CSV download of a Comparison. The mode and the weighting (with
// the half-life) are in the name, so two downloads of one window get different names.
export function csvFileName(comparison) {
  const safe = (text) => text.replace(/^@/, "").replace(/[^A-Za-z0-9._-]+/g, "-");
  const weighting = needsHalfLife(comparison.weighting_type)
    ? `${comparison.weighting_type}-${comparison.half_life}`
    : comparison.weighting_type || "disable";
  const parts = [
    comparison.ecosystem,
    ...comparison.packages,
    comparison.start_date,
    comparison.end_date,
    comparison.mode || WINDOW_SUMMARY,
    weighting,
  ];
  return `mttu-mttr_${parts.map((part) => safe(String(part))).join("_")}.csv`;
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

// --- Sample Comparison (ticket #45) ---

// The path of the Sample Comparison file, relative to the homepage. The file is a done Job
// of the Host (the shape of contract/examples/job_done_window_summary.json) plus
// "computed_on" and "osv_index_date". The script in webapp/sample/ makes it again with the
// Tool.
export const SAMPLE_COMPARISON_PATH = "static/sample/sample_comparison.json";

// Return the page URL for the Sample Comparison: the URL `href` with the sample Comparison
// in place of any Comparison and without a job ID, so that a reload fills the form with the
// sample and does not show an older Job.
export function sampleUrl(href, comparison) {
  const url = new URL(href);
  url.searchParams.delete(JOB_PARAM);
  url.search = withComparison(url.search, comparison);
  url.hash = "#try-it";
  return url.toString();
}

// Return the label that tells the reader when the Tool computed the Sample Comparison.
export function sampleLabel(sample) {
  return `Precomputed on ${sample.computed_on}`;
}
