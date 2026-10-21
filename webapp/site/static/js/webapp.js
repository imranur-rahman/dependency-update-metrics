// Page code of the Webapp section ("Try It"). It reads the form, sends the Comparison
// to the Host, and shows the result. All logic that has no DOM code is in comparison.js.
import {
  requestFromForm,
  describeJob,
  isFinished,
  resultRows,
  needsHalfLife,
  halfLifeError,
  resultsNote,
  packageErrors,
  dateErrors,
  submitErrorMessage,
  canAddPackage,
  withComparison,
  comparisonFromSearch,
  formFromComparison,
  barChartData,
  barChartAxisLabel,
  lineChartData,
  lineChartAxisLabel,
  tableColumns,
  rowsToCsv,
  csvFileName,
  progressOf,
  jobIdFromUrl,
  urlWithJobId,
  JOB_EVENTS,
} from "./comparison.js";

// Colors of the bar chart: slots 1 and 2 of the validated categorical palette
// (blue, orange), so that the two series stay apart for color-blind readers.
const SERIES_COLORS = { MTTU: "#2a78d6", MTTR: "#eb6834" };
// Colors and point shapes of the Per-release lines: one for each package, in the fixed
// order of the validated categorical palette (slots 1 to 5). The shape is a second
// encoding, so that a line does not depend on its color alone.
const PACKAGE_COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"];
const PACKAGE_POINT_STYLES = ["circle", "rect", "triangle", "rectRot", "crossRot"];

const section = document.getElementById("try-it");
const form = document.getElementById("comparison-form");
const statusLine = document.getElementById("job-status");
const progressBar = document.getElementById("job-progress");
const table = document.getElementById("result-table");
const submitButton = document.getElementById("submit-comparison");
const hostUrl = section.dataset.hostUrl.replace(/\/$/, "");
const weightingSelect = document.getElementById("weighting-type");
const halfLifeField = document.getElementById("half-life-field");
const halfLifeInput = document.getElementById("half-life");
const halfLifeMessage = document.getElementById("half-life-error");
const resultsNoteLine = document.getElementById("results-note");
const packageFields = document.getElementById("package-fields");
const addPackageButton = document.getElementById("add-package");
const packageMessage = document.getElementById("package-error");
const dateMessage = document.getElementById("date-error");
const startDateInput = document.getElementById("start-date");
const endDateInput = document.getElementById("end-date");
const chartFigure = document.getElementById("result-chart-figure");
const chartCanvas = document.getElementById("result-chart");
const releaseChartsFigure = document.getElementById("release-charts-figure");
const releaseCanvases = {
  mttu: document.getElementById("release-chart-mttu"),
  mttr: document.getElementById("release-chart-mttr"),
};
const modeSelect = document.getElementById("mode");
const csvButton = document.getElementById("download-csv");

let chart = null;
let releaseCharts = [];
let csv = { text: "", fileName: "" };

// --- Weighting fields (ticket #8) ---

// Show the half-life field only for the weighting type that needs it.
function showWeightingFields() {
  halfLifeField.classList.toggle("is-hidden", !needsHalfLife(weightingSelect.value));
  showHalfLifeError(null);
}

function showHalfLifeError(text) {
  halfLifeMessage.textContent = text ?? "";
  halfLifeMessage.hidden = !text;
  halfLifeInput.classList.toggle("is-danger", Boolean(text));
}

// Show the note that says what the numbers of the result are (a sum or a weighted mean).
function showResultsNote(weightingType) {
  resultsNoteLine.textContent = weightingType ? resultsNote(weightingType) : "";
  resultsNoteLine.hidden = !weightingType;
}

// --- Package fields ---

function packageInputs() {
  return [...packageFields.querySelectorAll("input[name=packages]")];
}

function packageValues() {
  return packageInputs().map((input) => input.value);
}

// Give each package field its number, and allow a new field only below the limit.
function renumberPackageFields() {
  packageInputs().forEach((input, index) => {
    input.id = `package-${index + 1}`;
    input.setAttribute("aria-label", `Package ${index + 1}`);
  });
  addPackageButton.disabled = !canAddPackage(packageValues());
}

function addPackageField(value = "") {
  if (!canAddPackage(packageValues())) return null;
  const field = document.createElement("div");
  field.className = "field has-addons package-field";
  const control = document.createElement("div");
  control.className = "control is-expanded";
  const input = document.createElement("input");
  input.className = "input";
  input.name = "packages";
  input.type = "text";
  input.value = value;
  control.append(input);
  const removeControl = document.createElement("div");
  removeControl.className = "control";
  const remove = document.createElement("button");
  remove.className = "button";
  remove.type = "button";
  remove.textContent = "Remove";
  remove.addEventListener("click", () => {
    field.remove();
    renumberPackageFields();
    showPackageErrors([]);
  });
  removeControl.append(remove);
  field.append(control, removeControl);
  packageFields.append(field);
  renumberPackageFields();
  return input;
}

// Make the form show exactly the given package names (at least one field).
function setPackageFields(names) {
  const [first, ...rest] = names.length > 0 ? names : [""];
  packageFields.querySelectorAll(".package-field").forEach((field, index) => {
    if (index > 0) field.remove();
  });
  packageInputs()[0].value = first;
  for (const name of rest) addPackageField(name);
  renumberPackageFields();
}

function showPackageErrors(errors) {
  packageMessage.textContent = errors.join(" ");
  packageMessage.hidden = errors.length === 0;
  for (const input of packageInputs()) input.classList.toggle("is-danger", errors.length > 0);
}

// --- Date fields ---

function showDateErrors(errors) {
  dateMessage.textContent = errors.join(" ");
  dateMessage.hidden = errors.length === 0;
  for (const input of [startDateInput, endDateInput]) {
    input.classList.toggle("is-danger", errors.length > 0);
  }
}

// Return the date of the user as "YYYY-MM-DD", in the time zone of the browser.
function localToday() {
  const now = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}`;
}

// --- Shareable URL ---

// Fill the form from a Comparison in the page URL (a shared link).
function fillFormFromUrl() {
  const comparison = comparisonFromSearch(window.location.search);
  if (comparison === null) return;
  const state = formFromComparison(comparison);
  form.elements.ecosystem.value = state.ecosystem;
  setPackageFields(state.packages);
  form.elements.startDate.value = state.startDate;
  form.elements.endDate.value = state.endDate;
  if (state.mode) modeSelect.value = state.mode;
  weightingSelect.value = state.weightingType;
  if (state.halfLife !== "") halfLifeInput.value = state.halfLife;
  showWeightingFields();
}

// Put the Comparison in the page URL, so that the user can share the link.
function showComparisonInUrl(comparison) {
  const url = `${window.location.pathname}${withComparison(window.location.search, comparison)}#try-it`;
  window.history.replaceState(null, "", url);
}

// --- Results ---

function showStatus(text) {
  statusLine.textContent = text;
}

// Show a bar with no value while the Job waits, and the share of packages that are done
// while it runs. A finished Job shows no bar.
function showProgress(progress) {
  progressBar.hidden = progress === null;
  if (progress === null) return;
  if (progress.value === null) {
    progressBar.removeAttribute("value");
    progressBar.removeAttribute("max");
  } else {
    progressBar.max = progress.max;
    progressBar.value = progress.value;
  }
}

function showJob(job) {
  showStatus(describeJob(job));
  showProgress(progressOf(job));
  if (job.state === "done") showResult(job);
}

function showRows(rows, columns) {
  const headerRow = document.createElement("tr");
  for (const [, header] of columns) {
    const th = document.createElement("th");
    th.textContent = header;
    headerRow.append(th);
  }
  table.querySelector("thead").replaceChildren(headerRow);
  table.querySelector("tbody").replaceChildren(
    ...rows.map((row) => {
      const tr = document.createElement("tr");
      for (const [key] of columns) {
        const td = document.createElement("td");
        td.textContent = String(row[key]);
        tr.append(td);
      }
      return tr;
    }),
  );
  table.hidden = rows.length === 0;
}

function showChart(job) {
  if (chart !== null) {
    chart.destroy();
    chart = null;
  }
  // Per-release Jobs get two line charts (showReleaseCharts).
  if (job === null || job.comparison.mode !== "window_summary" || !window.Chart) {
    chartFigure.hidden = true;
    return;
  }
  const data = barChartData(job);
  for (const dataset of data.datasets) {
    const color = SERIES_COLORS[dataset.label];
    Object.assign(dataset, { backgroundColor: color, borderColor: color, borderRadius: 4 });
  }
  chartFigure.hidden = false;
  chart = new window.Chart(chartCanvas, {
    type: "bar",
    data,
    options: {
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: { legend: { position: "top" } },
      scales: {
        x: { grid: { display: false } },
        y: { beginAtZero: true, title: { display: true, text: barChartAxisLabel(job) } },
      },
    },
  });
}

// Format a release date of the x-axis (milliseconds, UTC) as "YYYY-MM-DD".
function formatReleaseDate(ms) {
  return new Date(ms).toISOString().slice(0, 10);
}

// Show the MTTU and the MTTR line charts of a done Per-release Job: one line for each
// package, with a gap at each release that failed (lineChartData).
function showReleaseCharts(job) {
  for (const releaseChart of releaseCharts) releaseChart.destroy();
  releaseCharts = [];
  if (job === null || job.comparison.mode !== "per_release" || !window.Chart) {
    releaseChartsFigure.hidden = true;
    return;
  }
  releaseChartsFigure.hidden = false;
  for (const [metric, canvas] of Object.entries(releaseCanvases)) {
    const data = lineChartData(job, metric);
    data.datasets.forEach((dataset, index) => {
      const color = PACKAGE_COLORS[index % PACKAGE_COLORS.length];
      Object.assign(dataset, {
        borderColor: color,
        backgroundColor: color,
        borderWidth: 2,
        pointRadius: 4,
        pointHoverRadius: 6,
        pointStyle: PACKAGE_POINT_STYLES[index % PACKAGE_POINT_STYLES.length],
        spanGaps: false,
      });
    });
    releaseCharts.push(
      new window.Chart(canvas, {
        type: "line",
        data,
        options: {
          maintainAspectRatio: false,
          interaction: { mode: "nearest", intersect: false },
          plugins: {
            legend: { position: "top", labels: { usePointStyle: true } },
            tooltip: {
              callbacks: {
                title: (items) => formatReleaseDate(items[0].parsed.x),
                label: (item) =>
                  `${item.dataset.label} ${item.raw.version}: ${
                    item.raw.y === null ? "failed" : `${item.raw.y.toFixed(1)} days`
                  }`,
              },
            },
          },
          scales: {
            x: {
              type: "linear",
              title: { display: true, text: "Release date" },
              ticks: { callback: (value) => formatReleaseDate(value), maxTicksLimit: 6 },
              grid: { display: false },
            },
            y: { beginAtZero: true, title: { display: true, text: lineChartAxisLabel(job) } },
          },
        },
      }),
    );
  }
}

function showCsvButton(job) {
  if (job === null) {
    csvButton.hidden = true;
    return;
  }
  csv = {
    text: rowsToCsv(resultRows(job), tableColumns(job.comparison.mode)),
    fileName: csvFileName(job.comparison),
  };
  csvButton.hidden = false;
}

function showResult(job) {
  showRows(job === null ? [] : resultRows(job), tableColumns(job?.comparison.mode));
  showChart(job);
  showReleaseCharts(job);
  showCsvButton(job);
  showResultsNote(job === null ? null : job.comparison.weighting_type);
}

function downloadCsv() {
  const blob = new Blob([csv.text], { type: "text/csv;charset=utf-8" });
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = csv.fileName;
  link.click();
  URL.revokeObjectURL(link.href);
}

// --- Job ---

async function getJob(jobId) {
  const response = await fetch(`${hostUrl}/api/jobs/${encodeURIComponent(jobId)}`);
  if (!response.ok) throw new Error(`GET returned ${response.status}`);
  return response.json();
}

// Show each state of the Job from the server-sent event stream, until the Job is finished.
// When the connection drops, EventSource connects again by itself, and the Host sends the
// current state first. The promise resolves when the Job is finished.
function followJob(jobId) {
  return new Promise((resolve, reject) => {
    const source = new EventSource(`${hostUrl}/api/jobs/${encodeURIComponent(jobId)}/events`);
    for (const name of JOB_EVENTS) {
      source.addEventListener(name, (event) => {
        const job = JSON.parse(event.data);
        showJob(job);
        if (isFinished(job)) {
          source.close();
          resolve(job);
        }
      });
    }
    source.addEventListener("error", () => {
      // CLOSED means that the browser does not try again (for example, a 404).
      if (source.readyState === EventSource.CLOSED) reject(new Error("The event stream closed"));
    });
  });
}

// Put the job ID in the page URL, so that a reload shows the same Job.
function rememberJob(jobId) {
  history.replaceState(null, "", urlWithJobId(location.href, jobId));
}

// After a reload, get the current state of the Job in the URL at once, then follow it.
async function resumeJobFromUrl() {
  const jobId = jobIdFromUrl(location.href);
  if (jobId === null) return;
  submitButton.disabled = true;
  try {
    const job = await getJob(jobId);
    showJob(job);
    if (!isFinished(job)) await followJob(jobId);
  } catch (error) {
    console.error(error);
    showProgress(null);
    showStatus("Something went wrong. Try again later.");
  } finally {
    submitButton.disabled = false;
  }
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const data = new FormData(form);
  const packages = data.getAll("packages");
  const errors = packageErrors(data.get("ecosystem"), packages);
  showPackageErrors(errors);
  const halfLifeText = halfLifeError(data.get("weightingType"), data.get("halfLife"));
  showHalfLifeError(halfLifeText);
  const dateTexts = dateErrors(data.get("startDate"), data.get("endDate"), localToday());
  showDateErrors(dateTexts);
  if (errors.length > 0 || halfLifeText || dateTexts.length > 0) return;
  const body = requestFromForm(
    {
      ecosystem: data.get("ecosystem"),
      packages,
      startDate: data.get("startDate"),
      endDate: data.get("endDate"),
      mode: data.get("mode"),
      weightingType: data.get("weightingType"),
      halfLife: data.get("halfLife"),
    },
    "", // Ticket #10 adds the Turnstile token.
  );
  const { turnstile_token: _token, ...comparison } = body;
  showComparisonInUrl(comparison);
  submitButton.disabled = true;
  showResult(null);
  showStatus("Sending");
  try {
    const response = await fetch(`${hostUrl}/api/jobs`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!response.ok) {
      // The Host gives a user message for a refused submit (for example, a rate limit).
      const refusal = await response.json().catch(() => null);
      console.error(`POST returned ${response.status}`, refusal);
      showProgress(null);
      showStatus(submitErrorMessage(refusal));
      return;
    }
    const { job_id: jobId } = await response.json();
    rememberJob(jobId);
    await followJob(jobId);
  } catch (error) {
    console.error(error);
    showProgress(null);
    showStatus("Something went wrong. Try again later.");
  } finally {
    submitButton.disabled = false;
  }
});

weightingSelect.addEventListener("change", showWeightingFields);
addPackageButton.addEventListener("click", () => addPackageField()?.focus());
csvButton.addEventListener("click", downloadCsv);
showWeightingFields();
renumberPackageFields();
fillFormFromUrl();
resumeJobFromUrl();
