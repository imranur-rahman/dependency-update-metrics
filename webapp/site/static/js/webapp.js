// Page code of the Webapp section ("Try It"). It reads the form, sends the Comparison
// to the Host, and shows the result. All logic that has no DOM code is in comparison.js.
import {
  requestFromForm,
  describeJob,
  isFinished,
  canCancel,
  cancelRequest,
  resultRows,
  needsHalfLife,
  halfLifeError,
  resultsNote,
  packageErrors,
  dateErrors,
  botCheckError,
  canAddPackage,
  withComparison,
  comparisonFromSearch,
  formFromComparison,
  barChartData,
  barChartOptions,
  lineChartData,
  lineChartOptions,
  CHART_HEIGHTS,
  chartHasValues,
  tableColumns,
  rowsToCsv,
  csvFileName,
  progressOf,
  jobIdFromUrl,
  urlWithJobId,
  JOB_EVENTS,
} from "./comparison.js";
import {
  submitErrorNotice,
  connectionErrorNotice,
  jobErrorNotice,
  doneJobNotice,
  errorCell,
} from "./errorNotice.js";
import { showNotification, clearNotification } from "./notification.js";

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
const statusBox = document.getElementById("job-status-box");
const jobNotice = document.getElementById("job-notice");
const resultsNotice = document.getElementById("results-notice");
const resultsCard = document.getElementById("results-card");
const progressBar = document.getElementById("job-progress");
const table = document.getElementById("result-table");
const submitButton = document.getElementById("submit-comparison");
const cancelButton = document.getElementById("cancel-job");
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
const csvButton = document.getElementById("download-csv");
const botCheckMessage = document.getElementById("bot-check-error");

// Each chart box gets its compact height from comparison.js, which the Extension shares.
for (const box of document.querySelectorAll(".chart-box")) {
  box.style.height = `${CHART_HEIGHTS[box.dataset.chart]}px`;
}

let chart = null;
let releaseCharts = [];
let csv = { text: "", fileName: "" };
// The Job that the page shows now, so that the Cancel button knows which Job to cancel.
let currentJobId = null;

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
  remove.className = "button is-danger is-light";
  remove.type = "button";
  remove.setAttribute("aria-label", "Remove this package");
  remove.innerHTML = '<span class="icon is-small"><i class="fas fa-times"></i></span><span>Remove</span>';
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

// --- Bot check (Cloudflare Turnstile) ---

function showBotCheckError(text) {
  botCheckMessage.textContent = text ?? "";
  botCheckMessage.hidden = !text;
}

// A token works only once, so the widget needs a new token after each submit.
function resetBotCheck() {
  window.turnstile?.reset();
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
  if (state.mode) form.elements.mode.value = state.mode;
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

// Show the status of the Job while it waits or runs.
function showStatus(text) {
  statusLine.textContent = text;
  statusBox.hidden = !text;
}

// Show an error (a failed or refused Job) in a red notification in place of the status, and
// scroll to it, so that the user does not miss it. notice comes from errorNotice.js.
function showError(notice) {
  showStatus("");
  showProgress(null);
  cancelButton.hidden = true;
  showNotification(jobNotice, notice);
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
  currentJobId = job.job_id;
  cancelButton.hidden = !canCancel(job);
  if (job.state === "error") {
    showError(jobErrorNotice(job));
    return;
  }
  clearNotification(jobNotice);
  showStatus(describeJob(job));
  showProgress(progressOf(job));
  if (job.state === "done") showResult(job);
}

// Show the text of an error cell in red, with the next step under it (errorCell). A cell
// without an error, or with the no-dependencies text, keeps its plain text.
function fillErrorCell(td, text) {
  const cell = errorCell(text);
  if (cell === null) {
    td.textContent = String(text);
    return;
  }
  const message = document.createElement("span");
  message.className = "has-text-danger has-text-weight-semibold";
  message.textContent = cell.text;
  const nextStep = document.createElement("span");
  nextStep.className = "is-block is-size-7";
  nextStep.textContent = cell.nextStep;
  td.replaceChildren(message, nextStep);
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
        if (key === "error") fillErrorCell(td, row[key]);
        else td.textContent = String(row[key]);
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
  // A Job without values (each package failed or has no dependencies) shows no chart.
  if (!chartHasValues(data)) {
    chartFigure.hidden = true;
    return;
  }
  for (const dataset of data.datasets) {
    const color = SERIES_COLORS[dataset.label];
    Object.assign(dataset, { backgroundColor: color, borderColor: color, borderRadius: 4 });
  }
  chartFigure.hidden = false;
  chart = new window.Chart(chartCanvas, { type: "bar", data, options: barChartOptions(job) });
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
  const chartData = Object.fromEntries(
    Object.keys(releaseCanvases).map((metric) => [metric, lineChartData(job, metric)]),
  );
  // A Job without values (each release failed or has no dependencies) shows no charts.
  if (!Object.values(chartData).some(chartHasValues)) {
    releaseChartsFigure.hidden = true;
    return;
  }
  releaseChartsFigure.hidden = false;
  for (const [metric, canvas] of Object.entries(releaseCanvases)) {
    const data = chartData[metric];
    data.datasets.forEach((dataset, index) => {
      const color = PACKAGE_COLORS[index % PACKAGE_COLORS.length];
      Object.assign(dataset, {
        borderColor: color,
        backgroundColor: color,
        borderWidth: 2,
        pointRadius: 3,
        pointHoverRadius: 5,
        pointStyle: PACKAGE_POINT_STYLES[index % PACKAGE_POINT_STYLES.length],
        spanGaps: false,
      });
    });
    releaseCharts.push(new window.Chart(canvas, { type: "line", data, options: lineChartOptions(job) }));
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

// Show the results card only for a done Job.
function showResult(job) {
  resultsCard.hidden = job === null;
  showRows(job === null ? [] : resultRows(job), tableColumns(job?.comparison.mode));
  showChart(job);
  showReleaseCharts(job);
  showCsvButton(job);
  showResultsNote(job === null ? null : job.comparison.weighting_type);
  showPackageFailures(job);
}

// Show a red notification above the results of a done Job in which a package failed. The
// table shows each error next to its package (fillErrorCell).
function showPackageFailures(job) {
  const notice = job === null ? null : doneJobNotice(job);
  if (notice === null) clearNotification(resultsNotice);
  else showNotification(resultsNotice, notice);
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
  if (!response.ok) {
    // The Host gives a user message (for example "Job not found"); the error carries its notice.
    const refusal = await response.json().catch(() => null);
    throw Object.assign(new Error(`GET returned ${response.status}`), {
      notice: submitErrorNotice(refusal),
    });
  }
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

// Cancel the Job that the page shows. The event stream then sends "cancelled", and the
// page shows it. A Job that the user only leaves (for example, a closed tab) continues.
async function cancelJob() {
  if (currentJobId === null) return;
  const { url, method } = cancelRequest(hostUrl, currentJobId);
  cancelButton.disabled = true;
  try {
    const response = await fetch(url, { method });
    if (!response.ok) throw new Error(`DELETE returned ${response.status}`);
    showJob(await response.json());
  } catch (error) {
    console.error(error);
  } finally {
    cancelButton.disabled = false;
  }
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
    showError(error.notice ?? connectionErrorNotice());
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
  const token = data.get("cf-turnstile-response");
  const botCheckText = botCheckError(token);
  showBotCheckError(botCheckText);
  if (errors.length > 0 || halfLifeText || dateTexts.length > 0 || botCheckText) return;
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
    token,
  );
  const { turnstile_token: _token, ...comparison } = body;
  showComparisonInUrl(comparison);
  submitButton.disabled = true;
  showResult(null);
  clearNotification(jobNotice);
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
      showError(submitErrorNotice(refusal));
      return;
    }
    const { job_id: jobId } = await response.json();
    rememberJob(jobId);
    await followJob(jobId);
  } catch (error) {
    console.error(error);
    showError(connectionErrorNotice());
  } finally {
    resetBotCheck();
    submitButton.disabled = false;
  }
});

weightingSelect.addEventListener("change", showWeightingFields);
addPackageButton.addEventListener("click", () => addPackageField()?.focus());
csvButton.addEventListener("click", downloadCsv);
cancelButton.addEventListener("click", cancelJob);
showWeightingFields();
renumberPackageFields();
fillFormFromUrl();
resumeJobFromUrl();
