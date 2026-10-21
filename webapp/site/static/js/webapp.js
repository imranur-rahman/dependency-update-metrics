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
} from "./comparison.js";

const POLL_MS = 2000;

const section = document.getElementById("try-it");
const form = document.getElementById("comparison-form");
const statusLine = document.getElementById("job-status");
const table = document.getElementById("result-table");
const submitButton = document.getElementById("submit-comparison");
const hostUrl = section.dataset.hostUrl.replace(/\/$/, "");
const weightingSelect = document.getElementById("weighting-type");
const halfLifeField = document.getElementById("half-life-field");
const halfLifeInput = document.getElementById("half-life");
const halfLifeMessage = document.getElementById("half-life-error");
const resultsNoteLine = document.getElementById("results-note");

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

weightingSelect.addEventListener("change", showWeightingFields);
showWeightingFields();

function showStatus(text) {
  statusLine.textContent = text;
}

function showRows(rows) {
  const body = table.querySelector("tbody");
  body.replaceChildren(
    ...rows.map((row) => {
      const tr = document.createElement("tr");
      for (const value of [row.package, row.mttu, row.mttr, row.dependencies, row.error]) {
        const td = document.createElement("td");
        td.textContent = String(value);
        tr.append(td);
      }
      return tr;
    }),
  );
  table.hidden = rows.length === 0;
}

async function getJob(jobId) {
  const response = await fetch(`${hostUrl}/api/jobs/${encodeURIComponent(jobId)}`);
  if (!response.ok) throw new Error(`GET returned ${response.status}`);
  return response.json();
}

// Get the Job state with a plain GET until the Job is finished.
// Ticket #5 replaces this loop with the server-sent event stream.
async function followJob(jobId) {
  for (;;) {
    const job = await getJob(jobId);
    showStatus(describeJob(job));
    if (isFinished(job)) {
      if (job.state === "done") {
        showRows(resultRows(job));
        showResultsNote(job.comparison.weighting_type);
      }
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, POLL_MS));
  }
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const data = new FormData(form);
  const halfLifeText = halfLifeError(data.get("weightingType"), data.get("halfLife"));
  showHalfLifeError(halfLifeText);
  if (halfLifeText) return;
  const body = requestFromForm(
    {
      ecosystem: data.get("ecosystem"),
      package: data.get("package"),
      startDate: data.get("startDate"),
      endDate: data.get("endDate"),
      weightingType: data.get("weightingType"),
      halfLife: data.get("halfLife"),
    },
    "", // Ticket #10 adds the Turnstile token.
  );
  submitButton.disabled = true;
  showRows([]);
  showResultsNote(null);
  showStatus("Sending");
  try {
    const response = await fetch(`${hostUrl}/api/jobs`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!response.ok) throw new Error(`POST returned ${response.status}`);
    const { job_id: jobId } = await response.json();
    await followJob(jobId);
  } catch (error) {
    console.error(error);
    showStatus("Something went wrong. Try again later.");
  } finally {
    submitButton.disabled = false;
  }
});
