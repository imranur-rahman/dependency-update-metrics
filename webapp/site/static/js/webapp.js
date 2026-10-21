// Page code of the Webapp section ("Try It"). It reads the form, sends the Comparison
// to the Host, and shows the result. All logic that has no DOM code is in comparison.js.
import { requestFromForm, describeJob, isFinished, resultRows } from "./comparison.js";

const POLL_MS = 2000;

const section = document.getElementById("try-it");
const form = document.getElementById("comparison-form");
const statusLine = document.getElementById("job-status");
const table = document.getElementById("result-table");
const submitButton = document.getElementById("submit-comparison");
const hostUrl = section.dataset.hostUrl.replace(/\/$/, "");

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
      if (job.state === "done") showRows(resultRows(job));
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, POLL_MS));
  }
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const data = new FormData(form);
  const body = requestFromForm(
    {
      ecosystem: data.get("ecosystem"),
      package: data.get("package"),
      startDate: data.get("startDate"),
      endDate: data.get("endDate"),
    },
    "", // Ticket #10 adds the Turnstile token.
  );
  submitButton.disabled = true;
  showRows([]);
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
