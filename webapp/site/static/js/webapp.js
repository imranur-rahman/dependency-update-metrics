// Page code of the Webapp section ("Try It"). It reads the form, sends the Comparison
// to the Host, and shows the result. All logic that has no DOM code is in comparison.js.
import {
  requestFromForm,
  describeJob,
  isFinished,
  resultRows,
  progressOf,
  jobIdFromUrl,
  urlWithJobId,
  JOB_EVENTS,
} from "./comparison.js";

const section = document.getElementById("try-it");
const form = document.getElementById("comparison-form");
const statusLine = document.getElementById("job-status");
const progressBar = document.getElementById("job-progress");
const table = document.getElementById("result-table");
const submitButton = document.getElementById("submit-comparison");
const hostUrl = section.dataset.hostUrl.replace(/\/$/, "");

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
  if (job.state === "done") showRows(resultRows(job));
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

resumeJobFromUrl();
