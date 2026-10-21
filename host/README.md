# Host

The Host is the server that runs the Tool for Webapp users: the API, the Job queue, and the workers.
See `GLOSSARY.md` and spec #1.

## Layout

| File | What it holds |
|---|---|
| `dmhost/app.py` | The FastAPI app. `create_app(queue=, analysis=, turnstile_check=, allowed_origins=)` takes its dependencies as arguments, so that tests can give fakes. `create_app_from_env()` builds the production app. |
| `dmhost/job_queue.py` | `JobQueue`: the SQLite table that holds the Jobs. The API and the workers share it. |
| `dmhost/worker.py` | `Worker`: takes the oldest waiting Job, runs the analysis for each package, and stores the result. With `JobLimits`, it runs each Job in a new child process and stops the child on cancel, on the timeout, or over the RSS limit. `WorkerPool` runs several workers in parallel. `python -m dmhost.worker` starts the pool. |
| `dmhost/events.py` | The event stream of a Job (`GET /api/jobs/{id}/events`). `EventStreamSettings` holds its timing; `create_app(event_stream=)` takes it. |
| `dmhost/reuse.py` | Result reuse and retention: `reuse_key`, `find_reusable_job`, `delete_expired_jobs`, and `ReusePolicy` (72 hours, 30 days). |
| `dmhost/tool_analysis.py` | `ToolAnalysis`: the analysis function of the workers. It runs the Tool, always with deps.dev data. |
| `dmhost/models.py` | The request and response shapes of the API. |
| `dmhost/settings.py` | The configuration values, read from environment variables. |
| `tests/` | The Host tests. They use FastAPI's `TestClient`, run the queue and the worker in the test process, and need no network. |

`contract/examples/` (at the repo root) holds example API bodies. The Host tests and the Webapp tests
both read them.

## API

- `POST /api/jobs` takes a Comparison (`ecosystem`, `packages`, `start_date`, `end_date`, `mode`,
  `weighting_type`, `half_life`) and `turnstile_token`. It returns `202` with `{"job_id": ...}`.
  `ecosystem` is `npm`, `pypi`, or `cargo`, and all packages are from it. `packages` holds 1 to 5
  names with no duplicates. Two names are duplicates when they differ only in case (and, for PyPI,
  in runs of `-`, `_`, and `.`).
  `weighting_type` is `disable`, `linear`, `exponential`, or `inverse`. Exponential weighting needs a
  `half_life` from 1 to 3650 days; the other types drop `half_life`. Each name matches the naming
  rules of its ecosystem (`PACKAGE_NAME_RULES` in `dmhost/models.py`). The `end_date` is after the
  `start_date` and not later than the latest date in any time zone (UTC+14). The Host does not limit
  the window length. A body that breaks a rule gets `422`.
- Result reuse: if a done Job with the same reuse key finished in the last 72 hours, `POST /api/jobs`
  returns `200` with the body of `GET /api/jobs/{id}` for that Job, and starts no Job. The reuse key is
  the ecosystem, the set of packages (order, case, and PyPI separators do not matter, see
  `package_key()`), the window, the mode, the weighting type, the half-life, and the build date of the
  OSV index (`OSVBuilder.index_date()`). So a rebuild of the OSV index stops the reuse of older
  results. A Job that ended in `error` or `cancelled` is not reused. The rate limits run after the
  reuse lookup, so a reused result does not count against them.
- Retention: each `POST /api/jobs` first deletes the Job records that were submitted more than 30
  days ago. A `GET` for a deleted Job returns `404`.
- `GET /api/jobs/{id}` returns the Job: `queued` with `position`, `running` with `package_index`
  (1-based) and `package_count`, `done` with `results` (one Tool result for each package), or `error` with `message`. The worker runs the packages in
  sequence. A package whose analysis raises gets a result with `error` set, and the other packages
  keep their results. The Job is `error` only when every package raised. If all packages failed
  for one reason, the Job `message` is that reason. Otherwise it is the generic message.
- Error messages: the page shows only the six messages in `dmhost/errors.py`. The Host logs the full
  error under the job ID (`Job <id>: package <name> failed: <error>`). The Tool reports an unknown
  package and a window without a release in the same way (`DepsDevResolver` turns a failed
  GetPackage request into an empty version list). So `ToolAnalysis` asks deps.dev for the package
  again: a `404` raises `PackageNotFound`, and a listing with no release in the window raises
  `NoReleasesInWindow`. Every other Tool error gets `Something went wrong (ref: <job id>)`.
- `DELETE /api/jobs/{id}` cancels a Job that waits or runs, and returns the Job (`cancelled`). A waiting
  Job leaves the queue. A running Job stops within one worker poll (0.5 seconds). A Job in a final
  state does not change. A Job continues when its event stream closes, for example when the user
  closes the tab.
- A Job that runs longer than the timeout (15 minutes) gets `error` with the message
  "The analysis took too long". A Job whose child process goes over the RSS limit (2 GB) gets the
  "Something went wrong" message. The check is the one of the Tool's `--max-worker-memory-mb`
  (`psutil` RSS), and the Host logs the RSS under the job ID.
- `GET /api/jobs/{id}/events` sends the same bodies as server-sent events. Each event has the state as
  its name (`queued`, `running`, `done`, `error`, or `cancelled`). The stream sends the current state
  first, then one event each time the body changes, and it ends after a final state. It sends a
  heartbeat comment (`: heartbeat`) after 15 seconds with no event. It also ends after 5 minutes,
  because the browser connects again by itself and gets the current state first.

## Run the Host and the Webapp locally

Run each command from the repo root, in the venv.

1. Install the Tool and the Host dependencies:
   `pip install -e . && pip install -r host/requirements.txt`
2. Start the API on port 8080:
   `PYTHONPATH=host uvicorn --factory dmhost.app:create_app_from_env --port 8080`
3. In a second terminal, start one worker:
   `PYTHONPATH=host python -m dmhost.worker`
4. In a third terminal, serve the Webapp on port 8000:
   `python -m http.server -d webapp/site 8000`
5. Open http://localhost:8000/#try-it, enter 1 to 5 packages, and click "Compute".

The API and the worker keep their data in `host-data/` (the Job queue in `host-data/jobs.db`, and the
Tool cache and OSV index in `host-data/tool/`). Without an OSV index, every MTTR is 0. To build the
index, run `dependency-metrics --build-osv --output-dir host-data/tool`.

Environment variables change the defaults:

| Variable | Default | Meaning |
|---|---|---|
| `DM_HOST_DB` | `host-data/jobs.db` | The SQLite file of the Job queue |
| `DM_HOST_OUTPUT_DIR` | `host-data/tool` | The Tool output directory (cache and OSV index) |
| `DM_HOST_ALLOWED_ORIGINS` | `http://localhost:8000` | Comma-separated Webapp origins that CORS allows |
| `DM_HOST_WORKERS` | `6` | The number of workers. Each worker runs one Job at a time |
| `DM_HOST_JOB_TIMEOUT_SECONDS` | `900` | The time after which a running Job stops with "The analysis took too long" |
| `DM_HOST_WORKER_MEMORY_MB` | `2048` | The RSS limit of the child process of one Job, in MB (`0` means no limit) |

The Webapp sends its requests to the URL in the `data-host-url` attribute of the `#try-it` section in
`webapp/site/index.html` (`http://localhost:8080` by default).

## Checks

CI (`.github/workflows/host.yml`) runs these checks when a file in `host/`, `contract/`, or
`dependency_metrics/` changes:

```bash
black --check host/
flake8 host/ --max-line-length=100
pytest host/tests
```
