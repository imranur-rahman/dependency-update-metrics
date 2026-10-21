# Host

The Host is the server that runs the Tool for Webapp users: the API, the Job queue, and the workers.
See `GLOSSARY.md` and spec #1.

## Layout

| File | What it holds |
|---|---|
| `dmhost/app.py` | The FastAPI app. `create_app(queue=, analysis=, turnstile_check=, allowed_origins=)` takes its dependencies as arguments, so that tests can give fakes. `create_app_from_env()` builds the production app. |
| `dmhost/job_queue.py` | `JobQueue`: the SQLite table that holds the Jobs. The API and the workers share it. |
| `dmhost/worker.py` | `Worker`: takes the oldest waiting Job, runs the analysis for each package, and stores the result. `python -m dmhost.worker` starts one worker process. |
| `dmhost/events.py` | The event stream of a Job (`GET /api/jobs/{id}/events`). `EventStreamSettings` holds its timing; `create_app(event_stream=)` takes it. |
| `dmhost/tool_analysis.py` | `ToolAnalysis`: the analysis function of the workers. It runs the Tool, always with deps.dev data. |
| `dmhost/models.py` | The request and response shapes of the API. |
| `dmhost/settings.py` | The configuration values, read from environment variables. |
| `tests/` | The Host tests. They use FastAPI's `TestClient`, run the queue and the worker in the test process, and need no network. |

`contract/examples/` (at the repo root) holds example API bodies. The Host tests and the Webapp tests
both read them.

## API

- `POST /api/jobs` takes a Comparison (`ecosystem`, `packages`, `start_date`, `end_date`, `mode`,
  `weighting_type`, `half_life`) and `turnstile_token`. It returns `202` with `{"job_id": ...}`.
- `GET /api/jobs/{id}` returns the Job: `queued` with `position`, `running` with `package_index`
  (1-based) and `package_count`, `done` with `results` (one Tool result for each package), or `error`
  with `message`.
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
5. Open http://localhost:8000/#try-it, enter a package, and click "Compute".

The API and the worker keep their data in `host-data/` (the Job queue in `host-data/jobs.db`, and the
Tool cache and OSV index in `host-data/tool/`). Without an OSV index, every MTTR is 0. To build the
index, run `dependency-metrics --build-osv --output-dir host-data/tool`.

Environment variables change the defaults:

| Variable | Default | Meaning |
|---|---|---|
| `DM_HOST_DB` | `host-data/jobs.db` | The SQLite file of the Job queue |
| `DM_HOST_OUTPUT_DIR` | `host-data/tool` | The Tool output directory (cache and OSV index) |
| `DM_HOST_ALLOWED_ORIGINS` | `http://localhost:8000` | Comma-separated Webapp origins that CORS allows |

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
