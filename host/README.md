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
| `dmhost/limits.py` | `SubmitLimits` (the per-IP limits and the queue cap) and `IpHasher` (the keyed hash of an IP address). |
| `dmhost/turnstile.py` | `TurnstileVerifier`: the Cloudflare Turnstile check. It takes the HTTP call as `post=`, so that tests need no network. |
| `dmhost/client_ip.py` | `client_ip()`: the address of the user behind Caddy. |
| `Dockerfile`, `compose.yaml`, `Caddyfile`, `.env.example` | The deploy to the Oracle VM: the image, the services (API, workers, Caddy, DuckDNS), the HTTPS front, and the configuration values. |
| `RUNBOOK.md`, `wizard.sh`, `vm-setup.sh`, `deploy.sh`, `disk-check.sh` | The deploy steps. `RUNBOOK.md` lists the human steps and each deploy; `bash host/wizard.sh` walks a person through the first setup. |
| `tests/` | The Host tests. They use FastAPI's `TestClient`, run the queue and the worker in the test process, and need no network. |

`contract/examples/` (at the repo root) holds example API bodies. The Host tests and the Webapp tests
both read them.

## API

- `GET /api/health` returns `200 {"status":"ok"}` when the API can read the Job queue, and
  `503 {"status":"unavailable"}` when it cannot. The uptime monitor checks it. It does one cheap
  read, and the body shows no path, error text, or version, because the URL is public.
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
- Abuse protection: `POST /api/jobs` first checks `turnstile_token` with Cloudflare Siteverify. A
  refused token gets `403`. After the retention delete and the reuse lookup (below), one SQLite
  transaction checks the limits and adds the Job (`JobQueue.admit`):
  - 1 active (waiting or running) Job for each IP address, and 10 Jobs for each IP address in each
    hour. A submit over a limit gets `429` with `Too many requests, try again in N min` and a
    `Retry-After` header. For the hourly limit, N is the time until the oldest Job leaves the hour.
    For the active limit, the Host cannot know when the Job ends, so N is 1.
  - 20 waiting Jobs for all users. A submit over the cap gets `503` with `The server is busy, try later`.

  The Host stores only an HMAC-SHA256 hash of the IP address (key `DM_HOST_IP_HASH_KEY`), in the
  table `submitters`. It removes each hash one hour after the submit, or when the Job ends if the
  Job is still active then. SQLite overwrites removed rows (`secure_delete`), and the API
  truncates the WAL file after a removal, so the hash does not stay in the file. Do not turn on the
  uvicorn access log in production (`--no-access-log`), because it writes each client address.
- Client IP: the API reads `X-Forwarded-For` only when the TCP peer is a trusted proxy
  (`DM_HOST_TRUSTED_PROXIES`, the Caddy container). It then takes the rightmost address that is
  not a trusted proxy, because Caddy adds the address of its peer at the end and a user can put
  false addresses only in front of it. A request from any other peer uses the peer address.
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
2. Start the API on port 8080, with the Cloudflare test secret key that always passes:
   `DM_HOST_TURNSTILE_SECRET=1x0000000000000000000000000000000AA PYTHONPATH=host uvicorn --factory dmhost.app:create_app_from_env --port 8080`
   The Webapp uses the matching test site key `1x00000000000000000000AA`. The Turnstile check
   calls Cloudflare, so the local run needs the network.
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
| `DM_HOST_ALLOWED_ORIGINS` | `http://localhost:8000` | Comma-separated Webapp origins that CORS allows. In production: `https://imranur-rahman.github.io` only |
| `DM_HOST_WORKERS` | `6` | The number of workers. Each worker runs one Job at a time |
| `DM_HOST_JOB_TIMEOUT_SECONDS` | `900` | The time after which a running Job stops with "The analysis took too long" |
| `DM_HOST_WORKER_MEMORY_MB` | `2048` | The RSS limit of the child process of one Job, in MB (`0` means no limit) |
| `DM_HOST_TURNSTILE_SECRET` | none (the API does not start without it) | The Turnstile secret key. Keep it only on the instance, never in the repo |
| `DM_HOST_MAX_ACTIVE_JOBS_PER_IP` | `1` | Waiting and running Jobs for each IP address |
| `DM_HOST_MAX_JOBS_PER_IP_PER_HOUR` | `10` | Jobs for each IP address in each hour |
| `DM_HOST_MAX_WAITING_JOBS` | `20` | Waiting Jobs for all users |
| `DM_HOST_IP_HASH_KEY` | a random key at each start | The key of the IP hash. Without it, a restart resets the limits |
| `DM_HOST_TRUSTED_PROXIES` | none | Comma-separated addresses or CIDR networks of the proxies whose `X-Forwarded-For` the API reads (the Caddy container, for example the Docker network) |

`RUNBOOK.md` describes the production deploy.

The Webapp sends its requests to the URL in the `data-host-url` attribute of the `#try-it` section in
`webapp/site/index.html` (`http://localhost:8080` by default). The Turnstile site key is the
`data-sitekey` attribute of the `.cf-turnstile` widget in the same file. The Pages workflow
puts the production values (repository variables `DM_HOST_URL` and `DM_TURNSTILE_SITE_KEY`)
into the published copy, with `webapp/configure.mjs`.

## Checks

CI (`.github/workflows/host.yml`) runs these checks when a file in `host/`, `contract/`, or
`dependency_metrics/` changes:

```bash
black --check host/
flake8 host/ --max-line-length=100
pytest host/tests
```
