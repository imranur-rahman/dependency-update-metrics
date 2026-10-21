# Host runbook

This runbook deploys the Host to an Oracle Always Free Ampere A1 VM (ticket #15, spec #1).
The Webapp on GitHub Pages then calls the Host over HTTPS.

`host/wizard.sh` walks you through the first setup, stage by stage. Run it on your laptop
from the repo root: `bash host/wizard.sh`. The sections below give the same steps by hand,
and the steps for each later deploy.

## What runs on the VM

`host/compose.yaml` starts these services from the `host/` folder:

| Service | What it does |
|---|---|
| `api` | uvicorn with `--no-access-log`, on port 8080 inside the Docker network only |
| `worker` | the worker pool (`python -m dmhost.worker`, `DM_HOST_WORKERS` workers) |
| `caddy` | HTTPS on ports 80 and 443 for `<subdomain>.duckdns.org`, with automatic Let's Encrypt certificates and no access log |
| `duckdns` | sets the DuckDNS record to the public IP of the VM every 10 minutes |
| `ballast` | holds idle memory, so that memory use stays at 25% of the RAM and Oracle does not reclaim the VM (ADR 0001, see "Idle protection") |

The image (`host/Dockerfile`) is `python:3.12-slim`. It installs the Tool and the Host from
the commit that you deploy. The named volumes keep the data across deploys:

| Volume | Path in the containers | What it holds |
|---|---|---|
| `queue` | `/data/queue` | the Job queue (`jobs.db`) |
| `osv-index` | `/data/tool` | the Tool output directory: the OSV index (`osv_database.parquet`) |
| `resolver-cache` | `/data/tool/cache` | the ResolverCache (`cache.db`) |
| `caddy-data` | `/data` (Caddy) | the certificates |

The API trusts `X-Forwarded-For` only from the Caddy container, which has the fixed address
`172.30.0.10`. Caddy writes no access log, and its error log drops the client address. So
the Host never stores a plain IP.

## Secrets

The Turnstile secret key, the DuckDNS token, the IP hash key, and the Healthchecks.io ping
URL are only in `host/.env` on the VM (mode 600). Do not commit `host/.env`; `.gitignore`
excludes it. `host/.env.example` lists every value.

The Webapp needs two public values. The Pages workflow reads them from repository variables
(Settings > Secrets and variables > Actions > Variables) and puts them into the published
homepage (`webapp/configure.mjs`):

- `DM_HOST_URL`: `https://<subdomain>.duckdns.org`, with no trailing slash.
- `DM_TURNSTILE_SITE_KEY`: the Turnstile site key.

The workflow fails if a variable is missing, so a deploy never ships the local values
(`http://localhost:8080` and the Turnstile test key).

## First setup (human steps)

1. **Oracle account.** Sign up at https://signup.cloud.oracle.com/. Keep the account on the
   free tier. Do not upgrade to Pay As You Go (ADR 0001).
2. **VM.** Create an instance (Compute > Instances > Create):
   - Image: Canonical Ubuntu 24.04, the aarch64 build.
   - Shape: VM.Standard.A1.Flex with 4 OCPU and 24 GB memory.
   - Networking: a public subnet with a public IPv4 address.
   - Boot volume: the default 47 GB.
   - SSH key: your public key.

   If the console shows "Out of capacity", try another availability domain.
3. **Security list.** Open the subnet of the instance, then its Default Security List. Add two
   ingress rules from `0.0.0.0/0`: TCP port 80 and TCP port 443.
4. **Instance setup.** SSH to the VM, clone the repo, and run the setup script:

   ```bash
   git clone https://github.com/imranur-rahman/dependency-update-metrics.git
   cd dependency-update-metrics
   bash host/vm-setup.sh
   ```

   The script installs Docker, opens ports 80 and 443 in the instance firewall (iptables), and
   turns swap off. It also installs two cron jobs: the disk alert (every 15 minutes) and the
   weekly maintenance (every Sunday at 04:00 UTC). Log out and in again
   after it, so that your user can run `docker`.

   Oracle's Ubuntu images block ports 80 and 443 in iptables. So you need step 3 AND this
   step; one alone does not open the ports.
5. **Keep swap off.** Do not add swap to the VM, now or later. ADR 0001 needs a VM with no
   swap. The ballast holds idle memory, and with swap the kernel moves those idle pages to
   the swap device. Then memory use falls below Oracle's idle threshold, and Oracle can
   reclaim the VM. Check with `swapon --show`: the command must show nothing.
6. **DuckDNS.** Sign in at https://www.duckdns.org/ and add a subdomain. Set its IP to the
   public IP of the VM. Copy the token.
7. **Turnstile.** In the Cloudflare dashboard, open Turnstile and add a widget for the
   hostname `imranur-rahman.github.io` in Managed mode. Copy the site key and the secret key.
8. **Disk alert and maintenance alert.** Sign up at https://healthchecks.io/ and add a check
   with a period of 15 minutes and a grace time of 30 minutes. Copy its ping URL to
   `DM_HOST_DISK_CHECK_URL`. `host/disk-check.sh` pings it every 15 minutes, and pings
   `<url>/fail` when the disk is 80% full or more. Add a second check with a period of 7 days
   and a grace time of 1 day. Copy its ping URL to `DM_HOST_MAINTENANCE_CHECK_URL`.
   `host/weekly-maintenance.sh` pings it after each run, and pings `<url>/fail` when a step
   fails.
9. **host/.env.** On the VM, copy `host/.env.example` to `host/.env`. Fill in each value from
   steps 6 to 8. Make the IP hash key once with `openssl rand -hex 32` and keep it, because a
   new key resets the per-IP limits. Then run `chmod 600 host/.env`.
10. **Deploy.** Run `bash host/deploy.sh` on the VM (see "Each deploy").
11. **OSV index.** Without it, every MTTR is 0. The build downloads about 2.4 GB:

    ```bash
    cd host
    docker compose run --rm worker dependency-metrics --build-osv --output-dir /data/tool
    ```

12. **Check HTTPS.** From your laptop, run `curl https://<subdomain>.duckdns.org/api/health`.
    The answer must be `{"status":"ok"}`.
13. **Webapp.** Set the repository variables `DM_HOST_URL` and `DM_TURNSTILE_SITE_KEY`, then
    run the "Webapp CI and Pages" workflow on `main`. Open the public homepage and run one
    full Comparison.
14. **Uptime monitor.** In UptimeRobot (free), add an HTTP(s) monitor for
    `https://<subdomain>.duckdns.org/api/health` with a 5-minute interval and an e-mail alert.
15. **One week later: check the OCI memory metric.** Do this one time, 7 days after the
    first deploy, because Oracle measures the idle rule over 7 days. In the OCI console, open
    the instance, then Metrics, then "Memory Utilization". Set the interval to 1 hour, the
    statistic to P95, and the window to the last 7 days. The value must be above 20% for the
    whole week. If it is not, check the ballast (see "Idle protection").

## Each deploy

Deploy by hand on the VM until the Host is stable:

```bash
cd ~/dependency-update-metrics
bash host/deploy.sh
```

`deploy.sh` does these steps:

1. It runs `git pull --ff-only` on the current branch.
2. It runs `docker compose up -d --build`, so the image installs the Tool from the new commit.
3. It runs `docker image prune -f`, because each build leaves the old image and the boot
   volume is only 47 GB.
4. It waits until `GET /api/health` answers.

A deploy stops the workers. A Job that runs at that time ends with "Something went wrong".
So deploy when `docker compose logs --tail 20 worker` shows no running Job.

## Checks and troubleshooting

- Health: `GET /api/health` returns `200 {"status":"ok"}` when the API can read the Job queue,
  and `503 {"status":"unavailable"}` when it cannot. The body never shows paths or versions.
- Logs: `docker compose logs --tail 100 api worker caddy` from `host/`.
- Disk: `df -h /` and `docker system df`. To free space, run `docker builder prune -f`.
- No certificate: check that the DuckDNS record shows the VM IP (`dig +short
  <subdomain>.duckdns.org`), and that ports 80 and 443 are open in both the security list and
  iptables (`sudo iptables -L INPUT -n --line-numbers`).
- The Webapp shows a network error: check that `DM_HOST_ALLOWED_ORIGINS` in `host/.env` is
  `https://imranur-rahman.github.io`, and that the `DM_HOST_URL` variable has no trailing slash.

## Idle protection (the ballast)

Oracle can reclaim an Always Free A1 instance when its CPU, network, and memory use all
stay below 20% at the 95th percentile over 7 days (ADR 0001). The `ballast` service
(`dmhost/ballast.py`) keeps the memory use at 25% of the RAM:

- Every 2 seconds, it reads `MemTotal` and `MemAvailable` from `/proc/meminfo`. A container
  sees the values of the whole VM, so the API, the workers, and the Job child processes all
  count, with no per-process bookkeeping.
- It holds only the gap between 25% of the RAM and the use of the rest of the VM, in chunks
  of 64 MB. It writes to each page of each chunk, so the pages count as used memory.
- When the workers use more, the gap gets smaller, and the ballast releases chunks. When
  the workers use more than 25% of the RAM, the ballast holds nothing.
- It has `oom_score_adj: 1000`. If memory runs out before the ballast releases its chunks,
  the kernel stops the ballast first, not a Job. Docker then starts it again
  (`restart: always`).

Do not remove the ballast as "unused", and do not add swap (step 5). Checks:

- `docker compose logs --tail 20 ballast` shows the size that the ballast holds now.
- `free -m` on a quiet VM shows about 25% of the RAM as used.

## Weekly maintenance

Cron runs `host/weekly-maintenance.sh` every Sunday at 04:00 UTC (`host/vm-setup.sh` installs
the entry, `crontab -l` shows it). The script runs `python -m dmhost.maintenance` in a worker
container, with these steps:

1. `dependency-metrics --build-osv --output-dir /data/tool` rebuilds the OSV index from the
   npm, PyPI, and crates.io zips. The Tool writes the new index to a temporary file and
   renames it, so a failed rebuild keeps the old index. A new index also stops the reuse of
   older results (the reuse key holds the index date).
2. `dependency-metrics --cache-prune-gb 10 --output-dir /data/tool` prunes the
   ResolverCache to 10 GB. This step runs also after a failed rebuild, because the cache
   size does not depend on the index.

The script exits with 1 when a step fails, and pings `DM_HOST_MAINTENANCE_CHECK_URL/fail`.
The log is `~/dmhost-maintenance.log` on the VM. To run the maintenance by hand, run
`bash host/weekly-maintenance.sh`.
