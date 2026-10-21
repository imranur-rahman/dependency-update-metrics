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
| `ballast` | ticket #16 adds it (ADR 0001) |

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
   turns swap off. It also installs the cron job of the disk alert. Log out and in again
   after it, so that your user can run `docker`.

   Oracle's Ubuntu images block ports 80 and 443 in iptables. So you need step 3 AND this
   step; one alone does not open the ports.
5. **Keep swap off.** ADR 0001 needs a VM with no swap. The ballast (#16) holds idle memory,
   and with swap the kernel moves those pages out, so memory use falls below Oracle's idle
   threshold. Check with `swapon --show`: the command must show nothing. Do not add a swap
   file later.
6. **DuckDNS.** Sign in at https://www.duckdns.org/ and add a subdomain. Set its IP to the
   public IP of the VM. Copy the token.
7. **Turnstile.** In the Cloudflare dashboard, open Turnstile and add a widget for the
   hostname `imranur-rahman.github.io` in Managed mode. Copy the site key and the secret key.
8. **Disk alert.** Sign up at https://healthchecks.io/ and add a check with a period of 15
   minutes and a grace time of 30 minutes. Copy its ping URL. `host/disk-check.sh` pings it
   every 15 minutes, and pings `<url>/fail` when the disk is 80% full or more.
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
