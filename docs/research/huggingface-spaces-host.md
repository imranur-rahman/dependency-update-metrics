# Hugging Face Docker Space as the Host

Research date: 2026-10-21. Sources: Hugging Face docs, pricing pages, changelog, terms, content policy, and the `huggingface_hub` source (commit `e1f595d`, 2026-10-21). Forum posts appear only where no primary source exists, and this file marks them as forum posts.

## Summary

The largest finding: **a free account cannot create a Docker Space on CPU Basic today.** The current docs, the `huggingface_hub` docstrings, and the PRO page all say this. So the Host on Hugging Face costs money. The cheapest paths are PRO ($9/month) on CPU Basic, or CPU Upgrade ($0.03/hour, about $22/month) with only billing set up.

| # | Question | Fit for our Host |
|---|---|---|
| 1 | Free account, CPU Basic | **No fit for free.** Docker on CPU Basic needs PRO. The hardware (2 vCPU, 16 GB RAM, 50 GB disk) fits our 750 MB peak well. |
| 2 | Disk | **Partial fit.** The local disk is ephemeral. The old persistent storage is gone. Storage Buckets mount as volumes, but the mount is eventually consistent, so it is a poor fit for the SQLite job queue. |
| 3 | Sleep | **Partial fit.** CPU Basic sleeps after 48 h without use. A visitor wakes it. The docs give no wake time. Pings to stay awake are risky (one Space got flagged). |
| 4 | Docker specifics | **Fit.** One exposed port (`app_port`, default 7860), UID 1000, many internal ports and processes allowed. |
| 5 | Secrets and variables | **Fit.** Both arrive as environment variables at runtime. Build time needs `--mount=type=secret`. |
| 6 | Outbound network | **Fit.** Ports 80, 443, and 8080 are open. deps.dev and storage.googleapis.com use 443. No egress quota is documented. |
| 7 | Inbound | **Unknown.** HTTPS on `https://<subdomain>.hf.space`. The docs say nothing about `X-Forwarded-For`, proxy timeouts, SSE, or WebSockets. |
| 8 | CORS | **Probable fit.** The proxy can add only COEP, COOP, and CORP headers. The docs do not say that it strips the app's own CORS headers. Test it. |
| 9 | Deploy | **Fit.** The official `huggingface/hub-sync` action has a `subdirectory` input. Each push rebuilds and restarts. Each secret change restarts. |
| 10 | Limits and policy | **Uncertain.** The terms and content policy do not ban a non-ML API. The terms frame the service as "in the field of machine learning". No rule names keep-alive pings, but the content policy bans "excessive bulk activity". |

The five findings that most affect the design:

1. Docker on CPU Basic needs PRO. There is no free Docker Space.
2. The disk is ephemeral. Every restart, rebuild, or sleep loses the SQLite queue, the cache, and the OSV index.
3. A bucket volume is eventually consistent ("stale for up to 10 s", "no cross-node file locking"). Keep SQLite on local disk and accept the loss, or copy snapshots to a bucket.
4. The docs do not document the inbound proxy. We cannot yet know which `X-Forwarded-For` hop to trust, or whether SSE streams survive 15 minutes.
5. CPU Basic sleeps after 48 h idle, and the docs disagree on whether this is "sleep" (a visitor wakes it) or "pause" (only the owner restarts it).

## 1. Can a free account create a Docker Space on CPU Basic?

**No.** All current primary sources agree.

- Spaces Overview: "Static Spaces are free for everyone. Gradio and Docker Spaces run on compute and require a paid plan to create: PRO for personal accounts, Team or Enterprise for organizations. Free personal accounts in good standing can still host up to 2 Gradio Spaces running on ZeroGPU." ([spaces-overview](https://huggingface.co/docs/hub/spaces-overview))
- The same page, under hardware: "The default CPU Basic hardware has no hourly cost, but creating a Space that runs on compute (Gradio or Docker) requires a paid plan". ([spaces-overview#hardware-resources](https://huggingface.co/docs/hub/spaces-overview#hardware-resources))
- `huggingface_hub` guide: "on the free `cpu-basic` hardware, they require a subscription (PRO for users, Team or Enterprise for organizations), while paid hardware only requires billing (a payment method and prepaid credits). Without a subscription, creating, duplicating or moving a Gradio or Docker Space on `cpu-basic` fails with a payment error (HTTP 402)." ([manage-spaces](https://huggingface.co/docs/huggingface_hub/guides/manage-spaces))
- The `create_repo` docstring in `hf_api.py` (2026-10-21) says the same, and a comment reads "402 -> if payment required (e.g. if Gradio/Docker Space and free user)". ([huggingface_hub source](https://github.com/huggingface/huggingface_hub/blob/main/src/huggingface_hub/hf_api.py))
- PRO page: "Host up to 10, plus Gradio & Docker Spaces on compute." Price: "$9/month". ([pro](https://huggingface.co/pro))
- Billing docs list a PRO benefit: "Ability to create Gradio and Docker Spaces running on compute". ([billing](https://huggingface.co/docs/hub/billing))

**The conflict.** The pricing page lists CPU Basic as "FREE" and does not state the plan rule. ([pricing](https://huggingface.co/pricing)) "Free" means no hourly cost. It does not mean that a free account can create the Space. The Spaces changelog has no entry for this change. ([spaces-changelog](https://huggingface.co/docs/hub/spaces-changelog)) Forum posts date the rollout to about 2026-07-08 and quote a CLI error: "hosting Gradio and Docker Spaces on free cpu-basic requires a PRO subscription". ([forum thread, not official](https://discuss.huggingface.co/t/docker-sdk-now-marked-as-paid-when-creating-a-new-space/177580)) So the docs pages that say "paid plan" are the newer and correct source. Older pages and older blog posts that say Docker Spaces are free predate this change.

**Two paid options:**

- PRO ($9/month) plus CPU Basic (free per hour).
- No subscription, billing only, plus CPU Upgrade at $0.03/hour (8 vCPU, 32 GB). That is about $22/month if it runs all month. ([spaces-gpus](https://huggingface.co/docs/hub/spaces-gpus))

PRO also includes "monthly compute credits, usable across ... Spaces upgraded hardware". ([billing](https://huggingface.co/docs/hub/billing)) The amount is not on the pages I read.

**CPU Basic hardware:** 2 vCPU, 16 GB RAM, 50 GB disk, free per hour. ([spaces-gpus](https://huggingface.co/docs/hub/spaces-gpus)) The overview adds: "Each Spaces environment is limited to 16GB RAM, 2 CPU cores and 50GB of (not persistent) disk space by default." ([spaces-overview](https://huggingface.co/docs/hub/spaces-overview)) With a 750 MB peak per analysis, 16 GB holds many workers in memory. The 2 vCPUs limit the real parallelism, so CPU is the bottleneck, not RAM. The free-tier memory ballast from `docs/adr/0001-free-tier-memory-ballast.md` is likely not needed here, because the docs give no idle-reclaim rule for RAM.

## 2. Disk

**The disk is ephemeral.** "This disk space is ephemeral, meaning its content will be lost if your Space restarts or is stopped." ([spaces-storage](https://huggingface.co/docs/hub/spaces-storage)) The Docker page says: "The data written on disk is lost whenever your Docker Space restarts." ([spaces-sdks-docker](https://huggingface.co/docs/hub/spaces-sdks-docker))

**What survives:** only the image (what the Dockerfile builds) and attached volumes. A restart, a rebuild, a config change, and a sleep all lose the runtime disk. A factory reboot also rebuilds "from scratch without caching any requirements". ([hf_api.py `restart_space`](https://github.com/huggingface/huggingface_hub/blob/main/src/huggingface_hub/hf_api.py))

**The old persistent storage is gone.** The config reference says: "The persistent storage feature is no longer available so this setting will be ignored." ([spaces-config-reference](https://huggingface.co/docs/hub/spaces-config-reference)) A forum post from a Hugging Face account (2026-03-27) says: "We've sunset persistent storage for Spaces." ([forum](https://discuss.huggingface.co/t/spaces-persistent-storage-upgrade-not-accessible/171226/16))

**Storage Buckets replace it.** The changelog entry of 2026-03-31: "You can now mount HF Buckets as persistent storage volumes directly in your Spaces." ([changelog](https://huggingface.co/changelog/storage-buckets-for-spaces)) You pick the mount path, for example `/data`. ([manage-spaces](https://huggingface.co/docs/huggingface_hub/guides/manage-spaces)) "`/data` volume is only available at runtime". ([spaces-sdks-docker](https://huggingface.co/docs/hub/spaces-sdks-docker)) "Buckets are available to all users and organizations". ([storage-buckets](https://huggingface.co/docs/hub/storage-buckets))

**Bucket prices.** Buckets follow the account storage limits. A free account gets "Best-effort" public storage and 100 GB private storage. ([storage-limits](https://huggingface.co/docs/hub/storage-limits)) Above the free tier, the price is $12/TB/month public and $18/TB/month private. ([pricing](https://huggingface.co/pricing)) Our data (one SQLite file plus caches, a few GB) fits in the free tier.

**Bucket semantics matter for SQLite.** The platform mount is "the same idea as `hf-mount`". ([storage-buckets-access](https://huggingface.co/docs/hub/storage-buckets-access)) The `hf-mount` README says:

- "hf-mount provides **eventual consistency** with remote changes."
- Streaming mode is append-only. Random writes need advanced mode. "A crash before flush completes means data loss."
- Not recommended for a "General-purpose networked filesystem (no multi-writer support, no cross-node file locking)".

([hf-mount](https://github.com/huggingface/hf-mount)) The README does not mention SQLite. SQLite needs random writes and reliable locks. So a live SQLite file on a bucket mount is a risk. The docs do not say which write mode the Space mount uses.

## 3. Sleep

**When:** "If your Space runs on the default `cpu-basic` hardware, it will go to sleep if inactive for more than a set time (currently, 48 hours)." ([spaces-gpus#sleep-time](https://huggingface.co/docs/hub/spaces-gpus#sleep-time)) The `huggingface_hub` source says the value "is fixed to 48 hours of inactivity" and "is not configurable". ([hf_api.py](https://github.com/huggingface/huggingface_hub/blob/main/src/huggingface_hub/hf_api.py))

**How it wakes:** "Anyone visiting your Space will restart it automatically." ([spaces-gpus#sleep-time](https://huggingface.co/docs/hub/spaces-gpus#sleep-time)) The docs do not say whether an API request (for example a `fetch` from GitHub Pages) counts as a visit. The docs give no wake time. A wake is a restart, so the container starts from the image with an empty runtime disk.

**A conflict in the sources.** Two pages say "sleep" (a visitor wakes it). The `huggingface_hub` guide says: "Your Space will automatically be paused after 48h of inactivity." ([manage-spaces](https://huggingface.co/docs/huggingface_hub/guides/manage-spaces)) A paused Space "will not use resources until manually restarted, and only the owner of a paused Space can restart it." ([spaces-gpus#pause](https://huggingface.co/docs/hub/spaces-gpus#pause)) The billing section uses a third word: "suspended automatically if they are not used for an extended period of time (e.g. two days)". ([spaces-gpus#billing](https://huggingface.co/docs/hub/spaces-gpus)) I cannot tell which page is newer. The source code warning says "go to sleep", which matches the Hub docs.

**Stay awake for free:** No documented free option. "If you want your Space never to deactivate or if you want to set a custom sleep time, you need to upgrade to paid hardware." ([spaces-gpus#sleep-time](https://huggingface.co/docs/hub/spaces-gpus#sleep-time)) On paid hardware, `sleep_time=-1` means no sleep, and that is the default. ([hf_api.py](https://github.com/huggingface/huggingface_hub/blob/main/src/huggingface_hub/hf_api.py))

**Pings to stay awake:** No primary source allows or bans them by name. See section 10. One forum user (2026-05-26) used a Cloudflare Worker cron every 2 minutes. The Space got the status "Flagged as abusive — Blocked by abuse-handler by rule: Cloudflare". ([forum, not official](https://discuss.huggingface.co/t/keepalive-ping-get-health-ready-every-2-minutes/176238)) Note that a 48 h idle rule needs only one visit every two days, not a ping every few minutes.

## 4. Docker specifics

- **Port:** "You can also change the default exposed port `7860` by setting `app_port: 7860`." ([spaces-sdks-docker](https://huggingface.co/docs/hub/spaces-sdks-docker)) `app_port` is "Used only if `sdk` is `docker`". ([spaces-config-reference](https://huggingface.co/docs/hub/spaces-config-reference))
- **One exposed port:** "Internally you could have as many open ports as you want." For more than one external port, "a workaround is to use a reverse proxy like Nginx". ([spaces-sdks-docker](https://huggingface.co/docs/hub/spaces-sdks-docker)) So Caddy is not needed for TLS, but a local proxy is allowed.
- **User:** "The container runs with user ID 1000." The docs tell you to `useradd -m -u 1000 user` and to use `COPY --chown=user`. ([spaces-sdks-docker](https://huggingface.co/docs/hub/spaces-sdks-docker))
- **Writable paths:** the docs do not list them. They show the home directory of UID 1000 (`/home/user`) as the work directory, and `RUN mkdir -p /data && chmod 777 /data` for `/data`. ([spaces-sdks-docker](https://huggingface.co/docs/hub/spaces-sdks-docker))
- **Multiple processes:** the docs do not limit them. The Elasticsearch example runs a second server in the same container. ([spaces-sdks-docker](https://huggingface.co/docs/hub/spaces-sdks-docker)) Our child-process worker pool is therefore not blocked by a documented rule.
- **Build limits:** the docs give no build time limit or image size limit. Startup has a limit: `startup_duration_timeout` "Defaults to 30 minutes". ([spaces-config-reference](https://huggingface.co/docs/hub/spaces-config-reference)) Build time is not billed. ([spaces-gpus#billing](https://huggingface.co/docs/hub/spaces-gpus))
- **Built-in variables:** `SPACE_ID`, `SPACE_HOST` (for example `osanseviero-i-like-flan.hf.space`), `CPU_CORES`, `MEMORY`, and `SPACE_SIGNING_SECRET`, which "stays the same across restarts, rebuilds, and replicas". ([spaces-overview](https://huggingface.co/docs/hub/spaces-overview))

## 5. Secrets and variables

- **Runtime:** variables "are injected in the container's environment at runtime". Secrets work the same: "at runtime, you can access the secrets as environment variables". ([spaces-sdks-docker](https://huggingface.co/docs/hub/spaces-sdks-docker))
- **Build time:** variables are passed as `build-arg`s. Secrets need `RUN --mount=type=secret,id=NAME,...` and `$(cat /run/secrets/NAME)`. ([spaces-sdks-docker](https://huggingface.co/docs/hub/spaces-sdks-docker))
- **Visibility:** variables "are publicly accessible and viewable". Secrets "are private and their value cannot be read from the Space's settings page once set". ([spaces-overview](https://huggingface.co/docs/hub/spaces-overview)) So the Turnstile secret key must be a secret. The allowed CORS origin can be a variable.
- **Restart:** "Any change in your Space configuration (secrets or hardware) will trigger a restart of your app." ([manage-spaces](https://huggingface.co/docs/huggingface_hub/guides/manage-spaces))

## 6. Outbound network

"If your Space needs to make any network requests, you can make requests through the standard HTTP and HTTPS ports (80 and 443) along with port 8080. Any requests going to other ports will be blocked." ([spaces-overview#networking](https://huggingface.co/docs/hub/spaces-overview)) https://api.deps.dev and storage.googleapis.com use port 443, so both work. The docs give no egress quota or bandwidth limit for Spaces. The content policy bans the use of "proxies ... to bypass restrictions". ([content-policy](https://huggingface.co/content-policy)) Our outbound calls are normal API calls, not a proxy.

The OSV index build (about 260 MB) runs again after each restart, because the disk is ephemeral. Two options: bake the index into the image at build time, or keep it in a bucket. Both cost a rebuild or a read at start.

## 7. Inbound

- **URL:** "`https://<space-subdomain>.hf.space`". "The subdomain is unique and only changes if you move or rename your Space." "Your space is always served from the root of this subdomain." ([spaces-embed](https://huggingface.co/docs/hub/spaces-embed)) HTTPS comes from the platform.
- **Custom domain:** "This feature is part of PRO or Team & Enterprise plans." It uses a CNAME to `hf.space`. ([spaces-custom-domain](https://huggingface.co/docs/hub/spaces-custom-domain))
- **Visibility:** a private Space is not reachable by other users ("other users will receive a `404`"). The Space must be public or protected. Protected needs PRO. ([spaces-overview#space-visibility](https://huggingface.co/docs/hub/spaces-overview))
- **Client IP:** **no primary doc.** The docs do not mention `X-Forwarded-For` or any client-IP header. One Hugging Face repository has a merged PR (2026-10-18). It says: "On the Hugging Face Docker Space ... the direct peer is HF's ingress proxy, not loopback." It trusts the "**right-most** `X-Forwarded-For` hop outside" a configured list of proxy CIDRs. The author also says he "could not confirm the Space's ingress peer range or XFF chain". ([huggingface/pwc-cli PR #33](https://github.com/huggingface/pwc-cli/pull/33)) This is code from HF staff, not platform documentation.
- **Proxy buffering, timeouts, SSE, WebSockets, max request duration:** **no primary doc.** The docs give only the startup timeout (30 minutes). The Gradio SDK uses streaming over the same proxy, which suggests that streaming works, but no doc states a limit. The Hub's own APIs use SSE (log streaming at `/api/spaces/.../logs`), but that is huggingface.co, not `*.hf.space`. ([spaces-gpus#streaming](https://huggingface.co/docs/hub/spaces-gpus))

## 8. CORS and the iframe

- The YAML `custom_headers` key adds headers to all responses. "For now, only the ... (COEP), ... (COOP), and ... (CORP) headers are allowed." ([spaces-config-reference](https://huggingface.co/docs/hub/spaces-config-reference)) That rule is about headers the platform adds. The docs do not say that the proxy removes `Access-Control-*` headers that the app sends. I found no primary statement either way.
- **The iframe:** the Space page on huggingface.co shows the app in an iframe. `fullWidth` controls the layout "inside the iframe". `disable_embedding` controls "Whether the Space iframe can be embedded in other websites" and "Defaults to false". ([spaces-config-reference](https://huggingface.co/docs/hub/spaces-config-reference))
- **For an API-only Space:** the GitHub Pages front end calls `https://<subdomain>.hf.space/api/...` directly with `fetch` and `EventSource`. The iframe on huggingface.co is not in that path. It only matters if somebody opens the Space page, where the root path shows inside the iframe. A small root page (or a redirect to GitHub Pages) is enough. Set `disable_embedding: true` if we do not want other sites to frame it.

## 9. Deploy

- **Official GitHub Action:** `huggingface/hub-sync@v0.3.0`. Inputs: `github_repo_id`, `huggingface_repo_id`, `hf_token`, `repo_type` (default `space`), `space_sdk` (default `gradio`, so set `docker`), `private`, and `subdirectory` (default `.`, "useful for monorepos"). ([repositories-github-actions](https://huggingface.co/docs/hub/repositories-github-actions))
- **How it syncs:** "It is not a git-to-git sync — it uploads the file contents and automatically excludes `.github/` and `.git/` directories. Files removed from your GitHub repository will also be removed from the Hub." ([spaces-github-actions](https://huggingface.co/docs/hub/spaces-github-actions))
- **Subfolder deploy:** `subdirectory: host` would upload only `host/`. But `host/Dockerfile` probably needs `dependency_metrics/` and `vendor/pip` from the repository root. So we need a workflow step that builds a staging folder, then `hf upload`. The docs allow this: "For more complex workflows (e.g. build steps, custom logic), you can install and use the `hf` CLI directly". ([spaces-github-actions](https://huggingface.co/docs/hub/spaces-github-actions)) The Space repo also needs a `README.md` with the YAML block at its root.
- **Trusted Publishers** can replace the `HF_TOKEN` secret with a GitHub OIDC token. ([repositories-github-actions](https://huggingface.co/docs/hub/repositories-github-actions))
- **Rebuild on push:** "Each time a new commit is pushed, the Space will automatically rebuild and restart." ([spaces-overview](https://huggingface.co/docs/hub/spaces-overview))
- **Restart vs factory rebuild:** `restart_space()` restarts the Space. With `factory_reboot=True`, "the Space will be rebuilt from scratch without caching any requirements". ([hf_api.py](https://github.com/huggingface/huggingface_hub/blob/main/src/huggingface_hub/hf_api.py)) Both lose the runtime disk.
- **Env and secret changes:** "Any change in your Space configuration (secrets or hardware) will trigger a restart of your app." ([manage-spaces](https://huggingface.co/docs/huggingface_hub/guides/manage-spaces))
- **Logs:** `hf spaces logs <space> -f` streams run logs. ([manage-spaces](https://huggingface.co/docs/huggingface_hub/guides/manage-spaces)) Dev Mode (SSH) needs PRO.

## 10. Limits and policy

- **Rate limits on `*.hf.space`:** none documented. The Hub rate limits cover "Hub APIs", "Resolvers", and "Pages", which are "All the Web pages we host on huggingface.co". ([rate-limits](https://huggingface.co/docs/hub/rate-limits)) The page does not mention Space app traffic. Our own per-IP limits stay necessary.
- **Non-ML apps:** the terms define the services as "in the field of machine learning" and define a "Machine Learning Application" as a repository that lets a user "showcase Machine Learning experiments". They do not say that every Space must be ML. Effective date 2022-09-15. ([terms-of-service](https://huggingface.co/terms-of-service)) The Docker docs show non-ML examples: "From FastAPI and Go endpoints to Phoenix apps and ML Ops tools". ([spaces-sdks-docker](https://huggingface.co/docs/hub/spaces-sdks-docker)) So a dependency-metrics API is not banned by any rule I found. It is outside the stated focus.
- **Abuse rules:** the content policy (effective 2025-04-10) bans "Spam (e.g., advertising products/services, excessive bulk activity, or disrupting user experience)", "Cryptomining practices", the use of "Cloudflare Tunnel, TOR, proxies, VNC ... to bypass restrictions", and "Hosting excessive or irrelevant data in repositories". ([content-policy](https://huggingface.co/content-policy))
- **Keep-alive pings:** no rule names them. A frequent ping can count as "excessive bulk activity" or as a way "to bypass restrictions". See the forum case in section 3.
- **Suspension:** Hugging Face may suspend access "anytime with or without cause". ([terms-of-service](https://huggingface.co/terms-of-service)) A failing Space "will be automatically suspended". ([spaces-gpus#billing](https://huggingface.co/docs/hub/spaces-gpus))

## Open questions

The primary sources do not answer these. Test each one on a real Space before the move.

1. Which `X-Forwarded-For` hop is the client? Does the proxy append to a header that the client sends, or replace it? What are the proxy CIDRs? Is there another header (for example `X-Real-IP`)?
2. Does the proxy buffer `text/event-stream` responses? Does it close a stream after some idle time or total time? Does a 15-minute SSE stream with 15 s heartbeats survive?
3. What is the maximum request duration and body size on `*.hf.space`? Are WebSockets supported?
4. Does the proxy pass the app's `Access-Control-*` headers and answer `OPTIONS` preflights unchanged?
5. After 48 h idle, does a CPU Basic Space "sleep" (any visit wakes it) or "pause" (only the owner restarts it)? Does a cross-origin `fetch` to `*.hf.space` count as a visit? How long is a cold start?
6. Which write mode does the Space bucket mount use? Is SQLite with WAL safe on it with one writer?
7. Is there an egress or bandwidth limit for Spaces?
8. How many compute credits does PRO include each month, and do they cover CPU Upgrade all month?
9. Do Docker Spaces that a free account created before July 2026 keep running, and can they still be updated? Only forum posts cover this.
10. Is there a build time limit or an image size limit?
