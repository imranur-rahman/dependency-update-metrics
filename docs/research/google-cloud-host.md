# Google Cloud as the Host

Research date: 2026-10-21. Sources: Google Cloud docs, pricing pages, and terms only. Most docs pages show "Last updated 2026-10-13" or later. The cost numbers use `us-central1` (Tier 1) list prices in USD.

## Summary

The largest finding: **the Compute Engine e2-micro VM is not $0.** The VM is free, but its public IPv4 address costs $0.005 per hour (about $3.65 per month). The free tier for that address is "one hour per month per account". **Cloud Run with Firestore and Cloud Storage can run at $0** at our load. But it needs a large rewrite of the state layer, because SQLite cannot live on Cloud Run.

Every option needs a **Paid billing account with a card**. A Free Trial account closes after 90 days and stops its resources. The Free Tier then continues only on a Paid account.

| # | Question | Fit for our Host |
|---|---|---|
| 1 | Free Program | **Fit, with a card.** The Free Tier needs a billing account. After 90 days, only a Paid account keeps running. Budget alerts do not cap spend. A spend cap exists, but only for Cloud Run and Cloud Run functions. |
| 2 | e2-micro VM | **Poor fit, and not $0.** 0.25 vCPU sustained, 1 GB RAM, 30 GB standard disk. One 750 MB analysis plus the API almost fills the RAM. The IPv4 address costs about $3.65/month. |
| 3 | Cloud Run services | **Good fit.** The free tier is about 180,000 vCPU-seconds and 360,000 GiB-seconds per month. 60 min request timeout, streaming, scale to 0, up to 32 GiB per instance. Our load uses about half of the free CPU. |
| 4 | Worker side | **Fit.** Cloud Tasks (1 M operations free) or Cloud Run jobs. Cloud Storage FUSE is not safe for SQLite: no file locks, and the docs say not to use it for a database. |
| 5 | State | **Fit, with a rewrite.** Firestore: 1 GiB, 50,000 reads and 20,000 writes per day. Cloud Storage: 5 GB in us-east1, us-west1, or us-central1. |
| 6 | Weekly rebuild | **Fit.** Cloud Scheduler: 3 jobs free per billing account. |
| 7 | Terms and hidden costs | **No rule against our use.** Hidden costs: the VM IPv4 address, Artifact Registry above 0.5 GB, internet egress above 1 GiB, and Cloud Run use above the free tier. |

The five findings that most affect the design:

1. The e2-micro IPv4 address costs about $3.65/month. An IPv6-only VM does not help, because `api.deps.dev` and `github.com` have no AAAA record (checked with `dig` on 2026-10-21).
2. Cloud Run has no persistent disk. Its file system is in memory and is lost when the instance stops. So the SQLite Job queue, the ResolverCache, and the OSV index must move.
3. With the default request-based billing, Cloud Run gives CPU only while a request runs. The Host's worker pool runs in the background, so it must change. Each Job must run inside a request (Cloud Tasks) or inside a Cloud Run job.
4. A Cloud Run spend cap can pause the service before a bill grows. But the cap uses gross cost before the free tier. So the cap must be above the value of the free tier (about $6), or it pauses the service while the real bill is still $0.
5. The docs do not describe the `X-Forwarded-For` format on a `*.run.app` URL. We must test which hop is the client before we trust the per-IP limits.

**Recommendation:** Architecture B (Cloud Run API service, Cloud Run worker service driven by Cloud Tasks, Firestore, Cloud Storage). Expected cost: $0/month at 500 Comparisons, with a Cloud Run spend cap of about $5 as a guard.

## 1. The Google Cloud Free Program

**Free Trial.** "The Free Trial gets you a $300 Welcome credit to spend over 90 days on various Google Cloud products and you won't be billed." ([free-cloud-features](https://docs.cloud.google.com/free/docs/free-cloud-features)) Sign-up needs a card: "During the sign up, you must provide a credit card or other payment method that is valid for the period of the Free Trial." Google places a temporary hold of "$0.00 and $1.00 USD", which "isn't an actual charge". (same page)

**Limits during the trial.** No GPUs, no Marketplace, no quota increases, no Windows VMs. ([free-cloud-features](https://docs.cloud.google.com/free/docs/free-cloud-features)) The trial terms also say: "Customer's use of Google Compute Engine is restricted to only eight cores of concurrent usage", and "Customer may not use the Services to engage in mining cryptocurrency." ([Free Trial terms](https://cloud.google.com/terms/free-trial))

**After the trial: no auto-upgrade, no charges, but the resources stop.**

- "If you don't upgrade to a Paid billing account before 90 days pass or if you spend the $300 in free credit, then your Free Trial billing account will be closed and all of its associated projects and resources will be stopped." ([free-cloud-features](https://docs.cloud.google.com/free/docs/free-cloud-features))
- A 30-day grace period follows. "If you don't upgrade to a Paid billing account during the grace period, then your Free Trial resources are permanently deleted." (same page)
- An upgrade is manual ("click **Activate**"). After it, "Your payment method is charged for any usage that exceeds your remaining credit, the Free Tier limits, or usage on products that aren't part of the Free Trial." (same page)

**Always Free (the "Free Tier") needs a billing account.** "A Google Cloud billing account is required to access the Google Cloud Free Tier." The limits "are calculated per billing account". "Any usage that exceeds the Free Tier usage limits is billed at standard rates." "The Free Tier has no end date, but Google reserves the right to change the offering ... with 30 days' advance notice." Unused limits do not roll over. ([free-cloud-features](https://docs.cloud.google.com/free/docs/free-cloud-features))

So a long-running $0 Host needs a Paid billing account with a valid card. We then pay only for use above the Free Tier.

**Budget alerts do not cap spend.** "Setting an alerts-only budget doesn't automatically cap Google Cloud or Google Maps Platform usage or spending." ([budgets](https://docs.cloud.google.com/billing/docs/how-to/budgets)) Budgets can send Pub/Sub messages, for example to automate "programmatically disabling Cloud Billing on a project". (same page)

**Spend cap budgets exist, but only for a few services.** The eligible services are the Gemini API, Gemini Enterprise Agent Platform, Cloud Run, and Cloud Run functions. ([budgets-spend-caps](https://docs.cloud.google.com/billing/docs/how-to/budgets-spend-caps)) The rules that matter:

- "When a spend cap budget is enforced, usage of your specified services is automatically paused for the remainder of the budget period". The cap lifts on the first day of the next month. (same page)
- One project and one service per cap. The period is monthly only. (same page)
- "The cost calculations for spend caps are based on gross, estimated costs and don't include savings and credits." (same page) The Cloud Run free tier "is applied as a spending based discount". ([Cloud Run pricing](https://cloud.google.com/run/pricing)) So the cap counts free-tier use as spend.
- "the enforcement of spend caps isn't instant and any cost overages are billed as normal." ([budgets-spend-caps](https://docs.cloud.google.com/billing/docs/how-to/budgets-spend-caps))
- When the cap pauses Cloud Run, "any existing service will fail to serve traffic and return 5xx errors", and jobs "fail to start". ([billing settings](https://docs.cloud.google.com/run/docs/configuring/billing-settings)) After a lift, recovery can take "up to 30 minutes". ([known issues](https://docs.cloud.google.com/run/docs/issues))
- No spend cap exists for Compute Engine, Firestore, Cloud Storage, or Cloud Tasks.

## 2. Compute Engine e2-micro

**The free limits.** "1 non-preemptible e2-micro VM instance per month in one of the following US regions: Oregon: us-west1. Iowa: us-central1. South Carolina: us-east1." Plus "30 GB-months standard persistent disk" and "1 GB of outbound data transfer from North America to all region destinations (excluding China and Australia) per month". The limit "is by time, not by instance": all e2-micro hours in a month are free up to the hours in that month. ([free-cloud-features](https://docs.cloud.google.com/free/docs/free-cloud-features))

**The hardware.** e2-micro has 2 vCPUs, a fractional vCPU of 0.25, and 1 GB of memory. "e2-micro sustains 2 vCPUs, each for 12.5% of CPU time totaling 25% CPU time." Each vCPU "can burst up to 100% of CPU time, for short periods". At 100% use, the burst lasts "e2-micro: 30 seconds". It has "a virtio memory balloon device". ([general-purpose machines](https://docs.cloud.google.com/compute/docs/general-purpose-machines))

**Egress.** Premium Tier is the default. On the Standard Tier, "First 200 GiB per month free", and "Always Free usage limits do not apply to Standard Tier". ([network pricing](https://cloud.google.com/vpc/network-pricing)) Our responses are small JSON bodies, so 1 GB is likely enough. See section 7.

**The external IPv4 address is not free.**

- "You are charged for static and ephemeral external IP addresses". "Static and ephemeral IP addresses in use on standard VM instances": "$0.005 / 1 hour". ([network pricing](https://cloud.google.com/vpc/network-pricing))
- The free tier: "This free usage is limited to one hour per month per account." (same page)
- The Free Tier table for Compute Engine does not list an IP address. ([free-cloud-features](https://docs.cloud.google.com/free/docs/free-cloud-features))
- So one IPv4 address costs about 729 h × $0.005 = **$3.65/month**. A static address and an ephemeral address in use have the same price. An unused static address costs $0.01/hour. ([network pricing](https://cloud.google.com/vpc/network-pricing))
- External IPv6 is free: "You are not charged ... for external IPv6 addresses that are assigned to VM instances." (same page) But an IPv6-only VM cannot reach `api.deps.dev` or `github.com`, because neither has an AAAA record (`dig`, 2026-10-21). Many users also have no IPv6. Cloud NAT would add $0.005/hour per IP, so it does not save money. (same page)

**Can 1 GB of RAM run our Host?** Only a trimmed Host, and only with risk. This is my estimate, not a documented fact:

- One analysis peaks at about 750 MB. The API, the worker parent, Caddy, and the OS also need memory. Together they exceed 1 GB at peak.
- With a swap file on the 30 GB standard disk, one worker can run. Swap on a standard persistent disk is slow, so a large Job can reach the 15-minute timeout.
- The RSS limit must stay above 750 MB, so it cannot protect the VM. The kernel OOM killer becomes the real limit.
- `DM_HOST_WORKERS` must be 1. With 20 waiting Jobs at about 75 s each, the last user waits about 25 minutes.
- The OSV rebuild reads about 260 MB of zips. Its peak memory is not measured. It can push the VM into swap or OOM.
- The 2.5 s of CPU per analysis fits in the 30 s burst. The 0.25 vCPU sustained rate limits back-to-back Jobs.
- Docker adds overhead. A systemd service with a venv uses less memory.

The ballast (ADR 0001) is Oracle-specific. I found no Google doc about reclaim of an idle e2-micro.

## 3. Cloud Run services

**The free tier (request-based billing, the default).** "CPU - First 180,000 vCPU-seconds free per month", "RAM - First 360,000 GiB-seconds free per month", "Requests - 2 million requests free per month". ([Cloud Run pricing](https://cloud.google.com/run/pricing)) The Free Tier table also lists "1 GB of outbound data transfer from North America per month". ([free-cloud-features](https://docs.cloud.google.com/free/docs/free-cloud-features))

**The free tier (instance-based billing).** "CPU - First 240,000 vCPU-seconds free per month", "RAM - First 450,000 GiB-seconds free per month". Jobs list the same amounts. ([Cloud Run pricing](https://cloud.google.com/run/pricing))

**The free tier is one dollar amount.** "The free tier usage is aggregated across projects by billing account ... The free tier is applied as a spending based discount using Tier 1 pricing." ([Cloud Run pricing](https://cloud.google.com/run/pricing)) My inference: 180,000 × $0.000024 and 240,000 × $0.000018 both equal $4.32. 360,000 × $0.0000025 and 450,000 × $0.000002 both equal $0.90. So the free tier is about $4.32 of CPU plus $0.90 of memory, and services and jobs probably share it. The page does not say this directly.

**Prices above the free tier (us-central1).**

| Billing | CPU per vCPU-second | Memory per GiB-second | Requests |
|---|---|---|---|
| Request-based, active | $0.000024 | $0.0000025 | $0.40 per million |
| Request-based, idle min instance | $0.0000025 | $0.0000025 | |
| Instance-based, and jobs | $0.000018 | $0.000002 | |

([Cloud Run pricing](https://cloud.google.com/run/pricing))

**What is billed.** With request-based billing, Cloud Run bills when "The instance is starting", when it is "gracefully shutting down", and when "At least one request is being processed". With instance-based billing, it bills "the entire lifetime ... with a minimum of 1 minute". Jobs use the instance-based rate. ([Cloud Run pricing](https://cloud.google.com/run/pricing)) Billing rounds "up to the nearest 100 millisecond". (same page)

**Request timeout.** "The timeout is set by default to 5 minutes (300 seconds) and can be extended up to 60 minutes (3600 seconds)." Above 15 minutes, Google recommends retries and clients that reconnect. ([request timeout](https://docs.cloud.google.com/run/docs/configuring/request-timeout)) Our streams end after 5 minutes (`max_seconds` in `host/dmhost/events.py`), so the default timeout is almost enough. Set it to about 10 minutes.

**SSE and streaming.** "Cloud Run supports streaming HTTP responses. No configuration is required to enable the feature. The server has to respond with a `Transfer-Encoding: chunked` response header." Cloud Run sends HTTP/1 to the container by default. ([HTTPS requests](https://docs.cloud.google.com/run/docs/triggering/https-request)) The docs do not mention buffering of `text/event-stream` or an idle timeout below the request timeout. Each open stream keeps the instance billable, because it is a request.

**Min instances 0 and cold start.** "When a revision does not receive any traffic, it is scaled in to the minimum number of instances configured (zero by default)." A new instance must listen "within 4 minutes". A waiting request pends "for up to 3.5 times average startup time ... or 10 seconds, whichever is greater". ([container contract](https://docs.cloud.google.com/run/docs/container-contract)) "If min instances is set to 0, you are not billed when instances are idle." ([min instances](https://docs.cloud.google.com/run/docs/configuring/min-instances)) The docs give no typical cold-start time.

**The file system is in memory.** "It is an in-memory file system, so writing to it uses the instance's memory. Data written to the file system doesn't persist when the instance stops." ([container contract](https://docs.cloud.google.com/run/docs/container-contract)) An in-memory volume can have a size limit, but "it does not allocate additional space". ([in-memory volumes](https://docs.cloud.google.com/run/docs/configuring/services/in-memory-volume-mounts))

**Memory per instance.** "The maximum amount of memory you can configure is 32 gibibyte (32 Gi)." 1 vCPU allows "Up to 4 GiB". 0.5 vCPU allows "Up to 1 GiB". ([memory limits](https://docs.cloud.google.com/run/docs/configuring/services/memory-limits)) So one worker instance with 1 vCPU and 2 GiB fits one 750 MB analysis and the 31 MB OSV index.

**Background work after the response.** With request-based billing, "when the Cloud Run service finishes handling a request, the instance's access to CPU will be disabled or severely limited. You shouldn't start background threads or routines that run outside the scope of the request handlers". ([general tips](https://docs.cloud.google.com/run/docs/tips/general)) Instance-based billing allows background work. But "An instance will never stay idle for more than 15 minutes after processing a request unless it is kept active using minimum instances", and idle instances "can be shut down at any time". ([billing settings](https://docs.cloud.google.com/run/docs/configuring/billing-settings)) The same page recommends Cloud Tasks, which "supports running times up to 30 minutes".

**Child processes.** The docs say "The container might be executed on multiple cores simultaneously" and do not forbid more processes. ([container contract](https://docs.cloud.google.com/run/docs/container-contract)) So the Host's child process per Job can stay. The RSS check must count against the instance memory limit.

**Client IP and `X-Forwarded-For`.** I found no Cloud Run page that defines the header for a `*.run.app` URL. Related Google docs:

- Cloud Run functions: "The first IP in this list is generally the IP of the client". ([functions headers](https://docs.cloud.google.com/functions/docs/reference/headers))
- The external Application Load Balancer "appends two IP addresses": `<existing-value>,<client-ip>,<load-balancer-ip>`, and it "does not verify any IP addresses that precede" them. ([load balancing](https://docs.cloud.google.com/load-balancing/docs/https))

The Host trusts the rightmost hop that is not a trusted proxy. On Cloud Run, the TCP peer is Google's front end, and the docs give no address range for it. So `client_ip.py` needs a Cloud Run mode: trust every peer, and take the last hop (direct `run.app`) or the second-to-last hop (behind a load balancer). Test this before launch.

**HTTPS URL.** Each service gets `https://SERVICE_NAME-PROJECT_NUMBER.REGION.run.app` (deterministic) or `https://SERVICE_IDENTIFIER.run.app`. "Cloud Run redirects all HTTP requests to HTTPS but terminates TLS before they reach your web service." ([HTTPS requests](https://docs.cloud.google.com/run/docs/triggering/https-request)) So Caddy, DuckDNS, and Let's Encrypt are not needed.

**CORS.** I found no Cloud Run doc about CORS. A public service passes requests to the container, so FastAPI's CORS middleware answers them as today. The service must allow unauthenticated calls. An IAM-protected service would refuse the browser preflight. ([HTTPS requests](https://docs.cloud.google.com/run/docs/triggering/https-request) describes private services through the IAM invoker role.)

**Cost estimate for 500 Comparisons per month.** My assumptions: 1 to 5 packages, mean 3; 25 s wall time per analysis; about 10 s of start time per worker request; each user keeps an SSE stream open for about 90 s; no overlap between users (worst case for billing).

| Part | Size | Typical (3 packages) | Every Comparison has 5 packages |
|---|---|---|---|
| Worker service | 1 vCPU, 2 GiB, concurrency 1 | 500 × 85 s = 42,500 vCPU-s; 85,000 GiB-s | 500 × 135 s = 67,500 vCPU-s; 135,000 GiB-s |
| API service (SSE) | 1 vCPU, 0.5 GiB | 500 × 90 s = 45,000 vCPU-s; 22,500 GiB-s | 500 × 140 s = 70,000 vCPU-s; 35,000 GiB-s |
| **Total** | | **87,500 vCPU-s (49% of free); 107,500 GiB-s (30%)** | **137,500 vCPU-s (76%); 170,000 GiB-s (47%)** |
| Requests | | a few thousand (under 1% of 2 M) | same |
| **Bill** | | **$0** | **$0** |

The break-even is about 1,000 typical Comparisons per month. Above it, each extra typical Comparison costs about 175 vCPU-s × $0.000024 ≈ $0.004 plus memory. The worst case is abuse: 500 Jobs that each hit the 15-minute timeout use about 900,000 vCPU-s, a gross cost of about $22. The per-IP limits, the queue cap, and a spend cap guard against this.

## 4. The worker side

**Cloud Tasks.** "0 count to 1,000,000 count" of billable operations are free per month, then "$0.40 / 1,000,000". A billable operation is "an API call or push delivery attempt", chunked at 32 KB. ([Cloud Tasks pricing](https://cloud.google.com/tasks/pricing)) An HTTP target has a default timeout of "10 minutes, with a maximum of 30 minutes". ([HTTP target tasks](https://docs.cloud.google.com/tasks/docs/creating-http-target-tasks)) That covers our 15-minute Job timeout. Set the retry count to 0 or make the handler skip a Job that already ran, so that a retry does not run a Job twice.

**Pub/Sub.** "the first 10 GiB of throughput ... is free", then "$40 per TiB". ([Pub/Sub pricing](https://cloud.google.com/pubsub/pricing)) When a Pub/Sub push subscription calls a Cloud Run service, "the timeout of these services apply", so the Pub/Sub limit applies, not only the Cloud Run timeout. ([request timeout](https://docs.cloud.google.com/run/docs/configuring/request-timeout)) I did not check the Pub/Sub push limit. Cloud Tasks fits better, because its 30-minute limit is documented and it can limit the dispatch rate to the worker.

**Cloud Run jobs.** The same free tier as instance-based services. ([Cloud Run pricing](https://cloud.google.com/run/pricing)) "By default, each task runs for a maximum of 10 minutes", up to "168 hours (7 days)". ([task timeout](https://docs.cloud.google.com/run/docs/configuring/task-timeout)) Each execution bills at least 1 minute. The quota is 1,000 running executions per project and region. ([quotas](https://docs.cloud.google.com/run/quotas)) A known issue: tasks "might be spuriously marked as retried", so Google recommends `--max-retries` of 3 or more. ([known issues](https://docs.cloud.google.com/run/docs/issues)) The docs give no start latency for an execution.

**Cloud Storage FUSE volume mounts, and SQLite.** Do not put SQLite on them.

- "Cloud Storage FUSE does not provide concurrency control for multiple writes (file locking) to the same file. When multiple writes try to replace a file, the last write wins and all previous writes are lost." ([Cloud Run volume mounts](https://docs.cloud.google.com/run/docs/configuring/services/cloud-storage-volume-mounts))
- "Cloud Storage FUSE does not support file locking or file patching." "If you try to patch a file, Cloud Storage FUSE will reupload the entire file." ([Cloud Storage FUSE](https://docs.cloud.google.com/storage/docs/cloud-storage-fuse/overview))
- "Cloud Storage FUSE has much higher latency than a local file system, and as such, shouldn't be used as the backend for storing a database." (same page)

A read-only mount of the OSV parquet file is fine. Many readers are allowed. (same page)

## 5. State in the Always Free tier

**Firestore (Standard edition).** Free quota: "Stored data 1 GiB", "Document reads 50,000 per day", "Document writes 20,000 per day", "Document deletes 20,000 per day", "Outbound data transfer 10 GiB per month". "Firestore allows exactly one free database per project", and named databases get no free quota. The quota resets "around midnight Pacific time". Above it: $0.03 per 100,000 reads and $0.09 per 100,000 writes. "TTL deletes" have no free usage. ([Firestore pricing](https://cloud.google.com/firestore/pricing)) The maximum document size is "1 MiB (1,048,576 bytes)". ([Firestore quotas](https://docs.cloud.google.com/firestore/quotas)) TTL "Data is typically deleted within 24 hours after its expiration date." ([Firestore TTL](https://docs.cloud.google.com/firestore/docs/ttl))

**Cloud Storage.** "5 GB-months of regional storage (US regions only)", "5,000 Class A Operations", "50,000 Class B Operations", "100 GB of outbound data transfer from North America". The free tier applies "only to usage in the us-east1, us-west1, and us-central1 regions". ([free-cloud-features](https://docs.cloud.google.com/free/docs/free-cloud-features)) The pricing page adds: "Always Free quotas do not apply to Rapid Storage class." ([Cloud Storage pricing](https://cloud.google.com/storage/pricing))

**What this means for each piece of state:**

- **Job queue, per-IP limits, queue cap.** Move from SQLite to Firestore. One Firestore transaction can do what `JobQueue.admit` does now. Our load is small: about 10 writes and 100 reads per Comparison, far below the daily quota. The SSE stream polls every 0.5 s today (`poll_seconds`), which is 120 reads per minute per stream. Poll every 2 s, or use a Firestore listener, so that a busy day stays under 50,000 reads.
- **IP hash.** The Host removes each hash after one hour. Firestore TTL deletes only "within 24 hours", and TTL deletes are not free. So keep the explicit delete on each submit, as `delete_expired_jobs` does now.
- **Result reuse (72 h) and retention (30 days).** Store the Job metadata and the reuse key in Firestore. A Per-release result can be larger than 1 MiB, so store result bodies as JSON objects in Cloud Storage.
- **ResolverCache.** It cannot stay. The in-memory file system counts against the 2 GiB instance memory and is lost on scale-in. A 10 GB SQLite file does not fit the 5 GB free bucket or the RAM. Options: run without a shared cache (each worker instance starts cold), or keep a small snapshot (for example 500 MB) in Cloud Storage. The worker downloads it at start and uploads it after Jobs. Only one worker instance may upload, because the last write wins.
- **OSV index.** The 31 MB parquet file fits well. A weekly job writes it to the bucket. Workers download it at start, or read it from a read-only FUSE mount. Store the build date in object metadata or in Firestore, because the reuse key needs `OSVBuilder.index_date()`.

## 6. The weekly rebuild

"Each Google billing account gets 3 jobs per month free." Then "$0.10/job/31 days". A paused job still counts. ([Cloud Scheduler pricing](https://cloud.google.com/scheduler/pricing)) One Scheduler job starts a weekly Cloud Run job that runs `python -m dmhost.maintenance` (OSV rebuild only, with no cache prune). Cloud Run jobs can run up to 7 days, so the rebuild time is not a limit. ([task timeout](https://docs.cloud.google.com/run/docs/configuring/task-timeout)) The download from `storage.googleapis.com` is inbound traffic, and "No charge for inbound data transfer". ([network pricing](https://cloud.google.com/vpc/network-pricing))

## 7. Terms and hidden costs

**Terms.** The Acceptable Use Policy bans illegal use, malware, spam, attacks on the Services, and the use of the Services "to test or reverse-engineer the Services in order to find limitations". ([AUP](https://cloud.google.com/terms/aup)) Nothing in it bans a public research API. The Free Trial bans crypto mining. ([Free Trial terms](https://cloud.google.com/terms/free-trial)) Google may "immediately Suspend" use to protect the Services or other customers. ([Terms of Service](https://cloud.google.com/terms), section 4.2) I found no rule against keep-alive pings, and Cloud Run does not need them. The deps.dev terms are outside the scope of this file.

**Hidden costs:**

- **IPv4 on a VM:** about $3.65/month, see section 2. Cloud Run's pricing has no IP address charge. ([Cloud Run pricing](https://cloud.google.com/run/pricing))
- **Internet egress:** Cloud Run gets "1GiB free data transfer within North America per month", then Premium Tier rates. ([Cloud Run pricing](https://cloud.google.com/run/pricing)) Premium Tier above 1 GiB is $0.12/GiB in North America. ([network pricing](https://cloud.google.com/vpc/network-pricing)) "There is no charge for data transfer to Google Cloud resources in the same region". ([Cloud Run pricing](https://cloud.google.com/run/pricing)) Our responses are small. 500 Comparisons with 100 KB of results each are about 50 MB. Users outside North America may bill at other rates.
- **Artifact Registry:** "0.5 gibibyte month" free, then $0.000136986 per GiB-hour (about $0.10/GiB-month). ([Artifact Registry pricing](https://cloud.google.com/artifact-registry/pricing)) Our image has Python, pandas, and pyarrow, so it can exceed 0.5 GB. Keep only the newest image or two with a cleanup policy. Same-location pulls are free. (same page)
- **Cloud Build:** "2,500 free build-minutes per month" on `e2-standard-2`, and "the limit is subject to change". ([Cloud Build pricing](https://cloud.google.com/build/pricing)) A source deploy uses Cloud Build and Artifact Registry: "you will incur charges for deploying your functions, even when your use of Cloud Run falls within the free tier" if storage exceeds the free tier. ([Cloud Run pricing](https://cloud.google.com/run/pricing)) A build in GitHub Actions and a push to Artifact Registry avoid Cloud Build.
- **Logging:** "First 50 GiB of log data per project per month". **Secret Manager:** "6 active secret versions per month". We need 2 (the Turnstile key and the IP hash key). ([free-cloud-features](https://docs.cloud.google.com/free/docs/free-cloud-features))
- **Region:** Cloud Storage is free only in us-east1, us-west1, and us-central1. Put everything in `us-central1`, which is Tier 1 for Cloud Run.

## Candidate architectures

### A: e2-micro VM with a trimmed Host

**What changes in `host/`:**

- A new runbook and setup script for Ubuntu on an e2-micro in `us-central1`, with a 30 GB standard disk and a 2 to 4 GB swap file.
- `DM_HOST_WORKERS=1`. Lower the RSS limit to about 900 MB, or turn it off and rely on swap.
- Remove the `ballast` service. Run without Docker (systemd and a venv), or keep Compose with only `api`, `worker`, and `caddy`.
- Keep SQLite, Caddy, and DuckDNS. Prune the ResolverCache to about 10 GB.
- Possibly build the OSV index elsewhere (for example in GitHub Actions) and copy the parquet file to the VM, because the rebuild can run out of memory.

**Cost:** about $3.65/month for the IPv4 address. The VM, the disk, and the first 1 GB of egress are free.

**Risks:** not $0. 1 GB RAM with a 750 MB peak per analysis. One worker, so long waits. Slow swap. No spend cap for Compute Engine.

**Verdict:** No. It breaks the $0 goal and gives the worst performance.

### B: Cloud Run API service and Cloud Run worker service, with Cloud Tasks, Firestore, and Cloud Storage (recommended)

**What changes in `host/`:**

- `job_queue.py`: a new Firestore implementation of `JobQueue` (admit transaction, position, status, cancel, retention, IP hash delete). Keep the SQLite version for local runs and tests behind the same interface.
- `worker.py`: replace the poll loop with an HTTP handler, for example `POST /internal/run/{job_id}`. Cloud Tasks calls it with an OIDC token. The handler keeps the child process, the RSS limit, the 15-minute timeout, and the cancel check (it reads the Job status from Firestore every few seconds).
- `app.py`: after `admit`, create a Cloud Task. Store result bodies in Cloud Storage.
- `events.py`: poll Firestore every 2 s, or use a listener. Keep the 15 s heartbeats and the 5-minute stream end.
- `client_ip.py`: a Cloud Run mode for `X-Forwarded-For` (see section 3).
- `maintenance.py`: the weekly Cloud Run job writes the OSV parquet file to Cloud Storage. Remove the cache prune.
- `tool_analysis.py`: download the OSV index at start. Run without a shared ResolverCache at first.
- Remove `Caddyfile`, DuckDNS, the ballast, `vm-setup.sh`, and `disk-check.sh`. Replace `compose.yaml` and `deploy.sh` with a deploy script for two services, one job, one Scheduler job, and one Cloud Tasks queue.

**Settings:** API service: 1 vCPU, 512 MiB, concurrency 80, min 0, max 2 instances, timeout 600 s. Worker service: 1 vCPU, 2 GiB, concurrency 1, min 0, max 2 instances, timeout 1,200 s. The Cloud Tasks queue dispatches at most 2 at a time. The Cloud Run spend cap is about $5 gross. Because the cap counts gross cost and the free tier is about $6 of value, a $5 cap stops Cloud Run before the net bill goes above about $1.

**Cost:** $0/month at 500 Comparisons (section 3 table). Cloud Tasks, Firestore, Cloud Storage, and Scheduler all stay inside their free tiers. Artifact Registry can add a few cents if the image is above 0.5 GB.

**Risks:**

1. A large rewrite of the state layer (`job_queue.py` is 412 lines and has the most rules).
2. No warm ResolverCache, so each analysis can be slower than 25 s. This is not measured.
3. The `X-Forwarded-For` format on `run.app` is not documented, so the per-IP limits need a test.
4. Cold starts add latency to the first Job after an idle period. The docs give no number.
5. A spend cap that is set too low pauses the service with 5xx errors.

**Verdict:** Yes. It is the only design here that is $0 at our load and has a hard spend guard.

### C: Cloud Run API service with Firestore, and one Cloud Run job execution per Job

**What changes in `host/`:** the same Firestore and Cloud Storage changes as B. But the API starts a Cloud Run job execution for each Job through the Cloud Run Admin API, and passes the job ID as an override. The job container runs one Job and exits. Cancel becomes a call that cancels the execution, or a status check in Firestore.

**Cost:** $0/month. Jobs use the lower instance-based rate ($0.000018 per vCPU-second), but each execution bills at least 1 minute. 500 executions use about 45,000 vCPU-s.

**Risks:**

1. The docs give no start latency for a job execution. It adds to every Job.
2. The API needs IAM rights to run jobs, which widens its permissions.
3. The spurious-retry known issue forces `--max-retries` of 3 or more, so a failed Job can run again unless the code skips it.
4. Queue position and the cap of 20 waiting Jobs must be our own logic, because executions start at once.

**Verdict:** A fallback. Use it if Cloud Tasks with an HTTP handler does not work, for example if the worker needs more than 30 minutes.

### Variants I rejected

- **SQLite on a Cloud Storage FUSE mount:** the docs say no file locks and no database use. See section 4.
- **SQLite on the in-memory file system of one instance:** the data is lost when the instance stops, so Jobs and results disappear.
- **Instance-based billing with the current background worker pool:** an idle instance stays up to 15 minutes after each request. At 500 Comparisons, that can use about 500,000 vCPU-s, above the free tier (about $5 per month). Idle instances "can be shut down at any time", which can stop a running Job.

## Open questions

The primary sources do not answer these. Test each one before the move.

1. What exact `X-Forwarded-For` value does a `*.run.app` service get? Does Google append the client IP to a header that the client sends? Which hop must `client_ip.py` take?
2. Does a 5-minute SSE stream with 15 s heartbeats pass through `run.app` without buffering? Is there an idle timeout shorter than the request timeout?
3. How long is a cold start for our image (Python, pandas, pyarrow, and the OSV download) on Cloud Run? How long does a Cloud Run job execution take to start?
4. How long does one analysis take without a warm ResolverCache? Is the 25 s figure from a warm or a cold cache?
5. Do Cloud Run services and Cloud Run jobs share one free-tier amount? The equal dollar values suggest it, but the page does not say it.
6. How large is our image? Does it fit the 0.5 GB Artifact Registry free tier?
7. How much memory does the OSV rebuild need at peak? This decides if it can run on an e2-micro (A) and how large the job must be (B, C).
8. Does Cloud Run add or remove CORS headers, or answer `OPTIONS` itself, for a public service? I found no doc about it.
9. How fast does a spend cap act after the gross cost passes the cap? The docs say only that it "isn't instant".
10. Does Google reclaim or throttle an idle or busy e2-micro in practice? I found no doc about it.
