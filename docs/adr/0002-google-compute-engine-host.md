# Run the Host on a Google Compute Engine VM paid by a research credit

Oracle had no Ampere A1 capacity in our home region, so we could not make the free VM of ADR 0001. We have a Google Cloud research credit of $1,000 that ends on 2 September 2027. Therefore the Host runs on one Compute Engine VM, an `e2-standard-2` (2 vCPU, 8 GB) in `us-east1` with a static IPv4 address and a 50 GB balanced disk. It costs about $55 a month, so the credit pays for it until the credit ends. The VM runs the same Docker Compose setup as the Oracle plan, so the Tool, the Host code, and the Webapp do not change.

## Considered Options

- **Cloud Run, Firestore, and Cloud Tasks.** It costs about $0 inside the free quotas. We rejected it because SQLite cannot run on Cloud Run, so the Job queue, the limits, and the reuse records must move to Firestore, and the worker pool must become an HTTP handler. Each analysis also starts with an empty ResolverCache. `docs/research/google-cloud-host.md` has the details.
- **The Always Free e2-micro VM.** We rejected it. It has about 1 GB of RAM, and one analysis can use about 750 MB. Its IPv4 address also costs about $3.65 a month, and an IPv6-only VM cannot reach deps.dev or github.com.
- **A larger VM (`e2-standard-4`, 16 GB, 6 workers).** We rejected it. It costs about $105 a month, so the credit ends before 2 September 2027.
- **A Spot VM.** We rejected it, because Google can stop a Spot VM at any time, and that stops the running Jobs.

## Consequences

- Google does not reclaim an idle VM, so the ballast of ADR 0001 does not run. It starts only with the Compose profile `oracle`.
- A budget alert of $60 a month must exist before the VM starts. When the credit ends or runs out, Google bills the card of the account, so decide before 2 September 2027 whether to stop the VM, move it, or pay.
- 3 workers fit in 8 GB, so at most 3 Jobs run at the same time. The other Jobs wait in the queue.
