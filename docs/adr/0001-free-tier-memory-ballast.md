# Keep the Oracle free-tier host alive with a memory ballast

The Host runs on an Oracle Always Free Ampere A1 instance. Oracle can reclaim an Always Free instance when its CPU, network, and memory use all stay below 20% at the 95th percentile over 7 days. Memory counts only on A1 shapes. We stay on the free tier and do not change the account to Pay As You Go, so a quiet week can cause Oracle to reclaim the instance. Therefore a ballast service holds anonymous memory, so that memory use stays above the threshold all the time.

## Considered Options

- **Pay As You Go account.** Oracle does not reclaim idle instances on this account type, and usage inside the free limits costs nothing. We rejected it because we want no billing account on the project.
- **A scheduled job, for example an OSV rebuild every 11 hours.** We rejected it. The 95th percentile ignores the busiest 5% of the week, which is about 8.4 hours, so short periodic bursts do not count.
- **CPU load.** We rejected it because it takes CPU from the analysis workers.

## Consequences

- Do not remove the ballast service as "unused". It is the only thing that keeps the instance alive in a quiet week.
- The instance must have no swap, or the ballast must lock its pages in memory. Otherwise the kernel moves the idle ballast pages to swap, and memory use falls below the threshold.
- An uptime monitor checks `/api/health`, because Oracle can change the policy.
