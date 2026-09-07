# Tool Paper Planning Notes — `dependency-update-metrics`

Working notes for writing a 4-page tool/demo paper about this repository.
Not a paper draft — a set of facts, decisions, and arguments to draw from
when writing. Citations to code use `file.py:line`; verify line numbers
haven't drifted before quoting them in the paper.

Candidate venues (fetched 2026-09-07):

| | MSR 2027 Data & Tool Showcase | ICSE 2027 Demonstrations |
|---|---|---|
| Length | 4 pages + 1 ref page | 4 pages incl. everything |
| Format | IEEE conf. proceedings | IEEE conf., 10pt/24pt title, strict |
| Video | not required by CFP text | **required**, 3–5 min, YouTube |
| Tool access | available at submission time, reviewer-accessible | website/VM/Docker, no build required |
| Post-accept | DOI archive (Zenodo/Software Heritage) | — |
| Review | single-anon (authors visible) | single-anon (authors visible) |
| Abstract/paper deadline | Nov 5 / Nov 10, 2026 | Oct 23, 2026 |
| Emphasis | reusability, MSR relevance, extensibility | target users, SE challenge, validation, live demo |

**Recommendation:** MSR Tool Showcase is the more natural fit — the tool's
whole reason to exist is enabling *mining* studies (longitudinal MTTU/MTTR
across ecosystems), and MSR's rubric ("value and reusability," "connection to
mining software repositories") maps directly onto what's already built. ICSE
Demos wants a *live, interactive* demo narrative and a validation study or a
concrete planned one — doable, but requires more framing work around "who
watches this tool run live and why do they care." If time allows, MSR's
Data Showcase category is a nearly-free second submission: the 90-package
tier case study (`case_study/`) is already a reusable dataset with its own
provenance story, separate from the tool itself.

Also decide: is this the ICSE 2027 Demonstrations track, or does the fetched
page's title ("ICSE 2027 Tool Demonstration **and Data Showcase** Track")
mean ICSE merged both categories this year? Re-check the CFP page close to
submission — the fetch above was a single pass and conference sites change.

---

## 1. One-paragraph pitch (for the abstract)

`dependency-update-metrics` is an open-source CLI tool that computes two
longitudinal software-supply-chain metrics — **Mean Time To Update (MTTU)**
and **Mean Time To Remediate (MTTR)** — for a package's direct dependencies,
across npm, PyPI, and Cargo, either as a single current-state snapshot or as
a full **retrospective timeline reconstructed at every historical release**
of the package under study. Unlike point-in-time SCA scanners, it answers
"how promptly has this project historically kept its dependencies current
and its known vulnerabilities patched?" — a question MSR researchers studying
technical lag, supply-chain risk, or maintenance behavior currently have to
answer with bespoke, unpublished scripts.

---

## 2. Problem statement / motivation (why this tool needs to exist)

- SCA tools (Dependabot, Renovate, OSV-Scanner, Snyk) answer "is this
  repository vulnerable **right now**." They don't produce a *time-series*
  metric suitable for MSR-style regression/statistical analysis across many
  projects and many historical points.
- deps.dev and Libraries.io expose the raw *data* (version graphs, advisory
  keys) but not the *derived longitudinal metric* — a researcher still has to
  write the interval-reconstruction and remediation-window logic themselves.
  This tool is exactly that missing derivation layer, built on top of
  deps.dev (and, alternatively, the native registries directly).
- Prior "library lag" / "technical lag" literature typically measures lag at
  a single snapshot per project. This tool's `--per-release` mode instead
  reconstructs the *entire* update/remediation history retrospectively in one
  pass — useful for studies that want a full time series per project rather
  than one data point.

*(Fill in real citations here before submission — technical-lag / library-lag
MSR papers, OSV-Scanner/Dependabot papers, deps.dev's own MSR data paper if
one exists. Don't fabricate references — search and confirm.)*

---

## 3. High-level architecture

```
                         ┌─────────────────────────┐
                         │   CLI (dependency_metrics│
                         │        /cli.py)          │
                         │  argparse; 3 run modes;  │
                         │  bulk worker-pool driver │
                         └────────────┬─────────────┘
                                      │
                         ┌────────────▼─────────────┐
                         │   DependencyAnalyzer      │
                         │   (analyzer.py)           │
                         │  core algorithm: interval │
                         │  construction, TTU/TTR    │
                         │  scoring, weighting       │
                         └───┬───────────────────┬───┘
                              │                   │
          ┌──────────────────▼───┐     ┌────────▼────────────────┐
          │  PackageResolver     │     │  VulnerabilityService     │
          │  (Protocol,          │     │  (Protocol,               │
          │   interfaces.py)     │     │   interfaces.py)          │
          │                      │     │                           │
          │ ┌──────────────────┐ │     │  OSVService               │
          │ │ NpmResolver       │ │     │  (osv_service.py)         │
          │ │ PyPIResolver      │ │     │  is_remediated() /        │
          │ │ CratesResolver    │ │     │  is_remediated_by_        │
          │ │  (resolvers.py)   │ │     │  severity()               │
          │ ├──────────────────┤ │     │                           │
          │ │ DepsDevResolver   │ │     │  OSVBuilder               │
          │ │ (depsdev_        │ │     │  (osv_builder.py)         │
          │ │  resolver.py)     │ │     │  downloads OSV "all.zip"  │
          │ │  → DepsDevClient  │ │     │  (~2.4GB), parses to a    │
          │ │   (depsdev_       │ │     │  flat Parquet table       │
          │ │    client.py)     │ │     │                           │
          │ └──────────────────┘ │     └───────────────────────────┘
          └──────────┬───────────┘
                     │  all HTTP responses
          ┌──────────▼───────────┐
          │   ResolverCache       │
          │   (resolvers.py)      │
          │  in-memory LRU/FIFO   │
          │  caches + SQLite disk │
          │  cache (cache.db)     │
          └────────────────────────┘

                         ┌────────────────────────────┐
                         │   Reporting                 │
                         │   (reporting.py)            │
                         │  JSON / CSV / Excel export, │
                         │  per-release worksheets     │
                         └────────────────────────────┘
```

### Component responsibilities

- **CLI (`cli.py`, ~2060 lines)** — argument parsing; three execution
  surfaces (single package, `--input-csv` bulk, `--per-release`); owns the
  `ProcessPoolExecutor` worker pool, per-worker memory ceiling enforcement,
  `--resume` checkpoint ledger, per-package `--package-timeout`.
- **`DependencyAnalyzer` (`analyzer.py`, ~1690 lines)** — the actual metric
  engine. Ecosystem-agnostic: it never talks to a registry or HTTP client
  directly, only through the injected `PackageResolver`. Builds the
  interval timeline for every dependency, applies the remediation check, and
  aggregates into MTTU/MTTR (optionally per-severity).
- **`PackageResolver` Protocol (`interfaces.py`)** — the single seam that
  makes the tool multi-ecosystem. Four concrete implementations:
  `NpmResolver`, `PyPIResolver`, `CratesResolver` (all in `resolvers.py`,
  talking to the native registries), and `DepsDevResolver`
  (`depsdev_resolver.py`, talking to deps.dev's v3 REST API via
  `DepsDevClient`). Selected by `--depsdev`.
- **`VulnerabilityService` Protocol / `OSVService`** — remediation-window
  logic against a prebuilt OSV snapshot (see §5.2).
- **`OSVBuilder`** — one-time ETL: downloads OSV's full `all.zip` export
  (~2.4GB, ~1.9M raw JSON advisories across every ecosystem OSV tracks),
  parses `introduced`/`fixed` event pairs per affected package into a flat
  Parquet table (`osv_service.py`'s consumer then indexes it in memory as
  `package → [vuln records]` — `build_osv_index()`, `analyzer.py:38`).
- **`ResolverCache`** — a two-tier cache (in-process bounded dicts +
  SQLite-backed disk cache at `<output-dir>/cache/cache.db`) shared across
  all resolvers and worker processes. See §5.3 for the design decision this
  forced.
- **Reporting** — pandas-based export to JSON/CSV/Excel, including
  per-dependency "worksheets" (one Excel sheet per dependency showing every
  interval) for manual inspection/debugging of the metric.

### Why this decomposition matters for the paper

The **Protocol-based resolver abstraction** is probably the single most
reusable engineering idea here, and worth a dedicated paragraph: adding a new
ecosystem (Maven, Go modules, RubyGems, NuGet — everything deps.dev already
indexes) requires *zero* changes to `analyzer.py`. Since `DepsDevResolver`
already talks to deps.dev generically, most new ecosystems are actually just
a new entry in `_SYSTEM_TO_ECOSYSTEM` (`depsdev_resolver.py:44`) plus a
constraint-matching function for that ecosystem's version-range syntax — the
core TTU/TTR algorithm is untouched. That's a concrete extensibility claim
reviewers can verify by reading the diff size of the "Add native crates.io
support" commit (`78d5c1b`) vs. how small the deps.dev-side addition would
be for a new system.

---

## 4. Execution pipeline (three modes)

All three modes share the same `DependencyAnalyzer`; they differ in how many
(ecosystem, package, start_date, end_date) tuples get analyzed and whether
the output is one number per package or one row per historical release.

### 4.1 Single-package snapshot (`--ecosystem X --package Y`)

1. Fetch package metadata → find the version whose release date is the
   closest one ≤ `end_date`.
2. Extract its declared dependencies (`extract_dependencies`).
3. For each dependency: build the union of dates where either the parent
   package or the dependency released a new version → these are interval
   boundaries (`build_intervals`, `time_utils.py:29`).
4. For each interval, resolve "what version of the dependency would have
   been in effect" (`resolve_dependency_version` / constraint resolution)
   and "what's the highest version that existed at that time"
   (`get_highest_semver_version_at_date`). `updated = (these two match)`.
5. Check remediation via `OSVService.is_remediated()` per interval.
6. Aggregate: MTTU = (optionally weighted) sum of interval durations where
   `updated == False`; MTTR = same, gated on `remediated == False`.

### 4.2 Bulk batch mode (`--input-csv rows.csv`)

Same algorithm, fanned out over a `ProcessPoolExecutor`. Rows are **grouped
by `(ecosystem, package_name)`** before dispatch (`_process_group_per_release`
/ `_worker_run_group`, `cli.py:271` / `cli.py:424`) so that multiple date
windows for the same package share one fetch of its metadata and dependency
graph — this is the main reason large-scale (thousands-of-packages) mining
runs are tractable at all. Supports:
- `--resume`: a completed-rows ledger CSV lets a multi-hour/day run survive
  a crash or Ctrl-C and pick back up without re-doing finished packages.
- `--max-worker-memory-mb`: a worker that exceeds its RSS ceiling exits
  cleanly with a resumable error row instead of getting OOM-killed and
  silently losing the whole batch's progress.
- `--package-timeout`: bounds a single pathological package (huge dep
  graph, degenerate version history) from stalling the entire batch.

### 4.3 Per-release retrospective timeline (`--per-release`)

The most novel mode, and the one worth the most paper real-estate.
`analyze_at_release_points()` (`analyzer.py:603`) computes MTTU/MTTR **at
every historical release** of the package, not just at `end_date`, in a
single pass:

1. Fetch the parent package's *full* version history and *every* version's
   declared dependency set (not just the ones inside the analysis window —
   see the docstring at `analyzer.py:651` for why: a release's retrospective
   frame needs to know what governed the timeline all the way back).
2. For each dependency name ever declared by any version, walk the parent's
   version history chronologically and record **breakpoints**: the points
   where "the highest-semver package version that has ever declared this
   dependency" changes (`version_history`, `analyzer.py:691`). This is
   deliberately keyed on *highest semver so far*, not *most recently
   published*, so an out-of-order backport (e.g. `1.5.2` published after
   `2.0.0`) can't steal governance back from `2.0.0`.
3. Build **one continuous interval timeline per dependency** across the
   entire window, each interval tagged with whichever package version
   actually governed it. Bisect this timeline once per release to get that
   release's `[start_date, release_date]` sub-window
   (`bisect.bisect_left(int_starts, window_end)`, `analyzer.py:1052`).
4. Score MTTU/MTTR per release from the same underlying numpy arrays
   (`_ttu_ttr_numpy`, `analyzer.py:1254`) — releases share the expensive
   interval-construction step, only the aggregation slice differs.
5. Emit one row per release.

**This replaced an earlier, buggier design** that independently replayed N
overlapping per-release retrospectives — worth describing in the paper as a
concrete lesson: the old design could show the *same* `package_version` value
with two different `dependency_constraint` values across different rows,
because `package_version` meant "which release's replay this row lives in,"
not "which release's manifest declared this constraint." It looked like a
correctness bug (a single manifest's constraint can't change) but was really
a semantic confusion in the schema. The fix — one continuous timeline,
governance resolved once, replay removed — is a reusable design pattern for
any MSR tool that reconstructs "the dependency state as of many historical
points": model the union timeline once, don't replay per anchor point.

---

## 5. Design decisions worth their own paragraph (the paper's real contribution)

These are the decisions an SE researcher reusing or reimplementing this kind
of tool would want called out explicitly — several were only discovered by
dogfooding the tool on real packages during this project, which is itself a
good "lessons learned" narrative for a tool paper.

### 5.1 Fix-gated remediation semantics, not raw vulnerability-exposure

`OSVService.is_remediated()` (`osv_service.py:125`) does **not** mark an
interval "unremediated" just because the resolved dependency version falls
inside a vulnerable range. It only does so if a fix **had already been
published** as of that interval's start
(`fixed_date <= interval_start`, `osv_service.py:161`). If no fix exists yet,
the interval counts as remediated (`True`) by construction.

- **Rationale to state explicitly in the paper:** MTTR should measure
  *responsiveness to available remediation*, not raw vulnerability exposure
  (which conflates "the maintainer was slow" with "upstream hadn't shipped a
  patch yet" — a confound many naive SCA-based lag metrics don't control
  for).
- **Consequence / threat to validity to disclose:** combined with the
  per-release governance model always tracking "the highest version
  available" (§4.3), a package whose dependency constraints are
  **unbounded/unconstrained** (e.g. `numpy` with no version specifier at
  all — deps.dev returns `""` as the requirement string) will show MTTR ≈ 0
  across its *entire* release history, even when it depends on packages with
  a long, real CVE history. We found this empirically: `cmcrameri` (PyPI)
  showed `mttr=0.0` for all 21 releases because its `matplotlib`/`numpy`/
  `packaging` requirements are unconstrained, so the tool's model of "version
  in effect" always jumps to whatever's newest by the time any fix ships —
  there's no lag window left to accumulate. This is not a bug; it's a
  structural property of an "always-adopt-latest" governance model applied
  to unconstrained dependencies, and it's exactly the kind of thing a
  methods/limitations section should name precisely rather than gloss over.
  Good discussion material: is "assume immediate adoption of latest" the
  right null model, or should the per-release timeline instead snapshot
  "whatever was actually resolvable at release time and hold it" — a
  different governance policy with different validity tradeoffs? (Candidate
  future-work sentence.)

### 5.2 Bulk OSV snapshot + in-memory index, not per-package API calls

`OSVBuilder` downloads OSV's entire cross-ecosystem advisory export once
(~2.4GB zip, ~1.9M JSON records) and flattens it to a single Parquet table
of `(vul_id, ecosystem, package, vul_introduced, vul_fixed, severity)` rows —
one row per `introduced`→`fixed` event pair per affected range
(`osv_builder.py:140`). At analysis time this is loaded once and indexed as
`package_name → [vuln rows]` in memory (`build_osv_index`, `analyzer.py:38`),
turning what would otherwise be a per-(dependency, interval) network call
into an O(1) dict lookup. For a bulk run scoring millions of intervals across
thousands of packages, this is the difference between hours and weeks.

- **Known limitation to disclose:** the parser only records advisories that
  have an explicit `fixed` event (`osv_builder.py:200-216`) — a `fixed` event
  is required to compute `vul_fixed` at all, so currently-unpatched
  vulnerabilities (no fix ever shipped) are silently absent from the index
  entirely, which biases MTTR *toward looking better* for packages with
  unresolved CVEs (they're invisible to the metric rather than
  perpetually "not yet remediated"). Worth a sentence in limitations; also a
  candidate small fix before submission if time allows (treat a `fixed`-less
  vulnerability as "not remediated" from `introduced` onward with no
  ceiling, or exclude it explicitly and say so).

### 5.3 Two-tier caching, and the mutability distinction that broke it once

`ResolverCache` persists every HTTP response to SQLite
(`<output-dir>/cache/cache.db`), keyed by `(namespace, key)`, so repeated or
resumed runs never re-fetch anything. This is *correct* for per-version data
(a specific version's dependency manifest is immutable once published) but
was, until a recent fix, **incorrect** for package-level *version-list*
lookups (`DepsDevClient.get_package()`'s full `versions[]` array, and each
native resolver's `fetch_package_metadata()`): a package's version list is
*not* immutable — it grows every time a new release ships. The cache had no
TTL, so a package first queried against a given `--output-dir` would freeze
its known version list forever; a re-run months later, even against a fresh
`--end-date`, would silently keep reporting the same latest-known version and
never see anything published since.

- **Root-caused via dogfooding**, not code review: a real run against
  `cmcrameri` in one month vs. a re-run against the same output directory
  months later produced a different number of per-release rows for the *same
  historical window* — a strong signal something was frozen in time rather
  than genuinely absent.
- **Fix, generalizable as a pattern:** before trusting a cached *listing*,
  compare the newest publish date already known in it against the requested
  `end_date`. If `end_date` is newer, the cache cannot prove no newer release
  exists and must be refreshed; if `end_date` is already covered, trust the
  cache. This distinguishes "cache an immutable fact forever" from "cache a
  monotonically-growing collection, and treat its cached snapshot as valid
  only up to the point it's known to cover" — a reusable principle for any
  MSR tool that caches "list all versions of X" against a registry API.
  Tradeoff worth stating: this means an "analyze as of today" run always
  re-fetches each package's version list (since "today" is always newer than
  the last known release) — only truly historical, fixed `end_date` values
  benefit from a warm cache across repeated runs.

### 5.4 Recency-weighting as a pluggable, orthogonal axis

`--weighting-type {linear, exponential, inverse, disable}` (+ `--half-life`
for exponential) reweights each interval's contribution to MTTU/MTTR by its
age relative to `end_date`, independent of the interval-construction logic.
This lets one analysis run answer two different research questions without
re-running: "what's this project's *historical average* lag" (disable) vs.
"how responsive is it *right now*, discounting old behavior" (exponential
half-life). Worth stating as a design decision because it's implemented as a
pure post-hoc weighting function over `(duration, age)` pairs
(`_weighted_sum_numpy`, `analyzer.py:1240`) — it doesn't change what counts
as an "unremediated interval," only how heavily each one counts, which keeps
the two concerns (what happened vs. how much it should matter now) cleanly
separated.

### 5.5 Severity-stratified MTTR as an opt-in cost

`--severity-breakdown` computes MTTR separately for Critical/High/Medium/Low
(plus an all-severities aggregate) instead of a single boolean remediated
flag. This roughly doubles the per-interval bookkeeping (5 boolean arrays
instead of 1), so it's opt-in rather than always-on — a small but deliberate
performance/utility tradeoff worth one sentence: most mining studies only
need the aggregate number, but security-focused studies need severity strata,
and paying the cost only when asked keeps the common case fast.

### 5.6 Testability via dependency injection, not mocked HTTP

Every resolver takes its `ResolverCache` (and, for deps.dev, its
`DepsDevClient`) as a constructor argument rather than constructing its own
session internally. Combined with the `PackageResolver` Protocol, this means
the 246-test suite (`pytest tests/ -q`) never makes a real network call — it
injects fixture data at the resolver boundary and exercises the actual
production `DependencyAnalyzer` code path on top of it. This is what makes a
3-OS × 3-Python-version CI matrix (`ubuntu/macos/windows` × `3.10/3.11/3.12`,
`.github/workflows/ci.yml`) affordable to run on every push without flaking
on registry rate limits or network partitions — a concern any MSR tool
talking to live registries/APIs has to solve, and dependency injection at the
resolver boundary is the reusable answer.

### 5.7 Vendored, patched `pip` for date-bounded PyPI resolution

For native (non-`--depsdev`) PyPI analysis, the tool clones a **patched fork
of `pip`** on first use (`~/.dependency_metrics/vendor/pip`) that adds
date-bounded version resolution — i.e., "resolve this constraint as it would
have resolved *as of `<date>`*," which stock `pip`/`packaging` has no notion
of (they only resolve against "latest available now"). This is a concrete,
nontrivial piece of infrastructure worth naming in the paper explicitly:
it's the kind of dependency stock tooling doesn't provide and that MSR
studies needing point-in-time resolution semantics would otherwise have to
build themselves.

---

## 6. Evaluation material already available

- **Unit tests:** 246 tests (as of this writing), fully offline/deterministic
  (§5.6), covering every resolver, the OSV remediation logic, per-release
  timeline construction, CLI resume/timeout/memory-limit behavior, and the
  cache-staleness fix (§5.3). `black --check dependency_metrics/` clean.
  CI matrix: 3 OS × 3 Python versions.
- **Case study:** `case_study/` contains a 90-package MTTU/MTTR tier study
  across ecosystems (`case_study_input.csv`, `tier_summary_mttr.csv`,
  `tier_summary_mttu.csv`, figures). This is a candidate (a) validation study
  for the ICSE demo track's "validation study results" requirement, or (b) a
  standalone MSR Data Showcase submission in its own right — decide which,
  since using it for both without differentiation would look like
  self-plagiarism/salami-slicing to reviewers. If used for both, the tool
  paper should describe the *tool* and cite the dataset paper for the study's
  findings, not reproduce the findings.
- **Real-bug narrative:** the per-release timeline redesign (§4.3) and the
  cache-staleness fix (§5.3) are both real correctness issues found via
  dogfooding, with before/after evidence (differing row counts / MTTR values
  across reruns) that can be summarized as a short "lessons learned from
  building an MSR tool" paragraph — reviewers tend to respond well to
  concrete evidence of iterative validation rather than a claim of
  correctness.

### Suggested additional validation before submission (if time allows)

- A small **face-validity table**: pick 2-3 packages with a well-documented
  CVE history and a well-documented "fast" vs. "slow" remediation, show the
  tool's MTTR ranks them in the expected direction. This is cheap to produce
  (the CLI already supports it) and directly answers "does this metric mean
  what you say it means."
- A **performance/scalability number**: how long does a full `--per-release`
  run take for a package with N releases and M dependencies, and how does
  bulk mode scale with worker count? Useful for the "usage examples" /
  "value and reusability" MSR criterion.

---

## 7. Suggested 4-page outline

1. **Introduction** (~0.4 col) — the gap (§2), one-sentence tool summary,
   contributions list (tool + metric design + public dataset/case study).
2. **Related Work** (~0.3 col) — deps.dev/OSV-Scanner/Dependabot/Renovate as
   *data sources* vs. this tool as the *derived-metric layer*; prior
   technical-lag MSR literature. *(needs real citations — don't invent them)*.
3. **Tool Architecture** (~1 col) — the diagram in §3, one paragraph per
   component, explicit callout of the Protocol-based resolver abstraction as
   the extensibility mechanism.
4. **Metric Definitions & Key Design Decisions** (~1.2 col) — MTTU/MTTR
   formal-ish definitions, then 2-3 of the §5 decisions that most differentiate
   this tool from "just run OSV-Scanner in a loop" (fix-gated remediation,
   the per-release continuous-timeline model, weighting).
5. **Usage** (~0.5 col) — one CLI invocation per mode, a sample output row,
   pointer to README/USAGE.md for the rest.
6. **Evaluation** (~0.4 col) — test suite + CI matrix (engineering rigor),
   case study pointer (metric utility), one face-validity example if added.
7. **Limitations & Future Work** (~0.3 col) — unconstrained-dependency MTTR
   artifact (§5.1), unfixed-CVE blind spot (§5.2), planned ecosystem
   extensions (Maven/Go via deps.dev, §3).
8. **Availability** — license (MIT), repo URL, DOI (post-acceptance archive),
   video URL (ICSE).

A 4-page limit almost certainly can't fit a full "Lessons Learned" section on
top of the above — §8 below is written at tech-report depth on purpose.
Options: (a) compress 2-3 of §8's strongest items (§8.4 cache invalidation
and §8.8 metric blind spots are the most distinctive/citable) into a short
paragraph inside "Limitations & Future Work" or "Evaluation," or (b) submit
the 4-pager as the tool paper and post an extended version (arXiv/tech
report) that includes §8 in full, linked from the tool paper's availability
section — a common pattern for demo/showcase papers with more engineering
material than the page limit allows.

---

## 8. Lessons learned for SE researchers building similar mining tools

These are broader than any single design decision in §5 — they're the
recurring engineering problems that show up whenever you build a
longitudinal, cross-ecosystem, run-at-scale mining pipeline, largely
independent of the specific metric being computed. This section is written
to be quotable in a "Lessons Learned" or "Discussion" paragraph, and is
deliberately more thorough than a 4-page paper has room for — treat it as
the source material to compress from, or as content for an extended
tech-report/arXiv version if the venue's page limit forces cuts.

### 8.1 Resumability is a scale requirement, not a nicety

A full-corpus MSR run (thousands of packages, `--per-release` reconstructing
entire histories) can take hours to days. At that duration, *something* will
interrupt it — a laptop sleeping, a transient network partition, a worker
hitting its memory ceiling, a reviewer's Ctrl-C. `--resume` works via an
append-only completion ledger written incrementally per finished unit of
work (one row for bulk mode, one package for per-release mode), checked
against on startup to skip already-done work
(`existing_per_release`/`existing_status`, `cli.py:941-1101`).

- **Lesson:** design the checkpoint granularity to match the actual unit of
  redoable work, and make the ledger write happen *at* completion of that
  unit, not batched/deferred — a deferred write reintroduces exactly the
  "lost progress on crash" problem resumability exists to solve.
- **Lesson:** resumability only works for free if the underlying operation is
  idempotent. Here, re-scoring an already-completed package just recomputes
  the same numbers — safe to redo if the ledger and the result disagree
  (e.g., a crash between writing the result row and writing the ledger
  entry). Tools whose units of work have side effects (e.g., appending to a
  shared external system) need to reconcile that at-least-once semantics
  explicitly rather than assume idempotency for free.

### 8.2 Bulk mode: group work by the shared expensive resource, not by input row

A naive parallel batch pipeline dispatches one task per input row. A common
MSR input shape, though, is *multiple* rows per package (different date
windows, different research questions over the same repo) — dispatched
naively, that re-fetches the same package's metadata and dependency graph
once per row. This tool groups rows by `(ecosystem, package_name)` before
dispatch (`_process_group_per_release` / `_worker_run_group`,
`cli.py:271`/`cli.py:424`) so N rows for the same package share one fetch.

- **Lesson:** before parallelizing, identify the natural cache key of the
  expensive operation in your pipeline, and partition the work queue along
  *that*, not along whatever shape the input file happens to have. The
  input's row-per-(package, window) shape is a data-entry convenience, not
  the right unit of parallel dispatch.
- **Consequence:** this makes worker-pool sizing and load balancing slightly
  harder (groups have uneven size — a package with 200 releases and one with
  2 take very different time), which is a fair trade against the
  network/time savings; worth measuring and stating explicitly if the paper
  reports wall-clock performance numbers.

### 8.3 Multi-ecosystem support is two different extensibility strategies — know which one you're building

There are two orthogonal ways to add ecosystem coverage to a tool like this:

1. **Native resolvers per ecosystem** (`NpmResolver`/`PyPIResolver`/
   `CratesResolver`, `resolvers.py`) — talk directly to each registry. Full
   control, no third-party dependency, but maintenance cost scales linearly
   with ecosystem count: every ecosystem has its own version-range syntax,
   its own metadata quirks (PyPI needed a *vendored, patched `pip`* just to
   get date-bounded resolution — stock `pip` only resolves "as of now") and
   its own rate limits/outage modes to handle independently.
2. **Delegate to a cross-ecosystem aggregator** (`DepsDevResolver` →
   deps.dev's v3 API) — near-zero marginal cost per new ecosystem (add an
   entry to `_SYSTEM_TO_ECOSYSTEM`, `depsdev_resolver.py:44`, plus a
   constraint-matcher for that ecosystem's range syntax), but the tool's
   coverage, freshness, and uptime are now bounded by a third party's, not
   your own maintenance capacity.

- **Lesson:** this tradeoff should be a *stated, deliberate* choice, not an
  accident of which one got built first — a reviewer or downstream reuser
  will reasonably ask "is your ecosystem coverage limited by your own
  engineering effort or by someone else's service," and the honest answer
  differs by ecosystem in a tool that supports both paths.
- **Lesson (a nearly-free bonus of doing both):** keeping both strategies
  behind the same `PackageResolver` Protocol means a study can cross-validate
  one data source against the other for packages where both are available —
  a cheap sanity check on data-source reliability that most single-source
  tools can't offer at all.
- **What "extending to a new ecosystem" actually costs, concretely:** for
  the deps.dev path, a new ecosystem is a version-matcher function plus one
  dict entry — small enough to point at directly in the paper as evidence of
  the abstraction's value (contrast with the native-resolver commit size for
  crates.io, `78d5c1b`, which had to reimplement metadata fetching, version
  parsing, and constraint resolution from scratch).

### 8.4 Cache invalidation: still one of the two hard problems, even (especially) in research tooling

The concrete bug (§5.3): a disk cache with no TTL treated a package's
*version list* — a mutable, ever-growing collection — the same as a
specific version's *manifest* — an immutable fact once published. The
result was silent, permanent staleness: a package first queried against a
given output directory would freeze at whatever version list existed at that
moment, and every later run, even with a fresh `--end-date` months on, kept
replaying the frozen list.

- **How it actually surfaced:** not code review, not a crash, not a failing
  test — *dogfooding*. Re-running the same historical analysis window
  against the same output directory at two different points in time produced
  a *different number of rows*. "The reconstructed past changed between two
  runs of a deterministic historical query" is a specific, recognizable
  symptom of stale-cache bugs, worth naming explicitly as a debugging
  heuristic for other researchers: if a tool claims to reconstruct history
  and its answer to a fixed historical question changes across runs, suspect
  a cache that doesn't know it's caching something mutable.
  Notably, this went undetected by 246 passing unit tests, precisely because
  fixture-based unit tests inject data directly and never exercise the
  cache-hit-vs-actually-stale branch against a real "reality moved on since I
  cached this" scenario — that's a specific *coverage gap category*, not a
  general testing failure.
- **Generalizable lesson, as a design-time checklist:**
  1. At the moment you add a cache namespace, explicitly tag it as either
     "immutable fact" (cache forever, no further thought needed) or "mutable
     collection" (needs an invalidation signal).
  2. For mutable collections, a full TTL is often unnecessary — a cheap
     structural heuristic (does the cached collection's newest known
     timestamp already cover the query's own temporal bound?) is enough and
     avoids re-fetching collections that provably can't have changed
     relevant to the query.
  3. Add at least one test per mutable-collection cache that specifically
     constructs a "cache says N, ground truth now says N+1" scenario and
     asserts the cache is *not* trusted — this is a distinct test category
     from ordinary cache-hit/cache-miss tests and won't appear by accident.

### 8.5 Balancing memory, storage, and network: there are three places to pay a cost, and the right one differs per data type

A mining pipeline typically has several different data sources with
different mutability, size, and reuse profiles, and treating "caching" as
one blanket policy wastes the opportunity to make a different, better choice
for each:

| Data | Where the cost is paid | Why |
|---|---|---|
| OSV vulnerability corpus | **Storage**, once (2.4GB download → 66MB Parquet) | Reused across *every* package/interval in a run; small enough (~10^6 rows) to hold a full in-memory index (`build_osv_index`) after the one-time ETL — turns a per-interval network call into an O(1) dict lookup. |
| Per-version package/dependency metadata | **Storage**, forever (SQLite disk cache) | Immutable once published (§5.3/8.4) — free to cache forever, reused across runs and `--resume`. |
| Package-level version lists | **Network**, small recurring cost | Mutable collection (§8.4) — the staleness check trades a per-run re-fetch for correctness; only historical, fixed `end_date` values get to skip it. |
| Hot per-process lookups (constraint resolution, metadata) | **Memory**, bounded | In-front-of-disk in-memory caches (`metadata_cache`, `npm_resolve_cache`, etc., `cache_config.py`) with explicit size caps and FIFO/LRU eviction — a multi-thousand-package run holding every fetched blob in memory indefinitely would exhaust RAM long before finishing; evicted entries fall back to the disk cache, not the network, so eviction costs recomputation-from-disk, not a network round-trip. |
| Worst-case runaway workers | **A circuit breaker, not a budget** | `--max-worker-memory-mb` measures actual RSS and fails *that one worker* cleanly (resumable) rather than trying to analytically bound memory per package in advance — dependency-graph size varies too wildly across real-world packages to bound accurately ahead of time, so the tool detects the overrun empirically instead. |

- **Lesson:** decompose "caching strategy" per data source along
  *mutability × reuse-breadth × size*, not as a single tool-wide policy. The
  right placement on the memory/storage/network triangle is usually obvious
  once those three properties are stated for a given data source — the
  mistake is usually skipping the exercise and defaulting to "cache
  everything the same way."

### 8.6 Parallelism reintroduces classic concurrency problems the moment you share a cache

Getting the grouping benefit in §8.2 means sharing one `ResolverCache`
across threads/worker processes, which reopens textbook concurrency hazards
that a single-threaded prototype never had to face:

- **Thundering herd:** N threads all miss the in-memory cache for the same
  key simultaneously and all issue the same redundant network request unless
  serialized. Fixed with a per-key lock acquired before the fetch, with a
  double-check *after* acquiring the lock in case another thread already
  populated it while this one was waiting (`get_key_lock`, used throughout
  `resolvers.py`'s `fetch_package_metadata` implementations).
- **Concurrent disk writers:** multiple worker processes writing to the same
  SQLite cache file need WAL mode and a busy-timeout, or writers intermittently
  fail with "database is locked" (`resolvers.py:283-287`).
- **Lesson:** when a tool grows from single-threaded to parallel for scale
  (a very common trajectory for research tooling that starts as a script),
  every shared cache or piece of mutable state needs a fresh concurrency
  audit — correctness under single-threaded execution proves nothing about
  correctness under concurrent access, and the failure modes (silently
  duplicated network calls, intermittent "database locked" errors) are easy
  to miss in a small-scale test run and only show up at real batch scale.

### 8.7 Timeouts and failure isolation are a scale requirement, not defensive-programming polish

At the scale of a real MSR study (thousands of packages), the existence of
at least one pathological input — a package with a degenerate version
history, a dependency graph two orders of magnitude larger than typical, a
registry that hangs instead of erroring — is not a tail risk, it is a near
certainty somewhere in the corpus. `--package-timeout` and the per-worker
memory ceiling (§8.5) both exist for the same reason: a pipeline that a
*single* adversarial-by-accident input can stall or crash isn't usable at
scale even if it's correct on 99.9% of inputs, because at scale you will hit
the 0.1%.

- **Lesson:** isolate failure at the smallest unit that still lets the run
  make forward progress (one package, not the whole batch), and always leave
  a resumable, inspectable trace of what failed and why (an error row with a
  reason, not a silent skip) — combined with §8.1, this turns "the run died
  on package #4,213 of 10,000 six hours in" from a full restart into "fix or
  skip one row and resume."

### 8.8 A metric's structural blind spots are a testing category of their own

Ordinary unit tests check that the code does what the code is supposed to
do. They don't check whether "what the code is supposed to do" is the right
model of reality — that requires deliberately probing the *governance
assumptions* baked into the metric's design. This tool's assumption that a
dependency's "version in effect" always tracks the highest currently
available version produces a specific, discoverable blind spot: packages
with **unconstrained** dependency requirements show near-zero MTTR for their
*entire* history, regardless of how vulnerable their dependencies' history
actually was (§5.1) — not a code bug, but a case where the metric's null
model silently stops measuring what it claims to measure.

- **Lesson:** for any derived-metric mining tool, deliberately construct or
  seek out at least one known extreme/boundary case per governance
  assumption (here: an unconstrained-dependency package; a package with a
  known-unpatched CVE in a dependency, per §5.2's blind spot) and inspect
  the metric's output on it *before* trusting the metric on a large corpus.
  This is a distinct activity from unit testing the implementation — it's
  testing whether the implementation's *model of the world* has a hole, and
  it's exactly the kind of thing that's cheap to do once discovered and easy
  to never think to do at all.

### 8.9 The dependency-injection seam has to be designed before you need the tests, not after

The 246-test, 3-OS × 3-Python-version CI matrix runs entirely offline — no
resolver ever makes a real network call during tests — because every
resolver takes its cache/client as a constructor argument and implements a
shared `Protocol` (`interfaces.py`), so tests inject fixture data at exactly
the I/O boundary and exercise real production logic on top of it (§5.6).

- **Lesson:** for any tool whose core value proposition is "talks to
  external, rate-limited, occasionally-flaky data sources" (true of nearly
  every MSR tool), decide the injection seam — where fixtures get to
  substitute for the network — as part of the initial architecture, not as a
  refactor bolted on once tests are needed. Retrofitting an injection point
  after the algorithm and the I/O calls are already tangled together is
  substantially more expensive than designing it in from the start, and a
  tool without this seam either can't be tested deterministically at all or
  ends up with a CI suite that's flaky by construction (live network calls
  in test runs) — a common, avoidable reason MSR tool repositories accrete a
  disabled or ignored CI badge over time.

---

## 9. Artifact / submission checklist

- [ ] Tag a citable release (e.g. `v0.1.0` or whatever version accompanies
      the paper) *before* the submission deadline — MSR requires the tool be
      reviewer-accessible at submission time.
- [ ] Fix the placeholder author email in `pyproject.toml`
      (`your.email@example.com`) before anyone treats this as a real release.
- [ ] Post-acceptance: archive on Zenodo or Software Heritage, get a DOI, add
      a `CITATION.cff` and a DOI badge to the README.
- [ ] ICSE only: record a 3–5 minute walkthrough video (script: motivate the
      problem in 30s → show single-package mode → show `--per-release` output
      and explain one row → show a bulk/case-study result → 15s on
      availability/license), upload to YouTube, link at the end of the
      abstract per their instructions.
- [ ] Decide the case-study dual-submission question (§6) before writing.
- [ ] Confirm current CFP text close to the deadline — both fetched pages are
      live conference sites and can change; re-verify page limits, video
      requirement, and submission platform right before writing.
- [ ] Do not fabricate related-work citations — every reference in §2/§7
      needs to be found and verified, not written from memory.
