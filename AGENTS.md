# AGENTS.md

`dependency-metrics` computes MTTU and MTTR (mean time to update / to remediate) for the direct dependencies of an npm, PyPI, or Cargo package. `README.md` and `USAGE.md` describe the CLI.

## Setup and checks

- `vendor/pip` is a git submodule: a patched `pip` fork (github.com/imranur-rahman/pip) that resolves PyPI constraints as of a date. Clone with `--recurse-submodules`. Make changes to it in the fork.
- Locally, run every command inside the venv: `source metrics_venv/bin/activate && <command>`.
- Tests: `pytest tests/ -q`. CI runs them on ubuntu, macos, and windows with Python 3.10, 3.11, and 3.12.
- CI lints `dependency_metrics/` only, with `black --check`, `flake8 --max-line-length=100`, and `mypy --ignore-missing-imports`. Run `black dependency_metrics/` before each commit.

## Tests run offline

Tests get all registry, deps.dev, and OSV data from fixtures. Inject the data through constructors:

- Each resolver takes its `ResolverCache`. `DepsDevResolver` also takes its `DepsDevClient`.
- `DependencyAnalyzer` takes `resolver=`.
- CLI tests use the `in_process_executor` fixture (`tests/conftest.py`), so mocks reach the worker functions.

Turn each bug fix into a regression test. `TESTING.md` maps each test type to its files; update it when you add a regression test or a metamorphic relation.

## Architecture

- `analyzer.py` holds the interval timeline and the TTU/TTR scoring. It reads package data only through the `PackageResolver` protocol (`interfaces.py`). Registry and HTTP code belongs in the resolvers.
- To add an ecosystem that deps.dev indexes, extend `_SYSTEM_TO_ECOSYSTEM` in `depsdev_resolver.py` and add a constraint matcher for that ecosystem.
- To add a native ecosystem:
  1. Write a new resolver that implements `PackageResolver`.
  2. Select it in `DependencyAnalyzer.__init__`.
  3. Add it to `_VALID_ECOSYSTEMS` and the ecosystem maps in `cli.py`.
  4. Check the version-ordering branches in `analyzer.py`. They special-case npm and use PEP 440 for the other ecosystems.

## Metric semantics that are intended

These results look like bugs, but they are correct.

- Remediation is fix-gated. An interval counts against MTTR only if a fixed version was published by the interval start.
- The OSV index keeps only ranges with a `fixed` event. A vulnerability with no fix does not affect MTTR.
- A dependency with no version constraint always resolves to the newest version, so its MTTR is close to 0.
- With `--weighting-type disable`, TTU and TTR are sums of days. With weights, they are weighted means. These are different quantities.
- `--per-release` builds one continuous timeline per dependency. All `dependency_frames` sit on the result of the last release.
- Single-package mode reads `osv_database.parquet` from `--output-dir`. If the file is missing, every MTTR is 0. `--build-osv` downloads about 2.4 GB.

## Cache rules

`ResolverCache` (`<output-dir>/cache/cache.db`) holds two kinds of data:

- Per-version data never changes, so the cache keeps it forever.
- Version listings grow with each release. The tool fetches a listing again when the end date is later than the newest cached release.

When you add a cached lookup, decide which of the two kinds it is.

## Scale requirements

Bulk runs over thousands of packages take hours to days. A change in `cli.py` must keep `--resume`, `--max-worker-memory-mb`, and `--package-timeout` working. The memory budgets for the in-process caches are in `cache_config.py`.

## Paper

`mttu-mttr-tool-paper/` is a separate git repository that pushes to Overleaf. This repository ignores it.

## Agent skills

### Issue tracker

Issues and specs live in GitHub Issues on `imranur-rahman/dependency-update-metrics`; use the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

The default five labels: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: `GLOSSARY.md` and `docs/adr/` at the repo root. See `docs/agents/domain.md`.
