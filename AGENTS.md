# AGENTS.md

`dependency-metrics` computes MTTU and MTTR (mean time to update / to remediate) for the direct dependencies of an npm, PyPI, or Cargo package. `README.md` and `USAGE.md` describe the CLI.

## Repository layout

The repository holds the five parts that `GLOSSARY.md` defines. Each part has its own folder and its own CI workflow, and each workflow runs only when a file of its part changes.

| Part | Folders | CI workflow |
|---|---|---|
| Tool | `dependency_metrics/`, `tests/`, `pyproject.toml`, `vendor/`, `image/` (the container image `ghcr.io/imranur-rahman/mttx`, see `image/README.md`) | `.github/workflows/ci.yml` (also builds the image on each push and PR; on a GitHub Release, except for an `extension-v*` or `action-v*` tag, publishes to PyPI and then pushes the image to GHCR; keep the file name, because the PyPI trusted publisher names it) |
| Host | `host/` | `.github/workflows/host.yml` (also runs when `dependency_metrics/` changes, because the Host runs the Tool) |
| Webapp | `webapp/site/` (published), `webapp/tests/`, `webapp/sample/` (the script that makes the Sample Comparison) | `.github/workflows/webapp.yml` (tests with `node --test webapp/tests/*.test.mjs`, then publishes `webapp/site/` to GitHub Pages from `main`) |
| Extension | `extension/` | `.github/workflows/extension.yml` (also runs when `contract/`, `webapp/site/static/js/comparison.js`, or `webapp/site/static/js/explanation.js` changes; unit tests with `npm run test:unit`, then one VS Code integration test, on ubuntu, macos, and windows; a pushed tag `extension-v*` also publishes to the Marketplace and Open VSX, see `extension/README.md`) |
| Action | `action/` (the GitHub Action; it imports the Extension core parsers and the Webapp Explanation) | `.github/workflows/github-action.yml` (also runs when `contract/`, the Extension core, the Extension fake Tool, `comparison.js`, or `explanation.js` changes; unit tests with `npm test`, an end-to-end test of the bundle on the fixture PR files, and a run of `action.yml` with the fake Tool; a pushed tag `action-v*` also copies the Action to `imranur-rahman/mttx-action`, see `action/README.md`) |

`contract/examples/` holds example bodies of the Host API. The Host tests, the Webapp tests, the Extension tests, and the Action tests read them, so a change to the API contract fails a test on each side. A change in `contract/` runs the Host CI, the Webapp CI, the Extension CI, and the Action CI.

`docs/` holds the ADRs (`docs/adr/`), the agent docs (`docs/agents/`), and the cited research that ADRs depend on (`docs/research/`). GitHub Pages does not publish `docs/`. Only the files in `webapp/site/` are public on the site.

## Setup and checks

- `vendor/pip` is a git submodule: a patched `pip` fork (github.com/imranur-rahman/pip) that resolves PyPI constraints as of a date. Clone with `--recurse-submodules`. Make changes to it in the fork.
- Locally, run every command inside the venv: `source metrics_venv/bin/activate && <command>`.
- Tests: `pytest tests/ -q`. CI runs them on ubuntu, macos, and windows with Python 3.10, 3.11, and 3.12.
- The Tool CI lints `dependency_metrics/` only, with `black --check`, `flake8 --max-line-length=100`, and `mypy --ignore-missing-imports`. Run `black dependency_metrics/` before each commit.
- The deploy files in `host/` (scripts, `compose.yaml`, `Caddyfile`) run only on the VM. Check a change to them with `bash host/check-deploy-files.sh` (needs shellcheck and docker).

## Tests run offline

Tests get all registry, deps.dev, and OSV data from fixtures. Inject the data through constructors:

- Each resolver takes its `ResolverCache`. `DepsDevResolver` also takes its `DepsDevClient`.
- `DependencyAnalyzer` takes `resolver=`.
- CLI tests use the `in_process_executor` fixture (`tests/conftest.py`), so mocks reach the worker functions.

Turn each bug fix into a regression test. `TESTING.md` maps each test type to its files; update it when you add a regression test or a metamorphic relation.

## Architecture

- `analyzer.py` holds the interval timeline and the TTU/TTR scoring. It reads package data only through the `PackageResolver` protocol (`interfaces.py`). Registry and HTTP code belongs in the resolvers.
- To add an ecosystem that deps.dev indexes, extend `_SYSTEM_TO_ECOSYSTEM` in `depsdev_resolver.py` and add a constraint matcher for that ecosystem. `ECOSYSTEM_TO_SYSTEM` (the one map that `api.py` and `cli.py` use through `depsdev_system()`; the Host goes through `analyze()`) comes from it; add other names of an ecosystem, such as `crates.io`, there.
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
- Single-package mode reads `osv_database.parquet` from `--output-dir`, or the file that `DEPENDENCY_METRICS_OSV_INDEX` names (the `-osv-` image sets it). If the file is missing, every MTTR is 0. `--build-osv` downloads the OSV zips for npm, PyPI, and crates.io, about 260 MB in total (219 MB, 37 MB, and 3.6 MB on 2026-10-21). It reads the records directly from the zips and replaces the index in one rename. `--build-osv` without `--package` only rebuilds the index. `OSVBuilder.index_date()` returns the build date that the index file holds.

## Cache rules

`ResolverCache` (`<output-dir>/cache/cache.db`) holds two kinds of data:

- Per-version data never changes, so the cache keeps it until a prune (`--cache-prune-gb`) or a clear (`--cache-clear`) removes it.
- Version listings grow with each release. The tool fetches a listing again when the end date is later than the newest cached release.

When you add a cached lookup, decide which of the two kinds it is.

Each row records in `last_used` the day of its last read or write. A read writes that day only when the day changes, so most reads write nothing. A prune removes the rows with the oldest `last_used` first, and does not run `VACUUM`.

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
