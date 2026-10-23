# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Container image `ghcr.io/imranur-rahman/mttx` with pinned dependencies and the patched pip; the `<version>-osv-<YYYY-MM-DD>` tag holds the OSV index of that date.
- `DEPENDENCY_METRICS_OUTPUT_DIR` sets the default of `--output-dir`, and `DEPENDENCY_METRICS_OSV_INDEX` names the OSV index file.

## [0.2.1] - 2026-10-24

### Added
- A container image of the Tool on GitHub Container Registry, `ghcr.io/imranur-rahman/mttx`, for amd64 and arm64. Each release pushes two tags: `<version>` without an OSV index, and `<version>-osv-<YYYY-MM-DD>` with the OSV index of that date inside. The image holds the patched pip at the commit of the release and pins every Python dependency. See `image/README.md`.
- `DEPENDENCY_METRICS_OUTPUT_DIR` sets the default of `--output-dir`, and `DEPENDENCY_METRICS_OSV_INDEX` names the OSV index file.

### Changed
- The release job checks the tag against `pyproject.toml` before the PyPI publish.

## [0.2.0] - 2026-10-23

### Added
- `--json` prints one result as JSON on stdout, also for a failed package (exit code 1).
- `--osv-index-date` prints the build date of the OSV index.
- Public `analyze()` API with a progress callback, used by the hosted web version.
- Per-release mode results with a status for each release (`ok`, `no_deps`, `error`).
- `--build-osv` builds the OSV index from the npm, PyPI, and crates.io zips.
- Cache size flags (`--cache-prune-gb`, `--cache-clear`) and cache use tracking.

### Changed
- deps.dev failures now raise errors instead of returning empty data.
- One map from ecosystem to deps.dev system.

## [0.1.0] - 2025-11-11

### Added
- Initial release of dependency-metrics tool
- Support for npm and PyPI ecosystems
- Time-to-Update (TTU) metric calculation
- Time-to-Remediate (TTR) metric calculation
- Multiple weighting schemes (linear, exponential, inverse, disable)
- OSV (Open Source Vulnerabilities) database integration
- Command-line interface with argparse
- Export capabilities (JSON, CSV, Excel)
- Comprehensive README and documentation
- Example usage scripts
- Unit tests
- PyPI package structure

### Features
- Analyze package dependencies over custom date ranges
- Fetch package metadata from npm and PyPI registries
- Build timeline of version releases
- Resolve dependency versions at specific points in time
- Calculate weighted and unweighted metrics
- Export detailed worksheets for each dependency
- Filter OSV vulnerability data by ecosystem and package

## [Unreleased]

### Changed
- `--depsdev` runs no longer hide a failed deps.dev request. Before, a failed GetPackage request gave an empty version list and a failed GetRequirements request gave no dependencies, so the result showed an MTTU and MTTR of 0. Now, after the retries of the HTTP session (connection errors, 429, and 5xx), the failure stops the analysis of the package: a single-package run and a bulk row get `status="error"` with the message, and `--resume` analyzes that bulk row again. The cache never keeps a failed lookup. A 404 on GetPackage raises `dependency_metrics.errors.PackageNotFoundError`; a dependency that deps.dev does not know is still skipped. A window without a release raises `NoReleaseInWindowError` (a `ValueError`).
- `--per-release`: a release whose manifest (dependency list) cannot be read gets a row with `status="error"`, the message, and no MTTU or MTTR. Before, the release counted as a release without dependencies, with an MTTU of 0. The other releases keep their values.

### Added
- `--write-dependency-details` CLI flag: opt in to writing per-interval dependency detail CSVs for bulk and per-release runs; these files are skipped by default.
- `--per-release` CLI flag: computes MTTU/MTTR at every release of the parent package within the analysis window, producing one output row per release instead of one per input row.
- `DependencyAnalyzer.analyze_at_release_points()`: new method that fetches per-version dependency sets, builds a shared dep-cache for the full window, and slices it for each release sub-window `[start_date, release_date]`.
- `DependencyAnalyzer._calculate_weight_with_window()`: private helper that computes interval weights using the sub-window span as `max_age` (avoids mutating `self.end_date` for each release point).
- `export_per_release_summary_csv()` and `export_per_release_dependency_csv()` in `reporting.py`.
- Resume support for per-release mode: tracks completed `(ecosystem, package_name, window_start, package_version)` entries and skips duplicates at write time.
- 5 new unit tests in `tests/test_per_release.py` (no network calls).

### Planned
- Support for additional ecosystems (Maven, RubyGems, Go, etc.)
- Web dashboard for visualization
- GitHub Actions integration
- Docker containerization
- Support for custom vulnerability databases
- Comparative analysis across packages
