# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

### Added
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
