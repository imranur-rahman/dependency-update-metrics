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

### Planned
- Support for additional ecosystems (Maven, RubyGems, Go, etc.)
- Caching mechanism for package metadata
- Parallel processing for faster analysis
- Web dashboard for visualization
- GitHub Actions integration
- Docker containerization
- More sophisticated version resolution strategies
- Support for custom vulnerability databases
- Historical trend analysis
- Comparative analysis across packages
