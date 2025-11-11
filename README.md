# Dependency Update Metrics

A Python tool for analyzing time-to-update (TTU) and time-to-remediate (TTR) metrics for package dependencies across different ecosystems (npm, PyPI).

## Features

- **Multi-ecosystem support**: Analyze packages from npm and PyPI
- **Time-to-Update (TTU)**: Measure how long dependencies stay behind the latest version
- **Time-to-Remediate (TTR)**: Measure how long known vulnerabilities remain unpatched
- **Flexible weighting**: Support for linear, exponential, inverse, or no weighting
- **OSV integration**: Uses the Open Source Vulnerabilities database for security analysis
- **Export capabilities**: Export detailed worksheets and OSV data

## Installation

### From PyPI (when published)

```bash
pip install dependency-metrics
```

### From source

```bash
git clone https://github.com/imranur-rahman/dependency-update-metrics.git
cd dependency-update-metrics
pip install -e .
```

## Usage

### As a command-line tool

```bash
# Basic usage
dependency-metrics --ecosystem npm --package express

# With date range
dependency-metrics --ecosystem npm --package express \
  --start-date 2020-01-01 \
  --end-date 2023-12-31

# With exponential weighting
dependency-metrics --ecosystem npm --package express \
  --weighting-type exponential \
  --half-life 180

# Build OSV database first (one-time operation)
dependency-metrics --ecosystem npm --package express --build-osv

# Export detailed worksheets
dependency-metrics --ecosystem npm --package express --get-worksheets

# Get OSV vulnerability data
dependency-metrics --ecosystem npm --package express --get-osv
```

### As a Python module

```python
from datetime import datetime
from pathlib import Path
from dependency_metrics.analyzer import DependencyAnalyzer

# Create analyzer
analyzer = DependencyAnalyzer(
    ecosystem="npm",
    package="express",
    start_date=datetime(2020, 1, 1),
    end_date=datetime(2023, 12, 31),
    weighting_type="exponential",
    half_life=180,
    output_dir=Path("./output")
)

# Run analysis
results = analyzer.analyze()

print(f"Average TTU: {results['ttu']:.2f} days")
print(f"Average TTR: {results['ttr']:.2f} days")
```

## Command-line Arguments

- `--ecosystem`: Ecosystem to analyze (`npm` or `pypi`) [Required]
- `--package`: Package name to analyze [Required]
- `--start-date`: Start date for analysis (YYYY-MM-DD) [Default: 1900-01-01]
- `--end-date`: End date for analysis (YYYY-MM-DD) [Default: today]
- `--weighting-type`: Weighting method (`linear`, `exponential`, `inverse`, `disable`) [Default: disable]
- `--half-life`: Half-life in days (required for exponential weighting)
- `--build-osv`: Build/update the OSV vulnerability database
- `--get-osv`: Export OSV data for the package's dependencies
- `--get-worksheets`: Export detailed analysis worksheets to Excel
- `--output-dir`: Output directory for results [Default: ./output]

## Weighting Methods

### Disable (default)
No weighting applied. All time periods are weighted equally.

### Linear
Weight decreases linearly with age:
```
weight = 1 - (age / max_age)
```

### Exponential
Weight decreases exponentially based on half-life:
```
weight = exp(-λ * age)
where λ = ln(2) / half_life
```

### Inverse
Weight is inversely proportional to age:
```
weight = 1 / (1 + age)
```

## Output

The tool generates several outputs in the specified output directory:

1. **JSON results file**: Contains TTU, TTR, and metadata
2. **OSV data** (with `--get-osv`): CSV file with vulnerability information
3. **Excel worksheets** (with `--get-worksheets`): Detailed analysis for each dependency

### Example JSON output

```json
{
  "package": "express",
  "ecosystem": "npm",
  "version": "4.18.2",
  "start_date": "2020-01-01T00:00:00",
  "end_date": "2023-12-31T00:00:00",
  "weighting_type": "exponential",
  "half_life": 180,
  "ttu": 45.32,
  "ttr": 12.15,
  "num_dependencies": 30
}
```

## How It Works

1. **Package Analysis**: Fetches package metadata from the registry
2. **Dependency Extraction**: Extracts dependencies from the package version closest to the end date
3. **Timeline Construction**: Creates a timeline of version releases for each dependency
4. **Version Resolution**: Resolves dependency versions at each time interval
5. **Metric Calculation**:
   - **TTU**: Measures time when dependency is not at the highest available version
   - **TTR**: Measures time when dependency has known vulnerabilities
6. **Weighting**: Applies optional time-based weighting to prioritize recent periods
7. **Aggregation**: Averages metrics across all dependencies

## Requirements

- Python 3.8+
- npm CLI (for npm ecosystem analysis)
- Internet connection (for fetching package metadata and OSV data)

## Development

### Setup development environment

```bash
git clone https://github.com/imranur-rahman/dependency-update-metrics.git
cd dependency-update-metrics
pip install -e ".[dev]"
```

### Run tests

```bash
pytest
```

### Code formatting

```bash
black dependency_metrics/
```

## Publishing to PyPI

### Prepare for publishing

```bash
# Install build tools
pip install build twine

# Build distribution
python -m build

# Check distribution
twine check dist/*
```

### Upload to PyPI

```bash
# Upload to Test PyPI first
twine upload --repository testpypi dist/*

# If all looks good, upload to PyPI
twine upload dist/*
```

## License

MIT License - see LICENSE file for details

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Citation

If you use this tool in your research, please cite:

```bibtex
@software{dependency_metrics,
  author = {Rahman, Imranur},
  title = {Dependency Update Metrics},
  year = {2025},
  url = {https://github.com/imranur-rahman/dependency-update-metrics}
}
```

## Acknowledgments

- Uses data from [Open Source Vulnerabilities (OSV)](https://osv.dev/)
- Package metadata from [npm Registry](https://registry.npmjs.org/) and [PyPI](https://pypi.org/)
