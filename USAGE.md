# Installation and Usage Guide

## Installation

### Prerequisites

- Python 3.8 or higher
- npm CLI (for npm ecosystem analysis)
- Internet connection

### Install from source

1. Clone the repository:
```bash
git clone https://github.com/imranur-rahman/dependency-update-metrics.git
cd dependency-update-metrics
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Install in development mode:
```bash
pip install -e .
```

### Verify installation

```bash
dependency-metrics --help
```

## Quick Start

### Analyze an npm package

```bash
dependency-metrics --ecosystem npm --package express
```

### Analyze a PyPI package

```bash
dependency-metrics --ecosystem pypi --package requests
```

## Advanced Usage

### Specify date range

```bash
dependency-metrics \
  --ecosystem npm \
  --package react \
  --start-date 2020-01-01 \
  --end-date 2023-12-31
```

### Use exponential weighting

```bash
dependency-metrics \
  --ecosystem npm \
  --package vue \
  --weighting-type exponential \
  --half-life 180
```

### Build OSV database (one-time setup)

```bash
dependency-metrics \
  --ecosystem npm \
  --package express \
  --build-osv
```

### Export detailed worksheets

```bash
dependency-metrics \
  --ecosystem npm \
  --package lodash \
  --get-worksheets
```

### Export OSV vulnerability data

```bash
dependency-metrics \
  --ecosystem npm \
  --package axios \
  --get-osv
```

### Custom output directory

```bash
dependency-metrics \
  --ecosystem npm \
  --package next \
  --output-dir ./my-results
```

### Bulk CSV input (parallel processing)

Input CSV must include `ecosystem`, `package_name`, `end_date` and can include `start_date`.
Extra columns are allowed. The tool automatically removes duplicate rows by
`ecosystem`, `package_name`, and `end_date`.
Bulk mode computes metrics per package using the latest dependency set.

```bash
dependency-metrics \
  --input-csv ./input.csv \
  --workers 8 \
  --output-dir ./output
```

Bulk outputs:
- `<input>_bulk_results.csv` (summary)
- `<input>_dependency_details.csv` (per-interval dependency data)

## Understanding the Output

### Console Output

After analysis, you'll see output like:

```
============================================================
ANALYSIS RESULTS
============================================================
Package: express
Ecosystem: npm
Period: 2020-01-01 to 2023-12-31
Weighting: exponential
Half-life: 180.0 days
------------------------------------------------------------
Average Time-to-Update (TTU): 45.32 days
Average Time-to-Remediate (TTR): 12.15 days
Number of dependencies: 30
============================================================

Results saved to: ./output/express_results.json
```

### JSON Results File

The JSON file contains detailed results:

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

## Architecture Notes

- Ecosystem resolution lives in `dependency_metrics/resolvers.py`.
- OSV remediation checks are handled by `dependency_metrics/osv_service.py`.
- Reporting/export helpers live in `dependency_metrics/reporting.py`.
- Bulk CSV mode groups rows by `(ecosystem, package_name)` and processes each group sequentially while running groups in parallel to maximize cache reuse.
- For bulk runs, a single package analysis computes metrics across multiple end dates using the latest dependency set.

### Excel Worksheets

When using `--get-worksheets`, you'll get an Excel file with one sheet per dependency containing:

- `ecosystem`: The ecosystem (npm/pypi)
- `package`: Your package name
- `package_version`: The analyzed version
- `dependency`: Dependency name
- `dependency_constraint`: Version constraint from package.json/requirements
- `dependency_version`: Resolved version at interval start
- `dependency_highest_version`: Highest available version at interval start
- `interval_start`: Start of the time interval
- `interval_end`: End of the time interval
- `updated`: Whether dependency was at latest version
- `remediated`: Whether dependency had no unpatched vulnerabilities
- `age_of_interval`: Days from interval_start to end_date
- `weight`: Calculated weight for the interval

### OSV Data

The OSV CSV contains vulnerability information:

- `vul_id`: Vulnerability ID (e.g., CVE-2023-XXXX)
- `ecosystem`: Ecosystem name
- `package`: Package name
- `vul_introduced`: Version where vulnerability was introduced
- `vul_fixed`: Version where vulnerability was fixed

## Weighting Schemes

### Disable (default)

All time periods are weighted equally. Use this for unweighted analysis.

```bash
dependency-metrics --ecosystem npm --package express --weighting-type disable
```

### Linear

Recent periods are weighted more heavily, with weight decreasing linearly.

```bash
dependency-metrics --ecosystem npm --package express --weighting-type linear
```

Formula: `weight = 1 - (age / max_age)`

### Exponential

Weight decreases exponentially based on half-life. More aggressive decay than linear.

```bash
dependency-metrics --ecosystem npm --package express \
  --weighting-type exponential \
  --half-life 180
```

Formula: `weight = exp(-λ * age)` where `λ = ln(2) / half_life`

### Inverse

Weight is inversely proportional to age.

```bash
dependency-metrics --ecosystem npm --package express --weighting-type inverse
```

Formula: `weight = 1 / (1 + age)`

## Troubleshooting

### npm CLI not found

If you see errors about npm not being found:

1. Install Node.js and npm: https://nodejs.org/
2. Verify installation: `npm --version`

### Network timeouts

If you experience network timeouts:

1. Check your internet connection
2. Try again with a smaller date range
3. Some packages may have rate limiting on their registries

### OSV database build fails

If OSV database building fails:

1. Check your internet connection
2. Ensure you have sufficient disk space (~2GB for the database)
3. Try again - the download may have been interrupted

### Version resolution fails

If version resolution fails for some dependencies:

1. This may be normal for packages with complex version constraints
2. The tool will log warnings and continue with other dependencies
3. Check the output directory for detailed logs

## Performance Tips

1. **Use date ranges**: Narrow date ranges analyze faster
2. **Cache OSV database**: Build OSV database once with `--build-osv`, then reuse
3. **Limit dependencies**: Packages with fewer dependencies analyze faster
4. **Output directory**: Use local SSD for better I/O performance

## Python API Usage

You can also use the tool programmatically:

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

# Access results
print(f"TTU: {results['ttu']:.2f} days")
print(f"TTR: {results['ttr']:.2f} days")

# Access per-dependency data
for dep_name, dep_df in results['dependency_data'].items():
    print(f"\n{dep_name}:")
    print(dep_df.head())
```

## Contributing

Contributions are welcome! Please see CONTRIBUTING.md for guidelines.

## Support

- GitHub Issues: https://github.com/imranur-rahman/dependency-update-metrics/issues
- Documentation: See README.md and this guide
- Examples: Check the `examples/` directory

## License

MIT License - see LICENSE file
