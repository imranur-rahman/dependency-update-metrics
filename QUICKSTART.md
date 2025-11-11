# Quick Start Guide

Get started with dependency-metrics in 5 minutes!

## Installation

```bash
# Clone the repository
git clone https://github.com/imranur-rahman/dependency-update-metrics.git
cd dependency-update-metrics

# Install dependencies
pip install -r requirements.txt

# Install the package
pip install -e .
```

Or use the setup script:

```bash
chmod +x scripts/setup.sh
./scripts/setup.sh
```

## First Analysis

Analyze an npm package:

```bash
dependency-metrics --ecosystem npm --package express
```

This will:
1. Fetch package metadata from npm registry
2. Extract all dependencies
3. Analyze update patterns over time
4. Calculate Time-to-Update (TTU) and Time-to-Remediate (TTR) metrics
5. Save results to `./output/express_results.json`

## Understanding the Output

```
============================================================
ANALYSIS RESULTS
============================================================
Package: express
Ecosystem: npm
Period: 1900-01-01 to 2025-11-11
Weighting: disable
------------------------------------------------------------
Average Time-to-Update (TTU): 45.32 days
Average Time-to-Remediate (TTR): 12.15 days
Number of dependencies: 30
============================================================
```

**What does this mean?**

- **TTU (45.32 days)**: On average, dependencies stayed 45 days behind the latest version
- **TTR (12.15 days)**: On average, vulnerabilities remained unpatched for 12 days
- **30 dependencies**: The package has 30 dependencies that were analyzed

## Common Use Cases

### 1. Compare packages

```bash
dependency-metrics --ecosystem npm --package react
dependency-metrics --ecosystem npm --package vue
dependency-metrics --ecosystem npm --package angular
```

### 2. Analyze over specific time period

```bash
dependency-metrics \
  --ecosystem npm \
  --package express \
  --start-date 2022-01-01 \
  --end-date 2023-12-31
```

### 3. Use exponential weighting (prioritize recent data)

```bash
dependency-metrics \
  --ecosystem npm \
  --package express \
  --weighting-type exponential \
  --half-life 180
```

This gives more weight to recent time periods, with weight halving every 180 days.

### 4. Get detailed worksheets

```bash
dependency-metrics \
  --ecosystem npm \
  --package express \
  --get-worksheets
```

Creates `./output/express_worksheets.xlsx` with a sheet for each dependency showing:
- Time intervals
- Resolved versions
- Update status
- Vulnerability status
- Weights applied

### 5. Analyze PyPI packages

```bash
dependency-metrics --ecosystem pypi --package requests
```

## Next Steps

1. **Explore examples**: Run `python examples/usage_examples.py`
2. **Read documentation**: See `USAGE.md` for detailed guide
3. **Build OSV database**: Run with `--build-osv` for vulnerability analysis
4. **Customize analysis**: Try different weighting schemes

## Common Issues

### "npm: command not found"

Install Node.js and npm from https://nodejs.org/

### "No versions found before [date]"

The package may not have existed before your end-date. Try a more recent date.

### Network timeouts

Check your internet connection. Some registries may have rate limiting.

## Getting Help

- Documentation: `README.md`, `USAGE.md`, `PROJECT_STRUCTURE.md`
- Examples: `examples/usage_examples.py`
- Issues: https://github.com/imranur-rahman/dependency-update-metrics/issues

## Tips for Best Results

1. **Use realistic date ranges**: Analyzing from 1900 will take longer than necessary
2. **Start simple**: Begin with `--weighting-type disable` before trying other schemes
3. **Check output directory**: Results are saved in `./output/` by default
4. **Be patient**: First run may take time to fetch all metadata

Happy analyzing! 🚀
