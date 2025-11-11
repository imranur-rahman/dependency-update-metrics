# Project Structure

This document explains the organization of the dependency-metrics project.

## Directory Structure

```
dependency-update-metrics/
├── .github/
│   └── workflows/
│       └── ci.yml                 # GitHub Actions CI/CD pipeline
├── dependency_metrics/            # Main package directory
│   ├── __init__.py               # Package initialization
│   ├── cli.py                    # Command-line interface
│   ├── analyzer.py               # Core analysis logic
│   └── osv_builder.py            # OSV database builder
├── tests/                        # Test suite
│   ├── __init__.py
│   └── test_basic.py            # Basic unit tests
├── examples/                     # Example scripts
│   └── usage_examples.py        # Usage examples
├── scripts/                      # Utility scripts
│   ├── publish.py               # PyPI publishing script
│   └── setup.sh                 # Setup script
├── output/                       # Output directory (created at runtime)
├── pyproject.toml               # Project metadata and build config
├── requirements.txt             # Production dependencies
├── requirements-dev.txt         # Development dependencies
├── MANIFEST.in                  # Package manifest
├── .gitignore                   # Git ignore rules
├── LICENSE                      # MIT License
├── README.md                    # Main documentation
├── USAGE.md                     # Detailed usage guide
├── CHANGELOG.md                 # Version history
└── workflow.md                  # Original workflow notes
```

## Core Components

### 1. CLI Module (`cli.py`)

The command-line interface that handles:
- Argument parsing
- Input validation
- Output formatting
- Results export

**Key Functions:**
- `main()`: Entry point for the CLI

### 2. Analyzer Module (`analyzer.py`)

The core analysis engine that:
- Fetches package metadata from registries
- Extracts dependencies
- Creates timelines
- Resolves dependency versions
- Calculates TTU and TTR metrics
- Applies weighting schemes

**Key Classes:**
- `DependencyAnalyzer`: Main analyzer class

**Key Methods:**
- `analyze()`: Run complete analysis
- `analyze_dependency()`: Analyze single dependency
- `calculate_ttu_ttr()`: Calculate metrics
- `calculate_weight()`: Apply weighting

### 3. OSV Builder Module (`osv_builder.py`)

Handles vulnerability data:
- Downloads OSV database
- Parses JSON files
- Creates vulnerability dataframe
- Filters by ecosystem and package

**Key Classes:**
- `OSVBuilder`: OSV database management

**Key Methods:**
- `build_database()`: Build complete database
- `get_vulnerabilities()`: Get package vulnerabilities

## Data Flow

```
User Input (CLI)
    ↓
Argument Parsing & Validation
    ↓
DependencyAnalyzer Initialization
    ↓
Fetch Package Metadata (npm/PyPI registry)
    ↓
Extract Dependencies & Constraints
    ↓
For Each Dependency:
    ├─ Fetch Dependency Metadata
    ├─ Build Timeline
    ├─ Resolve Versions at Each Interval
    ├─ Check Update Status
    ├─ Check Vulnerability Status (OSV)
    ├─ Calculate Weights
    └─ Calculate TTU/TTR
    ↓
Aggregate Metrics Across Dependencies
    ↓
Export Results (JSON/CSV/Excel)
    ↓
Display Summary
```

## Key Algorithms

### Timeline Construction

1. Start with `start_date` and `end_date`
2. Add all dependency version release dates within range
3. Create continuous intervals: `[t1, t2), [t2, t3), ...`

### Version Resolution

For each interval `[t_start, t_end)`:
1. Resolve constraint at `t_start`
2. Get highest available version at `t_start`
3. Compare to determine if updated

### TTU Calculation

- **Without weighting**: Sum of interval durations where `updated = False`
- **With weighting**: `Σ(weight * duration) / Σ(weight)` for `updated = False`

### TTR Calculation

Same as TTU but using `remediated` flag instead of `updated`

### Weighting Schemes

- **Disable**: `weight = 1.0` (all intervals equal)
- **Linear**: `weight = 1 - (age / max_age)`
- **Exponential**: `weight = exp(-λ * age)` where `λ = ln(2) / half_life`
- **Inverse**: `weight = 1 / (1 + age)`

## Package Distribution

### Building

```bash
python -m build
```

Creates:
- `dist/*.tar.gz` (source distribution)
- `dist/*.whl` (wheel distribution)

### Publishing

```bash
# Test PyPI
twine upload --repository testpypi dist/*

# Production PyPI
twine upload dist/*
```

### Installing

```bash
# From PyPI
pip install dependency-metrics

# From source
pip install -e .
```

## Configuration Files

### `pyproject.toml`

Modern Python package configuration:
- Project metadata
- Dependencies
- Build system
- Entry points (CLI command)
- Tool configurations (black, mypy)

### `MANIFEST.in`

Specifies additional files to include in distribution:
- README.md
- LICENSE
- Documentation files

### `.gitignore`

Excludes from version control:
- Python cache files
- Build artifacts
- Output data
- IDE files

## Testing

### Running Tests

```bash
pytest tests/ -v
```

### Coverage

```bash
pytest tests/ --cov=dependency_metrics --cov-report=html
```

### CI/CD

GitHub Actions runs:
- Tests on multiple OS and Python versions
- Linting (black, flake8, mypy)
- Build verification
- Automatic PyPI publishing on releases

## Development Workflow

1. **Clone repository**
   ```bash
   git clone https://github.com/imranur-rahman/dependency-update-metrics.git
   cd dependency-update-metrics
   ```

2. **Setup environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # or `venv\Scripts\activate` on Windows
   pip install -r requirements.txt
   pip install -r requirements-dev.txt
   pip install -e .
   ```

3. **Make changes**
   - Edit code in `dependency_metrics/`
   - Add tests in `tests/`
   - Update documentation

4. **Test changes**
   ```bash
   pytest tests/ -v
   black dependency_metrics/
   flake8 dependency_metrics/
   ```

5. **Run locally**
   ```bash
   dependency-metrics --ecosystem npm --package express
   ```

6. **Commit and push**
   ```bash
   git add .
   git commit -m "Description of changes"
   git push
   ```

## Extension Points

### Adding New Ecosystems

1. Add ecosystem to `choices` in `cli.py`
2. Implement metadata fetching in `analyzer.py`:
   - Add URL pattern to `registry_urls`
   - Implement version resolution
   - Handle ecosystem-specific metadata format
3. Update documentation

### Adding New Weighting Schemes

1. Add option to `cli.py` choices
2. Implement calculation in `calculate_weight()` method
3. Update documentation with formula

### Adding New Metrics

1. Add dataframe columns in `analyze_dependency()`
2. Implement calculation logic
3. Add to `calculate_ttu_ttr()` or create new method
4. Update output format

## Dependencies

### Production
- `pandas`: Data manipulation
- `requests`: HTTP requests to registries
- `tqdm`: Progress bars
- `packaging`: Version parsing
- `openpyxl`: Excel export
- `pyarrow`: Parquet support for OSV database

### Development
- `pytest`: Testing framework
- `pytest-cov`: Coverage reporting
- `black`: Code formatting
- `flake8`: Linting
- `mypy`: Type checking
- `build`: Package building
- `twine`: PyPI uploading

## Performance Considerations

1. **Network calls**: Main bottleneck is fetching package metadata
2. **Caching**: Consider implementing cache for repeated analyses
3. **Parallel processing**: Could parallelize per-dependency analysis
4. **Database**: OSV database uses Parquet for efficient storage

## Security Considerations

1. **Input validation**: All user inputs are validated
2. **Network security**: Uses HTTPS for all requests
3. **Dependency security**: Regularly update dependencies
4. **OSV data**: Trusted source for vulnerability data

## Future Enhancements

1. Add more ecosystems (Maven, Go, RubyGems)
2. Implement caching mechanism
3. Add visualization (charts, graphs)
4. Create web interface
5. Support custom vulnerability databases
6. Add historical trend analysis
7. Implement parallel processing
8. Add Docker support
