# MTTX: Dependency Update Metrics

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.23608337.svg)](https://doi.org/10.5281/zenodo.23608337)

MTTX measures how quickly the developers of an open-source package respond to new dependency releases and to security fixes. For the direct dependencies of an npm, PyPI, or Cargo package, it computes:

- **MTTU (Mean Time To Update):** how long the dependencies stay behind their newest version.
- **MTTR (Mean Time To Remediate):** how long the dependencies keep a known vulnerability after a fix is available.

MTTX resolves each version constraint as of a past date, so a value never uses data from the future.

**Project homepage:** <https://imranur-rahman.github.io/dependency-update-metrics/>. It has the interactive dashboard, the demo video, and the links to every part.

## Use MTTX

| Where | How |
|---|---|
| Interactive dashboard | Compare up to five packages on the [project homepage](https://imranur-rahman.github.io/dependency-update-metrics/), with no installation. |
| IDE extension | Install `imranur-rahman.mttx` from the [Visual Studio Marketplace](https://marketplace.visualstudio.com/items?itemName=imranur-rahman.mttx) or [Open VSX](https://open-vsx.org/extension/imranur-rahman/mttx) (Cursor, Windsurf, VSCodium). |
| GitHub Action | `uses: imranur-rahman/mttx-action@v1`, from the [GitHub Marketplace](https://github.com/marketplace/actions/mttx-dependency-update-metrics). See [action/README.md](action/README.md). |
| CLI | `pip install dependency-update-metrics`, from [PyPI](https://pypi.org/project/dependency-update-metrics/). |
| Container image | `ghcr.io/imranur-rahman/mttx`, on [GHCR](https://github.com/imranur-rahman/dependency-update-metrics/pkgs/container/mttx). See [image/README.md](image/README.md). |

## Quick start

```bash
pip install dependency-update-metrics

# MTTU and MTTR of one package over a time window (the first run builds the OSV index)
dependency-metrics --ecosystem npm --package express \
  --start-date 2024-01-01 --end-date 2025-12-31 --build-osv

# The same run in the container image, with the OSV index of that date inside
docker run --rm -v "$PWD:/data" ghcr.io/imranur-rahman/mttx:0.2.1-osv-2026-10-23 \
  --ecosystem npm --package express --start-date 2024-01-01 --end-date 2025-12-31 --depsdev
```

[USAGE.md](USAGE.md) lists every option: batch runs from a CSV file, `--per-release`, weighting, `--json`, worksheets, and the cache. [ALGORITHM.md](ALGORITHM.md) describes how MTTU and MTTR are computed.

## Project structure

```text
dependency-update-metrics/
├── dependency_metrics/   The Tool: the dependency-metrics CLI and Python package
├── tests/                Tool tests: unit, integration, regression, and metamorphic
├── vendor/pip/           Patched pip fork (git submodule) that resolves PyPI constraints as of a date
├── image/                Container image of the Tool on GHCR
├── host/                 Server API, Job queue, and workers behind the interactive dashboard
├── webapp/               Interactive dashboard and project homepage (GitHub Pages)
├── extension/            IDE extension for VS Code and Open VSX, with a demo project
├── action/               GitHub Action, copied to imranur-rahman/mttx-action on release
├── contract/examples/    Shared JSON examples of the result format, which the tests of each part read
├── case_study/           Inputs and scripts of the manual validation of 90 packages
└── docs/                 Architecture decision records and cited research
```

## Development

```bash
git clone --recurse-submodules https://github.com/imranur-rahman/dependency-update-metrics.git
cd dependency-update-metrics
pip install -e ".[dev]"
pytest tests/ -q
```

[TESTING.md](TESTING.md) maps each test type to its tests, and [AGENTS.md](AGENTS.md) gives the checks and the CI workflow of each part.

## Citation

```bibtex
@software{rahman2026dependencyupdatemetric,
  title  = {Beyond Stars: {MTTX} Brings Developer Responsiveness to Dependency Decisions, from Dashboard to {IDE} to {CI}},
  author = {Rahman, Imranur and Paramitha, Ranindya and Tamanna, Mahzabin and Williams, Laurie},
  year   = {2026},
  doi    = {10.5281/zenodo.23608337},
  url    = {https://github.com/imranur-rahman/dependency-update-metrics}
}
```

## License

MIT. See [LICENSE](LICENSE).
