# MTTX for VS Code

MTTX shows two values next to each dependency in your manifest:

- **MTTU** (mean time to update): how many days the dependency takes to update its own dependencies.
- **MTTR** (mean time to remediate): how many days the dependency keeps a vulnerable version of its own dependencies after a fix is available.

The Extension runs the open-source Tool [`dependency-metrics`](https://github.com/imranur-rahman/dependency-update-metrics) on your machine. It needs no account and no server of ours.

The Extension reads `package.json` (npm), `pyproject.toml` (PyPI), and `Cargo.toml` (Cargo). It reads every dependency section, which includes the dev dependencies and the optional dependencies.

## Setup

1. Install Python 3.10 or later. If the Extension finds no such Python, it shows one message with an install link.
2. Install the Extension from the VS Code Marketplace or from Open VSX. The extension ID is `imranur-rahman.mttx`.
3. Open a manifest. On the first run, the Extension makes its own Python environment in its global storage folder. Then it installs the Tool there from PyPI. Your project environments stay unchanged.
4. The Extension asks you once before it downloads the OSV index (see below).

To use a Tool that you installed yourself, set `mttx.toolPath` to the path of your `dependency-metrics`. Then the Extension does not make its own Python environment.

In Remote SSH, WSL, and Codespaces, the Extension runs on the machine that holds the workspace. So it uses the Python and the cache of that machine.

## What you see

- **Inlay hint:** each dependency line ends with its MTTU and MTTR in days. The hint shows "computing…" while the Tool runs.
- **Hover:** the hover on a dependency shows the time window, the number of dependencies of that package, the data source, the date of the value, and the manifest section. From the hover, you can start a Comparison.
- **No dependencies:** a package that has no dependencies of its own gets the text `could not compute since '<package>' has no dependencies`. It does not get a value of 0 days.
- **Errors:** a short error shows in the hint, and the full error shows in the hover.
- **OSV index date:** while a manifest is open, the status bar item **OSV** shows the build date of the OSV index (or `none`). Click it to rebuild the index. VS Code cannot put a changing date into a command title or a setting description, so the status bar shows it, and the description of `mttx.cacheFolder` names the item.

By default, the values cover the last 2 years. The Extension reuses a value for one day. It analyzes a manifest only when you open it, and it skips `node_modules/` and `.venv/`. When another program changes or creates a manifest (for example `npm install` or a Git checkout), the hints update.

## Commands

Open the Command Palette and type "MTTX":

| Command (ID) | What it does |
|---|---|
| MTTX: Analyze dependencies (`mttx.analyzeDependencies`) | Starts the analysis of the open manifest. Use it when the automatic runs are off. |
| MTTX: Compare packages (`mttx.compare`) | Asks for 1 to 5 package names of one ecosystem and opens a Comparison with the same charts as the [Webapp](https://imranur-rahman.github.io/dependency-update-metrics/). The Comparison shares the queue and the one-day cache of the hints. |
| MTTX: Rebuild OSV index (`mttx.rebuildOsvIndex`) | Downloads the OSV data again, so that MTTR uses recent vulnerability data. |
| MTTX: Open Settings (`mttx.openSettings`) | Opens the Settings editor filtered to MTTX. |

## Settings

All settings are under `mttx.` in the Settings editor. Settings Sync carries them to your other machines, except the settings that hold a path on one machine.

| Setting | What it sets | Default |
|---|---|---|
| `mttx.dataSource` | The data source: `depsdev`, or `native` (the registries of the ecosystem) | `depsdev` |
| `mttx.windowYears` | The window in years | 2 |
| `mttx.startDate`, `mttx.endDate` | The start date and the end date of the window | empty |
| `mttx.weightingType` | The weighting type: `disable`, `linear`, `exponential`, or `inverse` | `disable` |
| `mttx.halfLife` | The half-life of the exponential weighting | empty |
| `mttx.toolPath` | The path of the Tool | empty: the Extension installs the Tool |
| `mttx.cacheFolder` | The cache folder | empty: the global storage of the Extension |
| `mttx.cacheSizeLimitGb` | The size limit of the cache, in GB | 5 |
| `mttx.packageTimeoutSeconds` | The package timeout, in seconds | 600 |
| `mttx.parallelRuns` | The most Tool processes that run at a time | 2 |
| `mttx.automaticRuns` | Run the Tool when you open a manifest | on |

The hints update after you change a setting.

## The OSV index

MTTR needs the OSV index, a local copy of the vulnerability data of [OSV](https://osv.dev). To build it, the Tool downloads the OSV files for npm, PyPI, and crates.io from `storage.googleapis.com/osv-vulnerabilities`. The prompt names the size of the download. [`AGENTS.md`](https://github.com/imranur-rahman/dependency-update-metrics/blob/main/AGENTS.md) gives the measured size of each file.

The Extension asks before the first download. If you decline, the hints show MTTU, and they mark MTTR as "needs the OSV index". Run "MTTX: Rebuild OSV index" to get recent vulnerability data later. After each build, the Extension computes the values again, so a value from before the build does not show as the MTTR of the new index.

## Privacy

- The Extension collects **no telemetry**.
- With the default data source, the Tool sends the names of the packages that it analyzes only to [deps.dev](https://deps.dev). The OSV download sends no package names: the Tool downloads the full files for each ecosystem.
- If you select the native registries as the data source, the Tool sends the package names to the registry of the ecosystem (registry.npmjs.org, pypi.org, or crates.io) and not to deps.dev.
- The first setup downloads the Tool and its Python packages from PyPI.

Nothing else leaves your machine. The values and the cache stay in the storage folder of the Extension or in the cache folder that you set.

## Links

- Source code and issues: <https://github.com/imranur-rahman/dependency-update-metrics>
- The Tool on PyPI: <https://pypi.org/project/dependency-update-metrics/>
- License: MIT (see `LICENSE`)

## Publishing (for maintainers)

A pushed tag `extension-v<version>` starts the job `publish` in `.github/workflows/extension.yml`. The job runs the tests, packages one `.vsix` with `vsce`, and publishes the same file to the VS Code Marketplace (`vsce`) and to Open VSX (`ovsx`). The tag must match `version` in `extension/package.json`, for example `extension-v0.1.0` for `0.1.0`.

```sh
git tag extension-v0.1.0
git push origin extension-v0.1.0
```

Do not make a GitHub Release for an `extension-v*` tag. The Tool CI (`ci.yml`) skips such a release, but a plain tag is all that the Extension needs.

The job stops before it packages if a registry has no credential. Each registry takes trusted publishing (no stored token) or a token secret. If both are set, the job uses trusted publishing.

| Registry | Trusted publishing (preferred) | Token (fallback) |
|---|---|---|
| VS Code Marketplace | Repository variable `VSCE_TRUSTED_PUBLISHING` = `true`, and a trusted publishing policy on the Marketplace for this repository and `.github/workflows/extension.yml`. The job runs `vsce publish --oidc`. | Secret `VSCE_PAT`. Azure DevOps retires global PATs on 2026-12-01, so this token stops working on that day. |
| Open VSX | Repository variable `OVSX_TRUSTED_PUBLISHING` = `true`, and a trusted publisher at <https://open-vsx.org/user-settings/trusted-publishers> for this repository and `.github/workflows/extension.yml`. The job runs `ovsx publish --trusted-publishing`. | Secret `OVSX_PAT` (an access token from <https://open-vsx.org/user-settings/tokens>). |

One-time setup:

1. Marketplace: create the publisher `imranur-rahman` at <https://marketplace.visualstudio.com/manage>. The ID cannot change later.
2. Marketplace: add the trusted publishing policy, or make a PAT (Azure DevOps, Organization "All accessible organizations", scope Marketplace > Manage).
3. Open VSX: make an eclipse.org account, sign in to open-vsx.org with GitHub, link the Eclipse account, and accept the Publisher Agreement.
4. Open VSX: make an access token and create the namespace once: `npx ovsx create-namespace imranur-rahman -p <token>`.
5. Open VSX: claim the namespace through the [Namespace Access](https://github.com/eclipse/openvsx/wiki/Namespace-Access) process, because a new namespace gives no exclusive rights.
6. Open VSX: add the trusted publisher, or store the token as the secret `OVSX_PAT`.
7. GitHub: set the repository variables or the secrets from the table above.

To check the package locally, run `npm run package` in `extension/`. Then run `npx @vscode/vsce@4.0.0 ls --no-dependencies` to list the files in the package.
