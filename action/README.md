# MTTX GitHub Action

The MTTX Action computes MTTU and MTTR (mean time to update and mean time to remediate) in a GitHub Actions workflow. It runs the [`dependency-metrics`](https://github.com/imranur-rahman/dependency-update-metrics) Tool for npm, PyPI, and Cargo packages. It has two targets:

- **packages**: a scheduled or manual workflow computes the MTTU and MTTR of 1 to 5 published packages. The Action writes them to the job summary, gives them as step outputs, and writes JSON files for [shields.io endpoint badges](https://shields.io/badges/endpoint-badge).
- **pull-request**: the Action finds the dependencies that a pull request adds to `package.json`, `pyproject.toml`, and `Cargo.toml`, and posts one comment with their MTTU and MTTR. A new push updates the same comment.

For both targets, the inputs `max-mttu-days` and `max-mttr-days` fail the step when a value is more than the limit. A package with no dependencies shows "could not compute since '<name>' has no dependencies", and a package that fails shows its error. Neither one fails the step.

The report explains what the numbers mean and lists the limitations of the data, with the same text as the [Webapp](https://imranur-rahman.github.io/dependency-update-metrics/) and the VS Code extension.

## Runner

The Action runs only on a Linux runner, because it runs the Tool with Docker. It also needs `node` (version 20 or later) on the `PATH`. Each GitHub-hosted Linux runner (`runs-on: ubuntu-latest`) has both. On macOS or Windows, the Action stops with an error.

## Target packages

The input `packages` takes 1 to 5 package names of one ecosystem. For more packages, use more steps.

```yaml
name: MTTX
on:
  schedule: [ { cron: '0 6 * * 1' } ]
  workflow_dispatch:

permissions:
  contents: read

jobs:
  mttx:
    runs-on: ubuntu-latest
    steps:
      - id: mttx
        uses: imranur-rahman/mttx-action@v1
        with:
          packages: express, koa
          ecosystem: npm
          max-mttr-days: 30
      - run: echo "MTTR of express is ${{ steps.mttx.outputs.mttr }} days"
```

### Badges

With target packages, the Action writes two files for each package to the folder `badge-dir` (default `mttx-badges/`): `<ecosystem>-<package>-mttu.json` and `<ecosystem>-<package>-mttr.json`. A `/` in a package name becomes `__`, and the Action removes the other characters that are not safe in a file name, so `@types/node` gives `npm-types__node-mttu.json`.

The badge color is red above the limit, bright green at or below it, blue when you set no limit, and light grey when the package has no value.

shields.io reads the file from a public URL. For example, push the folder to a branch `badges` after the Action step:

```yaml
      - if: always()
        run: |
          git fetch origin badges && git switch badges || git switch --orphan badges
          git add mttx-badges
          git -c user.name=mttx -c user.email=mttx@users.noreply.github.com commit -m "Update the MTTX badges" || exit 0
          git push origin badges
```

This step needs `permissions: { contents: write }`. Then use this badge URL:

```
https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/<owner>/<repo>/badges/mttx-badges/npm-express-mttr.json
```

## Target pull-request

```yaml
name: MTTX
on: pull_request

permissions:
  contents: read
  pull-requests: write

jobs:
  mttx:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7
        with: { fetch-depth: 0 }
      - uses: imranur-rahman/mttx-action@v1
        with:
          target: pull-request
```

The Action reads each changed manifest at the merge base of the pull request and at its head, with `git`. So the checkout needs the full history (`fetch-depth: 0`).

The Action reads the manifests with the parsers of the VS Code extension, so both find the same dependencies:

- `package.json`: `dependencies`, `devDependencies`, `peerDependencies`, and `optionalDependencies`.
- `pyproject.toml`: the PEP 621 dependencies and optional dependencies, the PEP 735 dependency groups, and the Poetry tables. Extras and markers do not change the name. Direct URLs and Poetry `git`, `path`, and `url` dependencies do not come from PyPI, so the Action skips them.
- `Cargo.toml`: `[dependencies]`, `[dev-dependencies]`, and `[build-dependencies]`, also below `[target.<cfg>]`. A `package = "..."` key gives the name of the crate. The Action skips `git`, `registry`, and `path` dependencies without a version.

The Action skips the manifests in `node_modules/` and `.venv/`.

A dependency counts as added when its name is in the head manifest and not in the base manifest. A changed version constraint does not count, and a move to another section does not count. PyPI names match as PEP 503 names, so `Typing_Extensions` and `typing-extensions` are the same package.

A pull request that adds no dependencies gets no comment. When an earlier push added dependencies and a later push removes them, the Action changes its comment to "This pull request adds no dependencies."

The Action finds its own comment by the first line `<!-- mttx-action -->`. Do not remove that line from the comment.

A pull request from a fork gets a read-only token on the event `pull_request`, so the Action cannot write the comment. Then the Action gives a warning and writes the report to the job summary only.

## Permissions

| Target | Permissions |
|---|---|
| packages | `contents: read`. Add `contents: write` only if a later step pushes the badge files. |
| pull-request | `contents: read` and `pull-requests: write` (to write the comment). |

## Inputs

The inputs `data-source`, `window-years`, `start-date`, `end-date`, `weighting-type`, `half-life`, `package-timeout-seconds`, and `parallel-runs` have the names of the `mttx.*` settings of the VS Code extension in kebab case (`mttx.windowYears` is `window-years`), with the same defaults and ranges.

| Input | Default | Description |
|---|---|---|
| `target` | `packages` | What the Action analyzes: `packages` (the packages in the input `packages`) or `pull-request` (the dependencies that the pull request adds). |
| `packages` | | Target packages: 1 to 5 package names, split by commas or spaces. |
| `ecosystem` | | Target packages: `npm`, `pypi`, or `cargo`. |
| `data-source` | `depsdev` | `depsdev` (deps.dev) or `native` (the registry of each ecosystem). |
| `window-years` | `2` | The length of the analysis window in years. The window ends on `end-date`. `start-date` replaces this value when it is set. |
| `start-date` | | The start date of the window (YYYY-MM-DD). Empty: `window-years` years before the end date. |
| `end-date` | | The end date of the window (YYYY-MM-DD). Empty: today (UTC). |
| `weighting-type` | `disable` | `disable`, `linear`, `exponential`, or `inverse`. With `disable`, MTTU and MTTR are total days. With a weighting, they are average days. |
| `half-life` | | The half-life in days (1 to 3650). Exponential weighting needs it. |
| `package-timeout-seconds` | `600` | The Action stops the Tool run for one package after this number of seconds. The package then shows an error. |
| `parallel-runs` | `2` | The most Tool runs at a time (1 to 16). |
| `max-mttu-days` | | The step fails when the MTTU of a package is more than this number of days. Empty: no limit. |
| `max-mttr-days` | | The step fails when the MTTR of a package is more than this number of days. Empty: no limit. |
| `osv-index` | `image` | `image`: use the OSV index inside the image, so that the run downloads no OSV data. `fresh`: build the OSV index of today. [AGENTS.md](https://github.com/imranur-rahman/dependency-update-metrics/blob/main/AGENTS.md) gives the download size. `actions/cache` keeps the index for the day. |
| `badge-dir` | `mttx-badges` | Target packages: the folder of the badge files, relative to the workspace. |
| `github-token` | `${{ github.token }}` | Target pull-request: the token that writes the comment. |

The Action always uses the Window summary mode of the Tool: one MTTU and one MTTR for each package over the whole window.

## Outputs

| Output | Description |
|---|---|
| `results` | A JSON array with one object for each package: `ecosystem`, `package`, `mttu`, `mttr` (days, or `null`), `num_dependencies`, `status` (`ok`, `no_deps`, or `error`), and `error`. |
| `mttu` | The MTTU of the first package in days, with two decimals. Empty when it has no value. |
| `mttr` | The MTTR of the first package in days, with two decimals. Empty when it has no value. |
| `over-threshold` | `true` when a value is more than `max-mttu-days` or `max-mttr-days`, else `false`. |
| `added` | Target pull-request: a JSON array of the added dependencies (`ecosystem`, `name`, `files`). |
| `comment-url` | Target pull-request: the URL of the comment. Empty when the Action wrote no comment. |

## How the Action runs the Tool

The Action runs the Tool in the container image `ghcr.io/imranur-rahman/mttx`. Each Action version pins one image digest (`image.json`), so a rerun with the same Action version uses the same Tool, the same dependencies of the Tool, and, with `osv-index: image`, the same OSV index.

- `osv-index: image` uses the tag `<version>-osv-<YYYY-MM-DD>`. The OSV index of that date is inside the image. The Limitations of the report name the date.
- `osv-index: fresh` uses the tag `<version>` and runs `dependency-metrics --build-osv` once a day. The index stays in `$RUNNER_TEMP/mttx-data`, and `actions/cache` keeps it under the key `mttx-osv-<date>`.

Each Tool run is one `docker run` with `$RUNNER_TEMP/mttx-data` on `/data`, as the runner user. So the Tool cache stays between the packages of one job, and the runner owns the files.

The [Runner](#runner) section lists what the runner needs.

## Development

The source and the tests live in [`action/`](https://github.com/imranur-rahman/dependency-update-metrics/tree/main/action) of the repository `imranur-rahman/dependency-update-metrics`. The Action imports the manifest parsers of the Extension core (`extension/src/core/`) and the Explanation of the Webapp (`webapp/site/static/js/explanation.js`). esbuild bundles them into `dist/index.mjs`.

```
cd action
npm ci
npm run check-types
npm test            # unit tests
npm run test:e2e    # builds dist/index.mjs and runs it on the fixture PR files
```

The Docker call is one seam (`openTool` in `ActionIO`, `src/main.ts`), which gets the checked `osv-index` input. The tests replace it with a fake. The variable `MTTX_ACTION_TOOL` (the path of a JavaScript file) replaces the Docker call in the built Action; the CI sets it to the fake Tool of the Extension. Only with it, `MTTX_ACTION_EVENT_PATH` and `MTTX_ACTION_API_URL` replace the pull request event and the REST API, so that the CI runs `action.yml` with target pull-request. Users never set them.

## Releases

The workflow `.github/workflows/github-action.yml` of the repository `imranur-rahman/dependency-update-metrics` releases the Action. A GitHub Marketplace listing needs one `action.yml` at the root of a repository, so the release job copies `action.yml`, `README.md`, `LICENSE`, and `dist/index.mjs` to the public repository `imranur-rahman/mttx-action`. Then it tags the copy `v<version>` and moves the major tag (`v1`).

To release:

1. Set `image.json` to the image release: `version`, `osvTag`, and the two digests (`sha256:...`). Get a digest with `docker buildx imagetools inspect ghcr.io/imranur-rahman/mttx:<tag>`.
2. Set `version` in `action/package.json`.
3. Push the tag `action-v<version>` (for example `action-v1.0.0`).

The release job stops with an error when the tag does not match `package.json`, or when `image.json` has no digests.

### One-time setup

The release job needs the secret `MTTX_ACTION_TOKEN`. Without it, the job skips the copy with a notice, and the release of the tag does nothing else.

1. Make the GHCR package `ghcr.io/imranur-rahman/mttx` public: open its package settings on GitHub and set the visibility to public. Without this step, `docker pull` on a runner of another repository fails. `image/README.md` names the same step.
2. Create the public repository `imranur-rahman/mttx-action`, with no files.
3. Create a fine-grained personal access token with access to `imranur-rahman/mttx-action` only, and the permission "Contents: Read and write".
4. In `imranur-rahman/dependency-update-metrics`, add the token as the Actions secret `MTTX_ACTION_TOKEN`.
5. Push the first tag `action-v1.0.0`.
6. In `imranur-rahman/mttx-action`, edit the release `v1.0.0` that the job created. Select "Publish this Action to the GitHub Marketplace", accept the Marketplace developer agreement, and update the release.

For each later release, edit the release of the new tag in `imranur-rahman/mttx-action` and publish it to the Marketplace in the same way.
