# Container image of the Tool

`ghcr.io/imranur-rahman/mttx` holds the Tool in a fixed environment, so that a reviewer or a researcher can run the same Tool years later. The Tool CI (`.github/workflows/ci.yml`) builds and pushes it on each GitHub Release of the Tool, for `linux/amd64` and `linux/arm64`.

## Tags

| Tag | OSV index |
|---|---|
| `<version>`, `latest` | None. MTTR needs `--build-osv` once, or an index in the folder on `/data`. |
| `<version>-osv-<YYYY-MM-DD>` | The index built on that date is inside the image, so MTTR is the same in each rerun. |

The release job writes the digest of each tag to its job summary. Cite the digest (`ghcr.io/imranur-rahman/mttx@sha256:…`) in a paper, because a tag can move and a digest cannot.

## Interface

- The entrypoint is the `dependency-metrics` CLI. With no arguments, the image prints `--help`.
- The output dir is `/data` (`DEPENDENCY_METRICS_OUTPUT_DIR=/data`), so `-v "$PWD:/data"` keeps the results and the cache. `--output-dir` still wins.
- The `-osv-` tag keeps its index at `/opt/mttx/osv/osv_database.parquet` (`DEPENDENCY_METRICS_OSV_INDEX`), so a mount on `/data` does not hide it. The Tool reads and builds the index at that path, also with `--build-osv`. For a fresh index that you keep in `/data`, use the plain tag.
- The container runs as root. On Linux, add `--user "$(id -u):$(id -g)"`, so that the files in your folder belong to you.

```bash
docker run --rm -v "$PWD:/data" ghcr.io/imranur-rahman/mttx:0.2.0-osv-2026-10-23 \
  --package express --ecosystem npm --start-date 2024-01-01 --end-date 2025-01-01
docker run --rm ghcr.io/imranur-rahman/mttx:0.2.0-osv-2026-10-23 --osv-index-date
```

## What makes a rebuild give the same image

- `image/Dockerfile` pins the Python and the Node base images by digest. Node gives the npm CLI that the native npm resolver calls.
- `image/requirements.lock` pins every Python package of the Tool, with hashes. Make it again after a change to the dependencies in `pyproject.toml`:

  ```bash
  uv pip compile pyproject.toml image/build-requirements.in --universal --python-version 3.12 \
    --generate-hashes --no-header --exclude-newer <date two weeks ago> -o image/requirements.lock
  ```

- The image copies `vendor/pip` at the submodule commit of the release. The editable install keeps the Tool next to `vendor/pip`, so the Tool never clones the pip fork at run time.

## Labels

| Label | Value |
|---|---|
| `org.opencontainers.image.source` | The repository URL |
| `org.opencontainers.image.version` | The Tool version |
| `org.opencontainers.image.revision` | The commit of the release |
| `io.github.imranur-rahman.mttx.pip-commit` | The `vendor/pip` commit |
| `io.github.imranur-rahman.mttx.osv-index-date` | The date of the index inside, or `none` |

## CI

- On each push and PR that changes the Tool or `image/`, the `image` job builds both targets for amd64 (no push), builds a real OSV index with the image (`image/build-osv-index.sh`), and runs `image/check-image.sh`.
- `image/check-image.sh` checks that `--help` starts with no network, that a native PyPI run writes to `/data` and leaves no `~/.dependency_metrics/vendor`, and that the `-osv-` image prints the date of its tag with a mount on `/data`.
- On a release (not `extension-v*`), the `image-release` job runs after the PyPI publish. It pushes both tags, then runs the same checks on the pushed digests.

Build locally from the repository root, with `vendor/pip` checked out:

```bash
docker build -f image/Dockerfile --target tool -t mttx .
bash image/build-osv-index.sh mttx osv-index
docker build -f image/Dockerfile --target osv --build-context osvindex=osv-index \
  --build-arg OSV_INDEX_DATE=<date that the script printed> -t mttx:osv .
bash image/check-image.sh mttx mttx:osv <date>
```

The GHCR package must be public. After the first push, set its visibility to public once in the package settings on GitHub.
