# syntax=docker/dockerfile:1
# Container image of the Tool: ghcr.io/imranur-rahman/mttx (issue #51).
# Build from the repository root, with vendor/pip checked out:
#   docker build -f image/Dockerfile --target tool -t mttx .
# The "osv" target also needs the OSV index in a build context named "osvindex":
#   docker build -f image/Dockerfile --target osv --build-context osvindex=<dir> -t mttx:osv .
# image/README.md describes the tags, the labels, and how CI builds them.

# Base images are pinned by digest (multi-platform index), so a rebuild gets the same OS.
FROM node:22-bookworm-slim@sha256:de170b39439eb0e5a3444ad7d60b015614a381e10fecae2e30ec9e735122f38d AS node
FROM python:3.12-slim-bookworm@sha256:6a0e06f5ba0b373d051aabd3fffb9b873f27f9a2a3a14bcbe37dee6f0e3bbc70 AS tool

# The native npm resolver calls the npm CLI, so the image carries Node and npm.
COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/lib/node_modules/npm /usr/local/lib/node_modules/npm
RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
    && node --version && npm --version

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /opt/mttx
# Every package comes from the lock, with its hash, so a rebuild gets the same packages.
COPY image/requirements.lock image/requirements.lock
RUN pip install --require-hashes --only-binary=:all: -r image/requirements.lock

# vendor/pip is the patched pip at the submodule commit of the release. The editable
# install keeps the Tool in /opt/mttx, next to vendor/pip, so the Tool finds it there
# and never clones the fork at run time.
COPY pyproject.toml README.md LICENSE ./
COPY vendor/pip/src vendor/pip/src
COPY dependency_metrics dependency_metrics
RUN test -f vendor/pip/src/pip/__init__.py \
    && pip install --no-deps --no-build-isolation -e . \
    && dependency-metrics --help > /dev/null

ARG VERSION=dev
ARG REVISION=unknown
ARG PIP_COMMIT=unknown
LABEL org.opencontainers.image.title="mttx" \
      org.opencontainers.image.description="dependency-metrics: MTTU and MTTR of the direct dependencies of an npm, PyPI, or Cargo package" \
      org.opencontainers.image.source="https://github.com/imranur-rahman/dependency-update-metrics" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      io.github.imranur-rahman.mttx.pip-commit="${PIP_COMMIT}" \
      io.github.imranur-rahman.mttx.osv-index-date="none"

# The results, the cache, and (in this target) the OSV index go to /data.
# Mount a folder there to keep them: docker run -v "$PWD:/data" ...
ENV DEPENDENCY_METRICS_OUTPUT_DIR=/data
WORKDIR /data
ENTRYPOINT ["dependency-metrics"]
CMD ["--help"]

# The same Tool with the OSV index of one date inside the image. The index sits outside
# /data, so a mount on /data does not hide it, and every rerun uses the same index.
FROM tool AS osv
ARG OSV_INDEX_DATE
COPY --from=osvindex osv_database.parquet /opt/mttx/osv/osv_database.parquet
ENV DEPENDENCY_METRICS_OSV_INDEX=/opt/mttx/osv/osv_database.parquet
RUN test -n "${OSV_INDEX_DATE}" \
    && test "$(dependency-metrics --osv-index-date)" = "{\"osv_index_date\": \"${OSV_INDEX_DATE}\"}"
LABEL io.github.imranur-rahman.mttx.osv-index-date="${OSV_INDEX_DATE}"
