# The image of the Host: the API and the workers.
#
# Build it from the repo root, so that the image installs the Tool from the same commit as
# the Host (docker compose does this, see host/compose.yaml):
#
#   docker build -f host/Dockerfile -t dmhost .
#
# python:3.12-slim is a multi-arch image, so the same file builds on the arm64 Oracle VM
# and on an amd64 laptop. The Host always uses deps.dev data, so the image does not need
# the vendored pip fork (vendor/pip).
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# The Host dependencies change less often than the code, so they get their own layer.
COPY host/requirements.txt /src/host/requirements.txt
RUN pip install -r /src/host/requirements.txt

# The Tool, from this commit.
COPY pyproject.toml README.md LICENSE MANIFEST.in /src/
COPY dependency_metrics /src/dependency_metrics
RUN pip install /src

# The Host package. PYTHONPATH makes `dmhost` importable, as in host/README.md.
COPY host/dmhost /app/dmhost
ENV PYTHONPATH=/app

# The data directories. The Compose volumes mount over them, and a new named volume takes
# the owner of the directory, so the user below can write to it.
#   /data/queue       the Job queue (jobs.db)
#   /data/tool        the Tool output directory: the OSV index (osv_database.parquet)
#   /data/tool/cache  the ResolverCache (cache.db)
RUN useradd --system --uid 10001 --home-dir /app dmhost \
    && mkdir -p /data/queue /data/tool/cache \
    && chown -R dmhost:dmhost /data
USER dmhost
WORKDIR /app

ENV DM_HOST_DB=/data/queue/jobs.db \
    DM_HOST_OUTPUT_DIR=/data/tool

EXPOSE 8080

# The API. --no-access-log: the uvicorn access log writes each client address, and the Host
# must never store a plain IP. --no-proxy-headers: the app reads X-Forwarded-For itself, only
# from the trusted proxy (dmhost/client_ip.py). The worker service overrides this command.
CMD ["uvicorn", "--factory", "dmhost.app:create_app_from_env", \
     "--host", "0.0.0.0", "--port", "8080", "--no-access-log", "--no-proxy-headers"]
