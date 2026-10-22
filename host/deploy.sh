#!/usr/bin/env bash
# Deploys the current commit of the repo on the VM. Run it on the VM, from the repo:
#
#   bash host/deploy.sh
#
# It pulls the branch, builds the image from this commit, starts the services, removes the
# old images, and waits until GET /api/health answers.
set -euo pipefail

host_dir=$(cd "$(dirname "$0")" && pwd)
cd "$host_dir"

if [ ! -f .env ]; then
  echo "host/.env is missing. Copy host/.env.example and fill it in (host/RUNBOOK.md)." >&2
  exit 1
fi
chmod 600 .env

git pull --ff-only
docker compose up -d --build --remove-orphans
# Each build leaves the old image without a tag. The boot disk is only 50 GB.
docker image prune -f

echo "Waiting for GET /api/health ..."
for _ in $(seq 1 30); do
  if docker compose exec -T api python -c \
    "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/api/health', timeout=5)" \
    2>/dev/null; then
    echo "The Host is healthy at commit $(git rev-parse --short HEAD)."
    exit 0
  fi
  sleep 2
done
echo "The Host did not become healthy. Run: docker compose logs api worker caddy" >&2
exit 1
