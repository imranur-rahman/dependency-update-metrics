#!/usr/bin/env bash
# The weekly maintenance (ticket #16). host/vm-setup.sh runs it from cron every 7 days.
#
# It runs `python -m dmhost.maintenance` in a worker container, with the volumes of the
# Host. That command rebuilds the OSV index (`--build-osv`) and then prunes the
# ResolverCache to 10 GB (`--cache-prune-gb 10`). A failed rebuild keeps the old index.
#
# The script exits with the exit code of the maintenance. When DM_HOST_MAINTENANCE_CHECK_URL
# (host/.env) is set, it also pings that Healthchecks.io check, or <url>/fail on a failure.
set -uo pipefail

host_dir=$(cd "$(dirname "$0")" && pwd)
# No set -e here (the script must ping the check after a failure), so stop by hand.
cd "$host_dir" || exit 1

echo "== Weekly maintenance, $(date -u +%Y-%m-%dT%H:%M:%SZ)"
docker compose run --rm -T worker python -m dmhost.maintenance
code=$?

url=$(grep -E '^DM_HOST_MAINTENANCE_CHECK_URL=' .env 2>/dev/null | tail -n1 | cut -d= -f2- \
  | tr -d "'\"")
if [ -n "$url" ]; then
  if [ "$code" -eq 0 ]; then
    curl -fsS -m 10 --retry 3 --data-raw "Weekly maintenance done" "$url" >/dev/null
  else
    curl -fsS -m 10 --retry 3 --data-raw "Weekly maintenance failed ($code)" "$url/fail" \
      >/dev/null
  fi
fi
exit "$code"
