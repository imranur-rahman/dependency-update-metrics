#!/usr/bin/env bash
# The disk alert. host/vm-setup.sh runs it from cron every 15 minutes.
#
# It pings the Healthchecks.io check in DM_HOST_DISK_CHECK_URL (host/.env). When the root
# file system is 80% full or more, it pings the /fail URL, and Healthchecks.io sends the
# alert. If the VM stops, the pings stop, and Healthchecks.io also sends an alert.
set -euo pipefail

host_dir=$(cd "$(dirname "$0")" && pwd)
threshold=80

url=$(grep -E '^DM_HOST_DISK_CHECK_URL=' "$host_dir/.env" 2>/dev/null | tail -n1 | cut -d= -f2- \
  | tr -d "'\"")
[ -n "$url" ] || exit 0

used=$(df --output=pcent / | tail -n1 | tr -dc '0-9')
if [ "$used" -ge "$threshold" ]; then
  curl -fsS -m 10 --retry 3 --data-raw "Disk: ${used}% of / is used" "$url/fail" >/dev/null
else
  curl -fsS -m 10 --retry 3 --data-raw "Disk: ${used}% of / is used" "$url" >/dev/null
fi
