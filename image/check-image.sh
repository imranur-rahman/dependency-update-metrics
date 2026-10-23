#!/usr/bin/env bash
# Check the two images of the Tool (issue #51). CI runs this on each PR build and
# after each release push.
#   image/check-image.sh <tool image> <osv image> <expected OSV index date YYYY-MM-DD>
set -euo pipefail

tool_image=$1
osv_image=$2
osv_date=$3

echo "== --help starts with no network access"
docker run --rm --network none "$tool_image" --help > /dev/null

echo "== a native PyPI run writes to /data and never clones the pip fork"
data=$(mktemp -d)
chmod 777 "$data"
docker run --rm -v "$data:/data" --entrypoint sh "$tool_image" -c '
  set -e
  dependency-metrics --ecosystem pypi --package requests \
    --start-date 2024-01-01 --end-date 2024-03-01
  if [ -e "$HOME/.dependency_metrics/vendor" ]; then
    echo "The Tool cloned the pip fork into $HOME/.dependency_metrics/vendor" >&2
    exit 1
  fi'
test -s "$data/requests_results.json"

echo "== a native npm run works as a user that is not root (docker run --user)"
docker run --rm --user 65534:65534 -v "$data:/data" "$tool_image" --ecosystem npm --package express \
  --start-date 2024-01-01 --end-date 2024-03-01 --json > "$data/express.json"
grep -q '"status": "ok"' "$data/express.json"

echo "== the plain image has no OSV index"
test "$(docker run --rm --network none -v "$data:/data" "$tool_image" --osv-index-date)" \
  = '{"osv_index_date": null}'

echo "== the -osv- image finds its own index, also with a mount on /data"
test "$(docker run --rm --network none -v "$data:/data" "$osv_image" --osv-index-date)" \
  = "{\"osv_index_date\": \"$osv_date\"}"

echo "== the -osv- image gives MTTR without --build-osv"
docker run --rm -v "$data:/data" "$osv_image" --ecosystem pypi --package requests \
  --start-date 2024-01-01 --end-date 2024-03-01 > /dev/null
test ! -e "$data/osv_database.parquet"

echo "All image checks passed."
