#!/usr/bin/env bash
# Build the OSV index with the image, and print "date=<YYYY-MM-DD>" (the build date that
# the index holds) for $GITHUB_OUTPUT. The osv target of image/Dockerfile copies the index.
#   image/build-osv-index.sh <tool image> <output dir>
set -euo pipefail

image=$1
dir=$2
mkdir -p "$dir"
# The container runs as root and writes into a folder of the runner.
chmod 777 "$dir"
dir=$(realpath "$dir")
docker run --rm -v "$dir:/data" "$image" --build-osv >&2
date=$(docker run --rm -v "$dir:/data" "$image" --osv-index-date \
  | python3 -c 'import json, sys; print(json.load(sys.stdin)["osv_index_date"])')
if ! [[ $date =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "The OSV index has no build date: $date" >&2
  exit 1
fi
echo "date=$date"
