#!/usr/bin/env bash
# Checks the deploy files of the Host without a VM. The Host CI runs it; run it locally from
# the repo root before you change a file in host/ that is not Python:
#
#   bash host/check-deploy-files.sh
#
# It needs bash, shellcheck, and docker. It:
#   1. parses each script with bash -n, then lints it with shellcheck;
#   2. renders compose.yaml with the values of .env.example, without and with the "oracle"
#      profile, and checks the services that each one starts;
#   3. validates the Caddyfile with the Caddy image that compose.yaml uses.
set -euo pipefail

host_dir=$(cd "$(dirname "$0")" && pwd)
cd "$host_dir"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

echo "== 1/3 Scripts"
for script in ./*.sh; do
  bash -n "$script"
done
# Warnings and errors only: the info notes flag the wizard library's helpers that a wizard
# does not call.
shellcheck --severity=warning ./*.sh

echo "== 2/3 compose.yaml"
# .env.example leaves the secrets and the address empty, and compose.yaml refuses an empty
# value, so the check fills them with placeholders. compose.yaml also reads host/.env, so the
# check renders a copy next to a placeholder .env, and a real host/.env never takes part.
cp compose.yaml "$work/compose.yaml"
sed -e 's/^DM_HOST_DUCKDNS_SUBDOMAIN=$/DM_HOST_DUCKDNS_SUBDOMAIN=example/' \
  -e 's/^DM_HOST_DUCKDNS_TOKEN=$/DM_HOST_DUCKDNS_TOKEN=placeholder/' \
  -e 's/^DM_HOST_ACME_EMAIL=$/DM_HOST_ACME_EMAIL=admin@example.org/' \
  .env.example >"$work/.env"
services() {
  docker compose --project-directory "$work" -f "$work/compose.yaml" config --services | sort | tr '\n' ' '
}
check_services() {
  local want="$1" got="$2"
  if [ "$got" != "$want" ]; then
    echo "compose.yaml starts '$got', expected '$want'" >&2
    exit 1
  fi
}
check_services "api caddy duckdns worker " "$(COMPOSE_PROFILES='' services)"
check_services "api ballast caddy duckdns worker " "$(COMPOSE_PROFILES=oracle services)"
docker compose --project-directory "$work" -f "$work/compose.yaml" config -q

echo "== 3/3 Caddyfile"
caddy_image=$(grep -E '^[[:space:]]+image: caddy:' compose.yaml | awk '{print $2}')
docker run --rm \
  -e DM_HOST_DUCKDNS_SUBDOMAIN=example -e DM_HOST_ACME_EMAIL=admin@example.org \
  -v "$host_dir/Caddyfile:/etc/caddy/Caddyfile:ro" \
  "$caddy_image" caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile

echo "The deploy files are valid."
