#!/usr/bin/env bash
# Prepares the Oracle VM (Ubuntu, arm64) for the Host. Run it on the VM, from the repo:
#
#   bash host/vm-setup.sh
#
# It is safe to run again. It:
#   1. installs Docker, Docker Compose, and git;
#   2. opens ports 80 and 443 in the instance firewall (Oracle's Ubuntu images block them);
#   3. turns swap off (ADR 0001: the kernel must not move the ballast pages to swap);
#   4. installs the cron job of the disk alert (host/disk-check.sh, every 15 minutes);
#   5. installs the cron job of the weekly maintenance (host/weekly-maintenance.sh, every
#      Sunday at 04:00 UTC: the OSV rebuild, then the cache prune to 10 GB).
set -euo pipefail

host_dir=$(cd "$(dirname "$0")" && pwd)

echo "== 1/5 Docker"
sudo apt-get update -q
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q \
  docker.io docker-compose-v2 git curl iptables-persistent netfilter-persistent
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"

echo "== 2/5 Instance firewall: ports 80 and 443"
# Oracle's images end the INPUT chain with a REJECT rule, so the ACCEPT rules go first.
open_port() {
  local proto="$1" port="$2"
  if ! sudo iptables -C INPUT -p "$proto" -m state --state NEW --dport "$port" -j ACCEPT 2>/dev/null; then
    sudo iptables -I INPUT 1 -p "$proto" -m state --state NEW --dport "$port" -j ACCEPT
  fi
}
open_port tcp 80
open_port tcp 443
open_port udp 443
sudo netfilter-persistent save

echo "== 3/5 Swap off"
if [ -n "$(swapon --show --noheadings)" ]; then
  sudo swapoff -a
fi
# Comment out each swap line in /etc/fstab, so that swap stays off after a reboot.
sudo sed -i -E 's/^([^#].*[[:space:]]swap[[:space:]].*)$/# \1/' /etc/fstab
swapon --show

echo "== 4/5 Disk alert cron job"
chmod +x "$host_dir/disk-check.sh" "$host_dir/deploy.sh" "$host_dir/weekly-maintenance.sh"
( crontab -l 2>/dev/null | grep -v 'disk-check.sh' || true
  echo "*/15 * * * * $host_dir/disk-check.sh >/dev/null 2>&1" ) | crontab -

echo "== 5/5 Weekly maintenance cron job"
# The VM clock is UTC on Oracle's Ubuntu images. The log is in the home folder, not in the repo.
( crontab -l 2>/dev/null | grep -v 'weekly-maintenance.sh' || true
  echo "0 4 * * 0 $host_dir/weekly-maintenance.sh >>\$HOME/dmhost-maintenance.log 2>&1" ) | crontab -
crontab -l

echo "Done. Log out and in again, so that your user can run docker without sudo."
