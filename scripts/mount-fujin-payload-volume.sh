#!/usr/bin/env bash
set -euo pipefail

# The Terraform attachment intentionally assigns a stable paravirtualized
# device. This script is idempotent so it can be used on the initial cloud-init
# boot and after attaching the new volume to an existing migration VM.
device="${RAIJIN_FUJIN_PAYLOAD_DEVICE:-/dev/oracleoci/oraclevdb}"
mount_dir="${RAIJIN_FUJIN_PAYLOAD_MOUNT:-/var/lib/raijin-data-migration/fujin-payloads}"

for attempt in $(seq 1 120); do
  [[ -b "$device" ]] && break
  sleep 5
done
[[ -b "$device" ]] || { echo "Fujin payload volume did not attach: $device" >&2; exit 1; }

if ! blkid "$device" >/dev/null 2>&1; then
  command -v mkfs.xfs >/dev/null 2>&1 || dnf install -y xfsprogs
  mkfs.xfs -f "$device"
fi

uuid=$(blkid -s UUID -o value "$device")
[[ -n "$uuid" ]] || { echo "Cannot resolve UUID for Fujin payload volume" >&2; exit 1; }
install -d -m 0700 "$mount_dir"
if ! grep -q "[[:space:]]$mount_dir[[:space:]]" /etc/fstab; then
  printf 'UUID=%s %s xfs defaults,nofail 0 2\n' "$uuid" "$mount_dir" >>/etc/fstab
fi
mountpoint -q "$mount_dir" || mount "$mount_dir"
chmod 0700 "$mount_dir"
