#!/usr/bin/env bash
set -euo pipefail

for container in fujin-local fujin-materializer fujin-simulator; do
  podman rm -f "$container" >/dev/null 2>&1 || true
done
