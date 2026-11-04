#!/usr/bin/env bash
# Start Fujin's independent services from an installed release.
set -euo pipefail

release_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
image="localhost/fujin-data-simulation:${FUJIN_BUILD_REVISION:-$(git -C "$release_dir" rev-parse --short HEAD 2>/dev/null || echo development)}"
data_dir="${FUJIN_DATA_DIR:-/var/lib/fujin-data-simulation}"
network="fujin-data-simulation"
local_host="${FUJIN_BIND_HOST:-127.0.0.1}"
local_port="${FUJIN_BIND_PORT:-8090}"
simulator_host="${FUJIN_SIMULATOR_BIND_HOST:-127.0.0.1}"
simulator_port="${FUJIN_SIMULATOR_BIND_PORT:-8091}"

mkdir -p "$data_dir"
podman network exists "$network" || podman network create "$network" >/dev/null
podman build --tag "$image" --build-arg "FUJIN_BUILD_REVISION=${FUJIN_BUILD_REVISION:-development}" "$release_dir"

replace_container() {
  local name="$1"
  podman rm -f "$name" >/dev/null 2>&1 || true
}

replace_container fujin-local
replace_container fujin-materializer
replace_container fujin-simulator

podman run -d --name fujin-local --restart unless-stopped --network "$network" \
  -p "${local_host}:${local_port}:8080" -v "${data_dir}:/var/lib/fujin:Z" \
  -e FUJIN_LOCAL_DATABASE_URL=sqlite+pysqlite:////var/lib/fujin/catalog.db \
  -e FUJIN_LOCAL_PAYLOAD_ROOT=/var/lib/fujin/payloads \
  -e FUJIN_LOCAL_STAGING_ROOT=/var/lib/fujin/staging \
  "$image" >/dev/null

podman run -d --name fujin-materializer --restart unless-stopped --network "$network" \
  -v "${data_dir}:/var/lib/fujin:Z" \
  -e FUJIN_LOCAL_DATABASE_URL=sqlite+pysqlite:////var/lib/fujin/catalog.db \
  -e FUJIN_LOCAL_PAYLOAD_ROOT=/var/lib/fujin/payloads \
  -e FUJIN_LOCAL_STAGING_ROOT=/var/lib/fujin/staging \
  "$image" python3 -m app.fujin_local_materializer >/dev/null

podman run -d --name fujin-simulator --restart unless-stopped --network "$network" \
  -p "${simulator_host}:${simulator_port}:8091" -v "${data_dir}:/var/lib/fujin:Z" \
  -e FUJIN_SIMULATOR_DATABASE_URL=sqlite+pysqlite:////var/lib/fujin/simulator.db \
  "$image" sh -c 'python3 scripts/migrate-fujin-simulator.py && exec uvicorn app.simulator:app --host 0.0.0.0 --port 8091' >/dev/null

wait_ready() {
  local container="$1" port="$2" attempt
  for attempt in $(seq 1 30); do
    if podman exec "$container" python3 -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:${port}/readyz', timeout=2).read()" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "${container} did not become ready" >&2
  podman logs "$container" >&2 || true
  return 1
}

wait_ready fujin-local 8080
wait_ready fujin-simulator 8091

echo "Fujin console: http://${local_host}:${local_port}/console"
echo "Fujin simulator: http://${simulator_host}:${simulator_port}/healthz"
