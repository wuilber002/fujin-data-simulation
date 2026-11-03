#!/usr/bin/env bash
set -euo pipefail

# Update only Raijin in an existing Fujin LOCAL topology. PostgreSQL, Fujin,
# materializer, DNS and the UI gateway remain online throughout the rollout.

target_image="${1:-}"
data_root="${RAIJIN_DATA_ROOT:-/var/lib/raijin-data-migration}"
secret_root="${RAIJIN_SECRET_ROOT:-/etc/raijin-data-migration/secrets}"
runtime_root="${RAIJIN_RUNTIME_ROOT:-/run/raijin-data-migration}"
main_network="${RAIJIN_MAIN_NETWORK:-raijin-data-migration}"
data_network="${FUJIN_LOCAL_DATA_NETWORK:-raijin-fujin-local-data}"
ui_network="${FUJIN_LOCAL_UI_NETWORK:-raijin-fujin-local-ui}"
local_oci_runtime="${RAIJIN_LOCAL_OCI_RUNTIME_CONFIG:-/etc/raijin-data-migration/oci-runtime-local.json}"
stop_timeout="${RAIJIN_UPDATE_STOP_TIMEOUT_SECONDS:-180}"
candidate_name="raijin-app-candidate"
handoff_started=0
release_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# shellcheck source=web-bind.sh
source "$release_root/scripts/web-bind.sh"
raijin_load_web_bind

[[ -n "$target_image" ]] || {
  echo "Usage: $0 TARGET_IMAGE" >&2
  exit 2
}
[[ "$stop_timeout" =~ ^[0-9]+$ ]] && ((stop_timeout >= 30)) || {
  echo "RAIJIN_UPDATE_STOP_TIMEOUT_SECONDS must be an integer of at least 30" >&2
  exit 2
}
podman image exists "$target_image" || { echo "Image not found: $target_image" >&2; exit 2; }
for name in raijin-postgres raijin-fujin-local raijin-fujin-local-materializer raijin-fujin-local-dns raijin-local-ui-gateway; do
  [[ "$(podman inspect --format '{{.State.Running}}' "$name" 2>/dev/null || true)" == true ]] || {
    echo "Required LOCAL container is not running: $name" >&2
    exit 2
  }
done
[[ -s "$local_oci_runtime" ]] || { echo "Missing $local_oci_runtime" >&2; exit 2; }
mountpoint -q "$data_root/fujin-payloads" || {
  echo "Fujin payload volume is not mounted" >&2
  exit 2
}

# Restore polling in READY is recoverable. An admitted transfer or leased
# queue item must first reach a durable boundary.
running_dispatchers="$(podman exec raijin-postgres psql -U migration -d migration -Atc \
  "SELECT count(*) FROM tasks WHERE kind='TRANSFER_CONTINUOUS' AND state='RUNNING'")"
leased_items="$(podman exec raijin-postgres psql -U migration -d migration -Atc \
  "SELECT count(*) FROM transfer_queue_items WHERE state='LEASED'")"
if ((running_dispatchers + leased_items > 0)); then
  echo "Refusing update: $((running_dispatchers + leased_items)) active transfer/lease record(s) remain" >&2
  exit 3
fi

common=(
  --network "$main_network" --dns 172.30.0.53
  -e RAIJIN_OPERATION_MODE=REAL
  -e DATABASE_URL=postgresql+psycopg://migration@postgres:5432/migration
  -e POSTGRES_PASSWORD_FILE=/run/secrets/postgres_password
  -e OCI_RUNTIME_CONFIG_FILE=/run/oci-runtime/oci-runtime.json
  -v "$secret_root/postgres_password:/run/secrets/postgres_password:ro,z"
  -v "$runtime_root:/run/platform-status:ro,z"
  -v "$local_oci_runtime:/run/oci-runtime/oci-runtime.json:ro,z"
  -v "$data_root/fujin-local/ca:/etc/fujin-local-ca:ro,z"
)

configure_private_dns() {
  local container="$1" resolver_ip resolv_conf
  resolver_ip="$(podman inspect --format "{{(index .NetworkSettings.Networks \"$main_network\").IPAddress}}" raijin-fujin-local-dns)"
  resolv_conf="$(podman inspect --format '{{.ResolvConfPath}}' "$container")"
  [[ -n "$resolver_ip" && -n "$resolv_conf" && -e "$resolv_conf" ]] || return 1
  printf 'nameserver %s\noptions ndots:1\n' "$resolver_ip" >"$resolv_conf"
}

remove_raijin() {
  podman stop -t "$stop_timeout" --ignore \
    raijin-transfer-worker raijin-governance-worker \
    raijin-raikou-shin raijin-raikou-sei \
    raijin-raikou-ko raijin-raikou-kai raijin-app
  podman rm -f --ignore \
    raijin-transfer-worker raijin-governance-worker \
    raijin-raikou-shin raijin-raikou-sei \
    raijin-raikou-ko raijin-raikou-kai raijin-app
}

start_app() {
  local name="$1" image="$2" expected_revision="${2##*:}"
  podman run -d --name "$name" --restart unless-stopped \
    --network-alias local-app --network-alias local-app-candidate "${common[@]}" "$image"
  podman network connect "$data_network" "$name"
  podman network connect --alias local-app --alias local-app-candidate "$ui_network" "$name"
  configure_private_dns "$name"
  for attempt in $(seq 1 60); do
    if podman exec "$name" python3 -c \
      "import json,urllib.request; health=json.load(urllib.request.urlopen('http://127.0.0.1:8080/healthz',timeout=2)); identity=json.load(urllib.request.urlopen('http://127.0.0.1:8080/api/runtime',timeout=2)); assert health['status']=='ok'; assert '$expected_revision' in ('latest', identity['raijin_build_revision'])" \
      >/dev/null 2>&1; then
      break
    fi
    [[ "$attempt" -lt 60 ]] || return 1
    sleep 1
  done
}

start_worker() {
  local image="$1" container="$2" worker_id="$3" worker_role="$4"
  podman run -d --name "$container" --replace --restart unless-stopped \
    "${common[@]}" -e RAIJIN_WORKER_ID="$worker_id" \
    -e RAIJIN_WORKER_ROLE="$worker_role" "$image" python3 -m app.real_worker
  podman network connect "$data_network" "$container"
  configure_private_dns "$container"
}

start_workers() {
  local image="$1"
  # One controller serializes planning. Independent consumers prevent an
  # audit or discovery from delaying restore admission and polling.
  start_worker "$image" raijin-raikou-shin raikou-shin-local raikou-shin
  start_worker "$image" raijin-raikou-sei raikou-sei-local raikou-sei
  start_worker "$image" raijin-raikou-ko raikou-ko-local raikou-ko
  start_worker "$image" raijin-raikou-kai raikou-kai-local raikou-kai
  start_worker "$image" raijin-transfer-worker raiju-local raiju
}

validate_release_images() {
  local image="$1" expected name actual
  # Podman can retain the local transport prefix in either the inspected image
  # name or the caller-supplied tag.  Compare normalized identities so a
  # successful blue/green handoff is not incorrectly aborted after workers
  # have already been replaced.
  expected="${image#localhost/}"
  for name in raijin-app raijin-raikou-shin raijin-raikou-sei raijin-raikou-ko raijin-raikou-kai raijin-transfer-worker; do
    actual="$(podman inspect --format '{{.ImageName}}' "$name")"
    # Podman qualifies locally built images as localhost/name:tag while the
    # requested release identity intentionally omits that transport prefix.
    actual="${actual#localhost/}"
    [[ "$actual" == "$expected" ]] || {
      echo "Release mismatch: $name uses $actual, expected $expected" >&2
      return 1
    }
  done
  actual="$(podman inspect --format '{{.ImageName}}' raijin-fujin-local)"
  if [[ "$actual" != "$image" ]]; then
    echo "Alignment notice: Fujin remains on $actual while Raijin targets $image; Fujin was intentionally not replaced."
  fi
}

install_platform_status_runtime() {
  local next_monotonic
  # Host telemetry is part of the release contract even though it runs
  # outside the containers. Reinstall and restart it on every rollout so an
  # already-active timer cannot retain an elapsed schedule from an older unit.
  install -m 0750 "$release_root/scripts/write-platform-status.sh" \
    /usr/local/sbin/raijin-write-platform-status
  cat >/etc/systemd/system/raijin-platform-status.service <<'EOF'
[Unit]
Description=Write S3 to OCI migration platform status
After=raijin-data-migration.service

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/raijin-write-platform-status
EOF
  cat >/etc/systemd/system/raijin-platform-status.timer <<'EOF'
[Unit]
Description=Refresh S3 to OCI migration platform status

[Timer]
OnActiveSec=30
OnUnitActiveSec=60
AccuracySec=5
Persistent=true

[Install]
WantedBy=timers.target
EOF
  systemctl daemon-reload
  systemctl enable raijin-platform-status.timer
  systemctl restart raijin-platform-status.timer
  systemctl start raijin-platform-status.service
  next_monotonic="$(systemctl show raijin-platform-status.timer -p NextElapseUSecMonotonic --value)"
  [[ -n "$next_monotonic" && "$next_monotonic" != 0 ]]
}

abort_update() {
  local status=$?
  trap - ERR
  echo "Target rollout failed; no automatic rollback is configured" >&2
  podman rm -f --ignore "$candidate_name" || true
  if ((handoff_started)); then
    remove_raijin || true
  fi
  exit "$status"
}
trap abort_update ERR

# Blue/green handoff: validate the candidate while the existing API remains
# reachable through the shared private alias. Only then stop the old control
# plane. The active-transfer guard above makes the worker boundary safe.
podman rm -f --ignore "$candidate_name"
start_app "$candidate_name" "$target_image"
handoff_started=1
podman stop -t "$stop_timeout" --ignore raijin-transfer-worker raijin-governance-worker raijin-raikou-shin raijin-raikou-sei raijin-raikou-ko raijin-raikou-kai
podman rm -f --ignore raijin-transfer-worker raijin-governance-worker raijin-raikou-shin raijin-raikou-sei raijin-raikou-ko raijin-raikou-kai
podman stop -t "$stop_timeout" --ignore raijin-app
podman rm -f --ignore raijin-app
podman rename "$candidate_name" raijin-app
# Force the local gateway to discard any cached address of the retired app.
podman exec raijin-local-ui-gateway nginx -s reload
start_workers "$target_image"
sleep 3
for name in raijin-app raijin-raikou-shin raijin-raikou-sei raijin-raikou-ko raijin-raikou-kai raijin-transfer-worker; do
  [[ "$(podman inspect --format '{{.State.Running}}' "$name")" == true ]]
done
curl --fail --silent --show-error "$RAIJIN_WEB_BIND_PROBE_URL/raijin/healthz"
validate_release_images "$target_image"
install_platform_status_runtime
# The freshly generated host snapshot must be accepted by the target API.
podman exec raijin-app python3 -c \
  "import json,urllib.request; status=json.load(urllib.request.urlopen('http://127.0.0.1:8080/api/platform/status',timeout=2)); assert status['available'] and not status.get('stale')"
trap - ERR
echo
echo "Raijin LOCAL control plane updated to $target_image"
