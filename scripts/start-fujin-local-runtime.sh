#!/usr/bin/env bash
set -euo pipefail

# Deploy the LOCAL private cloud on the Oracle Linux/Podman runtime.  LOCAL is
# deliberately not a Raijin operation mode: every Raijin process below runs
# with RAIJIN_OPERATION_MODE=REAL and reaches Fujin only through ordinary SDK
# endpoint settings and the isolated OCI runtime profile.
install_root="${RAIJIN_INSTALL_ROOT:-/opt/raijin-data-migration/release}"
data_root="${RAIJIN_DATA_ROOT:-/var/lib/raijin-data-migration}"
secret_root="${RAIJIN_SECRET_ROOT:-/etc/raijin-data-migration/secrets}"
runtime_root="${RAIJIN_RUNTIME_ROOT:-/run/raijin-data-migration}"
image="${RAIJIN_IMAGE:-localhost/raijin-data-migration:latest}"
main_network="${RAIJIN_MAIN_NETWORK:-raijin-data-migration}"
data_network="${FUJIN_LOCAL_DATA_NETWORK:-raijin-fujin-local-data}"
ui_network="${FUJIN_LOCAL_UI_NETWORK:-raijin-fujin-local-ui}"
local_oci_runtime="${RAIJIN_LOCAL_OCI_RUNTIME_CONFIG:-/etc/raijin-data-migration/oci-runtime-local.json}"

# shellcheck source=web-bind.sh
source "$install_root/scripts/web-bind.sh"
raijin_load_web_bind
source "$install_root/scripts/console-auth-lifecycle.sh"
# An old release directory can launch the compatible disabled mode while the
# host is being upgraded. Local authentication is always opt-in, never an
# implicit downgrade or activation during that transition.
if [[ -r "$install_root/scripts/web-auth.sh" ]]; then
  # shellcheck source=web-auth.sh
  source "$install_root/scripts/web-auth.sh"
  raijin_load_web_auth
else
  RAIJIN_WEB_AUTH_MODE=disabled
  RAIJIN_WEB_EXTERNAL_HTTPS=false
  RAIJIN_WEB_AUTH_TRUSTED_PROXY_CIDRS=
  RAIJIN_WEB_AUTH_SESSION_IDLE_MINUTES=30
  RAIJIN_WEB_AUTH_SESSION_ABSOLUTE_MINUTES=720
  RAIJIN_WEB_AUTH_BOOTSTRAP_USERNAME=admin
fi

[[ -s "$local_oci_runtime" ]] || {
  echo "Missing $local_oci_runtime. Create it with configure-fujin-local-oci-runtime first." >&2
  exit 2
}
mountpoint -q "$data_root/fujin-payloads" || {
  echo "Fujin physical payload volume is not mounted at $data_root/fujin-payloads." >&2
  exit 2
}

# This is a deployment transition, not an activation.  It refuses to replace
# an active migration runtime: drain it first, then start the LOCAL deployment.
for name in raijin-app raijin-governance-worker raijin-raikou-shin raijin-raikou-sei raijin-raikou-ko raijin-raikou-kai raijin-transfer-worker raijin-simulator; do
  if podman container exists "$name"; then
    echo "Container $name is active; drain and stop the current runtime before deploying LOCAL." >&2
    exit 3
  fi
done
if ! podman container exists raijin-postgres; then
  # `raijin-stop-runtime` intentionally removes PostgreSQL too.  Recreate the
  # same durable database service here so the documented drained transition is
  # self-contained; its data and password remain on host volumes.
  podman run -d --name raijin-postgres --replace --restart unless-stopped \
    --shm-size=512m --network "$main_network" --network-alias postgres \
    -e POSTGRES_DB=migration -e POSTGRES_USER=migration \
    -e POSTGRES_PASSWORD_FILE=/run/secrets/postgres_password \
    -v "$data_root/postgres:/var/lib/postgresql/data:Z" \
    -v "$secret_root/postgres_password:/run/secrets/postgres_password:ro,z" \
    docker.io/library/postgres:16-alpine
fi
for attempt in $(seq 1 30); do
  podman exec raijin-postgres pg_isready -U migration -d migration >/dev/null 2>&1 && break
  [[ "$attempt" -lt 30 ]] || { echo "PostgreSQL did not become ready" >&2; exit 1; }
  sleep 2
done

console_auth_lifecycle=READY
if [[ "$RAIJIN_WEB_AUTH_MODE" == local ]]; then
  console_auth_lifecycle="$(raijin_console_auth_lifecycle_state)"
  if [[ "$console_auth_lifecycle" == BOOTSTRAP_REQUIRED ]]; then
    raijin_force_loopback_web_bind "$RAIJIN_WEB_BIND_PORT"
  elif [[ -e "$secret_root/console_bootstrap_password" ]]; then
    echo "Console bootstrap file is still present; keeping the web bind on 127.0.0.1." >&2
    raijin_force_loopback_web_bind "$RAIJIN_WEB_BIND_PORT"
  fi
fi
web_auth_environment=(
  -e "RAIJIN_WEB_BIND_HOST=$RAIJIN_WEB_BIND_HOST"
  -e "RAIJIN_WEB_AUTH_MODE=$RAIJIN_WEB_AUTH_MODE"
  -e "RAIJIN_WEB_EXTERNAL_HTTPS=$RAIJIN_WEB_EXTERNAL_HTTPS"
  -e "RAIJIN_WEB_AUTH_TRUSTED_PROXY_CIDRS=$RAIJIN_WEB_AUTH_TRUSTED_PROXY_CIDRS"
  -e "RAIJIN_WEB_AUTH_SESSION_IDLE_MINUTES=$RAIJIN_WEB_AUTH_SESSION_IDLE_MINUTES"
  -e "RAIJIN_WEB_AUTH_SESSION_ABSOLUTE_MINUTES=$RAIJIN_WEB_AUTH_SESSION_ABSOLUTE_MINUTES"
  -e "RAIJIN_WEB_AUTH_BOOTSTRAP_REQUIRED=$([[ "$console_auth_lifecycle" == BOOTSTRAP_REQUIRED ]] && echo true || echo false)"
  -e "RAIJIN_WEB_AUTH_BOOTSTRAP_USERNAME=$RAIJIN_WEB_AUTH_BOOTSTRAP_USERNAME"
)
web_auth_secret_mounts=()
if [[ "$RAIJIN_WEB_AUTH_MODE" == local && "$console_auth_lifecycle" == BOOTSTRAP_REQUIRED ]]; then
  [[ -s "$secret_root/console_bootstrap_password" ]] || {
    echo "Initial local web authentication requires console_bootstrap_password from a protected local file" >&2
    exit 2
  }
  web_auth_environment+=( -e RAIJIN_WEB_AUTH_BOOTSTRAP_PASSWORD_FILE=/run/secrets/console_bootstrap_password )
  web_auth_secret_mounts+=( -v "$secret_root/console_bootstrap_password:/run/secrets/console_bootstrap_password:ro,z" )
fi

podman network exists "$data_network" 2>/dev/null || podman network create --internal --subnet 172.30.0.0/24 "$data_network"
podman network exists "$ui_network" 2>/dev/null || podman network create --internal "$ui_network"
install -d -m 0700 "$data_root/fujin-local" "$data_root/fujin-local/staging"

## CoreDNS must reach the VCN resolver to forward OCI public service names
## required by Raijin's ordinary Instance Principal flow.  The provider and
## payload services remain on the internal data network; only this resolver
## also joins the existing Raijin network.
podman run -d --name raijin-fujin-local-dns --replace --restart unless-stopped \
  --network "$main_network" \
  -v "$install_root/docker/fujin-local.Corefile:/etc/coredns/Corefile:ro,z" \
  docker.io/coredns/coredns:1.11.3 -conf /etc/coredns/Corefile
podman network connect --ip 172.30.0.53 "$data_network" raijin-fujin-local-dns

# The provider and materializer share the durable SQLite catalog.  `:z` gives
# both containers the same SELinux label; `:Z` would relabel it for whichever
# one started last and make the other fail to open the database.
podman run -d --name raijin-fujin-local --replace --restart unless-stopped \
  --network "$data_network" --ip 172.30.0.10 --network-alias fujin-local \
  -e FUJIN_LOCAL_DATABASE_URL=sqlite+pysqlite:////var/lib/fujin-local/catalog.db \
  -e FUJIN_LOCAL_PAYLOAD_ROOT=/var/lib/fujin-local/payloads \
  -e FUJIN_LOCAL_STAGING_ROOT=/var/lib/fujin-local/staging \
  -e FUJIN_LOCAL_AWS_REGION=us-east-1 -e FUJIN_LOCAL_OCI_REGION=sa-saopaulo-1 -e FUJIN_LOCAL_VPCE_ID=vpce-fujin-local \
  -v "$data_root/fujin-local:/var/lib/fujin-local:z" \
  -v "$data_root/fujin-payloads:/var/lib/fujin-local/payloads:ro,z" \
  "$image" scripts/run-fujin-local.sh

# Dataset bytes are owned by Fujin LOCAL, never by Simulation or Raijin.  Keep
# the S3 provider read-only and give the separate durable materializer the
# only read/write mount.  It has no listener and is not attached to any
# Raijin-facing network.
podman run -d --name raijin-fujin-local-materializer --replace --restart unless-stopped \
  -e FUJIN_LOCAL_DATABASE_URL=sqlite+pysqlite:////var/lib/fujin-local/catalog.db \
  -e FUJIN_LOCAL_PAYLOAD_ROOT=/var/lib/fujin-local/payloads \
  -e FUJIN_LOCAL_STAGING_ROOT=/var/lib/fujin-local/staging \
  -v "$data_root/fujin-local:/var/lib/fujin-local:z" \
  -v "$data_root/fujin-payloads:/var/lib/fujin-local/payloads:rw,z" \
  "$image" python3 -m app.fujin_local_materializer
podman network connect --alias fujin-local "$ui_network" raijin-fujin-local 2>/dev/null || true
# Publish the same private alias on Raijin's internal network.  This is still
# not host/LAN exposed, and lets the gateway use the reliable main resolver
# after Podman attaches its UI network.
podman network connect --alias fujin-local "$main_network" raijin-fujin-local 2>/dev/null || true

for attempt in $(seq 1 30); do
  podman exec raijin-fujin-local python3 -c "import ssl,urllib.request; urllib.request.urlopen('https://127.0.0.1:443/healthz',context=ssl._create_unverified_context(),timeout=2)" >/dev/null 2>&1 && break
  [[ "$attempt" -lt 30 ]] || { echo "Fujin LOCAL did not become healthy" >&2; exit 1; }
  sleep 2
done

# Raijin needs the public CA but must never receive the private endpoint key.
# The TLS directory is deliberately owner-only; publish a copy of the public
# certificate in a distinct read-only mount after Fujin has generated it.
ca_root="$data_root/fujin-local/ca"
install -d -m 0755 "$ca_root"
# A dry-run launcher contract may stub podman without materialising the
# certificate.  The real health check above guarantees it exists before the
# Raijin containers are started in an actual deployment.
if [[ -s "$data_root/fujin-local/tls/fujin-local.crt" ]]; then
  install -m 0644 "$data_root/fujin-local/tls/fujin-local.crt" "$ca_root/fujin-local.crt"
fi

common=(--network "$main_network" --dns 172.30.0.53 -e RAIJIN_OPERATION_MODE=REAL -e DATABASE_URL=postgresql+psycopg://migration@postgres:5432/migration -e POSTGRES_PASSWORD_FILE=/run/secrets/postgres_password "${web_auth_environment[@]}" "${web_auth_secret_mounts[@]}" -e OCI_RUNTIME_CONFIG_FILE=/run/oci-runtime/oci-runtime.json -v "$secret_root/postgres_password:/run/secrets/postgres_password:ro,z" -v "$runtime_root:/run/platform-status:ro,z" -v "$local_oci_runtime:/run/oci-runtime/oci-runtime.json:ro,z" -v "$ca_root:/etc/fujin-local-ca:ro,z")
podman run -d --name raijin-app --replace --restart unless-stopped --network-alias local-app "${common[@]}" "$image"
podman network connect "$data_network" raijin-app
# This is a control-plane-only attachment: it lets the UI gateway resolve the
# Raijin app without giving the gateway access to the data network.
podman network connect --alias local-app "$ui_network" raijin-app
for worker in \
  'raijin-raikou-shin:raikou-shin-local:raikou-shin' \
  'raijin-raikou-sei:raikou-sei-local:raikou-sei' \
  'raijin-raikou-ko:raikou-ko-local:raikou-ko' \
  'raijin-raikou-kai:raikou-kai-local:raikou-kai' \
  'raijin-transfer-worker:raiju-local:raiju'; do
  IFS=: read -r container worker_id worker_role <<<"$worker"
  podman run -d --name "$container" --replace --restart unless-stopped "${common[@]}" \
    -e RAIJIN_WORKER_ID="$worker_id" -e RAIJIN_WORKER_ROLE="$worker_role" \
    "$image" python3 -m app.real_worker
  podman network connect "$data_network" "$container"
done

# Netavark rewrites a container's resolv.conf when a second network is
# attached and does not preserve Podman's --dns option on this Oracle Linux
# runtime.  Point the ordinary Raijin containers at the CoreDNS address on
# the main network only after all attachments are complete.  CoreDNS resolves
# the private Fujin zone and forwards everything else to the VCN resolver.
resolver_ip="$(podman inspect --format "{{(index .NetworkSettings.Networks \"$main_network\").IPAddress}}" raijin-fujin-local-dns)"
configure_private_dns() {
  local container="$1" resolv_conf
  resolv_conf="$(podman inspect --format '{{.ResolvConfPath}}' "$container")"
  # The launcher contract test uses a no-op Podman shim, which has no
  # generated resolv.conf.  A real inspect always supplies this path.
  [[ -n "$resolv_conf" && -e "$resolv_conf" ]] || return 0
  printf 'nameserver %s\noptions ndots:1\n' "$resolver_ip" > "$resolv_conf"
}
configure_private_dns raijin-app
configure_private_dns raijin-raikou-shin
configure_private_dns raijin-raikou-sei
configure_private_dns raijin-raikou-ko
configure_private_dns raijin-raikou-kai
configure_private_dns raijin-transfer-worker

ui_resolver_ip="$(podman inspect --format "{{(index .NetworkSettings.Networks \"$main_network\").Gateway}}" raijin-fujin-local)"
podman create --name raijin-local-ui-gateway --replace --restart unless-stopped \
  --network "$main_network" -p "$RAIJIN_WEB_BIND_PUBLISH" \
  -e FUJIN_UI_RESOLVER="$ui_resolver_ip" \
  -v "$install_root/docker/nginx-local-ui.conf:/opt/fujin/nginx-local-ui.conf:ro,z" \
  -v "$install_root/docker/run-local-ui-gateway.sh:/opt/fujin/run-local-ui-gateway.sh:ro,z" \
  docker.io/library/nginx:1.27-alpine /bin/sh /opt/fujin/run-local-ui-gateway.sh
podman network connect "$ui_network" raijin-local-ui-gateway
# The second attachment can make Netavark select a different resolver. Keep
# the gateway on the main private-network resolver, where both control-plane
# aliases are published and where this Podman runtime has a routable path.
gateway_resolv_conf="$(podman inspect --format '{{.ResolvConfPath}}' raijin-local-ui-gateway)"
if [[ -n "$ui_resolver_ip" && -n "$gateway_resolv_conf" && -e "$gateway_resolv_conf" ]]; then
  printf 'nameserver %s\noptions ndots:1\n' "$ui_resolver_ip" > "$gateway_resolv_conf"
fi
podman start raijin-local-ui-gateway
echo "LOCAL private cloud is ready: $RAIJIN_WEB_BIND_PROBE_URL/raijin/ and /fujin/"
