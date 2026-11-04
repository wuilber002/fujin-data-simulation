#!/usr/bin/env bash
# Install a pinned Fujin release on a Podman host without Terraform.
set -euo pipefail

repo_url="https://github.com/wuilber002/fujin-data-simulation.git"
revision="main"
install_root="/opt/fujin-data-simulation"
config_dir="/etc/fujin-data-simulation"
clean=0
skip_start=0

usage() {
  cat <<'EOF'
Usage: sudo install-fujin.sh [options]

Options:
  --source URL       Git repository to install
  --revision REF     branch or tag to install (default: main)
  --root PATH        release root (default: /opt/fujin-data-simulation)
  --clean            replace the current release; persistent catalogues remain
  --skip-start       install files and systemd unit without starting services

The installer installs Podman, Git and curl when an Oracle Linux/RHEL or
Debian/Ubuntu package manager is available. Fujin has no cloud credentials or
bootstrap password: it is loopback-only by default.
EOF
}
while (($#)); do
  case "$1" in
    --source) repo_url="$2"; shift 2 ;;
    --revision) revision="$2"; shift 2 ;;
    --root) install_root="$2"; shift 2 ;;
    --clean) clean=1; shift ;;
    --skip-start) skip_start=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; exit 2 ;;
  esac
done

[[ $EUID -eq 0 ]] || { echo "run this installer with sudo" >&2; exit 1; }

install_dependencies() {
  local missing=()
  local command
  for command in podman git curl systemctl; do
    command -v "$command" >/dev/null 2>&1 || missing+=("$command")
  done
  ((${#missing[@]} == 0)) && return 0
  if command -v dnf >/dev/null 2>&1; then
    dnf install -y podman git curl
  elif command -v apt-get >/dev/null 2>&1; then
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y podman git curl
  else
    echo "Missing required commands: ${missing[*]}. Install Podman, Git and curl first." >&2
    return 1
  fi
  for command in "${missing[@]}"; do
    command -v "$command" >/dev/null || { echo "Dependency installation did not provide $command" >&2; return 1; }
  done
}

install_dependencies

if ((clean)); then
  "$install_root/release/scripts/stop-fujin.sh" 2>/dev/null || true
  rm -rf "$install_root/release"
fi
mkdir -p "$install_root" "$config_dir" /var/lib/fujin-data-simulation
rm -rf "$install_root/release.new"
git clone --depth 1 --branch "$revision" "$repo_url" "$install_root/release.new"
if [[ -d "$install_root/release" ]]; then
  "$install_root/release/scripts/stop-fujin.sh" 2>/dev/null || true
  rm -rf "$install_root/release.previous"
  mv "$install_root/release" "$install_root/release.previous"
fi
mv "$install_root/release.new" "$install_root/release"
rm -rf "$install_root/release.previous"
resolved_revision="$(git -C "$install_root/release" rev-parse HEAD)"

install -m 0644 /dev/null "$config_dir/fujin.env"
cat > "$config_dir/fujin.env" <<EOF
FUJIN_BUILD_REVISION=$resolved_revision
FUJIN_DATA_DIR=/var/lib/fujin-data-simulation
FUJIN_BIND_HOST=127.0.0.1
FUJIN_BIND_PORT=8090
FUJIN_SIMULATOR_BIND_HOST=127.0.0.1
FUJIN_SIMULATOR_BIND_PORT=8091
EOF
cat > /etc/systemd/system/fujin-data-simulation.service <<EOF
[Unit]
Description=Fujin data simulation platform
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
EnvironmentFile=$config_dir/fujin.env
ExecStart=$install_root/release/scripts/start-fujin.sh
ExecStop=$install_root/release/scripts/stop-fujin.sh

[Install]
WantedBy=multi-user.target
EOF
chmod 0755 "$install_root/release/scripts/install-fujin.sh" \
  "$install_root/release/scripts/start-fujin.sh" \
  "$install_root/release/scripts/stop-fujin.sh" \
  "$install_root/release/scripts/migrate-fujin-simulator.py"
systemctl daemon-reload
if ((skip_start)); then
  systemctl enable fujin-data-simulation.service
  echo "Fujin revision $resolved_revision installed but not started."
  exit 0
fi
systemctl enable --now fujin-data-simulation.service
curl --fail --silent http://127.0.0.1:8090/readyz >/dev/null
curl --fail --silent http://127.0.0.1:8091/readyz >/dev/null
echo "Fujin revision $resolved_revision installed and ready. Console: http://127.0.0.1:8090/console"
