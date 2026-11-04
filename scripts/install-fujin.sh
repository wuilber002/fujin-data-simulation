#!/usr/bin/env bash
# Install a pinned Fujin release on a Podman host without Terraform.
set -euo pipefail

repo_url="https://github.com/wuilber002/fujin-data-simulation.git"
revision="main"
install_root="/opt/fujin-data-simulation"
config_dir="/etc/fujin-data-simulation"
clean=0

usage() {
  echo "Usage: sudo $0 [--source URL] [--revision REF] [--root PATH] [--clean]"
}
while (($#)); do
  case "$1" in
    --source) repo_url="$2"; shift 2 ;;
    --revision) revision="$2"; shift 2 ;;
    --root) install_root="$2"; shift 2 ;;
    --clean) clean=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; exit 2 ;;
  esac
done

command -v podman >/dev/null || { echo "podman is required" >&2; exit 1; }
command -v git >/dev/null || { echo "git is required" >&2; exit 1; }
[[ $EUID -eq 0 ]] || { echo "run this installer with sudo" >&2; exit 1; }

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
systemctl enable --now fujin-data-simulation.service
echo "Fujin revision $resolved_revision installed. Console: http://127.0.0.1:8090/console"
