#!/bin/sh
set -eu

# The launcher explicitly supplies the resolver of Raijin's main private
# network.  Falling back to resolv.conf retains Compose compatibility, but on
# Oracle Linux a second Podman network can otherwise make the UI-only resolver
# the first entry even though that resolver is unreachable from the gateway.
# Render only this explicit placeholder, leaving Nginx variables (such as
# $host) untouched.  This makes the gateway follow Fujin when its IP changes.
resolver="${FUJIN_UI_RESOLVER:-$(awk '/^nameserver[[:space:]]+/{print $2; exit}' /etc/resolv.conf)}"
if [ -z "$resolver" ]; then
  echo "LOCAL UI gateway requires a private DNS resolver" >&2
  exit 1
fi

sed "s/__FUJIN_DNS_RESOLVER__/${resolver}/g" \
  /opt/fujin/nginx-local-ui.conf > /etc/nginx/conf.d/default.conf
exec nginx -g 'daemon off;'
