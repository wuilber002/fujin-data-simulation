#!/usr/bin/env bash
set -euo pipefail

# LOCAL data endpoints use an internal CA.  The certificate is generated once
# into the durable Fujin volume and clients mount only the public CA file.
cert_root="${FUJIN_LOCAL_TLS_DIRECTORY:-/var/lib/fujin-local/tls}"
cert_file="$cert_root/fujin-local.crt"
key_file="$cert_root/fujin-local.key"
aws_region="${FUJIN_LOCAL_AWS_REGION:-${FUJIN_LOCAL_REGION:-us-east-1}}"
oci_region="${FUJIN_LOCAL_OCI_REGION:-sa-saopaulo-1}"
endpoint_id="${FUJIN_LOCAL_VPCE_ID:-vpce-fujin-local}"
mkdir -p "$cert_root"
chmod 0700 "$cert_root"

renew_certificate=false
if [[ ! -s "$cert_file" || ! -s "$key_file" ]]; then
  renew_certificate=true
elif ! {
  openssl x509 -in "$cert_file" -noout -ext subjectAltName 2>/dev/null | grep -Fq "DNS:*.${endpoint_id}.s3.${aws_region}.fujin.internal" &&
  openssl x509 -in "$cert_file" -noout -ext subjectAltName 2>/dev/null | grep -Fq "DNS:sts.${aws_region}.fujin.internal" &&
  openssl x509 -in "$cert_file" -noout -ext subjectAltName 2>/dev/null | grep -Fq "DNS:control.${endpoint_id}.s3.${aws_region}.fujin.internal" &&
  openssl x509 -in "$cert_file" -noout -ext subjectAltName 2>/dev/null | grep -Fq "DNS:*.control.${endpoint_id}.s3.${aws_region}.fujin.internal" &&
  openssl x509 -in "$cert_file" -noout -ext subjectAltName 2>/dev/null | grep -Fq "DNS:oci.${oci_region}.fujin.internal"
}; then
  # A release before the OCI endpoint used a valid CA but lacked this
  # multi-label hostname. Regenerate only that local, self-signed material;
  # it is never an externally issued certificate.
  renew_certificate=true
fi

if [[ "$renew_certificate" == true ]]; then
  openssl req -x509 -newkey rsa:3072 -nodes -sha256 -days 3650 \
    -keyout "$key_file" -out "$cert_file" \
    -subj '/CN=*.fujin.internal' \
    -addext "subjectAltName=DNS:*.fujin.internal,DNS:fujin.internal,DNS:*.${endpoint_id}.s3.${aws_region}.fujin.internal,DNS:${endpoint_id}.s3.${aws_region}.fujin.internal,DNS:sts.${aws_region}.fujin.internal,DNS:control.${endpoint_id}.s3.${aws_region}.fujin.internal,DNS:*.control.${endpoint_id}.s3.${aws_region}.fujin.internal,DNS:oci.${oci_region}.fujin.internal" >/dev/null 2>&1
  chmod 0600 "$key_file"
  chmod 0644 "$cert_file"
fi

## Private AWS and OCI endpoint URLs intentionally omit a port, matching the
## public SDK contracts.  Serve TLS on the standard HTTPS port inside the
## isolated provider network so boto3 and the OCI SDK need no LOCAL-specific
## endpoint alteration.
exec python3 -m uvicorn app.fujin_local:app --host 0.0.0.0 --port 443 \
  --ssl-certfile "$cert_file" --ssl-keyfile "$key_file" \
  --timeout-graceful-shutdown 90
