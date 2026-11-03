#!/usr/bin/env python3
"""Derive an OCI runtime profile for the private Fujin endpoint.

The Raijin keeps its usual OCI SDK configuration and Instance Principal.  A
LOCAL deployment needs only a separate runtime JSON so the Object Storage
client uses the private service endpoint and its CA; the REAL profile remains
untouched for a clean switch back to production.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import stat


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--source", type=Path, required=True, help="Existing REAL OCI runtime JSON")
    result.add_argument("--output", type=Path, required=True, help="Derived LOCAL OCI runtime JSON")
    result.add_argument("--namespace", required=True, help="Namespace configured in the LOCAL OCI bucket")
    result.add_argument("--region", default="sa-saopaulo-1", help="Fujin private OCI endpoint region")
    result.add_argument("--ca-bundle", default="/etc/fujin-local-ca/fujin-local.crt")
    return result


def main() -> None:
    args = parser().parse_args()
    try:
        source = json.loads(args.source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit(f"Cannot read OCI runtime source: {error}") from error
    if not isinstance(source, dict):
        raise SystemExit("OCI runtime source must be a JSON object")
    namespace = args.namespace.strip()
    if not namespace:
        raise SystemExit("--namespace must not be empty")
    region = args.region.strip()
    if not region:
        raise SystemExit("--region must not be empty")
    source.update({
        "object_storage_namespace": namespace,
        "object_storage_endpoint_url": f"https://oci.{region}.fujin.internal",
        "object_storage_ca_bundle_path": args.ca_bundle,
    })
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary.write_text(json.dumps(source, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    temporary.chmod(stat.S_IRUSR | stat.S_IWUSR)
    temporary.replace(args.output)
    print(f"Wrote isolated Fujin LOCAL OCI runtime profile: {args.output}")


if __name__ == "__main__":
    main()
