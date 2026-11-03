#!/usr/bin/env python3
"""Preview or apply the AWS UTC-midnight repair to Fujin LOCAL restores."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.fujin_local_schema import repair_restore_expiries


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", default=os.environ.get(
        "FUJIN_LOCAL_DATABASE_URL", "sqlite+pysqlite:////tmp/fujin-local.db"
    ))
    parser.add_argument("--apply", action="store_true", help="persist the repair; default is dry-run")
    args = parser.parse_args()
    print(json.dumps(
        repair_restore_expiries(args.database_url, dry_run=not args.apply),
        indent=2,
        sort_keys=True,
    ))


if __name__ == "__main__":
    main()
