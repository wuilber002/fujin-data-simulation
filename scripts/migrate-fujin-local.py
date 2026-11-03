#!/usr/bin/env python3
"""Apply the explicit, isolated Fujin LOCAL catalogue migration."""

from __future__ import annotations

import os

from app.fujin_local_schema import migrate


def main() -> None:
    url = os.environ.get("FUJIN_LOCAL_DATABASE_URL", "").strip()
    if not url:
        raise SystemExit("FUJIN_LOCAL_DATABASE_URL is required")
    migrate(url)
    print("Fujin LOCAL schema is current")


if __name__ == "__main__":
    main()
