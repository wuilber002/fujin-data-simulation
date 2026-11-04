#!/usr/bin/env python3
"""Apply the explicit, isolated Fujin Provider catalogue migration."""

from __future__ import annotations

import os

from app.provider_schema import migrate


def main() -> None:
    url = os.environ.get("FUJIN_PROVIDER_DATABASE_URL", "").strip()
    if not url:
        raise SystemExit("FUJIN_PROVIDER_DATABASE_URL is required")
    migrate(url)
    print("Fujin Provider schema is current")


if __name__ == "__main__":
    main()
