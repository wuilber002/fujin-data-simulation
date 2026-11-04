#!/usr/bin/env python3
"""Apply the explicit Fujin simulator catalogue migration."""

from __future__ import annotations

from sqlalchemy import create_engine

from app.simulation_migrations import migrate
from app.simulator_store import simulator_database_url


def main() -> None:
    engine = create_engine(simulator_database_url(), pool_pre_ping=True)
    revision = migrate(engine)
    print(f"Fujin simulator schema is current (revision {revision})")


if __name__ == "__main__":
    main()
