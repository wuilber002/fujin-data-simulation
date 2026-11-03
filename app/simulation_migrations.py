"""Explicit, idempotent migrations for the simulation catalog."""

from __future__ import annotations

from datetime import timezone

from sqlalchemy import Engine, inspect, select, text
from sqlalchemy.orm import Session

from app.aws_restore_semantics import s3_restore_expiry
from app.simulation_schema import (
    FujinPayloadDataset,
    FujinPayloadFile,
    FujinPayloadGenerationJob,
    FujinPayloadValidationJob,
    GeneratorRelease,
    SIMULATION_SCHEMA_VERSION,
    SimulationBase,
    SimulationSchemaRevision,
    VirtualObject,
)


class SimulationMigrationError(RuntimeError):
    pass


def current_revision(engine: Engine) -> int:
    if not inspect(engine).has_table(SimulationSchemaRevision.__tablename__):
        return 0
    with Session(engine) as session:
        return int(
            session.scalar(select(SimulationSchemaRevision.version).order_by(
                SimulationSchemaRevision.version.desc()
            ))
            or 0
        )


def migrate(engine: Engine) -> int:
    revision = current_revision(engine)
    if revision > SIMULATION_SCHEMA_VERSION:
        raise SimulationMigrationError(
            f"Database revision {revision} is newer than supported revision "
            f"{SIMULATION_SCHEMA_VERSION}"
        )
    if revision == SIMULATION_SCHEMA_VERSION:
        return revision

    # Migrations are append-only and explicit. They never mutate production
    # tables and never run as a side effect of API startup.
    if revision < 1:
        SimulationBase.metadata.create_all(engine)
        with Session(engine) as session:
            session.add(
                SimulationSchemaRevision(
                    version=1,
                    description="Initial isolated simulator catalog",
                )
            )
            session.commit()
        revision = 1
    if revision < 2:
        GeneratorRelease.__table__.create(engine, checkfirst=True)
        with Session(engine) as session:
            session.add(
                SimulationSchemaRevision(
                    version=2,
                    description="Deterministic generator release lifecycle",
                )
            )
            session.commit()
        revision = 2
    if revision < 3:
        # SQLite does not enforce VARCHAR lengths and fresh schemas already use
        # the current model. PostgreSQL needs an explicit widening migration
        # for catalogs created before logical checksums gained their prefix.
        if engine.dialect.name == "postgresql":
            with engine.begin() as connection:
                connection.execute(text(
                    "ALTER TABLE sim_virtual_objects "
                    "ALTER COLUMN source_sha256 TYPE VARCHAR(128)"
                ))
        with Session(engine) as session:
            session.add(
                SimulationSchemaRevision(
                    version=3,
                    description="Allow prefixed logical SHA-256 evidence",
                )
            )
            session.commit()
        revision = 3
    if revision < 4:
        columns = {item["name"] for item in inspect(engine).get_columns("sim_clocks")}
        with engine.begin() as connection:
            if "hold_count" not in columns:
                connection.execute(text(
                    "ALTER TABLE sim_clocks ADD COLUMN hold_count INTEGER NOT NULL DEFAULT 0"
                ))
            if "held_virtual_at" not in columns:
                timestamp_type = "TIMESTAMP WITH TIME ZONE" if engine.dialect.name == "postgresql" else "DATETIME"
                connection.execute(text(
                    f"ALTER TABLE sim_clocks ADD COLUMN held_virtual_at {timestamp_type}"
                ))
        with Session(engine) as session:
            session.add(SimulationSchemaRevision(
                version=4,
                description="Phase-owned virtual clock holds",
            ))
            session.commit()
        revision = 4
    if revision < 5:
        # Fresh schemas obtain these definitions from ``create_all``. Existing
        # catalogs need additive columns and independent Fujin repository
        # tables. Legacy rows keep the deterministic default.
        columns = {item["name"] for item in inspect(engine).get_columns("sim_virtual_objects")}
        additions = {
            "payload_kind": "VARCHAR(24) NOT NULL DEFAULT 'DETERMINISTIC'",
            "payload_dataset_id": "VARCHAR(36)",
            "payload_file_id": "VARCHAR(36)",
            "payload_relative_path": "VARCHAR(1024)",
            "payload_size_bytes": "BIGINT",
            "payload_mtime_ns": "BIGINT",
            "payload_identity": "VARCHAR(255)",
            "payload_sha256": "VARCHAR(64)",
        }
        with engine.begin() as connection:
            for name, definition in additions.items():
                if name not in columns:
                    connection.execute(text(
                        f"ALTER TABLE sim_virtual_objects ADD COLUMN {name} {definition}"
                    ))
        FujinPayloadDataset.__table__.create(engine, checkfirst=True)
        FujinPayloadFile.__table__.create(engine, checkfirst=True)
        FujinPayloadGenerationJob.__table__.create(engine, checkfirst=True)
        with Session(engine) as session:
            session.add(SimulationSchemaRevision(
                version=5,
                description="Fujin-managed local payload repository catalog",
            ))
            session.commit()
        revision = 5
    if revision < 6:
        columns = {item["name"] for item in inspect(engine).get_columns("sim_executions")}
        additions = {
            "physical_read_operations": "INTEGER NOT NULL DEFAULT 0",
            "physical_local_bytes_read": "BIGINT NOT NULL DEFAULT 0",
            "physical_read_seconds": "DOUBLE PRECISION NOT NULL DEFAULT 0",
            "physical_snapshot_failures": "INTEGER NOT NULL DEFAULT 0",
        }
        with engine.begin() as connection:
            for name, definition in additions.items():
                if name not in columns:
                    connection.execute(text(
                        f"ALTER TABLE sim_executions ADD COLUMN {name} {definition}"
                    ))
        with Session(engine) as session:
            session.add(SimulationSchemaRevision(
                version=6,
                description="Physical payload reader telemetry",
            ))
            session.commit()
        revision = 6
    if revision < 7:
        columns = {item["name"] for item in inspect(engine).get_columns("sim_payload_datasets")}
        additions = {
            "last_validated_at": "TIMESTAMP WITH TIME ZONE" if engine.dialect.name == "postgresql" else "DATETIME",
            "last_validation_state": "VARCHAR(24)",
            "last_validation_error": "TEXT",
        }
        with engine.begin() as connection:
            for name, definition in additions.items():
                if name not in columns:
                    connection.execute(text(
                        f"ALTER TABLE sim_payload_datasets ADD COLUMN {name} {definition}"
                    ))
        with Session(engine) as session:
            session.add(SimulationSchemaRevision(
                version=7,
                description="Durable Fujin payload snapshot validation evidence",
            ))
            session.commit()
        revision = 7
    if revision < 8:
        FujinPayloadValidationJob.__table__.create(engine, checkfirst=True)
        with Session(engine) as session:
            session.add(SimulationSchemaRevision(
                version=8,
                description="Durable Fujin payload checksum validation jobs",
            ))
            session.commit()
        revision = 8
    if revision < 9:
        columns = {item["name"] for item in inspect(engine).get_columns("sim_virtual_objects")}
        timestamp_type = "TIMESTAMP WITH TIME ZONE" if engine.dialect.name == "postgresql" else "DATETIME"
        additions = {
            "restore_completed_at": timestamp_type,
            "restore_expiry_basis_at": timestamp_type,
            "restore_retention_days": "INTEGER",
            "restore_tier": "VARCHAR(16)",
        }
        with engine.begin() as connection:
            for name, definition in additions.items():
                if name not in columns:
                    connection.execute(text(
                        f"ALTER TABLE sim_virtual_objects ADD COLUMN {name} {definition}"
                    ))
            connection.execute(text(
                "UPDATE sim_virtual_objects "
                "SET restore_expiry_basis_at = restore_available_at "
                "WHERE restore_expiry_basis_at IS NULL AND restore_available_at IS NOT NULL"
            ))
        # Prior revisions stored the old exact available+days timestamp but
        # not the request parameters. Recover the integer retention before a
        # later repair applies UTC-midnight semantics.
        with Session(engine) as session:
            for item in session.query(VirtualObject).filter(
                VirtualObject.restore_available_at.is_not(None)
            ):
                if not item.restore_retention_days and item.restore_expires_at:
                    available = item.restore_available_at
                    expires = item.restore_expires_at
                    if available.tzinfo is None:
                        available = available.replace(tzinfo=timezone.utc)
                    if expires.tzinfo is None:
                        expires = expires.replace(tzinfo=timezone.utc)
                    item.restore_retention_days = max(
                        1, int(round((expires - available).total_seconds() / 86400))
                    )
                item.restore_tier = item.restore_tier or "BULK"
                if item.restore_state == "AVAILABLE":
                    item.restore_completed_at = item.restore_completed_at or item.restore_available_at
                if item.restore_expiry_basis_at and item.restore_retention_days:
                    item.restore_expires_at = s3_restore_expiry(
                        item.restore_expiry_basis_at, item.restore_retention_days
                    )
            session.commit()
        with Session(engine) as session:
            session.add(SimulationSchemaRevision(
                version=9,
                description="AWS UTC-midnight restore expiry evidence",
            ))
            session.commit()
        revision = 9
    return SIMULATION_SCHEMA_VERSION
