from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session

from app.fujin_local_schema import LocalRestore, migrate, repair_restore_expiries


def test_local_catalogue_is_separate_from_simulation_and_raijin(tmp_path):
    url = f"sqlite+pysqlite:///{Path(tmp_path) / 'fujin-local.db'}"
    migrate(url)
    names = set(inspect(create_engine(url)).get_table_names())
    assert {"local_datasets", "local_dataset_generation_jobs", "local_s3_buckets", "local_oci_buckets", "local_audit_events", "local_cloud_provider_states"} <= names
    assert "local_governance_templates" not in names
    assert not any(name.startswith("sim_") for name in names)
    assert "sources" not in names
    bucket_columns = {column["name"] for column in inspect(create_engine(url)).get_columns("local_s3_buckets")}
    assert "storage_class" in bucket_columns
    assert "logical_multiplier" in bucket_columns
    assert "governance_template_snapshot_json" not in bucket_columns
    oci_columns = {column["name"] for column in inspect(create_engine(url)).get_columns("local_oci_buckets")}
    assert "governance_template_snapshot_json" not in oci_columns


def test_schema_upgrade_removes_legacy_governance_template_metadata(tmp_path):
    url = f"sqlite+pysqlite:///{Path(tmp_path) / 'legacy-template.db'}"
    migrate(url)
    engine = create_engine(url)
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "ALTER TABLE local_s3_buckets ADD COLUMN governance_template_snapshot_json TEXT"
        )
        connection.exec_driver_sql(
            "ALTER TABLE local_oci_buckets ADD COLUMN governance_template_snapshot_json TEXT"
        )
        connection.exec_driver_sql(
            "CREATE TABLE local_governance_templates (id VARCHAR(36) PRIMARY KEY, name VARCHAR(255))"
        )
    migrate(url)
    inspector = inspect(create_engine(url))
    assert "local_governance_templates" not in inspector.get_table_names()
    assert "governance_template_snapshot_json" not in {
        column["name"] for column in inspector.get_columns("local_s3_buckets")
    }
    assert "governance_template_snapshot_json" not in {
        column["name"] for column in inspector.get_columns("local_oci_buckets")
    }


def test_schema_upgrade_clears_false_transferred_bytes_from_head_audits(tmp_path):
    url = f"sqlite+pysqlite:///{Path(tmp_path) / 'head-audit.db'}"
    migrate(url)
    engine = create_engine(url)
    with engine.begin() as connection:
        connection.execute(text(
            "INSERT INTO local_audit_events "
            "(request_id, operation, status_code, detail, bytes_transferred, created_at) "
            "VALUES ('head', 'S3_HEAD_OBJECT', 200, '', 1000, CURRENT_TIMESTAMP), "
            "('get', 'S3_GET_OBJECT', 200, '', 1000, CURRENT_TIMESTAMP)"
        ))

    migrate(url)

    with engine.connect() as connection:
        values = dict(connection.execute(text(
            "SELECT operation, bytes_transferred FROM local_audit_events ORDER BY operation"
        )).all())
    assert values == {"S3_GET_OBJECT": 1000, "S3_HEAD_OBJECT": None}


def test_restore_expiry_repair_has_dry_run_and_reopens_false_expiry(tmp_path):
    url = f"sqlite+pysqlite:///{Path(tmp_path) / 'restore-repair.db'}"
    migrate(url)
    engine = create_engine(url)
    available = datetime(2026, 9, 12, 16, 55, 26, tzinfo=timezone.utc)
    with Session(engine) as session:
        session.add(LocalRestore(
            bucket_id="bucket", object_key="object", state="EXPIRED",
            requested_at=available, available_at=available, completed_at=available,
            expiry_basis_at=available,
            expires_at=datetime(2026, 9, 15, 16, 55, 26, tzinfo=timezone.utc),
            retention_days=3, restore_tier="BULK",
        ))
        session.commit()

    preview = repair_restore_expiries(
        url, dry_run=True, now=datetime(2026, 9, 15, 19, 0, tzinfo=timezone.utc)
    )
    assert preview["changed_expiries"] == 1 and preview["reopened"] == 1
    with Session(engine) as session:
        assert session.query(LocalRestore).one().state == "EXPIRED"

    applied = repair_restore_expiries(
        url, dry_run=False, now=datetime(2026, 9, 15, 19, 0, tzinfo=timezone.utc)
    )
    assert applied["dry_run"] is False and applied["reopened"] == 1
    with Session(engine) as session:
        restored = session.query(LocalRestore).one()
        assert restored.state == "AVAILABLE"
        assert restored.expires_at.replace(tzinfo=timezone.utc) == datetime(
            2026, 9, 16, 0, 0, tzinfo=timezone.utc
        )

    repeated = repair_restore_expiries(
        url, dry_run=False, now=datetime(2026, 9, 15, 19, 0, tzinfo=timezone.utc)
    )
    assert repeated["changed_expiries"] == 0
    assert repeated["reopened"] == 0
    with Session(engine) as session:
        restored = session.query(LocalRestore).one()
        assert restored.state == "AVAILABLE"
        assert restored.expires_at.replace(tzinfo=timezone.utc) == datetime(
            2026, 9, 16, 0, 0, tzinfo=timezone.utc
        )
