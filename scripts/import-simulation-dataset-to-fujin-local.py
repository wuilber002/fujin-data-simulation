#!/usr/bin/env python3
"""Import validated Simulation payloads into the isolated LOCAL catalogue.

Only catalogue metadata is copied.  The immutable physical files remain owned
by Fujin's payload repository and are never duplicated.  S3 storage class is
intentionally absent from the imported manifest; each LOCAL bucket owns that
choice when it projects the dataset through the S3 API.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import uuid

# Keep the operational script directly executable as documented, independent
# of whether the caller preconfigured PYTHONPATH.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.fujin_local_schema import LocalAuditEvent, LocalDataset, migrate
from app.simulation_schema import FujinPayloadDataset, FujinPayloadFile
from app.simulator_store import simulator_database_url


def canonical_manifest_hash(files: list[FujinPayloadFile]) -> str:
    source_manifest = [
        {"path": item.relative_path, "size_bytes": item.size_bytes, "sha256": item.sha256}
        for item in files
    ]
    return hashlib.sha256(
        json.dumps(source_manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def import_dataset(source_engine, local_engine, payload_root: Path, source_id: str, name: str) -> LocalDataset:
    root = payload_root.resolve(strict=True)
    with Session(source_engine) as source_session:
        source = source_session.get(FujinPayloadDataset, source_id)
        if source is None:
            raise ValueError("Simulation dataset not found")
        if source.state != "READY" or source.last_validation_state != "VALID":
            raise ValueError("Simulation dataset must be READY and VALID before import")
        files = list(source_session.scalars(select(FujinPayloadFile).where(
            FujinPayloadFile.dataset_id == source.id
        ).order_by(FujinPayloadFile.relative_path)))
        if not files or len(files) != source.files_total:
            raise ValueError("Simulation file catalogue is incomplete")
        if sum(item.size_bytes for item in files) != source.physical_bytes:
            raise ValueError("Simulation physical byte total does not match its file catalogue")
        digest = canonical_manifest_hash(files)
        if not source.manifest_sha256 or digest != source.manifest_sha256:
            raise ValueError("Simulation manifest SHA-256 is inconsistent")

        prefix = Path(source.repository_relative_path)
        if prefix.is_absolute() or ".." in prefix.parts or not prefix.parts:
            raise ValueError("Simulation repository path is unsafe")
        objects = []
        for item in files:
            relative = Path(item.relative_path)
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError("Simulation payload path is unsafe")
            relative.relative_to(prefix)
            candidate = (root / relative).resolve(strict=True)
            candidate.relative_to(root)
            stat = candidate.stat()
            if not candidate.is_file() or stat.st_size != item.size_bytes:
                raise ValueError(f"Physical payload metadata changed: {relative}")
            if stat.st_mtime_ns != item.mtime_ns or f"{stat.st_dev}:{stat.st_ino}" != item.identity:
                raise ValueError(f"Physical payload identity changed after validation: {relative}")
            objects.append({
                "key": relative.relative_to(prefix).as_posix(),
                "relative_path": relative.as_posix(),
                "size_bytes": item.size_bytes,
                "sha256": item.sha256,
                "etag": item.sha256,
            })
        source_snapshot = {
            "id": source.id,
            "name": source.name,
            "model": source.model,
            "manifest_sha256": source.manifest_sha256,
            "last_validated_at": source.last_validated_at.isoformat() if source.last_validated_at else None,
        }

    migrate(str(local_engine.url))
    with Session(local_engine) as local_session:
        existing = local_session.scalar(select(LocalDataset).where(LocalDataset.name == name))
        if existing:
            manifest = json.loads(existing.manifest_json or "{}")
            if manifest.get("provenance", {}).get("simulation_dataset_id") == source_id:
                return existing
            raise ValueError("A different LOCAL dataset already uses the requested name")
        path_owner = local_session.scalar(select(LocalDataset).where(
            LocalDataset.repository_relative_path == source.repository_relative_path,
            LocalDataset.deleted_at.is_(None),
        ))
        if path_owner:
            raise ValueError(f"Payload path is already registered by LOCAL dataset {path_owner.name}")
        now = datetime.now(timezone.utc)
        imported = LocalDataset(
            name=name,
            state="READY",
            model="REPRESENTATIVE",
            snapshot_id=source.manifest_sha256,
            repository_relative_path=source.repository_relative_path,
            manifest_json=json.dumps({
                "format_version": 1,
                "provenance": {"simulation_dataset_id": source.id, **source_snapshot},
                "objects": objects,
            }, sort_keys=True, separators=(",", ":")),
            quota_bytes=source.quota_bytes,
            validated_objects=len(objects),
            validated_bytes=sum(item["size_bytes"] for item in objects),
            last_validated_at=source.last_validated_at or now,
            created_at=now,
        )
        local_session.add(imported)
        local_session.add(LocalAuditEvent(
            request_id=f"fujin-{uuid.uuid4().hex}",
            operation="LOCAL_DATASET_IMPORTED_FROM_SIMULATION",
            status_code=201,
            detail=f"{name}; source={source_id}; objects={len(objects)}; bytes={imported.validated_bytes}",
        ))
        local_session.commit()
        local_session.refresh(imported)
        return imported


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--name", required=True)
    args = parser.parse_args()
    local_url = os.environ.get("FUJIN_LOCAL_DATABASE_URL", "").strip()
    payload_root = os.environ.get("FUJIN_LOCAL_PAYLOAD_ROOT", "").strip()
    if not local_url or not payload_root:
        raise SystemExit("FUJIN_LOCAL_DATABASE_URL and FUJIN_LOCAL_PAYLOAD_ROOT are required")
    imported = import_dataset(
        create_engine(simulator_database_url(), pool_pre_ping=True),
        create_engine(local_url, pool_pre_ping=True),
        Path(payload_root), args.source_id, args.name,
    )
    print(json.dumps({
        "id": imported.id,
        "name": imported.name,
        "state": imported.state,
        "model": imported.model,
        "objects": imported.validated_objects,
        "bytes": imported.validated_bytes,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
