"""Durable physical-dataset materializer owned by Fujin LOCAL.

This worker is deliberately separate from the S3/OCI provider process.  It is
the only LOCAL component with a writable payload-volume mount; the provider
serves the published snapshot through its read-only mount.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import time

from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session

from app.fujin_local_schema import (
    LocalDataset,
    LocalDatasetGenerationJob,
    migrate,
)


DEFAULT_CHUNK_BYTES = 1024 * 1024


class LocalMaterializationError(RuntimeError):
    pass


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def payload_root(environ: dict[str, str] | None = None) -> Path:
    values = os.environ if environ is None else environ
    root = Path(values.get("FUJIN_LOCAL_PAYLOAD_ROOT", "/var/lib/fujin-local/payloads"))
    if not root.is_absolute():
        raise LocalMaterializationError("Fujin LOCAL payload root must be absolute")
    root.mkdir(parents=True, exist_ok=True)
    if root.is_symlink():
        raise LocalMaterializationError("Fujin LOCAL payload root must not be a symlink")
    return root.resolve(strict=True)


def normalized_profile(profile: dict) -> dict:
    if not isinstance(profile, dict):
        raise LocalMaterializationError("dataset profile must be an object")
    raw_files = profile.get("files")
    if raw_files is None:
        count = int(profile.get("file_count", 0))
        size = int(profile.get("file_size_bytes", 0))
        raw_files = [{"name": f"payload-{index + 1:08d}.bin", "size_bytes": size}
                     for index in range(count)]
    if not isinstance(raw_files, list) or not raw_files or len(raw_files) > 100_000:
        raise LocalMaterializationError("dataset profile needs between 1 and 100000 files")
    files: list[dict] = []
    for index, item in enumerate(raw_files):
        if not isinstance(item, dict):
            raise LocalMaterializationError("dataset file profile must be an object")
        name = str(item.get("name", f"payload-{index + 1:08d}.bin")).strip()
        size = int(item.get("size_bytes", 0))
        if not name or "/" in name or "\\" in name or name in {".", ".."}:
            raise LocalMaterializationError("dataset file name must be a plain relative name")
        if not 1 <= size <= 100 * 1024**3:
            raise LocalMaterializationError("dataset file size must be between 1 byte and 100 GiB")
        files.append({"name": name, "size_bytes": size})
    return {"format_version": 1, "files": files}


def generated_bytes(seed: str, relative_path: str, size: int, *, chunk_bytes: int = DEFAULT_CHUNK_BYTES):
    """Generate deterministic high-entropy bytes without retaining them in RAM."""
    produced = 0
    counter = 0
    prefix = f"{seed}:{relative_path}:".encode("utf-8")
    while produced < size:
        needed = min(chunk_bytes, size - produced)
        output = bytearray()
        while len(output) < needed:
            output.extend(hashlib.sha256(prefix + counter.to_bytes(16, "big")).digest())
            counter += 1
        yield bytes(output[:needed])
        produced += needed


def relative_path(dataset_id: str, index: int, name: str) -> str:
    digest = hashlib.sha256(f"{dataset_id}:{index}".encode("utf-8")).hexdigest()
    return f"datasets/{dataset_id}/{digest[:2]}/{digest[2:4]}/{index + 1:08d}-{name}"


def _write_one(root: Path, dataset_id: str, seed: str, index: int, spec: dict) -> dict:
    relative = relative_path(dataset_id, index, spec["name"])
    target = root / relative
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if target.parent.is_symlink():
        raise LocalMaterializationError("managed dataset directory cannot be a symlink")
    temporary = target.with_name(f".{target.name}.partial")
    digest = hashlib.sha256()
    try:
        with temporary.open("wb") as handle:
            for chunk in generated_bytes(seed, relative, int(spec["size_bytes"])):
                digest.update(chunk)
                handle.write(chunk)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        stat = target.stat()
        if stat.st_size != int(spec["size_bytes"]):
            raise LocalMaterializationError("generated dataset file has an unexpected size")
        return {
            "key": f"payload/{index + 1:08d}-{spec['name']}",
            "relative_path": relative,
            "size_bytes": stat.st_size,
            "sha256": digest.hexdigest(),
            "mtime_ns": stat.st_mtime_ns,
            "identity": f"{stat.st_dev}:{stat.st_ino}",
        }
    finally:
        if temporary.exists():
            temporary.unlink(missing_ok=True)


def _remove_owned_dataset(root: Path, dataset: LocalDataset) -> None:
    """Remove only a published, Fujin-owned dataset from the writable mount."""
    expected_relative = Path("datasets") / dataset.id
    if dataset.repository_relative_path != expected_relative.as_posix():
        raise LocalMaterializationError("cleanup is allowed only for Fujin-owned dataset paths")
    directory = root / expected_relative
    tombstone = root / "datasets" / f".deleting-{dataset.id}"
    if not directory.exists():
        return
    if directory.is_symlink() or tombstone.exists():
        raise LocalMaterializationError("generated dataset directory is not safe to remove")
    directory.resolve(strict=True).relative_to(root)
    directory.rename(tombstone)
    shutil.rmtree(tombstone, ignore_errors=False)


def generation_engine(database_url: str):
    """Use the same SQLite writer contract as the Fujin LOCAL API."""
    sqlite_database = database_url.startswith("sqlite")
    busy_timeout_seconds = max(1, int(os.environ.get("FUJIN_LOCAL_SQLITE_BUSY_TIMEOUT_SECONDS", "30")))
    engine = create_engine(
        database_url,
        pool_pre_ping=True,
        connect_args={"timeout": busy_timeout_seconds} if sqlite_database else {},
    )
    if sqlite_database:
        @event.listens_for(engine, "connect")
        def configure_sqlite_connection(dbapi_connection, _connection_record) -> None:
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute(f"PRAGMA busy_timeout={busy_timeout_seconds * 1000}")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.close()
    return engine


def process_generation_jobs(database_url: str, *, root: Path | None = None, max_files: int = 1) -> dict:
    """Advance at most ``max_files`` durable generation steps."""
    root = root or payload_root()
    engine = generation_engine(database_url)
    completed = failed = written = 0
    for _ in range(max(0, int(max_files))):
        with Session(engine) as session:
            cleanup = session.scalar(select(LocalDatasetGenerationJob).where(
                LocalDatasetGenerationJob.state == "CLEANUP_PENDING"
            ).order_by(LocalDatasetGenerationJob.updated_at).limit(1))
            if cleanup is not None:
                dataset = session.get(LocalDataset, cleanup.dataset_id)
                try:
                    if dataset is None:
                        raise LocalMaterializationError("dataset cleanup state disappeared")
                    _remove_owned_dataset(root, dataset)
                    cleanup.state, cleanup.last_error, cleanup.updated_at = "CLEANUP_SUCCEEDED", None, utcnow()
                    session.commit(); completed += 1
                except Exception as error:
                    cleanup.state, cleanup.last_error, cleanup.updated_at = "CLEANUP_FAILED", str(error), utcnow()
                    session.commit(); failed += 1
                continue
            job = session.scalar(select(LocalDatasetGenerationJob).where(
                LocalDatasetGenerationJob.state.in_(("READY", "RUNNING"))
            ).order_by(LocalDatasetGenerationJob.created_at).limit(1))
            if job is None:
                break
            dataset = session.get(LocalDataset, job.dataset_id)
            if dataset is None:
                job.state, job.last_error, job.updated_at = "FAILED", "dataset is missing", utcnow()
                session.commit(); failed += 1; continue
            profile = json.loads(job.profile_json)
            files = profile["files"]
            index = int(job.next_file_index)
            if index >= len(files):
                manifest = json.loads(dataset.manifest_json or "{}")
                objects = manifest.get("objects", [])
                canonical = json.dumps(objects, sort_keys=True, separators=(",", ":")).encode("utf-8")
                dataset.snapshot_id = hashlib.sha256(canonical).hexdigest()
                dataset.state = "READY"
                dataset.validated_objects = len(objects)
                dataset.validated_bytes = sum(int(item["size_bytes"]) for item in objects)
                dataset.last_validated_at = utcnow()
                dataset.last_validation_error = None
                job.state, job.updated_at = "SUCCEEDED", utcnow()
                session.commit(); completed += 1; continue
            job.state, job.updated_at = "RUNNING", utcnow()
            session.commit()
            dataset_id, seed, job_id = dataset.id, job.seed, job.id
            spec = files[index]
        try:
            entry = _write_one(root, dataset_id, seed, index, spec)
            with Session(engine) as session:
                dataset = session.get(LocalDataset, dataset_id)
                job = session.get(LocalDatasetGenerationJob, job_id)
                if dataset is None or job is None:
                    raise LocalMaterializationError("dataset generation state disappeared")
                manifest = json.loads(dataset.manifest_json or "{}")
                objects = list(manifest.get("objects", []))
                if len(objects) == index:
                    objects.append(entry)
                elif len(objects) > index:
                    entry = objects[index]
                else:
                    raise LocalMaterializationError("dataset generation progress is not contiguous")
                dataset.manifest_json = json.dumps({"format_version": 1, "objects": objects}, sort_keys=True)
                job.next_file_index = index + 1
                job.bytes_written += int(entry["size_bytes"])
                job.updated_at = utcnow()
                session.commit(); written += 1
        except Exception as error:
            with Session(engine) as session:
                job = session.get(LocalDatasetGenerationJob, job_id)
                dataset = session.get(LocalDataset, dataset_id)
                if job:
                    job.state, job.last_error, job.updated_at = "FAILED", str(error), utcnow()
                if dataset:
                    dataset.state, dataset.last_validation_error = "INVALID", str(error)
                session.commit()
            failed += 1
    engine.dispose()
    return {"completed_steps": completed, "written_files": written, "failed_steps": failed}


def run_forever(database_url: str, interval_seconds: float = 0.25) -> None:
    migrate(database_url)
    while True:
        process_generation_jobs(database_url, max_files=1)
        time.sleep(interval_seconds)


if __name__ == "__main__":
    run_forever(os.environ["FUJIN_LOCAL_DATABASE_URL"])
