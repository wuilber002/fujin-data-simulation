"""Managed physical payload repository for Fujin DATA simulations.

The repository never accepts a host path from an API caller.  It lives below
one configured root, creates high-entropy deterministic files itself and keeps
the durable catalog/manifest in the simulator database.  It is deliberately a
*source* reader only: simulated destination writes still consume and discard
payload while persisting their existing integrity evidence.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import threading
import time
from typing import Iterator

from sqlalchemy import func, select

from app.simulated_data import VirtualContentDescriptor, iter_deterministic_range
from app.simulation_schema import (
    FujinPayloadDataset,
    FujinPayloadFile,
    FujinPayloadGenerationJob,
    FujinPayloadValidationJob,
)


DEFAULT_PAYLOAD_ROOT = "/var/lib/raijin-fujin-payloads"
DEFAULT_CHUNK_BYTES = 1024 * 1024
DEFAULT_MAX_OPEN_FILES = 16


class FujinPayloadError(RuntimeError):
    pass


class PayloadSnapshotError(FujinPayloadError):
    pass


@dataclass(frozen=True)
class PayloadReference:
    dataset_id: str
    relative_path: str
    size_bytes: int
    mtime_ns: int
    identity: str
    sha256: str


@dataclass(frozen=True)
class PayloadReadLimits:
    """Frozen per-scenario bounds for local source reads.

    The limits are intentionally reader-only: network simulation continues to
    be governed by the existing Fujin network profile and no host path or
    cache-control operation is exposed to an operator.
    """

    chunk_bytes: int = DEFAULT_CHUNK_BYTES
    max_open_files: int = DEFAULT_MAX_OPEN_FILES
    max_read_mbps: float = 0.0


_reader_semaphore_lock = threading.Lock()
_reader_semaphores: dict[int, threading.BoundedSemaphore] = {}


def payload_read_limits(configuration: dict | None) -> PayloadReadLimits:
    raw = (configuration or {}).get("payload_resource_limits", {})
    if not isinstance(raw, dict):
        raise FujinPayloadError("payload_resource_limits must be an object")
    chunk_bytes = int(raw.get("chunk_bytes", DEFAULT_CHUNK_BYTES))
    max_open_files = int(raw.get("max_open_files", DEFAULT_MAX_OPEN_FILES))
    max_read_mbps = float(raw.get("max_read_mbps", 0.0))
    if not 4096 <= chunk_bytes <= 64 * 1024 * 1024:
        raise FujinPayloadError("payload reader chunk_bytes must be between 4 KiB and 64 MiB")
    if not 1 <= max_open_files <= 1024:
        raise FujinPayloadError("payload reader max_open_files must be between 1 and 1024")
    if not 0 <= max_read_mbps <= 100_000:
        raise FujinPayloadError("payload reader max_read_mbps must be between 0 and 100000")
    return PayloadReadLimits(chunk_bytes, max_open_files, max_read_mbps)


def normalized_payload_configuration(configuration: dict | None) -> dict:
    """Freeze the physical-reader contract into an immutable scenario config."""
    result = dict(configuration or {})
    cache_mode = str(result.get("payload_cache_mode", "COLD")).strip().upper()
    if cache_mode not in {"COLD", "WARM"}:
        raise FujinPayloadError("payload_cache_mode must be COLD or WARM")
    limits = payload_read_limits(result)
    result["payload_cache_mode"] = cache_mode
    result["payload_resource_limits"] = {
        "chunk_bytes": limits.chunk_bytes,
        "max_open_files": limits.max_open_files,
        "max_read_mbps": limits.max_read_mbps,
    }
    return result


def _reader_semaphore(limit: int) -> threading.BoundedSemaphore:
    with _reader_semaphore_lock:
        semaphore = _reader_semaphores.get(limit)
        if semaphore is None:
            semaphore = threading.BoundedSemaphore(limit)
            _reader_semaphores[limit] = semaphore
        return semaphore


def host_telemetry(root: Path) -> dict:
    """Return non-sensitive host/storage evidence for a physical run."""
    stat = os.statvfs(root)
    cache_bytes = 0
    try:
        values = {}
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            key, value = line.split(":", 1)
            values[key] = int(value.strip().split()[0]) * 1024
        cache_bytes = int(values.get("Cached", 0)) + int(values.get("Buffers", 0))
    except (OSError, ValueError, IndexError):
        pass
    try:
        load_1m = float(os.getloadavg()[0])
    except OSError:
        load_1m = 0.0
    total = int(stat.f_blocks * stat.f_frsize)
    available = int(stat.f_bavail * stat.f_frsize)
    return {
        "volume_total_bytes": total,
        "volume_available_bytes": available,
        "volume_used_bytes": max(0, total - available),
        "host_page_cache_bytes": cache_bytes,
        "host_load_1m": load_1m,
    }


def repository_root(environ: dict[str, str] | None = None) -> Path:
    values = os.environ if environ is None else environ
    configured = values.get("RAIJIN_FUJIN_PAYLOAD_ROOT", DEFAULT_PAYLOAD_ROOT)
    root = Path(configured).expanduser()
    if not root.is_absolute():
        raise FujinPayloadError("Fujin payload root must be an absolute path")
    root.mkdir(parents=True, exist_ok=True)
    if root.is_symlink():
        raise FujinPayloadError("Fujin payload root must not be a symlink")
    return root.resolve(strict=True)


def _identity(stat: os.stat_result) -> str:
    return f"{stat.st_dev}:{stat.st_ino}"


def _safe_relative(value: str) -> Path:
    path = Path(value)
    if not value or path.is_absolute() or "\x00" in value or any(part in {"", ".", ".."} for part in path.parts):
        raise FujinPayloadError("Invalid managed payload relative path")
    return path


def _resolved_file(root: Path, relative_path: str) -> Path:
    relative = _safe_relative(relative_path)
    candidate = root.joinpath(relative)
    # Resolve parent only after checking every segment, preventing a dataset
    # from escaping through a manually inserted symlink.
    current = root
    for part in relative.parts:
        current = current / part
        # ``is_symlink`` also detects a dangling link.  Requiring ``exists``
        # here would let that case reach ``resolve`` and produce an ambiguous
        # filesystem error instead of the explicit snapshot violation.
        if current.is_symlink():
            raise PayloadSnapshotError("Managed payload path contains a symlink")
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as error:
        raise PayloadSnapshotError("Managed payload is missing or cannot be resolved") from error
    try:
        resolved.relative_to(root)
    except ValueError as error:
        raise PayloadSnapshotError("Managed payload escaped Fujin repository") from error
    if not resolved.is_file():
        raise PayloadSnapshotError("Managed payload is not a regular file")
    return resolved


def _file_specs(profile: dict) -> list[dict]:
    """Normalize a deliberately small, versionable generation profile.

    A profile either supplies ``files`` (sizes in bytes) or a compact
    ``file_count``/``file_size_bytes`` pair.  The latter keeps API payloads
    manageable for large datasets while remaining deterministic.
    """
    raw_files = profile.get("files")
    if raw_files is None:
        count = int(profile.get("file_count", 0))
        size = int(profile.get("file_size_bytes", 0))
        raw_files = [{"size_bytes": size} for _ in range(count)]
    if not isinstance(raw_files, list) or not raw_files:
        raise FujinPayloadError("Payload profile requires at least one file")
    result: list[dict] = []
    for index, item in enumerate(raw_files):
        if not isinstance(item, dict):
            raise FujinPayloadError("Payload profile file entries must be objects")
        size = int(item.get("size_bytes", 0))
        if size < 0:
            raise FujinPayloadError("Payload file size cannot be negative")
        name = str(item.get("name") or f"payload-{index + 1:08d}.bin")
        if "/" in name or "\\" in name or name in {".", ".."}:
            raise FujinPayloadError("Payload file names must be simple names")
        result.append({"name": name, "size_bytes": size})
    return result


def normalized_profile(profile: dict) -> dict:
    if not isinstance(profile, dict):
        raise FujinPayloadError("Payload profile must be an object")
    specs = _file_specs(profile)
    return {
        "version": 1,
        "content_profile": str(profile.get("content_profile", "HIGH_ENTROPY")).upper(),
        "files": specs,
        "chunk_bytes": int(profile.get("chunk_bytes", DEFAULT_CHUNK_BYTES)),
    }


class LocalFilesystemPayloadReader:
    """Read a verified range from one Fujin-managed physical source file."""

    def __init__(self, root: Path, reference: PayloadReference, limits: PayloadReadLimits | None = None):
        self.root = root
        self.reference = reference
        self.limits = limits or PayloadReadLimits()
        self.chunk_bytes = self.limits.chunk_bytes

    def _validated_path(self) -> Path:
        path = _resolved_file(self.root, self.reference.relative_path)
        stat = path.stat()
        if (
            stat.st_size != self.reference.size_bytes
            or stat.st_mtime_ns != self.reference.mtime_ns
            or _identity(stat) != self.reference.identity
        ):
            raise PayloadSnapshotError("Managed payload no longer matches its immutable snapshot")
        return path

    def stream_range(self, offset: int, length: int) -> Iterator[bytes]:
        if offset < 0 or length < 0 or offset + length > self.reference.size_bytes:
            raise ValueError("Range is outside the managed payload")
        path = self._validated_path()
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        semaphore = _reader_semaphore(self.limits.max_open_files)
        semaphore.acquire()
        try:
            descriptor = os.open(path, flags)
        except OSError as error:
            semaphore.release()
            raise PayloadSnapshotError("Managed payload cannot be opened safely") from error
        started = time.monotonic()
        emitted = 0
        try:
            with os.fdopen(descriptor, "rb", closefd=True) as handle:
                handle.seek(offset)
                remaining = length
                while remaining:
                    chunk = handle.read(min(self.chunk_bytes, remaining))
                    if not chunk:
                        raise PayloadSnapshotError("Managed payload ended before requested range")
                    remaining -= len(chunk)
                    emitted += len(chunk)
                    if self.limits.max_read_mbps > 0:
                        minimum_elapsed = emitted * 8 / (self.limits.max_read_mbps * 1_000_000)
                        delay = minimum_elapsed - (time.monotonic() - started)
                        if delay > 0:
                            time.sleep(delay)
                    yield chunk
        except Exception:
            # fdopen owns the descriptor after successful construction.
            raise
        finally:
            semaphore.release()


class DeterministicPayloadReader:
    """Compatibility reader for the original logical-only simulator data."""

    def __init__(self, descriptor: VirtualContentDescriptor):
        self.descriptor = descriptor

    def stream_range(self, offset: int, length: int) -> Iterator[bytes]:
        return iter_deterministic_range(self.descriptor, offset=offset, length=length)


class FujinPayloadRepository:
    """Physical-file lifecycle; catalog changes stay durable through ``store``."""

    def __init__(self, store, root: Path | None = None):
        self.store = store
        self.root = root or repository_root()

    def create_dataset(self, name: str, quota_bytes: int, seed: str, profile: dict, model: str = "REPRESENTATIVE") -> FujinPayloadDataset:
        normalized_name = name.strip()
        normalized_model = model.strip().upper()
        if not normalized_name:
            raise FujinPayloadError("Payload dataset name is required")
        if normalized_model not in {"REPRESENTATIVE", "HYBRID"}:
            raise FujinPayloadError("Physical dataset model must be REPRESENTATIVE or HYBRID")
        if quota_bytes <= 0:
            raise FujinPayloadError("Payload dataset quota must be positive")
        if not seed:
            raise FujinPayloadError("Payload dataset seed is required")
        normalized = normalized_profile(profile)
        total = sum(int(item["size_bytes"]) for item in normalized["files"])
        if total > quota_bytes:
            raise FujinPayloadError("Payload profile exceeds the dataset quota")
        with self.store.sessions() as session:
            if session.scalar(select(FujinPayloadDataset.id).where(FujinPayloadDataset.name == normalized_name)):
                raise FujinPayloadError("Payload dataset name already exists")
            dataset = FujinPayloadDataset(
                name=normalized_name,
                state="GENERATING",
                model=normalized_model,
                repository_relative_path="pending",
                quota_bytes=quota_bytes,
                seed=seed,
                profile_json=json.dumps(normalized, sort_keys=True, separators=(",", ":")),
            )
            session.add(dataset)
            session.flush()
            dataset.repository_relative_path = f"datasets/{dataset.id}"
            session.add(FujinPayloadGenerationJob(
                dataset_id=dataset.id,
                state="READY",
                files_total=len(normalized["files"]),
            ))
            session.commit()
            return dataset

    def _dataset_directory(self, dataset: FujinPayloadDataset) -> Path:
        path = self.root / _safe_relative(dataset.repository_relative_path)
        path.mkdir(parents=True, exist_ok=True)
        if path.is_symlink():
            raise FujinPayloadError("Managed dataset directory cannot be a symlink")
        return path

    @staticmethod
    def _relative_file_path(dataset_id: str, index: int, name: str) -> str:
        digest = hashlib.sha256(f"{dataset_id}:{index}".encode("utf-8")).hexdigest()
        return f"datasets/{dataset_id}/{digest[:2]}/{digest[2:4]}/{index + 1:08d}-{name}"

    def _write_file(self, dataset: FujinPayloadDataset, index: int, spec: dict) -> FujinPayloadFile:
        relative_path = self._relative_file_path(dataset.id, index, spec["name"])
        target = self.root / _safe_relative(relative_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.parent.is_symlink():
            raise FujinPayloadError("Managed payload parent cannot be a symlink")
        descriptor = VirtualContentDescriptor(
            scenario_seed=dataset.seed,
            object_id=f"dataset:{dataset.id}:{index}",
            object_key=relative_path,
            object_version="v1",
            size_bytes=int(spec["size_bytes"]),
        )
        temporary = target.with_name(f".{target.name}.partial")
        digest = hashlib.sha256()
        with open(temporary, "wb") as handle:
            for chunk in iter_deterministic_range(descriptor, chunk_size=DEFAULT_CHUNK_BYTES):
                digest.update(chunk)
                handle.write(chunk)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        stat = target.stat()
        if stat.st_size != descriptor.size_bytes:
            raise FujinPayloadError("Generated payload size does not match profile")
        return FujinPayloadFile(
            dataset_id=dataset.id,
            relative_path=relative_path,
            size_bytes=stat.st_size,
            sha256=digest.hexdigest(),
            mtime_ns=stat.st_mtime_ns,
            identity=_identity(stat),
        )

    def process_generation_jobs(self, max_files: int = 1) -> dict:
        """Advance at most ``max_files`` durable steps; safe after restart."""
        completed = failed = 0
        for _ in range(max(0, int(max_files))):
            with self.store.sessions() as session:
                job = session.scalar(select(FujinPayloadGenerationJob).where(
                    FujinPayloadGenerationJob.state.in_(["READY", "RUNNING"])
                ).order_by(FujinPayloadGenerationJob.created_at).limit(1))
                if job is None:
                    break
                dataset = session.get(FujinPayloadDataset, job.dataset_id)
                if dataset is None:
                    job.state, job.last_error = "FAILED", "Dataset is missing"
                    session.commit(); failed += 1; continue
                profile = json.loads(dataset.profile_json)
                specs = profile["files"]
                index = int(job.next_file_index)
                if index >= len(specs):
                    files = list(session.scalars(select(FujinPayloadFile).where(
                        FujinPayloadFile.dataset_id == dataset.id
                    ).order_by(FujinPayloadFile.relative_path)))
                    manifest = [{"path": item.relative_path, "size_bytes": item.size_bytes, "sha256": item.sha256} for item in files]
                    dataset.files_total = len(files)
                    dataset.physical_bytes = sum(item.size_bytes for item in files)
                    dataset.manifest_sha256 = hashlib.sha256(
                        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
                    ).hexdigest()
                    dataset.state, dataset.ready_at, job.state = "READY", datetime.now(timezone.utc), "SUCCEEDED"
                    session.commit(); completed += 1; continue
                job.state = "RUNNING"
                session.commit()
            try:
                # Use a new transaction after bytes are fsync'd so a restart
                # can safely discover/validate the generated target again.
                with self.store.sessions() as session:
                    dataset = session.get(FujinPayloadDataset, job.dataset_id)
                    job = session.get(FujinPayloadGenerationJob, job.id)
                    existing = session.scalar(select(FujinPayloadFile).where(
                        FujinPayloadFile.dataset_id == dataset.id,
                        FujinPayloadFile.relative_path == self._relative_file_path(dataset.id, index, specs[index]["name"]),
                    ))
                    record = existing or self._write_file(dataset, index, specs[index])
                    if existing is None:
                        session.add(record)
                    job.next_file_index = index + 1
                    job.bytes_written = int(job.bytes_written or 0) + int(record.size_bytes if existing is None else 0)
                    session.commit()
            except Exception as error:
                with self.store.sessions() as session:
                    failed_job = session.get(FujinPayloadGenerationJob, job.id)
                    failed_dataset = session.get(FujinPayloadDataset, job.dataset_id)
                    if failed_job:
                        failed_job.state, failed_job.last_error = "FAILED", str(error)
                    if failed_dataset:
                        failed_dataset.state, failed_dataset.last_error = "FAILED", str(error)
                    session.commit()
                failed += 1
        return {"completed_steps": completed, "failed_steps": failed}

    def dataset_reference(self, dataset_id: str, index: int) -> PayloadReference:
        with self.store.sessions() as session:
            dataset = session.get(FujinPayloadDataset, dataset_id)
            if dataset is None or dataset.state != "READY":
                raise FujinPayloadError("Fujin payload dataset is not ready")
            files = list(session.scalars(select(FujinPayloadFile).where(
                FujinPayloadFile.dataset_id == dataset_id
            ).order_by(FujinPayloadFile.relative_path)))
            if not files:
                raise FujinPayloadError("Fujin payload dataset contains no files")
            file = files[index % len(files)]
            return PayloadReference(
                dataset_id=dataset.id, relative_path=file.relative_path,
                size_bytes=file.size_bytes, mtime_ns=file.mtime_ns,
                identity=file.identity, sha256=file.sha256,
            )

    def dataset_files(self, dataset_id: str) -> tuple[dict, list[PayloadReference]]:
        """Return a ready immutable snapshot without exposing a host path."""
        with self.store.sessions() as session:
            dataset = session.get(FujinPayloadDataset, dataset_id)
            if dataset is None or dataset.state != "READY":
                raise FujinPayloadError("Fujin payload dataset is not ready")
            files = list(session.scalars(select(FujinPayloadFile).where(
                FujinPayloadFile.dataset_id == dataset_id
            ).order_by(FujinPayloadFile.relative_path)))
            if not files:
                raise FujinPayloadError("Fujin payload dataset contains no files")
            return ({
                "id": dataset.id,
                "name": dataset.name,
                "model": dataset.model,
                "manifest_sha256": dataset.manifest_sha256,
                "physical_bytes": dataset.physical_bytes,
                "files_total": dataset.files_total,
                "profile": json.loads(dataset.profile_json),
            }, [PayloadReference(
                dataset_id=dataset.id, relative_path=item.relative_path,
                size_bytes=item.size_bytes, mtime_ns=item.mtime_ns,
                identity=item.identity, sha256=item.sha256,
            ) for item in files])

    def list_datasets(self) -> list[dict]:
        with self.store.sessions() as session:
            rows = list(session.scalars(select(FujinPayloadDataset).order_by(FujinPayloadDataset.created_at.desc())))
            jobs = {
                item.dataset_id: item for item in session.scalars(select(FujinPayloadValidationJob))
            }
            return [{
                "id": item.id, "name": item.name, "state": item.state, "model": item.model,
                "quota_bytes": item.quota_bytes, "physical_bytes": item.physical_bytes,
                "files_total": item.files_total, "manifest_sha256": item.manifest_sha256,
                "last_error": item.last_error, "created_at": item.created_at, "ready_at": item.ready_at,
                "last_validated_at": item.last_validated_at,
                "last_validation_state": item.last_validation_state,
                "last_validation_error": item.last_validation_error,
                "validation_job": ({
                    "id": job.id, "state": job.state,
                    "files_validated": job.next_file_index,
                    "files_total": job.files_total,
                    "bytes_validated": job.bytes_validated,
                    "last_error": job.last_error,
                    "updated_at": job.updated_at,
                } if (job := jobs.get(item.id)) else None),
            } for item in rows]

    def enqueue_validation(self, dataset_id: str) -> dict:
        """Queue a checksum pass and return immediately.

        Re-queueing intentionally starts a new full immutable-snapshot check;
        it replaces any completed or failed cursor for the same dataset.
        """
        with self.store.sessions() as session:
            dataset = session.get(FujinPayloadDataset, dataset_id)
            if dataset is None or dataset.state != "READY":
                raise FujinPayloadError("Fujin payload dataset is not ready")
            files_total = int(session.scalar(select(func.count()).select_from(FujinPayloadFile).where(
                FujinPayloadFile.dataset_id == dataset_id
            )) or 0)
            if not files_total:
                raise FujinPayloadError("Fujin payload dataset contains no files")
            job = session.scalar(select(FujinPayloadValidationJob).where(
                FujinPayloadValidationJob.dataset_id == dataset_id
            ))
            if job is None:
                job = FujinPayloadValidationJob(dataset_id=dataset_id)
                session.add(job)
            job.state, job.next_file_index = "READY", 0
            job.files_total, job.bytes_validated, job.last_error = files_total, 0, None
            dataset.last_validation_state, dataset.last_validation_error = "QUEUED", None
            session.commit()
            return {
                "dataset_id": dataset.id, "job_id": job.id, "state": job.state,
                "files_total": job.files_total, "physical_bytes": dataset.physical_bytes,
            }

    @staticmethod
    def _validation_reference(item: FujinPayloadFile) -> PayloadReference:
        return PayloadReference(
            dataset_id=item.dataset_id, relative_path=item.relative_path,
            size_bytes=item.size_bytes, mtime_ns=item.mtime_ns,
            identity=item.identity, sha256=item.sha256,
        )

    def process_validation_jobs(self, max_files: int = 1) -> dict:
        """Advance durable validation in bounded steps outside HTTP requests."""
        completed = failed = validated = 0
        for _ in range(max(0, int(max_files))):
            with self.store.sessions() as session:
                job = session.scalar(select(FujinPayloadValidationJob).where(
                    FujinPayloadValidationJob.state.in_(["READY", "RUNNING"])
                ).order_by(FujinPayloadValidationJob.created_at).limit(1))
                if job is None:
                    break
                dataset = session.get(FujinPayloadDataset, job.dataset_id)
                if dataset is None:
                    job.state, job.last_error = "FAILED", "Dataset is missing"
                    session.commit(); failed += 1; continue
                files = list(session.scalars(select(FujinPayloadFile).where(
                    FujinPayloadFile.dataset_id == dataset.id
                ).order_by(FujinPayloadFile.relative_path)))
                index = int(job.next_file_index)
                if index >= len(files):
                    job.state = "SUCCEEDED"
                    dataset.last_validated_at = datetime.now(timezone.utc)
                    dataset.last_validation_state, dataset.last_validation_error = "VALID", None
                    session.commit(); completed += 1; continue
                item = files[index]
                reference = self._validation_reference(item)
                job.state = "RUNNING"
                dataset.last_validation_state, dataset.last_validation_error = "VALIDATING", None
                session.commit()
            try:
                digest = hashlib.sha256()
                for chunk in LocalFilesystemPayloadReader(self.root, reference).stream_range(0, reference.size_bytes):
                    digest.update(chunk)
                if digest.hexdigest() != reference.sha256:
                    raise PayloadSnapshotError("Managed payload SHA-256 differs from its immutable manifest")
                with self.store.sessions() as session:
                    current = session.get(FujinPayloadValidationJob, job.id)
                    if current is None or current.state not in {"READY", "RUNNING"}:
                        continue
                    current.next_file_index = index + 1
                    current.bytes_validated = int(current.bytes_validated or 0) + int(reference.size_bytes)
                    current.state = "RUNNING"
                    session.commit()
                validated += 1
            except Exception as error:
                with self.store.sessions() as session:
                    current = session.get(FujinPayloadValidationJob, job.id)
                    current_dataset = session.get(FujinPayloadDataset, job.dataset_id)
                    if current:
                        current.state, current.last_error = "FAILED", str(error)[:8000]
                    if current_dataset:
                        current_dataset.last_validated_at = datetime.now(timezone.utc)
                        current_dataset.last_validation_state = "INVALID"
                        current_dataset.last_validation_error = str(error)[:8000]
                    session.commit()
                failed += 1
        return {"completed_steps": completed, "failed_steps": failed, "validated_files": validated}

    def validate_dataset(self, dataset_id: str) -> dict:
        """Re-read a snapshot and persist a durable validation result.

        Generation already calculates SHA-256, so this is both the
        administrative pre-flight and the streaming fallback evidence check.
        It intentionally reads only managed relative paths.
        """
        try:
            snapshot, files = self.dataset_files(dataset_id)
            for reference in files:
                digest = hashlib.sha256()
                for chunk in LocalFilesystemPayloadReader(self.root, reference).stream_range(0, reference.size_bytes):
                    digest.update(chunk)
                if digest.hexdigest() != reference.sha256:
                    raise PayloadSnapshotError("Managed payload SHA-256 differs from its immutable manifest")
        except Exception as error:
            with self.store.sessions() as session:
                dataset = session.get(FujinPayloadDataset, dataset_id)
                if dataset is not None:
                    dataset.last_validated_at = datetime.now(timezone.utc)
                    dataset.last_validation_state = "INVALID"
                    dataset.last_validation_error = str(error)[:8000]
                    session.commit()
            raise
        with self.store.sessions() as session:
            dataset = session.get(FujinPayloadDataset, dataset_id)
            if dataset is not None:
                dataset.last_validated_at = datetime.now(timezone.utc)
                dataset.last_validation_state = "VALID"
                dataset.last_validation_error = None
                session.commit()
        return {
            "dataset_id": snapshot["id"], "state": "VALID",
            "files_total": len(files), "physical_bytes": sum(item.size_bytes for item in files),
            "manifest_sha256": snapshot["manifest_sha256"],
        }

    def cleanup_unreferenced_ready_datasets(self) -> int:
        """Retire only explicit RETIRED datasets with no live object reference."""
        removed = 0
        # This intentionally does not run automatically: physical payloads
        # remain until lifecycle/quarantine policy makes retirement explicit.
        with self.store.sessions() as session:
            rows = list(session.scalars(select(FujinPayloadDataset).where(
                FujinPayloadDataset.state == "RETIRED"
            )))
            for dataset in rows:
                from app.simulation_schema import VirtualObject
                references = int(session.scalar(select(func.count(VirtualObject.id)).where(
                    VirtualObject.payload_dataset_id == dataset.id
                )) or 0)
                if references:
                    continue
                directory = self.root / _safe_relative(dataset.repository_relative_path)
                if directory.exists():
                    shutil.rmtree(directory)
                session.delete(dataset)
                removed += 1
            session.commit()
        return removed
