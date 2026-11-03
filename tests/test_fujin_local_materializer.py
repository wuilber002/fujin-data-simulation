import json
from pathlib import Path

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.fujin_local_materializer import normalized_profile, process_generation_jobs
from app.fujin_local_schema import LocalDataset, LocalDatasetGenerationJob, migrate


def test_local_materializer_creates_durable_ready_snapshot(tmp_path):
    database = tmp_path / "catalog.db"
    url = f"sqlite+pysqlite:///{database}"
    migrate(url)
    profile = normalized_profile({"files": [
        {"name": "first.bin", "size_bytes": 13},
        {"name": "second.bin", "size_bytes": 29},
    ]})
    with Session(create_engine(url)) as session:
        dataset = LocalDataset(
            name="generated", state="GENERATING", model="REPRESENTATIVE",
            snapshot_id="pending-test", repository_relative_path="datasets/dataset-test",
            manifest_json=json.dumps({"format_version": 1, "objects": []}), quota_bytes=42,
        )
        session.add(dataset)
        session.add(LocalDatasetGenerationJob(
            dataset_id="dataset-test", state="READY", seed="seed", profile_json=json.dumps(profile),
            files_total=2,
        ))
        # Use a stable id so the expected output path is deterministic.
        dataset.id = "dataset-test"
        session.commit()

    assert process_generation_jobs(url, root=tmp_path / "payloads", max_files=1)["written_files"] == 1
    assert process_generation_jobs(url, root=tmp_path / "payloads", max_files=1)["written_files"] == 1
    assert process_generation_jobs(url, root=tmp_path / "payloads", max_files=1)["completed_steps"] == 1

    with Session(create_engine(url)) as session:
        dataset = session.get(LocalDataset, "dataset-test")
        job = session.scalar(select(LocalDatasetGenerationJob))
        manifest = json.loads(dataset.manifest_json)
        assert dataset.state == "READY"
        assert dataset.validated_objects == 2 and dataset.validated_bytes == 42
        assert len(dataset.snapshot_id) == 64
        assert job.state == "SUCCEEDED" and job.next_file_index == 2
        for entry in manifest["objects"]:
            path = tmp_path / "payloads" / entry["relative_path"]
            assert path.is_file() and path.stat().st_size == entry["size_bytes"]


def test_local_materializer_profile_rejects_paths_and_unsafe_sizes():
    for invalid in (
        {"files": []},
        {"files": [{"name": "../escape", "size_bytes": 1}]},
        {"files": [{"name": "normal.bin", "size_bytes": 0}]},
    ):
        try:
            normalized_profile(invalid)
            raise AssertionError("unsafe generation profile was accepted")
        except RuntimeError:
            pass
