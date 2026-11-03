import importlib.util
from pathlib import Path
import sys

from sqlalchemy import create_engine, text


def _tool():
    path = Path("scripts/reconcile-fujin-multipart.py")
    spec = importlib.util.spec_from_file_location("reconcile_fujin_multipart", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _schema(url):
    engine = create_engine(url)
    with engine.begin() as db:
        db.execute(text("CREATE TABLE sources (id INTEGER PRIMARY KEY, destination_bucket TEXT)"))
        db.execute(text("CREATE TABLE objects (id INTEGER PRIMARY KEY, source_id INTEGER, object_key TEXT, state TEXT, multipart_upload_id TEXT)"))
    return engine


def _fujin_schema(url):
    engine = create_engine(url)
    with engine.begin() as db:
        db.execute(text("CREATE TABLE local_oci_buckets (id TEXT PRIMARY KEY, name TEXT)"))
        db.execute(text("CREATE TABLE local_multipart_uploads (id TEXT PRIMARY KEY, bucket_id TEXT, object_key TEXT, metadata_json TEXT)"))
        db.execute(text("CREATE TABLE local_multipart_parts (upload_id TEXT, part_number INTEGER, size_bytes INTEGER)"))
    return engine


def test_reconciler_only_selects_unreferenced_terminal_uploads_with_raijin_provenance(tmp_path):
    tool = _tool()
    raijin_url = f"sqlite+pysqlite:///{tmp_path / 'raijin.db'}"
    fujin_url = f"sqlite+pysqlite:///{tmp_path / 'fujin.db'}"
    raijin, fujin = _schema(raijin_url), _fujin_schema(fujin_url)
    with raijin.begin() as db:
        db.execute(text("INSERT INTO sources VALUES (1, 'target')"))
        db.execute(text("INSERT INTO objects VALUES (10, 1, 'done.bin', 'TRANSFERRED', NULL)"))
        db.execute(text("INSERT INTO objects VALUES (11, 1, 'active.bin', 'TRANSFERRING', 'active-upload')"))
    with fujin.begin() as db:
        db.execute(text("INSERT INTO local_oci_buckets VALUES ('bucket', 'target')"))
        db.execute(text("INSERT INTO local_multipart_uploads VALUES ('proven', 'bucket', 'done.bin', '{\"raijin-object-id\": \"10\"}')"))
        db.execute(text("INSERT INTO local_multipart_uploads VALUES ('active-upload', 'bucket', 'active.bin', '{\"raijin-object-id\": \"11\"}')"))
        db.execute(text("INSERT INTO local_multipart_uploads VALUES ('unknown', 'bucket', 'else.bin', '{}')"))
        db.execute(text("INSERT INTO local_multipart_parts VALUES ('proven', 1, 12)"))
    candidates, summary = tool.inspect(fujin_url, raijin_url)
    assert [candidate.upload_id for candidate in candidates] == ["proven"]
    assert summary["referenced_by_raijin"] == 1
    assert summary["unmarked_or_unknown"] == 1
    assert tool.apply(fujin_url, candidates) == 1
    with fujin.connect() as db:
        assert db.execute(text("SELECT count(*) FROM local_multipart_uploads WHERE id = 'proven'")).scalar() == 0
        assert db.execute(text("SELECT count(*) FROM local_multipart_parts WHERE upload_id = 'proven'")).scalar() == 0
