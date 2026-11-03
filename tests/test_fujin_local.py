"""Provider-level contract checks for the isolated Fujin LOCAL catalogue."""

import asyncio
import base64
from datetime import datetime, timedelta, timezone
import hashlib
import importlib
import importlib.util
import json
import os
from pathlib import Path
import socket
import sqlite3
import stat
import subprocess
import sys
import threading
import time
from urllib import error as urlerror
from urllib import request as urlrequest

import boto3
from botocore.config import Config
import oci
import pytest
from sqlalchemy import create_engine, inspect, select
from sqlalchemy.orm import Session, sessionmaker
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from starlette.requests import Request
import uvicorn
from fastapi import HTTPException
import zlib


def _request(method, path, headers=(), query=b"", body=b""):
    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}
    return Request({"type": "http", "method": method, "path": path,
                    "headers": list(headers), "query_string": query}, receive)


def _create_ready_dataset(module, request, payload, session):
    """Fixture-only legacy catalogue seed; public creation is Fujin-owned."""
    dataset_row = module.LocalDataset(
        name=payload.name, snapshot_id=payload.snapshot_id,
        repository_relative_path=payload.repository_relative_path,
        manifest_json=json.dumps(payload.manifest, sort_keys=True),
        quota_bytes=payload.quota_bytes, state="PENDING_VALIDATION",
    )
    session.add(dataset_row)
    session.commit()
    dataset = {"id": dataset_row.id}
    module.validate_dataset(dataset["id"], _request("POST", "/api/local/datasets/validate"), session=session)
    return dataset


def test_s3control_rest_xml_manifest_contract():
    import app.fujin_local as module
    request = _request("POST", "/v20180820/jobs", body=(
        b'<CreateJobRequest xmlns="http://awss3control.amazonaws.com/doc/2018-08-20/">'
        b'<Manifest><Location><ObjectArn>arn:aws:s3:::control/manifest.csv</ObjectArn>'
        b'</Location></Manifest></CreateJobRequest>'
    ))
    assert asyncio.run(module.s3control_manifest_location(request)) == "arn:aws:s3:::control/manifest.csv"


def test_sqlite_catalogue_uses_wal_and_bounded_busy_wait(tmp_path, monkeypatch):
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'wal.db'}")
    monkeypatch.setenv("FUJIN_LOCAL_SQLITE_BUSY_TIMEOUT_SECONDS", "30")
    import app.fujin_local as module
    module = importlib.reload(module)
    try:
        with module.engine.connect() as connection:
            assert connection.exec_driver_sql("PRAGMA journal_mode").scalar().lower() == "wal"
            assert connection.exec_driver_sql("PRAGMA busy_timeout").scalar() == 30000
            assert connection.exec_driver_sql("PRAGMA synchronous").scalar() == 1
        assert module.SQLITE_LOCK_RETRY_ATTEMPTS == 3
        assert module.sqlite_locked(sqlite3.OperationalError("database is locked"))
    finally:
        module.engine.dispose()


def test_local_dataset_generation_is_queued_without_a_host_path(tmp_path, monkeypatch):
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'generation.db'}")
    import app.fujin_local as module
    module = importlib.reload(module); module.startup()
    try:
        with module.SessionLocal() as session:
            result = module.generate_dataset(
                _request("POST", "/api/local/datasets/generate"),
                module.DatasetGenerateCreate(
                    name="generated-locally", quota_bytes=2048, seed="stable-seed",
                    profile={"file_count": 2, "file_size_bytes": 1024},
                ),
                session,
            )
            dataset = session.get(module.LocalDataset, result["id"])
            job = session.get(module.LocalDatasetGenerationJob, result["job_id"])
            assert result["state"] == "GENERATING"
            assert dataset.repository_relative_path == f"datasets/{dataset.id}"
            assert job.state == "READY" and job.files_total == 2
        with module.SessionLocal() as session:
            rows = module.list_datasets(_request("GET", "/api/local/datasets"), session)
            assert rows[0]["generation"] == {
                "state": "READY", "files_total": 2, "files_written": 0,
                "bytes_written": 0, "last_error": None,
            }
    finally:
        module.engine.dispose()


def test_local_rejects_external_dataset_paths_and_packages(tmp_path, monkeypatch):
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'no-import.db'}")
    import app.fujin_local as module
    module = importlib.reload(module); module.startup()
    try:
        with module.SessionLocal() as session:
            with pytest.raises(HTTPException) as package_error:
                module.list_dataset_packages(_request("GET", "/api/local/dataset-packages"), session)
            assert package_error.value.status_code == 410
            with pytest.raises(HTTPException) as path_error:
                module.create_dataset(
                    _request("POST", "/api/local/datasets"),
                    module.DatasetCreate(
                        name="external", snapshot_id="external", repository_relative_path="outside",
                        quota_bytes=1,
                    ), session,
                )
            assert path_error.value.status_code == 410
    finally:
        module.engine.dispose()


def test_deleting_a_generated_dataset_removes_its_owned_files(tmp_path, monkeypatch):
    database = Path(tmp_path) / "delete-generated.db"
    root = Path(tmp_path) / "payloads"
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{database}")
    monkeypatch.setenv("FUJIN_LOCAL_PAYLOAD_ROOT", str(root))
    import app.fujin_local as module
    from app.fujin_local_materializer import process_generation_jobs
    module = importlib.reload(module); module.startup()
    try:
        with module.SessionLocal() as session:
            created = module.generate_dataset(
                _request("POST", "/api/local/datasets/generate"),
                module.DatasetGenerateCreate(
                    name="remove-generated", quota_bytes=9, seed="seed",
                    profile={"file_count": 1, "file_size_bytes": 9},
                ), session,
            )
        process_generation_jobs(module.DATABASE_URL, root=root, max_files=2)
        with module.SessionLocal() as session:
            dataset = session.get(module.LocalDataset, created["id"])
            directory = root / dataset.repository_relative_path
            assert dataset.state == "READY" and directory.is_dir()
            response = module.delete_dataset(created["id"], _request("DELETE", "/api/local/datasets/id"), session)
            assert response.status_code == 202
            assert directory.is_dir()
        process_generation_jobs(module.DATABASE_URL, root=root, max_files=1)
        with module.SessionLocal() as session:
            job = session.scalar(select(module.LocalDatasetGenerationJob).where(
                module.LocalDatasetGenerationJob.dataset_id == created["id"]
            ))
            assert job.state == "CLEANUP_SUCCEEDED"
            assert not directory.exists()
    finally:
        module.engine.dispose()


def test_data_plane_provider_state_is_short_cached_and_admin_invalidation_is_immediate(tmp_path, monkeypatch):
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'provider-cache.db'}")
    monkeypatch.setenv("FUJIN_LOCAL_PROVIDER_STATE_CACHE_SECONDS", "30")
    import app.fujin_local as module
    module = importlib.reload(module); module.startup()
    try:
        with module.SessionLocal() as session:
            module.cloud_provider_state(session, "AWS").enabled = True
            session.commit()
        assert module.data_plane_provider_enabled("AWS") is True
        assert module.data_plane_provider_enabled("AWS") is True
        assert module.data_plane_metrics_snapshot()["provider_state_cache_hits"] == 1
        with module.SessionLocal() as session:
            module.cloud_provider_state(session, "AWS").enabled = False
            session.commit()
        module.invalidate_data_plane_provider_cache("AWS")
        assert module.data_plane_provider_enabled("AWS") is False
        assert module.operational_metrics(_request("GET", "/api/local/operational-metrics"))["data_plane"]["provider_state_cache_entries"] == 1
    finally:
        module.engine.dispose()


def test_data_plane_guard_returns_retryable_503_when_catalogue_checkout_is_unavailable(monkeypatch):
    import app.fujin_local as module

    monkeypatch.setattr(module, "data_plane_provider_enabled", lambda _provider: (_ for _ in ()).throw(
        module.SQLAlchemyTimeoutError("pool", None, None)
    ))

    async def next_handler(_request):
        raise AssertionError("provider handler must not run while the catalogue is unavailable")

    response = asyncio.run(module.local_data_plane_guard(_request("HEAD", "/bucket/key"), next_handler))
    assert response.status_code == 503
    assert response.headers["retry-after"] == "1"


def test_unhandled_oci_provider_error_is_searchable_without_exposing_multipart_handle(tmp_path, monkeypatch):
    """Unexpected data-plane failures must be diagnosable outside container logs."""
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'provider-500.db'}")
    import app.fujin_local as module
    module = importlib.reload(module); module.startup()
    try:
        with module.SessionLocal() as session:
            module.cloud_provider_state(session, "OCI").enabled = True
            session.commit()
        module.invalidate_data_plane_provider_cache("OCI")

        async def next_handler(_request):
            raise NameError("simulated provider regression")

        upload_id = "opaque-multipart-handle"
        response = asyncio.run(module.local_data_plane_guard(
            _request("POST", "/n/local/b/destination/u/object.bin", query=f"uploadId={upload_id}".encode()),
            next_handler,
        ))
        identifier = response.headers["x-fujin-request-id"]
        assert response.status_code == 500 and identifier.startswith("fujin-")
        with module.SessionLocal() as session:
            event = session.scalar(select(module.LocalAuditEvent).where(
                module.LocalAuditEvent.request_id == identifier
            ))
            assert event is not None
            assert event.operation == "OCI_UNHANDLED_PROVIDER_ERROR"
            assert event.status_code == 500 and event.bucket == "destination"
            assert event.object_key == "object.bin"
            assert event.error_code == "NameError"
            assert "multipart_upload=sha256:" in event.detail
            assert upload_id not in event.detail and upload_id not in (event.endpoint or "")
    finally:
        module.engine.dispose()


def test_hot_multipart_cleanup_does_not_delete_audit_evidence(tmp_path, monkeypatch):
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'retention.db'}")
    import app.fujin_local as module
    module = importlib.reload(module); module.startup()
    try:
        with module.SessionLocal() as session:
            old = module.utcnow() - module.timedelta(hours=module.FUJIN_LOCAL_AUDIT_SUCCESS_RETENTION_HOURS + 1)
            session.add(module.LocalAuditEvent(request_id="retention-old", operation="OCI_UPLOAD_PART", status_code=200, created_at=old))
            session.commit()
            assert module.cleanup_expired_multipart_uploads(session, now=module.utcnow()) == 0
            assert session.get(module.LocalAuditEvent, 1) is not None
            module.cleanup_expired_multipart_uploads(session, now=module.utcnow(), include_audit_retention=True)
            session.commit()
            assert session.get(module.LocalAuditEvent, 1) is None
    finally:
        module.engine.dispose()


def test_aws_chunked_object_body_removes_framing_and_validates_crc32():
    import app.fujin_local as module
    content = b'bucket,"folder/object, one.bin"\n'
    checksum = base64.b64encode((zlib.crc32(content) & 0xffffffff).to_bytes(4, "big"))
    framed = (
        f"{len(content):x};chunk-signature=synthetic\r\n".encode()
        + content
        + b"\r\n0;chunk-signature=synthetic\r\n"
        + b"x-amz-checksum-crc32:" + checksum + b"\r\n\r\n"
    )
    headers = [
        (b"content-encoding", b"aws-chunked"),
        (b"x-amz-decoded-content-length", str(len(content)).encode()),
        (b"x-amz-trailer", b"x-amz-checksum-crc32"),
    ]
    assert asyncio.run(module.s3_object_body(_request("PUT", "/manifest.csv", headers, body=framed))) == content

    bad_headers = headers[:-1] + [(b"x-amz-trailer", b"x-amz-checksum-crc32")]
    bad_framed = framed.replace(checksum, b"AAAAAA==")
    with pytest.raises(HTTPException, match="invalid trailer") as raised:
        asyncio.run(module.s3_object_body(_request("PUT", "/manifest.csv", bad_headers, body=bad_framed)))
    assert raised.value.status_code == 400


def test_aws_chunked_object_body_rejects_length_mismatch():
    import app.fujin_local as module
    framed = b"4\r\ndata\r\n0\r\n\r\n"
    headers = [
        (b"content-encoding", b"aws-chunked"),
        (b"x-amz-decoded-content-length", b"5"),
    ]
    with pytest.raises(HTTPException, match="decoded length mismatch") as raised:
        asyncio.run(module.s3_object_body(_request("PUT", "/object", headers, body=framed)))
    assert raised.value.status_code == 400


def test_s3_batch_manifest_parser_supports_csv_and_url_encoded_keys():
    import app.fujin_local as module
    assert module.parse_s3_batch_manifest(
        b'bucket,"folder/object%2C%20one.bin"\n'
    ) == [("folder/object, one.bin", "folder/object%2C%20one.bin")]
    with pytest.raises(HTTPException, match="invalid row 1") as raised:
        module.parse_s3_batch_manifest(b"aws-chunk-size-without-a-key\n")
    assert raised.value.status_code == 422


def test_local_console_exposes_only_local_administration_controls():
    import app.fujin_local as module
    page = module.console()
    assert "Fujin LOCAL" in page
    assert "aws-provider-toggle" in page and "oci-provider-toggle" in page
    assert "Encerrar restores AWS ativos" in page and "Encerrar uploads OCI ativos" in page
    assert "'Operações AWS':'aws'" in page and "'Operações OCI':'oci'" in page
    assert "id='provider-toggle'" not in page and "id='cancel-active'" not in page
    assert "Buckets S3" in page
    assert "id='s3-dataset-select'" in page and "syncS3DatasetOptions" in page
    assert "name='logical_multiplier'" in page and "Multiplicador lógico" in page
    assert "s3-logical-summary" in page and "renderS3LogicalSummary" in page
    assert "Política restore JSON" not in page
    assert "Disponibilização após restore" in page and "restore_minimum_hours" in page
    assert "restore_random_variation_hours" in page and "random_variation_hours" in page
    assert "restore_gradual_min_hours" in page and "gradual_window_min_hours" in page
    assert "restore_gradual_max_hours" in page and "gradual_window_max_hours" in page
    assert "raijin-e2e-destination-10tb" in page
    assert "id='oci-private-region'" in page
    assert "name='configuration'" not in page
    assert "bucket_configuration" not in page
    assert "file_count" in page and "file_size_mib" in page
    assert "Criar dataset LOCAL" in page
    assert "/api/local/datasets/generate" in page
    assert "/api/local/datasets/from-package" not in page
    assert page.count("class='help'") >= 9
    assert "enableHelpTooltips()" in page
    assert "fujin-table" in page
    assert "s3BucketAction" in page
    assert "renderOciBuckets" in page and "ociBucketAction" in page
    assert "renderDatasets" in page and "datasetAction" in page
    assert "Dataset vinculado" in page and "fujin-dataset-binding" in page
    assert "dataset_state" in page
    assert "Desabilitar" in page and "Habilitar" in page and "Excluir" in page
    assert "s3-connection-modal" in page
    assert "openFujinModal('s3-connection-modal')" in page
    assert "oci-connection-modal" in page
    assert "openFujinModal('oci-connection-modal')" in page
    assert "dataset-details-modal" in page
    assert "openFujinModal('dataset-details-modal')" in page
    assert "dataset-validation-form" not in page and "delete-form" not in page
    assert "focus({preventScroll:true})" in page
    assert "function mountActionMenu" in page
    assert "menu.style.visibility='hidden'" in page
    assert "menu.style.left='0px'" in page and "menu.style.top='0px'" in page
    assert page.count("mountActionMenu(menu,trigger)") == 4
    assert "overflow-anchor:none" in page
    assert "tokenLabel" not in page
    assert "audit-search" in page
    assert "audit-pagination" in page and "loadAuditPage" in page
    assert "id='audit-bucket'" in page and "/api/local/audit/buckets" in page
    assert "if(auditQuery)await loadAuditPage(auditPage)" in page
    assert "Nenhum evento é carregado automaticamente" in page
    assert "query.set('limit','11')" in page and "query.set('offset',String(auditPage*10))" in page
    assert "/api/local/audit/count?" not in page and "Página ${auditPage+1}" in page
    assert "html,body,*{overflow-anchor:none}" in page and "stabilizePointerClicks()" in page
    assert "updateUrlState" in page and "initialUrlState.get('view')" in page
    assert "audit_page" in page and "selected_id" in page and "restoreUrlSelection" in page
    assert "renderOverview" in page and "renderAudit" in page
    assert "<small>AWS S3 Bucket(s)</small>" in page
    assert "<small>OCI Object Storage Bucket(s)</small>" in page
    assert "<small>Datasets</small>" in page
    assert "<h3>Armazenamento local</h3>" in page
    overview = page[page.index("function renderOverview"):page.index("function renderAudit")]
    assert '<div class="fujin-metric"><small>Repositório de payloads</small>' not in overview
    assert '<div class="fujin-metric"><small>Área temporária</small>' not in overview
    assert '<div class="fujin-connection-field"><small>Repositório de payloads</small>' in overview
    assert '<div class="fujin-connection-field"><small>Área temporária</small>' in overview
    assert page.count("fujin-status-grid fujin-compact-metrics") == 2
    assert "compactMetricGroup" in page and "Math.ceil(longest+30)" in page
    assert "requestAnimationFrame(compactAllMetricGroups)" in page
    assert "audit-details-modal" in page and "renderAuditDetails" in page
    assert "<pre id='overview'" not in page and "<pre id='audit'" not in page
    assert "Request ID" in page and "Latência" in page and "Bytes transferidos" in page
    assert "/api/simulation" not in page


def test_local_console_has_fujin_operational_shell_and_cloud_views():
    import app.fujin_local as module
    page = module.console()
    assert "static/operational-shell.css?v=20260910-1" in page
    assert "static/operational-shell.js?v=20260910-1" in page
    assert "restore-availability-fieldset" in page
    assert "s3NameLabel.childNodes[0].textContent='Nome do bucket'" in page
    assert "s3NameInput.placeholder='raijin-e2e-10tb'" in page
    shell = Path("app/static/operational-shell.js").read_text()
    assert "stabilizePointerClicks" in shell and "window.scrollTo" in shell
    assert "enableHelpTooltips" in shell and "operational-help-tooltip" in shell
    assert "requestAnimationFrame(()=>requestAnimationFrame" in shell
    stabilizer = shell[shell.index("stabilizePointerClicks"):shell.index("createCard")]
    assert "window.addEventListener('scroll'" not in stabilizer
    assert "['wheel','touchmove','keydown']" in shell and "overScrollbar" in shell
    assert "operational-statusbar" in page
    assert "fujin-topbar" in page
    assert "fujin-ticker" in page
    assert "fujin-clock" in page
    for view in ("status", "datasets", "aws", "oci", "settings"):
        assert f"data-fujin-view='{view}'" in page
    assert "PRIVATE LOCAL CLOUD" in page
    assert "Estado e endpoints" in page


def test_local_console_uses_the_shared_operational_card_contract():
    import app.fujin_local as module
    page = module.console()
    shell = Path("app/static/operational-shell.js").read_text()
    styles = Path("app/static/operational-shell.css").read_text()
    assert "shell?.enhanceCards()" in page
    assert "data-card-help" in page
    assert "createCard" in shell and "enhanceCards" in shell
    assert ".operational-card[data-card-status=\"success\"]" in styles
    assert ".operational-card[data-card-status=\"warning\"]" in styles
    assert ".operational-card[data-card-status=\"error\"]" in styles
    assert ".operational-card-info" in styles
    assert ".operational-card-tooltip" in styles
    assert 'data-card-kind="status"' in page
    assert 'data-card-kind="number"' in page
    assert '.operational-card[data-card-kind="number"]>b' in styles
    assert '.operational-card[data-card-kind="status"]>b' in styles


def test_local_console_exposes_generic_private_aws_and_oci_endpoint_profiles(tmp_path, monkeypatch):
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'endpoints.db'}")
    import app.fujin_local as module
    module = importlib.reload(module); module.startup()
    with module.SessionLocal() as session:
        profile = module.overview(_request("GET", "/api/local/overview"), session)["private_endpoint_profile"]
    assert profile["aws_region"] == "us-east-1"
    assert profile["oci_region"] == "sa-saopaulo-1"
    assert profile["oci_object_storage_endpoint_url"] == "https://oci.sa-saopaulo-1.fujin.internal"
    assert profile["s3_endpoint_pattern"].startswith("https://<bucket>.vpce-")
    assert profile["tls_ca_bundle_path"].startswith("/")


def test_private_tls_script_covers_all_regional_provider_hostnames():
    script = Path("scripts/run-fujin-local.sh").read_text(encoding="utf-8")
    assert "DNS:sts.${aws_region}.fujin.internal" in script
    assert "DNS:control.${endpoint_id}.s3.${aws_region}.fujin.internal" in script
    assert "DNS:oci.${oci_region}.fujin.internal" in script
    assert 'grep -Fq "DNS:oci.${oci_region}.fujin.internal"' in script


def test_local_oci_runtime_profile_keeps_real_configuration_isolated(tmp_path):
    source = Path(tmp_path) / "real-runtime.json"
    output = Path(tmp_path) / "local-runtime.json"
    source.write_text(json.dumps({"tenancy_ocid": "preserved", "object_storage_namespace": "real"}), encoding="utf-8")
    completed = subprocess.run([
        sys.executable, "scripts/configure-fujin-local-oci-runtime.py", "--source", str(source),
        "--output", str(output), "--namespace", "local-target",
    ], check=True, capture_output=True, text=True)
    derived = json.loads(output.read_text(encoding="utf-8"))
    assert "isolated Fujin LOCAL OCI runtime" in completed.stdout
    assert derived == {
        "tenancy_ocid": "preserved", "object_storage_namespace": "local-target",
        "object_storage_endpoint_url": "https://oci.sa-saopaulo-1.fujin.internal",
        "object_storage_ca_bundle_path": "/etc/fujin-local-ca/fujin-local.crt",
    }
    assert stat.S_IMODE(output.stat().st_mode) == 0o600
    assert json.loads(source.read_text(encoding="utf-8"))["object_storage_namespace"] == "real"


def test_local_compose_isolates_the_fujin_materializer_from_the_read_only_provider():
    compose = Path("docker-compose.yml").read_text(encoding="utf-8")
    assert "- /var/lib/raijin-data-migration/fujin-payloads:/var/lib/fujin-local/payloads:ro" in compose
    materializer = compose.split("  fujin-local-materializer:\n", 1)[1].split("  fujin-local-dns:\n", 1)[0]
    assert 'command: ["python3", "-m", "app.fujin_local_materializer"]' in materializer
    assert "- /var/lib/raijin-data-migration/fujin-payloads:/var/lib/fujin-local/payloads:rw" in materializer
    assert "ports:" not in materializer


def test_local_compose_exposes_only_the_loopback_ui_gateway():
    compose = Path("docker-compose.yml").read_text(encoding="utf-8")
    fujin = compose.split("  fujin-local:\n", 1)[1].split("  fujin-local-dns:\n", 1)[0]
    local_app = compose.split("  local-app:\n", 1)[1].split("  local-ui-gateway:\n", 1)[0]
    gateway = compose.split("  local-ui-gateway:\n", 1)[1].split("  simulator:\n", 1)[0]
    assert "ports:" not in fujin and "ports:" not in local_app
    assert 'ports: ["${RAIJIN_WEB_BIND_HOST:-127.0.0.1}:${RAIJIN_WEB_BIND_PORT:-1604}:8080"]' in gateway
    assert "fujin-local-data:\n    internal: true" in compose
    assert "fujin-local-ui:\n    internal: true" in compose
    assert "networks: [fujin-local-ui, default, fujin-local-data]" in compose
    assert "networks: [default, fujin-local-ui]" in gateway


def test_local_ui_gateway_resolves_provider_aliases_after_container_restart():
    config = Path("docker/nginx-local-ui.conf").read_text(encoding="utf-8")
    launcher = Path("docker/run-local-ui-gateway.sh").read_text(encoding="utf-8")
    assert "resolver __FUJIN_DNS_RESOLVER__ valid=10s" in config
    assert "$fujin_local_upstream" in config
    assert "$raijin_local_upstream" in config
    assert "rewrite ^/raijin/(.*)$ /$1 break;" in config
    assert "proxy_pass http://$raijin_local_upstream:8080;" in config
    assert "location ~ ^/api/sources/[0-9]+/validate-destination$" in config
    assert "proxy_read_timeout 10m;" in config
    assert "location = /favicon.ico" in config
    assert "/static/favicon.svg" in config
    assert Path("app/static/favicon.svg").is_file()
    assert "proxy_ssl_server_name on" in config
    assert "rewrite ^/fujin/api/(.*)$ /api/$1 break;" in config
    assert "location /fujin/static/" in config
    assert "rewrite ^/fujin/static/(.*)$ /static/$1 break;" in config
    assert "proxy_pass https://$fujin_local_upstream:443;" in config
    assert "awk '/^nameserver" in launcher
    assert "__FUJIN_DNS_RESOLVER__" in launcher


def test_oracle_linux_podman_launcher_keeps_raijin_real_and_the_data_plane_private():
    launcher = Path("scripts/start-fujin-local-runtime.sh").read_text(encoding="utf-8")
    assert "RAIJIN_OPERATION_MODE=REAL" in launcher
    assert "RAIJIN_OPERATION_MODE=LOCAL" not in launcher
    assert "podman network create --internal --subnet 172.30.0.0/24" in launcher
    assert 'podman network connect --alias fujin-local "$ui_network" raijin-fujin-local' in launcher
    assert "raijin-stop-runtime" in launcher and "docker.io/library/postgres:16-alpine" in launcher
    assert 'mountpoint -q "$data_root/fujin-payloads"' in launcher
    assert 'source "$install_root/scripts/web-bind.sh"' in launcher
    assert '-p "$RAIJIN_WEB_BIND_PUBLISH"' in launcher
    assert "raijin-fujin-local" in launcher and "fujin-payloads:/var/lib/fujin-local/payloads:ro" in launcher
    assert "raijin-fujin-local-materializer" in launcher
    assert 'fujin-local:/var/lib/fujin-local:z' in launcher
    assert 'fujin-local:/var/lib/fujin-local:Z' not in launcher
    assert 'fujin-payloads:/var/lib/fujin-local/payloads:rw,z' in launcher
    assert '"$image" python3 -m app.fujin_local_materializer' in launcher
    assert 'podman network connect --alias local-app "$ui_network" raijin-app' in launcher
    assert '--network "$main_network" -p "$RAIJIN_WEB_BIND_PUBLISH"' in launcher
    assert 'ui_resolver_ip=' in launcher and 'gateway_resolv_conf=' in launcher
    assert 'FUJIN_UI_RESOLVER="$ui_resolver_ip"' in launcher
    assert 'FUJIN_UI_RESOLVER:-' in Path("docker/run-local-ui-gateway.sh").read_text(encoding="utf-8")
    bootstrap = Path("scripts/bootstrap.sh").read_text(encoding="utf-8")
    assert "raijin-start-fujin-local-runtime" in bootstrap
    assert "deployment-profile" in bootstrap
    assert 'deployment_profile" == LOCAL' in bootstrap
    assert '/usr/local/sbin/raijin-start-fujin-local-runtime' in bootstrap
    assert '/usr/local/sbin/raijin-start-runtime "$operation_mode"' in bootstrap
    assert 'printf \'STANDARD\\n\'' in bootstrap
    assert '"$install_root/VERSION"' in bootstrap
    assert 'basename "$(readlink -f "$install_root")"' in bootstrap
    assert 'release_image="localhost/raijin-data-migration:$build_revision"' in bootstrap
    assert "'{{.ImageName}}'" in bootstrap
    assert '!= "$release_image"' in bootstrap
    assert 'podman build --no-cache' in bootstrap
    assert 'RAIJIN_IMAGE="$release_image" /usr/local/sbin/raijin-start-fujin-local-runtime' in bootstrap
    standard_launcher = Path("scripts/start-runtime.sh").read_text(encoding="utf-8")
    assert 'image="${RAIJIN_IMAGE:-localhost/raijin-data-migration:latest}"' in standard_launcher
    updater = Path("scripts/update-fujin-local-raijin-runtime.sh").read_text(encoding="utf-8")
    assert "identity['raijin_build_revision']" in updater
    assert "expected_revision" in updater
    assert 'candidate_name="raijin-app-candidate"' in updater
    assert 'start_app "$candidate_name" "$target_image"' in updater
    assert 'podman rename "$candidate_name" raijin-app' in updater
    assert 'podman exec raijin-local-ui-gateway nginx -s reload' in updater
    assert "validate_release_images" in updater
    assert '[[ "$actual" == "$expected" ]]' in updater
    assert "no automatic rollback is configured" in updater
    assert "abort_update()" in updater
    assert "active transfer/lease record(s) remain" in updater
    config = Path("docker/nginx-local-ui.conf").read_text(encoding="utf-8")
    assert "proxy_next_upstream error timeout http_502 http_503 http_504" in config
    assert "proxy_next_upstream_tries 4" in config
    candidate = updater.index('start_app "$candidate_name" "$target_image"')
    retire_old = updater.index('podman stop -t "$stop_timeout" --ignore raijin-app')
    promote = updater.index('podman rename "$candidate_name" raijin-app')
    assert candidate < retire_old < promote
    coredns = Path("docker/fujin-local.Corefile").read_text(encoding="utf-8")
    assert "forward . /etc/resolv.conf" in coredns
    assert "forward . 127.0.0.11" not in coredns


def test_oracle_linux_podman_launcher_executes_the_private_topology_in_dry_run(tmp_path):
    install = tmp_path / "release"; (install / "docker").mkdir(parents=True)
    (install / "docker" / "fujin-local.Corefile").write_text(".:53 {}", encoding="utf-8")
    (install / "docker" / "nginx-local-ui.conf").write_text("server {}", encoding="utf-8")
    (install / "scripts").mkdir()
    (install / "scripts" / "web-bind.sh").write_text(
        Path("scripts/web-bind.sh").read_text(encoding="utf-8"), encoding="utf-8"
    )
    (install / "scripts" / "console-auth-lifecycle.sh").write_text(
        Path("scripts/console-auth-lifecycle.sh").read_text(encoding="utf-8"), encoding="utf-8"
    )
    data = tmp_path / "data"; (data / "fujin-payloads").mkdir(parents=True)
    secret = tmp_path / "secrets"; secret.mkdir(); (secret / "fujin_local_admin_token").write_text("token", encoding="utf-8")
    runtime = tmp_path / "oci-local.json"; runtime.write_text("{}", encoding="utf-8")
    fake_bin = tmp_path / "bin"; fake_bin.mkdir(); log = tmp_path / "podman.log"
    (fake_bin / "podman").write_text("#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$PODMAN_LOG\"\ncase \"$1 $2\" in 'container exists'|'network exists') exit 1;; esac\nexit 0\n", encoding="utf-8")
    (fake_bin / "mountpoint").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (fake_bin / "podman").chmod(0o755); (fake_bin / "mountpoint").chmod(0o755)
    env = os.environ | {"PATH": f"{fake_bin}:{os.environ['PATH']}", "PODMAN_LOG": str(log),
                        "RAIJIN_INSTALL_ROOT": str(install), "RAIJIN_DATA_ROOT": str(data),
                        "RAIJIN_SECRET_ROOT": str(secret), "RAIJIN_LOCAL_OCI_RUNTIME_CONFIG": str(runtime)}
    run = subprocess.run(["bash", "scripts/start-fujin-local-runtime.sh"], env=env, text=True, capture_output=True, check=True)
    commands = log.read_text(encoding="utf-8")
    assert "LOCAL private cloud is ready" in run.stdout
    assert "network create --internal --subnet 172.30.0.0/24" in commands
    assert "--name raijin-fujin-local" in commands
    assert "--name raijin-fujin-local-materializer" in commands
    assert "--name raijin-app" in commands and "RAIJIN_OPERATION_MODE=REAL" in commands
    assert "/run/platform-status:ro,z" in commands
    assert "--name raijin-local-ui-gateway" in commands and "-p 127.0.0.1:1604:8080" in commands


def test_local_administration_uses_the_ssh_tunnel_boundary_not_a_browser_token(monkeypatch):
    monkeypatch.setenv("FUJIN_LOCAL_ADMIN_TOKEN", "obsolete-token")
    import app.fujin_local as module
    module = importlib.reload(module)
    assert module.require_admin(_request("GET", "/api/local/overview")) is None
    assert "FUJIN_LOCAL_ADMIN_TOKEN_FILE" not in Path("scripts/start-fujin-local-runtime.sh").read_text(encoding="utf-8")
    assert "fujin_local_admin_token" not in Path("scripts/bootstrap.sh").read_text(encoding="utf-8")


def test_batch_restore_retention_reads_boto3_expiration_in_days_xml():
    import app.fujin_local as module

    body = b"""<CreateJobRequest xmlns="http://awss3.amazonaws.com/doc/2006-03-01/">
      <Operation><S3InitiateRestoreObject>
        <ExpirationInDays>3</ExpirationInDays><GlacierJobTier>BULK</GlacierJobTier>
      </S3InitiateRestoreObject></Operation>
    </CreateJobRequest>"""
    request = _request("POST", "/v20180820/jobs", body=body)

    assert asyncio.run(module.restore_retention_days(request, batch=True)) == 3


def test_s3_bucket_logical_multiplier_projects_independent_keys_without_copying_payloads(tmp_path, monkeypatch):
    root = Path(tmp_path) / "payloads"
    package = root / "snapshot"
    package.mkdir(parents=True)
    (package / "a.bin").write_bytes(b"aaa")
    (package / "b.bin").write_bytes(b"bbbb")
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'multiplier.db'}")
    monkeypatch.setenv("FUJIN_LOCAL_PAYLOAD_ROOT", str(root))

    import app.fujin_local as module
    module = importlib.reload(module); module.startup(); session = module.SessionLocal()
    try:
        manifest = {"objects": [
            {"key": "a.bin", "relative_path": "snapshot/a.bin", "size_bytes": 3,
             "sha256": hashlib.sha256(b"aaa").hexdigest(), "etag": "etag-a"},
            {"key": "b.bin", "relative_path": "snapshot/b.bin", "size_bytes": 4,
             "sha256": hashlib.sha256(b"bbbb").hexdigest(), "etag": "etag-b"},
        ]}
        admin = _request("POST", "/api/local")
        dataset = _create_ready_dataset(module, admin, module.DatasetCreate(
            name="physical-7b", snapshot_id="physical-7b", repository_relative_path="snapshot",
            quota_bytes=7, manifest=manifest,
        ), session)
        created = module.create_s3_bucket(admin, module.S3BucketCreate(
            name="logical-3x", dataset_id=dataset["id"], logical_multiplier=3,
            restore_policy={"minimum_delay_hours": 0, "random_variation_hours": 0,
                            "gradual_window_min_hours": 0, "gradual_window_max_hours": 0},
        ), session)
        bucket = session.get(module.LocalS3Bucket, created["id"])
        headers = [(b"host", b"logical-3x.vpce-fujin.s3.us-east-1.fujin.internal"),
                   (b"authorization", f"AWS4-HMAC-SHA256 Credential={bucket.access_key_id}/test".encode())]

        listed = asyncio.run(module.s3_data_plane(
            _request("GET", "/", headers, b"list-type=2&max-keys=2"), "", session,
        ))
        assert b"replica-001/a.bin" in listed.body and b"replica-001/b.bin" in listed.body
        assert b"<KeyCount>2</KeyCount>" in listed.body and b"<IsTruncated>true</IsTruncated>" in listed.body
        second = asyncio.run(module.s3_data_plane(
            _request("GET", "/", headers, b"list-type=2&max-keys=10&continuation-token=replica-001%2Fb.bin"), "", session,
        ))
        assert second.body.count(b"<Contents>") == 4
        assert b"replica-002/a.bin" in second.body and b"replica-003/b.bin" in second.body

        logical = "replica-002/a.bin"
        restore = asyncio.run(module.s3_data_plane(
            _request("PUT", "/" + logical, headers, b"restore=", b"<RestoreRequest><Days>2</Days></RestoreRequest>"),
            logical, session,
        ))
        assert restore.status_code == 202
        row = session.scalar(module.select(module.LocalRestore).where(module.LocalRestore.object_key == logical))
        assert row is not None and module.utc_datetime(row.expires_at) == module.s3_restore_expiry(row.available_at, 2)
        assert module.bucket_dataset_object(bucket, session.get(module.LocalDataset, dataset["id"]), logical)["relative_path"] == "snapshot/a.bin"

        overview = module.list_s3_buckets(admin, session=session)[0]
        assert overview["logical_multiplier"] == 3
        assert overview["physical_objects"] == 2 and overview["logical_objects"] == 6
        assert overview["physical_bytes"] == 7 and overview["logical_bytes"] == 21
    finally:
        session.close()


def test_s3_restore_policy_update_only_affects_future_requests(tmp_path, monkeypatch):
    root = Path(tmp_path) / "payloads"
    package = root / "snapshot"
    package.mkdir(parents=True)
    payload = b"restore-policy"
    (package / "object.bin").write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'policy.db'}")
    monkeypatch.setenv("FUJIN_LOCAL_PAYLOAD_ROOT", str(root))

    import app.fujin_local as module
    module = importlib.reload(module); module.startup(); session = module.SessionLocal()
    try:
        admin = _request("POST", "/api/local")
        dataset = _create_ready_dataset(module, admin, module.DatasetCreate(
            name="policy-dataset", snapshot_id="policy-snapshot", repository_relative_path="snapshot",
            quota_bytes=len(payload), manifest={"objects": [{
                "key": "object.bin", "relative_path": "snapshot/object.bin", "size_bytes": len(payload),
                "sha256": digest, "etag": digest,
            }]},
        ), session)
        created = module.create_s3_bucket(admin, module.S3BucketCreate(
            name="policy-source", dataset_id=dataset["id"], restore_policy={
                "minimum_delay_hours": 2, "random_variation_hours": 0,
                "gradual_window_min_hours": 0, "gradual_window_max_hours": 0,
            },
        ), session)
        bucket = session.get(module.LocalS3Bucket, created["id"])
        original = module.request_restore(
            session, bucket, "already-requested.bin", json.loads(bucket.restore_policy_json), module.utcnow(),
        )
        original_available_at, original_expires_at = original.available_at, original.expires_at
        session.commit()

        changed = module.update_s3_restore_policy(
            bucket.id,
            module.S3RestorePolicyUpdate(restore_policy={
                "minimum_delay_hours": 12, "random_variation_hours": 1,
                "gradual_window_min_hours": 1, "gradual_window_max_hours": 2,
            }),
            _request("PUT", f"/api/local/s3-buckets/{bucket.id}/restore-policy"), session,
        )
        assert changed["changed"] is True
        assert changed["effective_for"] == "future_restore_requests_only"
        preserved = session.get(module.LocalRestore, original.id)
        assert (module.as_utc(preserved.available_at), module.as_utc(preserved.expires_at)) == (
            module.as_utc(original_available_at), module.as_utc(original_expires_at),
        )
        future = module.request_restore(
            session, bucket, "future-request.bin", json.loads(bucket.restore_policy_json), module.utcnow(),
        )
        assert 12 * 3600 <= (module.as_utc(future.available_at) - module.as_utc(future.requested_at)).total_seconds() <= 13 * 3600
        evidence = session.scalar(module.select(module.LocalAuditEvent).where(
            module.LocalAuditEvent.operation == "LOCAL_S3_RESTORE_POLICY_UPDATED"
        ))
        assert evidence and "existing restores retain" in evidence.detail
    finally:
        session.close()


def test_local_s3_restore_batch_and_oci_reference(tmp_path, monkeypatch):
    root = Path(tmp_path) / "payloads"
    relative = "snap/object.bin"
    source = root / relative
    source.parent.mkdir(parents=True)
    payload = b"representative-bytes"
    source.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'local.db'}")
    monkeypatch.setenv("FUJIN_LOCAL_PAYLOAD_ROOT", str(root))
    monkeypatch.setenv("FUJIN_LOCAL_STAGING_ROOT", str(Path(tmp_path) / "staging"))

    import app.fujin_local as module
    module = importlib.reload(module)
    module.startup()
    session = module.SessionLocal()
    try:
        admin_request = _request("POST", "/api/local/datasets")
        dataset = _create_ready_dataset(module, admin_request, module.DatasetCreate(
            name="representative", snapshot_id="snapshot", repository_relative_path="snap", quota_bytes=len(payload),
            manifest={"objects": [{"key": "object.bin", "relative_path": relative, "size_bytes": len(payload),
                                     "sha256": digest, "etag": digest, "storage_class": "DEEP_ARCHIVE", "tags": {"team": "migration"}}]},
        ), session=session)
        created = module.create_s3_bucket(admin_request, module.S3BucketCreate(name="source-local", dataset_id=dataset["id"], restore_policy={"delay_seconds": 0}), session=session)
        bucket = session.get(module.LocalS3Bucket, created["id"])
        headers = [(b"host", b"source-local.vpce-fujin.s3.us-east-1.fujin.internal"),
                   (b"authorization", f"AWS4-HMAC-SHA256 Credential={bucket.access_key_id}/test".encode())]

        restore_body = b"<RestoreRequest><Days>3</Days><GlacierJobParameters><Tier>Bulk</Tier></GlacierJobParameters></RestoreRequest>"
        restore = asyncio.run(module.s3_data_plane(_request("PUT", "/object.bin", headers, b"restore=", restore_body), "object.bin", session))
        assert restore.status_code == 202
        restore_row = session.scalar(module.select(module.LocalRestore).where(module.LocalRestore.object_key == "object.bin"))
        assert module.utc_datetime(restore_row.expires_at) == module.s3_restore_expiry(restore_row.available_at, 3)
        head = asyncio.run(module.s3_data_plane(_request("HEAD", "/object.bin", headers), "object.bin", session))
        assert head.status_code == 200 and "x-amz-restore" in head.headers
        tags = asyncio.run(module.s3_data_plane(_request("GET", "/object.bin", headers, b"tagging="), "object.bin", session))
        assert tags.status_code == 200 and b"team" in tags.body

        control = module.LocalControlObject(bucket_id=bucket.id, object_key="manifest.csv", content=b"source-local,object.bin\n", content_type="text/csv", etag="manifest")
        session.add(control); session.commit()
        job_body = b'{"Manifest":{"Location":{"ObjectArn":"arn:aws:s3:::' + bucket.control_bucket.encode() + b'/manifest.csv"}},"Report":{"Bucket":"arn:aws:s3:::' + bucket.control_bucket.encode() + b'","Prefix":"raikou/reports/wave-1"}}'
        job = asyncio.run(module.s3control_create_job(_request("POST", "/v20180820/jobs", headers, body=job_body), session))
        assert b"<JobId>fujin-job-" in job.body
        report = session.scalar(module.select(module.LocalControlObject).where(module.LocalControlObject.bucket_id == bucket.id, module.LocalControlObject.object_key == "raikou/reports/wave-1/results.csv"))
        assert report and b"succeeded,200" in report.content

        module.create_oci_bucket(admin_request, module.OciBucketCreate(namespace="local", name="destination"), session=session)
        put = asyncio.run(module.oci_put_object("local", "destination", "object.bin", _request("PUT", "/n/local/b/destination/o/object.bin", body=payload), session))
        assert put.status_code == 200
        object_head = module.oci_get_object("local", "destination", "object.bin", _request("HEAD", "/n/local/b/destination/o/object.bin"), session)
        assert object_head.status_code == 200
    finally:
        session.close()


def test_fujin_restore_head_e2e_keeps_per_object_utc_midnight_boundaries(tmp_path, monkeypatch):
    """Exercise restore -> HEAD -> expiry through the Fujin S3 data-plane contract."""
    root = Path(tmp_path) / "payloads"
    (root / "snap").mkdir(parents=True)
    objects = []
    for name in ("before.bin", "after.bin"):
        payload = name.encode("utf-8")
        relative = f"snap/{name}"
        (root / relative).write_bytes(payload)
        objects.append({
            "key": name,
            "relative_path": relative,
            "size_bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "storage_class": "DEEP_ARCHIVE",
        })
    monkeypatch.setenv(
        "FUJIN_LOCAL_DATABASE_URL",
        f"sqlite+pysqlite:///{Path(tmp_path) / 'utc-midnight-e2e.db'}",
    )
    monkeypatch.setenv("FUJIN_LOCAL_PAYLOAD_ROOT", str(root))

    import app.fujin_local as module
    module = importlib.reload(module)
    module.startup()
    session = module.SessionLocal()
    try:
        admin = _request("POST", "/api/local/datasets")
        dataset = _create_ready_dataset(module, admin, module.DatasetCreate(
            name="utc-boundary", snapshot_id="utc-boundary",
            repository_relative_path="snap", quota_bytes=sum(item["size_bytes"] for item in objects),
            manifest={"objects": objects},
        ), session)
        created = module.create_s3_bucket(admin, module.S3BucketCreate(
            name="utc-boundary-source", dataset_id=dataset["id"],
            restore_policy={"delay_seconds": 0},
        ), session)
        bucket = session.get(module.LocalS3Bucket, created["id"])
        headers = [
            (b"host", b"utc-boundary-source.vpce-fujin.s3.us-east-1.fujin.internal"),
            (b"authorization", f"AWS4-HMAC-SHA256 Credential={bucket.access_key_id}/test".encode()),
        ]
        body = b"<RestoreRequest><Days>1</Days><GlacierJobParameters><Tier>Bulk</Tier></GlacierJobParameters></RestoreRequest>"
        instants = {
            "before.bin": datetime(2026, 9, 12, 23, 59, tzinfo=timezone.utc),
            "after.bin": datetime(2026, 9, 13, 0, 1, tzinfo=timezone.utc),
        }
        expected_expiries = {
            "before.bin": datetime(2026, 9, 14, 0, 0, tzinfo=timezone.utc),
            "after.bin": datetime(2026, 9, 15, 0, 0, tzinfo=timezone.utc),
        }
        for name, completed_at in instants.items():
            monkeypatch.setattr(module, "utcnow", lambda value=completed_at: value)
            response = asyncio.run(module.s3_data_plane(
                _request("PUT", f"/{name}", headers, b"restore=", body), name, session,
            ))
            assert response.status_code == 202
            head = asyncio.run(module.s3_data_plane(
                _request("HEAD", f"/{name}", headers), name, session,
            ))
            assert head.status_code == 200
            assert 'ongoing-request="false"' in head.headers["x-amz-restore"]
            row = session.scalar(module.select(module.LocalRestore).where(
                module.LocalRestore.object_key == name
            ))
            assert module.utc_datetime(row.expires_at) == expected_expiries[name]

        monkeypatch.setattr(
            module, "utcnow", lambda: datetime(2026, 9, 14, 0, 0, tzinfo=timezone.utc)
        )
        expired_head = asyncio.run(module.s3_data_plane(
            _request("HEAD", "/before.bin", headers), "before.bin", session,
        ))
        assert "x-amz-restore" not in expired_head.headers
        with pytest.raises(HTTPException, match="InvalidObjectState") as raised:
            asyncio.run(module.s3_data_plane(
                _request("GET", "/before.bin", headers), "before.bin", session,
            ))
        assert raised.value.status_code == 403
        still_available = asyncio.run(module.s3_data_plane(
            _request("HEAD", "/after.bin", headers), "after.bin", session,
        ))
        assert 'ongoing-request="false"' in still_available.headers["x-amz-restore"]
    finally:
        session.close()


def test_local_dataset_deletion_is_protected_by_s3_references(tmp_path, monkeypatch):
    root = Path(tmp_path) / "payloads"; (root / "snap").mkdir(parents=True)
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'lifecycle.db'}")
    monkeypatch.setenv("FUJIN_LOCAL_PAYLOAD_ROOT", str(root))
    import app.fujin_local as module
    module = importlib.reload(module); module.startup(); session = module.SessionLocal()
    try:
        request = _request("POST", "/api/local/datasets")
        dataset = _create_ready_dataset(module, request, module.DatasetCreate(name="protected", snapshot_id="protected-snapshot", repository_relative_path="snap", quota_bytes=0), session=session)
        module.create_s3_bucket(request, module.S3BucketCreate(name="protected-source", dataset_id=dataset["id"]), session=session)
        try:
            module.delete_dataset(dataset["id"], _request("DELETE", "/api/local/datasets/id"), session)
        except HTTPException as error:
            assert error.status_code == 409
        else:
            raise AssertionError("dataset with an active S3 reference was deleted")
    finally:
        session.close()


def test_local_dataset_can_be_deleted_after_s3_revocation(tmp_path, monkeypatch):
    root = Path(tmp_path) / "payloads"; (root / "snap").mkdir(parents=True)
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'revocation.db'}")
    monkeypatch.setenv("FUJIN_LOCAL_PAYLOAD_ROOT", str(root))
    import app.fujin_local as module
    module = importlib.reload(module); module.startup(); session = module.SessionLocal()
    try:
        request = _request("POST", "/api/local/datasets")
        dataset = _create_ready_dataset(module, request, module.DatasetCreate(name="releasable", snapshot_id="releasable-snapshot", repository_relative_path="snap", quota_bytes=0), session=session)
        bucket = module.create_s3_bucket(request, module.S3BucketCreate(name="releasable-source", dataset_id=dataset["id"]), session=session)
        # No restore was requested, so revocation is immediately safe.
        assert module.delete_s3_bucket(bucket["id"], _request("DELETE", "/api/local/s3-buckets/id"), session).status_code == 204
        assert module.delete_dataset(dataset["id"], _request("DELETE", "/api/local/datasets/id"), session).status_code == 204
    finally:
        session.close()


def test_deleted_s3_bucket_name_can_be_recreated_with_new_projection(tmp_path, monkeypatch):
    root = Path(tmp_path) / "payloads"; (root / "snap").mkdir(parents=True)
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'recreate.db'}")
    monkeypatch.setenv("FUJIN_LOCAL_PAYLOAD_ROOT", str(root))
    import app.fujin_local as module
    module = importlib.reload(module); module.startup(); session = module.SessionLocal()
    try:
        request = _request("POST", "/api/local")
        dataset = _create_ready_dataset(module, request, module.DatasetCreate(
            name="recreate", snapshot_id="recreate-snapshot", repository_relative_path="snap", quota_bytes=0,
        ), session)
        original = module.create_s3_bucket(request, module.S3BucketCreate(
            name="recreated-source", dataset_id=dataset["id"], logical_multiplier=1,
        ), session)
        assert module.delete_s3_bucket(original["id"], _request("DELETE", "/api/local/s3-buckets/id"), session).status_code == 204
        historical = session.get(module.LocalS3Bucket, original["id"])
        assert historical.deleted_at is not None and historical.name.startswith("recreated-source-deleted-")

        recreated = module.create_s3_bucket(request, module.S3BucketCreate(
            name="recreated-source", dataset_id=dataset["id"], logical_multiplier=11,
        ), session)
        current = session.get(module.LocalS3Bucket, recreated["id"])
        assert recreated["id"] != original["id"]
        assert current.name == "recreated-source" and current.logical_multiplier == 11
        assert current.access_key_id != historical.access_key_id
    finally:
        session.close()


def test_user_friendly_local_generation_builds_metadata_and_validates(tmp_path, monkeypatch):
    root = Path(tmp_path) / "payloads"
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'package-flow.db'}")
    monkeypatch.setenv("FUJIN_LOCAL_PAYLOAD_ROOT", str(root))
    import app.fujin_local as module
    from app.fujin_local_materializer import process_generation_jobs
    module = importlib.reload(module); module.startup(); session = module.SessionLocal()
    try:
        request = _request("POST", "/api/local/datasets/generate")
        created = module.generate_dataset(
            request, module.DatasetGenerateCreate(
                name="Friendly package", quota_bytes=11, seed="local-test",
                profile={"files": [{"name": "first.bin", "size_bytes": 5}, {"name": "second.bin", "size_bytes": 6}]},
            ), session,
        )
        assert created["state"] == "GENERATING" and created["files"] == 2 and created["bytes"] == 11
        database_url = module.DATABASE_URL
        process_generation_jobs(database_url, root=root, max_files=1)
        process_generation_jobs(database_url, root=root, max_files=1)
        process_generation_jobs(database_url, root=root, max_files=1)
        session.expire_all()
        dataset = session.get(module.LocalDataset, created["id"])
        manifest = json.loads(dataset.manifest_json)
        assert dataset.state == "READY" and dataset.quota_bytes == 11
        assert dataset.validated_objects == 2 and dataset.validated_bytes == 11
        assert len(dataset.snapshot_id) == 64 and len(manifest["objects"]) == 2
    finally:
        session.close()


def test_local_s3_bucket_can_be_disabled_enabled_and_deleted(tmp_path, monkeypatch):
    root = Path(tmp_path) / "payloads"; (root / "snap").mkdir(parents=True)
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'bucket-state.db'}")
    monkeypatch.setenv("FUJIN_LOCAL_PAYLOAD_ROOT", str(root))
    import app.fujin_local as module
    module = importlib.reload(module); module.startup(); session = module.SessionLocal()
    try:
        request = _request("POST", "/api/local/datasets")
        dataset = _create_ready_dataset(
            module, request,
            module.DatasetCreate(name="stateful", snapshot_id="stateful-snapshot",
                                 repository_relative_path="snap", quota_bytes=0),
            session=session,
        )
        created = module.create_s3_bucket(
            request, module.S3BucketCreate(name="stateful-source", dataset_id=dataset["id"]),
            session=session,
        )

        disabled = module.disable_s3_bucket(
            created["id"], _request("POST", "/api/local/s3-buckets/id/disable"), session
        )
        assert disabled["active"] is False
        listed = module.list_s3_buckets(_request("GET", "/api/local/s3-buckets"), session)
        assert listed[0]["dataset_name"] == "stateful"
        assert listed[0]["dataset_state"] == "READY"
        assert listed[0]["storage_class"] == "DEEP_ARCHIVE"
        assert listed[0]["active"] is False

        enabled = module.enable_s3_bucket(
            created["id"], _request("POST", "/api/local/s3-buckets/id/enable"), session
        )
        assert enabled["active"] is True

        assert module.delete_s3_bucket(
            created["id"], _request("DELETE", "/api/local/s3-buckets/id"), session
        ).status_code == 204
        assert module.list_s3_buckets(_request("GET", "/api/local/s3-buckets"), session) == []
        assert module.delete_dataset(
            dataset["id"], _request("DELETE", "/api/local/datasets/id"), session
        ).status_code == 204
    finally:
        session.close()


def test_bucket_storage_class_overrides_legacy_dataset_tier(tmp_path, monkeypatch):
    root = Path(tmp_path) / "payloads"; (root / "snap").mkdir(parents=True)
    payload = b"tier-neutral"; (root / "snap/object.bin").write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'tier.db'}")
    monkeypatch.setenv("FUJIN_LOCAL_PAYLOAD_ROOT", str(root))
    import app.fujin_local as module
    module = importlib.reload(module); module.startup(); session = module.SessionLocal()
    try:
        request = _request("POST", "/api/local/datasets")
        dataset = _create_ready_dataset(module, request, module.DatasetCreate(
            name="neutral", snapshot_id="neutral-snapshot", repository_relative_path="snap", quota_bytes=len(payload),
            manifest={"objects":[{"key":"object.bin", "relative_path":"snap/object.bin",
                                  "size_bytes":len(payload), "sha256":digest,
                                  "storage_class":"DEEP_ARCHIVE"}]},
        ), session)
        created = module.create_s3_bucket(request, module.S3BucketCreate(
            name="standard-projection", dataset_id=dataset["id"], storage_class="STANDARD"
        ), session)
        bucket = session.get(module.LocalS3Bucket, created["id"])
        assert module.bucket_storage_class(bucket, {"storage_class": "DEEP_ARCHIVE"}) == "STANDARD"
        headers = [(b"host", b"standard-projection.vpce-fujin.s3.us-east-1.fujin.internal"),
                   (b"authorization", f"AWS4-HMAC-SHA256 Credential={bucket.access_key_id}/test".encode())]
        response = asyncio.run(module.s3_data_plane(
            _request("HEAD", "/object.bin", headers), "object.bin", session
        ))
        assert response.headers["x-amz-storage-class"] == "STANDARD"
        assert "x-amz-restore" not in response.headers
    finally:
        session.close()


def test_validated_simulation_dataset_import_is_zero_copy_and_tier_neutral(tmp_path):
    from app.simulation_schema import FujinPayloadDataset, FujinPayloadFile, SimulationBase
    from app.fujin_local_schema import LocalBase, LocalDataset

    root = Path(tmp_path) / "payloads"
    relative = Path("datasets/source-id/aa/bb/00000001-payload.bin")
    physical = root / relative; physical.parent.mkdir(parents=True)
    payload = b"already-validated-physical-bytes"; physical.write_bytes(payload)
    stat_result = physical.stat(); digest = hashlib.sha256(payload).hexdigest()
    source_engine = create_engine(f"sqlite+pysqlite:///{Path(tmp_path) / 'simulation.db'}")
    local_engine = create_engine(f"sqlite+pysqlite:///{Path(tmp_path) / 'local-import.db'}")
    SimulationBase.metadata.create_all(source_engine); LocalBase.metadata.create_all(local_engine)
    source_file = FujinPayloadFile(
        dataset_id="source-id", relative_path=relative.as_posix(), size_bytes=len(payload),
        sha256=digest, mtime_ns=stat_result.st_mtime_ns,
        identity=f"{stat_result.st_dev}:{stat_result.st_ino}",
    )
    source_manifest = [{"path": source_file.relative_path, "size_bytes": source_file.size_bytes,
                        "sha256": source_file.sha256}]
    manifest_sha = hashlib.sha256(json.dumps(
        source_manifest, sort_keys=True, separators=(",", ":")
    ).encode()).hexdigest()
    with Session(source_engine) as session:
        session.add(FujinPayloadDataset(
            id="source-id", name="simulation-10tb", state="READY", model="HYBRID",
            repository_relative_path="datasets/source-id", quota_bytes=len(payload), seed="seed",
            profile_json="{}", manifest_sha256=manifest_sha, physical_bytes=len(payload), files_total=1,
            last_validation_state="VALID", last_validated_at=datetime.now(timezone.utc),
        ))
        session.add(source_file); session.commit()
    spec = importlib.util.spec_from_file_location(
        "import_simulation_dataset", "scripts/import-simulation-dataset-to-fujin-local.py"
    )
    importer = importlib.util.module_from_spec(spec); spec.loader.exec_module(importer)
    imported = importer.import_dataset(source_engine, local_engine, root, "source-id", "local-representative")
    assert imported.state == "READY" and imported.model == "REPRESENTATIVE"
    assert imported.validated_bytes == len(payload) and physical.read_bytes() == payload
    with Session(local_engine) as session:
        row = session.get(LocalDataset, imported.id); manifest = json.loads(row.manifest_json)
        assert manifest["provenance"]["simulation_dataset_id"] == "source-id"
        assert "storage_class" not in manifest["objects"][0]


def test_deleting_oci_bucket_releases_only_its_referential_objects(tmp_path, monkeypatch):
    root = Path(tmp_path) / "payloads"; (root / "snap").mkdir(parents=True)
    payload = b"referential-oci"; (root / "snap/object.bin").write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'oci-lifecycle.db'}")
    monkeypatch.setenv("FUJIN_LOCAL_PAYLOAD_ROOT", str(root))
    monkeypatch.setenv("FUJIN_LOCAL_STAGING_ROOT", str(Path(tmp_path) / "staging"))
    import app.fujin_local as module
    module = importlib.reload(module); module.startup(); session = module.SessionLocal()
    try:
        request = _request("POST", "/api/local")
        dataset = _create_ready_dataset(module, request, module.DatasetCreate(name="oci-releasable", snapshot_id="oci-releasable-snapshot", repository_relative_path="snap", quota_bytes=len(payload), manifest={"objects":[{"key":"object.bin","relative_path":"snap/object.bin","size_bytes":len(payload),"sha256":digest,"etag":digest}]}), session)
        destination = module.create_oci_bucket(request, module.OciBucketCreate(namespace="local", name="release-target"), session)
        connection = module.oci_connection(destination["id"], request, session)
        assert connection["connection"] == {
            "object_storage_namespace": "local", "destination_bucket": "release-target",
            "region": "sa-saopaulo-1", "object_storage_endpoint_url": "https://oci.sa-saopaulo-1.fujin.internal",
            "object_storage_ca_bundle_path": "/etc/fujin-local-ca/fujin-local.crt",
        }
        assert module.disable_oci_bucket(destination["id"], request, session)["active"] is False
        assert module.list_oci_buckets(request, session)[0]["active"] is False
        assert module.enable_oci_bucket(destination["id"], request, session)["active"] is True
        assert asyncio.run(module.oci_put_object("local", "release-target", "object.bin", _request("PUT", "/", body=payload), session)).status_code == 200
        assert session.query(module.LocalOciObject).count() == 1
        assert module.delete_oci_bucket(destination["id"], _request("DELETE", "/api/local/oci-buckets/id"), session).status_code == 204
        assert session.query(module.LocalOciObject).count() == 0
        assert module.delete_dataset(dataset["id"], _request("DELETE", "/api/local/datasets/id"), session).status_code == 204
    finally:
        session.close()


def test_representative_dataset_validation_records_physical_evidence(tmp_path, monkeypatch):
    root = Path(tmp_path) / "payloads"; (root / "snap").mkdir(parents=True)
    payload = b"validated-representative"; (root / "snap/object.bin").write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'validation.db'}")
    monkeypatch.setenv("FUJIN_LOCAL_PAYLOAD_ROOT", str(root))
    import app.fujin_local as module
    module = importlib.reload(module); module.startup(); session = module.SessionLocal()
    try:
        request = _request("POST", "/api/local/datasets")
        seeded = module.LocalDataset(name="validated", snapshot_id="validated-snapshot", state="PENDING_VALIDATION", repository_relative_path="snap", quota_bytes=len(payload), manifest_json=json.dumps({"objects":[{"key":"object.bin","relative_path":"snap/object.bin","size_bytes":len(payload),"sha256":digest}]}))
        session.add(seeded); session.commit()
        dataset = {"id": seeded.id, "state": seeded.state}
        before = module.dataset_details(dataset["id"], request, session)
        assert before["state"] == "PENDING_VALIDATION" and before["manifest_objects"] == 1
        assert "manifest" not in before and before["repository_relative_path"] == "snap"
        assert dataset["state"] == "PENDING_VALIDATION"
        try:
            module.create_s3_bucket(request, module.S3BucketCreate(name="unvalidated-source", dataset_id=dataset["id"]), session)
        except HTTPException as error:
            assert error.status_code == 422
        else:
            raise AssertionError("unvalidated dataset was accepted by a LOCAL S3 bucket")
        result = module.validate_dataset(dataset["id"], _request("POST", "/api/local/datasets/id/validate"), session)
        assert result["state"] == "READY" and result["objects"] == 1 and result["bytes"] == len(payload)
        after = module.dataset_details(dataset["id"], request, session)
        assert after["state"] == "READY" and after["validated_objects"] == 1
        assert after["validated_bytes"] == len(payload)
        (root / "snap/object.bin").write_bytes(b"tampered")
        try:
            module.validate_dataset(dataset["id"], _request("POST", "/api/local/datasets/id/validate"), session)
        except HTTPException as error:
            assert error.status_code == 422
        else:
            raise AssertionError("tampered representative payload was accepted")
        assert session.get(module.LocalDataset, dataset["id"]).state == "INVALID"
    finally:
        session.close()


def test_oci_reference_deep_audit_and_expired_state_gc(tmp_path, monkeypatch):
    root = Path(tmp_path) / "payloads"; (root / "snap").mkdir(parents=True)
    payload = b"deep-audit-reference"; path = root / "snap/object.bin"; path.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'deep-audit.db'}")
    monkeypatch.setenv("FUJIN_LOCAL_PAYLOAD_ROOT", str(root)); monkeypatch.setenv("FUJIN_LOCAL_STAGING_ROOT", str(Path(tmp_path) / "staging"))
    import app.fujin_local as module
    module = importlib.reload(module); module.startup(); session = module.SessionLocal()
    try:
        request = _request("POST", "/api/local")
        dataset = _create_ready_dataset(module, request, module.DatasetCreate(name="audit-dataset", snapshot_id="audit-snapshot", repository_relative_path="snap", quota_bytes=len(payload), manifest={"objects":[{"key":"object.bin","relative_path":"snap/object.bin","size_bytes":len(payload),"sha256":digest}]}), session)
        destination = module.create_oci_bucket(request, module.OciBucketCreate(namespace="local", name="audit-target"), session)
        assert asyncio.run(module.oci_put_object("local", "audit-target", "object.bin", _request("PUT", "/", body=payload), session)).status_code == 200
        audited = module.deep_audit_oci_reference(destination["id"], "object.bin", request, session)
        assert audited["verified"] is True and audited["sha256"] == digest
        upload_id = "expired-upload"; directory = module.STAGING_ROOT / f"multipart/{upload_id}"; directory.mkdir(parents=True)
        bucket = session.get(module.LocalOciBucket, destination["id"])
        session.add(module.LocalMultipartUpload(id=upload_id, bucket_id=bucket.id, object_key="expired", staging_relative_path=f"multipart/{upload_id}", expires_at=module.utcnow() - module.timedelta(seconds=1)))
        session.commit()
        gc = module.garbage_collect_local(request, session)
        assert gc["discarded_multipart_uploads"] == 1 and not directory.exists()
    finally:
        session.close()


def test_aborted_multipart_leaves_no_oci_reference_or_cache(tmp_path, monkeypatch):
    root = Path(tmp_path) / "payloads"; staging = Path(tmp_path) / "staging"
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'abort.db'}")
    monkeypatch.setenv("FUJIN_LOCAL_PAYLOAD_ROOT", str(root)); monkeypatch.setenv("FUJIN_LOCAL_STAGING_ROOT", str(staging))
    import app.fujin_local as module
    module = importlib.reload(module); module.startup(); session = module.SessionLocal()
    try:
        request = _request("POST", "/api/local/oci-buckets")
        module.create_oci_bucket(request, module.OciBucketCreate(namespace="local", name="abort-target"), session=session)
        created = asyncio.run(module.oci_create_multipart("local", "abort-target", _request("POST", "/n/local/b/abort-target/u", body=b'{"object":"discard.bin"}'), session))
        upload_id = created["uploadId"]
        uploaded = asyncio.run(module.oci_upload_part("local", "abort-target", "discard.bin", _request("PUT", "/", body=b"discard"), upload_id, 1, session))
        assert uploaded.status_code == 200
        assert not (staging / f"multipart/{upload_id}").exists()
        assert module.oci_abort_multipart("local", "abort-target", "discard.bin", _request("DELETE", "/"), upload_id, session).status_code == 204
        assert not (staging / f"multipart/{upload_id}").exists()
        assert not session.query(module.LocalOciObject).count()
    finally:
        session.close()


def test_active_multipart_upload_renews_its_idle_expiry(tmp_path, monkeypatch):
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'multipart-idle.db'}")
    import app.fujin_local as module
    module = importlib.reload(module); module.startup(); session = module.SessionLocal()
    try:
        bucket = module.LocalOciBucket(namespace="local", name="multipart-idle-target")
        session.add(bucket); session.flush()
        reference = datetime(2026, 9, 15, 12, tzinfo=timezone.utc)
        upload = module.LocalMultipartUpload(
            id="active-upload", bucket_id=bucket.id, object_key="large.bin",
            staging_relative_path="multipart/active-upload", created_at=reference - timedelta(hours=23),
            last_activity_at=reference - timedelta(hours=23),
            expires_at=reference + timedelta(hours=1),
        )
        session.add(upload); session.flush()
        module.touch_multipart_upload(upload, reference)
        assert module.as_utc(upload.last_activity_at) == reference
        assert module.as_utc(upload.expires_at) == reference + timedelta(hours=module.FUJIN_LOCAL_MULTIPART_IDLE_TTL_HOURS)
        assert module.cleanup_expired_multipart_uploads(session, reference + timedelta(hours=1)) == 0
    finally:
        session.close()


def test_oci_uploads_keep_only_evidence_and_reference_original_payload(tmp_path, monkeypatch):
    root = Path(tmp_path) / "payloads"; staging = Path(tmp_path) / "staging"
    payload = b"reference-only-multipart-payload"
    (root / "snap").mkdir(parents=True); (root / "snap/object.bin").write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'reference-only.db'}")
    monkeypatch.setenv("FUJIN_LOCAL_PAYLOAD_ROOT", str(root)); monkeypatch.setenv("FUJIN_LOCAL_STAGING_ROOT", str(staging))
    import app.fujin_local as module
    module = importlib.reload(module); module.startup(); session = module.SessionLocal()
    try:
        request = _request("POST", "/api/local")
        dataset = _create_ready_dataset(module, request, module.DatasetCreate(
            name="reference-only", snapshot_id="reference-only-snapshot", repository_relative_path="snap",
            quota_bytes=len(payload), manifest={"objects": [{"key": "object.bin", "relative_path": "snap/object.bin",
                "size_bytes": len(payload), "sha256": digest, "etag": digest}]},
        ), session)
        module.create_s3_bucket(request, module.S3BucketCreate(name="reference-source", dataset_id=dataset["id"]), session)
        module.create_oci_bucket(request, module.OciBucketCreate(namespace="local", name="reference-target"), session)

        direct = asyncio.run(module.oci_put_object("local", "reference-target", "direct.bin", _request("PUT", "/", body=payload), session))
        assert direct.status_code == 200
        assert not staging.exists() or not any(staging.rglob("*.upload"))

        metadata = {"opc-meta-raijin-source-etag": digest}
        created = asyncio.run(module.oci_create_multipart(
            "local", "reference-target", _request("POST", "/", body=json.dumps({"object": "object.bin", "metadata": metadata}).encode()), session,
        ))
        upload_id = created["uploadId"]
        split = 11
        for number, part in enumerate((payload[:split], payload[split:]), start=1):
            result = asyncio.run(module.oci_upload_part(
                "local", "reference-target", "object.bin", _request("PUT", "/", body=part), upload_id, number, session,
            ))
            assert result.status_code == 200
        assert not staging.exists() or not any(staging.rglob("*.part"))
        assert session.query(module.LocalMultipartPart).count() == 2
        assert all(part.source_verified for part in session.query(module.LocalMultipartPart))

        monkeypatch.setattr(module, "source_reference_for_multipart", lambda *_args: (_ for _ in ()).throw(AssertionError("commit reread source")))
        committed = module.oci_commit_multipart("local", "reference-target", "object.bin", _request("POST", "/"), upload_id, session)
        assert committed.status_code == 200
        reference = session.query(module.LocalOciObject).filter(module.LocalOciObject.object_key == "object.bin").one()
        assert reference.dataset_id == dataset["id"]
        assert reference.source_relative_path == "snap/object.bin"
        assert session.query(module.LocalMultipartPart).count() == 0
        assert session.query(module.LocalMultipartUpload).count() == 0
        assert not staging.exists() or not any(staging.rglob("*"))
    finally:
        session.close()


def test_oci_list_paginates_and_head_returns_persisted_provenance(tmp_path, monkeypatch):
    root = Path(tmp_path) / "payloads"; (root / "snap").mkdir(parents=True)
    payload = b"pagination-provenance"; (root / "snap/object.bin").write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'pagination.db'}")
    monkeypatch.setenv("FUJIN_LOCAL_PAYLOAD_ROOT", str(root))
    import app.fujin_local as module
    module = importlib.reload(module); module.startup(); session = module.SessionLocal()
    try:
        request = _request("POST", "/api/local")
        dataset = _create_ready_dataset(module, request, module.DatasetCreate(
            name="pagination", snapshot_id="pagination-snapshot", repository_relative_path="snap",
            quota_bytes=len(payload), manifest={"objects": [{"key": "object.bin",
                "relative_path": "snap/object.bin", "size_bytes": len(payload),
                "sha256": digest, "etag": digest}]},
        ), session)
        module.create_s3_bucket(request, module.S3BucketCreate(name="pagination-source", dataset_id=dataset["id"]), session)
        module.create_oci_bucket(request, module.OciBucketCreate(namespace="local", name="pagination-target"), session)
        metadata = [(b"opc-meta-raijin-source-etag", digest.encode()),
                    (b"opc-meta-raijin-source-last-modified", b"2026-09-25T12:00:00+00:00")]
        for key in ("a.bin", "b.bin", "c.bin"):
            assert asyncio.run(module.oci_put_object("local", "pagination-target", key,
                _request("PUT", "/", headers=metadata, body=payload), session)).status_code == 200
        # Multipart records produced by older Fujin releases persisted the
        # complete opc-meta-* key rather than only its suffix.
        legacy = session.scalar(select(module.LocalOciObject).where(
            module.LocalOciObject.object_key == "c.bin"))
        legacy.metadata_json = json.dumps({
            "opc-meta-raijin-source-etag": digest,
            "opc-meta-raijin-source-last-modified": "2026-09-25T12:00:00+00:00",
        })
        session.commit()
        first = module.oci_list_objects("local", "pagination-target",
            _request("GET", "/", query=b"limit=2"), session)
        assert [item["name"] for item in first["objects"]] == ["a.bin", "b.bin"]
        assert first["nextStartWith"] == "b.bin"
        second = module.oci_list_objects("local", "pagination-target",
            _request("GET", "/", query=b"limit=2&start=b.bin"), session)
        assert [item["name"] for item in second["objects"]] == ["c.bin"]
        assert second["nextStartWith"] is None
        head = module.oci_get_object("local", "pagination-target", "a.bin",
            _request("HEAD", "/"), session)
        assert head.headers["opc-meta-raijin-source-etag"] == digest
        assert head.headers["opc-meta-raijin-source-last-modified"] == "2026-09-25T12:00:00+00:00"
        legacy_head = module.oci_get_object("local", "pagination-target", "c.bin",
            _request("HEAD", "/"), session)
        assert legacy_head.headers["opc-meta-raijin-source-etag"] == digest
        assert "opc-meta-opc-meta-raijin-source-etag" not in legacy_head.headers
    finally:
        session.close()


def test_multipart_commit_rejects_evidence_that_differs_from_source(tmp_path, monkeypatch):
    root = Path(tmp_path) / "payloads"; payload = b"immutable-source"
    (root / "snap").mkdir(parents=True); (root / "snap/object.bin").write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'mismatch.db'}")
    monkeypatch.setenv("FUJIN_LOCAL_PAYLOAD_ROOT", str(root)); monkeypatch.setenv("FUJIN_LOCAL_STAGING_ROOT", str(Path(tmp_path) / "staging"))
    import app.fujin_local as module
    module = importlib.reload(module); module.startup(); session = module.SessionLocal()
    try:
        request = _request("POST", "/api/local")
        dataset = _create_ready_dataset(module, request, module.DatasetCreate(
            name="mismatch", snapshot_id="mismatch-snapshot", repository_relative_path="snap", quota_bytes=len(payload),
            manifest={"objects": [{"key": "object.bin", "relative_path": "snap/object.bin", "size_bytes": len(payload), "sha256": digest, "etag": digest}]},
        ), session)
        module.create_s3_bucket(request, module.S3BucketCreate(name="mismatch-source", dataset_id=dataset["id"]), session)
        module.create_oci_bucket(request, module.OciBucketCreate(namespace="local", name="mismatch-target"), session)
        created = asyncio.run(module.oci_create_multipart("local", "mismatch-target", _request("POST", "/", body=b'{"object":"object.bin"}'), session))
        upload_id = created["uploadId"]
        asyncio.run(module.oci_upload_part("local", "mismatch-target", "object.bin", _request("PUT", "/", body=b"X" * len(payload)), upload_id, 1, session))
        with pytest.raises(HTTPException) as rejected:
            module.oci_commit_multipart("local", "mismatch-target", "object.bin", _request("POST", "/"), upload_id, session)
        assert rejected.value.status_code == 422
        assert session.query(module.LocalOciObject).count() == 0
    finally:
        session.close()


def test_oci_bucket_creation_rejects_duplicate_name_in_namespace(tmp_path, monkeypatch):
    monkeypatch.setenv(
        "FUJIN_LOCAL_DATABASE_URL",
        f"sqlite+pysqlite:///{Path(tmp_path) / 'oci-duplicate.db'}",
    )
    import app.fujin_local as module
    module = importlib.reload(module); module.startup()
    with module.SessionLocal() as session:
        request = _request("POST", "/api/local/oci-buckets")
        payload = module.OciBucketCreate(namespace="local", name="destination")
        module.create_oci_bucket(request, payload, session)
        with pytest.raises(HTTPException) as duplicate:
            module.create_oci_bucket(request, payload, session)
        assert duplicate.value.status_code == 409


def test_s3_local_rejects_a_region_without_a_private_tls_profile(tmp_path, monkeypatch):
    root = Path(tmp_path) / "payloads"; (root / "snap").mkdir(parents=True)
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'region.db'}")
    monkeypatch.setenv("FUJIN_LOCAL_PAYLOAD_ROOT", str(root)); monkeypatch.setenv("FUJIN_LOCAL_REGION", "us-east-1")
    import app.fujin_local as module
    module = importlib.reload(module); module.startup(); session = module.SessionLocal()
    try:
        dataset = _create_ready_dataset(module, _request("POST", "/api/local"), module.DatasetCreate(name="region-dataset", snapshot_id="region-snapshot", repository_relative_path="snap", quota_bytes=0), session)
        try:
            module.create_s3_bucket(_request("POST", "/api/local"), module.S3BucketCreate(name="wrong-region", dataset_id=dataset["id"], region="eu-west-1"), session)
        except HTTPException as error:
            assert error.status_code == 422 and "us-east-1" in error.detail
        else:
            raise AssertionError("S3 LOCAL accepted a region absent from its TLS profile")
    finally:
        session.close()


def test_restore_policy_randomizes_batch_start_then_distributes_availability(tmp_path, monkeypatch):
    root = Path(tmp_path) / "payloads"; (root / "snap").mkdir(parents=True)
    payload = b"restore-policy"; (root / "snap/a.bin").write_bytes(payload); (root / "snap/b.bin").write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'restore-policy.db'}")
    monkeypatch.setenv("FUJIN_LOCAL_PAYLOAD_ROOT", str(root))
    import app.fujin_local as module
    module = importlib.reload(module); module.startup(); session = module.SessionLocal()
    try:
        request = _request("POST", "/api/local")
        dataset = _create_ready_dataset(module, request, module.DatasetCreate(name="restore-dataset", snapshot_id="restore-policy", repository_relative_path="snap", quota_bytes=2 * len(payload), manifest={"objects":[
            {"key":"a.bin","relative_path":"snap/a.bin","size_bytes":len(payload),"sha256":digest,"storage_class":"GLACIER"},
            {"key":"b.bin","relative_path":"snap/b.bin","size_bytes":len(payload),"sha256":digest,"storage_class":"GLACIER"},
        ]}), session)
        policy = {
            "minimum_delay_hours": 36,
            "random_variation_hours": 3,
            "gradual_window_min_hours": 1,
            "gradual_window_max_hours": 3,
        }
        created = module.create_s3_bucket(request, module.S3BucketCreate(
            name="restore-policy-source", dataset_id=dataset["id"], restore_policy=policy
        ), session)
        bucket = session.get(module.LocalS3Bucket, created["id"])
        now = module.utcnow()
        random_values = iter((3600, 5520))
        monkeypatch.setattr(module.secrets, "randbelow", lambda upper: next(random_values))
        operation_start = module.restore_delay_seconds(policy)
        gradual_window = module.restore_gradual_window_seconds(policy)
        assert gradual_window == 9120  # 2h32 between the configured 1h and 3h.
        first = module.request_restore(
            session, bucket, "a.bin", policy, now, retention_days=5,
            availability_delay_seconds=operation_start + module.gradual_restore_offset_seconds(gradual_window, 0, 2),
        )
        last = module.request_restore(
            session, bucket, "b.bin", policy, now, retention_days=2,
            availability_delay_seconds=operation_start + module.gradual_restore_offset_seconds(gradual_window, 1, 2),
        )
        assert first.state == "IN_PROGRESS" and first.available_at == now + timedelta(hours=37)
        assert last.available_at == now + timedelta(hours=39, minutes=32)
        assert module.utc_datetime(first.expires_at) == module.s3_restore_expiry(first.available_at, 5)
        assert module.utc_datetime(last.expires_at) == module.s3_restore_expiry(last.available_at, 2)
        with pytest.raises(HTTPException, match="RestoreAlreadyInProgress"):
            module.request_restore(
                session, bucket, "a.bin", policy, now, retention_days=5,
                availability_delay_seconds=operation_start, restore_tier="BULK",
            )
        with pytest.raises(HTTPException, match="retention cannot change"):
            module.request_restore(
                session, bucket, "a.bin", policy, now, retention_days=6,
                availability_delay_seconds=operation_start, restore_tier="STANDARD",
            )
        upgraded = module.request_restore(
            session, bucket, "a.bin", policy, now, retention_days=5,
            availability_delay_seconds=operation_start, restore_tier="STANDARD",
        )
        assert upgraded.id == first.id and upgraded.restore_tier == "STANDARD"
        third = module.request_restore(
            session, bucket, "third.bin", {"max_concurrent_restores": 1}, now
        )
        assert third.state == "AVAILABLE"
        extended_at = now + timedelta(hours=1)
        extended = module.request_restore(
            session, bucket, "third.bin", {}, extended_at, retention_days=3,
            restore_tier="BULK",
        )
        assert extended.id == third.id
        assert module.utc_datetime(extended.expiry_basis_at) == extended_at
        assert module.utc_datetime(extended.expires_at) == module.s3_restore_expiry(extended_at, 3)
    finally:
        session.close()


def test_restore_availability_window_cannot_exceed_48_hours():
    import app.fujin_local as module
    normalized = module.normalized_restore_policy({
        "minimum_delay_hours": 36,
        "random_variation_hours": 3,
        "gradual_window_min_hours": 1,
        "gradual_window_max_hours": 3,
    })
    assert normalized == {
        "minimum_delay_hours": 36,
        "random_variation_hours": 3,
        "gradual_window_min_hours": 1,
        "gradual_window_max_hours": 3,
    }
    legacy = module.normalized_restore_policy({"delay_hours": 36, "gradual_window_hours": 2})
    assert legacy == {
        "minimum_delay_hours": 36,
        "random_variation_hours": 0,
        "gradual_window_min_hours": 2,
        "gradual_window_max_hours": 2,
    }
    with pytest.raises(ValueError, match="48 hours"):
        module.normalized_restore_policy({
            "minimum_delay_hours": 44,
            "random_variation_hours": 3,
            "gradual_window_min_hours": 1,
            "gradual_window_max_hours": 2,
        })
    with pytest.raises(ValueError, match="Minimum gradual"):
        module.normalized_restore_policy({
            "gradual_window_min_hours": 3,
            "gradual_window_max_hours": 1,
        })


def test_local_restore_survives_fujin_process_reload(tmp_path, monkeypatch):
    root = Path(tmp_path) / "payloads"; (root / "snap").mkdir(parents=True)
    payload = b"restart-restore"; (root / "snap/object.bin").write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    database = Path(tmp_path) / "restart.db"
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{database}")
    monkeypatch.setenv("FUJIN_LOCAL_PAYLOAD_ROOT", str(root))
    import app.fujin_local as module
    module = importlib.reload(module); module.startup(); session = module.SessionLocal()
    try:
        request = _request("POST", "/api/local")
        dataset = _create_ready_dataset(module, request, module.DatasetCreate(name="restart-dataset", snapshot_id="restart-snapshot", repository_relative_path="snap", quota_bytes=len(payload), manifest={"objects":[{"key":"object.bin","relative_path":"snap/object.bin","size_bytes":len(payload),"sha256":digest,"storage_class":"GLACIER"}]}), session)
        created = module.create_s3_bucket(request, module.S3BucketCreate(name="restart-source", dataset_id=dataset["id"], restore_policy={"delay_seconds": 3600}), session)
        bucket = session.get(module.LocalS3Bucket, created["id"])
        restore = module.request_restore(session, bucket, "object.bin", {"delay_seconds": 3600}, module.utcnow())
        session.commit(); restore_id = restore.id
    finally:
        session.close()
    module = importlib.reload(module); module.startup()
    with module.SessionLocal() as reloaded:
        restore = reloaded.get(module.LocalRestore, restore_id)
        assert restore and restore.state == "IN_PROGRESS" and restore.object_key == "object.bin"


def test_local_data_plane_requires_explicit_fujin_activation(tmp_path, monkeypatch):
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'activation.db'}")
    import app.fujin_local as module
    module = importlib.reload(module); module.startup()

    async def next_handler(_request):
        from fastapi.responses import Response
        return Response(status_code=299)

    async def exercise():
        disabled = await module.local_data_plane_guard(_request("GET", "/a-bucket/a-key"), next_handler)
        assert disabled.status_code == 503
        assert disabled.headers["x-fujin-request-id"].startswith("fujin-")
        # Administrative UI assets are independent from both cloud data
        # planes; disabling AWS must never strip the Fujin shell styling.
        assert (await module.local_data_plane_guard(
            _request("GET", "/static/operational-shell.css"), next_handler,
        )).status_code == 299
        assert (await module.local_data_plane_guard(
            _request("GET", "/static/operational-shell.js"), next_handler,
        )).status_code == 299
        with module.SessionLocal() as session:
            assert module.activate_local_provider(_request("POST", "/api/local/provider/activate"), session)["enabled"] is True
        # A subsequent request reaches the data-plane handler; authentication
        # then decides its outcome rather than the LOCAL enablement switch.
        assert (await module.local_data_plane_guard(_request("GET", "/a-bucket/a-key"), next_handler)).status_code == 299
    asyncio.run(exercise())


def test_administrator_can_explicitly_cancel_local_operations_before_deactivation(tmp_path, monkeypatch):
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'cancel.db'}")
    monkeypatch.setenv("FUJIN_LOCAL_STAGING_ROOT", str(Path(tmp_path) / "staging"))
    import app.fujin_local as module
    module = importlib.reload(module); module.startup(); session = module.SessionLocal()
    try:
        request = _request("POST", "/api/local")
        bucket = module.LocalOciBucket(namespace="local", name="cancel-target")
        session.add(bucket); session.flush()
        restore = module.LocalRestore(bucket_id="unused", object_key="object", state="IN_PROGRESS", available_at=module.utcnow(), expires_at=module.utcnow() + module.timedelta(hours=1))
        upload_id = "cancel-upload"; staging = module.STAGING_ROOT / f"multipart/{upload_id}"; staging.mkdir(parents=True)
        upload = module.LocalMultipartUpload(id=upload_id, bucket_id=bucket.id, object_key="object", staging_relative_path=f"multipart/{upload_id}", expires_at=module.utcnow() + module.timedelta(hours=1))
        session.add_all([restore, upload]); session.commit()
        result = module.cancel_active_local_operations(request, session)
        assert result["restores_cancelled"] == 1 and result["multipart_uploads_cancelled"] == 1
        assert session.get(module.LocalRestore, restore.id).state == "CANCELLED"
        assert session.get(module.LocalMultipartUpload, upload_id) is None and not staging.exists()
    finally:
        session.close()


def test_cloud_provider_controls_are_isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'cloud-controls.db'}")
    monkeypatch.setenv("FUJIN_LOCAL_STAGING_ROOT", str(Path(tmp_path) / "staging-cloud"))
    import app.fujin_local as module
    module = importlib.reload(module); module.startup(); session = module.SessionLocal()
    try:
        request = _request("POST", "/api/local")
        module.activate_local_provider(request, session)
        bucket = module.LocalOciBucket(namespace="local", name="cloud-target")
        session.add(bucket); session.flush()
        restore = module.LocalRestore(bucket_id="aws-bucket", object_key="source", state="IN_PROGRESS",
                                      available_at=module.utcnow(), expires_at=module.utcnow() + module.timedelta(hours=1))
        upload_id = "oci-upload"
        staging = module.STAGING_ROOT / f"multipart/{upload_id}"; staging.mkdir(parents=True)
        upload = module.LocalMultipartUpload(id=upload_id, bucket_id=bucket.id, object_key="destination",
                                             staging_relative_path=f"multipart/{upload_id}",
                                             expires_at=module.utcnow() + module.timedelta(hours=1))
        session.add_all([restore, upload]); session.commit()

        aws_result = module.cancel_active_cloud_operations("aws", request, session)
        assert aws_result["restores_cancelled"] == 1 and aws_result["multipart_uploads_cancelled"] == 0
        assert session.get(module.LocalMultipartUpload, upload_id) is not None and staging.exists()
        assert module.deactivate_cloud_provider("aws", request, session)["enabled"] is False
        assert module.cloud_provider_state(session, "OCI").enabled is True
        with pytest.raises(module.HTTPException) as blocked:
            module.deactivate_cloud_provider("oci", request, session)
        assert blocked.value.status_code == 409

        async def next_handler(_request):
            from fastapi.responses import Response
            return Response(status_code=299)
        assert asyncio.run(module.local_data_plane_guard(_request("GET", "/source/object"), next_handler)).status_code == 503
        assert asyncio.run(module.local_data_plane_guard(_request("GET", "/n/local/b/destination/o/object"), next_handler)).status_code == 299

        oci_result = module.cancel_active_cloud_operations("oci", request, session)
        assert oci_result["restores_cancelled"] == 0 and oci_result["multipart_uploads_cancelled"] == 1
        assert session.get(module.LocalMultipartUpload, upload_id) is None and not staging.exists()
        assert module.deactivate_cloud_provider("oci", request, session)["enabled"] is False
    finally:
        session.close()


def test_local_audit_is_searchable_by_request_bucket_object_and_period(tmp_path, monkeypatch):
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'audit-search.db'}")
    import app.fujin_local as module
    module = importlib.reload(module); module.startup(); session = module.SessionLocal()
    try:
        index_names = {
            item["name"] for item in inspect(session.get_bind()).get_indexes("local_audit_events")
        }
        assert {
            "ix_local_audit_bucket_created_id",
            "ix_local_audit_object_created_id",
            "ix_local_audit_created_id",
        } <= index_names
        identifier = module.audit(session, "S3_GET_OBJECT", 200, "source", object_key="archive/object.bin", endpoint="s3.local/object")
        session.commit()
        rows = module.audit_events(_request("GET", "/api/local/audit"), request_id=identifier,
                                   bucket="source", object_key="archive/object.bin", since=module.utcnow() - module.timedelta(minutes=1), session=session)
        assert len(rows) == 1 and rows[0]["request_id"] == identifier and rows[0]["object_key"] == "archive/object.bin"
        assert module.audit_events(_request("GET", "/api/local/audit"), bucket="other", session=session) == []
        for index in range(15):
            module.audit(session, "S3_LIST_OBJECTS", 200, "source", object_key=f"archive/{index:02d}.bin")
        session.commit()
        period_start = module.utcnow() - module.timedelta(minutes=1)
        first_page = module.audit_events(_request("GET", "/api/local/audit"), since=period_start, limit=10, offset=0, session=session)
        second_page = module.audit_events(_request("GET", "/api/local/audit"), since=period_start, limit=10, offset=10, session=session)
        assert len(first_page) == 10 and len(second_page) == 6
        assert {row["request_id"] for row in first_page}.isdisjoint(row["request_id"] for row in second_page)
        assert module.audit_events(_request("GET", "/api/local/audit"), session=session) == []
        assert module.audit_event_count(_request("GET", "/api/local/audit/count"), session=session)["total"] == 0
        assert module.audit_event_count(_request("GET", "/api/local/audit/count"), since=period_start, session=session)["total"] == 16
        assert module.audit_event_count(_request("GET", "/api/local/audit/count"), bucket="other", session=session)["total"] == 0
        assert module.audit_event_buckets(_request("GET", "/api/local/audit/buckets"), session=session) == ["source"]
    finally:
        session.close()


def test_provider_errors_return_correlatable_s3_and_oci_request_ids(tmp_path, monkeypatch):
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'provider-errors.db'}")
    import app.fujin_local as module
    module = importlib.reload(module); module.startup()
    with module.SessionLocal() as session:
        module.activate_local_provider(_request("POST", "/api/local/provider/activate"), session)
    listener = socket.socket(); listener.bind(("127.0.0.1", 0)); port = listener.getsockname()[1]; listener.close()
    server = uvicorn.Server(uvicorn.Config(module.app, host="127.0.0.1", port=port, log_level="error"))
    thread = threading.Thread(target=server.run, daemon=True); thread.start()
    for _ in range(100):
        if server.started:
            break
        time.sleep(0.01)
    try:
        with pytest.raises(urlerror.HTTPError) as s3:
            urlrequest.urlopen(f"http://127.0.0.1:{port}/missing-bucket/object")
        with pytest.raises(urlerror.HTTPError) as oci:
            urlrequest.urlopen(f"http://127.0.0.1:{port}/n/local/b/missing-bucket/o/object")
        s3_error, oci_error = s3.value, oci.value
    finally:
        server.should_exit = True
        thread.join(timeout=3)
    assert s3_error.code == 404 and s3_error.headers["x-amz-request-id"].startswith("fujin-")
    assert oci_error.code == 404 and oci_error.headers["opc-request-id"].startswith("fujin-")
    with module.SessionLocal() as session:
        assert len(module.audit_events(_request("GET", "/api/local/audit"), request_id=s3_error.headers["x-amz-request-id"], session=session)) == 1
        assert len(module.audit_events(_request("GET", "/api/local/audit"), request_id=oci_error.headers["opc-request-id"], session=session)) == 1


def test_local_aws_contract_with_boto3(tmp_path, monkeypatch):
    """Exercise STS and S3 using the same boto3 transport used by Raijin."""
    root = Path(tmp_path) / "payloads"
    payload = b"sdk-compatible-payload"
    (root / "snapshot").mkdir(parents=True)
    (root / "snapshot/object.bin").write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'sdk.db'}")
    monkeypatch.setenv("FUJIN_LOCAL_PAYLOAD_ROOT", str(root))
    monkeypatch.setenv("FUJIN_LOCAL_STAGING_ROOT", str(Path(tmp_path) / "staging"))
    # The private DNS name must reach the loopback provider directly, never an
    # environment HTTP proxy.
    for variable in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        monkeypatch.delenv(variable, raising=False)
    import app.fujin_local as module
    module = importlib.reload(module)
    module.startup()
    session = module.SessionLocal()
    try:
        admin = _request("POST", "/api/local/datasets")
        dataset = _create_ready_dataset(module, admin, module.DatasetCreate(
            name="sdk-dataset", snapshot_id="sdk-snapshot", repository_relative_path="snapshot", quota_bytes=len(payload),
            manifest={"objects": [{"key": "object.bin", "relative_path": "snapshot/object.bin", "size_bytes": len(payload),
                                   "sha256": digest, "etag": digest, "storage_class": "DEEP_ARCHIVE"}]},
        ), session=session)
        created = module.create_s3_bucket(admin, module.S3BucketCreate(name="sdk-source", dataset_id=dataset["id"], restore_policy={"delay_seconds": 0}), session=session)
        bucket = session.get(module.LocalS3Bucket, created["id"])
        assert module.activate_local_provider(_request("POST", "/api/local/provider/activate"), session)["enabled"] is True
        listener = socket.socket(); listener.bind(("127.0.0.1", 0)); port = listener.getsockname()[1]; listener.close()
        server = uvicorn.Server(uvicorn.Config(module.app, host="127.0.0.1", port=port, log_level="error"))
        thread = threading.Thread(target=server.run, daemon=True); thread.start()
        for _ in range(100):
            if server.started:
                break
            time.sleep(0.01)
        endpoint = f"http://vpce-fujin-local.s3.us-east-1.fujin.internal:{port}"
        original_getaddrinfo = socket.getaddrinfo
        def private_dns(host, *args, **kwargs):
            # S3 Control intentionally injects the synthetic account prefix.
            # The production LOCAL DNS wildcard resolves it; emulate only that
            # private-DNS behavior in this loopback SDK contract test.
            if str(host).endswith(".fujin.internal"):
                host = "127.0.0.1"
            return original_getaddrinfo(host, *args, **kwargs)
        monkeypatch.setattr(socket, "getaddrinfo", private_dns)
        config = Config(s3={"addressing_style": "virtual"}, retries={"max_attempts": 0})
        sts = boto3.client("sts", endpoint_url=endpoint, region_name=bucket.region,
                           aws_access_key_id=bucket.access_key_id, aws_secret_access_key=bucket.secret_access_key,
                           config=config)
        assert sts.get_caller_identity()["Account"] == bucket.account_id
        assumed = sts.assume_role(RoleArn=bucket.migration_role_arn, RoleSessionName="contract")["Credentials"]
        s3 = boto3.client("s3", endpoint_url=endpoint, region_name=bucket.region,
                          aws_access_key_id=assumed["AccessKeyId"], aws_secret_access_key=assumed["SecretAccessKey"],
                          aws_session_token=assumed["SessionToken"], config=config)
        assert s3.head_bucket(Bucket=bucket.name)["ResponseMetadata"]["HTTPHeaders"]["x-amz-bucket-region"] == bucket.region
        listed = s3.list_objects_v2(Bucket=bucket.name)
        assert listed["KeyCount"] == 1 and listed["Contents"][0]["LastModified"]
        s3.restore_object(Bucket=bucket.name, Key="object.bin", RestoreRequest={"Days": 1, "GlacierJobParameters": {"Tier": "Bulk"}})
        assert s3.head_object(Bucket=bucket.name, Key="object.bin")["Restore"]
        assert s3.get_object(Bucket=bucket.name, Key="object.bin")["Body"].read() == payload
        s3.put_object(Bucket=bucket.control_bucket, Key="manifest.csv", Body=f"{bucket.name},object.bin\n".encode())
        assert s3.get_object(Bucket=bucket.control_bucket, Key="manifest.csv")["Body"].read()
        control = boto3.client("s3control", endpoint_url=endpoint, region_name=bucket.region,
                                aws_access_key_id=assumed["AccessKeyId"], aws_secret_access_key=assumed["SecretAccessKey"],
                                aws_session_token=assumed["SessionToken"], config=config)
        created_job = control.create_job(
            AccountId=bucket.account_id, ConfirmationRequired=False, Priority=10, RoleArn=bucket.batch_role_arn,
            Operation={"S3InitiateRestoreObject": {"ExpirationInDays": 1, "GlacierJobTier": "BULK"}},
            Manifest={"Spec": {"Format": "S3BatchOperations_CSV_20180820", "Fields": ["Bucket", "Key"]},
                      "Location": {"ObjectArn": f"arn:aws:s3:::{bucket.control_bucket}/manifest.csv", "ETag": "manifest"}},
            Report={"Bucket": f"arn:aws:s3:::{bucket.control_bucket}", "Prefix": "reports/contract/", "Format": "Report_CSV_20180820", "Enabled": True, "ReportScope": "AllTasks"},
            Description="contract", ClientRequestToken="local-contract",
        )
        progress = control.describe_job(
            AccountId=bucket.account_id, JobId=created_job["JobId"]
        )["Job"]["ProgressSummary"]
        assert progress == {
            "TotalNumberOfTasks": 1,
            "NumberOfTasksSucceeded": 1,
            "NumberOfTasksFailed": 0,
        }

        # This is the actual Raijin connection pre-check path: its Secret is
        # structurally a normal AWS Secret and its only provider-specific
        # inputs are the generic SDK endpoint fields on AwsConnection.
        password_file = Path(tmp_path) / "raijin-password"
        password_file.write_text("contract-password", encoding="utf-8")
        monkeypatch.setenv("DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'raijin-import.db'}")
        monkeypatch.setenv("POSTGRES_PASSWORD_FILE", str(password_file))
        monkeypatch.setenv("RAIJIN_OPERATION_MODE", "REAL")
        import app.main as raijin
        connection_package = module.s3_connection(created["id"], admin, session=session)
        serialized_secret = json.dumps(connection_package["secret"], ensure_ascii=False)
        assert all(ord(character) <= 255 for character in serialized_secret)
        assert connection_package["secret"]["connection_name"] == f"Fujin LOCAL - {bucket.name}"
        assert raijin.parse_aws_connection_payload(json.dumps(connection_package["secret"]))["aws_account_id"] == bucket.account_id
        assert connection_package["secret"]["private_endpoint"] == connection_package["endpoints"]
        assert connection_package["endpoints"]["tls_ca_bundle_path"] == "/etc/fujin-local-ca/fujin-local.crt"
        raijin_engine = create_engine(f"sqlite+pysqlite:///{Path(tmp_path) / 'raijin-precheck.db'}")
        raijin.Base.metadata.create_all(raijin_engine)
        RaijinSession = sessionmaker(bind=raijin_engine)
        synthetic_secret = {
            "schema_version": 1,
            "connection_name": "local-contract",
            "aws_account_id": bucket.account_id,
            "default_region": bucket.region,
            "bootstrap_access_key_id": bucket.access_key_id,
            "bootstrap_secret_access_key": bucket.secret_access_key,
            "migration_role_arn": bucket.migration_role_arn,
            "batch_operations_role_arn": bucket.batch_role_arn,
            "control_bucket": bucket.control_bucket,
        }
        with RaijinSession() as raijin_session:
            connection = raijin.AwsConnection(
                label="ordinary-private-endpoints", secret_ocid="ocid1.vaultsecret.oc1..synthetic",
                aws_account_id=bucket.account_id, default_region=bucket.region,
                control_bucket=bucket.control_bucket, sts_endpoint_url=endpoint,
                s3_endpoint_url=endpoint, s3control_endpoint_url=endpoint,
                s3_addressing_style="virtual",
            )
            raijin_session.add(connection); raijin_session.commit()
            monkeypatch.setattr(raijin, "aws_secret_payload", lambda _ocid: synthetic_secret)
            # Event persistence is already covered by the Raijin operational
            # suite.  SQLite's BIGINT autoincrement difference is irrelevant
            # to this network contract, so keep this assertion focused on the
            # real pre-check SDK sequence.
            monkeypatch.setattr(raijin, "record_event", lambda *_args, **_kwargs: None)
            checked = raijin.precheck_aws_connection(connection.id, session=raijin_session)
            assert checked["status"] == "VALIDATED"

            # Exercise the real Raiju copy routine against both local provider
            # APIs.  No simulator transport is involved: Boto3 reads S3 and
            # OCI's SDK commits a reference to the validated physical bytes.
            module.create_oci_bucket(admin, module.OciBucketCreate(namespace="local", name="worker-destination"), session=session)
            key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
            pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()).decode()
            signer = oci.signer.Signer(
                "ocid1.tenancy.oc1..aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "ocid1.user.oc1..aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99", None,
                private_key_content=pem,
            )
            oci_client = oci.object_storage.ObjectStorageClient(
                {"user": "ocid1.user.oc1..aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                 "tenancy": "ocid1.tenancy.oc1..aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                 "fingerprint": "aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99",
                 "key_file": "/dev/null", "region": "us-east-1"},
                signer=signer, service_endpoint=f"http://127.0.0.1:{port}",
            )
            import app.real_worker as worker
            monkeypatch.setattr(worker, "SessionLocal", RaijinSession)
            monkeypatch.setattr(worker, "object_storage_client", lambda _signer: oci_client)
            # The worker creates the normal Instance Principal signer before
            # constructing its OCI SDK client.  The contract replaces only
            # that external identity acquisition; the actual OCI SDK client
            # and all data-plane calls still target Fujin.
            class ContractInstancePrincipal:
                def __new__(cls):
                    return signer
            monkeypatch.setattr(worker.oci.auth.signers, "InstancePrincipalsSecurityTokenSigner", ContractInstancePrincipal)
            first = raijin.ObjectRecord(id=501, source_id=1, object_key="object.bin", size_bytes=len(payload),
                                        etag=digest, state=raijin.ObjectState.RESTORED)
            raijin_session.add(first); raijin_session.commit()
            worker.transfer_object(s3, "local", bucket.name, "worker-destination", first.id, 0, False)
            raijin_session.expire_all()
            assert raijin_session.get(raijin.ObjectRecord, first.id).state == raijin.ObjectState.TRANSFERRED
            assert oci_client.get_object("local", "worker-destination", "object.bin").data.content == payload

            # A lowered test threshold makes the durable multipart branch
            # observable without creating an artificial large payload.
            monkeypatch.setattr(worker, "DIRECT_SHA_LIMIT", 4)
            multipart = raijin.ObjectRecord(id=502, source_id=1, object_key="object.bin", size_bytes=len(payload),
                                            etag=digest, state=raijin.ObjectState.RESTORED)
            multipart.object_key = "multipart.bin"
            # The source must still resolve an existing physical key; expose a
            # second logical name with identical immutable evidence.
            dataset_row = session.get(module.LocalDataset, dataset["id"])
            manifest = json.loads(dataset_row.manifest_json)
            manifest["objects"].append({**manifest["objects"][0], "key": "multipart.bin"})
            dataset_row.manifest_json = json.dumps(manifest); session.commit()
            s3.restore_object(Bucket=bucket.name, Key="multipart.bin",
                              RestoreRequest={"Days": 1, "GlacierJobParameters": {"Tier": "Bulk"}})
            raijin_session.add(multipart); raijin_session.commit()
            worker.transfer_object(s3, "local", bucket.name, "worker-destination", multipart.id, 0, False,
                                   configured_multipart_part_size=5)
            raijin_session.expire_all()
            transferred_multipart = raijin_session.get(raijin.ObjectRecord, multipart.id)
            assert transferred_multipart.state == raijin.ObjectState.TRANSFERRED
            assert transferred_multipart.delivery_integrity_algorithm == "SHA256_MULTIPART"
            assert oci_client.get_object("local", "worker-destination", "multipart.bin").data.content == payload
    finally:
        # Setup can fail before the loopback server is allocated (for example,
        # in a restricted test sandbox).  Do not let cleanup hide that cause.
        if "server" in locals():
            server.should_exit = True
        if "thread" in locals():
            thread.join(timeout=3)
        session.close()


def test_local_oci_contract_with_sdk(tmp_path, monkeypatch):
    """Exercise namespace/list/put/head/get with OCI's real SDK signer."""
    root = Path(tmp_path) / "payloads"; payload = b"oci-sdk-payload"
    (root / "snapshot").mkdir(parents=True); (root / "snapshot/object.bin").write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    monkeypatch.setenv("FUJIN_LOCAL_DATABASE_URL", f"sqlite+pysqlite:///{Path(tmp_path) / 'oci-sdk.db'}")
    monkeypatch.setenv("FUJIN_LOCAL_PAYLOAD_ROOT", str(root)); monkeypatch.setenv("FUJIN_LOCAL_STAGING_ROOT", str(Path(tmp_path) / "staging"))
    import app.fujin_local as module
    module = importlib.reload(module); module.startup(); session = module.SessionLocal()
    try:
        admin = _request("POST", "/api/local/datasets")
        dataset = _create_ready_dataset(module, admin, module.DatasetCreate(name="oci-sdk", snapshot_id="oci-sdk-snapshot", repository_relative_path="snapshot", quota_bytes=len(payload), manifest={"objects":[{"key":"object.bin","relative_path":"snapshot/object.bin","size_bytes":len(payload),"sha256":digest,"etag":digest}]}), session=session)
        module.create_oci_bucket(admin, module.OciBucketCreate(namespace="local", name="destination"), session=session)
        assert module.activate_local_provider(_request("POST", "/api/local/provider/activate"), session)["enabled"] is True
        listener = socket.socket(); listener.bind(("127.0.0.1", 0)); port = listener.getsockname()[1]; listener.close()
        server = uvicorn.Server(uvicorn.Config(module.app, host="127.0.0.1", port=port, log_level="error")); thread = threading.Thread(target=server.run, daemon=True); thread.start()
        for _ in range(100):
            if server.started: break
            time.sleep(.01)
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()).decode()
        tenancy, user, fingerprint = "ocid1.tenancy.oc1..aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "ocid1.user.oc1..aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99"
        signer = oci.signer.Signer(tenancy, user, fingerprint, None, private_key_content=pem)
        client = oci.object_storage.ObjectStorageClient({"user":user,"tenancy":tenancy,"fingerprint":fingerprint,"key_file":"/dev/null","region":"us-ashburn-1"}, signer=signer, service_endpoint=f"http://127.0.0.1:{port}")
        # get_namespace in OCI SDK uses the configured endpoint's namespace route.
        assert client.get_namespace().data == "local"
        client.put_object("local", "destination", "object.bin", payload)
        assert client.head_object("local", "destination", "object.bin").headers["etag"] == digest
        assert client.get_object("local", "destination", "object.bin").data.content == payload
        assert client.list_objects("local", "destination").data.objects[0].name == "object.bin"
        upload = client.create_multipart_upload("local", "destination", oci.object_storage.models.CreateMultipartUploadDetails(object="multipart.bin")).data
        first, second = payload[:5], payload[5:]
        first_part = client.upload_part("local", "destination", "multipart.bin", upload.upload_id, 1, first)
        second_part = client.upload_part("local", "destination", "multipart.bin", upload.upload_id, 2, second)
        listed = client.list_multipart_upload_parts("local", "destination", "multipart.bin", upload.upload_id).data
        assert len(listed) == 2
        commit = oci.object_storage.models.CommitMultipartUploadDetails(parts_to_commit=[
            oci.object_storage.models.CommitMultipartUploadPartDetails(part_num=1, etag=first_part.headers["etag"]),
            oci.object_storage.models.CommitMultipartUploadPartDetails(part_num=2, etag=second_part.headers["etag"]),
        ])
        client.commit_multipart_upload("local", "destination", "multipart.bin", upload.upload_id, commit)
        assert client.get_object("local", "destination", "multipart.bin").data.content == payload
    finally:
        if "server" in locals(): server.should_exit = True
        if "thread" in locals(): thread.join(timeout=3)
        session.close()
