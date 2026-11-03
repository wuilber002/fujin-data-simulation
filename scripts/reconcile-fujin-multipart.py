#!/usr/bin/env python3
"""Conservatively remove only provable stale Fujin multipart sessions.

Fujin multipart records contain checkpoint metadata, never a second copy of
the payload.  A record must therefore not be deleted merely because it is
old: an interrupted Raiju can still resume it.  This tool defaults to a
read-only report and accepts ``--apply`` only for records whose exact Raijin
object has reached a terminal delivery state and whose destination/key match.

For uploads created by current releases the immutable
``raijin-object-id`` metadata is the proof.  Older records are
reported as *manual review* unless they remain actively referenced by Raijin;
they are never removed automatically.
"""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from dataclasses import dataclass

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url


TERMINAL_STATES = {"TRANSFERRED", "VERIFIED"}
PROVENANCE_KEY = "raijin-object-id"


@dataclass(frozen=True)
class Candidate:
    upload_id: str
    bucket_name: str
    object_key: str
    object_id: int
    parts: int
    bytes: int


def parse_metadata(raw: str | None) -> dict[str, str]:
    try:
        value = json.loads(raw or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def database_url_with_secret(url: str, password_file: str | None) -> str:
    """Match Raijin's password-file contract without printing credentials."""
    parsed = make_url(url)
    if parsed.password is not None or not password_file:
        return url
    try:
        password = open(password_file, encoding="utf-8").read().strip()
    except OSError:
        return url
    return parsed.set(password=password).render_as_string(hide_password=False)


def provenance_object_id(metadata: dict[str, str]) -> int | None:
    for key in (PROVENANCE_KEY, f"opc-meta-{PROVENANCE_KEY}"):
        try:
            return int(metadata[key])
        except (KeyError, TypeError, ValueError):
            continue
    return None


def inspect(fujin_url: str, raijin_url: str) -> tuple[list[Candidate], Counter]:
    fujin = create_engine(fujin_url)
    raijin = create_engine(raijin_url)
    with raijin.connect() as connection:
        active_ids = {
            row[0] for row in connection.execute(text(
                "SELECT DISTINCT multipart_upload_id FROM objects "
                "WHERE multipart_upload_id IS NOT NULL"
            ))
        }
        objects = {
            int(row.id): row
            for row in connection.execute(text(
                "SELECT objects.id, objects.object_key, objects.state, sources.destination_bucket "
                "FROM objects JOIN sources ON sources.id = objects.source_id"
            )).mappings()
        }
    with fujin.connect() as connection:
        uploads = list(connection.execute(text(
            "SELECT uploads.id, buckets.name AS bucket_name, uploads.object_key, uploads.metadata_json, "
            "COUNT(parts.part_number) AS parts, COALESCE(SUM(parts.size_bytes), 0) AS bytes "
            "FROM local_multipart_uploads AS uploads "
            "JOIN local_oci_buckets AS buckets ON buckets.id = uploads.bucket_id "
            "LEFT JOIN local_multipart_parts AS parts ON parts.upload_id = uploads.id "
            "GROUP BY uploads.id, buckets.name, uploads.object_key, uploads.metadata_json"
        )).mappings())

    summary: Counter = Counter()
    candidates: list[Candidate] = []
    for upload in uploads:
        upload_id = str(upload["id"])
        if upload_id in active_ids:
            summary["referenced_by_raijin"] += 1
            continue
        object_id = provenance_object_id(parse_metadata(upload["metadata_json"]))
        target = objects.get(object_id) if object_id is not None else None
        if not target:
            summary["unmarked_or_unknown"] += 1
            continue
        if (str(target.state) not in TERMINAL_STATES
                or str(target.object_key) != str(upload["object_key"])
                or str(target.destination_bucket) != str(upload["bucket_name"])):
            summary["provenance_not_terminal_or_mismatch"] += 1
            continue
        candidates.append(Candidate(
            upload_id, str(upload["bucket_name"]), str(upload["object_key"]), object_id,
            int(upload["parts"]), int(upload["bytes"]),
        ))
        summary["proven_terminal_orphan"] += 1
    summary["total_uploads"] = len(uploads)
    return candidates, summary


def apply(fujin_url: str, candidates: list[Candidate]) -> int:
    if not candidates:
        return 0
    engine = create_engine(fujin_url)
    with engine.begin() as connection:
        for candidate in candidates:
            # Recheck the relation in the mutation transaction.  The FK does
            # not rely on SQLite's optional cascade enforcement.
            connection.execute(text(
                "DELETE FROM local_multipart_parts WHERE upload_id = :upload_id"
            ), {"upload_id": candidate.upload_id})
            connection.execute(text(
                "DELETE FROM local_multipart_uploads WHERE id = :upload_id"
            ), {"upload_id": candidate.upload_id})
    return len(candidates)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fujin-database-url", default=os.getenv("FUJIN_LOCAL_DATABASE_URL"))
    parser.add_argument("--raijin-database-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument("--apply", action="store_true", help="delete only proven terminal Raiju remnants")
    args = parser.parse_args()
    if not args.fujin_database_url or not args.raijin_database_url:
        parser.error("both Fujin and Raijin database URLs are required")
    raijin_url = database_url_with_secret(
        args.raijin_database_url, os.getenv("POSTGRES_PASSWORD_FILE")
    )
    candidates, summary = inspect(args.fujin_database_url, raijin_url)
    print(json.dumps({"summary": summary, "candidates": [candidate.__dict__ for candidate in candidates]}, default=dict, indent=2))
    if not args.apply:
        return 0
    removed = apply(args.fujin_database_url, candidates)
    print(json.dumps({"applied": True, "removed_uploads": removed}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
