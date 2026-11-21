#!/usr/bin/env python3
"""One-shot archive integrity check: S3 object -> local temp -> pg_restore TOC.
Read-only to the source database; no database connections or restores are made.
"""
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config

OBJECT_KEY = "migration-backups/railway-20261117-full-pg18.dump"
EXPECTED_SIZE = 727898203
EXPECTED_SHA256 = "4d44ad1f73ffcb5a3ee48ee8b30c78c2430f803da05915b40b84b6101a736628"


def main():
    if os.getenv("VERIFY_ONCE_APPROVED") != "YES_BUCKET_ARCHIVE_ONLY":
        raise RuntimeError("verification_not_approved")
    names = ["S3_ENDPOINT", "S3_BUCKET", "S3_REGION", "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY"]
    if any(not os.getenv(n) for n in names):
        raise RuntimeError("missing_bucket_reference")
    client = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"],
        region_name=os.environ["S3_REGION"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
        config=Config(s3={"addressing_style": "virtual"}))
    bucket = os.environ["S3_BUCKET"]
    # Do not trust a previous log or a manifest alone: read all bytes again.
    h = hashlib.sha256()
    count = 0
    with tempfile.TemporaryDirectory(prefix="railway-archive-verify-") as directory:
        path = directory + "/archive.dump"
        body = client.get_object(Bucket=bucket, Key=OBJECT_KEY)["Body"]
        try:
            with open(path, "wb") as out:
                for block in body.iter_chunks(chunk_size=4 * 1024 * 1024):
                    if not block:
                        continue
                    count += len(block)
                    if count > EXPECTED_SIZE:
                        raise RuntimeError("backup_exceeds_expected_size")
                    h.update(block)
                    out.write(block)
        finally:
            body.close()
        if count != EXPECTED_SIZE or h.hexdigest() != EXPECTED_SHA256:
            raise RuntimeError("backup_checksum_mismatch")
        with open(path, "rb") as obj:
            if obj.read(5) != b"PGDMP":
                raise RuntimeError("not_postgres_custom_archive")
        result = subprocess.run(["pg_restore", "--list", path], text=True,
             capture_output=True, timeout=90)
        if result.returncode:
            raise RuntimeError("pg_restore_toc_failed")
        entries = [line for line in result.stdout.splitlines() if re.match(r"^[0-9]+;", line)]
        if not entries:
            raise RuntimeError("toc_is_empty")
        table_data = sum(bool(re.search(r"; .* TABLE DATA ", line)) for line in entries)
        tables = sum(bool(re.search(r"; .* TABLE ", line)) and
                     not bool(re.search(r"; .* TABLE DATA ", line)) for line in entries)
        indexes = sum(bool(re.search(r"; .* INDEX ", line)) for line in entries)
        if not tables or not table_data:
            raise RuntimeError("backup_missing_tables_or_data")
        verification = {
            "status": "ARCHIVE_TOC_AND_SHA256_VERIFIED_NOT_RESTORE_TESTED",
            "checkedAt": datetime.now(timezone.utc).isoformat(),
            "objectKey": OBJECT_KEY,
            "bytes": count,
            "sha256": h.hexdigest(),
            "tocEntries": len(entries),
            "tableDefinitions": tables,
            "tableDataEntries": table_data,
            "indexEntries": indexes,
            "pgRestoreVersion": subprocess.run(["pg_restore", "--version"],
                capture_output=True, text=True, check=True).stdout.strip()
        }
        client.put_object(Bucket=bucket, Key=OBJECT_KEY + ".toc-verification.json",
            Body=json.dumps(verification, sort_keys=True, indent=2).encode(),
            ContentType="application/json")
        print("ARCHIVE_TOC_VERIFIED " + json.dumps(verification, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        message = str(exc) if isinstance(exc, RuntimeError) else type(exc).__name__
        print("ARCHIVE_VERIFY_FAILED " + message, file=sys.stderr, flush=True)
        sys.exit(1)
