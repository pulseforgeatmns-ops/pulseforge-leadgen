#!/usr/bin/env python3
"""One-time, read-only PostgreSQL 18 custom-format dump to a private S3 bucket.

Streams pg_dump directly to multipart S3 with bounded memory and no local dump file.
The destination is a *staging backup*, not an independently verified off-provider copy.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

PART_SIZE = 32 * 1024 * 1024
REQUIRED = (
    "PGHOST", "PGUSER", "PGDATABASE", "PGPASSWORD", "PGPORT",
    "S3_ENDPOINT", "S3_BUCKET", "S3_REGION", "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY",
    "BACKUP_OBJECT_KEY",
)


def fail(message: str) -> None:
    raise RuntimeError(message)


def validated_env() -> dict[str, str]:
    if os.environ.get("EXPORT_ONCE_APPROVED") != "YES_READ_ONLY_TO_BUCKET":
        fail("export_not_approved")
    missing = [key for key in REQUIRED if not os.environ.get(key)]
    if missing:
        fail("missing_required_environment: " + ",".join(missing))
    host = os.environ["PGHOST"]
    if not host.endswith(".railway.internal"):
        fail("source_must_be_a_private_railway_database_host")
    if os.environ["PGDATABASE"] != "railway":
        fail("unexpected_source_database")
    if not os.environ["PGPORT"].isdigit():
        fail("invalid_pgport")
    uri = urlparse(os.environ["S3_ENDPOINT"])
    if uri.scheme != "https" or not uri.hostname or uri.username or uri.password:
        fail("invalid_s3_endpoint")
    key = os.environ["BACKUP_OBJECT_KEY"]
    if (not key.startswith("migration-backups/") or not key.endswith(".dump")
            or ".." in key or key.startswith("/")):
        fail("invalid_backup_object_key")
    return {key: os.environ[key] for key in REQUIRED}


def s3_client(cfg: dict[str, str]):
    return boto3.client(
        "s3", endpoint_url=cfg["S3_ENDPOINT"],
        aws_access_key_id=cfg["S3_ACCESS_KEY_ID"],
        aws_secret_access_key=cfg["S3_SECRET_ACCESS_KEY"],
        region_name=cfg["S3_REGION"],
        config=Config(s3={"addressing_style": "virtual"},
                      retries={"mode": "standard", "max_attempts": 5}),
    )


def already_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def pg_dump_env() -> dict[str, str]:
    # Avoid propagating storage credentials to child process.
    allow = ("PATH", "HOME", "PGHOST", "PGUSER", "PGDATABASE", "PGPASSWORD",
             "PGPORT", "PGSSLMODE")
    env = {k: os.environ[k] for k in allow if k in os.environ}
    env["PGCONNECT_TIMEOUT"] = "15"
    env["PGOPTIONS"] = "-c default_transaction_read_only=on"
    return env


def verify_storage(s3, bucket: str, key: str, expected_sha: str, size: int) -> None:
    head = s3.head_object(Bucket=bucket, Key=key)
    if int(head["ContentLength"]) != size:
        fail("backup_size_mismatch")
    body = s3.get_object(Bucket=bucket, Key=key)["Body"]
    digest = hashlib.sha256()
    total = 0
    try:
        for block in body.iter_chunks(chunk_size=1024 * 1024):
            if block:
                total += len(block)
                digest.update(block)
    finally:
        body.close()
    if total != size or digest.hexdigest() != expected_sha:
        fail("backup_download_checksum_mismatch")


def export(s3, cfg: dict[str, str]) -> dict:
    bucket, key = cfg["S3_BUCKET"], cfg["BACKUP_OBJECT_KEY"]
    if already_exists(s3, bucket, key):
        fail("archive_key_already_exists_refusing_overwrite")
    pg_version = subprocess.run(["pg_dump", "--version"], capture_output=True,
                                check=True, text=True).stdout.strip()
    if "PostgreSQL) 18." not in pg_version:
        fail("pg_dump_18_required")
    upload_id = None
    process = None
    try:
        result = s3.create_multipart_upload(Bucket=bucket, Key=key,
                                           ContentType="application/octet-stream")
        upload_id = result["UploadId"]
        process = subprocess.Popen(
            ["pg_dump", "--format=custom", "--compress=6", "--no-owner", "--no-acl", "--no-password"],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=pg_dump_env(), bufsize=0,
        )
        assert process.stdout is not None
        count = 0
        digest = hashlib.sha256()
        parts = []
        part_number = 1
        while True:
            chunks = []
            n = 0
            while n < PART_SIZE:
                block = process.stdout.read(PART_SIZE - n)
                if not block:
                    break
                chunks.append(block)
                n += len(block)
            if not n:
                break
            payload = b"".join(chunks)
            count += n
            digest.update(payload)
            result = s3.upload_part(Bucket=bucket, Key=key, UploadId=upload_id,
                                    PartNumber=part_number, Body=payload)
            parts.append({"PartNumber": part_number, "ETag": result["ETag"]})
            part_number += 1
            if n < PART_SIZE:
                break
        process.stdout.close()
        code = process.wait()
        if code != 0:
            fail("pg_dump_failed_exit_code_" + str(code))
        if count < 1024 or not parts:
            fail("backup_archive_too_small")
        s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id,
                                     MultipartUpload={"Parts": parts})
        upload_id = None
        sha = digest.hexdigest()
        print("UPLOAD_COMPLETE; verifying remote object checksum", flush=True)
        verify_storage(s3, bucket, key, sha, count)
        manifest = {
            "status": "CHECKSUM_VERIFIED_NOT_RESTORE_TESTED",
            "archiveKey": key,
            "sha256": sha,
            "bytes": count,
            "pgDumpVersion": pg_version,
            "verifiedAt": datetime.now(timezone.utc).isoformat(),
            "sourceType": "Railway private PostgreSQL logical backup",
        }
        s3.put_object(Bucket=bucket, Key=key + ".manifest.json",
                      Body=json.dumps(manifest, indent=2).encode("utf-8"),
                      ContentType="application/json")
        return manifest
    except BaseException:
        if process and process.poll() is None:
            process.kill()
            process.wait()
        if upload_id:
            try:
                s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
            except Exception:
                pass
        raise


def main() -> int:
    try:
        cfg = validated_env()
        print("STARTING_READ_ONLY_BACKUP_TO_PRIVATE_BUCKET", flush=True)
        result = export(s3_client(cfg), cfg)
        print("BACKUP_CHECKSUM_VERIFIED " + json.dumps(result, sort_keys=True), flush=True)
        return 0
    except Exception as exc:
        code = str(exc) if isinstance(exc, RuntimeError) else type(exc).__name__
        print("BACKUP_FAILED " + code, file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
