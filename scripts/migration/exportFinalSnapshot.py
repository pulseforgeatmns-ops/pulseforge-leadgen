#!/usr/bin/env python3
"""One-shot, read-only Railway PostgreSQL 18 export for the Render cutover.

Opens ONE REPEATABLE READ READ ONLY transaction, exports its snapshot, runs
pg_dump --snapshot on that exact snapshot (custom format, full database incl.
Signal) streamed to the private bucket, and computes content fingerprints for
every non-Signal table plus sequence values and catalog counts in the same
snapshot. The dump and the fingerprints therefore describe the same state even
if writes continue. Uploads <key> and <key>.manifest.json; never overwrites.
Logs counts/checksums only - never rows, URLs or credentials.
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
import psycopg2
from botocore.config import Config
from botocore.exceptions import ClientError

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fingerprintSql as fp  # noqa: E402

PART_SIZE = 32 * 1024 * 1024
# Railway project amused-harmony, service Postgres (c50a4d45-...), TCP proxy 72a576d6-...
PINNED_PUBLIC_PROXY = ("shinkansen.proxy.rlwy.net", "43478")
REQUIRED = ("PGHOST", "PGUSER", "PGDATABASE", "PGPASSWORD", "PGPORT",
            "S3_ENDPOINT", "S3_BUCKET", "S3_REGION", "S3_ACCESS_KEY_ID",
            "S3_SECRET_ACCESS_KEY", "BACKUP_OBJECT_KEY")


def fail(msg: str):
    raise RuntimeError(msg)


def log(status: str, detail: dict | None = None):
    print("FINAL_EXPORT_" + status + (" " + json.dumps(detail, sort_keys=True) if detail else ""), flush=True)


def validated_env() -> dict[str, str]:
    if os.environ.get("FINAL_EXPORT_APPROVED") != "YES_READ_ONLY_SNAPSHOT_TO_BUCKET":
        fail("export_not_approved")
    missing = [k for k in REQUIRED if not os.environ.get(k)]
    if missing:
        fail("missing_required_environment: " + ",".join(missing))
    # Source host: the private Railway host (worker inside Railway), or - only
    # with an explicit second approval - the pinned public TCP proxy of the
    # same Railway Postgres service, TLS required (sslmode=require). Nothing else.
    source_mode = os.environ.get("FINAL_EXPORT_SOURCE", "railway_private")
    if source_mode == "railway_private":
        if not os.environ["PGHOST"].endswith(".railway.internal"):
            fail("source_must_be_private_railway_host")
    elif source_mode == "railway_public_proxy":
        if os.environ.get("FINAL_EXPORT_EXTERNAL_APPROVED") != "YES_PINNED_RAILWAY_TCP_PROXY":
            fail("external_source_not_approved")
        if (os.environ["PGHOST"], os.environ["PGPORT"]) != PINNED_PUBLIC_PROXY:
            fail("source_must_be_pinned_railway_tcp_proxy")
        if os.environ.get("PGSSLMODE") != "require":
            fail("external_source_requires_pgsslmode_require")
    else:
        fail("unknown_final_export_source")
    if os.environ["PGDATABASE"] != "railway":
        fail("unexpected_source_database")
    uri = urlparse(os.environ["S3_ENDPOINT"])
    if uri.scheme != "https" or not uri.hostname or uri.username or uri.password:
        fail("invalid_s3_endpoint")
    key = os.environ["BACKUP_OBJECT_KEY"]
    if not key.startswith("migration-backups/") or not key.endswith(".dump") or ".." in key:
        fail("invalid_backup_object_key")
    return {k: os.environ[k] for k in REQUIRED}


def s3_client(cfg):
    return boto3.client("s3", endpoint_url=cfg["S3_ENDPOINT"],
                        aws_access_key_id=cfg["S3_ACCESS_KEY_ID"],
                        aws_secret_access_key=cfg["S3_SECRET_ACCESS_KEY"],
                        region_name=cfg["S3_REGION"],
                        config=Config(s3={"addressing_style": "virtual"},
                                      retries={"mode": "standard", "max_attempts": 5}))


def exists(s3, bucket, key) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def child_env() -> dict[str, str]:
    allow = ("PATH", "HOME", "PGHOST", "PGUSER", "PGDATABASE", "PGPASSWORD", "PGPORT", "PGSSLMODE")
    env = {k: os.environ[k] for k in allow if k in os.environ}
    env["PGCONNECT_TIMEOUT"] = "15"
    env["PGOPTIONS"] = "-c default_transaction_read_only=on"
    return env


def stream_dump(s3, bucket, key, snapshot) -> tuple[str, int]:
    upload_id = None
    proc = None
    try:
        upload_id = s3.create_multipart_upload(Bucket=bucket, Key=key,
                                               ContentType="application/octet-stream")["UploadId"]
        proc = subprocess.Popen(
            ["pg_dump", "--format=custom", "--compress=6", "--no-owner", "--no-acl",
             "--no-password", "--snapshot=" + snapshot],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=child_env(), bufsize=0)
        digest, total, parts, number = hashlib.sha256(), 0, [], 1
        while True:
            chunks, n = [], 0
            while n < PART_SIZE:
                block = proc.stdout.read(PART_SIZE - n)
                if not block:
                    break
                chunks.append(block)
                n += len(block)
            if not n:
                break
            payload = b"".join(chunks)
            total += n
            digest.update(payload)
            etag = s3.upload_part(Bucket=bucket, Key=key, UploadId=upload_id,
                                  PartNumber=number, Body=payload)["ETag"]
            parts.append({"PartNumber": number, "ETag": etag})
            number += 1
            if n < PART_SIZE:
                break
        proc.stdout.close()
        err = proc.stderr.read().decode("utf-8", "replace")
        code = proc.wait()
        if code != 0:
            # pg_dump errors name objects, not row values; keep only the tail.
            fail("pg_dump_failed_exit_code_%d: %s" % (code, err.strip()[-300:]))
        if total < 1024:
            fail("backup_archive_too_small")
        s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id,
                                     MultipartUpload={"Parts": parts})
        upload_id = None
        return digest.hexdigest(), total
    except BaseException:
        if proc and proc.poll() is None:
            proc.kill()
            proc.wait()
        if upload_id:
            try:
                s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
            except Exception:
                pass
        raise


def verify_remote(s3, bucket, key, sha, size):
    head = s3.head_object(Bucket=bucket, Key=key)
    if int(head["ContentLength"]) != size:
        fail("backup_size_mismatch")
    body = s3.get_object(Bucket=bucket, Key=key)["Body"]
    digest, total, first = hashlib.sha256(), 0, b""
    for block in body.iter_chunks(chunk_size=1024 * 1024):
        if not first:
            first = block[:5]
        total += len(block)
        digest.update(block)
    if total != size or digest.hexdigest() != sha:
        fail("backup_download_checksum_mismatch")
    if first != b"PGDMP":
        fail("custom_archive_magic_missing")


def main() -> int:
    try:
        cfg = validated_env()
        s3 = s3_client(cfg)
        bucket, key = cfg["S3_BUCKET"], cfg["BACKUP_OBJECT_KEY"]
        if exists(s3, bucket, key) or exists(s3, bucket, key + ".manifest.json"):
            fail("archive_key_already_exists_refusing_overwrite")
        version = subprocess.run(["pg_dump", "--version"], capture_output=True, text=True, check=True).stdout.strip()
        if "PostgreSQL) 18." not in version:
            fail("pg_dump_18_required")
        conn = psycopg2.connect(host=cfg["PGHOST"], port=cfg["PGPORT"], user=cfg["PGUSER"],
                                password=cfg["PGPASSWORD"], dbname=cfg["PGDATABASE"],
                                sslmode=os.environ.get("PGSSLMODE", "prefer"),
                                connect_timeout=15, application_name="final-export-snapshot")
        conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
        cur = conn.cursor()
        cur.execute("SET LOCAL statement_timeout = 0")
        cur.execute("""SELECT pg_export_snapshot(), now(), current_database(),
                       current_setting('server_version_num')::int, current_setting('transaction_read_only'),
                       pg_database_size(current_database()), txid_current_snapshot()::text""")
        snapshot, captured, dbname, server_num, ro, db_bytes, txsnap = cur.fetchone()
        if ro != "on":
            fail("transaction_not_read_only")
        fp.session_guards(cur)  # after the export: no savepoint may precede it
        log("SNAPSHOT_EXPORTED", {"capturedAt": captured.isoformat(), "serverVersionNum": server_num,
                                  "databaseBytes": int(db_bytes)})
        # Start the dump first so pg_dump imports the snapshot while this
        # transaction (the exporter) is still open; fingerprint concurrently.
        import threading
        result: dict = {}

        def run_dump():
            try:
                result["dump"] = stream_dump(s3, bucket, key, snapshot)
            except BaseException as exc:  # noqa: BLE001
                result["error"] = exc
        worker = threading.Thread(target=run_dump)
        worker.start()
        tables = fp.list_tables(cur)
        core = [t for t in tables if not fp.is_signal(t)]
        signal = [t for t in tables if fp.is_signal(t)]
        catalog = fp.catalog_counts(cur)
        prints = fp.fingerprint_tables(cur, core)
        seqs = fp.sequences(cur)
        log("FINGERPRINTS_COMPUTED", {"pulseforgeTables": len(core), "signalTables": len(signal),
                                      "pulseforgeRows": sum(v["rows"] for v in prints.values()),
                                      "sequences": len(seqs), "catalog": catalog})
        worker.join()
        cur.close()
        conn.rollback()
        conn.close()
        if "error" in result:
            raise result["error"]
        sha, size = result["dump"]
        log("UPLOAD_COMPLETE", {"bytes": size, "sha256": sha})
        verify_remote(s3, bucket, key, sha, size)
        manifest = {
            "status": "CHECKSUM_VERIFIED_NOT_RESTORE_TESTED",
            "archiveKey": key, "sha256": sha, "bytes": size, "pgDumpVersion": version,
            "snapshotCapturedAt": captured.isoformat(), "txidSnapshot": txsnap,
            "serverVersionNum": server_num, "sourceDatabaseBytes": int(db_bytes),
            "catalog": catalog, "signalTables": signal,
            "fingerprints": prints, "sequences": seqs,
            "verifiedAt": datetime.now(timezone.utc).isoformat(),
            "sourceType": "Railway PostgreSQL logical backup (exported snapshot) via " + os.environ.get("FINAL_EXPORT_SOURCE", "railway_private"),
        }
        s3.put_object(Bucket=bucket, Key=key + ".manifest.json",
                      Body=json.dumps(manifest, indent=1, sort_keys=True).encode(),
                      ContentType="application/json")
        manifest_sha = hashlib.sha256(json.dumps(manifest, indent=1, sort_keys=True).encode()).hexdigest()
        log("COMPLETE", {"archiveKey": key, "bytes": size, "sha256": sha, "manifestSha256": manifest_sha,
                         "pulseforgeRows": sum(v["rows"] for v in prints.values())})
        return 0
    except Exception as exc:  # noqa: BLE001
        log("FAILED", {"reason": str(exc)[:400] if isinstance(exc, RuntimeError) else type(exc).__name__})
        return 1


if __name__ == "__main__":
    sys.exit(main())
