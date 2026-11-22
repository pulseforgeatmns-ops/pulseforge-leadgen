#!/usr/bin/env python3
"""Isolated Render PostgreSQL 18 one-time restore receiver for November 21.

Downloads the pinned full archive, restores 274 Pulseforge TABLE DATA entries
into empty pulseforge_production, and keeps the 45 Signal tables as empty
schema for application compatibility. Signal history stays in the archive.
Leaves the November 17 snapshot database untouched. Never points at Railway,
enables application workers, merges PR #1146, increases the 10 GB disk, or
starts live operations.
"""
from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hmac
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import threading

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parent))
from restoreGatewayContract import (  # noqa: E402
    APPROVED_TIGRIS_BUCKETS,
    ARCHIVE_BYTES,
    ARCHIVE_OBJECT_KEY,
    ARCHIVE_SHA256,
    CORE_TABLE_COUNT,
    DEFAULT_DISK_BYTES,
    FINGERPRINT_TABLES,
    FORBIDDEN_SNAPSHOT_DATABASE,
    PULSEFORGE_RESTORE_FLOOR,
    SIGNAL_TABLE_COUNT,
    TARGET_DATABASE,
    TIGRIS_BUCKET,
    assert_empty_postgres18,
    assert_restore_endpoint,
    assert_snapshot_preserved,
    assert_staging_and_approval,
    assert_writers_disabled,
    catalog_tables,
    content_fingerprint_sql,
    pinned_disk_bytes,
    require_pulseforge_restore_capacity,
    select_restore_toc,
    snapshot_database_url,
    tigris_settings,
    write_verified_archive,
)

COPY_RE = re.compile(rb"^COPY public\.([a-z][a-z0-9_]*) (?:\(|FROM )")
_lock = threading.Lock()
_started = False


def message(status: str, detail: dict | None = None):
    # Deliberately never logs URL, token, SQL, customer values, or connection info.
    print("MIGRATION_RESTORE_" + status + (" " + json.dumps(detail, sort_keys=True)
          if detail else ""), flush=True)


def call(cmd, *, timeout=90, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL):
    return subprocess.run(cmd, input=stdin, stdout=stdout, stderr=stderr,
                          timeout=timeout, check=True)


def sql_output(query: str, *, timeout=45, url: str | None = None) -> str:
    target = url or os.environ.get("DATABASE_URL", "")
    if not target.startswith(("postgresql://", "postgres://")):
        raise RuntimeError("missing_internal_render_database")
    if url is None:
        assert_restore_endpoint(target)
    env = os.environ.copy()
    env["PGCONNECT_TIMEOUT"] = "12"
    return subprocess.run(
        ["psql", "-X", "-A", "-t", "-w", "-v", "ON_ERROR_STOP=1",
         "--dbname", target, "--file", "-"],
        input=query, env=env, capture_output=True, text=True, timeout=timeout, check=True
    ).stdout.strip()


def inspect_identity(url: str | None = None) -> tuple[str, str, int, int]:
    raw = sql_output(
        "SELECT current_database() || ':' || current_setting('server_version_num') || ':' || "
        "(SELECT count(*) FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid "
        "WHERE n.nspname NOT IN ('pg_catalog','information_schema') "
        "AND n.nspname NOT LIKE 'pg_toast%' AND c.relkind IN ('r','p','v','m','S')) "
        "|| ':' || pg_database_size(current_database())::text;",
        url=url,
    )
    database, version, count, size = raw.split(":")
    return database, version, int(count), int(size)


def list_database_sizes() -> dict[str, int]:
    raw = sql_output("SELECT datname || E'\\t' || pg_database_size(datname)::text FROM pg_database;")
    sizes = {}
    for line in raw.splitlines():
        name, size = line.split("\t", 1)
        sizes[name] = int(size)
    return sizes


def disk_bytes() -> int:
    raw = os.environ.get("RENDER_DISK_BYTES", str(DEFAULT_DISK_BYTES))
    return pinned_disk_bytes(int(raw))


def guard_empty_target() -> None:
    database, version, count, _size = inspect_identity()
    assert_empty_postgres18(database=database, version=version, object_count=count)


def guard_snapshot() -> dict:
    snap = snapshot_database_url(os.environ.get("DATABASE_URL", ""))
    database, version, count, size = inspect_identity(url=snap)
    assert_snapshot_preserved(
        database=database, version=version, object_count=count, size_bytes=size)
    return {"database": database, "objectCount": count, "bytes": size, "postgres": version}


def preflight() -> dict:
    assert_staging_and_approval()
    assert_writers_disabled()
    assert_restore_endpoint(os.environ.get("DATABASE_URL", ""))
    guard_empty_target()
    snapshot = guard_snapshot()
    assessment = require_pulseforge_restore_capacity(list_database_sizes(), disk_bytes=disk_bytes())
    tables = catalog_tables()
    archive = verify_tigris_head()
    return {
        "status": "ready",
        "targetDatabase": TARGET_DATABASE,
        "snapshotDatabase": FORBIDDEN_SNAPSHOT_DATABASE,
        "archiveObjectKey": ARCHIVE_OBJECT_KEY,
        "archiveBytes": ARCHIVE_BYTES,
        "archiveHeadBytes": archive["bytes"],
        "physicalTables": len(tables["all"]),
        "pulseforgeTableData": len(tables["core"]),
        "signalTableDataExcluded": len(tables["signal"]),
        "restoreAllTableData": False,
        "authorizedSignalDataExclusion": True,
        "silentlyDiscardSignalData": False,
        "signalHistoryRetainedInArchive": True,
        "increaseRenderDisk": False,
        "restoreFloor": PULSEFORGE_RESTORE_FLOOR,
        "requiredBytes": assessment["required_bytes"],
        "diskBytes": assessment["disk_bytes"],
        "siblingBytes": assessment["sibling_bytes"],
        "snapshot": snapshot,
        "source": "tigris",
    }


def tigris_client(settings: dict):
    return boto3.client(
        "s3",
        endpoint_url=settings["endpoint"],
        aws_access_key_id=settings["access_key_id"],
        aws_secret_access_key=settings["secret_access_key"],
        region_name=settings["region"],
        config=Config(
            s3={"addressing_style": "virtual", "signature_version": "s3v4"},
            retries={"mode": "standard", "max_attempts": 5},
        ),
    )


def _bucket_candidates(preferred: str) -> list[str]:
    ordered = []
    for name in (preferred, TIGRIS_BUCKET, "8580503c-e7fb-41a7-ac9e-715081f95ad1"):
        if name in APPROVED_TIGRIS_BUCKETS and name not in ordered:
            ordered.append(name)
    return ordered


def verify_tigris_head() -> dict:
    settings = tigris_settings()
    client = tigris_client(settings)
    last_error = "tigris_head_failed"
    for bucket in _bucket_candidates(settings["bucket"]):
        try:
            head = client.head_object(Bucket=bucket, Key=settings["key"])
        except Exception:
            last_error = "tigris_head_failed"
            continue
        size = int(head.get("ContentLength") or 0)
        if size != ARCHIVE_BYTES:
            raise RuntimeError("backup_size_or_checksum_mismatch")
        return {"bytes": size, "bucket": bucket, "objectKey": ARCHIVE_OBJECT_KEY}
    raise RuntimeError(last_error)


def download(filename: Path) -> None:
    settings = tigris_settings()
    client = tigris_client(settings)
    located = verify_tigris_head()
    body = None
    try:
        obj = client.get_object(Bucket=located["bucket"], Key=settings["key"])
        body = obj["Body"]
        total = write_verified_archive(body, filename)
    except RuntimeError:
        raise
    except Exception:
        raise RuntimeError("bucket_download_failed") from None
    finally:
        close = getattr(body, "close", None)
        if close:
            close()
    message("ARCHIVE_VERIFIED", {
        "bytes": total,
        "sha256": ARCHIVE_SHA256,
        "objectKey": ARCHIVE_OBJECT_KEY,
        "source": "tigris",
        "signalHistoryRetainedInArchive": True,
    })


def build_toc(archive: Path, toc: Path, tables: dict[str, set[str]]):
    listing = call(["pg_restore", "--list", str(archive)], timeout=90).stdout.decode()
    selected, detail = select_restore_toc(listing, tables)
    toc.write_text(selected)
    signal_data_active = sum(
        1 for line in selected.splitlines()
        if re.match(r"^\d+;\s+\d+\s+\d+\s+TABLE DATA\s+public\s+signal_", line)
    )
    signal_schema = sum(
        1 for line in selected.splitlines()
        if re.match(r"^\d+;\s+\d+\s+\d+\s+TABLE\s+public\s+signal_", line)
    )
    if signal_data_active != 0:
        raise RuntimeError("signal_table_data_must_be_excluded")
    if signal_schema != SIGNAL_TABLE_COUNT:
        raise RuntimeError("signal_schema_must_be_retained")
    if detail["pulseforgeTableData"] != CORE_TABLE_COUNT:
        raise RuntimeError("pulseforge_table_count_mismatch")
    if detail["signalTableDataExcluded"] != SIGNAL_TABLE_COUNT:
        raise RuntimeError("signal_table_count_mismatch")
    message("SELECTION_VERIFIED", {
        "allTableData": len(tables["all"]),
        "pulseforgeTableData": detail["pulseforgeTableData"],
        "signalTableDataExcluded": detail["signalTableDataExcluded"],
        "signalSequenceSetsExcluded": detail["signalSequenceSetsExcluded"],
        "signalSchemaTables": signal_schema,
        "restoreAllTableData": False,
        "authorizedSignalDataExclusion": True,
        "silentlyDiscardSignalData": False,
        "signalHistoryRetainedInArchive": True,
        "increaseRenderDisk": False,
    })


def archive_counts(archive: Path, toc: Path, tables: set[str]) -> dict[str, int]:
    # Custom pg_restore emits COPY text; each row is one escaped physical line.
    # No customer values are retained or logged, only counts.
    command = ["pg_restore", "--data-only", "--use-list", str(toc), "--file=-", str(archive)]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    assert process.stdout is not None
    counts: dict[str, int] = {}
    active = None
    try:
        for raw in process.stdout:
            if active is None:
                if raw.startswith(b"COPY public."):
                    match = COPY_RE.match(raw[:200])
                    if not match:
                        raise RuntimeError("unrecognized_copy_statement")
                    active = match.group(1).decode("ascii")
                    if active not in tables or active in counts:
                        raise RuntimeError("unexpected_copy_table")
                    counts[active] = 0
            elif raw in (b"\\.\n", b"\\.\r\n"):
                active = None
            else:
                counts[active] += 1
        if active is not None:
            raise RuntimeError("unterminated_copy_stream")
    except BaseException:
        process.kill()
        raise
    finally:
        process.stdout.close()
        result = process.wait(timeout=1800)
    if result != 0 or set(counts) != tables:
        raise RuntimeError("archive_copy_count_failed")
    message("SOURCE_ROWS_COUNTED", {"tables": len(counts), "rows": sum(counts.values())})
    return counts


def restore(archive: Path, toc: Path):
    db = os.environ["DATABASE_URL"]
    assert_restore_endpoint(db)
    message("TRANSACTION_STARTED")
    code = subprocess.run([
        "pg_restore", "--exit-on-error", "--single-transaction",
        "--no-owner", "--no-acl", "--no-tablespaces",
        "--use-list", str(toc), "--dbname", db, str(archive)
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=1800).returncode
    if code != 0:
        raise RuntimeError("pg_restore_failed_code_" + str(code))
    message("TRANSACTION_COMMITTED")


def verify_counts(tables: dict[str, set[str]], expected: dict[str, int]) -> dict[str, int]:
    sql = ["SELECT '" + name + "' || E'\\t' || COUNT(*)::text FROM public." + name + ";"
           for name in sorted(tables["all"])]
    values = sql_output("\n".join(sql), timeout=180)
    actual: dict[str, int] = {}
    for line in values.splitlines():
        name, raw = line.split("\t", 1)
        actual[name] = int(raw)
    if set(actual) != tables["all"]:
        raise RuntimeError("restored_table_inventory_mismatch")
    core_mismatches = [name for name in tables["core"] if actual[name] != expected.get(name)]
    if core_mismatches:
        raise RuntimeError("restored_row_count_mismatch")
    nonempty_signal = [name for name in tables["signal"] if actual[name] != 0]
    if nonempty_signal:
        raise RuntimeError("signal_table_data_must_remain_empty")
    if len(tables["core"]) != CORE_TABLE_COUNT or len(tables["signal"]) != SIGNAL_TABLE_COUNT:
        raise RuntimeError("unexpected_table_partition")
    return actual


def verify_sequences(expected: set[str]) -> int:
    raw = sql_output(
        "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid "
        "WHERE n.nspname='public' AND c.relkind='S';"
    )
    count = int(raw)
    if expected and count != len(expected):
        raise RuntimeError("restored_sequence_count_mismatch")
    if count < 1:
        raise RuntimeError("sequences_not_restored")
    return count


def verify_tenants() -> dict[str, int]:
    raw = sql_output(
        "SELECT 'clients:' || COUNT(*)::text FROM public.clients;\n"
        "SELECT 'users:' || COUNT(*)::text FROM public.users;\n"
        "SELECT 'prospects:' || COUNT(*)::text FROM public.prospects;\n"
        "SELECT 'tenant_21:' || COUNT(*)::text FROM public.clients WHERE id = 21;"
    )
    counts = {}
    for line in raw.splitlines():
        name, value = line.split(":", 1)
        counts[name] = int(value)
    if counts.get("clients", 0) < 1 or counts.get("users", 0) < 1 or counts.get("prospects", 0) < 1:
        raise RuntimeError("tenant_row_counts_missing")
    if counts.get("tenant_21", 0) < 1:
        raise RuntimeError("required_tenant_missing")
    return counts


def verify_fingerprints() -> dict[str, str]:
    fingerprints = {}
    for table in FINGERPRINT_TABLES:
        fingerprints[table] = sql_output(content_fingerprint_sql(table), timeout=180)
        if not re.fullmatch(r"[0-9a-f]{32}", fingerprints[table] or ""):
            raise RuntimeError("content_fingerprint_failed")
    return fingerprints


def process(_unused=None):
    try:
        tables = catalog_tables()
        preflight()
        with tempfile.TemporaryDirectory(prefix="pg18-nov21-restore-") as folder:
            archive = Path(folder) / "source.dump"
            toc = Path(folder) / "selective.toc"
            download(archive)
            build_toc(archive, toc, tables)
            expected = archive_counts(archive, toc, tables["core"])
            preflight()  # Fail-closed empty-target + disk + snapshot check before any write.
            restore(archive, toc)
            actual = verify_counts(tables, expected)
            sequences = verify_sequences(tables["sequences"])
            tenants = verify_tenants()
            fingerprints = verify_fingerprints()
            snapshot = guard_snapshot()
            identity = inspect_identity()
        message("RECONCILIATION_PASSED", {
            "database": identity[0],
            "postgres": identity[1],
            "objectCount": identity[2],
            "bytes": identity[3],
            "tables": len(actual),
            "pulseforgeTables": len(tables["core"]),
            "pulseforgeRows": sum(actual[name] for name in tables["core"]),
            "signalTables": len(tables["signal"]),
            "signalRows": sum(actual[name] for name in tables["signal"]),
            "sequences": sequences,
            "tenants": tenants,
            "fingerprints": fingerprints,
            "snapshotUnchanged": snapshot,
            "postgreSQLVersion": sql_output("SHOW server_version;"),
            "signalHistoryRetainedInArchive": True,
            "increaseRenderDisk": False,
        })
        message("COMPLETE", {
            "status": "RESTORED_AND_ROW_COUNTS_MATCH",
            "note": "November 21 Pulseforge-selective restore; Signal schema empty; archive preserved; cutover prohibited.",
        })
    except Exception as e:
        message("FAILED", {"reason": str(e) if isinstance(e, RuntimeError) else type(e).__name__})


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        return  # Avoid logging request URLs and headers.

    def _json(self, code: int, payload: dict):
        body = json.dumps(payload, sort_keys=True).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/healthz":
            try:
                if os.environ.get("PF_MIGRATION_STAGING") != "1":
                    raise RuntimeError("staging_disabled")
                sql_output("SELECT 1;")
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"isolated restore service available")
            except Exception:
                self.send_error(503)
            return
        if self.path == "/internal/migration/preflight":
            try:
                payload = preflight()
                self._json(200, payload)
            except RuntimeError as exc:
                reason = str(exc)
                code = 409 if reason in (
                    "pulseforge_restore_exceeds_render_disk",
                    "increase_render_disk_forbidden",
                    "full_restore_not_authorized_for_pulseforge_cutover",
                ) else 503
                self._json(code, {"status": "blocked", "reason": reason})
            except Exception:
                self._json(503, {"status": "blocked", "reason": "preflight_failed"})
            return
        self.send_error(404)

    def do_POST(self):
        global _started
        if self.path != "/internal/migration/restore-once":
            return self.send_error(404)
        supplied = self.headers.get("Authorization", "")
        desired = "Bearer " + os.environ.get("MIGRATION_BRIDGE_TOKEN", "")
        if len(desired) < 45 or not hmac.compare_digest(supplied, desired):
            return self.send_error(403)
        try:
            assert_staging_and_approval()
            assert_writers_disabled()
        except RuntimeError:
            return self.send_error(403)
        try:
            n = int(self.headers.get("Content-Length", "0"))
            if n < 0 or n > 8192:
                raise ValueError("invalid length")
            payload = {}
            if n > 0:
                payload = json.loads(self.rfile.read(n) or b"{}")
            if not isinstance(payload, dict):
                raise ValueError("bad request")
            if "downloadUrl" in payload:
                raise ValueError("presigned_url_restore_disabled")
            extra = set(payload) - {"source"}
            if extra:
                raise ValueError("bad request")
            if payload.get("source", "tigris") != "tigris":
                raise ValueError("bad request")
            tigris_settings()
        except Exception:
            return self.send_error(400)
        with _lock:
            if _started:
                return self.send_error(409)
            _started = True
        threading.Thread(target=process, daemon=True).start()
        self._json(202, {"status": "accepted", "source": "tigris"})


if __name__ == "__main__":
    if os.environ.get("PF_MIGRATION_STAGING") != "1":
        raise SystemExit("restore_gateway_requires_staging")
    port = int(os.environ.get("PORT", "10000"))
    try:
        message("PREFLIGHT", preflight())
    except Exception as e:
        reason = str(e) if isinstance(e, RuntimeError) else type(e).__name__
        message("PREFLIGHT_BLOCKED", {"reason": reason})
        if reason == "increase_render_disk_forbidden":
            message("DISK_FAIL_CLOSED", {
                "restoreFloor": PULSEFORGE_RESTORE_FLOOR,
                "defaultDiskBytes": DEFAULT_DISK_BYTES,
                "note": "Refusing to increase Render disk; Signal data stays in the archive.",
            })
    message("GATEWAY_READY")
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
