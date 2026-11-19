#!/usr/bin/env python3
"""Render PostgreSQL 18 restore receiver for the Railway -> Render cutover.

POST /internal/migration/restore-final (bearer token) with
  {"downloadUrl": <presigned dump>, "manifestUrl": <presigned manifest>, "mode": "rehearsal"|"final"}

1. Verifies the archive bytes/SHA-256 against EXPECTED_ARCHIVE_SHA256 (pinned by the
   operator from the Railway export log) and the manifest.
2. CREATEs a NEW database on the same Render instance (never touches an existing one):
   rehearsal -> pulseforge_rehearsal_<stamp>, final -> pulseforge_cutover_<stamp>.
3. Restores schema for everything and data for every non-Signal table, in ONE
   transaction, exit-on-error (Signal tables exist empty, as on 2026-11-17).
4. Fingerprints the destination with the same SQL as the source snapshot and
   compares rows, content digests, sequences and catalog counts. Any difference
   = FAILED, and nothing else happens.
5. final mode only, with MIGRATION_FINAL_SWAP_APPROVED: in one transaction
   renames the 2026-11-17 database to pulseforge_snapshot_20261117 (kept) and the
   verified cutover database to pulseforge_production_migration, so the existing
   internal DATABASE_URL points at the new data. Nothing is dropped.
Logs counts and checksums only.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import subprocess
import sys
import tempfile
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit, urlunsplit

import psycopg2
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fingerprintSql as fp  # noqa: E402

PRIMARY_DB = "pulseforge_production_migration"
SNAPSHOT_DB = "pulseforge_snapshot_20261117"
RENDER_HOST_PREFIX = "dpg-dbtgtloae00c73dd8040-a"
DATA_RE = re.compile(r"^\d+;\s+\d+\s+\d+\s+TABLE DATA\s+public\s+([a-z_][a-z0-9_]*)\s")
SEQ_RE = re.compile(r"^\d+;\s+\d+\s+\d+\s+SEQUENCE SET\s+public\s+([a-z_][a-z0-9_]*)\s")
_lock = threading.Lock()
_started = False


def log(status, detail=None):
    print("FINAL_RESTORE_" + status + (" " + json.dumps(detail, sort_keys=True) if detail else ""), flush=True)


def base_url() -> str:
    url = os.environ.get("DATABASE_URL", "")
    parts = urlsplit(url)
    if parts.scheme not in ("postgres", "postgresql") or not (parts.hostname or "").startswith(RENDER_HOST_PREFIX):
        raise RuntimeError("unapproved_render_database_endpoint")
    if parts.path != "/" + PRIMARY_DB:
        raise RuntimeError("unexpected_primary_database_name")
    return url


def url_for(db: str) -> str:
    parts = urlsplit(base_url())
    return urlunsplit((parts.scheme, parts.netloc, "/" + db, parts.query, ""))


def connect(db: str, autocommit=False):
    conn = psycopg2.connect(url_for(db), connect_timeout=12, application_name="final-restore")
    conn.autocommit = autocommit
    return conn


def scalar(db, sql, params=None):
    conn = connect(db, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()[0]
    finally:
        conn.close()


def execute(db, sql):
    conn = connect(db, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall() if cur.description else None
    finally:
        conn.close()


def database_exists(name) -> bool:
    return bool(scalar("postgres", "SELECT count(*) FROM pg_database WHERE datname=%s", (name,)))


def checked_url(url: str, suffix: str) -> str:
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    if (parts.scheme != "https" or not (host == "t3.storageapi.dev" or host.endswith(".t3.storageapi.dev"))
            or not parts.path.endswith(suffix) or ".." in parts.path or parts.username or parts.password):
        raise RuntimeError("invalid_object_url")
    q = parse_qs(parts.query)
    if "X-Amz-Signature" not in q or int(q.get("X-Amz-Expires", ["99999"])[0]) > 3600:
        raise RuntimeError("unsigned_or_overlong_object_url")
    return url


def download(url: str, target: Path, expected_sha: str, expected_bytes: int):
    digest, total = hashlib.sha256(), 0
    with requests.get(url, stream=True, allow_redirects=False, timeout=(15, 120)) as r:
        if r.status_code != 200:
            raise RuntimeError("bucket_download_failed_%d" % r.status_code)
        with target.open("xb") as out:
            os.chmod(target, 0o600)
            for chunk in r.iter_content(chunk_size=4 * 1024 * 1024):
                if chunk:
                    total += len(chunk)
                    if total > expected_bytes:
                        raise RuntimeError("archive_size_exceeded")
                    digest.update(chunk)
                    out.write(chunk)
    if total != expected_bytes or digest.hexdigest() != expected_sha:
        raise RuntimeError("archive_size_or_checksum_mismatch")
    with target.open("rb") as f:
        if f.read(5) != b"PGDMP":
            raise RuntimeError("custom_archive_magic_missing")


def build_toc(archive: Path, toc: Path, manifest: dict):
    listing = subprocess.run(["pg_restore", "--list", str(archive)], capture_output=True,
                             check=True, timeout=180).stdout.decode()
    core = set(manifest["fingerprints"])
    signal = set(manifest["signalTables"])
    data, lines, excluded = set(), [], 0
    for line in listing.splitlines():
        d, s = DATA_RE.match(line), SEQ_RE.match(line)
        if d:
            name = d.group(1)
            if name not in core and name not in signal:
                raise RuntimeError("archive_table_not_in_manifest")
            data.add(name)
            if name in signal:
                lines.append(";" + line)
                excluded += 1
                continue
        if s and fp.is_signal(s.group(1)):
            lines.append(";" + line)
            continue
        if re.search(r";\s+\d+\s+\d+\s+(MATERIALIZED VIEW DATA|BLOBS|BLOB)\s", line):
            raise RuntimeError("unhandled_archive_data_kind")
        lines.append(line)
    if data != core | signal:
        raise RuntimeError("archive_manifest_table_disagreement")
    toc.write_text("\n".join(lines) + "\n")
    log("SELECTION_VERIFIED", {"tableData": len(data), "pulseforgeData": len(core), "signalDataExcluded": excluded})


def destination_report(db: str, manifest: dict) -> dict:
    conn = connect(db)
    conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
    try:
        cur = conn.cursor()
        cur.execute("SET LOCAL work_mem = '64MB'")
        tables = fp.list_tables(cur)
        core = [t for t in tables if not fp.is_signal(t)]
        signal = [t for t in tables if fp.is_signal(t)]
        return {"catalog": fp.catalog_counts(cur), "fingerprints": fp.fingerprint_tables(cur, core),
                "sequences": fp.sequences(cur), "signalRowCounts": fp.count_tables(cur, signal)}
    finally:
        conn.rollback()
        conn.close()


def swap(cutover_db: str):
    if os.environ.get("MIGRATION_FINAL_SWAP_APPROVED") != "RENAME_SNAPSHOT_KEEP_AND_PROMOTE_VERIFIED":
        log("SWAP_NOT_APPROVED", {"verifiedDatabase": cutover_db})
        return
    if database_exists(SNAPSHOT_DB):
        raise RuntimeError("snapshot_archive_name_already_taken")
    conn = connect("postgres")
    try:
        cur = conn.cursor()
        # Our own pooled connections are not held; refuse if anyone else is attached.
        cur.execute("SELECT count(*) FROM pg_stat_activity WHERE datname IN (%s,%s) AND pid<>pg_backend_pid()",
                    (PRIMARY_DB, cutover_db))
        if cur.fetchone()[0]:
            raise RuntimeError("databases_in_use_swap_refused")
        cur.execute(f'ALTER DATABASE "{PRIMARY_DB}" RENAME TO "{SNAPSHOT_DB}"')
        cur.execute(f'ALTER DATABASE "{cutover_db}" RENAME TO "{PRIMARY_DB}"')
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    log("SWAP_COMMITTED", {"primary": PRIMARY_DB, "previousSnapshotKeptAs": SNAPSHOT_DB})


def process(download_url: str, manifest_url: str, mode: str):
    target = None
    try:
        expected_sha = os.environ.get("EXPECTED_ARCHIVE_SHA256", "")
        if not re.fullmatch(r"[0-9a-f]{64}", expected_sha):
            raise RuntimeError("expected_archive_sha_not_pinned")
        r = requests.get(manifest_url, allow_redirects=False, timeout=(15, 60))
        if r.status_code != 200:
            raise RuntimeError("manifest_download_failed_%d" % r.status_code)
        manifest = r.json()
        if manifest.get("sha256") != expected_sha:
            raise RuntimeError("manifest_sha_does_not_match_pinned_sha")
        if manifest.get("serverVersionNum", 0) // 10000 != 18:
            raise RuntimeError("source_not_postgres18")
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M")
        target = ("pulseforge_rehearsal_" if mode == "rehearsal" else "pulseforge_cutover_") + stamp
        if database_exists(target):
            raise RuntimeError("target_database_exists")
        if int(scalar(PRIMARY_DB, "SELECT current_setting('server_version_num')::int")) // 10000 != 18:
            raise RuntimeError("destination_not_postgres18")
        with tempfile.TemporaryDirectory(prefix="final-restore-") as folder:
            archive, toc = Path(folder) / "source.dump", Path(folder) / "selection.toc"
            download(download_url, archive, expected_sha, int(manifest["bytes"]))
            log("ARCHIVE_VERIFIED", {"bytes": manifest["bytes"], "sha256": expected_sha,
                                     "snapshotCapturedAt": manifest["snapshotCapturedAt"]})
            build_toc(archive, toc, manifest)
            execute("postgres", f'CREATE DATABASE "{target}"')
            log("DATABASE_CREATED", {"database": target})
            code = subprocess.run(["pg_restore", "--exit-on-error", "--single-transaction", "--no-owner",
                                   "--no-acl", "--no-tablespaces", "--use-list", str(toc),
                                   "--dbname", url_for(target), str(archive)],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=3600)
            if code.returncode != 0:
                raise RuntimeError("pg_restore_failed: " + code.stderr.decode("utf-8", "replace").strip()[-300:])
            log("TRANSACTION_COMMITTED", {"database": target})
        execute(target, "ANALYZE")
        dest = destination_report(target, manifest)
        diffs = fp.compare(manifest, dest)
        summary = {"database": target, "pulseforgeTables": len(dest["fingerprints"]),
                   "pulseforgeRows": sum(v["rows"] for v in dest["fingerprints"].values()),
                   "emptySignalTables": len(dest["signalRowCounts"]), "catalog": dest["catalog"],
                   "sequencesCompared": len(manifest["sequences"]), "differences": len(diffs)}
        if diffs:
            log("RECONCILIATION_FAILED", {**summary, "firstDifferences": diffs[:25]})
            return
        log("RECONCILIATION_PASSED", summary)
        if mode == "final":
            swap(target)
        log("COMPLETE", {"mode": mode, "database": target})
    except Exception as exc:  # noqa: BLE001
        log("FAILED", {"reason": str(exc)[:400] if isinstance(exc, RuntimeError) else type(exc).__name__,
                       "database": target})


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        return

    def do_GET(self):
        if self.path != "/healthz":
            return self.send_error(404)
        try:
            scalar("postgres", "SELECT 1")
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"final restore receiver available")
        except Exception:
            self.send_error(503)

    def do_POST(self):
        global _started
        if self.path != "/internal/migration/restore-final":
            return self.send_error(404)
        desired = "Bearer " + os.environ.get("MIGRATION_BRIDGE_TOKEN", "")
        if len(desired) < 45 or not hmac.compare_digest(self.headers.get("Authorization", ""), desired):
            return self.send_error(403)
        if os.environ.get("MIGRATION_FINAL_RESTORE_APPROVED") != "CREATE_NEW_DATABASE_ONLY":
            return self.send_error(403)
        try:
            n = int(self.headers.get("Content-Length", "0"))
            if n <= 0 or n > 16384:
                raise ValueError
            body = json.loads(self.rfile.read(n))
            if set(body) != {"downloadUrl", "manifestUrl", "mode"} or body["mode"] not in ("rehearsal", "final"):
                raise ValueError
            checked_url(body["downloadUrl"], ".dump")
            checked_url(body["manifestUrl"], ".dump.manifest.json")
        except Exception:
            return self.send_error(400)
        with _lock:
            if _started:
                return self.send_error(409)
            _started = True
        threading.Thread(target=process, args=(body["downloadUrl"], body["manifestUrl"], body["mode"]),
                         daemon=True).start()
        self.send_response(202)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"accepted"}')


if __name__ == "__main__":
    base_url()
    rows = execute("postgres", "SELECT datname, pg_database_size(datname) FROM pg_database WHERE NOT datistemplate ORDER BY 1")
    log("INSTANCE_DATABASES", {"databases": {n: int(s) for n, s in rows}})
    log("GATEWAY_READY")
    ThreadingHTTPServer(("0.0.0.0", int(os.environ.get("PORT", "10000"))), Handler).serve_forever()
