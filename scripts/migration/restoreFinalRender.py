#!/usr/bin/env python3
"""Render PostgreSQL 18 restore receiver for the Railway -> Render cutover.

Runs as the Render web service process only while PF_MIGRATION_RECEIVER=1
(see Dockerfile). The application does not run in this mode.

Upload (each request carries a GitHub Actions OIDC token for the
railway-final-export workflow on the pinned cutover branch; no shared secret):
  POST /internal/migration/upload/begin     {"mode": "rehearsal"|"final", "manifest": {...}}
  POST /internal/migration/upload/chunk?index=N   raw bytes, sequential, <= 64 MiB
  POST /internal/migration/upload/complete
  GET  /internal/migration/status
  GET  /healthz

1. The archive must match the manifest's byte count and SHA-256 (and
   EXPECTED_ARCHIVE_SHA256 when pinned) and carry the PGDMP header.
2. A NEW database is created on the same Render instance. Existing databases
   are never overwritten: rehearsal -> pulseforge_rehearsal_<stamp>,
   final -> pulseforge_cutover_<stamp>.
3. Schema for everything plus data for every non-Signal table is restored in ONE
   transaction, exit-on-error (Signal tables exist empty; Signal history stays
   in the full archive).
4. The destination is fingerprinted with the source's SQL and compared: rows,
   content digests, sequences, catalog counts. Any difference -> FAILED.
5. rehearsal: the verified rehearsal copy is dropped (it is only a test copy
   of Railway data and the Render disk is 10 GB).
   final: requires a manifest exported under the source write freeze. With
   MIGRATION_FINAL_SWAP_APPROVED, in one transaction the current primary
   database is renamed pulseforge_pre_cutover_<stamp> (kept) and the verified
   database takes the primary name, so the existing DATABASE_URL serves it.
   Nothing is dropped.
Logs counts and checksums only.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit, urlunsplit

import psycopg2

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fingerprintSql as fp  # noqa: E402
import githubOidc  # noqa: E402

PRIMARY_NAMES = ("pulseforge_production", "pulseforge_production_migration")
RENDER_HOST_PREFIX = "dpg-dbtgtloae00c73dd8040-a"
DISK_BYTES = int(os.environ.get("MIGRATION_RENDER_DISK_BYTES", str(10 * 1024 ** 3)))
DISK_CEILING = 0.85
CHUNK_MAX = 64 * 1024 * 1024
MANIFEST_MAX = 16 * 1024 * 1024
RESTORE_APPROVAL = "CREATE_NEW_DATABASE_ONLY"
SWAP_APPROVAL = "RENAME_PRIMARY_KEEP_AND_PROMOTE_VERIFIED"
DATA_RE = re.compile(r"^\d+;\s+\d+\s+\d+\s+TABLE DATA\s+public\s+([a-z_][a-z0-9_]*)\s")
SEQ_RE = re.compile(r"^\d+;\s+\d+\s+\d+\s+SEQUENCE SET\s+public\s+([a-z_][a-z0-9_]*)\s")

_lock = threading.Lock()
_session: dict | None = None
_state: dict = {"phase": "idle"}


def log(status, detail=None):
    print("FINAL_RESTORE_" + status + (" " + json.dumps(detail, sort_keys=True) if detail else ""), flush=True)
    _state.update({"lastEvent": status})
    _state.setdefault("events", []).append({"event": status, **(detail or {})})
    del _state["events"][:-20]


# ---------------------------------------------------------------- database

def base_url() -> str:
    url = os.environ.get("DATABASE_URL", "")
    parts = urlsplit(url)
    host_ok = (parts.hostname or "").startswith(RENDER_HOST_PREFIX) or os.environ.get("MIGRATION_TEST_ANY_HOST") == "1"
    if parts.scheme not in ("postgres", "postgresql") or not host_ok:
        raise RuntimeError("unapproved_render_database_endpoint")
    if parts.path.lstrip("/") not in PRIMARY_NAMES:
        raise RuntimeError("unexpected_primary_database_name")
    return url


def primary_db() -> str:
    return urlsplit(base_url()).path.lstrip("/")


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


def execute(db, sql, params=None):
    conn = connect(db, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall() if cur.description else None
    finally:
        conn.close()


def database_exists(name) -> bool:
    return bool(scalar("postgres", "SELECT count(*) FROM pg_database WHERE datname=%s", (name,)))


def database_sizes() -> dict[str, int]:
    rows = execute("postgres", "SELECT datname, pg_database_size(datname) FROM pg_database "
                               "WHERE NOT datistemplate ORDER BY 1")
    return {n: int(s) for n, s in rows}


def require_disk_headroom():
    sizes = database_sizes()
    estimate = int(sizes.get(primary_db(), 0) * 1.3) or 2 * 1024 ** 3
    used = sum(sizes.values())
    if used + estimate > DISK_BYTES * DISK_CEILING:
        raise RuntimeError("insufficient_render_disk_headroom")
    log("DISK_HEADROOM_OK", {"databases": sizes, "estimatedRestoreBytes": estimate, "diskBytes": DISK_BYTES})


# ----------------------------------------------------------------- restore

def verify_archive(path: Path, expected_sha: str, expected_bytes: int):
    digest, total = hashlib.sha256(), 0
    with path.open("rb") as f:
        head = f.read(5)
        f.seek(0)
        while True:
            block = f.read(4 * 1024 * 1024)
            if not block:
                break
            total += len(block)
            digest.update(block)
    if total != expected_bytes or digest.hexdigest() != expected_sha:
        raise RuntimeError("archive_size_or_checksum_mismatch")
    if head != b"PGDMP":
        raise RuntimeError("custom_archive_magic_missing")


def build_toc(archive: Path, toc: Path, manifest: dict):
    listing = subprocess.run(["pg_restore", "--list", str(archive)], capture_output=True,
                             check=True, timeout=300).stdout.decode()
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
    log("SELECTION_VERIFIED", {"tableData": len(data), "pulseforgeTables": len(core), "signalDataExcluded": excluded})


def destination_report(db: str) -> dict:
    conn = connect(db)
    conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
    try:
        cur = conn.cursor()
        fp.session_guards(cur)
        tables = fp.list_tables(cur)
        core = [t for t in tables if not fp.is_signal(t)]
        signal = [t for t in tables if fp.is_signal(t)]
        return {"catalog": fp.catalog_counts(cur), "fingerprints": fp.fingerprint_tables(cur, core),
                "sequences": fp.sequences(cur), "signalRowCounts": fp.count_tables(cur, signal)}
    finally:
        conn.rollback()
        conn.close()


def swap(cutover_db: str, stamp: str):
    primary = primary_db()
    keep_as = "pulseforge_pre_cutover_" + stamp
    if database_exists(keep_as):
        raise RuntimeError("pre_cutover_name_already_taken")
    conn = connect("postgres")
    try:
        cur = conn.cursor()
        # The application is not running in receiver mode; refuse if anyone is attached.
        cur.execute("SELECT count(*) FROM pg_stat_activity WHERE datname IN (%s,%s) AND pid<>pg_backend_pid()",
                    (primary, cutover_db))
        if cur.fetchone()[0]:
            raise RuntimeError("databases_in_use_swap_refused")
        cur.execute(f'ALTER DATABASE "{primary}" RENAME TO "{keep_as}"')
        cur.execute(f'ALTER DATABASE "{cutover_db}" RENAME TO "{primary}"')
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    log("SWAP_COMMITTED", {"primary": primary, "previousPrimaryKeptAs": keep_as})


def process(archive: Path, manifest: dict, mode: str, workdir: str):
    target = None
    _state["phase"] = "restoring"
    try:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        target = ("pulseforge_rehearsal_" if mode == "rehearsal" else "pulseforge_cutover_") + stamp
        if database_exists(target):
            raise RuntimeError("target_database_exists")
        if int(scalar(primary_db(), "SELECT current_setting('server_version_num')::int")) // 10000 != 18:
            raise RuntimeError("destination_not_postgres18")
        require_disk_headroom()
        toc = Path(workdir) / "selection.toc"
        build_toc(archive, toc, manifest)
        execute("postgres", f'CREATE DATABASE "{target}"')
        log("DATABASE_CREATED", {"database": target})
        code = subprocess.run(["pg_restore", "--exit-on-error", "--single-transaction", "--no-owner",
                               "--no-acl", "--no-tablespaces", "--use-list", str(toc),
                               "--dbname", url_for(target), str(archive)],
                              stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=5400)
        if code.returncode != 0:
            raise RuntimeError("pg_restore_failed: " + code.stderr.decode("utf-8", "replace").strip()[-300:])
        log("TRANSACTION_COMMITTED", {"database": target})
        execute(target, "ANALYZE")
        dest = destination_report(target)
        diffs = fp.compare(manifest, dest)
        # Sequences are not transactional: on a live (unfrozen) source the
        # manifest read can follow pg_dump's read while writers advance them.
        # Only a frozen source (required for final) makes them exact.
        advanced = []
        if manifest.get("sourceFrozen") is not True:
            advanced = [d for d in diffs if d["kind"] == "sequence_behind"]
            diffs = [d for d in diffs if d["kind"] != "sequence_behind"]
        summary = {"mode": mode, "database": target, "snapshotCapturedAt": manifest["snapshotCapturedAt"],
                   "pulseforgeTables": len(dest["fingerprints"]),
                   "pulseforgeRows": sum(v["rows"] for v in dest["fingerprints"].values()),
                   "emptySignalTables": len(dest["signalRowCounts"]), "catalog": dest["catalog"],
                   "sequencesCompared": len(manifest["sequences"]), "differences": len(diffs),
                   "sourceFrozen": manifest.get("sourceFrozen") is True,
                   "sequencesAdvancedWhileSourceLive": len(advanced)}
        if diffs:
            log("RECONCILIATION_FAILED", {**summary, "firstDifferences": diffs[:25]})
            if mode == "rehearsal":
                execute("postgres", f'DROP DATABASE "{target}"')
                log("REHEARSAL_COPY_DROPPED", {"database": target})
            _state["phase"] = "failed"
            return
        log("RECONCILIATION_PASSED", summary)
        if mode == "rehearsal":
            execute("postgres", f'DROP DATABASE "{target}"')
            log("REHEARSAL_COPY_DROPPED", {"database": target})
        elif os.environ.get("MIGRATION_FINAL_SWAP_APPROVED") == SWAP_APPROVAL:
            swap(target, stamp)
        else:
            log("SWAP_NOT_APPROVED", {"verifiedDatabase": target})
        _state["phase"] = "complete"
        log("COMPLETE", {"mode": mode, "database": target})
    except Exception as exc:  # noqa: BLE001
        _state["phase"] = "failed"
        log("FAILED", {"reason": str(exc)[:400] if isinstance(exc, RuntimeError) else type(exc).__name__,
                       "database": target})
        if mode == "rehearsal" and target:
            try:
                if database_exists(target):
                    execute("postgres", f'DROP DATABASE "{target}"')
                    log("REHEARSAL_COPY_DROPPED", {"database": target})
            except Exception:  # noqa: BLE001
                pass
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


# --------------------------------------------------------------------- http

def validate_manifest(manifest: dict, mode: str):
    if not isinstance(manifest, dict):
        raise ValueError("manifest_not_object")
    if not re.fullmatch(r"[0-9a-f]{64}", str(manifest.get("sha256", ""))):
        raise ValueError("manifest_sha_missing")
    pinned = os.environ.get("EXPECTED_ARCHIVE_SHA256", "")
    if pinned and pinned != manifest["sha256"]:
        raise ValueError("manifest_sha_does_not_match_pinned_sha")
    if not isinstance(manifest.get("bytes"), int) or not 1024 <= manifest["bytes"] <= 8 * 1024 ** 3:
        raise ValueError("manifest_bytes_invalid")
    if manifest.get("serverVersionNum", 0) // 10000 != 18:
        raise ValueError("source_not_postgres18")
    for key in ("fingerprints", "sequences", "catalog", "signalTables", "snapshotCapturedAt"):
        if key not in manifest:
            raise ValueError("manifest_missing_" + key)
    if mode == "final" and manifest.get("sourceFrozen") is not True:
        raise ValueError("final_requires_frozen_source_snapshot")


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        return

    def reply(self, code, payload):
        body = json.dumps(payload, sort_keys=True).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def claims(self):
        auth = self.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            raise githubOidc.OidcError("missing_token")
        return githubOidc.verify(auth[7:].strip())

    def read_body(self, limit):
        n = int(self.headers.get("Content-Length", "0"))
        if n <= 0 or n > limit:
            raise ValueError("bad_length")
        return self.rfile.read(n)

    def do_GET(self):
        if self.path == "/healthz":
            try:
                scalar("postgres", "SELECT 1")
                return self.reply(200, {"receiver": "ready"})
            except Exception:  # noqa: BLE001
                return self.reply(503, {"receiver": "database_unavailable"})
        if self.path == "/internal/migration/status":
            try:
                self.claims()
            except githubOidc.OidcError as exc:
                return self.reply(403, {"error": str(exc)})
            return self.reply(200, dict(_state))
        self.reply(404, {"error": "not_found"})

    def do_POST(self):  # noqa: C901
        global _session
        route = urlsplit(self.path)
        try:
            claims = self.claims()
        except githubOidc.OidcError as exc:
            return self.reply(403, {"error": str(exc)})
        if os.environ.get("MIGRATION_RESTORE_APPROVED") != RESTORE_APPROVAL:
            return self.reply(403, {"error": "restore_not_approved"})
        run = str(claims["run_id"])
        try:
            if route.path == "/internal/migration/upload/begin":
                body = json.loads(self.read_body(MANIFEST_MAX))
                mode, manifest = body.get("mode"), body.get("manifest")
                if set(body) != {"mode", "manifest"} or mode not in ("rehearsal", "final"):
                    raise ValueError("bad_request")
                validate_manifest(manifest, mode)
                with _lock:
                    if _session or _state.get("phase") == "restoring":
                        return self.reply(409, {"error": "upload_or_restore_in_progress"})
                    workdir = tempfile.mkdtemp(prefix="final-restore-")
                    archive = Path(workdir) / "source.dump"
                    fd = os.open(archive, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                    os.close(fd)
                    _session = {"run": run, "mode": mode, "manifest": manifest, "workdir": workdir,
                                "archive": archive, "next": 0, "bytes": 0}
                    _state.clear()
                    _state.update({"phase": "uploading", "mode": mode, "runId": run})
                log("UPLOAD_BEGUN", {"mode": mode, "runId": run, "bytes": manifest["bytes"],
                                     "sha256": manifest["sha256"]})
                return self.reply(200, {"status": "ready", "chunkMax": CHUNK_MAX})
            with _lock:
                s = _session
                if not s or s["run"] != run:
                    return self.reply(409, {"error": "no_upload_for_this_run"})
                if route.path == "/internal/migration/upload/chunk":
                    index = int(parse_qs(route.query).get("index", ["-1"])[0])
                    if index != s["next"]:
                        return self.reply(409, {"error": "out_of_order_chunk", "expected": s["next"]})
                    data = self.read_body(CHUNK_MAX)
                    if s["bytes"] + len(data) > s["manifest"]["bytes"]:
                        raise ValueError("archive_larger_than_manifest")
                    with open(s["archive"], "ab") as f:
                        f.write(data)
                    s["next"] += 1
                    s["bytes"] += len(data)
                    return self.reply(200, {"received": s["bytes"]})
                if route.path == "/internal/migration/upload/complete":
                    manifest = s["manifest"]
                    try:
                        verify_archive(s["archive"], manifest["sha256"], manifest["bytes"])
                    except RuntimeError as exc:
                        shutil.rmtree(s["workdir"], ignore_errors=True)
                        _session = None
                        _state["phase"] = "failed"
                        log("FAILED", {"reason": str(exc)})
                        return self.reply(400, {"error": str(exc)})
                    log("ARCHIVE_VERIFIED", {"bytes": manifest["bytes"], "sha256": manifest["sha256"],
                                             "snapshotCapturedAt": manifest["snapshotCapturedAt"]})
                    _session = None
                    _state["phase"] = "restoring"
                    threading.Thread(target=process, args=(s["archive"], manifest, s["mode"], s["workdir"]),
                                     daemon=True).start()
                    return self.reply(202, {"status": "restoring"})
            return self.reply(404, {"error": "not_found"})
        except (ValueError, json.JSONDecodeError) as exc:
            return self.reply(400, {"error": str(exc)[:120]})


if __name__ == "__main__":
    base_url()
    if "PostgreSQL) 18." not in subprocess.run(["pg_restore", "--version"], capture_output=True, text=True).stdout:
        log("FAILED", {"reason": "pg_restore_18_required"})
        sys.exit(1)
    log("INSTANCE_DATABASES", {"databases": database_sizes(), "primary": primary_db()})
    log("RECEIVER_READY")
    ThreadingHTTPServer(("0.0.0.0", int(os.environ.get("PORT", "10000"))), Handler).serve_forever()
