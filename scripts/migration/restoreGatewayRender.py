#!/usr/bin/env python3
"""Isolated Render PostgreSQL 18 one-time restore receiver.

Accepts an authenticated short-lived presigned S3 GET URL; verifies the frozen
archive and selectively restores 274 Pulseforge table DATA entries. The common
source schema (including *empty* Signal tables) is restored as a rehearsal.
Never points at Railway, enables application workers, or starts live operations.
"""
from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import threading
from urllib.parse import parse_qs, urlsplit

import requests

ARCHIVE_BYTES = 727898203
ARCHIVE_SHA = "4d44ad1f73ffcb5a3ee48ee8b30c78c2430f803da05915b40b84b6101a736628"
ARCHIVE_KEY = "/migration-backups/railway-20261117-full-pg18.dump"
CATALOG_PATH = "/usr/local/share/migration/railway-source-catalog-20261117.json"
DATA_RE = re.compile(r"^\d+;\s+\d+\s+\d+\s+TABLE DATA\s+public\s+([a-z][a-z0-9_]*)\s")
SEQ_RE = re.compile(r"^\d+;\s+\d+\s+\d+\s+SEQUENCE SET\s+public\s+([a-z][a-z0-9_]*)\s")
COPY_RE = re.compile(rb"^COPY public\.([a-z][a-z0-9_]*) (?:\(|FROM )")
IDENT_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_lock = threading.Lock()
_started = False


def message(status: str, detail: dict | None = None):
    # Deliberately never logs URL, token, SQL, customer values, or connection info.
    print("MIGRATION_RESTORE_" + status + (" " + json.dumps(detail, sort_keys=True)
          if detail else ""), flush=True)


def call(cmd, *, timeout=90, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL):
    return subprocess.run(cmd, input=stdin, stdout=stdout, stderr=stderr,
                          timeout=timeout, check=True)


def sql_output(query: str, *, timeout=45) -> str:
    url = os.environ.get("DATABASE_URL", "")
    if not url.startswith(("postgresql://", "postgres://")):
        raise RuntimeError("missing_internal_render_database")
    target = urlsplit(url)
    # Production restore into pulseforge_production is not armed. Do not retarget
    # this gateway until a fresh Railway dump is verified and writers stay off.
    if (target.path != "/pulseforge_production_migration" or
            not (target.hostname or "").startswith("dpg-dbtgtloae00c73dd8040-a")):
        raise RuntimeError("unapproved_restore_database_endpoint")
    env = os.environ.copy()
    env["PGCONNECT_TIMEOUT"] = "12"
    return subprocess.run(
        ["psql", "-X", "-A", "-t", "-w", "-v", "ON_ERROR_STOP=1",
         "--dbname", url, "--file", "-"],
        input=query, env=env, capture_output=True, text=True, timeout=timeout, check=True
    ).stdout.strip()


def expected_tables():
    catalog = json.loads(Path(CATALOG_PATH).read_text())
    names = [x["table_name"] for x in catalog["objects"] if x["relkind"] in ("r", "p")]
    if len(names) != 319 or any(not IDENT_RE.fullmatch(n) for n in names):
        raise RuntimeError("invalid_snapshot_catalog")
    signal = {n for n in names if n.startswith("signal_")}
    core = set(names) - signal
    if len(signal) != 45 or len(core) != 274 or len(set(names)) != 319:
        raise RuntimeError("unexpected_table_partition")
    return core, signal


def preflight() -> None:
    if os.environ.get("PF_MIGRATION_STAGING") != "1":
        raise RuntimeError("migration_staging_not_enabled")
    if os.environ.get("MIGRATION_RESTORE_ONE_TIME_APPROVED") != "ALLOW_EMPTY_RENDER_PG18_ONLY":
        raise RuntimeError("restore_authority_not_enabled")
    current = sql_output(
        "SELECT current_database() || ':' || current_setting('server_version_num') || ':' || "
        "(SELECT count(*) FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid "
        "WHERE n.nspname NOT IN ('pg_catalog','information_schema') "
        "AND n.nspname NOT LIKE 'pg_toast%' AND c.relkind IN ('r','p','v','m','S'));"
    )
    db_name, version, count = current.split(":")
    if db_name != "pulseforge_production_migration" or not version.startswith("18") or int(count) != 0:
        raise RuntimeError("target_is_not_empty_approved_postgres18")


def download(url: str, filename: Path) -> None:
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    if (parts.scheme != "https" or
            not (host == "t3.storageapi.dev" or host.endswith(".t3.storageapi.dev")) or
            not parts.path.endswith(ARCHIVE_KEY) or ".." in parts.path or
            parts.username or parts.password or parts.fragment):
        raise RuntimeError("invalid_backup_object_url")
    q = parse_qs(parts.query)
    if ("X-Amz-Signature" not in q or "X-Amz-Expires" not in q or
            int(q["X-Amz-Expires"][0]) > 3600):
        raise RuntimeError("unsigned_or_overlong_backup_url")
    digest = hashlib.sha256()
    total = 0
    with requests.get(url, stream=True, allow_redirects=False, timeout=(15, 60)) as response:
        if response.status_code != 200:
            raise RuntimeError("bucket_download_failed_" + str(response.status_code))
        with filename.open("xb") as target:
            os.chmod(filename, 0o600)
            for chunk in response.iter_content(chunk_size=4*1024*1024):
                if chunk:
                    total += len(chunk)
                    if total > ARCHIVE_BYTES:
                        raise RuntimeError("backup_size_exceeded")
                    digest.update(chunk)
                    target.write(chunk)
    if total != ARCHIVE_BYTES or digest.hexdigest() != ARCHIVE_SHA:
        raise RuntimeError("backup_size_or_checksum_mismatch")
    with filename.open("rb") as item:
        if item.read(5) != b"PGDMP":
            raise RuntimeError("custom_archive_magic_missing")
    message("ARCHIVE_VERIFIED", {"bytes":total,"sha256":ARCHIVE_SHA})


def build_toc(archive: Path, toc: Path, core: set[str], signal: set[str]):
    listing=call(["pg_restore","--list",str(archive)],timeout=90).stdout.decode()
    data=set()
    signal_excluded=set()
    seq_excluded=0
    lines=[]
    for line in listing.splitlines():
        is_data=DATA_RE.match(line)
        is_seq=SEQ_RE.match(line)
        if is_data:
            name=is_data.group(1)
            if name not in core and name not in signal:
                raise RuntimeError("archive_table_not_in_catalog")
            if name in data:
                raise RuntimeError("duplicate_data_table")
            data.add(name)
            if name in signal:
                signal_excluded.add(name)
                lines.append(";" + line)
                continue
        if is_seq and is_seq.group(1).startswith("signal_"):
            seq_excluded += 1
            lines.append(";" + line)
            continue
        if re.search(r";\s+\d+\s+\d+\s+(MATERIALIZED VIEW DATA|BLOBS|BLOB)\s",line):
            raise RuntimeError("unhandled_archive_data_kind")
        lines.append(line)
    if len(data)!=319 or signal_excluded != signal:
        raise RuntimeError("archive_catalog_table_data_disagreement")
    toc.write_text("\n".join(lines)+"\n")
    message("SELECTION_VERIFIED",{"allTableData":len(data),"pulseforgeTableData":len(core),
      "signalTableDataExcluded":len(signal_excluded),"signalSequenceSetsExcluded":seq_excluded})


def archive_counts(archive: Path, toc: Path, core: set[str]) -> dict[str,int]:
    # Custom pg_restore emits COPY text; each row is one escaped physical line.
    # No customer values are retained or logged, only counts.
    command=["pg_restore","--data-only","--use-list",str(toc),"--file=-",str(archive)]
    p=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
    assert p.stdout is not None
    counts={}
    active=None
    try:
        for raw in p.stdout:
            if active is None:
                if raw.startswith(b"COPY public."):
                    match=COPY_RE.match(raw[:200])
                    if not match:
                        raise RuntimeError("unrecognized_copy_statement")
                    active=match.group(1).decode("ascii")
                    if active not in core or active in counts:
                        raise RuntimeError("unexpected_copy_table")
                    counts[active]=0
            elif raw in (b"\\.\n",b"\\.\r\n"):
                active=None
            else:
                counts[active]+=1
        if active is not None:
            raise RuntimeError("unterminated_copy_stream")
    except BaseException:
        p.kill()
        raise
    finally:
        p.stdout.close()
        result=p.wait(timeout=90)
    if result!=0 or set(counts)!=core:
        raise RuntimeError("archive_copy_count_failed")
    message("SOURCE_ROWS_COUNTED",{"tables":len(counts),"rows":sum(counts.values())})
    return counts


def restore(archive: Path, toc: Path):
    db=os.environ["DATABASE_URL"]
    message("TRANSACTION_STARTED")
    # Any pg_restore failure rolls back all prior schema and data changes.
    code=subprocess.run([
       "pg_restore","--exit-on-error","--single-transaction",
       "--no-owner","--no-acl","--no-tablespaces",
       "--use-list",str(toc),"--dbname",db,str(archive)
    ],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=1800).returncode
    if code!=0:
        raise RuntimeError("pg_restore_failed_code_"+str(code))
    message("TRANSACTION_COMMITTED")


def verify_counts(core,signal,expected):
    sql=[]
    for n in sorted(core|signal):
        sql.append("SELECT '"+n+"' || E'\\t' || COUNT(*)::text FROM public."+n+";")
    values=sql_output("\n".join(sql),timeout=180)
    actual={}
    for line in values.splitlines():
        name,raw=line.split("\t",1)
        actual[name]=int(raw)
    if set(actual)!=core|signal:
        raise RuntimeError("restored_table_inventory_mismatch")
    mismatches=[n for n in core if actual[n]!=expected[n]]
    unexpected=[n for n in signal if actual[n]!=0]
    if mismatches or unexpected:
        raise RuntimeError("restored_row_count_mismatch")
    message("RECONCILIATION_PASSED",{
      "pulseforgeTables":len(core),"pulseforgeRows":sum(actual[n] for n in core),
      "emptySignalSchemaTables":len(signal),
      "postgreSQLVersion":sql_output("SHOW server_version;")
    })


def process(url):
    try:
        preflight()
        core,signal=expected_tables()
        with tempfile.TemporaryDirectory(prefix="pg18-one-shot-restore-") as folder:
            archive=Path(folder)/"source.dump"
            toc=Path(folder)/"pulseforge.toc"
            download(url,archive)
            build_toc(archive,toc,core,signal)
            expected=archive_counts(archive,toc,core)
            preflight()  # A second fail-closed empty-target check before any write.
            restore(archive,toc)
            verify_counts(core,signal,expected)
        message("COMPLETE",{"status":"RESTORED_AND_ROW_COUNTS_MATCH",
            "note":"Source snapshot only; Signal schema exists empty; cutover prohibited."})
    except Exception as e:
        message("FAILED",{"reason":str(e) if isinstance(e,RuntimeError) else type(e).__name__})


class Handler(BaseHTTPRequestHandler):
    def log_message(self,*args):
        return  # Avoid logging request URLs and headers.
    def do_GET(self):
        if self.path=="/healthz":
            try:
                if os.environ.get("PF_MIGRATION_STAGING")!="1":
                    raise RuntimeError("staging_disabled")
                sql_output("SELECT 1;")
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"isolated restore service available")
            except Exception:
                self.send_error(503)
        else:
            self.send_error(404)
    def do_POST(self):
        global _started
        if self.path!="/internal/migration/restore-once":
            return self.send_error(404)
        supplied=self.headers.get("Authorization","")
        desired="Bearer "+os.environ.get("MIGRATION_BRIDGE_TOKEN","")
        if len(desired)<45 or not hmac.compare_digest(supplied,desired):
            return self.send_error(403)
        if (os.environ.get("PF_MIGRATION_STAGING")!="1" or
            os.environ.get("MIGRATION_RESTORE_ONE_TIME_APPROVED")!="ALLOW_EMPTY_RENDER_PG18_ONLY"):
            return self.send_error(403)
        try:
            n=int(self.headers.get("Content-Length","0"))
            if n<=0 or n>8192: raise ValueError("invalid length")
            payload=json.loads(self.rfile.read(n))
            if set(payload)!={"downloadUrl"} or not isinstance(payload["downloadUrl"],str):
                raise ValueError("bad request")
            target=urlsplit(payload["downloadUrl"])
            if target.scheme!="https" or not (target.hostname or "").endswith(".t3.storageapi.dev"):
                raise ValueError("bad host")
        except Exception:
            return self.send_error(400)
        with _lock:
            if _started:
                return self.send_error(409)
            _started=True
        threading.Thread(target=process,args=(payload["downloadUrl"],),daemon=True).start()
        self.send_response(202)
        self.send_header("Content-Type","application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"accepted"}')


if __name__=="__main__":
    if os.environ.get("PF_MIGRATION_STAGING")!="1":
        raise SystemExit("restore_gateway_requires_staging")
    port=int(os.environ.get("PORT","10000"))
    try:
        query=("SELECT n.nspname||'.'||c.relname||':'||c.relkind::text "
          "FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid "
          "WHERE n.nspname NOT IN ('pg_catalog','information_schema') "
          "AND n.nspname NOT LIKE 'pg_toast%' AND c.relkind IN ('r','p','v','m','S') "
          "ORDER BY n.nspname,c.relname LIMIT 40;")
        objects=sql_output(query).splitlines()
        total=int(sql_output("SELECT count(*) FROM pg_class c JOIN pg_namespace n "
          "ON c.relnamespace=n.oid WHERE n.nspname NOT IN ('pg_catalog','information_schema') "
          "AND n.nspname NOT LIKE 'pg_toast%' AND c.relkind IN ('r','p','v','m','S');"))
        message("TARGET_CATALOG_INSPECTED",{"nonSystemObjectCount":total,
          "sampleObjectNames":objects})
        size=int(sql_output("SELECT pg_database_size(current_database());"))
        create_priv=sql_output("SELECT rolcreatedb::text FROM pg_roles WHERE rolname=current_user;")
        sample=sql_output("SELECT 'users:'||COUNT(*)::text FROM public.users; "
           + "SELECT 'prospects:'||COUNT(*)::text FROM public.prospects; "
           + "SELECT 'signal_dashboard_records:'||COUNT(*)::text FROM public.signal_dashboard_records;")
        message("TARGET_REHEARSAL_READINESS",{"dbBytes":size,
          "canCreateLogicalDatabase":create_priv,
          "sampleRowCounts":sample.splitlines()})
    except Exception as e:
        message("TARGET_CATALOG_INSPECTION_UNAVAILABLE",{
          "exceptionType":type(e).__name__,
          "pgExitCode":getattr(e,"returncode",None),
          "pgError":getattr(e,"stderr","")[-350:] if hasattr(e,"stderr") else ""
        })
    message("GATEWAY_READY")
    ThreadingHTTPServer(("0.0.0.0",port),Handler).serve_forever()
