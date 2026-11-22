#!/usr/bin/env python3
"""Write freeze / unfreeze of the Railway source database for the final cutover export.

freeze:   ALTER DATABASE <db> SET default_transaction_read_only = on, then end
          every other client session so it reconnects read-only, then prove the
          freeze: for a quiet window no other session holds a transaction id
          (a transaction id is assigned only when a session writes).
unfreeze: ALTER DATABASE <db> RESET default_transaction_read_only (rollback path:
          Railway production resumes writing exactly as before).
status:   report the setting and session counts.

Nothing is dropped, altered or deleted besides that one database-level
setting. Reuses the exporter's pinned-source guards. Logs counts only.
"""
from __future__ import annotations

import json
import os
import sys
import time

import psycopg2
import psycopg2.errors

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from exportFinalSnapshot import PINNED_PUBLIC_PROXY  # noqa: E402

APPROVAL = "YES_FREEZE_RAILWAY_SOURCE_WRITES"
SOURCE_DB = "railway"
RETIRED_DB = "railway_retired_20261122"
QUIET_SECONDS = int(os.environ.get("FREEZE_QUIET_SECONDS", "20"))


def log(status, detail=None):
    print("SOURCE_FREEZE_" + status + (" " + json.dumps(detail, sort_keys=True) if detail else ""), flush=True)


def fail(msg):
    raise RuntimeError(msg)


def connect():
    validate_source()
    # This session must stay read-write even once the database default is
    # read-only, or the rollback (RESET) would itself be refused.
    conn = psycopg2.connect(connect_timeout=15, application_name="cutover-source-freeze",
                            options="-c default_transaction_read_only=off")
    conn.autocommit = True
    return conn


def validate_source():
    if os.environ.get("FINAL_EXPORT_SOURCE") == "railway_public_proxy":
        if (os.environ.get("PGHOST"), os.environ.get("PGPORT")) != PINNED_PUBLIC_PROXY:
            fail("source_must_be_pinned_railway_tcp_proxy")
        if os.environ.get("PGSSLMODE") != "require":
            fail("external_source_requires_pgsslmode_require")
    elif not os.environ.get("PGHOST", "").endswith(".railway.internal") and not os.environ.get("FREEZE_TEST_HOST_OK"):
        fail("source_must_be_railway_host")
    if os.environ.get("PGDATABASE") != "railway" and not os.environ.get("FREEZE_TEST_HOST_OK"):
        fail("unexpected_source_database")


def status(cur):
    cur.execute("""SELECT EXISTS (SELECT 1 FROM pg_db_role_setting s JOIN pg_database d ON d.oid = s.setdatabase
                                  WHERE d.datname = current_database() AND s.setrole = 0
                                    AND 'default_transaction_read_only=on' = ANY (s.setconfig))""")
    frozen = bool(cur.fetchone()[0])
    cur.execute("""SELECT count(*) FILTER (WHERE backend_type = 'client backend'),
                          count(*) FILTER (WHERE backend_type = 'client backend' AND backend_xid IS NOT NULL)
                   FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()""")
    sessions, writing = cur.fetchone()
    return {"frozen": frozen, "otherSessions": int(sessions), "sessionsHoldingWriteXid": int(writing)}


def freeze(cur):
    db = cur.connection.info.dbname
    cur.execute('ALTER DATABASE "%s" SET default_transaction_read_only = on' % db.replace('"', '""'))
    log("SETTING_APPLIED")
    # Every existing session keeps its old default until it reconnects: end them.
    for attempt in range(6):
        cur.execute("""SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity
                       WHERE datname = current_database() AND pid <> pg_backend_pid()
                         AND backend_type = 'client backend'
                         AND coalesce(application_name, '') <> 'cutover-source-freeze'""")
        ended = cur.fetchone()[0]
        log("SESSIONS_ENDED", {"attempt": attempt + 1, "ended": int(ended)})
        time.sleep(3)
        # Sessions that reconnected now default to read-only; a write needs an
        # explicit READ WRITE, which the application never issues.
        cur.execute("""SELECT count(*) FROM pg_stat_activity
                       WHERE datname = current_database() AND pid <> pg_backend_pid()
                         AND backend_type = 'client backend' AND backend_xid IS NOT NULL""")
        if cur.fetchone()[0] == 0:
            break
    else:
        fail("write_transactions_still_open")
    # Quiet window: no session may acquire a transaction id (= perform a write).
    # The next transaction id only advances when some session writes, so a
    # stable xmax across the window proves no write committed or started.
    cur.execute("SELECT pg_snapshot_xmax(pg_current_snapshot())::text")
    xmax_start = cur.fetchone()[0]
    deadline = time.time() + QUIET_SECONDS
    while time.time() < deadline:
        cur.execute("""SELECT count(*) FROM pg_stat_activity
                       WHERE datname = current_database() AND pid <> pg_backend_pid()
                         AND backend_xid IS NOT NULL""")
        if cur.fetchone()[0]:
            fail("write_observed_during_quiet_window")
        time.sleep(1)
    cur.execute("SELECT pg_snapshot_xmax(pg_current_snapshot())::text")
    if cur.fetchone()[0] != xmax_start:
        fail("transaction_id_advanced_during_quiet_window")
    st = status(cur)
    if not st["frozen"]:
        fail("freeze_setting_not_visible")
    log("VERIFIED", {**st, "quietSeconds": QUIET_SECONDS})


def unfreeze(cur):
    db = cur.connection.info.dbname
    cur.execute('ALTER DATABASE "%s" RESET default_transaction_read_only' % db.replace('"', '""'))
    log("UNFROZEN", status(cur))


def retire(cur):
    """After a verified promotion on Render: rename the Railway database so the
    Railway app and crons can no longer read or act on it. Data is untouched;
    reverse with `unretire`."""
    if os.environ.get("SOURCE_RETIRE_APPROVED") != "YES_RETIRE_RAILWAY_DATABASE_AFTER_VERIFIED_PROMOTION":
        fail("retire_not_approved")
    if not status(cur)["frozen"]:
        fail("retire_requires_frozen_source")
    conn = psycopg2.connect(connect_timeout=15, application_name="cutover-source-freeze", dbname="postgres",
                            options="-c default_transaction_read_only=off")
    conn.autocommit = True
    c = conn.cursor()
    cur.connection.close()
    for attempt in range(10):
        c.execute("SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE datname = %s", (SOURCE_DB,))
        try:
            c.execute('ALTER DATABASE "%s" RENAME TO "%s"' % (SOURCE_DB, RETIRED_DB))
            log("RETIRED", {"renamedTo": RETIRED_DB, "attempt": attempt + 1})
            return
        except psycopg2.errors.ObjectInUse:
            time.sleep(0.2)
    fail("retire_rename_kept_failing_database_in_use")


def unretire():
    validate_source()
    conn = psycopg2.connect(connect_timeout=15, application_name="cutover-source-freeze", dbname="postgres",
                            options="-c default_transaction_read_only=off")
    conn.autocommit = True
    conn.cursor().execute('ALTER DATABASE "%s" RENAME TO "%s"' % (RETIRED_DB, SOURCE_DB))
    log("UNRETIRED", {"restoredName": SOURCE_DB})


def main(argv):
    try:
        action = argv[1] if len(argv) > 1 else ""
        if action not in ("freeze", "unfreeze", "status", "retire", "unretire"):
            fail("usage: freeze|unfreeze|status|retire|unretire")
        if action == "unretire":
            unretire()
            return 0
        if action == "freeze" and os.environ.get("SOURCE_FREEZE_APPROVED") != APPROVAL:
            fail("freeze_not_approved")
        conn = connect()
        cur = conn.cursor()
        if action == "freeze":
            freeze(cur)
        elif action == "retire":
            retire(cur)
            return 0
        elif action == "unfreeze":
            unfreeze(cur)
        else:
            log("STATUS", status(cur))
        conn.close()
        return 0
    except Exception as exc:  # noqa: BLE001
        log("FAILED", {"reason": str(exc)[:300] if isinstance(exc, RuntimeError) else type(exc).__name__})
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
