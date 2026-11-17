#!/usr/bin/env python3
"""Portable, read-only Railway PostgreSQL backup to the machine running this script.

Requires PostgreSQL 18 client binaries (pg_dump and pg_restore), Python 3,
free disk space, and DATABASE_PUBLIC_URL in the environment.
No data is written to Railway's volume; no credentials are stored in files.
"""
import datetime as dt
import hashlib
import os
import pathlib
import shutil
import subprocess
import sys
import urllib.parse


def die(message):
    raise SystemExit("Backup aborted: " + message)


def main():
    connection = os.environ.get("DATABASE_PUBLIC_URL", "").strip()
    if not connection:
        die("Set DATABASE_PUBLIC_URL locally (never paste it into chat or commit it).")

    parsed = urllib.parse.urlsplit(connection)
    if parsed.scheme not in ("postgresql", "postgres") or not parsed.hostname:
        die("DATABASE_PUBLIC_URL must be a valid PostgreSQL connection URL.")
    if not parsed.username:
        die("URL lacks PostgreSQL username.")
    dbname = parsed.path.lstrip("/")
    if not dbname:
        die("URL lacks database name.")

    # Fail closed before pg_dump if the user copied Railway's internal URL
    # or accidentally concatenated multiple URLs/variable references.
    if parsed.hostname.endswith(".internal") or ".railway.internal" in parsed.hostname:
        die("Private Railway host detected. In Railway Postgres use DATABASE_PUBLIC_URL (public TCP proxy), not DATABASE_URL.")
    if "${{" in connection or "}}" in connection:
        die("URL contains a Railway variable placeholder rather than a resolved value.")
    if connection.count("postgres://") + connection.count("postgresql://") != 1:
        die("Connection input appears to contain more than one URL.")
    if dbname != "railway":
        die("Unexpected database path. It should be exactly /railway. Copy only one resolved public connection URL.")

    if not shutil.which("pg_dump") or not shutil.which("pg_restore"):
        die("Install PostgreSQL 18 client tools: pg_dump and pg_restore.")
    version = subprocess.run(["pg_dump", "--version"], capture_output=True, text=True, check=True).stdout
    try:
        major = int(version.split("PostgreSQL)", 1)[1].strip().split(".")[0].split()[0])
    except (ValueError, IndexError):
        die("Cannot verify pg_dump major version.")
    if major != 18:
        die("Use PostgreSQL 18 pg_dump (source server is PostgreSQL 18.6).")

    outdir = pathlib.Path(os.environ.get("BACKUP_DIR", ".")).expanduser().resolve()
    if not outdir.is_dir():
        die("BACKUP_DIR must be an existing directory.")
    free_bytes = shutil.disk_usage(outdir).free
    if free_bytes < 10 * 1024**3:
        die("Require at least 10 GiB free locally for conservative safety; available %.2f GiB." % (free_bytes / 1024**3))

    safe_dbname = "".join(x for x in dbname if x.isalnum() or x in ("-", "_"))[:40] or "database"
    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    destination = outdir / f"railway-{safe_dbname}-{timestamp}.dump"
    partial = outdir / (destination.name + ".partial")

    # Password and URL never appear in process arguments or log output.
    env = dict(os.environ)
    env.pop("DATABASE_PUBLIC_URL", None)
    env.update({
        "PGHOST": parsed.hostname,
        "PGPORT": str(parsed.port or 5432),
        "PGUSER": urllib.parse.unquote(parsed.username),
        "PGPASSWORD": urllib.parse.unquote(parsed.password or ""),
        "PGDATABASE": urllib.parse.unquote(dbname),
        "PGSSLMODE": "require",
        "PGCONNECT_TIMEOUT": "15",
        "PGAPPNAME": "pulseforge-readonly-offsite-backup",
        "PGOPTIONS": "-c default_transaction_read_only=on",
    })
    os.umask(0o077)
    print("Starting read-only PostgreSQL 18 logical backup to this machine.")
    print("Output:", str(destination))
    print("Nothing will be written to Railway's database volume.")
    try:
        subprocess.run(
            ["pg_dump", "--format=custom", "--compress=6", "--no-owner", "--no-acl",
             "--file", str(partial)],
            env=env, check=True,
        )
        if partial.stat().st_size == 0:
            die("Dump file is empty.")
        with open(os.devnull, "w") as sink:
            subprocess.run(["pg_restore", "--list", str(partial)], stdout=sink, check=True)

        partial.replace(destination)
        digest = hashlib.file_digest(destination.open("rb"), "sha256").hexdigest()
        checksum = destination.with_suffix(destination.suffix + ".sha256")
        checksum.write_text(f"{digest}  {destination.name}\n", encoding="utf-8")
        print("Dump is readable by pg_restore --list.")
        print("Backup:", destination)
        print("Size (GiB):", round(destination.stat().st_size / 1024**3, 3))
        print("SHA-256:", digest)
        print("Checksum manifest:", checksum)
        print("Important: archive validation is NOT a full restore test.")
    except Exception:
        if partial.exists():
            partial.unlink()
        raise


if __name__ == "__main__":
    main()
