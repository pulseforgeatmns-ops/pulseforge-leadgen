#!/usr/bin/env python3
"""Pure helpers for the November 21 Pulseforge-selective Render restore.

Restores 274 Pulseforge operational tables into empty pulseforge_production.
Signal table DATA is excluded by explicit cutover authorization; Signal schema
is kept empty for compatibility. The full November 21 archive, including
Signal history, must remain in object storage. Do not increase the 10 GB
Render disk. Do not merge PR #1146 or enable writers.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from urllib.parse import parse_qs, urlsplit, urlunsplit

IDENT_RE = re.compile(r"^[a-z][a-z0-9_]*$")
DATA_RE = re.compile(r"^\d+;\s+\d+\s+\d+\s+TABLE DATA\s+public\s+([a-z][a-z0-9_]*)\s")
SEQ_RE = re.compile(r"^\d+;\s+\d+\s+\d+\s+SEQUENCE SET\s+public\s+([a-z][a-z0-9_]*)\s")
BLOB_RE = re.compile(r";\s+\d+\s+\d+\s+(MATERIALIZED VIEW DATA|BLOBS|BLOB)\s")
RAILWAY_HOST_SUFFIXES = (
    ".railway.internal",
    ".proxy.rlwy.net",
    ".rlwy.net",
    ".railway.app",
)
FINGERPRINT_TABLES = ("users", "clients", "prospects")
PULSEFORGE_SIGNAL_EXCEPTIONS = (
    "prospect_signal_events",
    "warm_signal_state",
    "warm_signal_events",
)
WRITER_FLAGS = (
    "SIGNAL_OPERATOR_ENABLED",
    "SIGNAL_OPPORTUNITY_ENGINE_ENABLED",
    "SIGNAL_SHADOW_MODE",
)
CUTOVER_FLAGS = (
    "SIGNAL_DB_SEPARATION_REQUIRED",
    "PF_MIGRATION_CUTOVER",
    "MIGRATION_CUTOVER_APPROVED",
    "PF_PRODUCTION_TRAFFIC",
)
ENABLED_FLAG_VALUES = {"1", "true", "TRUE", "yes", "YES", "on", "ON"}

FORBIDDEN_ARCHIVE_OBJECT_KEY = "migration-backups/railway-20261117-full-pg18.dump"
PULSEFORGE_RESTORE_FLOOR = 1_032_675_328
SOURCE_DATABASE_BYTES_FLOOR = 9_218_275_007
DEFAULT_DISK_BYTES = 10_000_000_000
WAL_HEADROOM_RATIO = 0.12
SNAPSHOT_OBJECT_COUNT = 378
SNAPSHOT_MIN_BYTES = 900 * 1024 * 1024
PHYSICAL_TABLE_COUNT = 319
CORE_TABLE_COUNT = 274
SIGNAL_TABLE_COUNT = 45
APPROVED_RESTORE = "ALLOW_EMPTY_RENDER_PG18_ONLY"


def _share_path(name: str) -> Path:
    docker = Path("/usr/local/share/migration") / name
    if docker.is_file():
        return docker
    return Path(__file__).resolve().parents[2] / "docs" / "migration" / name


def contract_path() -> Path:
    return _share_path("railway-restore-contract-20261121.json")


def catalog_path() -> Path:
    env = os.environ.get("MIGRATION_CATALOG_PATH")
    if env:
        return Path(env)
    return _share_path("railway-source-catalog-20261117.json")


def load_contract(path: Path | None = None) -> dict:
    payload = json.loads((path or contract_path()).read_text())
    required = (
        "objectKey",
        "bytes",
        "sha256",
        "targetDatabase",
        "forbiddenSnapshotDatabase",
        "hostPrefix",
        "restoreAllTableData",
        "restorePulseforgeTableData",
        "restoreSignalTableData",
        "authorizedSignalDataExclusion",
        "silentlyDiscardSignalData",
        "signalHistoryRetainedInArchive",
        "increaseRenderDisk",
        "pulseforgeTableDataCount",
        "signalTableDataExcluded",
        "mergePullRequest1146",
        "tigrisEndpoint",
        "tigrisRegion",
        "tigrisBucket",
    )
    missing = [key for key in required if key not in payload]
    if missing:
        raise RuntimeError("restore_contract_incomplete")
    if payload["mergePullRequest1146"] is not False:
        raise RuntimeError("merge_of_pull_request_1146_forbidden")
    if payload["restoreAllTableData"] is not False:
        raise RuntimeError("full_restore_not_authorized_for_pulseforge_cutover")
    if payload["restorePulseforgeTableData"] is not True:
        raise RuntimeError("pulseforge_table_data_required")
    if payload["restoreSignalTableData"] is not False:
        raise RuntimeError("signal_table_data_not_part_of_pulseforge_cutover")
    if payload["authorizedSignalDataExclusion"] is not True:
        raise RuntimeError("signal_data_exclusion_not_authorized")
    if payload["silentlyDiscardSignalData"] is not False:
        raise RuntimeError("silent_signal_discard_forbidden")
    if payload["signalHistoryRetainedInArchive"] is not True:
        raise RuntimeError("signal_archive_must_be_preserved")
    if payload["increaseRenderDisk"] is not False:
        raise RuntimeError("increase_render_disk_forbidden")
    if int(payload["pulseforgeTableDataCount"]) != CORE_TABLE_COUNT:
        raise RuntimeError("pulseforge_table_count_mismatch")
    if int(payload["signalTableDataExcluded"]) != SIGNAL_TABLE_COUNT:
        raise RuntimeError("signal_table_count_mismatch")
    if payload["tigrisEndpoint"] != "https://t3.storageapi.dev":
        raise RuntimeError("unrecognized_s3_endpoint")
    if payload["tigrisRegion"] != "auto":
        raise RuntimeError("unrecognized_s3_region")
    if payload["tigrisBucket"] != "migrationbackup-njzkny-vy":
        raise RuntimeError("unapproved_tigris_bucket")
    return payload


CONTRACT = load_contract()
ARCHIVE_OBJECT_KEY = CONTRACT["objectKey"]
ARCHIVE_BYTES = int(CONTRACT["bytes"])
ARCHIVE_SHA256 = CONTRACT["sha256"]
TARGET_DATABASE = CONTRACT["targetDatabase"]
FORBIDDEN_SNAPSHOT_DATABASE = CONTRACT["forbiddenSnapshotDatabase"]
HOST_PREFIX = CONTRACT["hostPrefix"]
TIGRIS_ENDPOINT = CONTRACT["tigrisEndpoint"]
TIGRIS_REGION = CONTRACT["tigrisRegion"]
TIGRIS_BUCKET = CONTRACT["tigrisBucket"]
APPROVED_TIGRIS_BUCKETS = frozenset({TIGRIS_BUCKET})


def _flag_enabled(value: str | None) -> bool:
    return str(value or "") in ENABLED_FLAG_VALUES


def _database_name(url: str) -> str:
    path = urlsplit(url).path or ""
    return path[1:] if path.startswith("/") else path


def is_railway_host(host: str) -> bool:
    host = (host or "").lower()
    return any(host.endswith(suffix) for suffix in RAILWAY_HOST_SUFFIXES)


def tigris_settings(env=None) -> dict:
    """Return pinned Tigris settings. Never log the returned secrets."""
    env = os.environ if env is None else env
    if not env.get("S3_ACCESS_KEY_ID") or not env.get("S3_SECRET_ACCESS_KEY"):
        raise RuntimeError("missing_tigris_credentials")
    if env.get("S3_ENDPOINT"):
        host = (urlsplit(env["S3_ENDPOINT"]).hostname or "").lower()
        if host != "t3.storageapi.dev":
            raise RuntimeError("unrecognized_s3_endpoint")
    if env.get("S3_REGION") and env["S3_REGION"] not in ("auto", "iad"):
        raise RuntimeError("unrecognized_s3_region")
    bucket = env.get("S3_BUCKET") or TIGRIS_BUCKET
    if bucket not in APPROVED_TIGRIS_BUCKETS:
        raise RuntimeError("unapproved_tigris_bucket")
    if ARCHIVE_OBJECT_KEY != CONTRACT["objectKey"] or ".." in ARCHIVE_OBJECT_KEY:
        raise RuntimeError("unpinned_archive_key")
    return {
        "endpoint": TIGRIS_ENDPOINT,
        "region": TIGRIS_REGION,
        "bucket": bucket,
        "key": ARCHIVE_OBJECT_KEY,
        "access_key_id": env["S3_ACCESS_KEY_ID"],
        "secret_access_key": env["S3_SECRET_ACCESS_KEY"],
    }


def write_verified_archive(body, filename: Path) -> int:
    """Stream an archive to disk and require the pinned size, SHA-256, and PGDMP magic."""
    digest = hashlib.sha256()
    total = 0
    with filename.open("xb") as target:
        os.chmod(filename, 0o600)
        while True:
            chunk = body.read(4 * 1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > ARCHIVE_BYTES:
                raise RuntimeError("backup_size_exceeded")
            digest.update(chunk)
            target.write(chunk)
    if total != ARCHIVE_BYTES or digest.hexdigest() != ARCHIVE_SHA256:
        raise RuntimeError("backup_size_or_checksum_mismatch")
    with filename.open("rb") as item:
        if item.read(5) != b"PGDMP":
            raise RuntimeError("custom_archive_magic_missing")
    return total


def assert_backup_url(url: str) -> None:
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    path = parts.path or ""
    if FORBIDDEN_ARCHIVE_OBJECT_KEY in path or "railway-20261117-full-pg18.dump" in path:
        raise RuntimeError("forbidden_november_17_archive")
    if (
        parts.scheme != "https"
        or not (host == "t3.storageapi.dev" or host.endswith(".t3.storageapi.dev"))
        or not path.endswith("/" + ARCHIVE_OBJECT_KEY)
        or ".." in path
        or parts.username
        or parts.password
        or parts.fragment
    ):
        raise RuntimeError("invalid_backup_object_url")
    query = parse_qs(parts.query)
    try:
        expires = int(query.get("X-Amz-Expires", [""])[0])
    except (TypeError, ValueError):
        raise RuntimeError("unsigned_or_overlong_backup_url") from None
    if "X-Amz-Signature" not in query or expires > 3600:
        raise RuntimeError("unsigned_or_overlong_backup_url")


def assert_restore_endpoint(url: str) -> None:
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    path = parts.path or ""
    database = _database_name(url)
    if database == FORBIDDEN_SNAPSHOT_DATABASE or path.rstrip("/") == "/" + FORBIDDEN_SNAPSHOT_DATABASE:
        raise RuntimeError("snapshot_database_must_remain_untouched")
    if is_railway_host(host):
        raise RuntimeError("railway_host_forbidden")
    if not host.startswith(HOST_PREFIX) or path != "/" + TARGET_DATABASE:
        raise RuntimeError("unapproved_restore_database_endpoint")


def snapshot_database_url(url: str) -> str:
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    if is_railway_host(host) or not host.startswith(HOST_PREFIX):
        raise RuntimeError("unapproved_snapshot_read_endpoint")
    return urlunsplit((
        parts.scheme,
        parts.netloc,
        "/" + FORBIDDEN_SNAPSHOT_DATABASE,
        parts.query,
        parts.fragment,
    ))


def catalog_tables(path: str | Path | None = None) -> dict[str, set[str]]:
    catalog = json.loads(Path(path or catalog_path()).read_text())
    names = [item["table_name"] for item in catalog["objects"] if item["relkind"] in ("r", "p")]
    sequences = [item["table_name"] for item in catalog["objects"] if item["relkind"] == "S"]
    if len(names) != PHYSICAL_TABLE_COUNT or any(not IDENT_RE.fullmatch(name) for name in names):
        raise RuntimeError("invalid_snapshot_catalog")
    signal = {name for name in names if name.startswith("signal_")}
    core = set(names) - signal
    if (
        len(signal) != SIGNAL_TABLE_COUNT
        or len(core) != CORE_TABLE_COUNT
        or len(set(names)) != PHYSICAL_TABLE_COUNT
    ):
        raise RuntimeError("unexpected_table_partition")
    missing_exceptions = [name for name in PULSEFORGE_SIGNAL_EXCEPTIONS if name not in core]
    if missing_exceptions:
        raise RuntimeError("pulseforge_signal_exceptions_missing")
    return {
        "all": set(names),
        "core": core,
        "signal": signal,
        "sequences": set(sequences),
    }


def select_restore_toc(listing: str, tables) -> tuple[str, dict]:
    """Keep Pulseforge TABLE DATA; skip Signal TABLE DATA and SEQUENCE SET.

    Signal TABLE / schema entries stay in the TOC so empty Signal relations
    exist for application compatibility. Signal history remains in the archive.
    """
    if isinstance(tables, dict):
        expected = set(tables["all"])
        core = set(tables["core"])
        signal = set(tables["signal"])
    else:
        expected = set(tables)
        signal = {name for name in expected if name.startswith("signal_")}
        core = expected - signal
    data: set[str] = set()
    signal_excluded: set[str] = set()
    seq_excluded = 0
    lines: list[str] = []
    for raw in listing.splitlines():
        line = raw.rstrip("\n")
        commented = line.startswith(";")
        body = line.lstrip(";") if commented else line
        data_match = DATA_RE.match(body)
        seq_match = SEQ_RE.match(body)
        if data_match:
            name = data_match.group(1)
            if name not in expected:
                raise RuntimeError("archive_table_not_in_catalog")
            if name in core:
                if commented:
                    raise RuntimeError("operational_table_data_exclusion_forbidden")
                if name in data:
                    raise RuntimeError("duplicate_data_table")
                data.add(name)
                lines.append(line)
                continue
            if name in signal:
                if name in signal_excluded:
                    raise RuntimeError("duplicate_data_table")
                signal_excluded.add(name)
                lines.append(line if commented else ";" + line)
                continue
        if seq_match and seq_match.group(1).startswith("signal_"):
            seq_excluded += 1
            lines.append(line if commented else ";" + line)
            continue
        if BLOB_RE.search(line):
            raise RuntimeError("unhandled_archive_data_kind")
        lines.append(line)
    if data != core:
        raise RuntimeError("archive_catalog_table_data_disagreement")
    if signal_excluded != signal:
        raise RuntimeError("signal_table_data_exclusion_incomplete")
    if len(data) != CORE_TABLE_COUNT:
        raise RuntimeError("pulseforge_table_count_mismatch")
    if len(signal_excluded) != SIGNAL_TABLE_COUNT:
        raise RuntimeError("signal_table_count_mismatch")
    detail = {
        "pulseforgeTableData": len(data),
        "signalTableDataExcluded": len(signal_excluded),
        "signalSequenceSetsExcluded": seq_excluded,
        "restoreAllTableData": False,
        "authorizedSignalDataExclusion": True,
        "silentlyDiscardSignalData": False,
        "signalHistoryRetainedInArchive": True,
        "increaseRenderDisk": False,
    }
    return "\n".join(lines) + "\n", detail


def pinned_disk_bytes(disk_bytes: int | None = None) -> int:
    requested = DEFAULT_DISK_BYTES if disk_bytes is None else int(disk_bytes)
    if requested > DEFAULT_DISK_BYTES:
        raise RuntimeError("increase_render_disk_forbidden")
    return DEFAULT_DISK_BYTES


def assess_storage(
    database_sizes,
    disk_bytes: int | None = None,
    restore_floor: int | None = None,
) -> dict:
    disk = pinned_disk_bytes(disk_bytes)
    floor = PULSEFORGE_RESTORE_FLOOR if restore_floor is None else int(restore_floor)
    sibling_bytes = sum(
        int(size) for name, size in dict(database_sizes).items() if name != TARGET_DATABASE
    )
    required_bytes = int(floor * (1 + WAL_HEADROOM_RATIO)) + sibling_bytes
    return {
        "restore_floor": floor,
        "sibling_bytes": sibling_bytes,
        "required_bytes": required_bytes,
        "disk_bytes": disk,
        "headroom_ratio": WAL_HEADROOM_RATIO,
        "fits": required_bytes <= disk,
        "increaseRenderDisk": False,
        "includesSignalData": False,
    }


def require_pulseforge_restore_capacity(
    database_sizes,
    disk_bytes: int | None = None,
    restore_floor: int | None = None,
) -> dict:
    assessment = assess_storage(database_sizes, disk_bytes=disk_bytes, restore_floor=restore_floor)
    if not assessment["fits"]:
        raise RuntimeError("pulseforge_restore_exceeds_render_disk")
    return assessment


def require_full_restore_capacity(*_args, **_kwargs):
    raise RuntimeError("full_restore_not_authorized_for_pulseforge_cutover")


def assert_empty_postgres18(*, database: str, version: str, object_count: int) -> None:
    if database != TARGET_DATABASE or not str(version).startswith("18") or int(object_count) != 0:
        raise RuntimeError("target_is_not_empty_approved_postgres18")


def assert_snapshot_preserved(
    *,
    database: str,
    version: str,
    object_count: int,
    size_bytes: int,
) -> None:
    if database != FORBIDDEN_SNAPSHOT_DATABASE:
        raise RuntimeError("snapshot_database_must_remain_untouched")
    if not str(version).startswith("18"):
        raise RuntimeError("snapshot_not_postgres18")
    if int(object_count) != SNAPSHOT_OBJECT_COUNT:
        raise RuntimeError("snapshot_object_count_changed")
    if int(size_bytes) < SNAPSHOT_MIN_BYTES:
        raise RuntimeError("snapshot_size_regressed")


def assert_staging_and_approval(env=None) -> None:
    env = os.environ if env is None else env
    if env.get("PF_MIGRATION_STAGING") != "1":
        raise RuntimeError("migration_staging_not_enabled")
    if env.get("MIGRATION_RESTORE_ONE_TIME_APPROVED") != APPROVED_RESTORE:
        raise RuntimeError("restore_authority_not_enabled")
    for flag in CUTOVER_FLAGS:
        if _flag_enabled(env.get(flag)):
            raise RuntimeError("cutover_flags_must_remain_off")
    if CONTRACT["mergePullRequest1146"] is not False:
        raise RuntimeError("merge_of_pull_request_1146_forbidden")
    if CONTRACT["increaseRenderDisk"] is not False:
        raise RuntimeError("increase_render_disk_forbidden")


def assert_writers_disabled(env=None) -> None:
    env = os.environ if env is None else env
    for flag in WRITER_FLAGS:
        if _flag_enabled(env.get(flag)):
            raise RuntimeError("writers_must_remain_disabled")


def content_fingerprint_sql(table: str) -> str:
    if table not in FINGERPRINT_TABLES or not IDENT_RE.fullmatch(table):
        raise RuntimeError("invalid_fingerprint_table")
    return (
        "SELECT md5(coalesce(string_agg(h, '' ORDER BY h), '')) "
        f"FROM (SELECT md5(t::text) AS h FROM public.{table} t) s;"
    )
