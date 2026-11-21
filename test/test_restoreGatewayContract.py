#!/usr/bin/env python3
"""Unit tests for the November 21 full Render restore contract helpers."""
from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts" / "migration"))

import restoreGatewayContract as contract  # noqa: E402

LIVE_RENDER_SIZES = {
    "postgres": 7952063,
    "pulseforge_production": 7935679,
    "pulseforge_production_migration": 991557311,
    "template0": 7935679,
    "template1": 7774735,
}
LIVE_DISK_BYTES = 10_000_000_000
PRESIGNED = (
    "https://migrationbackup.t3.storageapi.dev/"
    "migration-backups/railway-20261121-full-pg18.dump"
    "?X-Amz-Signature=abc&X-Amz-Expires=3600"
)
NOV17 = (
    "https://migrationbackup.t3.storageapi.dev/"
    "migration-backups/railway-20261117-full-pg18.dump"
    "?X-Amz-Signature=abc&X-Amz-Expires=3600"
)
PRODUCTION_URL = "postgresql://user:secret@dpg-dbtgtloae00c73dd8040-a/pulseforge_production"
SNAPSHOT_URL = "postgresql://user:secret@dpg-dbtgtloae00c73dd8040-a/pulseforge_production_migration"
RAILWAY_URL = "postgresql://user:secret@postgres.railway.internal/pulseforge_production"


def _toc_listing(tables):
    lines = ["; Archive created at 2026-11-21"]
    oid = 1000
    for name in sorted(tables):
        lines.append(f"{oid}; 0 {oid} TABLE public {name} postgres")
        oid += 1
        lines.append(f"{oid}; 0 {oid} TABLE DATA public {name} postgres")
        oid += 1
        lines.append(f"{oid}; 0 {oid} SEQUENCE SET public {name}_id_seq postgres")
        oid += 1
    return "\n".join(lines) + "\n"


class RestoreGatewayContractTest(unittest.TestCase):
    def test_nov21_pin(self):
        self.assertEqual(
            contract.ARCHIVE_OBJECT_KEY,
            "migration-backups/railway-20261121-full-pg18.dump",
        )
        self.assertEqual(contract.ARCHIVE_BYTES, 728012480)
        self.assertEqual(
            contract.ARCHIVE_SHA256,
            "4cf4e95bf56706e09ba411349ed68cfcf8725fef2ee4964ed6af73fce29fd734",
        )
        self.assertEqual(contract.TARGET_DATABASE, "pulseforge_production")
        self.assertEqual(
            contract.FORBIDDEN_SNAPSHOT_DATABASE, "pulseforge_production_migration")
        self.assertEqual(contract.HOST_PREFIX, "dpg-dbtgtloae00c73dd8040-a")
        self.assertIs(contract.CONTRACT["restoreAllTableData"], True)
        self.assertIs(contract.CONTRACT["silentlyDiscardSignalData"], False)
        self.assertIs(contract.CONTRACT["mergePullRequest1146"], False)
        self.assertEqual(
            contract.FORBIDDEN_ARCHIVE_OBJECT_KEY,
            "migration-backups/railway-20261117-full-pg18.dump",
        )
        self.assertEqual(contract.SOURCE_DATABASE_BYTES_FLOOR, 9218275007)
        self.assertEqual(contract.DEFAULT_DISK_BYTES, 10_000_000_000)
        contract.assert_backup_url(PRESIGNED)

    def test_reject_november_17_archive(self):
        with self.assertRaisesRegex(RuntimeError, "forbidden_november_17_archive"):
            contract.assert_backup_url(NOV17)
        with self.assertRaisesRegex(RuntimeError, "unsigned_or_overlong_backup_url"):
            contract.assert_backup_url(
                PRESIGNED.replace("X-Amz-Expires=3600", "X-Amz-Expires=3601")
            )
        with self.assertRaisesRegex(RuntimeError, "invalid_backup_object_url"):
            contract.assert_backup_url(PRESIGNED.replace("https://", "http://"))

    def test_reject_snapshot_target(self):
        contract.assert_restore_endpoint(PRODUCTION_URL)
        with self.assertRaisesRegex(RuntimeError, "snapshot_database_must_remain_untouched"):
            contract.assert_restore_endpoint(SNAPSHOT_URL)
        with self.assertRaisesRegex(RuntimeError, "railway_host_forbidden"):
            contract.assert_restore_endpoint(RAILWAY_URL)
        with self.assertRaisesRegex(RuntimeError, "unapproved_restore_database_endpoint"):
            contract.assert_restore_endpoint(
                "postgresql://user:secret@dpg-other-a/pulseforge_production"
            )

    def test_toc_includes_signal_and_forbids_commenting(self):
        tables = contract.catalog_tables()
        self.assertEqual(len(tables["all"]), 319)
        self.assertEqual(len(tables["core"]), 274)
        self.assertEqual(len(tables["signal"]), 45)
        listing = _toc_listing(tables["all"])
        selected = contract.select_restore_toc(listing, tables["all"])
        for name in tables["signal"]:
            self.assertIn(f"TABLE DATA public {name} ", selected)
            self.assertNotIn(f";TABLE DATA public {name} ", selected.replace(" ", ""))
        self.assertTrue(any(
            line.startswith(tuple("0123456789")) and "SEQUENCE SET" in line
            for line in selected.splitlines()
        ))
        signal_name = sorted(tables["signal"])[0]
        commented = []
        for line in listing.splitlines():
            if f"TABLE DATA public {signal_name} " in line:
                commented.append(";" + line)
            else:
                commented.append(line)
        with self.assertRaisesRegex(RuntimeError, "silent_table_data_exclusion_forbidden"):
            contract.select_restore_toc("\n".join(commented) + "\n", tables["all"])
        with self.assertRaisesRegex(RuntimeError, "unhandled_archive_data_kind"):
            contract.select_restore_toc(
                listing + "2300; 0 0 BLOBS blobs\n", tables["all"])

    def test_live_10gb_fails_full_restore_exceeds_render_disk(self):
        with self.assertRaisesRegex(RuntimeError, "full_restore_exceeds_render_disk"):
            contract.require_full_restore_capacity(LIVE_RENDER_SIZES, LIVE_DISK_BYTES)
        blocked = contract.assess_storage(LIVE_RENDER_SIZES, LIVE_DISK_BYTES)
        self.assertFalse(blocked["fits"])
        self.assertGreater(blocked["required_bytes"], LIVE_DISK_BYTES)
        self.assertEqual(blocked["restore_floor"], 9218275007)
        self.assertEqual(
            blocked["sibling_bytes"],
            LIVE_RENDER_SIZES["postgres"]
            + LIVE_RENDER_SIZES["pulseforge_production_migration"]
            + LIVE_RENDER_SIZES["template0"]
            + LIVE_RENDER_SIZES["template1"],
        )

    def test_20gb_passes_full_restore_capacity(self):
        assessment = contract.require_full_restore_capacity(
            LIVE_RENDER_SIZES, 20_000_000_000)
        self.assertTrue(assessment["fits"])
        self.assertLessEqual(assessment["required_bytes"], 20_000_000_000)

    def test_staging_and_writer_flags(self):
        staging = {
            "PF_MIGRATION_STAGING": "1",
            "MIGRATION_RESTORE_ONE_TIME_APPROVED": "ALLOW_EMPTY_RENDER_PG18_ONLY",
            "SIGNAL_OPERATOR_ENABLED": "0",
            "SIGNAL_OPPORTUNITY_ENGINE_ENABLED": "0",
            "SIGNAL_SHADOW_MODE": "0",
            "SIGNAL_DB_SEPARATION_REQUIRED": "0",
        }
        contract.assert_staging_and_approval(staging)
        contract.assert_writers_disabled(staging)
        disabled = dict(staging, PF_MIGRATION_STAGING="0")
        with self.assertRaisesRegex(RuntimeError, "migration_staging_not_enabled"):
            contract.assert_staging_and_approval(disabled)
        with self.assertRaisesRegex(RuntimeError, "restore_authority_not_enabled"):
            contract.assert_staging_and_approval(
                dict(staging, MIGRATION_RESTORE_ONE_TIME_APPROVED="no"))
        with self.assertRaisesRegex(RuntimeError, "cutover_flags_must_remain_off"):
            contract.assert_staging_and_approval(
                dict(staging, PF_PRODUCTION_TRAFFIC="1"))
        with self.assertRaisesRegex(RuntimeError, "writers_must_remain_disabled"):
            contract.assert_writers_disabled(
                dict(staging, SIGNAL_OPERATOR_ENABLED="1"))
        with self.assertRaisesRegex(RuntimeError, "writers_must_remain_disabled"):
            contract.assert_writers_disabled(
                dict(staging, SIGNAL_OPPORTUNITY_ENGINE_ENABLED="true"))
        with self.assertRaisesRegex(RuntimeError, "writers_must_remain_disabled"):
            contract.assert_writers_disabled(
                dict(staging, SIGNAL_SHADOW_MODE="1"))

    def test_empty_target_and_snapshot_guards(self):
        contract.assert_empty_postgres18(
            database="pulseforge_production", version="18.6", object_count=0)
        with self.assertRaisesRegex(RuntimeError, "target_is_not_empty_approved_postgres18"):
            contract.assert_empty_postgres18(
                database="pulseforge_production", version="18.6", object_count=1)
        with self.assertRaisesRegex(RuntimeError, "target_is_not_empty_approved_postgres18"):
            contract.assert_empty_postgres18(
                database="pulseforge_production_migration", version="18.6", object_count=0)
        contract.assert_snapshot_preserved(
            database="pulseforge_production_migration",
            version="180006",
            object_count=378,
            size_bytes=991557311,
        )
        with self.assertRaisesRegex(RuntimeError, "snapshot_object_count_changed"):
            contract.assert_snapshot_preserved(
                database="pulseforge_production_migration",
                version="180006",
                object_count=377,
                size_bytes=991557311,
            )
        with self.assertRaisesRegex(RuntimeError, "snapshot_size_regressed"):
            contract.assert_snapshot_preserved(
                database="pulseforge_production_migration",
                version="180006",
                object_count=378,
                size_bytes=100,
            )
        with self.assertRaisesRegex(RuntimeError, "snapshot_not_postgres18"):
            contract.assert_snapshot_preserved(
                database="pulseforge_production_migration",
                version="170004",
                object_count=378,
                size_bytes=991557311,
            )
        with self.assertRaisesRegex(RuntimeError, "snapshot_database_must_remain_untouched"):
            contract.assert_snapshot_preserved(
                database="pulseforge_production",
                version="180006",
                object_count=378,
                size_bytes=991557311,
            )

    def test_content_fingerprint_sql_is_row_hash_digest(self):
        sql = contract.content_fingerprint_sql("users")
        self.assertIn("md5(t::text)", sql)
        self.assertIn("public.users", sql)
        with self.assertRaisesRegex(RuntimeError, "invalid_fingerprint_table"):
            contract.content_fingerprint_sql("agent_log")

    def test_os_environ_staging_helpers(self):
        env = {
            "PF_MIGRATION_STAGING": "1",
            "MIGRATION_RESTORE_ONE_TIME_APPROVED": "ALLOW_EMPTY_RENDER_PG18_ONLY",
            "SIGNAL_OPERATOR_ENABLED": "0",
            "SIGNAL_OPPORTUNITY_ENGINE_ENABLED": "0",
            "SIGNAL_SHADOW_MODE": "0",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            for flag in (
                "SIGNAL_DB_SEPARATION_REQUIRED",
                "PF_MIGRATION_CUTOVER",
                "MIGRATION_CUTOVER_APPROVED",
                "PF_PRODUCTION_TRAFFIC",
            ):
                os.environ.pop(flag, None)
            contract.assert_staging_and_approval()
            contract.assert_writers_disabled()


if __name__ == "__main__":
    unittest.main()
