#!/usr/bin/env python3
"""Unit tests for the November 21 Pulseforge-selective Render restore contract."""
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


def _active_data(selected, name):
    return any(
        (not line.startswith(";")) and f"TABLE DATA public {name} " in line
        for line in selected.splitlines()
    )


def _commented_data(selected, name):
    return any(
        line.startswith(";") and f"TABLE DATA public {name} " in line
        for line in selected.splitlines()
    )


class RestoreGatewayContractTest(unittest.TestCase):
    def test_nov21_pin_and_selective_authorization(self):
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
        self.assertIs(contract.CONTRACT["restoreAllTableData"], False)
        self.assertIs(contract.CONTRACT["restorePulseforgeTableData"], True)
        self.assertIs(contract.CONTRACT["restoreSignalTableData"], False)
        self.assertIs(contract.CONTRACT["authorizedSignalDataExclusion"], True)
        self.assertIs(contract.CONTRACT["silentlyDiscardSignalData"], False)
        self.assertIs(contract.CONTRACT["signalHistoryRetainedInArchive"], True)
        self.assertIs(contract.CONTRACT["increaseRenderDisk"], False)
        self.assertEqual(contract.CONTRACT["pulseforgeTableDataCount"], 274)
        self.assertEqual(contract.CONTRACT["signalTableDataExcluded"], 45)
        self.assertIs(contract.CONTRACT["mergePullRequest1146"], False)
        self.assertEqual(
            contract.FORBIDDEN_ARCHIVE_OBJECT_KEY,
            "migration-backups/railway-20261117-full-pg18.dump",
        )
        self.assertEqual(contract.PULSEFORGE_RESTORE_FLOOR, 1032675328)
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

    def test_toc_includes_274_operational_and_excludes_45_signal_data(self):
        tables = contract.catalog_tables()
        self.assertEqual(len(tables["all"]), 319)
        self.assertEqual(len(tables["core"]), 274)
        self.assertEqual(len(tables["signal"]), 45)
        for name in (
            "prospect_signal_events",
            "warm_signal_state",
            "warm_signal_events",
        ):
            self.assertIn(name, tables["core"])
            self.assertNotIn(name, tables["signal"])
        listing = _toc_listing(tables["all"])
        selected, detail = contract.select_restore_toc(listing, tables)
        self.assertEqual(detail["pulseforgeTableData"], 274)
        self.assertEqual(detail["signalTableDataExcluded"], 45)
        self.assertIs(detail["restoreAllTableData"], False)
        self.assertIs(detail["authorizedSignalDataExclusion"], True)
        self.assertIs(detail["silentlyDiscardSignalData"], False)
        self.assertIs(detail["signalHistoryRetainedInArchive"], True)
        included = excluded = 0
        for name in tables["core"]:
            self.assertTrue(_active_data(selected, name), name)
            self.assertFalse(_commented_data(selected, name), name)
            included += 1
        for name in tables["signal"]:
            self.assertTrue(_commented_data(selected, name), name)
            self.assertFalse(_active_data(selected, name), name)
            self.assertTrue(
                any(
                    (not line.startswith(";")) and f"TABLE public {name} " in line
                    for line in selected.splitlines()
                ),
                name,
            )
            excluded += 1
        self.assertEqual(included, 274)
        self.assertEqual(excluded, 45)
        self.assertTrue(
            any(
                line.startswith(";") and "SEQUENCE SET public signal_" in line
                for line in selected.splitlines()
            )
        )
        self.assertTrue(
            any(
                (not line.startswith(";")) and "SEQUENCE SET public users_id_seq " in line
                for line in selected.splitlines()
            )
        )

    def test_toc_forbids_excluding_operational_data(self):
        tables = contract.catalog_tables()
        listing = _toc_listing(tables["all"])
        commented = []
        for line in listing.splitlines():
            if "TABLE DATA public users " in line:
                commented.append(";" + line)
            else:
                commented.append(line)
        with self.assertRaisesRegex(RuntimeError, "operational_table_data_exclusion_forbidden"):
            contract.select_restore_toc("\n".join(commented) + "\n", tables)
        with self.assertRaisesRegex(RuntimeError, "unhandled_archive_data_kind"):
            contract.select_restore_toc(
                listing + "2300; 0 0 BLOBS blobs\n", tables)

    def test_live_10gb_fits_pulseforge_restore_and_forbids_disk_increase(self):
        assessment = contract.require_pulseforge_restore_capacity(
            LIVE_RENDER_SIZES, LIVE_DISK_BYTES)
        self.assertTrue(assessment["fits"])
        self.assertLess(assessment["required_bytes"], LIVE_DISK_BYTES)
        self.assertEqual(assessment["restore_floor"], 1032675328)
        self.assertFalse(assessment["includesSignalData"])
        self.assertFalse(assessment["increaseRenderDisk"])
        self.assertEqual(
            assessment["sibling_bytes"],
            LIVE_RENDER_SIZES["postgres"]
            + LIVE_RENDER_SIZES["pulseforge_production_migration"]
            + LIVE_RENDER_SIZES["template0"]
            + LIVE_RENDER_SIZES["template1"],
        )
        self.assertEqual(
            assessment["required_bytes"],
            int(1032675328 * 1.12) + assessment["sibling_bytes"],
        )
        with self.assertRaisesRegex(RuntimeError, "increase_render_disk_forbidden"):
            contract.require_pulseforge_restore_capacity(
                LIVE_RENDER_SIZES, 20_000_000_000)
        with self.assertRaisesRegex(RuntimeError, "full_restore_not_authorized_for_pulseforge_cutover"):
            contract.require_full_restore_capacity(LIVE_RENDER_SIZES, LIVE_DISK_BYTES)
        with self.assertRaisesRegex(RuntimeError, "increase_render_disk_forbidden"):
            contract.pinned_disk_bytes(10_000_000_001)

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
        with self.assertRaisesRegex(RuntimeError, "invalid_fingerprint_table"):
            contract.content_fingerprint_sql("signal_dashboard_records")

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
