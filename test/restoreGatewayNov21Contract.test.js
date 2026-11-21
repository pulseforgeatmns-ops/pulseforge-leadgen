'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {
  validateMigrationDatabaseUrl,
  EXPECTED_STAGING_DB,
  EXPECTED_PRODUCTION_DB,
} = require('../scripts/migration/validateMigrationDatabaseUrl');

const root = path.resolve(__dirname, '..');
const read = (rel) => fs.readFileSync(path.join(root, rel), 'utf8');
const contract = JSON.parse(read('docs/migration/railway-restore-contract-20261121.json'));
const stagingHost = 'dpg-dbtgtloae00c73dd8040-a';
const stagingUrl = `postgresql://user:secret@${stagingHost}/pulseforge_production_migration`;
const productionUrl = `postgresql://user:secret@${stagingHost}/pulseforge_production`;

test('pins the November 21 full restore archive and destination', () => {
  assert.equal(contract.objectKey, 'migration-backups/railway-20261121-full-pg18.dump');
  assert.equal(contract.bytes, 728012480);
  assert.equal(contract.sha256, '4cf4e95bf56706e09ba411349ed68cfcf8725fef2ee4964ed6af73fce29fd734');
  assert.equal(contract.targetDatabase, 'pulseforge_production');
  assert.equal(contract.forbiddenSnapshotDatabase, 'pulseforge_production_migration');
  assert.equal(contract.hostPrefix, 'dpg-dbtgtloae00c73dd8040-a');
  assert.equal(contract.restoreAllTableData, true);
  assert.equal(contract.silentlyDiscardSignalData, false);
  assert.equal(contract.mergePullRequest1146, false);
});

test('gateway helpers and receiver target production and keep Signal table data', () => {
  const helpers = read('scripts/migration/restoreGatewayContract.py');
  const gateway = read('scripts/migration/restoreGatewayRender.py');
  assert.match(helpers, /ARCHIVE_OBJECT_KEY/);
  assert.match(helpers, /forbidden_november_17_archive/);
  assert.match(helpers, /snapshot_database_must_remain_untouched/);
  assert.match(helpers, /silent_table_data_exclusion_forbidden/);
  assert.match(helpers, /full_restore_exceeds_render_disk/);
  assert.match(helpers, /SOURCE_DATABASE_BYTES_FLOOR = 9218275007/);
  assert.match(helpers, /SIGNAL_OPERATOR_ENABLED/);
  assert.match(helpers, /SIGNAL_OPPORTUNITY_ENGINE_ENABLED/);
  assert.match(helpers, /SIGNAL_SHADOW_MODE/);
  assert.match(gateway, /from restoreGatewayContract import/);
  assert.match(gateway, /pulseforge_production/);
  assert.match(gateway, /--exit-on-error/);
  assert.match(gateway, /--single-transaction/);
  assert.match(gateway, /\/internal\/migration\/restore-once/);
  assert.match(gateway, /\/internal\/migration\/preflight/);
  assert.match(gateway, /\/healthz/);
  assert.match(gateway, /md5\(t::text\)|content_fingerprint_sql/);
  assert.match(gateway, /tenant_21|id = 21/);
  assert.doesNotMatch(gateway, /lines\.append\(";" \+ line\)/);
  assert.doesNotMatch(gateway, /signalTableDataExcluded/);
});

test('ticket sender pins November 21 bytes and rejects the November 17 key', () => {
  const ticket = read('scripts/migration/sendRenderRestoreTicket.py');
  assert.match(ticket, /migration-backups\/railway-20261121-full-pg18.dump/);
  assert.match(ticket, /728012480/);
  assert.match(ticket, /head_object/);
  assert.match(ticket, /ContentLength/);
  assert.match(ticket, /forbidden_november_17_archive/);
  assert.match(ticket, /railway-20261117-full-pg18.dump/);
});

test('restore Dockerfile copies contract helpers and both JSON files', () => {
  const dockerfile = read('Dockerfile.migration-render-restore');
  assert.match(dockerfile, /restoreGatewayContract\.py/);
  assert.match(dockerfile, /restoreGatewayRender\.py/);
  assert.match(dockerfile, /railway-source-catalog-20261117\.json/);
  assert.match(dockerfile, /railway-restore-contract-20261121\.json/);
});

test('staging Dockerfile runs the November 21 contract test with the existing four', () => {
  const dockerfile = read('Dockerfile');
  assert.match(
    dockerfile,
    /node --test test\/migrationSplitManifest\.test\.js test\/signalDatabaseRouting\.test\.js test\/migrationDatabaseTarget\.test\.js test\/maxAnchorOperationalInspectionIntent\.test\.js test\/restoreGatewayNov21Contract\.test\.js/,
  );
});

test('staging validateMigrationDatabaseUrl still defaults to the snapshot name', () => {
  assert.equal(EXPECTED_STAGING_DB, 'pulseforge_production_migration');
  assert.equal(EXPECTED_PRODUCTION_DB, 'pulseforge_production');
  assert.deepEqual(validateMigrationDatabaseUrl(stagingUrl), {
    expectedDatabase: 'pulseforge_production_migration',
  });
  assert.throws(
    () => validateMigrationDatabaseUrl(productionUrl),
    /migration_database_name_mismatch/,
  );
});

test('checklist records that the November 21 restore was not executed', () => {
  const checklist = read('docs/migration/RESTORE_REHEARSAL_CHECKLIST.md');
  assert.match(checklist, /2026-11-21/);
  assert.match(checklist, /NOT EXECUTED|not executed/i);
  assert.match(checklist, /full_restore_exceeds_render_disk|10 GB cannot hold/i);
  const exportOnce = read('docs/migration/RAILWAY_FRESH_EXPORT_ONCE.md');
  assert.doesNotMatch(
    exportOnce,
    /No retarget of `restoreGatewayRender\.py` to `pulseforge_production`/,
  );
});
