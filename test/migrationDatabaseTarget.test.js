'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const {
  validateMigrationDatabaseUrl,
  validateRenderDestinationUrl,
  EXPECTED_STAGING_DB,
  EXPECTED_PRODUCTION_DB,
}=require('../scripts/migration/validateMigrationDatabaseUrl');

const stagingHost='dpg-dbtgtloae00c73dd8040-a';
const stagingUrl=`postgresql://user:secret@${stagingHost}/pulseforge_production_migration`;
const productionUrl=`postgresql://user:secret@${stagingHost}/pulseforge_production`;

test('rejects live Railway source even if present in staging env',()=>{
 assert.throws(()=>validateMigrationDatabaseUrl('postgresql://postgres:secret@shinkansen.proxy.rlwy.net:43478/railway'),/migration_database_name_mismatch/);
 assert.throws(()=>validateMigrationDatabaseUrl('postgresql://postgres:secret@postgres.railway.internal/pulseforge_production_migration'),/migration_source_host_forbidden/);
 assert.throws(()=>validateMigrationDatabaseUrl('postgresql://postgres:secret@postgres.railway.internal/pulseforge_production',EXPECTED_PRODUCTION_DB),/migration_source_host_forbidden/);
 assert.throws(()=>validateRenderDestinationUrl('postgresql://postgres:secret@host.proxy.rlwy.net/pulseforge_production_migration','staging'),/migration_source_host_forbidden/);
 assert.throws(()=>validateRenderDestinationUrl('postgresql://postgres:secret@example.railway.app/pulseforge_production','production'),/migration_source_host_forbidden/);
 assert.throws(()=>validateRenderDestinationUrl('postgresql://postgres:secret@example.rlwy.net/pulseforge_production','production'),/migration_source_host_forbidden/);
});

test('requires exact isolated database name before touching connection',()=>{
 assert.throws(()=>validateMigrationDatabaseUrl('postgresql://user:secret@host.example/test'),/migration_database_name_mismatch/);
 assert.throws(()=>validateMigrationDatabaseUrl('bad-url'),/migration_database_url_invalid/);
});

test('accepts internal Render migration target',()=>{
 assert.deepEqual(validateMigrationDatabaseUrl(stagingUrl),
 {expectedDatabase:'pulseforge_production_migration'});
 assert.deepEqual(validateRenderDestinationUrl(stagingUrl,'staging'),
 {expectedDatabase:'pulseforge_production_migration',role:'staging'});
});

test('staging default rejects pulseforge_production',()=>{
 assert.throws(()=>validateMigrationDatabaseUrl(productionUrl),/migration_database_name_mismatch/);
 assert.throws(()=>validateRenderDestinationUrl(productionUrl,'staging'),/migration_database_name_mismatch/);
});

test('production name is accepted only via EXPECTED_PRODUCTION_DB or role production',()=>{
 assert.deepEqual(validateMigrationDatabaseUrl(productionUrl,EXPECTED_PRODUCTION_DB),
 {expectedDatabase:'pulseforge_production'});
 assert.deepEqual(validateRenderDestinationUrl(productionUrl,'production'),
 {expectedDatabase:'pulseforge_production',role:'production'});
 assert.equal(EXPECTED_STAGING_DB,'pulseforge_production_migration');
 assert.equal(EXPECTED_PRODUCTION_DB,'pulseforge_production');
});

test('unapproved database names are rejected even when passed explicitly',()=>{
 assert.throws(()=>validateMigrationDatabaseUrl(`postgresql://user:secret@${stagingHost}/railway`,'railway'),/migration_database_name_unapproved/);
 assert.throws(()=>validateMigrationDatabaseUrl(stagingUrl,'pulseforge'),/migration_database_name_unapproved/);
});

test('invalid destination role restore is rejected',()=>{
 assert.throws(()=>validateRenderDestinationUrl(stagingUrl,'restore'),/migration_destination_role_invalid/);
 assert.throws(()=>validateRenderDestinationUrl(productionUrl,'cutover'),/migration_destination_role_invalid/);
 assert.throws(()=>validateRenderDestinationUrl(productionUrl,undefined),/migration_destination_role_invalid/);
});
