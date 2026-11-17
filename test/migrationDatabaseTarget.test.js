'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const {validateMigrationDatabaseUrl}=require('../scripts/migration/validateMigrationDatabaseUrl');
test('rejects live Railway source even if present in staging env',()=>{
 assert.throws(()=>validateMigrationDatabaseUrl('postgresql://postgres:secret@shinkansen.proxy.rlwy.net:43478/railway'),/migration_database_name_mismatch/);
 assert.throws(()=>validateMigrationDatabaseUrl('postgresql://postgres:secret@postgres.railway.internal/pulseforge_production_migration'),/migration_source_host_forbidden/);
});
test('requires exact isolated database name before touching connection',()=>{
 assert.throws(()=>validateMigrationDatabaseUrl('postgresql://user:secret@host.example/test'),/migration_database_name_mismatch/);
 assert.throws(()=>validateMigrationDatabaseUrl('bad-url'),/migration_database_url_invalid/);
});
test('accepts internal Render migration target',()=>{
 assert.deepEqual(validateMigrationDatabaseUrl('postgresql://user:secret@dpg-dbtgtloae00c73dd8040-a/pulseforge_production_migration'),
 {expectedDatabase:'pulseforge_production_migration'});
});
