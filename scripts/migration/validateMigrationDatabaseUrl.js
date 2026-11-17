'use strict';
// No secret values are emitted; fail closed when staging is pointed at Railway.
// Render's isolated PG18 database name is fixed for the current migration.
const EXPECTED_STAGING_DB = 'pulseforge_production_migration';
function validateMigrationDatabaseUrl(raw, expectedName=EXPECTED_STAGING_DB) {
  if (typeof raw!=='string' || !raw.trim()) throw Error('migration_database_url_missing');
  let u;
  try {u=new URL(raw);}catch {throw Error('migration_database_url_invalid');}
  if (!['postgres:','postgresql:'].includes(u.protocol) || !u.hostname)
    throw Error('migration_database_url_invalid');
  const db=decodeURIComponent(u.pathname.slice(1));
  if (db!==expectedName) throw Error('migration_database_name_mismatch');
  const host=u.hostname.toLowerCase();
  if (host.endsWith('.railway.internal') || host.endsWith('.proxy.rlwy.net') ||
      host.endsWith('.rlwy.net') || host.endsWith('.railway.app'))
    throw Error('migration_source_host_forbidden');
  return {expectedDatabase:db};
}
module.exports={validateMigrationDatabaseUrl,EXPECTED_STAGING_DB};
