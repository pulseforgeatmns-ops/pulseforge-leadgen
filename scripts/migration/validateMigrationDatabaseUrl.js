'use strict';
// No secret values are emitted; fail closed when a Render destination is pointed at Railway.
// Staging remains the November 17 snapshot name. Production restore/cutover must pass
// EXPECTED_PRODUCTION_DB (or role "production") explicitly and is not armed by default.
const EXPECTED_STAGING_DB = 'pulseforge_production_migration';
const EXPECTED_PRODUCTION_DB = 'pulseforge_production';
const APPROVED_DESTINATION_ROLES = Object.freeze({
  staging: EXPECTED_STAGING_DB,
  production: EXPECTED_PRODUCTION_DB,
});

function validateMigrationDatabaseUrl(raw, expectedName = EXPECTED_STAGING_DB) {
  if (typeof raw !== 'string' || !raw.trim()) throw Error('migration_database_url_missing');
  if (expectedName !== EXPECTED_STAGING_DB && expectedName !== EXPECTED_PRODUCTION_DB) {
    throw Error('migration_database_name_unapproved');
  }
  let u;
  try { u = new URL(raw); } catch { throw Error('migration_database_url_invalid'); }
  if (!['postgres:', 'postgresql:'].includes(u.protocol) || !u.hostname) {
    throw Error('migration_database_url_invalid');
  }
  const db = decodeURIComponent(u.pathname.slice(1));
  if (db !== expectedName) throw Error('migration_database_name_mismatch');
  const host = u.hostname.toLowerCase();
  if (host.endsWith('.railway.internal') || host.endsWith('.proxy.rlwy.net') ||
      host.endsWith('.rlwy.net') || host.endsWith('.railway.app')) {
    throw Error('migration_source_host_forbidden');
  }
  return { expectedDatabase: db };
}

function validateRenderDestinationUrl(raw, role) {
  const expectedName = APPROVED_DESTINATION_ROLES[role];
  if (!expectedName) throw Error('migration_destination_role_invalid');
  return { ...validateMigrationDatabaseUrl(raw, expectedName), role };
}

module.exports = {
  validateMigrationDatabaseUrl,
  validateRenderDestinationUrl,
  EXPECTED_STAGING_DB,
  EXPECTED_PRODUCTION_DB,
  APPROVED_DESTINATION_ROLES,
};
