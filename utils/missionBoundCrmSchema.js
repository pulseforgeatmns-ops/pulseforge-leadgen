'use strict';

/**
 * Tenant-scoped external identity for mission-bound CRM admission.
 * google_place_id links Scout Place-ID candidates to companies rows.
 */

let schemaPromise;

function ensureMissionBoundCrmSchema(pool = require('../db')) {
  if (!schemaPromise) {
    schemaPromise = pool.query(`
      ALTER TABLE companies
        ADD COLUMN IF NOT EXISTS google_place_id TEXT;

      CREATE UNIQUE INDEX IF NOT EXISTS companies_client_google_place_id_idx
        ON companies (client_id, google_place_id)
        WHERE google_place_id IS NOT NULL AND TRIM(google_place_id) <> '';
    `).catch((err) => {
      schemaPromise = null;
      throw err;
    });
  }
  return schemaPromise;
}

module.exports = {
  ensureMissionBoundCrmSchema,
};
