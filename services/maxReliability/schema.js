'use strict';

/**
 * MAX-P0-003 — reliability tables are created only by the operator migration
 * migrations/2026-11-25-max-p0-003-reliability-core.sql (never startup DDL).
 * Every ledger/receipt operation fails closed when they are absent, so no
 * external action can run without a durable claim.
 */

const REQUIRED_TABLES = Object.freeze([
  'max_external_action_claims',
  'max_request_receipts',
  'max_reliability_audit',
]);

const verifiedPools = new WeakSet();

async function reliabilitySchemaStatus(pool) {
  const r = await pool.query(
    `SELECT t AS name, to_regclass(t) IS NOT NULL AS installed FROM unnest($1::text[]) AS t`,
    [REQUIRED_TABLES]
  );
  const missing = r.rows.filter((row) => row.installed !== true).map((row) => row.name);
  return { installed: missing.length === 0 && r.rows.length === REQUIRED_TABLES.length, missing };
}

async function assertReliabilitySchema(pool) {
  if (pool && typeof pool === 'object' && verifiedPools.has(pool)) return;
  let status;
  try {
    status = await reliabilitySchemaStatus(pool);
  } catch (err) {
    throw Object.assign(new Error('Reliability schema could not be verified.'), {
      code: 'reliability_schema_unavailable', spec: 'MAX-P0-003', cause: err,
    });
  }
  if (!status.installed) {
    throw Object.assign(new Error(`Reliability schema missing: ${status.missing.join(', ') || 'unknown'}.`), {
      code: 'reliability_schema_missing', spec: 'MAX-P0-003', missing: status.missing,
    });
  }
  // Positive results only are cached; a missing schema is re-checked every call.
  if (pool && typeof pool === 'object') verifiedPools.add(pool);
}

module.exports = { REQUIRED_TABLES, reliabilitySchemaStatus, assertReliabilitySchema };
