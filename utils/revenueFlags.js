'use strict';

const FLAG_NAMES = Object.freeze([
  'revenue_schema_enabled',
  'revenue_operator_reads_enabled',
  'revenue_operator_writes_enabled',
  'revenue_max_reads_enabled',
  'revenue_followup_recommendations_enabled',
]);

function envEnabled(env, name) {
  return String(env[name.toUpperCase()] || '').trim().toLowerCase() === 'true';
}

async function loadRevenueFlags(db, clientId, env = process.env) {
  const disabled = Object.fromEntries(FLAG_NAMES.map(name => [name, false]));
  if (!envEnabled(env, 'revenue_schema_enabled')) return disabled;
  try {
    const { rows } = await db.query(
      `SELECT ${FLAG_NAMES.join(', ')} FROM revenue_feature_flags WHERE client_id = $1 LIMIT 1`,
      [clientId]
    );
    if (!rows[0]) return disabled;
    return Object.fromEntries(FLAG_NAMES.map(name => [name, envEnabled(env, name) && rows[0][name] === true]));
  } catch (error) {
    if (error.code === '42P01' || error.code === '42703') return disabled;
    throw error;
  }
}

function assertRevenueFlag(flags, name) {
  if (!FLAG_NAMES.includes(name) || flags[name] !== true) {
    const error = new Error('Revenue capability is disabled');
    error.code = 'REVENUE_CAPABILITY_DISABLED';
    error.status = 404;
    throw error;
  }
}

module.exports = { FLAG_NAMES, assertRevenueFlag, envEnabled, loadRevenueFlags };
