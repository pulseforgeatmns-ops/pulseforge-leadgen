'use strict';

const { randomUUID } = require('crypto');
const { readFileSync } = require('fs');
const path = require('path');
const { PLACES_UPPER_BOUND } = require('./apiPricing');
const ready = new WeakMap();
const PROVIDERS = new Set(['google_places', 'anthropic']);
const LIMITS = ['daily_request_limit', 'monthly_request_limit', 'daily_usd_limit', 'monthly_usd_limit'];
const COST_SQL = `CASE endpoint ${Object.entries(PLACES_UPPER_BOUND).map(([k, v]) => `WHEN '${k}' THEN ${v}`).join(' ')} ELSE NULL END`;
const defaultDb = () => require('../db');

class ApiSpendError extends Error {
  constructor(code) { super(code); this.name = 'ApiSpendError'; this.code = code; }
}
function validProvider(provider) {
  if (!PROVIDERS.has(provider)) throw new ApiSpendError('invalid_api_provider');
  return provider;
}
function positiveTenant(value) {
  return Number.isSafeInteger(Number(value)) && Number(value) > 0 ? Number(value) : null;
}
function label(value, fallback) {
  const text = String(value || fallback || 'unknown');
  return /^[a-zA-Z0-9_.:/ -]{1,120}$/.test(text) ? text : (fallback || 'unknown');
}
async function ensureApiUsageSchema(db = defaultDb()) {
  if (!ready.has(db)) {
    const promise = db.query(readFileSync(path.join(__dirname, '../migrations/2026-10-14-api-usage-control.sql'), 'utf8'))
      .catch(err => { ready.delete(db); throw err; });
    ready.set(db, promise);
  }
  return ready.get(db);
}
async function transaction(db, action) {
  const c = await db.connect();
  try {
    await c.query('BEGIN');
    await c.query("SET LOCAL statement_timeout = '10s'");
    await c.query("SET LOCAL lock_timeout = '5s'");
    const result = await action(c);
    await c.query('COMMIT');
    return result;
  } catch (err) { await c.query('ROLLBACK').catch(() => {}); throw err; }
  finally { c.release(); }
}
async function lockProvider(c, provider) {
  await c.query('SELECT pg_advisory_xact_lock(7141026, hashtext($1))', [provider]);
}

// A historical Places row is used only before this shared meter starts. This
// prevents counting the same HTTP request in both the old and new ledgers.
async function historicalPlacesTotals(c) {
  const exists = await c.query("SELECT to_regclass('public.places_api_requests') AS ledger");
  if (!exists.rows[0]?.ledger) return { daily_calls: 0, monthly_calls: 0, daily_usd: 0, monthly_usd: 0, daily_unknown: 0, monthly_unknown: 0 };
  const { rows } = await c.query(`
    SELECT COUNT(*) FILTER (WHERE created_at >= date_trunc('day', NOW() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC') AS daily_calls,
      COUNT(*) AS monthly_calls,
      COALESCE(SUM(${COST_SQL}) FILTER (WHERE created_at >= date_trunc('day', NOW() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'), 0) AS daily_usd,
      COALESCE(SUM(${COST_SQL}), 0) AS monthly_usd,
      COUNT(*) FILTER (WHERE (${COST_SQL}) IS NULL AND created_at >= date_trunc('day', NOW() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC') AS daily_unknown,
      COUNT(*) FILTER (WHERE (${COST_SQL}) IS NULL) AS monthly_unknown
    FROM places_api_requests
    WHERE cache_hit IS NOT TRUE
      AND created_at >= date_trunc('month', NOW() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
      AND created_at < COALESCE((SELECT MIN(created_at) FROM api_usage_events WHERE provider = 'google_places'), NOW())
  `);
  return rows[0];
}
function enforcePolicy(policy, totals, estimatedUsd) {
  if (!policy) return;
  if (policy.paused) throw new ApiSpendError('api_provider_paused');
  for (const period of ['daily', 'monthly']) {
    const requests = policy[`${period}_request_limit`];
    if (requests != null && Number(totals[`${period}_calls`]) + 1 > Number(requests)) throw new ApiSpendError(`api_${period}_request_limit`);
    const usd = policy[`${period}_usd_limit`];
    if (usd != null) {
      if (estimatedUsd == null || Number(totals[`${period}_unknown`]) > 0) throw new ApiSpendError('api_cost_unknown');
      if (Number(totals[`${period}_usd`]) + estimatedUsd > Number(usd) + 1e-9) throw new ApiSpendError(`api_${period}_usd_limit`);
    }
  }
}
async function reserveApiCall(metadata, { db = defaultDb() } = {}) {
  const provider = validProvider(metadata.provider);
  await ensureApiUsageSchema(db);
  return transaction(db, async c => {
    await lockProvider(c, provider);
    const policy = (await c.query('SELECT * FROM api_spend_policies WHERE provider = $1', [provider])).rows[0];
    const { rows } = await c.query(`
      SELECT COUNT(*) FILTER (WHERE created_at >= date_trunc('day', NOW() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC') AS daily_calls,
        COUNT(*) AS monthly_calls,
        COALESCE(SUM(COALESCE(estimated_usd, reserved_usd)) FILTER (WHERE created_at >= date_trunc('day', NOW() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'), 0) AS daily_usd,
        COALESCE(SUM(COALESCE(estimated_usd, reserved_usd)), 0) AS monthly_usd,
        COUNT(*) FILTER (WHERE COALESCE(estimated_usd, reserved_usd) IS NULL AND created_at >= date_trunc('day', NOW() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC') AS daily_unknown,
        COUNT(*) FILTER (WHERE COALESCE(estimated_usd, reserved_usd) IS NULL) AS monthly_unknown
      FROM api_usage_events WHERE provider = $1
        AND created_at >= date_trunc('month', NOW() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
    `, [provider]);
    const totals = { ...rows[0] };
    if (provider === 'google_places') {
      const historic = await historicalPlacesTotals(c);
      for (const key of Object.keys(totals)) totals[key] = Number(totals[key] || 0) + Number(historic[key] || 0);
    }
    const estimate = metadata.estimatedUsd == null ? null : Number(metadata.estimatedUsd);
    if (estimate != null && (!Number.isFinite(estimate) || estimate < 0)) throw new ApiSpendError('invalid_api_estimate');
    enforcePolicy(policy, totals, estimate);
    const id = randomUUID();
    await c.query(`INSERT INTO api_usage_events
      (id, provider, agent_name, tenant_id, mission_id, operation, requested_model, model, status, reserved_usd, pricing_basis)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$7,'reserved',$8,$9)`,
    [id, provider, label(metadata.agentName), positiveTenant(metadata.tenantId), metadata.missionId == null ? null : label(metadata.missionId),
      label(metadata.operation), metadata.model == null ? null : label(metadata.model), estimate, label(metadata.pricingBasis, 'unpriced')]);
    return id;
  });
}
async function settleApiCall(id, outcome = {}, { db = defaultDb() } = {}) {
  const status = ['ok', 'error', 'uncertain'].includes(outcome.status) ? outcome.status : 'uncertain';
  const estimate = outcome.estimatedUsd == null ? null : Number(outcome.estimatedUsd);
  if (estimate != null && (!Number.isFinite(estimate) || estimate < 0)) throw new ApiSpendError('invalid_api_estimate');
  await db.query(`UPDATE api_usage_events SET status = $2, estimated_usd = $3,
    usage = $4::jsonb, http_status = $5, model = COALESCE($6, model), finished_at = NOW()
    WHERE id = $1 AND status = 'reserved'`,
  [id, status, estimate, JSON.stringify(outcome.usage || {}), Number.isInteger(outcome.httpStatus) ? outcome.httpStatus : null,
    outcome.model == null ? null : label(outcome.model)]);
}
function validatePolicyPatch(patch) {
  if (!patch || typeof patch !== 'object' || Array.isArray(patch)) throw new ApiSpendError('invalid_api_policy');
  if (Object.keys(patch).some(k => ![...LIMITS, 'paused'].includes(k)) || Object.keys(patch).length === 0) throw new ApiSpendError('invalid_api_policy');
  for (const key of LIMITS) {
    if (!(key in patch) || patch[key] === null) continue;
    if (typeof patch[key] !== 'number' || !Number.isFinite(patch[key]) || patch[key] < 0 || patch[key] > 1e9 || (key.endsWith('request_limit') && !Number.isInteger(patch[key]))) throw new ApiSpendError('invalid_api_policy');
  }
  if ('paused' in patch && typeof patch.paused !== 'boolean') throw new ApiSpendError('invalid_api_policy');
}
function mergePolicy(before, patch, actor) {
  validatePolicyPatch(patch);
  const next = { daily_request_limit: null, monthly_request_limit: null, daily_usd_limit: null, monthly_usd_limit: null, paused: false, ...before, ...patch };
  if (actor === 'max') {
    for (const key of LIMITS) {
      const old = before?.[key];
      if (old != null && (next[key] == null || Number(next[key]) > Number(old))) throw new ApiSpendError('max_cannot_increase_api_budget');
    }
    if (before?.paused && !next.paused) throw new ApiSpendError('max_cannot_resume_api_provider');
  }
  return next;
}
async function updateApiPolicy(provider, patch, { actor = 'operator', db = defaultDb(), expectedUpdatedAt } = {}) {
  validProvider(provider);
  if (!['operator', 'max'].includes(actor)) throw new ApiSpendError('invalid_api_actor');
  validatePolicyPatch(patch);
  await ensureApiUsageSchema(db);
  return transaction(db, async c => {
    await lockProvider(c, provider);
    const before = (await c.query('SELECT * FROM api_spend_policies WHERE provider = $1', [provider])).rows[0] || null;
    if (expectedUpdatedAt !== undefined) {
      const current = before ? new Date(before.updated_at).toISOString() : 'none';
      if (expectedUpdatedAt !== current) throw new ApiSpendError('api_policy_changed');
    }
    const next = mergePolicy(before, patch, actor);
    const { rows } = await c.query(`INSERT INTO api_spend_policies
      (provider,daily_request_limit,monthly_request_limit,daily_usd_limit,monthly_usd_limit,paused,updated_by)
      VALUES ($1,$2,$3,$4,$5,$6,$7)
      ON CONFLICT (provider) DO UPDATE SET daily_request_limit=$2,monthly_request_limit=$3,daily_usd_limit=$4,monthly_usd_limit=$5,paused=$6,updated_by=$7,updated_at=NOW()
      RETURNING *`, [provider, ...LIMITS.map(k => next[k]), next.paused, actor]);
    await c.query('INSERT INTO api_spend_policy_changes (provider,actor,before_policy,after_policy) VALUES ($1,$2,$3::jsonb,$4::jsonb)',
      [provider, actor, JSON.stringify(before), JSON.stringify(rows[0])]);
    return rows[0];
  });
}

module.exports = { ApiSpendError, PROVIDERS, LIMITS, COST_SQL, ensureApiUsageSchema, reserveApiCall, settleApiCall,
  updateApiPolicy, enforcePolicy, mergePolicy, validatePolicyPatch };
