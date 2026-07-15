'use strict';

const crypto = require('crypto');
const pool = require('../db');
const { recordDecayRunEvent, run: runDecay } = require('../maxDecayAgent');

const FIXED_POLICY = Object.freeze({
  client_id: 10,
  limit: 250,
  dry_run: false,
  resume: true,
});

const REQUIRED_TRUE_FLAGS = Object.freeze([
  'MAX_ORCHESTRATION_ENABLED',
  'MAX_SCORING_ENABLED',
  'MAX_SHADOW_MODE',
]);

const REQUIRED_FALSE_FLAGS = Object.freeze([
  'MAX_STATE_TRANSITIONS_ENABLED',
  'MAX_ENRICHMENT_ACTIONS_ENABLED',
  'MAX_WARM_SEQUENCE_ENABLED',
  'MAX_CALL_TASKS_ENABLED',
  'MAX_HOT_ESCALATIONS_ENABLED',
  'MAX_RECYCLE_ACTIONS_ENABLED',
  'MAX_SEQUENCE_ACTIONS_ENABLED',
  'MAX_OPERATOR_TASKS_ENABLED',
  'MAX_ENRICHMENT_RETRY_ENABLED',
  'MAX_PROSPECT_ACTIONS_ENABLED',
]);

function secretMatches(expected, authorization) {
  if (typeof expected !== 'string' || expected.length < 32) return false;
  if (typeof authorization !== 'string' || !/^Bearer /i.test(authorization)) return false;
  const supplied = authorization.slice(7);
  const expectedBuffer = Buffer.from(expected, 'utf8');
  const suppliedBuffer = Buffer.from(supplied, 'utf8');
  return expectedBuffer.length === suppliedBuffer.length
    && crypto.timingSafeEqual(expectedBuffer, suppliedBuffer);
}

function validateShadowEnvironment(env = process.env) {
  const invalid = [];
  for (const name of REQUIRED_TRUE_FLAGS) {
    if (env[name] !== 'true') invalid.push(name);
  }
  for (const name of REQUIRED_FALSE_FLAGS) {
    if (env[name] !== 'false') invalid.push(name);
  }
  return { valid: invalid.length === 0, invalid };
}

function createInvocationLimiter({ limit = 4, windowMs = 24 * 60 * 60 * 1000, now = Date.now } = {}) {
  let acceptedAt = [];
  return function allowInvocation() {
    const cutoff = now() - windowMs;
    acceptedAt = acceptedAt.filter(value => value > cutoff);
    if (acceptedAt.length >= limit) return false;
    acceptedAt.push(now());
    return true;
  };
}

function totalOperationalEffects(effects = {}) {
  return Object.values(effects).reduce((sum, value) => sum + Math.abs(Number(value || 0)), 0);
}

async function recordRejectedConfiguration(db, { runId, invocationId, invalid }) {
  const now = new Date();
  await recordDecayRunEvent(db, {
    run_id: runId,
    mode: 'shadow-write',
    status: 'failed',
    started_at: now,
    completed_at: now,
    lock_acquired: false,
    client_scope: FIXED_POLICY.client_id,
    batch_limit: FIXED_POLICY.limit,
    errors: 1,
    error_stage: 'safety_configuration',
    error_code: 'UNSAFE_SHADOW_CONFIGURATION',
    error_summary: 'Decay invocation rejected because required shadow safety flags were not explicit',
    retryable: false,
    operational_effects: {},
    details: { invocation_id: invocationId, invalid_flags: invalid },
  });
}

function sanitizedResponse(report, invocationId, durationMs) {
  const operationalEffects = totalOperationalEffects(report.side_effects);
  return {
    ok: report.status === 'completed' || report.status === 'skipped_overlap',
    invocation_id: invocationId,
    run_id: report.run_id,
    status: report.status,
    client_id: FIXED_POLICY.client_id,
    evaluated: Number(report.evaluated || 0),
    score_changes: Number(report.scores_changed || 0),
    decisions_created: Number(report.decisions_created || 0),
    transitions_created: Number(report.transitions_recommended || 0),
    actions_created: Number(report.actions_skipped || 0),
    operational_effects: operationalEffects,
    executed_actions: 0,
    applied_transitions: 0,
    lock_acquired: report.lock_acquired === true,
    lock_released: report.status === 'skipped_overlap' ? null : true,
    duration_ms: durationMs,
  };
}

function createMaxDecayCronHandler({
  db = pool,
  env = process.env,
  runDecayFn = runDecay,
  allowInvocation = createInvocationLimiter(),
  randomUUID = crypto.randomUUID,
  recordRejectedFn = recordRejectedConfiguration,
} = {}) {
  return async function maxDecayCronHandler(req, res) {
    const invocationId = randomUUID();
    const expected = env.MAX_DECAY_CRON_SECRET;
    if (!secretMatches(expected, req.get('authorization'))) {
      return res.status(401).json({ ok: false, status: 'unauthorized' });
    }
    if (Object.keys(req.query || {}).length || Object.keys(req.body || {}).length) {
      return res.status(400).json({ ok: false, status: 'caller_overrides_not_allowed' });
    }
    if (!allowInvocation()) {
      return res.status(429).json({ ok: false, status: 'rate_limited' });
    }

    const safety = validateShadowEnvironment(env);
    if (!safety.valid) {
      const runId = randomUUID();
      await recordRejectedFn(db, { runId, invocationId, invalid: safety.invalid }).catch(error => {
        console.error(`[max_decay_cron] rejected run record failed invocation_id=${invocationId}: ${error.message}`);
      });
      console.error(`[max_decay_cron] unsafe configuration invocation_id=${invocationId} run_id=${runId}`);
      return res.status(503).json({
        ok: false,
        invocation_id: invocationId,
        run_id: runId,
        status: 'rejected_unsafe_configuration',
      });
    }

    const started = Date.now();
    try {
      const report = await runDecayFn(FIXED_POLICY, db);
      const response = sanitizedResponse(report, invocationId, Date.now() - started);
      if (response.operational_effects !== 0 || response.executed_actions !== 0 || response.applied_transitions !== 0) {
        console.error(`[max_decay_cron] operational safety violation invocation_id=${invocationId} run_id=${report.run_id}`);
        return res.status(500).json({ ...response, ok: false, status: 'operational_safety_violation' });
      }
      const status = report.status === 'failed' ? 500 : 200;
      console.log(`[max_decay_cron] invocation_id=${invocationId} run_id=${report.run_id} status=${report.status} duration_ms=${response.duration_ms}`);
      return res.status(status).json(response);
    } catch (error) {
      console.error(`[max_decay_cron] invocation failed invocation_id=${invocationId}: ${error.message}`);
      return res.status(500).json({ ok: false, invocation_id: invocationId, status: 'failed' });
    }
  };
}

module.exports = {
  FIXED_POLICY,
  REQUIRED_FALSE_FLAGS,
  REQUIRED_TRUE_FLAGS,
  createInvocationLimiter,
  createMaxDecayCronHandler,
  recordRejectedConfiguration,
  sanitizedResponse,
  secretMatches,
  totalOperationalEffects,
  validateShadowEnvironment,
};
