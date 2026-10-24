'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
  FIXED_POLICY,
  createInvocationLimiter,
  createMaxDecayCronHandler,
  secretMatches,
  validateShadowEnvironment,
} = require('../utils/maxDecayCron');

const SECRET = 'a'.repeat(48);
const SAFE_ENV = Object.freeze({
  MAX_DECAY_CRON_SECRET: SECRET,
  MAX_ORCHESTRATION_ENABLED: 'true',
  MAX_SCORING_ENABLED: 'true',
  MAX_SHADOW_MODE: 'true',
  MAX_STATE_TRANSITIONS_ENABLED: 'false',
  MAX_ENRICHMENT_ACTIONS_ENABLED: 'false',
  MAX_WARM_SEQUENCE_ENABLED: 'false',
  MAX_CALL_TASKS_ENABLED: 'false',
  MAX_HOT_ESCALATIONS_ENABLED: 'false',
  MAX_RECYCLE_ACTIONS_ENABLED: 'false',
  MAX_SEQUENCE_ACTIONS_ENABLED: 'false',
  MAX_OPERATOR_TASKS_ENABLED: 'false',
  MAX_ENRICHMENT_RETRY_ENABLED: 'false',
  MAX_PROSPECT_ACTIONS_ENABLED: 'false',
});

function request({ authorization, query = {}, body = {} } = {}) {
  return { query, body, get(name) { return name === 'authorization' ? authorization : undefined; } };
}

function response() {
  return {
    statusCode: 200,
    payload: null,
    status(code) { this.statusCode = code; return this; },
    json(payload) { this.payload = payload; return this; },
  };
}

test('dedicated decay authentication requires a distinct sufficiently long bearer secret', () => {
  assert.equal(secretMatches(SECRET, `Bearer ${SECRET}`), true);
  assert.equal(secretMatches(SECRET, `Bearer ${SECRET}x`), false);
  assert.equal(secretMatches('short', 'Bearer short'), false);
  assert.equal(secretMatches(SECRET, SECRET), false);
});

test('shadow environment requires all safety values to be explicit', () => {
  assert.deepEqual(validateShadowEnvironment(SAFE_ENV), { valid: true, invalid: [] });
  const unsafe = validateShadowEnvironment({ ...SAFE_ENV, MAX_SHADOW_MODE: 'false', MAX_CALL_TASKS_ENABLED: undefined });
  assert.equal(unsafe.valid, false);
  assert.deepEqual(unsafe.invalid, ['MAX_SHADOW_MODE', 'MAX_CALL_TASKS_ENABLED']);
});

test('missing and incorrect authentication are rejected without running decay', async () => {
  let runs = 0;
  const handler = createMaxDecayCronHandler({ env: SAFE_ENV, runDecayFn: async () => { runs++; } });
  for (const authorization of [undefined, 'Bearer wrong']) {
    const res = response();
    await handler(request({ authorization }), res);
    assert.equal(res.statusCode, 401);
    assert.deepEqual(res.payload, { ok: false, status: 'unauthorized' });
  }
  assert.equal(runs, 0);
});

test('authenticated endpoint rejects caller overrides and uses only fixed server policy', async () => {
  let received;
  const handler = createMaxDecayCronHandler({
    env: SAFE_ENV,
    runDecayFn: async params => {
      received = params;
      return { run_id: 'run-1', status: 'completed', lock_acquired: true, side_effects: {} };
    },
  });
  const rejected = response();
  await handler(request({ authorization: `Bearer ${SECRET}`, query: { limit: '2000' } }), rejected);
  assert.equal(rejected.statusCode, 400);

  const accepted = response();
  await handler(request({ authorization: `Bearer ${SECRET}` }), accepted);
  assert.deepEqual(received, FIXED_POLICY);
  assert.equal(accepted.statusCode, 200);
  assert.equal(accepted.payload.operational_effects, 0);
  assert.equal(accepted.payload.executed_actions, 0);
  assert.equal(accepted.payload.applied_transitions, 0);
  assert.equal(Object.hasOwn(accepted.payload, 'errors'), false);
});

test('unsafe configuration fails closed and persists a rejected run event', async () => {
  let runs = 0;
  let rejected;
  const handler = createMaxDecayCronHandler({
    env: { ...SAFE_ENV, MAX_SEQUENCE_ACTIONS_ENABLED: 'true' },
    randomUUID: (() => { const ids = ['invocation-1', 'run-1']; return () => ids.shift(); })(),
    runDecayFn: async () => { runs++; },
    recordRejectedFn: async (_db, event) => { rejected = event; },
  });
  const res = response();
  await handler(request({ authorization: `Bearer ${SECRET}` }), res);
  assert.equal(res.statusCode, 503);
  assert.equal(runs, 0);
  assert.equal(rejected.runId, 'run-1');
  assert.deepEqual(rejected.invalid, ['MAX_SEQUENCE_ACTIONS_ENABLED']);
});

test('concurrent authenticated requests return completed and skipped_overlap', async () => {
  let active = false;
  let release;
  const blocked = new Promise(resolve => { release = resolve; });
  const handler = createMaxDecayCronHandler({
    env: SAFE_ENV,
    runDecayFn: async () => {
      if (active) return { run_id: 'overlap', status: 'skipped_overlap', lock_acquired: false, side_effects: {} };
      active = true;
      await blocked;
      active = false;
      return { run_id: 'primary', status: 'completed', lock_acquired: true, side_effects: {}, evaluated: 5 };
    },
  });
  const firstResponse = response();
  const first = handler(request({ authorization: `Bearer ${SECRET}` }), firstResponse);
  await new Promise(resolve => setImmediate(resolve));
  const secondResponse = response();
  await handler(request({ authorization: `Bearer ${SECRET}` }), secondResponse);
  release();
  await first;
  assert.equal(firstResponse.payload.status, 'completed');
  assert.equal(secondResponse.payload.status, 'skipped_overlap');
  assert.equal(secondResponse.statusCode, 200);
});

test('successful invocation volume is bounded per process window', () => {
  let now = 1_000;
  const allow = createInvocationLimiter({ limit: 2, windowMs: 100, now: () => now });
  assert.equal(allow(), true);
  assert.equal(allow(), true);
  assert.equal(allow(), false);
  now += 101;
  assert.equal(allow(), true);
});
