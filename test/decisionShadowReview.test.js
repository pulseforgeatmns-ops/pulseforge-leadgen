'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { FIELDS, insertShadowEvent, listShadowEvents } = require('../packages/decision-service/ShadowEventRepository');
const { createShadowEventSink, createShadowPool } = require('../packages/decision-service/ShadowEventSink');
const { buildShadowReview, likelyMissionInspection } = require('../packages/decision-service/shadowReview');
const { buildRoutingWarning } = require('../packages/decision-service/shadowRoutingWarning');
const { parseArgs } = require('../scripts/reviewDecisionShadow');
const fixture = require('./fixtures/decisionShadowEvent.json');
const ENV = { DECISION_SHADOW_ENABLED: 'true', DATABASE_URL: 'postgresql://test.invalid/test' };
const tick = () => new Promise(resolve => setImmediate(resolve));

test('repository preserves every field and nested data with bound parameters and duplicate protection', async () => {
  let sql, values;
  const row = { ...fixture, raw_redacted_response: { usage: { input_tokens: 123 } } };
  await insertShadowEvent({ query: async (text, params) => { sql = text; values = params; } }, row);
  assert.deepEqual([...FIELDS].sort(), Object.keys(row).sort());
  for (const [i, field] of FIELDS.entries()) {
    assert.deepEqual(['errors', 'current_route', 'raw_redacted_response'].includes(field)
      ? JSON.parse(values[i]) : values[i], row[field]);
  }
  assert.match(sql, /ON CONFLICT \(decision_id\) DO NOTHING/);
  assert.doesNotMatch(sql, /mission_82e8102f/);
});

test('known production mismatch is prominent; confidence thresholds do not fabricate unavailable or error cases', () => {
  assert.equal(likelyMissionInspection(fixture), true);
  assert.deepEqual(buildRoutingWarning(fixture), {
    event: 'DECISION_SHADOW_ROUTING_WARNING',
    spec: 'SPEC-JEV-003',
    schema_version: 1,
    mode: 'shadow_warning',
    severity: 'review',
    reason: 'likely_mission_inspection',
    decision_id: fixture.decision_id,
    source: fixture.source,
    session_id: fixture.session_id,
    tenant_id: fixture.tenant_id,
    mission_id: fixture.mission_id,
    timestamp: fixture.timestamp,
    current_route: fixture.current_route,
    intent: fixture.intent,
    confidence: fixture.confidence,
    inspection_probability: fixture.inspection_probability,
    recommended_route: fixture.recommended_route,
    comparison: fixture.comparison,
    action: 'review_current_route_without_changing_routing',
  });
  for (const current_route of [{ route: 'conversation' }, { route: 'other', raw_route: 'intelligence' }]) {
    assert.equal(likelyMissionInspection({ ...fixture, current_route, confidence: 0.2, inspection_probability: 0.85 }), true);
  }
  assert.equal(likelyMissionInspection({ ...fixture, confidence: 0.85, inspection_probability: 0.1 }), true);
  for (const patch of [
    { confidence: 0.849, inspection_probability: 0.849 },
    { confidence: null, inspection_probability: null },
    { confidence: '0.99', inspection_probability: '0.99' },
    { current_route: { route: 'mission' } }, { status: 'error' }, { provider: 'noop' },
    { current_route: { ...fixture.current_route, failed: true } },
    { comparison: 'unavailable' }, { comparison: 'match' },
    { intent: 'approval', recommended_route: 'approval' },
  ]) assert.equal(likelyMissionInspection({ ...fixture, ...patch }), false, JSON.stringify(patch));
  const error = { ...fixture, status: 'error', comparison: 'unavailable', intent: null,
    errors: [{ code: 'http_error', http_status: 401 }] };
  const report = buildShadowReview([fixture, error, { ...fixture, comparison: 'match' },
    { ...fixture, status: 'fallback', comparison: 'unavailable' }]);
  assert.equal(report.summary.total, 4);
  assert.equal(report.summary.mismatches, 1);
  assert.equal(report.summary.mismatch_rate, 0.5);
  assert.equal(report.summary.likely_mission_inspections, 1);
  assert.equal(report.summary.errors, 1);
  assert.equal(report.summary.error_codes.http_error, 1);
  assert.equal(report.likely_mission_inspections[0].decision_id, fixture.decision_id);
  assert.equal(report.operator_warnings[0].decision_id, fixture.decision_id);
  assert.equal(buildShadowReview([]).summary.mismatch_rate, null);
  assert.equal(buildRoutingWarning({ ...fixture, comparison: 'match' }), null);
});

test('read query validates bounds, scopes tenants, and applies mismatch/error filters before LIMIT', async () => {
  const calls = [];
  const db = { query: async (sql, values) => { calls.push({ sql, values }); return { rows: [fixture] }; } };
  const tenantId = "10' OR true --";
  assert.deepEqual(await listShadowEvents(db, { tenantId, limit: 50, filter: 'mismatches' }), [fixture]);
  assert.deepEqual(calls[0].values, [tenantId, 50]);
  assert.doesNotMatch(calls[0].sql, /10' OR/);
  assert.match(calls[0].sql, /tenant_id = \$1 AND comparison = 'mismatch'[\s\S]*LIMIT \$2/);
  await listShadowEvents(db, { filter: 'errors' });
  assert.match(calls[1].sql, /status = 'error' OR jsonb_array_length\(errors\) > 0/);
  await listShadowEvents(db, { filter: 'warnings' });
  assert.match(calls[2].sql, /provider = 'jev' AND comparison = 'mismatch'/);
  assert.match(calls[2].sql, /current_route->>'route' = 'conversation'/);
  assert.match(calls[2].sql, /confidence >= 0\.85 OR inspection_probability >= 0\.85/);
  for (const limit of [0, 501, -1, NaN, 1.5, '50']) await assert.rejects(listShadowEvents(db, { limit }));
  await assert.rejects(listShadowEvents(db, { filter: 'anything' }));
  assert.equal(calls.length, 3, 'invalid options never reach the database');
});

test('disabled persistence never creates a pool or writes, including explicit rollback switch', async () => {
  for (const env of [{}, { ...ENV, DECISION_SHADOW_ENABLED: 'false' },
    { ...ENV, DECISION_SHADOW_PERSIST_ENABLED: 'false' }]) {
    const sink = createShadowEventSink({ env, createPool: () => assert.fail('pool created'), warn: () => assert.fail('warning') });
    sink.write(fixture);
    await sink.drain();
    assert.deepEqual(sink.stats(), { persisted: 0, failed: 0, dropped: 0, pending: 0 });
  }
});

test('database writes are deferred, capacity remains bounded while stalled, and recover on settlement', async () => {
  let calls = 0, release, ended = 0;
  const warnings = [];
  const sink = createShadowEventSink({ env: { ...ENV, DECISION_SHADOW_DB_MAX_PENDING: '1' },
    createPool: () => ({ on() {}, end: async () => { ended += 1; }, query: () => {
      calls += 1;
      return calls === 1 ? new Promise(resolve => { release = resolve; }) : Promise.resolve();
    } }), warn: row => warnings.push(row) });
  sink.write(fixture);
  assert.equal(calls, 0, 'no database work on the response stack');
  await tick();
  for (let i = 0; i < 100; i++) sink.write(fixture);
  await tick();
  assert.equal(calls, 1);
  assert.deepEqual(sink.stats(), { persisted: 0, failed: 0, dropped: 100, pending: 1 });
  assert.equal(warnings.length, 1, 'capacity warnings rate limited');
  release();
  await sink.drain();
  sink.write(fixture);
  await sink.drain();
  assert.equal(calls, 2);
  assert.equal(sink.stats().persisted, 2);
  await sink.close();
  assert.equal(ended, 1);
});

test('sync/async DB and idle errors are contained; warnings omit secrets and may themselves fail', async () => {
  for (const query of [() => { throw Error('secret-password'); }, async () => { throw Error('secret-password'); }]) {
    const warnings = [];
    let onError;
    const sink = createShadowEventSink({ env: ENV, createPool: () => ({
      on: (_, fn) => { onError = fn; }, query,
    }), warn: row => warnings.push(row) });
    assert.doesNotThrow(() => sink.write(fixture));
    await sink.drain();
    assert.equal(sink.stats().failed, 1);
    assert.doesNotThrow(() => onError(Error('secret-password')));
    assert.doesNotMatch(JSON.stringify(warnings), /secret-password|test.invalid/);
    assert.equal(warnings[0].reason, 'write_failed');
  }
  for (const warn of [() => { throw Error('sink'); }, async () => { throw Error('sink'); }]) {
    const sink = createShadowEventSink({ env: ENV, createPool: () => { throw Error('pool'); }, warn });
    sink.write(fixture);
    await sink.drain();
    assert.equal(sink.stats().failed, 1);
  }
});

test('dedicated pool has finite connection, server statement, client query and idle limits', async () => {
  const pool = createShadowPool({ ...ENV, DATABASE_SSL: 'false', DECISION_SHADOW_DB_TIMEOUT_MS: '80' });
  assert.equal(pool.options.max, 2);
  assert.equal(pool.options.connectionTimeoutMillis, 80);
  assert.equal(pool.options.statement_timeout, 80);
  assert.equal(pool.options.query_timeout, 330);
  assert.equal(pool.options.ssl, false);
  await pool.end();
});

test('review CLI defaults to 50 and accepts bounded filters without writes', () => {
  assert.deepEqual(parseArgs([]), { limit: 50, tenantId: null, filter: 'all', json: false });
  assert.deepEqual(parseArgs(['--tenant', '10', '--limit', '20', '--mismatches', '--json']),
    { limit: 20, tenantId: '10', filter: 'mismatches', json: true });
  assert.deepEqual(parseArgs(['--warnings']), { limit: 50, tenantId: null, filter: 'warnings', json: false });
  assert.equal(parseArgs(['--help']).help, true);
  for (const args of [['--limit'], ['--tenant'], ['--limit', '501'], ['--mismatches', '--errors'],
    ['--warnings', '--errors'], ['--apply']]) {
    assert.throws(() => parseArgs(args));
  }
});
