'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');
const { classifyMission, timestampContext, SPEC137_MERGED_AT } = require('../packages/acquisition-mission/audit/LifecycleTransitionAudit');
const { parseArgs, missionFromRow, auditFromDatabase, main, EVENT_LIMIT } = require('../scripts/audit019LifecycleTransitionCoverage');

const ROOT = path.resolve(__dirname, '..');
const opts = { database: true, tenantId: 'tenant_fixture', missionId: null, limit: 100 };
function mission(overrides = {}) {
  return { stage: 'discover', pendingOperatorDecision: null, ...overrides };
}
function row(overrides = {}) {
  return { id: 'mission_fixture', tenant_id: opts.tenantId, stage: 'understand', status: 'active',
    created_at: '2026-08-01T00:00:00Z', updated_at: '2026-10-24T00:00:00Z',
    payload: { pendingOperatorDecision: { stage: 'discover', kind: 'discovery_approval' } }, ...overrides };
}
function fakeClient({ rows = [row()], events = [], failAt = null, failEnd = false } = {}) {
  const calls = [];
  const failure = new Error('fixture failure');
  const client = {
    async connect() {
      calls.push({ sql: 'connect' });
      if (failAt === 'connect') throw failure;
    },
    async query(sql, values) {
      calls.push({ sql, values });
      if (failAt && sql.includes(failAt)) throw failure;
      if (sql.includes('FROM acquisition_missions')) return { rows };
      if (sql.includes('FROM acquisition_mission_events')) return { rows: events };
      return { rows: [] };
    },
    async end() {
      calls.push({ sql: 'end' });
      if (failEnd) throw new Error('fixture cleanup failure');
    },
  };
  return { calls, client, failure };
}

for (const [id, input] of [
  ['stage_pending_stage_mismatch', { stage: 'understand', pendingOperatorDecision: { stage: 'discover' } }],
  ['discovery_approval_off_discover', { stage: 'understand', pendingOperatorDecision: { kind: 'discovery_approval' } }],
  ['plan_decision_off_discover', { stage: 'understand', pendingOperatorDecision: { kind: 'plan_edit' } }],
  ['approved_plan_stale_pending', { structuredMissionApproved: true, pendingOperatorDecision: { kind: 'plan_clarification' } }],
]) {
  test(`classifies ${id} as observed inconsistency with unknown origin`, () => {
    const result = classifyMission(mission(input));
    assert.equal(result.category, 'inconsistent_origin_unknown');
    assert.equal(result.origin, 'unknown');
    assert.deepEqual(result.hits.map((hit) => hit.id), [id]);
    assert.equal('transition' in result.hits[0], false);
  });
}

test('all plan decision kinds and immutable plan flag match the corresponding patterns', () => {
  for (const kind of ['plan_approval', 'plan_edit', 'plan_clarification']) {
    const result = classifyMission(mission({ stage: 'understand', structuredMission: { immutable: true }, pendingOperatorDecision: { kind } }));
    assert.deepEqual(result.hits.map((hit) => hit.id), ['plan_decision_off_discover', 'approved_plan_stale_pending']);
  }
});

test('old creation and new mutation cannot establish stale origin', () => {
  const result = classifyMission(missionFromRow(row()));
  assert.equal(result.category, 'inconsistent_origin_unknown');
  assert.equal(result.origin, 'unknown');
  assert.equal(result.temporalContext.createdAt.relationToSpec137Merge, 'before');
  assert.equal(result.temporalContext.updatedAt.relationToSpec137Merge, 'after');
  assert.equal(result.temporalContext.reference.deploymentVerified, false);
});

test('post-merge creation is also unknown origin, not an uncovered transition diagnosis', () => {
  const result = classifyMission(missionFromRow(row({ created_at: '2026-10-24T00:00:00Z' })));
  assert.equal(result.category, 'inconsistent_origin_unknown');
  assert.equal(result.temporalContext.createdAt.relationToSpec137Merge, 'after');
});

test('missing and invalid dates never assign stale or uncovered origins', () => {
  for (const date of [undefined, null, '', 'bad-date', '2026-02-30T00:00:00Z', '2026-08-22', '2026-08-22T12:52:50', {}, 0, new Date(NaN)]) {
    const result = classifyMission(missionFromRow(row({ created_at: date, updated_at: date })));
    assert.equal(result.category, 'inconsistent_origin_unknown');
    assert.equal(result.temporalContext.createdAt.relationToSpec137Merge, 'unknown');
  }
  assert.equal(timestampContext(null).status, 'missing');
  assert.equal(timestampContext('bad-date').status, 'invalid');
});

test('timestamp context handles merge equality, timezone offsets, Dates and leap dates', () => {
  assert.equal(timestampContext(SPEC137_MERGED_AT).relationToSpec137Merge, 'at');
  assert.equal(timestampContext('2026-08-22T14:52:50+02:00').relationToSpec137Merge, 'at');
  assert.equal(timestampContext(new Date(SPEC137_MERGED_AT)).relationToSpec137Merge, 'at');
  assert.equal(timestampContext('2024-02-29T12:00:00Z').status, 'valid');
  assert.equal(timestampContext('2026-08-22T24:00:00Z').status, 'invalid');
});

test('no historical match is not a full consistency certificate and inputs remain unchanged', () => {
  const input = Object.freeze({ stage: 'discover', pendingOperatorDecision: Object.freeze({ stage: 'discover', kind: 'plan_approval' }) });
  const result = classifyMission(input);
  assert.equal(result.category, 'no_known_inconsistency');
  assert.equal(result.origin, 'unknown');
  assert.deepEqual(result.hits, []);
  assert.equal(classifyMission({}).category, 'no_known_inconsistency');
  assert.throws(() => classifyMission(null), TypeError);
});

test('database columns override stale payload identity, tenant, stage and timestamps', () => {
  const dbRow = row({ payload: { id: 'wrong', tenantId: 'wrong', stage: 'discover', createdAt: 'wrong', updatedAt: 'wrong' } });
  const result = missionFromRow(dbRow);
  assert.equal(result.id, dbRow.id);
  assert.equal(result.tenantId, dbRow.tenant_id);
  assert.equal(result.stage, dbRow.stage);
  assert.equal(result.createdAt, dbRow.created_at);
  assert.equal(result.updatedAt, dbRow.updated_at);
});

test('import and default CLI do not load pg, dotenv or shared DB even with ambient DATABASE_URL', () => {
  const source = `
    const failNetwork = () => { throw new Error('network forbidden'); };
    require('node:net').Socket.prototype.connect = failNetwork;
    require('node:tls').connect = failNetwork;
    for (const name of ['node:http', 'node:https']) {
      require(name).request = require(name).get = failNetwork;
    }
    global.fetch = failNetwork;
    const Module = require('node:module');
    const original = Module._load;
    Module._load = function (name, ...args) {
      if (name === 'pg' || name === 'dotenv' || /(?:^|\\/)db(?:\\.js)?$/.test(name)) throw new Error('forbidden dependency: ' + name);
      return original.call(this, name, ...args);
    };
    require('./packages/acquisition-mission/audit/LifecycleTransitionAudit');
    require('./scripts/audit019LifecycleTransitionCoverage').main();
  `;
  const child = spawnSync(process.execPath, ['-e', source], {
    cwd: ROOT, encoding: 'utf8', timeout: 5000,
    env: { PATH: process.env.PATH, DATABASE_URL: 'postgresql://fixture.invalid/not-a-real-credential' },
  });
  assert.equal(child.status, 0, child.stderr);
  assert.match(child.stdout, /No database access occurs by default/);
  assert.doesNotMatch(child.stdout, /category A|NONE FOUND|First offending transition/);
});

test('no-argument and help modes do not read any environment values or create clients', async () => {
  const env = new Proxy({}, { get() { throw new Error('environment access forbidden'); } });
  const createClient = () => { throw new Error('client construction forbidden'); };
  const messages = [];
  await main([], { env, createClient, log: (value) => messages.push(value) });
  await main(['--help', '--database'], { env, createClient, log: (value) => messages.push(value) });
  assert.equal(messages.length, 2);
});

test('manual mode rejects missing, unknown, duplicate and unbounded scope options', async () => {
  for (const argv of [
    ['--database'], ['--database', '--mission-id', 'm'], ['--tenant-id', 't'],
    ['--mission-id', 'm'], ['--limit', '3'], ['--all'], ['--database', '--tenant-id'],
    ['--database', '--tenant-id', '--mission-id', 'm'], ['--database', '--tenant-id', ' '],
    ['--database', '--database', '--tenant-id', 't'],
    ...['0', '-1', '1001', '1.5', 'abc'].map((limit) => ['--database', '--tenant-id', 't', '--limit', limit]),
  ]) assert.throws(() => parseArgs(argv));
  assert.equal(parseArgs(['--database', '--tenant-id', 't', '--limit', '1000']).limit, 1000);
  await assert.rejects(main(['--database', '--tenant-id', 't'], {
    env: { DATABASE_URL: 'ignored' }, createClient() { throw new Error('must not create'); },
  }), /AUDIT019_DATABASE_URL/);
  await assert.rejects(auditFromDatabase({ ...opts, database: false }, () => { throw new Error('must not create'); }), /Explicit database mode/);
});

test('reads are scoped, parameterized, bounded and inside one read-only transaction; cleanup closes the dedicated client', async () => {
  const tenantId = "tenant'; DROP TABLE x; --";
  const missionId = "mission' OR true --";
  const fake = fakeClient({ rows: [row({ tenant_id: tenantId, id: missionId })], events: [{ id: 'event_fixture', at: SPEC137_MERGED_AT }] });
  const report = await auditFromDatabase({ ...opts, tenantId, missionId }, () => fake.client);
  assert.deepEqual(fake.calls.slice(0, 4).map((call) => call.sql), [
    'connect', 'BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY',
    "SET LOCAL statement_timeout = '10s'", "SET LOCAL idle_in_transaction_session_timeout = '10s'",
  ]);
  const selects = fake.calls.filter((call) => call.sql.startsWith('SELECT'));
  assert.equal(selects.length, 2);
  assert.match(selects[0].sql, /WHERE tenant_id = \$1 AND id = \$2/);
  assert.match(selects[1].sql, /WHERE tenant_id = \$1 AND mission_id = \$2/);
  assert.deepEqual(selects[0].values, [tenantId, missionId, 101]);
  assert.deepEqual(selects[1].values, [tenantId, missionId, EVENT_LIMIT + 1]);
  for (const query of selects) assert.equal(query.sql.includes(tenantId), false);
  assert.deepEqual(fake.calls.slice(-2).map((call) => call.sql), ['ROLLBACK', 'end']);
  assert.equal(report.missions[0].classification.origin, 'unknown');
  assert.equal(report.missions[0].transitions.observedCount, 1);
  assert.equal(report.missions[0].transitions.truncated, false);
  assert.equal('payload' in report.missions[0], false);
});

test('empty scope results and no-pattern results still roll back and close without event reads', async () => {
  for (const rows of [[], [row({ stage: 'discover', payload: {} })]]) {
    const fake = fakeClient({ rows });
    const report = await auditFromDatabase(opts, () => fake.client);
    assert.equal(report.missions.length, rows.length);
    assert.equal(report.missionsTruncated, false);
    assert.equal(fake.calls.filter((call) => call.sql.includes('acquisition_mission_events')).length, 0);
    if (rows.length) assert.equal(report.missions[0].transitions, null);
    assert.deepEqual(fake.calls.slice(-2).map((call) => call.sql), ['ROLLBACK', 'end']);
  }
});

test('mission and event truncation are explicit; missing events do not establish non-execution', async () => {
  const events = Array.from({ length: EVENT_LIMIT + 1 }, (_, i) => ({ id: `e${i}`, at: null }));
  const fake = fakeClient({ rows: [row(), row({ id: 'overflow' })], events });
  const report = await auditFromDatabase({ ...opts, limit: 1 }, () => fake.client);
  assert.equal(report.missionsTruncated, true);
  assert.equal(report.missions.length, 1);
  assert.equal(report.missions[0].transitions.truncated, true);
  assert.equal(report.missions[0].transitions.recorded.length, EVENT_LIMIT);
  const emptyEvents = fakeClient();
  const emptyReport = await auditFromDatabase(opts, () => emptyEvents.client);
  assert.equal(emptyReport.missions[0].transitions.observedCount, 0);
  assert.equal(emptyReport.missions[0].classification.origin, 'unknown');
  assert.match(emptyReport.limitations.join(' '), /absence.*do not prove/);
});

test('out-of-scope rows fail closed before event reads', async () => {
  const fake = fakeClient({ rows: [row({ tenant_id: 'another_tenant' })] });
  await assert.rejects(auditFromDatabase(opts, () => fake.client), /outside the requested scope/);
  assert.equal(fake.calls.some((call) => call.sql.includes('acquisition_mission_events')), false);
  assert.deepEqual(fake.calls.slice(-2).map((call) => call.sql), ['ROLLBACK', 'end']);
});

for (const failAt of ['connect', 'BEGIN', 'SET LOCAL statement_timeout', 'SET LOCAL idle_in_transaction_session_timeout', 'FROM acquisition_missions', 'FROM acquisition_mission_events', 'ROLLBACK']) {
  test(`closes dedicated client when ${failAt} fails`, async () => {
    const fake = fakeClient({ failAt, failEnd: true });
    await assert.rejects(auditFromDatabase(opts, () => fake.client), (error) => error === fake.failure);
    assert.equal(fake.calls.at(-1).sql, 'end');
    if (!['connect', 'BEGIN'].includes(failAt)) assert.equal(fake.calls.at(-2).sql, 'ROLLBACK');
  });
}

test('cleanup failure prevents reporting success', async () => {
  const fake = fakeClient({ failEnd: true });
  await assert.rejects(auditFromDatabase(opts, () => fake.client), /fixture cleanup failure/);
});

test('main does not log a partial or successful report when a database read fails', async () => {
  const fake = fakeClient({ failAt: 'FROM acquisition_mission_events' });
  let logs = 0;
  await assert.rejects(main(['--database', '--tenant-id', opts.tenantId], {
    env: { AUDIT019_DATABASE_URL: 'fixture-only-string' },
    createClient: () => fake.client,
    log: () => { logs += 1; },
  }), (error) => error === fake.failure);
  assert.equal(logs, 0);
  assert.equal(fake.calls.at(-1).sql, 'end');
});

test('main creates only a dedicated read-only client after opt-in and logs one complete report after cleanup', async () => {
  const fake = fakeClient();
  let config;
  const logs = [];
  const report = await main(['--database', '--tenant-id', opts.tenantId], {
    env: { AUDIT019_DATABASE_URL: 'fixture-only-string' },
    createClient(value) { config = value; return fake.client; },
    log(value) {
      assert.equal(fake.calls.at(-1).sql, 'end');
      logs.push(value);
    },
  });
  assert.equal(config.connectionString, 'fixture-only-string');
  assert.equal(config.options, '-c default_transaction_read_only=on');
  assert.equal(config.connectionTimeoutMillis, 5000);
  assert.equal(logs.length, 1);
  assert.deepEqual(JSON.parse(logs[0]), report);
});
