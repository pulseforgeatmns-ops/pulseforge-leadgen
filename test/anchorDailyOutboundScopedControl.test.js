'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const path = require('node:path');
const { setImmediate: nextTurn } = require('node:timers/promises');
const scheduler = require('../services/governedOutboundStageScheduler');
const dispatch = require('../services/governedOutboundControlDispatch');
const clock = require('../services/governedOutboundExecutionClock');
const SCOPE = 'GOVERNED_OUTBOUND_DURABLE_CONTROL_TENANT_IDS';
const GLOBAL = 'GOVERNED_OUTBOUND_DURABLE_SCHEDULING_ENABLED';
const logger = { log() {}, error() {} };
function env(t, changes) {
  const previous = Object.fromEntries(Object.keys(changes).map(key => [key, process.env[key]]));
  Object.entries(changes).forEach(([key, value]) => value == null ? delete process.env[key] : process.env[key] = value);
  t.after(() => Object.entries(previous).forEach(([key, value]) => value == null ? delete process.env[key] : process.env[key] = value));
}
function fixture() {
  const calls = [], rows = new Map();
  const pool = { options: { max: 10 }, async connect() {
    return { async query(sql, args) { calls.push(['legacy-lock', args?.[1]]); return { rows: [{ locked: true }] }; }, release() {} };
  } };
  const store = { leaseMs: 3000, async acquire(tenantId, stage) {
    calls.push(['claim', tenantId, stage]);
    if (rows.has(tenantId)) return { status: 'skipped', reason: 'not_due' };
    rows.set(tenantId, 'claimed');
    return { status: 'claimed', tenantId, stage, runId: randomUUID(), fence: 1, release: async () => {} };
  }, async start(c) { calls.push(['start', c.tenantId]); }, async renew() {},
  async finish(c, status) { rows.set(c.tenantId, status); calls.push(['finish', c.tenantId, status]); },
  async quarantine(c, reason) { rows.set(c.tenantId, 'uncertain'); calls.push(['quarantine', c.tenantId, reason]); } };
  return { pool, store, calls, rows, logger, supervisorOptions: { workerPath: path.join(__dirname, 'fixtures/durable-stage/controlHeldWorker.js') } };
}

test('scoped config is an exact tenant10-only opt-in, never a general allowlist', () => {
  for (const value of [undefined, '']) assert.deepEqual(scheduler.scopedDurableControlTenantIds(['10', '13'], { [SCOPE]: value }), []);
  assert.deepEqual(scheduler.scopedDurableControlTenantIds(['10', '13'], { [SCOPE]: '10' }), ['10']);
  assert.deepEqual(scheduler.scopedDurableControlTenantIds(['13'], { [SCOPE]: '10' }), []);
  for (const value of ['13', '1', '17', '*', '10,13', '10,10', '010', ' 10', '10 ', 'false', 'true']) {
    assert.throws(() => scheduler.scopedDurableControlTenantIds(['10', '13'], { [SCOPE]: value }), { code: 'invalid_durable_control_tenant_scope' });
  }
});

test('mixed dispatch uses contained durable control only for10; boolean execute is never a handler', async t => {
  env(t, { [SCOPE]: '10', [GLOBAL]: 'false' });
  const f = fixture(), legacy = [];
  const result = await dispatch.dispatchGovernedOutboundControl({ ...f, tenantIds: ['10', '13'], execute: true,
    runControl: async ({ tenantId }) => { legacy.push(tenantId); return { halted: 'no_enabled_program' }; } });
  assert.equal(result.tenants['10'].status, 'requested');
  assert.equal(result.tenants['10'].cycleId, null);
  assert.equal(result.tenants['10'].durable, true);
  assert.equal(result.tenants['13'].status, 'started');
  assert.deepEqual(legacy, ['13']);
  const terminal = await scheduler.whenStagesSettled();
  await dispatch.whenControlDispatchSettled();
  assert.equal(terminal[0].status, 'held');
  assert.equal(terminal[0].reason, 'anchor_recovery_separate_preparation_approval_required');
  assert.deepEqual(f.calls.filter(row => row[0] === 'claim'), [['claim', '10', 'control']]);
  assert.equal(f.calls.some(row => row[0] === 'quarantine'), false);
  assert.equal(f.rows.get('10'), 'held');
});

test('existing global mode remains compatible and also strips boolean execution injection', async t => {
  env(t, { [SCOPE]: undefined, [GLOBAL]: 'true' });
  const f = fixture();
  const result = await dispatch.dispatchGovernedOutboundControl({ ...f, tenantId: '10', execute: true,
    runControl: () => assert.fail('legacy override must not bypass the supervised worker') });
  assert.equal(result.status, 'requested');
  assert.equal((await scheduler.whenStagesSettled())[0].status, 'held');
});

test('missing scope preserves legacy dispatch and execute=false stays observation-only even with invalid scope', async t => {
  env(t, { [SCOPE]: undefined, [GLOBAL]: 'false' });
  const f = fixture(), seen = [];
  const options = { ...f, tenantId: '10', runControl: async o => { seen.push(o.execute); return { halted: 'no_enabled_program' }; } };
  assert.equal((await dispatch.dispatchGovernedOutboundControl(options)).tenants['10'].status, 'started');
  await dispatch.whenControlDispatchSettled();
  process.env[SCOPE] = '10,13';
  const dry = await dispatch.dispatchGovernedOutboundControl({ ...options, execute: false });
  await dispatch.whenControlDispatchSettled();
  assert.equal(dry.execute, false);
  assert.deepEqual(seen, [true, false]);
  assert.equal(f.calls.some(row => row[0] === 'claim'), false);
});

test('invalid active scope rejects before any claim, lock or legacy handler', async t => {
  env(t, { [SCOPE]: '13', [GLOBAL]: 'false' });
  const f = fixture();
  await assert.rejects(dispatch.dispatchGovernedOutboundControl({ ...f, tenantIds: ['10', '13'],
    runControl: () => assert.fail('must not fall back') }), { code: 'invalid_durable_control_tenant_scope' });
  assert.deepEqual(f.calls, []);
});

test('native control keeps scanning across multiple hung-poll ticks without adding poll loops', async t => {
  env(t, { [SCOPE]: '10', [GLOBAL]: 'false' });
  let releasePoll;
  const pollGate = new Promise(resolve => { releasePoll = resolve; });
  const events = [], requests = [];
  const handle = clock.startGovernedOutboundExecutionClock({ enabled: true, maxControlEnabled: true,
    autostart: false, tenantIds: ['10', '13'], pool: {}, logger,
    setInterval() { return { unref() {} }; }, clearInterval() {},
    cron: { poll: async () => { events.push('poll'); await pollGate; }, run: async () => ({ sent: 0 }) },
    dispatchControl: async options => { events.push('control'); requests.push(options.tenantIds); return { status: 'requested' }; } });
  const first = handle.cycle();
  await nextTurn();
  for (let i = 0; i < 4; i++) { await handle.cycle(); await nextTurn(); }
  assert.deepEqual(events.slice(0, 2), ['control', 'poll']);
  assert.equal(events.filter(x => x === 'poll').length, 1);
  assert.equal(requests.length, 5);
  assert.ok(requests.every(ids => ids.join(',') === '10'));
  releasePoll(); await first;
  assert.deepEqual(requests.at(-1), ['13'], 'legacy tenant retains its original control cadence');
  handle.stop();
  await handle.cycle();
  assert.equal(requests.length, 6);
});

test('native scoped admission itself cannot accumulate while its acknowledgement is pending', async t => {
  env(t, { [SCOPE]: '10', [GLOBAL]: 'false' });
  let resolveAck, requests = 0;
  const ack = new Promise(resolve => { resolveAck = resolve; });
  const handle = clock.startGovernedOutboundExecutionClock({ enabled: true, maxControlEnabled: true,
    autostart: false, tenantIds: ['10'], pool: {}, logger,
    setInterval() { return { unref() {} }; }, clearInterval() {},
    cron: { poll: async () => {}, run: async () => ({ sent: 0 }) },
    dispatchControl: () => { requests++; return ack; } });
  for (let i = 0; i < 20; i++) await handle.cycle();
  assert.equal(requests, 1);
  resolveAck({ status: 'requested' }); await nextTurn();
  await handle.cycle(); assert.equal(requests, 2);
  handle.stop();
});

test('native scoped control obeys both scheduler and control enable gates', async t => {
  env(t, { [SCOPE]: '10', [GLOBAL]: 'false' });
  let requests = 0, schedules = 0;
  const options = { enabled: false, maxControlEnabled: true, autostart: false,
    tenantIds: ['10'], pool: {}, logger, setInterval() { schedules++; return { unref() {} }; }, clearInterval() {},
    cron: { poll: async () => {}, run: async () => ({ sent: 0 }) }, dispatchControl: async () => { requests++; } };
  assert.equal(clock.startGovernedOutboundExecutionClock(options), null);
  assert.equal(schedules, 0);
  const disabledControl = clock.startGovernedOutboundExecutionClock({ ...options, enabled: true, maxControlEnabled: false });
  await disabledControl.cycle(); disabledControl.stop();
  assert.equal(requests, 0);
});

test('invalid native scope fails closed rather than running legacy control', async t => {
  env(t, { [SCOPE]: '*', [GLOBAL]: 'false' });
  let effects = 0;
  const options = { enabled: true, maxControlEnabled: true,
    autostart: false, tenantIds: ['10', '13'], pool: {}, logger,
    setInterval() { effects++; return { unref() {} }; }, clearInterval() {},
    cron: { poll: async () => { effects++; }, run: async () => { effects++; } },
    dispatchControl: async () => { effects++; } };
  assert.throws(() => clock.startGovernedOutboundExecutionClock(options), { code: 'invalid_durable_control_tenant_scope' });
  process.env[GLOBAL] = 'true';
  assert.throws(() => clock.startGovernedOutboundExecutionClock(options), { code: 'invalid_durable_control_tenant_scope' });
  await assert.rejects(dispatch.dispatchGovernedOutboundControl({ pool: {}, tenantId: '10' }), { code: 'invalid_durable_control_tenant_scope' });
  assert.equal(effects, 0);
  for (const value of ['', '10']) {
    process.env[SCOPE] = value;
    const handle = clock.startGovernedOutboundExecutionClock(options);
    assert.ok(handle);
    handle.stop();
  }
  assert.equal(effects, 2, 'valid/absent scoped value preserves existing global timer behavior');
});

test('database UTC instants define the exact cooldown boundary across midnight and both DST transitions', async () => {
  const { PostgresStageStore } = require('../services/governedOutboundStageStore');
  for (const instant of ['2026-10-26T23:59:59.999Z', '2026-11-01T01:59:59.999-04:00',
    '2026-11-01T01:00:00.000-05:00', '2027-03-14T03:00:00.000-04:00']) {
    for (const offsetMs of [-1, 0, 1]) {
      const now = new Date(instant);
      const row = { status: 'due', due_at: new Date(+now + offsetMs), db_now: now, attempts: 0, fence: 0 };
      const pool = { async connect() { return { release() {}, async query({ text, values }) {
        if (text.startsWith('SELECT *,')) return { rows: [row] };
        if (text.startsWith('UPDATE governed_outbound_stage_schedule')) return { rows: [{ ...row, fence: 1, attempts: 1 }] };
        return { rows: [] };
      } }; } };
      const claim = await new PostgresStageStore(pool).acquire('10', 'control');
      assert.equal(claim.status, offsetMs > 0 ? 'skipped' : 'claimed', `${instant} offset ${offsetMs}`);
      if (offsetMs > 0) assert.equal(claim.reason, 'not_due');
    }
  }
});
