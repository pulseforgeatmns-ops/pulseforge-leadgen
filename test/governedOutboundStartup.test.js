'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const outbound = require('../services/governedOutbound');
const clock = require('../services/governedOutboundExecutionClock');
const legacy = require('../services/anchorGovernedScheduler');
const tenants = require('../services/governedOutboundTenant');
const cron = require('../anchorDailyOutboundCron');

const flags = [
  'BABRUN_GOVERNED_OUTBOUND_ENABLED', 'GOVERNED_OUTBOUND_TENANT_13_ENABLED',
  'SUBSTRAL_GOVERNED_OUTBOUND_ENABLED', 'GOVERNED_OUTBOUND_TENANT_17_ENABLED',
  'GOVERNED_OUTBOUND_EXECUTION_CLOCK_ENABLED', 'GOVERNED_OUTBOUND_TENANT_IDS',
  'ANCHOR_GOVERNED_OUTBOUND_SCHEDULER_ENABLED', 'ANCHOR_GOVERNED_OUTBOUND_ENABLED',
  'ANCHOR_MAX_OUTBOUND_CONTROL_ENABLED',
];
function environment(t, values) {
  const saved = Object.fromEntries(flags.map(key => [key, process.env[key]]));
  for (const key of flags) delete process.env[key];
  Object.assign(process.env, values);
  t.after(() => {
    for (const key of flags) {
      if (saved[key] === undefined) delete process.env[key];
      else process.env[key] = saved[key];
    }
  });
}

// Execute the actual server startup section, including the real scheduler/clock
// entrypoints. Unrelated web/schema/worker dependencies are inert; cron.run,
// governed tick and event persistence remain real, backed by an in-memory DB.
function boot(t) {
  const events = new Map();
  const calls = [];
  const timers = [];
  const errors = [];
  const handles = [];
  let now = new Date('2026-10-01T15:00:00Z');
  const pool = {
    async query(sql, args) {
      assert.match(sql, /INSERT INTO acquisition_outbound_events/);
      const [id, tenantId, , , , type, payload] = args;
      if (events.has(id)) return { rows: [] };
      events.set(id, { tenantId, type, payload });
      return { rows: [{ id }] };
    },
  };
  t.mock.method(outbound, 'productionService', (db, { tenantId }) => {
    assert.equal(db, pool);
    calls.push(`tick:${tenantId}`);
    const service = outbound.service({ pool, tenantId, adapters: {}, now: () => now });
    service.store.lock = fn => fn();
    // A paused grant exercises the real blocked-tick observability path without
    // preparing or sending anything. Clock enablement still uses the live flags.
    service.store.program = async () => ({ id: `program_${tenantId}`, mode: 'paused' });
    service.store.health = async () => {};
    return service;
  });
  const injected = {
    cron: {
      ...cron,
      poll: async options => {
        calls.push(`poll:${options.tenantIds.join(',')}`);
        if (options.tenantIds.some(id => id !== '10')) assert.equal(options.mailboxOnly, false);
      },
    },
    logger: { log() {}, error(...args) { errors.push(args); } },
    dispatchControl: async ({ tenantIds }) => { calls.push(`max:${tenantIds.join(',')}`); },
    setInterval(callback, ms) {
      assert.equal(ms, 60000);
      const timer = { callback, unref() {} };
      timers.push(timer);
      return timer;
    },
    clearInterval() {},
  };
  const noop = () => Promise.resolve();
  const stub = new Proxy(noop, { get: () => noop });
  const startup = fs.readFileSync(require.resolve('../server'), 'utf8').split('app.use(session(')[0];
  vm.runInNewContext(startup, {
    process: { env: process.env, on() {} }, console,
    require(name) {
      if (name === './db') return pool;
      if (name === './services/governedOutboundTenant') return tenants;
      if (name === './services/anchorGovernedScheduler') return {
        startAnchorGovernedScheduler: options => {
          const handle = legacy.startAnchorGovernedScheduler({ ...injected, ...options });
          handles.push(handle);
          return handle;
        },
      };
      if (name === './services/governedOutboundExecutionClock') return {
        startGovernedOutboundExecutionClock: options => {
          const handle = clock.startGovernedOutboundExecutionClock({ ...injected, ...options });
          handles.push(handle);
          return handle;
        },
      };
      return stub;
    },
  }, { filename: 'server.js' });
  t.after(() => handles.forEach(handle => handle?.stop()));
  return {
    calls, timers, events, errors,
    flush: () => new Promise(resolve => setImmediate(resolve)),
    async minute() {
      now = new Date(+now + 60000);
      for (const timer of timers) await timer.callback();
    },
  };
}

test('application startup arms recurring Babrun ticks without a new clock or legacy flag', async t => {
  environment(t, { BABRUN_GOVERNED_OUTBOUND_ENABLED: 'true' });
  const app = boot(t);
  await app.flush();
  assert.equal(app.timers.length, 1);
  assert.deepEqual(app.calls, ['poll:13', 'tick:13']);
  for (let i = 0; i < 4; i++) await app.minute();
  assert.equal(app.calls.filter(call => call === 'tick:13').length, 1);
  await app.minute();
  assert.equal(app.calls.filter(call => call === 'tick:13').length, 2);
  const evaluated = [...app.events.values()].filter(event => event.type === 'governed_tick_evaluated');
  assert.equal(evaluated.length, 2);
  assert.ok(evaluated.every(event => event.tenantId === '13'
    && event.payload.tenantId === '13' && event.payload.halted === 'program_disabled'));
  assert.deepEqual(app.errors, []);
});

test('production flags start separate owners and preserve Anchor poll -> tick -> Max', async t => {
  environment(t, {
    BABRUN_GOVERNED_OUTBOUND_ENABLED: 'true', GOVERNED_OUTBOUND_TENANT_IDS: '10,13',
    ANCHOR_GOVERNED_OUTBOUND_SCHEDULER_ENABLED: 'true',
    ANCHOR_GOVERNED_OUTBOUND_ENABLED: 'true', ANCHOR_MAX_OUTBOUND_CONTROL_ENABLED: 'true',
  });
  const app = boot(t);
  await app.flush();
  assert.equal(app.timers.length, 2);
  for (let i = 0; i < 15; i++) await app.minute();
  assert.deepEqual(app.calls.filter(call => call.endsWith(':10')), [
    'poll:10', 'tick:10', 'max:10',
    ...Array(4).fill('poll:10'), 'poll:10', 'tick:10',
    ...Array(4).fill('poll:10'), 'poll:10', 'tick:10',
    ...Array(4).fill('poll:10'), 'poll:10', 'tick:10', 'max:10',
  ]);
  assert.equal(app.calls.filter(call => call === 'tick:13').length, 4);
  assert.ok(!app.calls.includes('max:13'));
  assert.deepEqual(app.errors, []);
});

for (const [name, values, expected] of [
  ['disabled by default', {}, []],
  ['explicit global stop overrides Babrun', {
    BABRUN_GOVERNED_OUTBOUND_ENABLED: 'true', GOVERNED_OUTBOUND_EXECUTION_CLOCK_ENABLED: 'false',
  }, []],
  ['explicit global stop preserves legacy Anchor flag', {
    BABRUN_GOVERNED_OUTBOUND_ENABLED: 'true', GOVERNED_OUTBOUND_EXECUTION_CLOCK_ENABLED: 'false',
    ANCHOR_GOVERNED_OUTBOUND_SCHEDULER_ENABLED: 'true',
  }, ['10']],
  ['configured IDs exclude Babrun despite enablement', {
    BABRUN_GOVERNED_OUTBOUND_ENABLED: 'true', GOVERNED_OUTBOUND_TENANT_IDS: '10',
  }, []],
  ['explicit global opt-in uses configured mailbox IDs once', {
    GOVERNED_OUTBOUND_EXECUTION_CLOCK_ENABLED: 'true', GOVERNED_OUTBOUND_TENANT_IDS: '13,17,13',
  }, ['13', '17']],
  ['Babrun alias enables its clock', { GOVERNED_OUTBOUND_TENANT_13_ENABLED: 'true' }, ['13']],
  ['Substral enablement uses the same generic boundary', {
    SUBSTRAL_GOVERNED_OUTBOUND_ENABLED: 'true', GOVERNED_OUTBOUND_TENANT_IDS: '17',
  }, ['17']],
]) {
  test(`startup predicates: ${name}`, async t => {
    environment(t, values);
    const app = boot(t);
    await app.flush();
    assert.deepEqual(app.calls.filter(call => call.startsWith('tick:')), expected.map(id => `tick:${id}`));
    assert.deepEqual(app.errors, []);
  });
}
