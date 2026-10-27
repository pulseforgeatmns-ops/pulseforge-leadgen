'use strict';

// SPEC-SCOUT-P0-001: Anchor replenishment must distinguish governed holds from
// failed or empty runs, and one failed enrichment row must not abort a cycle.
// Synthetic pools and transports only; no provider or email traffic.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');

const AGENT_LOG_STATUSES = new Set(['success', 'failed', 'skipped', 'pending', 'completed', 'posted', 'in_progress']);
const quiet = { log() {}, warn() {}, error() {} };

function load(relative, overrides = {}) {
  const filename = path.join(__dirname, '..', relative);
  const nativeRequire = createRequire(filename);
  const module = { exports: {} };
  const deps = { dotenv: { config() {} }, ...overrides };
  vm.runInNewContext(fs.readFileSync(filename, 'utf8'), {
    module, exports: module.exports, require: name => Object.hasOwn(deps, name) ? deps[name] : nativeRequire(name),
    process, console: quiet, setTimeout, clearTimeout, setInterval, URL, Date, Buffer,
  }, { filename });
  return module.exports;
}

function withEnv(t, values) {
  for (const [key, value] of Object.entries(values)) {
    const prior = process.env[key];
    if (value == null) delete process.env[key]; else process.env[key] = value;
    t.after(() => { if (prior == null) delete process.env[key]; else process.env[key] = prior; });
  }
}

// agent_log_status_check is enforced like production so a 'partial' write fails.
function enrichmentFixture(records, promote) {
  const calls = [];
  const db = { query: async (sql, params = []) => {
    calls.push({ sql, params });
    if (/INSERT INTO agent_log/.test(sql)) {
      const status = /'skipped'/.test(sql) ? 'skipped' : params[2];
      if (!AGENT_LOG_STATUSES.has(status)) {
        throw Object.assign(new Error('new row for relation "agent_log" violates check constraint "agent_log_status_check"'), { code: '23514' });
      }
    }
    return { rows: /SELECT \*/.test(sql) ? records : [] };
  } };
  const agent = load('scoutUnenrichedEnrichmentAgent.js', {
    './db': db,
    './utils/scoutUnenrichedSchema': { ensureScoutUnenrichedTable: async () => {} },
    './utils/emailVerificationSchema': { ensureEmailVerificationColumns: async () => {} },
    './scripts/promoteUnenriched': { promoteRecord: promote },
  });
  return { agent, db, calls };
}

const record = id => ({ id, client_id: 10, company: `Co ${id}`, domain: `${id}.example`, enrichment_attempts: 0 });

test('one failed enrichment row keeps completed promotions and writes an allowed agent_log status', async t => {
  withEnv(t, { ANCHOR_RESEARCH_PROVIDERS_PAUSED: null });
  const f = enrichmentFixture([record('a'), record('b')], async row => {
    if (row.id === 'b') throw new Error('Hunter 502');
    return { promoted: true, emailResolved: true, emailVerified: true, prospectId: 501 };
  });
  const summary = await f.agent.run({ client_id: 10, db: f.db });
  assert.equal(summary.promoted, 1);
  assert.equal(summary.failed, 1);
  const log = f.calls.find(call => /INSERT INTO agent_log/.test(call.sql));
  assert.ok(log, 'run summary is durably logged');
  assert.equal(log.params[2], 'success');
  assert.match(log.params[3], /1 row\(s\) failed/);
  // The failed row consumes exactly one attempt, so it is not reselected immediately.
  const failedUpdates = f.calls.filter(call => /UPDATE scout_unenriched/.test(call.sql) && call.params[0] === 'b');
  assert.ok(failedUpdates.length >= 1);
});

test('a rejected run-log write never discards the enrichment summary', async t => {
  withEnv(t, { ANCHOR_RESEARCH_PROVIDERS_PAUSED: null });
  const f = enrichmentFixture([record('a')], async () => ({ promoted: true, prospectId: 7 }));
  const original = f.db.query;
  f.db.query = async (sql, params) => {
    if (/INSERT INTO agent_log/.test(sql)) throw new Error('connection reset');
    return original(sql, params);
  };
  const summary = await f.agent.run({ client_id: 10, db: f.db });
  assert.equal(summary.promoted, 1);
});

test('Max enrichment batches survive a failed row instead of aborting the replenishment cycle', async t => {
  withEnv(t, { ANCHOR_RESEARCH_PROVIDERS_PAUSED: null });
  const f = enrichmentFixture([record('a'), record('b')], async row => {
    if (row.id === 'a') throw new Error('verifier timeout');
    return { promoted: true, prospectId: 9 };
  });
  const { _test } = require('../services/maxOutboundControlLoop');
  const result = await _test.runEnrichmentBatches(f.agent, f.db, 2, { tenantId: '10', program: { tenant_id: '10' } });
  assert.equal(result.promoted, 1);
});

test('paused research is recorded as a governed block, distinct from an empty run', async t => {
  withEnv(t, { ANCHOR_RESEARCH_PROVIDERS_PAUSED: 'true' });
  let promotions = 0;
  const f = enrichmentFixture([record('a')], async () => { promotions++; return { promoted: true }; });
  await assert.rejects(f.agent.run({ client_id: 10, db: f.db }), { code: 'anchor_research_provider_paused' });
  assert.equal(promotions, 0);
  const logs = f.calls.filter(call => /INSERT INTO agent_log/.test(call.sql));
  assert.equal(logs.length, 1);
  assert.equal(JSON.parse(logs[0].params[2]).reason, 'anchor_research_provider_paused');
  assert.equal(logs[0].params[4], 10);
  assert.equal(f.calls.some(call => /UPDATE scout_unenriched/.test(call.sql)), false, 'no attempt consumed');
});

test('tenant isolation: armed Anchor pause leaves non-Anchor enrichment and its log unchanged', async t => {
  withEnv(t, { ANCHOR_RESEARCH_PROVIDERS_PAUSED: 'true' });
  const f = enrichmentFixture([{ ...record('x'), client_id: 13 }], async () => ({ promoted: true }));
  const summary = await f.agent.run({ client_id: 13, db: f.db });
  assert.equal(summary.promoted, 1);
  assert.equal(f.calls.some(call => /'skipped'/.test(call.sql)), false);
});

function maxOptions(pool, overrides = {}) {
  const inventory = { clean: [], excluded: [], scope: {}, exclusionCounts: {} };
  const events = [];
  return {
    events,
    options: {
      pool, tenantId: '10', logger: quiet, now: new Date('2026-10-19T16:00:00Z'),
      program: { id: 'local-10', tenant_id: '10', mode: 'active', source_mission_id: 'source-10', policy: { dailyCap: 5 } },
      source: { id: 'source-10', payload: {} },
      store: { tenantId: '10', clientId: 10, event: async (type, key, data) => { events.push({ type, data }); },
        envelope: async () => null },
      inventory, inventoryAfter: inventory, timestamps: {}, funnel: {},
      infrastructure: { cap: 5, snapshot: { sentToday: 0 },
        operating: { planningDailyCapacity: 5, authorizationLimitedCapacity: 5,
          dispatchCapacityNow: 5, scheduleLimitedCapacity: 5, governor: 'proceed' } },
      refillAoBooks: async () => ({ assigned: [] }),
      ...overrides,
    },
  };
}

function recordingPool(controlRow = null) {
  const calls = [];
  return { calls, query: async (sql, params) => {
    calls.push({ sql, params });
    if (/to_regclass\('public.anchor_email_recovery_control'\)/.test(sql)) {
      return { rows: [{ control_table: controlRow ? 'anchor_email_recovery_control' : null }] };
    }
    if (/FROM anchor_email_recovery_control/.test(sql)) return { rows: controlRow ? [controlRow] : [] };
    return { rows: [] };
  } };
}

const holds = pool => pool.calls.filter(call => /scout_replenishment_blocked/.test(call.sql));

test('legacy Anchor cycle with paused research completes, records the block and never calls Scout', async t => {
  withEnv(t, { ANCHOR_RESEARCH_PROVIDERS_PAUSED: 'true', ANCHOR_EMAIL_RECOVERY_MAINTENANCE: null,
    ANCHOR_EMAIL_RECOVERY_MANIFEST_ID: null, ANCHOR_EMAIL_SCOPED_RESUME_ID: null });
  const { runMaxOutboundControlLoop } = require('../services/maxOutboundControlLoop');
  const pool = recordingPool();
  let ramps = 0;
  const { options, events } = maxOptions(pool, { scoutRamp: async () => { ramps++; return {}; } });
  const result = await runMaxOutboundControlLoop(options);
  assert.equal(ramps, 0, 'no provider spend path entered');
  assert.equal(result.discoveryBlockedReason, 'anchor_research_provider_paused');
  const control = events.find(event => event.type === 'max_outbound_control');
  assert.ok(control, 'the cycle still writes its control event');
  assert.equal(control.data.discoveryBlockedReason, 'anchor_research_provider_paused');
  assert.equal(control.data.scoutInvoked, false);
  const [hold] = holds(pool);
  assert.ok(hold, 'operator attention is raised');
  assert.match(hold.sql, /WHERE NOT EXISTS/);
  assert.equal(hold.params[3], 10);
  assert.equal(hold.params[4], 'anchor_research_provider_paused');
});

test('unpaused legacy Anchor cycle still invokes Scout and raises no hold', async t => {
  withEnv(t, { ANCHOR_RESEARCH_PROVIDERS_PAUSED: null, ANCHOR_EMAIL_RECOVERY_MAINTENANCE: null,
    ANCHOR_EMAIL_RECOVERY_MANIFEST_ID: null, ANCHOR_EMAIL_SCOPED_RESUME_ID: null });
  const { runMaxOutboundControlLoop } = require('../services/maxOutboundControlLoop');
  const pool = recordingPool();
  let ramps = 0;
  const { options } = maxOptions(pool, { scoutRamp: async () => { ramps++; return { admission: {} }; } });
  const result = await runMaxOutboundControlLoop(options);
  assert.equal(ramps, 1);
  assert.equal(result.discoveryBlockedReason, null);
  assert.equal(holds(pool).length, 0);
});

test('recovery maintenance halt raises one deduplicated operator hold and performs no research', async t => {
  withEnv(t, { ANCHOR_RESEARCH_PROVIDERS_PAUSED: 'true', ANCHOR_EMAIL_RECOVERY_MAINTENANCE: 'true' });
  const { runMaxOutboundControlLoop } = require('../services/maxOutboundControlLoop');
  const pool = recordingPool({ singleton: true, maintenance_armed: true, manifest: null });
  let effects = 0;
  const blocked = async () => { effects++; throw new Error('Unapproved callback'); };
  const { options } = maxOptions(pool, { scoutRamp: blocked, refillAoBooks: blocked });
  const result = await runMaxOutboundControlLoop(options);
  assert.equal(result.halted, 'anchor_recovery_separate_preparation_approval_required');
  assert.equal(effects, 0);
  const [hold] = holds(pool);
  assert.ok(hold);
  assert.match(hold.sql, /status = 'pending' AND payload->>'reason' = \$5/);
  assert.equal(JSON.parse(hold.params[2]).detail, 'maintenance:recovery_kill_switch');
});

test('non-Anchor tenants never receive Anchor replenishment holds', async () => {
  const { _test } = require('../services/maxOutboundControlLoop');
  const pool = recordingPool();
  await _test.recordReplenishmentHold(pool, { tenantId: '13', clientId: 13, reason: 'anchor_research_provider_paused', logger: quiet });
  assert.equal(pool.calls.length, 0);
});

test('a hold-record failure never breaks the control cycle', async () => {
  const { _test } = require('../services/maxOutboundControlLoop');
  await _test.recordReplenishmentHold({ query: async () => { throw new Error('db down'); } },
    { tenantId: '10', clientId: 10, reason: 'x', logger: quiet });
});

function loadCron(pool) {
  const stub = (rel, exports) => {
    const id = require.resolve(rel);
    require.cache[id] = { id, filename: id, loaded: true, exports };
  };
  stub('../db', pool);
  stub('../utils/agentDispatchPolicy', { BLOCK_REASON: 'blocked', isAgentEnabledForClient: async () => ({ allowed: true }),
    logBlockedDispatch: async () => {} });
  delete require.cache[require.resolve('../routes/cron')];
  return require('../routes/cron');
}

test('cron Scout for paused Anchor answers skipped with a durable record instead of success', async t => {
  withEnv(t, { ANCHOR_RESEARCH_PROVIDERS_PAUSED: 'true' });
  const pool = recordingPool();
  const cron = loadCron(pool);
  for (const agent of ['scout', 'scout_unenriched_enrichment']) {
    let body = null;
    await cron.runCronAgent(agent, { json(value) { body = value; return this; }, status() { return this; } }, { client_id: '10' });
    assert.deepEqual({ success: body.success, skipped: body.skipped, reason: body.reason },
      { success: false, skipped: true, reason: 'anchor_research_provider_paused' });
  }
  const logs = pool.calls.filter(call => /INSERT INTO agent_log/.test(call.sql));
  assert.equal(logs.length, 2);
  assert.deepEqual(logs.map(call => call.params[0]), ['scout', 'scout_unenriched_enrichment']);
});
