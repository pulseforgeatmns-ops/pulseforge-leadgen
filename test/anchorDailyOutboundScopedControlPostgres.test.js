'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
const { PostgresStageStore } = require('../services/governedOutboundStageStore');
const scheduler = require('../services/governedOutboundStageScheduler');
const dispatch = require('../services/governedOutboundControlDispatch');
const { startGovernedOutboundExecutionClock } = require('../services/governedOutboundExecutionClock');
const logger = { log() {}, error() {} };
function environment(t, changes) {
  const saved = Object.fromEntries(Object.keys(changes).map(key => [key, process.env[key]]));
  for (const [key, value] of Object.entries(changes)) value == null ? delete process.env[key] : process.env[key] = value;
  t.after(() => { for (const [key, value] of Object.entries(saved)) value == null ? delete process.env[key] : process.env[key] = value; });
}

test('scoped native/external control shares real durable state without providers or fabricated completion', { timeout: 90000 }, async t => {
  const instance = await startDisposablePostgres('scoped-control-');
  const pool = createTrackedPostgresPool({ connectionString: instance.connectionString });
  t.after(() => closePostgresFixture(pool, instance));
  const port = new URL(instance.connectionString).port;
  environment(t, { DATABASE_URL: instance.connectionString, DATABASE_SSL: 'false',
    GOVERNED_OUTBOUND_DURABLE_CONTROL_TENANT_IDS: '10', GOVERNED_OUTBOUND_DURABLE_SCHEDULING_ENABLED: 'false',
    ANCHOR_EMAIL_RECOVERY_MAINTENANCE: 'true', ANCHOR_RESEARCH_PROVIDERS_PAUSED: 'true',
    ANCHOR_GOVERNED_OUTBOUND_ENABLED: 'false', ANCHOR_GOVERNED_OUTBOUND_SCHEDULER_ENABLED: 'true',
    ANCHOR_EMAIL_SCOPED_RESUME_ID: undefined, ANCHOR_EMAIL_RECOVERY_MANIFEST_ID: undefined,
    TEST_STAGE_PG_PORT: port,
    NODE_OPTIONS: `${process.env.NODE_OPTIONS || ''} --require=${path.join(__dirname, 'fixtures/durable-stage/denyExternalNetwork.cjs')}`.trim(),
    CRON_SECRET: 'scoped-native-local-fixture' });
  await t.test('absent prerequisite cannot fall back to legacy execution', async () => {
    const ack = await dispatch.dispatchGovernedOutboundControl({ pool, tenantId: '10', logger,
      runControl: () => assert.fail('missing schema must not fall back') });
    assert.equal(ack.tenants['10'].status, 'requested');
    const terminal = await scheduler.whenStagesSettled();
    assert.equal(terminal[0].status, 'failed');
    assert.equal(terminal[0].reason, '42P01');
    assert.equal((await pool.query("SELECT to_regclass('governed_outbound_stage_schedule') AS table_name")).rows[0].table_name, null);
  });
  await pool.query(fs.readFileSync(path.join(__dirname, '../migrations/2026-10-21-governed-stage-scheduling.sql'), 'utf8'));
  // Minimal synthetic program/control prerequisite. No source mission, provider
  // credentials, grants, recipients, envelopes, tasks or send tables exist.
  await pool.query(`CREATE TABLE acquisition_outbound_programs(id text PRIMARY KEY,tenant_id text,mode text,authorized_at timestamptz);
    INSERT INTO acquisition_outbound_programs VALUES('local-held-program','10','active',clock_timestamp());
    CREATE TABLE anchor_email_recovery_control(singleton boolean,maintenance_armed boolean,manifest jsonb);
    INSERT INTO anchor_email_recovery_control VALUES(true,true,null);`);
  const dbPath = require.resolve('../db'), previousDb = require.cache[dbPath];
  require.cache[dbPath] = { id: dbPath, filename: dbPath, loaded: true, exports: pool };
  const routerPath = require.resolve('../routes/cron'), previousRouter = require.cache[routerPath];
  delete require.cache[routerPath];
  const router = require('../routes/cron');
  const handler = router.stack.find(layer => layer.route?.path === '/cron/anchor-max-outbound-control').route.stack[0].handle;
  t.after(() => { if (previousDb) require.cache[dbPath] = previousDb; else delete require.cache[dbPath];
    if (previousRouter) require.cache[routerPath] = previousRouter; else delete require.cache[routerPath]; });
  async function external() {
    let body, status = 200;
    await handler({ query: { tenant_id: '10' }, get: () => 'Bearer scoped-native-local-fixture' },
      { set() { return this; }, status(code) { status = code; return this; }, json(value) { body = value; return this; } });
    assert.equal(status, 200);
    return body;
  }
  function nativeClock() { return startGovernedOutboundExecutionClock({ enabled: true, maxControlEnabled: true,
    tenantIds: ['10'], autostart: false, pool, logger, setInterval() { return { unref() {} }; }, clearInterval() {},
    cron: { poll: async () => ({ results: [] }), run: async () => ({ sent: 0 }) } }); }
  const rows = async () => (await pool.query('SELECT * FROM governed_outbound_stage_runs ORDER BY due_at')).rows;
  await t.test('actual cron handler and native clock contend for one safe held native worker', async () => {
    const clock = nativeClock();
    const [ack] = await Promise.all([external(), clock.cycle()]);
    clock.stop();
    assert.ok(['requested', 'already_requested'].includes(ack.tenants['10'].status));
    const terminal = await scheduler.whenStagesSettled();
    assert.equal(terminal.length, 1);
    assert.equal(terminal[0].status, 'held');
    assert.equal(terminal[0].reason, 'anchor_recovery_separate_preparation_approval_required');
    const runs = await rows();
    assert.equal(runs.length, 1);
    assert.equal(runs[0].status, 'held');
    assert.ok(runs[0].started_at && runs[0].finished_at);
    assert.deepEqual(runs[0].evidence_refs, { programId: 'local-held-program' });
    const schedule = (await pool.query('SELECT *,extract(epoch from due_at-updated_at) AS cooldown FROM governed_outbound_stage_schedule')).rows;
    assert.equal(schedule.length, 1);
    assert.equal(schedule[0].tenant_id, '10');
    assert.equal(schedule[0].stage, 'control');
    assert.equal(schedule[0].last_completed_at, null);
    assert.equal(schedule[0].last_outcome, 'held');
    assert.equal(schedule[0].claim_token, null);
    assert.ok(Number(schedule[0].cooldown) > 899.9 && Number(schedule[0].cooldown) < 900.1);
  });
  await t.test('sequential external retry and restarted native clock cannot replay same due slot', async () => {
    assert.equal((await external()).tenants['10'].status, 'requested');
    assert.equal((await scheduler.whenStagesSettled())[0].reason, 'not_due');
    const clock = nativeClock(); await clock.cycle(); clock.stop();
    assert.equal((await scheduler.whenStagesSettled())[0].reason, 'not_due');
    assert.equal((await rows()).length, 1);
  });
  await t.test('fresh process restart reuses durable cooldown rather than its startup-relative counter', async () => {
    const script = `const pool=require('./db'); const d=require('./services/governedOutboundControlDispatch');
      const s=require('./services/governedOutboundStageScheduler');
      (async()=>{const ack=await d.dispatchGovernedOutboundControl({pool,tenantId:'10',logger:{}});
        const terminal=await s.whenStagesSettled();console.log(JSON.stringify({ack,terminal}));await pool.end();})().catch(e=>{console.error(e.code||e.message);process.exitCode=1});`;
    const { stdout } = await promisify(execFile)(process.execPath, ['-e', script], { cwd: path.join(__dirname, '..'), env: process.env, timeout: 10000 });
    const result = JSON.parse(stdout.trim().split('\n').at(-1));
    assert.equal(result.ack.tenants['10'].status, 'requested');
    assert.equal(result.terminal[0].reason, 'not_due');
    assert.equal((await rows()).length, 1);
  });
  await t.test('timezone presentation and synthetic next due slot do not invent business success', async () => {
    for (const zone of ['UTC', 'America/New_York', 'Pacific/Auckland']) {
      const db = await pool.connect();
      try {
        await db.query("SELECT set_config('TimeZone',$1,false)", [zone]);
        assert.equal((await db.query('SHOW TimeZone')).rows[0].TimeZone, zone);
        // Pin the store to this very session; changing an unrelated borrowed
        // session would not exercise its timestamp representation at all.
        const sessionPool = { connect: async () => ({ query: db.query.bind(db), release() {} }) };
        const store = new PostgresStageStore(sessionPool);
        assert.equal((await store.acquire('10', 'control')).reason, 'not_due');
      } finally { await db.query("SET TIME ZONE 'UTC'"); db.release(); }
    }
    // Test-only clock positioning, never represented as a natural production interval.
    await pool.query("UPDATE governed_outbound_stage_schedule SET due_at=clock_timestamp()-interval '1 second' WHERE tenant_id='10' AND stage='control'");
    await external();
    assert.equal((await scheduler.whenStagesSettled())[0].status, 'held');
    const runs = await rows();
    assert.equal(runs.length, 2);
    assert.notEqual(runs[0].run_id, runs[1].run_id);
    assert.ok(runs.every(row => row.status === 'held'));
    assert.equal((await pool.query('SELECT last_completed_at FROM governed_outbound_stage_schedule')).rows[0].last_completed_at, null);
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM acquisition_outbound_programs')).rows[0].n, 1);
  });
  for (const [label, fixture] of [['invalid receipt', 'receiptWorker.js'], ['timeout', 'supervisedWorker.js']]) {
    await t.test(`contained worker ${label} stays durably uncertain across retry and cannot release another fence`, async () => {
      await pool.query('TRUNCATE governed_outbound_stage_runs,governed_outbound_stage_schedule');
      await dispatch.dispatchGovernedOutboundControl({ pool, tenantId: '10', logger, maxRuntimeMs: 1000,
        supervisorOptions: { workerPath: path.join(__dirname, 'fixtures/durable-stage', fixture), graceMs: 20 } });
      const terminal = (await scheduler.whenStagesSettled())[0];
      assert.equal(terminal.status, 'uncertain');
      const run = (await rows())[0];
      assert.equal(run.status, 'uncertain');
      assert.ok(run.started_at && run.finished_at);
      const store = new PostgresStageStore(pool);
      assert.equal((await store.acquire('10', 'control')).status, 'uncertain');
      await assert.rejects(store.finish({ tenantId: '10', stage: 'control', runId: run.run_id,
        fence: String(Number(run.fence) + 1), token: '00000000-0000-0000-0000-000000000000' }, 'completed'), { code: 'scheduler_claim_lost' });
      await external();
      assert.equal((await scheduler.whenStagesSettled())[0].status, 'uncertain');
      assert.equal((await rows()).length, 1);
      const state = (await pool.query('SELECT status,last_completed_at,claim_token,lease_until FROM governed_outbound_stage_schedule')).rows[0];
      assert.deepEqual(state, { status: 'uncertain', last_completed_at: null, claim_token: null, lease_until: null });
    });
  }
});
