'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const Module = require('node:module');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const http = require('node:http');
const net = require('node:net');
const dns = require('node:dns');
const { spawnSync } = require('node:child_process');
const script = path.resolve(__dirname, '../scripts/audit062WarmRoutingObservation.js');

const importState = { load: Module._load, cache: Module._cache, env: process.env, fetch: globalThis.fetch };
const { runAudit, main, withOfflineSandbox } = require(script);

function snapshot() {
  return {
    load: Module._load, cache: Module._cache, env: process.env,
    fetch: globalThis.fetch, interval: globalThis.setInterval, request: http.request,
    connect: net.Socket.prototype.connect, read: fs.readFileSync, write: fs.writeFileSync,
  };
}
function assertRestored(before) {
  assert.deepEqual(snapshot(), before);
}

test('import is inert and exposes only explicitly invoked entry points', () => {
  assert.equal(Module._load, importState.load);
  assert.equal(Module._cache, importState.cache);
  assert.equal(process.env, importState.env);
  assert.equal(globalThis.fetch, importState.fetch);
  assert.equal(typeof runAudit, 'function');
  assert.equal(require.cache[require.resolve('../warmRoutingAgent')], undefined);
  assert.equal(require.cache[require.resolve('../db')], undefined);
});

test('all three fixture paths execute without live I/O or scheduler startup', async () => {
  const before = snapshot();
  const keys = Object.keys(require.cache);
  const report = await runAudit();
  assertRestored(before);
  assert.deepEqual(Object.keys(require.cache), keys);
  assert.equal(report.mode, 'offline-fixtures');
  assert.deepEqual(report.fixtureCalls, ['router', 'direct_scout_repository', 'direct_approval']);
  assert.deepEqual(report.blockedIoAttempts, []);
  assert.deepEqual(report.mission.warmRoutingLoadedDuringMission, []);
  assert.equal(report.mission.router.specialist, 'scout');
  assert.equal(report.mission.router.action, 'discovery_approved');
  assert.equal(report.mission.router.executionOutcome, 'completed');
  assert.equal(report.mission.directApproval.executionOutcome, 'completed');
  assert.notEqual(report.mission.missionId, report.mission.directApprovalMissionId);
  assert.equal(report.mission.scout.outcome, 'DISCOVERY_BLOCKED');
  assert.equal(report.mission.scout.hasMarketDefinition, true);
  assert.deepEqual(report.mission.scout.stages, ['understand_market']);
  assert.equal(report.routing.disabled.reason, 'WARM_ROUTING_ENABLED_not_true');
  assert.equal(report.routing.enabledUnseeded.reason, 'warm_routing_seed_incomplete');
  assert.equal(report.routing.dbQueriesOnDisabledPath, 0);
  assert.equal(report.routing.fixtureDbQueries.length, 1);
  assert.deepEqual(report.routing.fixtureDbQueries[0].params, [9062, '2026-07-04-edge-v1']);
  assert.equal(report.routing.schedulerStarted, false);
  assert.match(report.limitations.join(' '), /not production/);
});

test('ambient credentials are never read and previous DB module cache is preserved', async () => {
  const originalEnv = process.env;
  const dbPath = require.resolve('../db');
  const priorDb = require.cache[dbPath];
  const sentinel = { exports: { query() { assert.fail('ambient DB must not be used'); } } };
  const ambient = new Proxy({}, { get() { throw new Error('ambient credential read'); }, ownKeys() { throw new Error('ambient enumeration'); } });
  try {
    process.env = ambient;
    require.cache[dbPath] = sentinel;
    await runAudit();
    assert.equal(process.env, ambient);
    assert.equal(require.cache[dbPath], sentinel);
  } finally {
    process.env = originalEnv;
    if (priorDb) require.cache[dbPath] = priorDb;
    else delete require.cache[dbPath];
  }
});

test('exceptions restore environment, imports, transport hooks and filesystem hooks', async () => {
  const before = snapshot();
  const keys = Object.keys(require.cache);
  await assert.rejects(withOfflineSandbox(async () => {
    require('../warmRoutingAgent');
    process.env.WARM_ROUTING_ENABLED = 'true';
    throw new Error('intentional observation failure');
  }), /intentional observation failure/);
  assertRestored(before);
  assert.deepEqual(Object.keys(require.cache), keys);
  // Failure must not leave the exclusive-use guard engaged.
  assert.equal(await withOfflineSandbox(async () => 42), 42);
});

test('transport denial is fail-closed even when callers swallow each error', async () => {
  const before = snapshot();
  const probes = [
    () => http.request('http://example.invalid'),
    () => require('node:https').get('https://example.invalid'),
    () => net.Socket.prototype.connect(),
    () => require('node:tls').connect(),
    () => require('node:http2').connect(),
    () => require('node:dgram').Socket.prototype.send(),
    () => dns.lookup('example.invalid'),
    () => dns.promises.resolve4('example.invalid'),
    () => new dns.Resolver().resolveMx('example.invalid'),
    () => new dns.promises.Resolver().resolveTxt('example.invalid'),
    () => globalThis.fetch('https://example.invalid'),
    () => new globalThis.WebSocket('wss://example.invalid'),
    () => require('node:child_process').spawn('curl', []),
    () => new (require('node:worker_threads').Worker)(''),
  ];
  await assert.rejects(withOfflineSandbox(async state => {
    for (const probe of probes) assert.throws(probe, /prohibited I\/O/);
    assert.equal(state.blocked.length, probes.length);
  }), /blocked unexpected I\/O/);
  assertRestored(before);
});

test('unexpected DB/provider calls, scheduler creation, data reads and writes are denied', async () => {
  const before = snapshot();
  await assert.rejects(withOfflineSandbox(async state => {
    const database = require('../db');
    await assert.rejects(database.query('SELECT * FROM clients'), /prohibited I\/O/);
    assert.throws(() => new (require('pg').Pool)(), /prohibited I\/O/);
    assert.throws(() => require('axios').post('https://example.invalid'), /prohibited I\/O/);
    assert.throws(() => new (require('@anthropic-ai/sdk'))(), /prohibited I\/O/);
    assert.throws(() => setInterval(() => {}, 1), /prohibited I\/O/);
    assert.throws(() => require('node:timers').setInterval(() => {}, 1), /prohibited I\/O/);
    assert.throws(() => fs.readFileSync(path.resolve(__dirname, '../.env')), /prohibited I\/O/);
    assert.throws(() => fs.readFileSync(path.resolve(__dirname, '../gmail_credentials.json')), /prohibited I\/O/);
    assert.throws(() => fs.writeFileSync('/tmp/audit062-should-not-exist', 'no'), /prohibited I\/O/);
    assert.equal(state.queries.length, 0);
    assert.deepEqual(require('dotenv').config(), { parsed: {} });
  }), /blocked unexpected I\/O/);
  assertRestored(before);
});

test('overlapping sandbox calls are rejected without disturbing the owner', async () => {
  await withOfflineSandbox(async () => {
    await assert.rejects(withOfflineSandbox(async () => {}), /exclusive process use/);
    assert.equal(process.env.NODE_ENV, 'test');
  });
});

test('CLI requires an explicit output path and importing it never runs the audit', () => {
  for (const args of [[], ['--output'], ['--output', '--bad']]) {
    const result = spawnSync(process.execPath, [script, ...args], {
      env: { NODE_ENV: 'test' }, encoding: 'utf8', timeout: 10000,
    });
    assert.equal(result.status, 1);
    assert.match(result.stderr, /Usage:/);
  }
  const imported = spawnSync(process.execPath, ['-e', `require(${JSON.stringify(script)})`], {
    env: { NODE_ENV: 'test' }, encoding: 'utf8', timeout: 10000,
  });
  assert.equal(imported.status, 0);
  assert.equal(imported.stdout + imported.stderr, '');
});

test('explicit report creation is owner-only and never overwrites existing files', async () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'audit062-test-'));
  const output = path.join(directory, 'report.json');
  const before = snapshot();
  try {
    assert.equal(await main(['--output', output]), output);
    const original = fs.readFileSync(output, 'utf8');
    assert.equal(JSON.parse(original).mode, 'offline-fixtures');
    assert.equal(fs.statSync(output).mode & 0o777, 0o600);
    await assert.rejects(main(['--output', output]), { code: 'EEXIST' });
    assert.equal(fs.readFileSync(output, 'utf8'), original);
    const link = path.join(directory, 'link.json');
    fs.symlinkSync(output, link);
    await assert.rejects(main(['--output', link]), { code: 'EEXIST' });
    assertRestored(before);
  } finally {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});
