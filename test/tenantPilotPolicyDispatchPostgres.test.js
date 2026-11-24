'use strict';
// P0-SCOUT-LEVI-001 pilot policy, DB-backed boundaries (separate process so db.js binds to the disposable server).
// Proves every paid-provider, outbound and execution chokepoint refuses the
// pilot tenant BEFORE any provider or sender is reached (all HTTP is recorded
// and denied), that permitted prospecting still works, that AIM / enabled_agents
// / request parameters cannot lift the policy, and that other tenants are
// unchanged. Disposable PostgreSQL only.

// ---- network capture: every attempted provider/sender request is recorded and refused ----
const attempts = [];
let allowedPort = null;
const record = label => { attempts.push(label); throw Object.assign(new Error(`network blocked in test: ${label}`), { code: 'TEST_NETWORK_BLOCKED' }); };
for (const protocol of ['http', 'https']) {
  const mod = require(`node:${protocol}`);
  const original = mod.request;
  mod.request = function (...args) {
    const o = typeof args[0] === 'object' && args[0] && !(args[0] instanceof URL) ? args[0] : (args[0] instanceof URL ? args[0] : new URL(String(args[0])));
    const host = o.hostname || o.host;
    if (['127.0.0.1', 'localhost'].includes(host)) return original.apply(this, args);
    return record(`${protocol}:${host}`);
  };
  mod.get = function (...args) { return mod.request(...args); };
}
globalThis.fetch = async url => record(`fetch:${new URL(String(url)).hostname}`);
const axios = require('axios');
axios.Axios.prototype.request = async function (config) {
  const url = typeof config === 'string' ? config : (config && config.url) || '';
  let host = '';
  try { host = new URL(url).hostname; } catch (_) { host = url; }
  return record(`axios:${host}`);
};
const net = require('node:net');
const originalConnect = net.Socket.prototype.connect;
net.Socket.prototype.connect = function (...args) {
  const o = typeof args[0] === 'object' && args[0] ? args[0] : { port: args[0], host: args[1] };
  const local = o.path || ['127.0.0.1', 'localhost', '::1', undefined].includes(o.host);
  if (!local) return record(`net:${o.host}:${o.port}`);
  return originalConnect.apply(this, args);
};

const { test } = require('node:test');
const assert = require('node:assert/strict');
const policy = require('../utils/tenantPilotPolicy');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');

const DENIED = { code: 'tenant_pilot_policy_denied' };

test('agent dispatch, cron and Scout entry on disposable PostgreSQL: enabled_agents cannot lift the policy', async t => {
  const pg = await startDisposablePostgres('tenant-pilot-policy-');
  allowedPort = Number(pg.port);
  process.env.DATABASE_URL = pg.connectionString;
  process.env.DATABASE_SSL = 'false';
  process.env.CRON_SECRET = 'disposable-cron-secret';
  const pool = require('../db');
  pool.on('error', () => {});
  t.after(async () => { await pool.end().catch(() => {}); await pg.stop(); });
  await pool.query(`
    CREATE TABLE clients(id SERIAL PRIMARY KEY, name TEXT, slug TEXT, active BOOLEAN DEFAULT true, enabled_agents TEXT[]);
    CREATE TABLE agent_log(id SERIAL PRIMARY KEY, agent_name TEXT, action TEXT, prospect_id UUID, payload JSONB, status TEXT,
      error_msg TEXT, duration_ms INTEGER, ran_at TIMESTAMPTZ DEFAULT NOW(), client_id INTEGER);
    -- Tenant 21 deliberately has every agent enabled: the pilot policy must still deny.
    INSERT INTO clients(id, name, slug, enabled_agents) VALUES
      (1, 'Home', 'home', '{scout,emmett,max,sam}'),
      (21, 'Pilot', 'pilot', '{scout,emmett,max,sam,cal,tiered_enrichment,paige}');`);

  const { isAgentEnabledForClient } = require('../utils/agentDispatchPolicy');
  for (const agent of ['scout', 'emmett', 'sam', 'cal', 'tiered_enrichment', 'paige']) {
    const gate = await isAgentEnabledForClient(21, agent);
    assert.deepEqual([gate.allowed, gate.reason], [false, 'tenant_pilot_policy_denied'], agent);
  }
  assert.equal((await isAgentEnabledForClient(21, 'max')).allowed, true, 'Max briefing stays available');
  assert.equal((await isAgentEnabledForClient({ id: 21, enabled_agents: ['scout'] }, 'scout')).allowed, false, 'row form too');
  for (const agent of ['scout', 'emmett', 'max', 'sam']) assert.equal((await isAgentEnabledForClient(1, agent)).allowed, true, `tenant 1 ${agent}`);

  // Cron boundary: /cron/scout?client_id=21 is skipped; nothing runs.
  const express = require('express');
  const http = require('node:http');
  const app = express();
  app.use(express.json());
  app.use('/', require('../routes/cron'));
  const server = await new Promise(resolve => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
  t.after(() => new Promise(resolve => server.close(resolve)));
  const post = path => new Promise((resolve, reject) => {
    const req = http.request({ host: '127.0.0.1', port: server.address().port, path, method: 'POST', headers: { 'Content-Type': 'application/json' } }, res => {
      let text = ''; res.on('data', c => { text += c; }); res.on('end', () => resolve({ status: res.statusCode, json: JSON.parse(text || '{}') }));
    });
    req.on('error', reject); req.end('{}');
  });
  const before = attempts.length;
  for (const agent of ['scout', 'emmett', 'sam', 'tiered_enrichment']) {
    const r = await post(`/cron/${agent}?client_id=21&secret=disposable-cron-secret`);
    assert.equal(r.json.skipped, true, agent);
    assert.equal(r.json.reason, 'tenant_pilot_policy_denied', agent);
  }
  // The free research path keeps its own operator-only handler: it is never refused by the pilot policy.
  const leviCron = await new Promise((resolve, reject) => {
    const req = http.request({ host: '127.0.0.1', port: server.address().port, path: '/cron/scout_levi', method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: 'Bearer disposable-cron-secret' } }, res => {
      let text = ''; res.on('data', c => { text += c; }); res.on('end', () => resolve({ status: res.statusCode, json: JSON.parse(text || '{}') }));
    });
    req.on('error', reject); req.end('{}');
  });
  assert.notEqual(leviCron.json.reason, 'tenant_pilot_policy_denied');
  assert.equal(leviCron.json.reason, 'levi_research_disabled', 'flag-off here; the pilot policy is not what stops it');

  const blocked = await pool.query(`SELECT count(*)::int AS n FROM agent_log WHERE client_id = 21 AND action = 'dispatch_blocked'`);
  assert.equal(blocked.rows[0].n, 4, 'each refusal is audited');

  // Scout entry itself (a different route into the same code): refused before any provider.
  const leadgen = require('../leadgen');
  await assert.rejects(leadgen.run({ client_id: 21, industry: 'real estate', location: 'Chicago IL' }), DENIED);
  await assert.rejects(leadgen.run({ client_id: '21' }), DENIED);
  assert.equal(attempts.length, before, `no provider request was attempted: ${attempts.slice(before).join(', ')}`);
});
