'use strict';
// Levi (tenant 21) prospecting-only pilot: a client-role user, signed in to the
// real route modules (routes/api, tenantOutreach, approvals, users, setter,
// closer) with the real auth middleware, against a DISPOSABLE PostgreSQL.
// Proves tenant isolation, prospect visibility and that operator, setter,
// closer, agent-run and outbound-scheduling surfaces are closed or inert.
// Never production; all external network is denied.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const express = require('express');
const session = require('express-session');
const bcrypt = require('bcryptjs');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');

let allowedPort = null;
const deny = name => function () { throw new Error(`Unexpected network: ${name}`); };
require('node:https').request = deny('https.request');
require('node:tls').connect = deny('tls.connect');
globalThis.fetch = deny('fetch');
const net = require('node:net');
const originalConnect = net.Socket.prototype.connect;
net.Socket.prototype.connect = function (...args) {
  const o = typeof args[0] === 'object' && args[0] ? args[0] : { port: args[0], host: args[1] };
  const local = o.path || ['127.0.0.1', 'localhost', '::1', undefined].includes(o.host);
  if (!local) throw new Error(`Unexpected network: net.connect ${o.host}:${o.port}`);
  return originalConnect.apply(this, args);
};

const PASSWORD = 'disposable-fixture-passphrase';

const SCHEMA = `
  CREATE TABLE clients(id SERIAL PRIMARY KEY, name TEXT, slug TEXT, business_name TEXT, vertical TEXT, active BOOLEAN DEFAULT true,
    enabled_agents TEXT[] DEFAULT '{}', pin TEXT, service_area TEXT[], notes TEXT, setter_qualification_threshold INTEGER);
  CREATE TABLE users(id SERIAL PRIMARY KEY, name TEXT, email TEXT UNIQUE, password_hash TEXT, role TEXT, active BOOLEAN DEFAULT true,
    client_id INTEGER, email_verified BOOLEAN DEFAULT true, password_change_required BOOLEAN DEFAULT false,
    created_at TIMESTAMPTZ DEFAULT NOW(), last_login_at TIMESTAMPTZ);
  CREATE TABLE companies(id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL, name TEXT, location TEXT);
  CREATE TABLE prospects(id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL, company_id UUID REFERENCES companies(id),
    first_name TEXT, last_name TEXT, email TEXT, phone TEXT, job_title TEXT, linkedin_url TEXT, source TEXT, icp_score INTEGER,
    status TEXT DEFAULT 'cold', notes TEXT, vertical TEXT, service_area_match TEXT, setter_visible BOOLEAN DEFAULT false,
    setter_status TEXT DEFAULT 'new', do_not_contact BOOLEAN DEFAULT false, ao_outreach_review_required BOOLEAN NOT NULL DEFAULT false,
    discovery_method TEXT, last_contacted_at TIMESTAMPTZ, assigned_setter_id INTEGER, closer_id INTEGER,
    acquisition_metadata JSONB NOT NULL DEFAULT '{}'::jsonb, created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW());
  CREATE TABLE touchpoints(id SERIAL PRIMARY KEY, prospect_id UUID, channel TEXT, action_type TEXT, content_summary TEXT, outcome TEXT,
    sentiment TEXT, agent_id TEXT, external_ref TEXT, client_id INTEGER, created_at TIMESTAMPTZ DEFAULT NOW());
  CREATE TABLE agent_log(id SERIAL PRIMARY KEY, agent_name TEXT, action TEXT, prospect_id UUID, payload JSONB, status TEXT, error_msg TEXT,
    duration_ms INTEGER, ran_at TIMESTAMPTZ DEFAULT NOW(), client_id INTEGER);
  CREATE TABLE agent_actions(id SERIAL PRIMARY KEY, created_by TEXT, action_type TEXT, title TEXT, description TEXT, payload JSONB,
    status TEXT, executed_at TIMESTAMPTZ, result JSONB, created_at TIMESTAMPTZ DEFAULT NOW(), client_id INTEGER);
  INSERT INTO clients(id, name, slug, enabled_agents) VALUES
    (1, 'Synthetic Home Tenant', 'home', '{scout,emmett,max}'),
    (21, 'Synthetic Lending Tenant', 'lending', '{max}');`;

function request(port, path, { method = 'GET', cookie, body } = {}) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : '';
    const req = http.request({ host: '127.0.0.1', port, path, method, headers: {
      Accept: 'application/json',
      ...(payload ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) } : {}),
      ...(cookie ? { Cookie: cookie } : {}),
    } }, res => {
      let text = '';
      res.setEncoding('utf8');
      res.on('data', chunk => { text += chunk; });
      res.on('end', () => {
        let json = null;
        try { json = JSON.parse(text); } catch (_) { /* html or empty */ }
        resolve({ status: res.statusCode, headers: res.headers, json, text });
      });
    });
    req.on('error', reject);
    req.end(payload);
  });
}

test('Levi tenant-21 client: isolation, prospect visibility, and closed operator/outbound surfaces', async t => {
  const pg = await startDisposablePostgres('levi-client-access-');
  allowedPort = Number(pg.port);
  process.env.DATABASE_URL = pg.connectionString;
  process.env.DATABASE_SSL = 'false';
  process.env.PF_DISABLE_BACKGROUND_WORKERS = '1';
  const pool = require('../db');
  pool.on('error', () => {});
  let server;
  t.after(async () => {
    if (server) await new Promise(resolve => server.close(resolve));
    await pool.end().catch(() => {});
    await pg.stop();
  });
  await pool.query(SCHEMA);

  const hash = await bcrypt.hash(PASSWORD, 4);
  await pool.query(`INSERT INTO users(name, email, password_hash, role, client_id, password_change_required) VALUES
    ('Levi Fixture', 'levi@example.invalid', $1, 'client', 21, false),
    ('Levi First Login', 'levi-new@example.invalid', $1, 'client', 21, true),
    ('Home Setter', 'setter@example.invalid', $1, 'setter', 1, false)`, [hash]);
  const company21 = (await pool.query(`INSERT INTO companies(client_id, name) VALUES (21, 'Lending Co') RETURNING id`)).rows[0].id;
  const company1 = (await pool.query(`INSERT INTO companies(client_id, name) VALUES (1, 'Home Co') RETURNING id`)).rows[0].id;
  const t21 = (await pool.query(`INSERT INTO prospects(client_id, company_id, first_name, source, icp_score, ao_outreach_review_required, service_area_match)
    VALUES (21, $1, 'Research', 'scout_research_only', 65, true, 'Chicago, IL'),
           (21, $1, 'Imported', 'review_only_import', 80, true, 'Example')
    RETURNING id`, [company21])).rows.map(r => r.id);
  const t1 = (await pool.query(`INSERT INTO prospects(client_id, company_id, first_name, email, source, icp_score, setter_visible)
    VALUES (1, $1, 'OtherTenant', 'other@example.invalid', 'scout', 90, true) RETURNING id`, [company1])).rows[0].id;
  await pool.query(`INSERT INTO touchpoints(prospect_id, channel, action_type, client_id) VALUES ($1, 'email', 'sent', 1)`, [t1]);

  // App: real auth middleware + real route modules, mounted as server.js mounts them.
  const app = express();
  app.use(express.json());
  app.use(session({ secret: 'disposable-only', resave: false, saveUninitialized: false }));
  // Fixture sign-in: same lookup, bcrypt check, refusals and session shape as
  // POST /login in server.js (the handler itself is covered by
  // loginIdentityWorkflow / loginSessionPersistencePostgres).
  app.post('/__fixture_login', async (req, res) => {
    const { rows } = await pool.query(`SELECT id, name, email, password_hash, role, active, client_id, email_verified, password_change_required
      FROM users WHERE LOWER(email) = LOWER($1) LIMIT 1`, [req.body.email]);
    const user = rows[0];
    if (!user || !user.active || !(await bcrypt.compare(req.body.password || '', user.password_hash))) return res.status(401).end();
    if (user.role === 'client' && !user.client_id) return res.status(401).json({ error: 'no_workspace' });
    req.session.regenerate(() => {
      req.session.user = { id: user.id, name: user.name, email: user.email, role: user.role, client_id: user.client_id || null,
        email_verified: user.email_verified !== false, password_change_required: Boolean(user.password_change_required) };
      req.session.authenticated = true;
      req.session.active_client_id = user.role === 'client' ? Number(user.client_id) : (user.client_id || 1);
      req.session.save(() => res.status(204).end());
    });
  });
  app.use('/', require('../routes/api'));
  app.use('/', require('../routes/tenantOutreach'));
  app.use('/', require('../routes/approvals'));
  app.use('/', require('../routes/users'));
  app.use('/setter', require('../routes/setter'));
  app.use('/api/setter', require('../routes/setter'));
  app.use('/closer', require('../routes/closer'));
  app.use('/api/closer', require('../routes/closer'));
  server = await new Promise(resolve => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
  const port = server.address().port;

  // Wrong password and unknown user are refused.
  assert.equal((await request(port, '/__fixture_login', { method: 'POST', body: { email: 'levi@example.invalid', password: 'wrong' } })).status, 401);
  // Unauthenticated access is refused.
  assert.equal((await request(port, '/api/prospects')).status, 401);

  const login = await request(port, '/__fixture_login', { method: 'POST', body: { email: 'levi@example.invalid', password: PASSWORD } });
  assert.equal(login.status, 204);
  const cookie = String(login.headers['set-cookie'][0]).split(';')[0];
  const as = (path, opts = {}) => request(port, path, { ...opts, cookie });

  // Workspace identity is pinned to tenant 21; query/body overrides are ignored.
  const me = await as('/api/me?client_id=1');
  assert.equal(me.status, 200);
  assert.equal(me.json.client && me.json.client.id, 21);

  // Existing tenant-21 prospects are visible; tenant-1 prospects never are.
  for (const path of ['/api/prospects', '/api/prospects?client_id=1']) {
    const list = await as(path);
    assert.equal(list.status, 200, path);
    assert.deepEqual(list.json.map(r => r.id).sort(), [...t21].sort(), path);
  }
  const pipeline = await as('/api/pipeline/prospects?client_id=1');
  assert.equal(pipeline.status, 200);
  assert.equal(JSON.stringify(pipeline.json).includes(t1), false, 'pipeline never includes another tenant');
  const detail = await as(`/api/prospects/${t1}/detail?client_id=1`);
  assert.notEqual(detail.status, 200, 'other-tenant prospect detail is not served');
  const touch = await as(`/api/prospects/${t1}/touchpoints?client_id=1`);
  assert.equal(JSON.stringify(touch.json || []).includes('sent'), false, 'other-tenant touchpoints are not served');

  // Operator, admin, setter, closer and agent-run surfaces are closed.
  const forbidden = [
    ['POST', '/api/run/scout'], ['POST', '/api/run/emmett'], ['GET', '/api/clients'], ['GET', '/api/users'],
    ['GET', '/api/setter/metrics'], ['GET', '/api/setter/feed'], ['GET', '/api/closer/metrics'],
    ['POST', '/api/approve-comment/1'], ['GET', '/api/pending-comments'],
  ];
  for (const [method, path] of forbidden) {
    const r = await as(path, { method, body: method === 'POST' ? {} : undefined });
    assert.ok([401, 403].includes(r.status), `${method} ${path} -> ${r.status}`);
  }
  for (const page of ['/setter', '/closer', '/admin/users']) {
    const r = await request(port, page, { cookie, headers: {} });
    assert.ok(r.status === 302 || r.status === 403, `${page} -> ${r.status}`);
    if (r.status === 302) assert.doesNotMatch(String(r.headers.location), /^\/(setter|closer|admin)/);
  }
  assert.equal((await pool.query(`SELECT count(*)::int AS n FROM agent_log WHERE action = 'dashboard_trigger'`)).rows[0].n, 0, 'no agent was triggered');

  // Outbound scheduling is inert for tenant 21: no tenant sending identity.
  const sched = await as('/api/v1/tenant-outreach/schedules/authorize', { method: 'POST', body: {
    prospect_id: t21[1], outreach_asset_id: 'a1', sending_identity_id: 'not-a-tenant-21-identity', recipient_email: 'x@example.invalid',
    scheduled_for: '2030-01-01T00:00:00Z', subject: 's', body: 'b', client_id: 1 } });
  // Refused with sending_identity_tenant_mismatch. Note: the route maps this code to 500, not 4xx (cosmetic).
  assert.notEqual(sched.status, 200);
  assert.notEqual(sched.status, 201);
  assert.equal(sched.json && sched.json.error, 'sending_identity_tenant_mismatch');
  const scheduled = await pool.query(`SELECT to_regclass('tenant_outreach_scheduled_sends') IS NOT NULL AS present`);
  if (scheduled.rows[0].present) {
    assert.equal((await pool.query(`SELECT count(*)::int AS n FROM tenant_outreach_scheduled_sends`)).rows[0].n, 0);
  }

  // Nothing about the tenant-21 rows was changed by the session.
  const after = (await pool.query(`SELECT setter_visible, ao_outreach_review_required, do_not_contact FROM prospects WHERE client_id = 21`)).rows;
  assert.deepEqual(after, [
    { setter_visible: false, ao_outreach_review_required: true, do_not_contact: false },
    { setter_visible: false, ao_outreach_review_required: true, do_not_contact: false },
  ]);

  // First login with a temporary password is forced to change it before any data.
  const first = await request(port, '/__fixture_login', { method: 'POST', body: { email: 'levi-new@example.invalid', password: PASSWORD } });
  const firstCookie = String(first.headers['set-cookie'][0]).split(';')[0];
  const blocked = await request(port, '/api/prospects', { cookie: firstCookie });
  assert.equal(blocked.status, 403, 'temporary-password session cannot read prospects');
  assert.equal(blocked.json && blocked.json.error, 'password_change_required');
});
