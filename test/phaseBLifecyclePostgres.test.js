'use strict';

// Phase B HTTP integration coverage over disposable PostgreSQL.
// Validates the structured lifecycle reasons introduced in Phase B §5 without
// collapsing non-terminal outcomes into permanent Dead semantics:
//   - wrong_number / disconnected → data_remediation (alive, phone cleared)
//   - answered_not_interested     → nurture (alive, long-dated callback)
//   - do_not_call                 → terminal_suppression (dead + global DNC)
//   - nurture vs permanent Dead vs Do Not Call are three distinct states
//   - Pipeline stage moves and Calls dispositions read/write the same
//     canonical lifecycle stream (parity)
//
// Gated on MAX_SMOKE_DISPOSABLE_PG=true (repo convention for disposable PG).

const assert = require('node:assert/strict');
const path = require('node:path');
const test = require('node:test');

const root = path.join(__dirname, '..');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');

const BASE_SCHEMA = `
  CREATE EXTENSION IF NOT EXISTS pgcrypto;
  CREATE TABLE clients (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true
  );
  CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL DEFAULT 'x',
    role TEXT NOT NULL,
    client_id INTEGER,
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_login_at TIMESTAMPTZ
  );
  CREATE TABLE companies (
    id SERIAL PRIMARY KEY,
    client_id INTEGER NOT NULL,
    name TEXT,
    location TEXT,
    website TEXT
  );
  CREATE TABLE prospects (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id INTEGER NOT NULL REFERENCES clients(id),
    company_id INTEGER,
    first_name TEXT, last_name TEXT, email TEXT, phone TEXT, job_title TEXT,
    icp_score INTEGER,
    status TEXT DEFAULT 'cold',
    source TEXT,
    vertical TEXT,
    service_area_match TEXT,
    city TEXT,
    do_not_contact BOOLEAN DEFAULT false,
    is_synthetic BOOLEAN NOT NULL DEFAULT false,
    synthetic_label TEXT,
    callback_completed_at TIMESTAMPTZ,
    notes TEXT,
    callback_at TIMESTAMPTZ,
    is_hot BOOLEAN DEFAULT false,
    setter_status TEXT DEFAULT 'new',
    setter_visible BOOLEAN DEFAULT false,
    setter_updated_at TIMESTAMPTZ DEFAULT NOW(),
    enrichment_attempted BOOLEAN DEFAULT false,
    assigned_setter_id INTEGER,
    last_contacted_at TIMESTAMPTZ,
    mrr_value NUMERIC,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
  );
  CREATE TABLE touchpoints (
    id SERIAL PRIMARY KEY,
    prospect_id UUID,
    channel TEXT, action_type TEXT, content_summary TEXT,
    outcome TEXT, sentiment TEXT, agent_id TEXT, external_ref TEXT,
    client_id INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
  );
  CREATE TABLE agent_actions (
    id SERIAL PRIMARY KEY,
    created_by TEXT, action_type TEXT, title TEXT, description TEXT,
    payload JSONB, status TEXT, executed_at TIMESTAMPTZ, result TEXT,
    client_id INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
  );
  CREATE TABLE agent_log (id SERIAL PRIMARY KEY, agent_name TEXT, action TEXT, created_at TIMESTAMPTZ DEFAULT NOW());
  CREATE TABLE pending_comments (id SERIAL PRIMARY KEY, status TEXT, created_at TIMESTAMPTZ DEFAULT NOW());
  CREATE TABLE post_analytics (id SERIAL PRIMARY KEY, platform TEXT, created_at TIMESTAMPTZ DEFAULT NOW());
  CREATE TABLE activity_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    lead_id UUID REFERENCES prospects(id) ON DELETE CASCADE,
    action_type TEXT NOT NULL,
    notes TEXT,
    setter_id TEXT,
    client_id INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
  );
  INSERT INTO clients (id, name) VALUES (1, 'Pulseforge');
  SELECT setval(pg_get_serial_sequence('clients', 'id'), 20);
  INSERT INTO users (name, email, role, client_id) VALUES
    ('William Setter', 'william@pulseforge.local', 'setter', 1),
    ('Levi Closer', 'levi@pulseforge.local', 'closer', 1),
    ('Ada Admin', 'admin@pulseforge.local', 'admin', NULL);
`;

function resetAppModules(connectionString) {
  process.env.DATABASE_URL = connectionString;
  process.env.DATABASE_SSL = 'false';
  delete process.env.BREVO_API_KEY;
  delete process.env.LEVI_CLOSER_ID;
  for (const key of Object.keys(require.cache)) {
    if (!key.startsWith(root)) continue;
    if (key.includes(`${path.sep}node_modules${path.sep}`)) continue;
    if (key.startsWith(path.join(root, 'test'))) continue;
    delete require.cache[key];
  }
}

test('Phase B structured lifecycle reasons over PostgreSQL', { timeout: 180000 }, async t => {
  if (process.env.MAX_SMOKE_DISPOSABLE_PG !== 'true') {
    t.skip('set MAX_SMOKE_DISPOSABLE_PG=true with PostgreSQL binaries for Phase B integration coverage');
    return;
  }

  let pg;
  try {
    pg = await startDisposablePostgres('phase-b-pg-');
  } catch (error) {
    if (/shmget|shared memory|Operation not permitted/i.test(error.message)) {
      t.skip(`disposable postgres unavailable in this environment: ${error.message.split('\n')[0]}`);
      return;
    }
    throw error;
  }

  resetAppModules(pg.connectionString);
  const pool = require('../db');
  await pool.query(BASE_SCHEMA);

  const express = require('express');
  const workspaceRouter = require('../routes/workspace');
  const setterRouter = require('../routes/setter');

  const sessionState = { session: {} };
  const app = express();
  app.use(express.json());
  app.use((req, _res, next) => { req.session = sessionState.session; next(); });
  app.use('/', workspaceRouter);
  app.use('/setter', setterRouter);
  const server = app.listen(0, '127.0.0.1');
  await new Promise(resolve => server.once('listening', resolve));
  const base = `http://127.0.0.1:${server.address().port}`;

  t.after(async () => {
    server.close();
    await pool.end().catch(() => {});
    await pg.stop();
  });

  const users = {};
  for (const row of (await pool.query('SELECT id, name, email, role, client_id FROM users')).rows) {
    users[row.role] = row;
  }

  function actAs(user, activeClientId = null) {
    sessionState.session = user ? { user, active_client_id: activeClientId } : {};
  }

  async function call(method, url, body) {
    const res = await fetch(`${base}${url}`, {
      method,
      headers: body ? { 'Content-Type': 'application/json' } : {},
      body: body ? JSON.stringify(body) : undefined,
    });
    let json = null;
    try { json = await res.json(); } catch { /* non-JSON */ }
    return { status: res.status, body: json };
  }

  actAs(users.setter);
  let warm;
  for (let attempt = 0; attempt < 15; attempt += 1) {
    warm = await call('GET', '/setter/api/leads');
    if (warm.status === 200) break;
    await new Promise(resolve => setTimeout(resolve, 400));
  }
  assert.equal(warm.status, 200, JSON.stringify(warm.body));

  const company = await pool.query(`
    INSERT INTO companies (client_id, name, location, website)
    VALUES (1, 'Queen City Books', 'Manchester, NH', 'https://queencitybooks.example')
    RETURNING id
  `);
  const seedProspect = async (overrides = {}) => {
    const defaults = {
      client_id: 1, company_id: company.rows[0].id,
      first_name: 'Jo', last_name: 'Owner', email: 'jo@qcb.example',
      phone: '603-555-4321', job_title: 'Owner', icp_score: 80,
      status: 'cold', source: 'scout', vertical: 'cleaning',
      service_area_match: 'Manchester NH', setter_status: 'new', setter_visible: true,
    };
    const row = { ...defaults, ...overrides };
    const keys = Object.keys(row);
    const result = await pool.query(`
      INSERT INTO prospects (${keys.join(', ')})
      VALUES (${keys.map((_, i) => `$${i + 1}`).join(', ')})
      RETURNING id
    `, keys.map(k => row[k]));
    return result.rows[0].id;
  };

  const remediation = await seedProspect({ email: 'a@qcb.example' });
  const nurture = await seedProspect({ email: 'b@qcb.example', is_hot: true, status: 'warm' });
  const dnc = await seedProspect({ email: 'c@qcb.example' });
  const permanentDead = await seedProspect({ email: 'd@qcb.example' });

  // ── 1. wrong_number → data remediation, prospect survives ─────────────
  await t.test('wrong number clears the phone but keeps the prospect alive for repair', async () => {
    actAs(users.setter);
    const { status, body } = await call('POST', `/setter/api/leads/${remediation}/call-disposition`, {
      disposition: 'wrong_number',
      notes: 'Number belongs to a florist now',
      idempotency_key: 'phase-b-wrong-number-1',
    });
    assert.equal(status, 200, JSON.stringify(body));

    const row = (await pool.query(
      'SELECT status, setter_status, phone, do_not_contact FROM prospects WHERE id = $1', [remediation])).rows[0];
    assert.equal(row.phone, null, 'phone must be cleared for remediation');
    assert.equal(row.setter_status, 'follow_up', 'prospect stays alive on follow_up');
    assert.equal(row.status, 'cold', 'legacy status preserved — NOT dead');
    assert.equal(row.do_not_contact, false, 'remediation is not suppression');

    const event = (await pool.query(
      `SELECT to_stage, lifecycle_reason FROM prospect_lifecycle_events
       WHERE prospect_id = $1 ORDER BY created_at DESC LIMIT 1`, [remediation])).rows[0];
    assert.equal(event.to_stage, 'follow_up');
    assert.equal(event.lifecycle_reason, 'data_remediation');

    // The workspace surfaces the remediation next action; the queue payload
    // carries the same lifecycle reason (Pipeline/Calls parity read model).
    const ws = await call('GET', `/api/prospects/${remediation}/workspace`);
    assert.equal(ws.body.lifecycle.lifecycleReason, 'data_remediation');
    assert.equal(ws.body.nextAction.type, 'find_phone');

    const leads = await call('GET', '/setter/api/leads?all_statuses=true');
    const lead = leads.body.find(l => l.id === remediation);
    assert.ok(lead, 'remediation prospect must remain in the queue payload');
    assert.equal(lead.lifecycle_reason, 'data_remediation');
    assert.equal(lead.phone, null);
  });

  // ── 2. answered_not_interested → nurture, distinct from Dead ──────────
  await t.test('not-interested nurtures with a long-dated callback instead of dying', async () => {
    actAs(users.setter);
    const { status, body } = await call('POST', `/setter/api/leads/${nurture}/call-disposition`, {
      disposition: 'answered_not_interested',
      notes: 'Happy with current provider, revisit in a quarter',
      structured_notes: { summary: 'Spoke with Jo — not interested today', reason: 'Happy with incumbent' },
      idempotency_key: 'phase-b-nurture-1',
    });
    assert.equal(status, 200, JSON.stringify(body));

    const row = (await pool.query(
      'SELECT status, setter_status, is_hot, callback_at, do_not_contact FROM prospects WHERE id = $1', [nurture])).rows[0];
    assert.equal(row.setter_status, 'follow_up', 'nurture stays alive on follow_up');
    assert.equal(row.status, 'cold', 'nurture downgrades to cold — NOT dead');
    assert.equal(row.is_hot, false, 'hot flag cleared on nurture');
    assert.equal(row.do_not_contact, false, 'nurture is not suppression');
    assert.ok(row.callback_at, 'nurture schedules a re-check callback');
    const days = (new Date(row.callback_at).getTime() - Date.now()) / 86400000;
    assert.ok(days > 60 && days < 120, `nurture callback should be ~90 days out, got ${days.toFixed(1)}`);

    const event = (await pool.query(
      `SELECT to_stage, lifecycle_reason FROM prospect_lifecycle_events
       WHERE prospect_id = $1 ORDER BY created_at DESC LIMIT 1`, [nurture])).rows[0];
    assert.equal(event.to_stage, 'follow_up');
    assert.equal(event.lifecycle_reason, 'nurture');

    const ws = await call('GET', `/api/prospects/${nurture}/workspace`);
    assert.equal(ws.body.lifecycle.lifecycleReason, 'nurture');
    assert.equal(ws.body.nextAction.type, 'nurture_callback');
  });

  // ── 3. do_not_call → terminal suppression ──────────────────────────────
  await t.test('do_not_call suppresses globally and refuses callbacks', async () => {
    actAs(users.setter);
    const rejected = await call('POST', `/setter/api/leads/${dnc}/call-disposition`, {
      disposition: 'do_not_call',
      notes: 'Asked to never be called again',
      structured_notes: { summary: 'Owner asked to be removed', reason: '"Take me off your list"' },
      callback_at: new Date(Date.now() + 86400000).toISOString(),
      idempotency_key: 'phase-b-dnc-rejected',
    });
    assert.equal(rejected.status, 400, 'a DNC outcome cannot schedule a callback');

    const missingReason = await call('POST', `/setter/api/leads/${dnc}/call-disposition`, {
      disposition: 'do_not_call',
      notes: 'Asked to never be called again',
      structured_notes: { summary: 'Owner asked to be removed' },
      idempotency_key: 'phase-b-dnc-noreason',
    });
    assert.equal(missingReason.status, 400, 'DNC requires the verbatim request as the reason');

    const { status, body } = await call('POST', `/setter/api/leads/${dnc}/call-disposition`, {
      disposition: 'do_not_call',
      notes: 'Asked to never be called again',
      structured_notes: { summary: 'Owner asked to be removed', reason: '"Take me off your list"' },
      idempotency_key: 'phase-b-dnc-1',
    });
    assert.equal(status, 200, JSON.stringify(body));

    const row = (await pool.query(
      'SELECT status, setter_status, do_not_contact, callback_at FROM prospects WHERE id = $1', [dnc])).rows[0];
    assert.equal(row.status, 'dead');
    assert.equal(row.setter_status, 'dead');
    assert.equal(row.do_not_contact, true, 'terminal suppression sets global DNC');
    assert.equal(row.callback_at, null, 'no callback survives suppression');

    const event = (await pool.query(
      `SELECT to_stage, lifecycle_reason FROM prospect_lifecycle_events
       WHERE prospect_id = $1 ORDER BY created_at DESC LIMIT 1`, [dnc])).rows[0];
    assert.equal(event.to_stage, 'dead');
    assert.equal(event.lifecycle_reason, 'terminal_suppression');

    // Suppressed prospects fall out of the callable setter queue.
    const leads = await call('GET', '/setter/api/leads?all_statuses=true');
    assert.ok(!leads.body.some(l => l.id === dnc), 'DNC prospect must not surface in the setter queue');
  });

  // ── 4. Permanent Dead (disqualified) is distinct from nurture and DNC ─
  await t.test('disqualified is permanent Dead without global suppression', async () => {
    actAs(users.setter);
    const { status, body } = await call('POST', `/setter/api/leads/${permanentDead}/call-disposition`, {
      disposition: 'disqualified',
      notes: 'Business is closing next month',
      structured_notes: { summary: 'Closing down', reason: 'Business shutting down' },
      idempotency_key: 'phase-b-dead-1',
    });
    assert.equal(status, 200, JSON.stringify(body));

    const row = (await pool.query(
      'SELECT status, setter_status, do_not_contact FROM prospects WHERE id = $1', [permanentDead])).rows[0];
    assert.equal(row.status, 'dead');
    assert.equal(row.setter_status, 'dead');
    assert.equal(row.do_not_contact, false, 'plain disqualification does NOT set global DNC');

    const event = (await pool.query(
      `SELECT to_stage, lifecycle_reason FROM prospect_lifecycle_events
       WHERE prospect_id = $1 ORDER BY created_at DESC LIMIT 1`, [permanentDead])).rows[0];
    assert.equal(event.to_stage, 'dead');
    assert.equal(event.lifecycle_reason, null, 'permanent dead carries no structured reason');

    // Three distinct terminal/parked states must not be conflated.
    const states = await pool.query(`
      SELECT id, status, setter_status, do_not_contact FROM prospects WHERE id = ANY($1::uuid[])
    `, [[nurture, dnc, permanentDead]]);
    const byId = Object.fromEntries(states.rows.map(r => [r.id, r]));
    assert.equal(byId[nurture].setter_status, 'follow_up');
    assert.equal(byId[dnc].do_not_contact, true);
    assert.equal(byId[permanentDead].do_not_contact, false);
  });

  // ── 5. Pipeline and Calls converge on the same lifecycle stream ───────
  await t.test('Pipeline stage move and Calls disposition share the canonical event stream', async () => {
    actAs(users.setter);
    // Pipeline move (nurture prospect → contacted) writes to the same table
    // with a different source, and the workspace reflects it immediately.
    const moved = await call('PATCH', `/setter/api/leads/${nurture}/status`, { status: 'contacted' });
    assert.equal(moved.status, 200, JSON.stringify(moved.body));

    const events = await pool.query(`
      SELECT to_stage, source FROM prospect_lifecycle_events
      WHERE prospect_id = $1 ORDER BY created_at
    `, [nurture]);
    const sources = events.rows.map(r => r.source);
    assert.ok(sources.includes('call_disposition'), 'disposition event recorded');
    assert.ok(sources.includes('setter_status_endpoint'), 'pipeline move recorded in the same stream');

    const ws = await call('GET', `/api/prospects/${nurture}/workspace`);
    assert.equal(ws.body.lifecycle.canonicalStage, 'contacted');
    const leads = await call('GET', '/setter/api/leads?all_statuses=true');
    const lead = leads.body.find(l => l.id === nurture);
    assert.equal(lead.status, 'contacted', 'queue and workspace report the same stage');
  });
});
