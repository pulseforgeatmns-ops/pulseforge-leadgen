'use strict';

// Phase A2 HTTP integration coverage over disposable PostgreSQL:
//   - canonical ProspectWorkspace endpoint (shape, phone, permissions)
//   - tenant isolation (Tenant A cannot load Tenant B workspace)
//   - role access (viewer reads, cannot write lifecycle)
//   - lifecycle transitions (Pipeline move ≡ call disposition convergence)
//   - meeting_booked disposition → booked, idempotent, no duplicate handoff
//   - callback precedence (setter_callbacks over legacy prospects.callback_at)
//   - phone visibility in queue + workspace
//   - deterministic call preparation
//
// Gated on MAX_SMOKE_DISPOSABLE_PG=true (repo convention for disposable PG).

const assert = require('node:assert/strict');
const path = require('node:path');
const test = require('node:test');

const root = path.join(__dirname, '..');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');

const BASE_SCHEMA = `
  CREATE EXTENSION IF NOT EXISTS pgcrypto;
  -- Serial id: the client-architecture migration seeds rows without ids.
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
    -- Phase 3D columns: production received these via the controlled
    -- 2026-07-19 setter-pilot migration; the runtime reconciler refuses to
    -- soft-create them from a blank schema, so the fixture provides them.
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
  -- Stubs required by the client-architecture migration (CLIENT_SCOPED_TABLES).
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
  INSERT INTO clients (id, name) VALUES (1, 'Pulseforge'), (2, 'MSHI');
  SELECT setval(pg_get_serial_sequence('clients', 'id'), 20);
  INSERT INTO users (name, email, role, client_id) VALUES
    ('William Setter', 'william@pulseforge.local', 'setter', 1),
    ('Tenant Two Setter', 'setter2@pulseforge.local', 'setter', 2),
    ('Levi Closer', 'levi@pulseforge.local', 'closer', 1),
    ('Ada Admin', 'admin@pulseforge.local', 'admin', NULL),
    ('Vera Viewer', 'viewer@pulseforge.local', 'viewer', NULL);
`;

function freshRequire(name) {
  return require(name);
}

function resetAppModules(connectionString) {
  process.env.DATABASE_URL = connectionString;
  process.env.DATABASE_SSL = 'false';
  delete process.env.BREVO_API_KEY; // handoff email must be a no-op in tests
  delete process.env.LEVI_CLOSER_ID;
  for (const key of Object.keys(require.cache)) {
    if (!key.startsWith(root)) continue;
    if (key.includes(`${path.sep}node_modules${path.sep}`)) continue;
    if (key.startsWith(path.join(root, 'test'))) continue;
    delete require.cache[key];
  }
}

test('Phase A2 workspace + lifecycle convergence over PostgreSQL', { timeout: 180000 }, async t => {
  if (process.env.MAX_SMOKE_DISPOSABLE_PG !== 'true') {
    t.skip('set MAX_SMOKE_DISPOSABLE_PG=true with PostgreSQL binaries for Phase A2 HTTP integration coverage');
    return;
  }

  let pg;
  try {
    pg = await startDisposablePostgres('phase-a2-pg-');
  } catch (error) {
    if (/shmget|shared memory|Operation not permitted/i.test(error.message)) {
      t.skip(`disposable postgres unavailable in this environment: ${error.message.split('\n')[0]}`);
      return;
    }
    throw error;
  }

  resetAppModules(pg.connectionString);
  const pool = freshRequire('../db');
  await pool.query(BASE_SCHEMA);

  const express = freshRequire('express');
  const workspaceRouter = freshRequire('../routes/workspace');
  const setterRouter = freshRequire('../routes/setter');

  // Session-injection shim: same shape express-session provides, no cookies.
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
    users[row.role === 'setter' && row.client_id === 2 ? 'setter2' : row.role] = row;
  }

  function actAs(user, activeClientId = null) {
    sessionState.session = user
      ? { user, active_client_id: activeClientId }
      : {};
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

  // Warm the schema through the same path production uses: the setter router
  // runs its idempotent migrations (closer fields, dispositions, callbacks,
  // lifecycle tables) on first request. The router also fires a startup
  // migration at require time; retry briefly while the two concurrent
  // CREATE IF NOT EXISTS passes settle.
  actAs(users.setter);
  let warm;
  for (let attempt = 0; attempt < 15; attempt += 1) {
    warm = await call('GET', '/setter/api/leads');
    if (warm.status === 200) break;
    await new Promise(resolve => setTimeout(resolve, 400));
  }
  assert.equal(warm.status, 200, JSON.stringify(warm.body));

  // ── Seed prospects ─────────────────────────────────────────────────────
  const company = await pool.query(`
    INSERT INTO companies (client_id, name, location, website)
    VALUES (1, 'Granite State Cleaning', 'Manchester, NH', 'https://granitestateclean.example')
    RETURNING id
  `);
  const seedProspect = async (overrides = {}) => {
    const defaults = {
      client_id: 1, company_id: company.rows[0].id,
      first_name: 'Pat', last_name: 'Owner', email: 'pat@granite.example',
      phone: '603-555-1234', job_title: 'Owner', icp_score: 82,
      status: 'cold', source: 'scout', vertical: 'cleaning',
      service_area_match: 'Manchester NH', setter_status: 'new', setter_visible: true,
      notes: 'Granite State Cleaning — granitestateclean.example',
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

  const prospectA = await seedProspect();
  const prospectB = await seedProspect({ first_name: 'Blake', email: 'blake@granite.example', phone: '(603) 555-9876' });
  const prospectC = await seedProspect({ first_name: 'Casey', email: 'casey@granite.example', phone: '6035550000' });
  const tenantBProspect = await seedProspect({ client_id: 2, company_id: null, first_name: 'Morgan', email: 'morgan@mshi.example', phone: '304-555-2222' });

  // ── 1. Canonical workspace read model ─────────────────────────────────
  await t.test('workspace endpoint returns canonical read model with normalized phone', async () => {
    actAs(users.setter);
    const { status, body } = await call('GET', `/api/prospects/${prospectA}/workspace`);
    assert.equal(status, 200, JSON.stringify(body));
    assert.equal(body.prospect.id, prospectA);
    assert.equal(body.prospect.clientId, 1);
    assert.equal(body.prospect.companyName, 'Granite State Cleaning');
    assert.deepEqual(body.prospect.phone, {
      raw: '603-555-1234',
      normalized: '+16035551234',
      display: '(603) 555-1234',
      callable: true,
    });
    assert.equal(body.lifecycle.canonicalStage, 'new');
    assert.equal(body.lifecycle.legacyStatus, 'cold');
    assert.equal(body.lifecycle.setterStatus, 'new');
    assert.equal(body.callback.dueAt, null);
    assert.equal(body.nextAction.type, 'call');
    assert.equal(body.permissions.canCall, true);
    assert.equal(body.permissions.canChangeStage, true);
    assert.ok(body.knownFacts.some(f => f.id === 'phone' && f.value === '(603) 555-1234'));
    assert.ok(Array.isArray(body.history));
    assert.equal(body.opportunity.exists, false);
  });

  // ── 2. Tenant isolation ────────────────────────────────────────────────
  await t.test('tenant A user cannot load tenant B workspace', async () => {
    actAs(users.setter); // client 1
    const cross = await call('GET', `/api/prospects/${tenantBProspect}/workspace`);
    assert.equal(cross.status, 404);

    actAs(users.setter2); // client 2 can see its own
    const own = await call('GET', `/api/prospects/${tenantBProspect}/workspace`);
    assert.equal(own.status, 200);

    // Admin scoped to tenant 2 cannot read tenant 1 prospect either.
    actAs(users.admin, 2);
    const adminCross = await call('GET', `/api/prospects/${prospectA}/workspace`);
    assert.equal(adminCross.status, 404);
  });

  // ── 3. Role access ─────────────────────────────────────────────────────
  await t.test('viewer can read the workspace but cannot write lifecycle', async () => {
    actAs(users.viewer, 1);
    const read = await call('GET', `/api/prospects/${prospectA}/workspace`);
    assert.equal(read.status, 200);
    const write = await call('POST', `/api/prospects/${prospectA}/lifecycle`, { target_stage: 'contacted' });
    assert.equal(write.status, 403);
    actAs(null);
    const anon = await call('GET', `/api/prospects/${prospectA}/workspace`);
    assert.equal(anon.status, 401);
  });

  // ── 4. Canonical lifecycle transitions ─────────────────────────────────
  await t.test('workspace lifecycle endpoint writes both legacy fields and one event', async () => {
    actAs(users.admin, 1);
    const { status, body } = await call('POST', `/api/prospects/${prospectA}/lifecycle`, {
      target_stage: 'contacted',
      idempotency_key: 'a2-test-contact-1',
    });
    assert.equal(status, 200, JSON.stringify(body));
    assert.deepEqual(body.transition, { from: 'new', to: 'contacted' });
    assert.equal(body.workspace.lifecycle.canonicalStage, 'contacted');

    const row = (await pool.query('SELECT status, setter_status FROM prospects WHERE id = $1', [prospectA])).rows[0];
    assert.equal(row.setter_status, 'contacted');
    assert.equal(row.status, 'cold', 'stage moves must preserve legacy status (production behavior)');

    const events = await pool.query(
      `SELECT to_stage, source FROM prospect_lifecycle_events WHERE prospect_id = $1`, [prospectA]);
    assert.equal(events.rows.length, 1);
    assert.equal(events.rows[0].to_stage, 'contacted');

    // Idempotent replay: same key, no second event.
    const replay = await call('POST', `/api/prospects/${prospectA}/lifecycle`, {
      target_stage: 'contacted',
      idempotency_key: 'a2-test-contact-1',
    });
    assert.equal(replay.status, 200);
    assert.equal(replay.body.idempotent, true);
    const eventsAfter = await pool.query(
      `SELECT COUNT(*)::int AS count FROM prospect_lifecycle_events WHERE prospect_id = $1`, [prospectA]);
    assert.equal(eventsAfter.rows[0].count, 1);

    // Dead requires a reason.
    const deadNoReason = await call('POST', `/api/prospects/${prospectA}/lifecycle`, { target_stage: 'dead' });
    assert.equal(deadNoReason.status, 400);
  });

  // ── 5. Callback precedence and dual-store writes ──────────────────────
  await t.test('callback writes hit both stores; canonical store wins on conflict', async () => {
    actAs(users.setter);
    const dueAt = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
    const scheduled = await call('PATCH', `/setter/api/leads/${prospectA}/callback`, { callback_at: dueAt });
    assert.equal(scheduled.status, 200, JSON.stringify(scheduled.body));

    const legacy = (await pool.query('SELECT callback_at FROM prospects WHERE id = $1', [prospectA])).rows[0];
    assert.equal(new Date(legacy.callback_at).toISOString(), dueAt);
    const canonical = await pool.query(
      `SELECT due_at, status FROM setter_callbacks WHERE prospect_id = $1 AND status = 'pending'`, [prospectA]);
    assert.equal(canonical.rows.length, 1);
    assert.equal(new Date(canonical.rows[0].due_at).toISOString(), dueAt);

    let ws = await call('GET', `/api/prospects/${prospectA}/workspace`);
    assert.equal(ws.body.callback.source, 'setter_callbacks');
    assert.equal(ws.body.callback.conflict, false);
    assert.equal(ws.body.nextAction.type, 'callback');

    // Simulate legacy drift: the conflict must be surfaced, canonical wins.
    const drifted = new Date(Date.now() + 48 * 60 * 60 * 1000).toISOString();
    await pool.query('UPDATE prospects SET callback_at = $2 WHERE id = $1', [prospectA, drifted]);
    ws = await call('GET', `/api/prospects/${prospectA}/workspace`);
    assert.equal(new Date(ws.body.callback.dueAt).toISOString(), dueAt, 'canonical store wins');
    assert.equal(ws.body.callback.conflict, true, 'conflict must be surfaced, not silently overwritten');
    assert.equal(new Date(ws.body.callback.legacyDueAt).toISOString(), drifted);
  });

  // ── 6. meeting_booked disposition converges on the booked transition ──
  await t.test('meeting_booked disposition books, hands off once, and replays idempotently', async () => {
    actAs(users.setter);
    const payload = {
      disposition: 'meeting_booked',
      notes: 'Booked walkthrough with Blake',
      structured_notes: { summary: 'Booked a discovery call', next_step: 'Discovery call Tuesday 10am with Levi' },
      idempotency_key: 'a2-meeting-booked-1',
    };
    const first = await call('POST', `/setter/api/leads/${prospectB}/call-disposition`, payload);
    assert.equal(first.status, 200, JSON.stringify(first.body));
    assert.equal(first.body.lead.status, 'booked');
    assert.equal(first.body.handoff.assigned, true);
    assert.equal(first.body.handoff.closer_name, 'Levi Closer');

    const row = (await pool.query(
      'SELECT status, setter_status, booked_at, closer_id, closer_status FROM prospects WHERE id = $1', [prospectB])).rows[0];
    assert.equal(row.setter_status, 'booked');
    assert.ok(row.booked_at, 'booked_at must be stamped');
    assert.equal(row.closer_id, users.closer.id);
    assert.equal(row.closer_status, 'booked');

    const events = await pool.query(
      `SELECT to_stage, disposition, source FROM prospect_lifecycle_events WHERE prospect_id = $1`, [prospectB]);
    assert.equal(events.rows.length, 1);
    assert.deepEqual(events.rows[0], { to_stage: 'booked', disposition: 'meeting_booked', source: 'call_disposition' });

    const handoffs = await pool.query(
      `SELECT COUNT(*)::int AS count FROM agent_actions
       WHERE action_type = 'closer_handoff' AND payload->>'prospect_id' = $1::text`, [prospectB]);
    assert.equal(handoffs.rows[0].count, 1);

    // Idempotent replay: same key → no new disposition, no duplicate handoff.
    const replay = await call('POST', `/setter/api/leads/${prospectB}/call-disposition`, payload);
    assert.equal(replay.status, 200);
    assert.equal(replay.body.idempotent, true);
    const dispositionCount = await pool.query(
      `SELECT COUNT(*)::int AS count FROM call_dispositions WHERE prospect_id = $1`, [prospectB]);
    assert.equal(dispositionCount.rows[0].count, 1);
    const handoffsAfter = await pool.query(
      `SELECT COUNT(*)::int AS count FROM agent_actions
       WHERE action_type = 'closer_handoff' AND payload->>'prospect_id' = $1::text`, [prospectB]);
    assert.equal(handoffsAfter.rows[0].count, 1);

    // A subsequent Pipeline move to Booked converges on the SAME canonical
    // result and must NOT create a second handoff card.
    const pipelineMove = await call('PATCH', `/setter/api/leads/${prospectB}/status`, {
      status: 'booked',
      handoff_note: 'Re-confirming the booking from Pipeline',
    });
    assert.equal(pipelineMove.status, 200, JSON.stringify(pipelineMove.body));
    assert.equal(pipelineMove.body.handoff.duplicate_handoff_prevented, true);
    const handoffsFinal = await pool.query(
      `SELECT COUNT(*)::int AS count FROM agent_actions
       WHERE action_type = 'closer_handoff' AND payload->>'prospect_id' = $1::text`, [prospectB]);
    assert.equal(handoffsFinal.rows[0].count, 1, 'exactly one handoff/opportunity per booked prospect');
  });

  // ── 7. Pipeline booked move reflects in the shared workspace ──────────
  await t.test('Pipeline move to Booked shows Booked + opportunity in Calls workspace', async () => {
    actAs(users.setter);
    const noNote = await call('PATCH', `/setter/api/leads/${prospectC}/status`, { status: 'booked' });
    assert.equal(noNote.status, 400, 'booked requires a handoff note');
    const noReason = await call('PATCH', `/setter/api/leads/${prospectC}/status`, { status: 'dead' });
    assert.equal(noReason.status, 400, 'dead requires a reason');

    const moved = await call('PATCH', `/setter/api/leads/${prospectC}/status`, {
      status: 'booked',
      handoff_note: 'Walkthrough scheduled Thursday 2pm',
    });
    assert.equal(moved.status, 200, JSON.stringify(moved.body));

    const ws = await call('GET', `/api/prospects/${prospectC}/workspace`);
    assert.equal(ws.body.lifecycle.canonicalStage, 'booked');
    assert.equal(ws.body.lifecycle.lastTransitionSource, 'setter_status_endpoint');
    assert.equal(ws.body.opportunity.exists, true);
    assert.equal(ws.body.nextAction.type, 'closer_handoff');

    // The queue (Calls surface) reports the same stage — no divergent writers.
    const leads = await call('GET', '/setter/api/leads?all_statuses=true');
    const lead = leads.body.find(l => l.id === prospectC);
    assert.equal(lead.status, 'booked');
    assert.equal(lead.phone, '6035550000', 'phone stays visible in the queue payload');
  });

  // ── 8. Deterministic call preparation ──────────────────────────────────
  await t.test('call preparation is deterministic and separates facts from hypotheses', async () => {
    actAs(users.setter);
    const { status, body } = await call('GET', `/api/prospects/${prospectA}/call-preparation`);
    assert.equal(status, 200, JSON.stringify(body));
    assert.equal(body.generationMode, 'deterministic');
    assert.ok(body.objective);
    assert.ok(body.opener);
    assert.ok(body.verifiedFacts.length > 0);
    for (const fact of body.verifiedFacts) {
      assert.ok(fact.sourceType, 'every verified fact needs a source');
    }
    for (const hypothesis of body.painPointHypotheses) {
      assert.equal(hypothesis.clearlyLabeledHypothesis, true);
    }
    assert.ok(body.discoveryQuestions.length > 0);
    assert.ok(body.desiredOutcome);
    assert.ok(body.fallbackOutcome);
    assert.ok(body.generatedAt);

    // Tenant isolation applies to call preparation too.
    actAs(users.setter2);
    const cross = await call('GET', `/api/prospects/${prospectA}/call-preparation`);
    assert.equal(cross.status, 404);
  });

  // ── 9. Structured notes endpoint ───────────────────────────────────────
  await t.test('workspace notes write structured storage and surface legacy notes read-only', async () => {
    actAs(users.setter);
    const created = await call('POST', `/api/prospects/${prospectA}/notes`, {
      text: 'Gatekeeper is friendly; call before 9am',
      note_type: 'operator',
    });
    assert.equal(created.status, 201, JSON.stringify(created.body));

    const ws = await call('GET', `/api/prospects/${prospectA}/workspace`);
    assert.ok(ws.body.notes.operatorNotes.some(n => n.text.includes('call before 9am')));
    assert.equal(ws.body.notes.legacyBaseNotes, 'Granite State Cleaning — granitestateclean.example');

    const empty = await call('POST', `/api/prospects/${prospectA}/notes`, { text: '   ' });
    assert.equal(empty.status, 400);
  });
});
