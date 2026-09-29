'use strict';

const assert = require('node:assert/strict');
const express = require('express');
const test = require('node:test');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');

const PROSPECT_ID = '20000000-0000-4000-8000-000000000099';
const COMPANY_ID = '20000000-0000-4000-8000-000000000010';
const AO_ID = 101;

async function seedMinimal(db) {
  await db.query(`
    CREATE TABLE clients(id INTEGER PRIMARY KEY, name TEXT);
    CREATE TABLE users(
      id INTEGER PRIMARY KEY, client_id INTEGER NOT NULL REFERENCES clients(id),
      name TEXT, email TEXT, role TEXT, active BOOLEAN DEFAULT true
    );
    CREATE TABLE companies(
      id UUID PRIMARY KEY, client_id INTEGER NOT NULL REFERENCES clients(id),
      name TEXT, location TEXT, website TEXT, industry TEXT
    );
    CREATE TABLE prospects(
      id UUID PRIMARY KEY, client_id INTEGER NOT NULL REFERENCES clients(id),
      company_id UUID REFERENCES companies(id),
      first_name TEXT, last_name TEXT, email TEXT, phone TEXT, job_title TEXT,
      vertical TEXT, assigned_ao_id INTEGER REFERENCES users(id),
      ao_current_status TEXT, ao_next_action TEXT,
      help_requested BOOLEAN NOT NULL DEFAULT false,
      help_reason TEXT,
      help_requested_at TIMESTAMPTZ,
      do_not_contact BOOLEAN DEFAULT false,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE UNIQUE INDEX prospects_client_id_id ON prospects(client_id, id);
    CREATE TABLE touchpoints(
      id BIGSERIAL PRIMARY KEY, client_id INTEGER NOT NULL, prospect_id UUID NOT NULL,
      action_type TEXT, channel TEXT, outcome TEXT, content_summary TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
  await db.query("INSERT INTO clients VALUES (10, 'Anchor Cleaning')");
  await db.query(`INSERT INTO users(id, client_id, name, email, role, active) VALUES
    ($1, 10, 'Tony', 'tony@example.com', 'ao', true)`, [AO_ID]);
  await db.query(`INSERT INTO companies(id, client_id, name, location) VALUES ($1, 10, 'NH Family Dentistry', 'Manchester NH')`, [COMPANY_ID]);
  await db.query(`INSERT INTO prospects(
    id, client_id, company_id, first_name, last_name, email, assigned_ao_id, ao_next_action
  ) VALUES ($1::uuid, 10, $2::uuid, 'Lori', '', 'lori@example.com', $3, 'follow_up')`,
  [PROSPECT_ID, COMPANY_ID, AO_ID]);
}

function listen(app) {
  const server = app.listen(0, '127.0.0.1');
  return new Promise(resolve => server.on('listening', () => resolve({
    server,
    base: `http://127.0.0.1:${server.address().port}`,
  })));
}

test('follow-up save persists draft row and activity with consistent UUID', { skip: !process.env.RUN_DISPOSABLE_PG }, async t => {
  const pg = await startDisposablePostgres();
  t.after(async () => { await pg.stop(); });

  const db = new Pool({ connectionString: pg.connectionString });
  t.after(async () => { await db.end(); });

  await seedMinimal(db);
  const { ensureAoCrmSchema } = require('../utils/aoCrmSchema');
  await ensureAoCrmSchema(db);

  delete require.cache[require.resolve('../db')];
  require.cache[require.resolve('../db')] = {
    id: require.resolve('../db'),
    filename: require.resolve('../db'),
    loaded: true,
    exports: db,
  };

  const aoFollowup = require('../services/aoFollowupService');

  const gen = await aoFollowup.generateFollowUpDraft({
    clientId: 10,
    aoUserId: AO_ID,
    prospectId: PROSPECT_ID,
    body: { aoNotes: 'Front desk receptionist cleans the office.' },
    profile: { id: AO_ID, name: 'Tony', email: 'tony@example.com' },
    db,
  });
  assert.equal(gen.ok, true);
  assert.match(gen.draft.emailDraft, /Tony/);
  assert.equal(gen.input_snapshot.accountId, PROSPECT_ID);

  const beforeHelp = (await db.query('SELECT help_requested FROM prospects WHERE id = $1::uuid', [PROSPECT_ID])).rows[0];
  assert.equal(beforeHelp.help_requested, false);

  const saved = await aoFollowup.saveFollowUpDraft({
    clientId: 10,
    aoUserId: AO_ID,
    prospectId: PROSPECT_ID,
    draft: gen.draft,
    inputSnapshot: gen.input_snapshot,
    flagJakeReview: false,
    db,
  });
  assert.equal(saved.ok, true);

  const afterHelp = (await db.query('SELECT help_requested FROM prospects WHERE id = $1::uuid', [PROSPECT_ID])).rows[0];
  assert.equal(afterHelp.help_requested, false);

  const draftRow = (await db.query(
    'SELECT account_id, assigned_ao_id, status FROM ao_followup_drafts WHERE id = $1',
    [saved.draft.id]
  )).rows[0];
  assert.equal(String(draftRow.account_id), PROSPECT_ID);
  assert.equal(draftRow.assigned_ao_id, AO_ID);

  const activity = (await db.query(`
    SELECT activity_type, prospect_id, metadata
    FROM ao_prospect_activity
    WHERE prospect_id = $1::uuid AND activity_type = 'followup_draft_created'
  `, [PROSPECT_ID])).rows[0];
  assert.ok(activity);
  assert.equal(String(activity.prospect_id), PROSPECT_ID);
  assert.equal(String(activity.metadata.draft_id), saved.draft.id);

  const flagged = await aoFollowup.saveFollowUpDraft({
    clientId: 10,
    aoUserId: AO_ID,
    prospectId: PROSPECT_ID,
    draft: gen.draft,
    inputSnapshot: gen.input_snapshot,
    flagJakeReview: true,
    db,
  });
  assert.equal(flagged.ok, true);
  const afterFlag = (await db.query('SELECT help_requested, help_reason FROM prospects WHERE id = $1::uuid', [PROSPECT_ID])).rows[0];
  assert.equal(afterFlag.help_requested, true);

  const listed = await aoFollowup.listFollowUpDrafts({
    clientId: 10,
    prospectId: PROSPECT_ID,
    aoUserId: AO_ID,
    db,
  });
  assert.ok(listed.drafts.length >= 2);

  delete require.cache[require.resolve('../db')];
});

test('follow-up HTTP draft/save/list use prospect UUID end-to-end', { skip: !process.env.RUN_DISPOSABLE_PG }, async t => {
  const pg = await startDisposablePostgres();
  t.after(async () => { await pg.stop(); });

  const db = new Pool({ connectionString: pg.connectionString });
  t.after(async () => { await db.end(); });

  await seedMinimal(db);
  const { ensureAoCrmSchema } = require('../utils/aoCrmSchema');
  await ensureAoCrmSchema(db);

  const stubs = [
    '../utils/aoFieldSchema',
    '../services/aoFieldService',
  ];
  for (const mod of stubs) {
    require.cache[require.resolve(mod)] = {
      id: require.resolve(mod),
      filename: require.resolve(mod),
      loaded: true,
      exports: mod.includes('aoFieldService')
        ? { getAoProfile: async id => ({ id, name: 'Tony', email: 'tony@example.com' }) }
        : { ensureAoFieldSchema: async () => {} },
    };
  }

  require.cache[require.resolve('../db')] = {
    id: require.resolve('../db'),
    filename: require.resolve('../db'),
    loaded: true,
    exports: db,
  };

  delete require.cache[require.resolve('../routes/ao')];
  const router = require('../routes/ao');
  const app = express();
  app.use(express.json());
  app.use((req, _res, next) => {
    req.user = { id: AO_ID, role: 'ao', client_id: 10, name: 'Tony', email: 'tony@example.com' };
    req.session = { user: req.user, active_client_id: 10 };
    next();
  });
  app.use('/ao', router);
  const running = await listen(app);
  t.after(async () => { await new Promise(r => running.server.close(r)); });

  let res = await fetch(`${running.base}/ao/api/crm/accounts/${PROSPECT_ID}/followup/draft`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ aoNotes: 'Front desk receptionist cleans the office.' }),
  });
  assert.equal(res.status, 200);
  const { draft } = await res.json();
  assert.match(draft.emailDraft, /Tony/);

  res = await fetch(`${running.base}/ao/api/crm/accounts/${PROSPECT_ID}/followup/save`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ draft, input_snapshot: { accountId: PROSPECT_ID, accountName: 'NH Family Dentistry' } }),
  });
  assert.equal(res.status, 200);

  res = await fetch(`${running.base}/ao/api/crm/accounts/${PROSPECT_ID}/followup/drafts`);
  assert.equal(res.status, 200);
  const { drafts } = await res.json();
  assert.ok(drafts.length >= 1);

  delete require.cache[require.resolve('../db')];
  delete require.cache[require.resolve('../routes/ao')];
});
