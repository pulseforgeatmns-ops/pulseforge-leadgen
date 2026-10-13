'use strict';
// Non-skipping disposable PostgreSQL release gate. No external DATABASE_URL.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { Client, Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { inspectDatabase, connectionConfig, QUERIES } = require('../scripts/rolloutPreflight');
const fixture = name => fs.readFileSync(path.join(__dirname, 'fixtures', name), 'utf8');

test('rollout preflight real PostgreSQL: absent, partial, migrated, drift, cleanup and read-only enforcement', async t => {
  const pg = await startDisposablePostgres('rollout-preflight-');
  const admin = new Pool({ connectionString: pg.connectionString, max: 2 });
  t.after(async () => { await admin.end(); await pg.stop(); });
  const calls = [], pids = [];
  class ObservedClient extends Client {
    async connect() { await super.connect(); pids.push(this.processID); }
    query(q, ...rest) { calls.push(typeof q === 'string' ? q : q.text); return super.query(q, ...rest); }
  }
  const run = async () => {
    calls.length = 0;
    const result = await inspectDatabase(pg.connectionString, { Client: ObservedClient });
    assert.deepEqual(calls, ['BEGIN READ ONLY', ...Object.values(QUERIES), 'ROLLBACK']);
    assert.equal((await admin.query('SELECT count(*)::int AS n FROM pg_stat_activity WHERE pid=ANY($1::int[])', [pids])).rows[0].n, 0, 'no preflight backend or transaction may leak');
    return result;
  };
  await t.test('absent schema is blocked, never mistaken for a successful empty result', async () => {
    const r = await run(); assert.equal(r.status, 'blocked'); assert.ok(r.checks.find(c => c.id === 'table.prospects').action);
  });
  await admin.query(fixture('rolloutPreflightBaseSchema.sql'));
  await t.test('base schema lacks migration; partial fields cannot pass', async () => {
    let r = await run(); assert.equal(r.status, 'blocked');
    await admin.query('ALTER TABLE prospects ADD COLUMN ao_call_suppressed boolean NOT NULL DEFAULT false');
    r = await run(); assert.equal(r.status, 'blocked');
    assert.equal(r.checks.find(c => c.id === 'column.prospects.ao_call_suppressed').status, 'ready');
    assert.equal(r.checks.find(c => c.id === 'column.prospects.ao_outreach_review_required').status, 'blocked');
  });
  await admin.query(fixture('rolloutPreflightMigration.sql'));
  await t.test('exact reviewed migration passes real catalog deparsing and triggers', async () => {
    const r = await run(); assert.equal(r.status, 'ready', JSON.stringify(r.checks.filter(c => c.status !== 'ready')));
  });
  await t.test('owned connection defaults to read-only and cannot perform DML', async () => {
    const client = new Client(connectionConfig(pg.connectionString)); await client.connect();
    try { await assert.rejects(client.query('INSERT INTO public.clients(id) VALUES(987)'), e => e.code === '25006'); }
    finally { await client.end(); }
    assert.equal((await admin.query('SELECT count(*)::int AS n FROM clients')).rows[0].n, 0);
  });
  await t.test('real server statement timeout is sanitized and leaves no backend', async () => {
    class DelayedClient extends ObservedClient {
      query(q, ...rest) {
        if (q.text === QUERIES.columns) return super.query({ text: 'SELECT pg_sleep(10)', query_timeout: 3500 });
        return super.query(q, ...rest);
      }
    }
    const r = await inspectDatabase(pg.connectionString, { Client: DelayedClient });
    assert.equal(r.status, 'unknown'); assert.equal(r.checks[0].id, 'timeout');
    assert.equal((await admin.query('SELECT count(*)::int AS n FROM pg_stat_activity WHERE pid=ANY($1::int[])', [pids])).rows[0].n, 0);
  });
  await t.test('constant CHECK(false) has empty conkey and must prevent ready', async () => {
    for (const table of ['prospects', 'max_spreadsheet_contacts']) {
      await admin.query(`ALTER TABLE ${table} ADD CONSTRAINT preflight_constant_false CHECK(false)`);
      const r = await run(); assert.equal(r.status, 'unknown');
      assert.equal(r.checks.find(c => c.id === `check.${table}.additional`).status, 'unknown');
      await admin.query(`ALTER TABLE ${table} DROP CONSTRAINT preflight_constant_false`);
    }
  });
  await t.test('disabled immutable trigger blocks', async () => {
    await admin.query('ALTER TABLE max_spreadsheet_proposals DISABLE TRIGGER max_spreadsheet_proposal_immutable');
    const r = await run(); assert.equal(r.checks.find(c => c.id === 'trigger.max_spreadsheet_proposal_immutable').status, 'blocked');
    await admin.query('ALTER TABLE max_spreadsheet_proposals ENABLE TRIGGER max_spreadsheet_proposal_immutable');
  });
  await t.test('disabled internal FK triggers cannot certify tenant enforcement', async () => {
    await admin.query('ALTER TABLE max_spreadsheet_contacts DISABLE TRIGGER ALL');
    const r = await run(); assert.equal(r.checks.find(c => c.id === 'foreign_key.max_spreadsheet_contacts.client_id+prospect_id').status, 'blocked');
    await admin.query('ALTER TABLE max_spreadsheet_contacts ENABLE TRIGGER ALL');
  });
  await t.test('NOT VALID foreign key cannot pass', async () => {
    await admin.query('ALTER TABLE max_spreadsheet_contacts DROP CONSTRAINT max_spreadsheet_contacts_client_id_prospect_id_fkey');
    await admin.query('ALTER TABLE max_spreadsheet_contacts ADD CONSTRAINT test_fk FOREIGN KEY(client_id,prospect_id) REFERENCES prospects(client_id,id) NOT VALID');
    const r = await run(); assert.equal(r.checks.find(c => c.id === 'foreign_key.max_spreadsheet_contacts.client_id+prospect_id').status, 'blocked');
  });
  await t.test('missing composite unique prerequisite blocks even with tables and columns present', async () => {
    await admin.query('ALTER TABLE users DROP CONSTRAINT users_client_id_id_key CASCADE');
    const r = await run(); assert.equal(r.checks.find(c => c.id === 'unique.users.client_id+id').status, 'blocked');
  });
});
