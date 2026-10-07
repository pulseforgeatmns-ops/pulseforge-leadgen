'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { fetchSpreadsheetCrmEvidence } = require('../utils/spreadsheetCrmEvidence');

test('approved contacts, provider identities and historical activities are read back with tenant/AO isolation', { timeout: 60000 }, async t => {
  const instance = await startDisposablePostgres('spreadsheet-consumers-');
  const db = new Pool({ connectionString: instance.connectionString });
  t.after(async () => { await db.end(); await instance.stop(); });
  await db.query(fs.readFileSync(path.join(__dirname, 'fixtures/maxSpreadsheetBaseSchema.sql'), 'utf8'));
  await db.query(fs.readFileSync(path.join(__dirname, '../migrations/2026-10-07-max-spreadsheet-reliability.sql'), 'utf8'));
  await db.query("INSERT INTO clients VALUES(1),(2); INSERT INTO users(id,client_id,name,role) VALUES(10,1,'AO','ao'),(11,1,'Other AO','ao'),(20,2,'Other tenant','ao')");
  const accountId = randomUUID(), providerId = randomUUID(), foreignId = randomUUID();
  for (const [id, clientId, aoId, name] of [[accountId,1,10,'Account'], [providerId,1,10,'Provider'], [foreignId,2,20,'Other tenant']]) {
    const company = (await db.query('INSERT INTO companies(client_id,name) VALUES($1,$2) RETURNING id', [clientId,name])).rows[0];
    await db.query('INSERT INTO prospects(id,client_id,company_id,assigned_ao_id) VALUES($1,$2,$3,$4)', [id,clientId,company.id,aoId]);
    await db.query('INSERT INTO max_spreadsheet_contacts(id,client_id,prospect_id,data) VALUES($1,$2,$3,$4::jsonb)', [randomUUID(),clientId,id,JSON.stringify({ name: `${name} contact`, evidence: [{ cell: 'F3' }] })]);
  }
  await db.query('INSERT INTO max_spreadsheet_relationships(id,client_id,prospect_id,provider_id,data) VALUES($1,1,$2,$3,$4::jsonb)', [randomUUID(),accountId,providerId,JSON.stringify({ verified: true, role: 'building_management', evidence: [{ cell: 'J3' }] })]);
  for (const [created, occurred, notes] of [['2026-10-07T12:00:00Z','2026-09-23','  Historical call.  '], ['2026-10-01T12:00:00Z','2026-09-30','Later event.']]) {
    await db.query('INSERT INTO ao_prospect_activity(id,prospect_id,tenant_id,ao_id,activity_type,notes,metadata,created_at) VALUES($1,$2,1,10,\'call\',$3,$4::jsonb,$5)', [randomUUID(),accountId,notes,JSON.stringify({ occurredOn: occurred }),created]);
  }
  const read = () => fetchSpreadsheetCrmEvidence({ db, clientId: 1, prospectId: accountId, aoId: 10, includeActivities: true });
  const result = await read();
  assert.equal(result.contacts[0].name, 'Account contact');
  assert.deepEqual(result.contacts[0].evidence, [{ cell: 'F3' }]);
  assert.equal(result.relationships[0].providerName, 'Provider');
  assert.equal(result.relationships[0].providerId, providerId);
  assert.deepEqual(result.activities.map(a => a.occurredOn), ['2026-09-30','2026-09-23']);
  assert.equal(result.activities[1].notes, '  Historical call.  ');
  assert.equal(result.activities[1].recordedAt, '2026-10-07T12:00:00.000Z');
  for (const [clientId, prospectId, aoId] of [[2,accountId,20],[1,foreignId,10],[1,accountId,11]]) {
    assert.deepEqual(await fetchSpreadsheetCrmEvidence({ db, clientId, prospectId, aoId, includeActivities: true }), { contacts: [], relationships: [], activities: [] });
  }
  await db.query('UPDATE prospects SET assigned_ao_id=11 WHERE id=$1', [providerId]);
  assert.equal((await read()).relationships.length, 0, 'Provider reassigned outside AO scope must not leak its current identity');
});
