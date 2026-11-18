'use strict';

/**
 * Field-note correction memory against disposable PostgreSQL: a decision-maker
 * correction leaves exactly one active record per contact, supersedes the
 * contact's prior REFERENCE_BINDING, and never touches another conversation or
 * tenant. Never connects to a real database.
 */

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const {
  interpretWithDurableConversationContext,
  PostgresConversationMemoryRepository,
  SEMANTIC_TYPE,
} = require('../packages/max/understanding');

const NOW = new Date('2026-10-05T15:00:00.000Z');

test('PostgreSQL: decision-maker correction supersedes the stale contact and binding only', { timeout: 60000 }, async (t) => {
  const instance = await startDisposablePostgres('max-fieldnote-');
  const db = new Pool({ connectionString: instance.connectionString });
  t.after(async () => { await db.end(); await instance.stop(); });
  await db.query('CREATE TABLE clients (id INTEGER PRIMARY KEY); INSERT INTO clients VALUES (1), (2);');
  await db.query(fs.readFileSync('migrations/2026-10-06-max-conversation-memory.sql', 'utf8'));
  const repo = new PostgresConversationMemoryRepository(db);
  const turn = (tenantId, conversationId, text) => interpretWithDurableConversationContext({
    tenantId, clientId: tenantId, conversationId, actor: { userId: '10', role: 'manager' }, memoryRepository: repo, now: NOW, text,
  });
  const active = async (tenantId, conversationId) => (await repo.loadActive({ tenantId, conversationId, now: NOW })).records;

  // An untouched conversation in the same tenant and one in another tenant.
  await turn(1, 'other-conv', 'Dave is the decision maker at Exeter Phillips.');
  await turn(2, 'm3-conv', 'Dave is the decision maker at Exeter Phillips.');

  await turn(1, 'm3-conv', 'Dave is the decision maker at Exeter Phillips.');
  await turn(1, 'm3-conv', "Actually Dave isn't the decision maker. Lisa handles vendors.");
  await turn(1, 'm3-conv', "Actually Dave isn't the decision maker, he's an influencer.");

  const records = await active(1, 'm3-conv');
  const contacts = (name) => records.filter((r) => r.semanticType === SEMANTIC_TYPE.ACTIVE_CONTACT && r.payload?.contact?.name === name);
  assert.equal(contacts('Dave').length, 1, 'one active record for the corrected contact');
  assert.equal(contacts('Dave')[0].payload.contact.role, 'influencer');
  assert.equal(contacts('Lisa').length, 1);
  const bindings = records.filter((r) => r.semanticType === SEMANTIC_TYPE.REFERENCE_BINDING && r.payload?.bindingKey === 'dm:dave');
  assert.equal(bindings.length, 1, 'repeated corrections do not accumulate dm: bindings');

  for (const [tenantId, conversationId] of [[1, 'other-conv'], [2, 'm3-conv']]) {
    const untouched = (await active(tenantId, conversationId)).filter((r) => r.semanticType === SEMANTIC_TYPE.ACTIVE_CONTACT && r.payload?.contact?.name === 'Dave');
    assert.equal(untouched.length, 1);
    assert.equal(untouched[0].payload.contact.role, 'decision_maker', `${tenantId}/${conversationId} untouched`);
  }
});
