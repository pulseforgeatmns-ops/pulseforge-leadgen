'use strict';

/**
 * P0-MAX-REASONING-001 PR 3 — durable projection of the ledger in the existing
 * ConversationContextStore (version 2), hermetic.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { project, validate, MAX_BYTES } = require('../ConversationContextStore');
const { ConversationLedgerRecorder } = require('../ConversationLedgerRecorder');

const TENANT = '21';
function sessionWithLedger(id = 'sess-a') {
  const s = { id, context: { tenantId: TENANT, missionId: 'm1' } };
  new ConversationLedgerRecorder({ env: { MAX_CONVERSATION_LEDGER: 'true' } }).observe({ session: s, text: 'Show me the prospects.', result: {
    structured: { metadata: { items: ['1', '2'].map((x) => ({ recordRef: `prospect:${TENANT}:${x}` })) } },
  } });
  return s;
}

describe('ConversationContextStore version 2', () => {
  it('without includeLedger the projection is the unchanged version 1', () => {
    const s = sessionWithLedger();
    const v1 = project(s, TENANT);
    assert.deepEqual(Object.keys(v1), ['version', 'candidates', 'focusId', 'evidenceRefs', 'requiresEvidenceRefresh']);
    assert.equal(v1.version, 1);
    const noLedger = { id: 'sess-a', context: { tenantId: TENANT, missionId: 'm1' } };
    assert.deepEqual(project(noLedger, TENANT, { includeLedger: true }), v1, 'no ledger on the session: version 1');
  });

  it('with includeLedger it adds a validated ledger bound to the row scope', () => {
    const s = sessionWithLedger();
    const v2 = project(s, TENANT, { includeLedger: true });
    assert.equal(v2.version, 2);
    assert.ok(Buffer.byteLength(JSON.stringify(v2)) <= MAX_BYTES);
    assert.deepEqual(validate(v2, { tenantId: TENANT, sessionId: 'sess-a' }).ledger.referents.length, v2.ledger.referents.length);
  });

  it('refuses a version-2 row whose ledger names another tenant or session, or no scope', () => {
    const v2 = project(sessionWithLedger(), TENANT, { includeLedger: true });
    assert.throws(() => validate(v2, { tenantId: '10', sessionId: 'sess-a' }), /Invalid conversation context/);
    assert.throws(() => validate(v2, { tenantId: TENANT, sessionId: 'sess-b' }), /Invalid conversation context/);
    assert.throws(() => validate(v2), /Invalid conversation context/);
    assert.throws(() => validate({ ...v2, ledger: { ...v2.ledger, approvals: [] } }, { tenantId: TENANT, sessionId: 'sess-a' }), /Invalid conversation context/);
    assert.throws(() => validate({ ...v2, extra: true }, { tenantId: TENANT, sessionId: 'sess-a' }), /Invalid conversation context/);
  });

  it('a ledger left on a session for another tenant is never projected', () => {
    const s = sessionWithLedger();
    s.context.tenantId = '10';
    assert.equal(project(s, '10', { includeLedger: true }).version, 1);
  });
});
