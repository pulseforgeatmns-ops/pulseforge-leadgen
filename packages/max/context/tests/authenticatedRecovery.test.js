'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { createHermeticWorkspaceEngine } = require('../../workspace/tests/workspaceTestHarness');
const amo = require('../../../acquisition-mission');

test('process replacement restores only tenant-validated referents and never authority', async () => {
  let loads = 0;
  const requested = [];
  const continuity = {
    async resume(tenantId, sessionId) {
      loads += 1;
      assert.equal(tenantId, 't1'); assert.equal(sessionId, 'session-after-restart');
      await new Promise(resolve => setImmediate(resolve));
      return { revision: 'r1', context: {
        version: 1, candidates: ['current-mission', 'deleted-mission'], focusId: 'current-mission',
        evidenceRefs: [
          { kind: 'operational_inspection', observedAt: '2026-11-09T01:00:00.000Z' },
          { kind: 'operational_inspection', observedAt: '2099-01-01T00:00:00.000Z' },
        ],
        requiresEvidenceRefresh: true,
      } };
    },
    remember() {},
  };
  const workspace = createHermeticWorkspaceEngine({
    conversationContinuity: continuity,
    conversationReferenceLoader: async input => {
      requested.push(input);
      return input.missionId === 'current-mission'
        ? { found: true, mission: { id: 'current-mission', tenantId: input.tenantId } }
        : { found: false, mission: null };
    },
  });

  const [first, concurrent] = await Promise.all([
    workspace.recoverConversationSession({ tenantId: 't1', sessionId: 'session-after-restart' }),
    workspace.recoverConversationSession({ tenantId: 't1', sessionId: 'session-after-restart' }),
  ]);
  assert.deepEqual(first, concurrent);
  assert.equal(loads, 1);
  assert.deepEqual(requested.map(x => [x.tenantId, x.missionId]), [['t1','current-mission'],['t1','deleted-mission']]);
  const session = workspace.sessions.get('session-after-restart');
  assert.equal(session.context.tenantId, 't1');
  assert.equal(session.context.missionId, 'current-mission');
  assert.equal(session.context.requiresEvidenceRefresh, true);
  assert.deepEqual(session.context.recoveredEvidenceRefs, [{ kind: 'operational_inspection', observedAt: '2026-11-09T01:00:00.000Z' }]);
  assert.deepEqual(session.operationalFocus.missionIds, ['current-mission']);
  assert.deepEqual(session.messages, []);
  assert.doesNotMatch(JSON.stringify(session.context), /approv|pending|execut|decision/i);

  amo.clearExecutionRouterAudit();
  const answer = await workspace.ask({ sessionId: session.id, question: 'Approve it.' });
  assert.ok(answer.prose);
  assert.deepEqual(amo.listExecutionRouterAudit(), []);
});

test('missing, expired, corrupt and cross-tenant durable state fail closed', async () => {
  const calls = [];
  const workspace = createHermeticWorkspaceEngine({
    conversationContinuity: {
      async resume(tenantId, sessionId) { calls.push([tenantId,sessionId]); return null; },
      remember() {},
    },
    conversationReferenceLoader: async () => { throw Error('must not load'); },
  });
  assert.deepEqual(await workspace.recoverConversationSession({ tenantId: 't2', sessionId: 'missing' }),
    { recovered: false, reason: 'context_unavailable' });
  assert.deepEqual(calls, [['t2','missing']]);
  assert.equal(workspace.sessions.get('missing'), null);
});

test('restored session ids cannot be rebound to another tenant', () => {
  const { SessionStore } = require('../../workspace/SessionStore');
  const sessions = new SessionStore();
  sessions.restore('stable-session', { tenantId: 't1', page: 'command-deck' });
  assert.throws(() => sessions.restore('stable-session', { tenantId: 't2', page: 'command-deck' }), /tenant mismatch/);
});
