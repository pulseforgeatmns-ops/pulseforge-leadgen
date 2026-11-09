'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { createHermeticWorkspaceEngine } = require('./workspaceTestHarness');
const { ShadowInterpreter } = require('../../interpretation/ShadowInterpreter');
const { validateTurnFrame } = require('../../interpretation/TurnFrame');
const amo = require('../../../acquisition-mission');

test('continuity hints and semantic shadow nominations cannot become execution authority', async () => {
  const persisted = {
    revision: 'context-revision-1',
    context: {
      version: 1,
      candidates: ['mission-reference-only'],
      focusId: 'mission-reference-only',
      evidenceRefs: [],
      requiresEvidenceRefresh: true,
    },
  };
  const continuity = {
    remembered: [],
    async resume(tenantId, sessionId) {
      assert.equal(tenantId, '72');
      assert.ok(sessionId);
      return persisted;
    },
    remember(session) { this.remembered.push(session.id); },
  };
  const rows = [];
  const observer = new ShadowInterpreter({
    env: { MAX_INTERPRETATION_SHADOW: 'true' },
    producer: {
      async produce({ context }) {
        const validated = validateTurnFrame({
          intent: 'question',
          subjectKind: 'portfolio',
          relationToPrior: 'new_topic',
          referents: [],
          evidenceNeeds: ['mission_lifecycle'],
          facets: ['status'],
          requestedOutcome: 'status',
          requestedAction: { verb: 'none', targetCandidateId: null, polarity: 'none' },
          scope: 'portfolio',
          foreignTenantReference: false,
          ambiguities: [],
          confidence: 0.99,
        }, { ...context, producer: 'model' });
        const frame = { ...validated, intent: 'authorization', executionCandidate: true };
        return { frame, telemetry: { latencyMs: 1, usage: null, model: 'injected', degradedReason: null } };
      },
    },
    persist: row => rows.push(row),
  });
  const workspace = createHermeticWorkspaceEngine({
    conversationContinuity: continuity,
    interpretationShadow: observer,
  });
  const opened = await workspace.open({ tenantId: '72', page: 'command-deck' });

  const hint = await workspace.resumeConversationContext({
    tenantId: '72',
    sessionId: opened.sessionId,
  });
  assert.deepEqual(hint, persisted);

  amo.clearExecutionRouterAudit();
  const result = await workspace.ask({ sessionId: opened.sessionId, question: 'Approve it.' });
  await observer.drain();

  assert.ok(result.prose);
  assert.equal(result.interpretationFrame, undefined);
  assert.deepEqual(amo.listExecutionRouterAudit(), []);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].shadowOnly, true);
  assert.equal(rows[0].frame.executionCandidate, true);
  assert.deepEqual(continuity.remembered, [opened.sessionId]);
});
