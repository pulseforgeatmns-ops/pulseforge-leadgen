'use strict';

/**
 * SPEC-148 — Reflective Cognition.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { STAGES, OPERATOR_DECISION_KINDS } = amo;
const {
  THINKING_MODES,
  classifyOperatorCognition,
  isReadOnlyCognition,
} = require('../../operatorCognition');
const {
  CONVERSATION_SUBJECTS,
  detectConversationSubject,
} = require('../ConversationSubject');
const { buildReflectionContext } = require('../ReflectionContext');
const {
  composeReflectiveResponse,
  classifyReflectQuestion,
} = require('../ReflectionLayer');
const { maybeHandleReflectionTurn } = require('../ReflectionRouting');
const { resolveWorkspaceOwner, WORKSPACE_OWNERS } = require('../WorkspaceOwnershipResolver');
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const { installTestAmoRuntime } = require('./amoTestRuntime');
const { advancePlanAfterApproval } = require('../AmoOperatorApproval');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

function seedSessionWithPriorTurn(session, priorQuestion, priorResponse, priorContext = {}) {
  session.messages.push(
    { role: 'operator', text: priorQuestion, at: new Date().toISOString() },
    { role: 'max', text: priorResponse, at: new Date().toISOString() },
    { role: 'operator', text: 'placeholder-current', at: new Date().toISOString() }
  );
  session.context = {
    tenantId: '10',
    conversationIntent: priorContext.conversationIntent || {
      intent: THINKING_MODES.STRATEGY,
      via: 'strategy_phrase',
      confidence: 0.9,
    },
    workspaceOwner: priorContext.workspaceOwner || WORKSPACE_OWNERS.REASONING,
    workspaceOwnerReason: priorContext.workspaceOwnerReason || 'no_owner_claim',
    missionId: priorContext.missionId || null,
  };
}

describe('SPEC-148 — Reflective Cognition', () => {
  describe('classifyOperatorCognition — REFLECT before EXPLAIN', () => {
    it('classifies meta-cognitive questions as REFLECT', () => {
      const cases = [
        'What did you think I was asking?',
        'Why did you answer like that?',
        'Walk me through your reasoning.',
        'Why are you waiting?',
        'What assumptions are you making?',
        'What were you trying to accomplish?',
        'Did you misunderstand me?',
        'What pipeline answered that?',
        "Why didn't Scout execute?",
        'Why did Scout run?',
        'Why did you recommend Harbor?',
        'Would you answer differently now?',
      ];
      for (const q of cases) {
        const intent = classifyOperatorCognition(q);
        assert.equal(intent.intent, THINKING_MODES.REFLECT, `expected REFLECT for: ${q}`);
        assert.equal(intent.mutatesMission, false);
        assert.equal(intent.thinkingMode, 'reflection');
      }
    });

    it('keeps mission causal questions as EXPLAIN', () => {
      const intent = classifyOperatorCognition('Why did Scout stop?');
      assert.equal(intent.intent, THINKING_MODES.EXPLAIN);
    });
  });

  describe('detectConversationSubject', () => {
    it('locks subject to reflection for REFLECT intent phrases', () => {
      const subject = detectConversationSubject('What did you think I meant?');
      assert.equal(subject.subject, CONVERSATION_SUBJECTS.REFLECTION);
      assert.equal(subject.locked, true);
    });

    it('defaults to business for ordinary questions', () => {
      const subject = detectConversationSubject('What is our ICP?', {
        intent: THINKING_MODES.EXPLAIN,
      });
      assert.equal(subject.subject, CONVERSATION_SUBJECTS.BUSINESS);
      assert.equal(subject.locked, false);
    });
  });

  describe('resolveWorkspaceOwner — reflection subject lock', () => {
    it('returns REFLECTION owner when subject is locked reflection', async () => {
      const owner = await resolveWorkspaceOwner({
        question: 'What did you think I was asking?',
        session: { id: 's1', context: { tenantId: '10' } },
        conversationSubject: {
          subject: CONVERSATION_SUBJECTS.REFLECTION,
          locked: true,
          reason: 'reflect_subject_phrase',
          confidence: 0.96,
        },
      });
      assert.equal(owner.owner, WORKSPACE_OWNERS.REFLECTION);
      assert.equal(owner.subjectLock, true);
    });
  });

  describe('ReflectionLayer', () => {
    it('Test 1 — intent misinterpretation references prior turn, not business advice', () => {
      const session = { id: 't1', messages: [] };
      seedSessionWithPriorTurn(
        session,
        'Give me an executive briefing on today\'s priorities.',
        'Property managers in the downtown corridor are your highest-leverage target this week.',
        {
          conversationIntent: {
            intent: THINKING_MODES.STRATEGY,
            via: 'strategy_phrase',
            confidence: 0.9,
          },
          workspaceOwner: WORKSPACE_OWNERS.REASONING,
        }
      );

      const ctx = buildReflectionContext({
        question: 'What did you think I was asking?',
        session,
        conversationIntent: { intent: THINKING_MODES.REFLECT },
        previousTurnContext: {
          conversationIntent: session.context.conversationIntent,
          workspaceOwner: session.context.workspaceOwner,
          workspaceOwnerReason: session.context.workspaceOwnerReason,
        },
      });

      const { prose } = composeReflectiveResponse({
        question: 'What did you think I was asking?',
        reflectionContext: ctx,
      });

      assert.match(prose, /interpret|classified|strategy|previous/i);
      assert.match(prose, /executive briefing|priorities|Give me an executive/i);
      assert.doesNotMatch(prose, /Property managers/i);
      assert.doesNotMatch(prose, /Blueprint|Client Intelligence/i);
    });

    it('Test 2 — answer style discusses reasoning, not Blueprint', () => {
      const session = { id: 't2', messages: [] };
      seedSessionWithPriorTurn(
        session,
        'What should we focus on?',
        'I recommend targeting property managers first.',
        {
          conversationIntent: { intent: THINKING_MODES.STRATEGY, via: 'strategy_phrase' },
          workspaceOwner: WORKSPACE_OWNERS.REASONING,
        }
      );

      const ctx = buildReflectionContext({
        question: 'Why did you answer like that?',
        session,
        conversationIntent: { intent: THINKING_MODES.REFLECT },
        previousTurnContext: {
          conversationIntent: session.context.conversationIntent,
          workspaceOwner: session.context.workspaceOwner,
        },
      });

      const { prose } = composeReflectiveResponse({
        question: 'Why did you answer like that?',
        reflectionContext: ctx,
      });

      assert.match(prose, /classified|pipeline|answered that way|framing/i);
      assert.doesNotMatch(prose, /Blueprint|ICP|ideal customer/i);
    });

    it('Test 3 — misunderstanding is reflection, not business advice', () => {
      const { prose } = composeReflectiveResponse({
        question: 'Did you misunderstand me?',
        reflectionContext: buildReflectionContext({
          question: 'Did you misunderstand me?',
          session: {
            messages: [
              { role: 'operator', text: 'Brief me.' },
              { role: 'max', text: 'Here is a market recommendation.' },
              { role: 'operator', text: 'Did you misunderstand me?' },
            ],
          },
          conversationIntent: { intent: THINKING_MODES.REFLECT },
          previousTurnContext: {
            conversationIntent: { intent: THINKING_MODES.STRATEGY },
            workspaceOwner: WORKSPACE_OWNERS.REASONING,
          },
        }),
      });

      assert.match(prose, /misunderstood|follow-up/i);
      assert.doesNotMatch(prose, /recommend targeting|property managers/i);
    });

    it('Test 4 — Scout pipeline explanation without recommendation', () => {
      const { prose, reflectKind } = composeReflectiveResponse({
        question: "Why didn't Scout execute?",
        reflectionContext: buildReflectionContext({
          question: "Why didn't Scout execute?",
          session: { messages: [{ role: 'operator', text: "Why didn't Scout execute?" }] },
          conversationIntent: { intent: THINKING_MODES.REFLECT },
          previousTurnContext: {
            conversationIntent: { intent: THINKING_MODES.INSPECT },
            workspaceOwner: WORKSPACE_OWNERS.ACTIVE_MISSION,
          },
        }),
      });

      assert.equal(reflectKind, 'scout_not_executed');
      assert.match(prose, /Scout did not execute|did not run/i);
      assert.doesNotMatch(prose, /recommend|target|property managers/i);
    });

    it('Test 5 — assumption inspection', () => {
      const { prose, reflectKind } = composeReflectiveResponse({
        question: 'What assumptions did you make?',
        reflectionContext: buildReflectionContext({
          question: 'What assumptions did you make?',
          session: {
            messages: [
              { role: 'operator', text: 'What should we do?' },
              { role: 'max', text: 'Focus on law firms.' },
              { role: 'operator', text: 'What assumptions did you make?' },
            ],
          },
          conversationIntent: { intent: THINKING_MODES.REFLECT },
          previousTurnContext: {
            conversationIntent: { intent: THINKING_MODES.STRATEGY },
            workspaceOwner: WORKSPACE_OWNERS.REASONING,
          },
        }),
      });

      assert.equal(reflectKind, 'assumptions');
      assert.match(prose, /assumptions/i);
      assert.match(prose, /assumed/i);
    });
  });

  describe('maybeHandleReflectionTurn', () => {
    it('returns reflection prose without business intelligence metadata', async () => {
      const turn = await maybeHandleReflectionTurn({
        question: 'What assumptions are you making?',
        conversationIntent: classifyOperatorCognition('What assumptions are you making?'),
        conversationSubject: {
          subject: CONVERSATION_SUBJECTS.REFLECTION,
          locked: true,
          reason: 'reflect_subject_phrase',
        },
        session: {
          messages: [
            { role: 'operator', text: 'Help me plan.' },
            { role: 'max', text: 'Here is a plan.' },
            { role: 'operator', text: 'What assumptions are you making?' },
          ],
        },
        previousTurnContext: {
          conversationIntent: { intent: THINKING_MODES.STRATEGY },
          workspaceOwner: WORKSPACE_OWNERS.REASONING,
        },
      });

      assert.ok(turn);
      assert.equal(turn.handled, true);
      assert.equal(turn.structured.metadata.businessIntelligenceUsed, false);
      assert.equal(turn.structured.metadata.reflectiveCognition, true);
      assert.match(turn.prose, /assumptions/i);
    });

    it('is read-only cognition', () => {
      const intent = classifyOperatorCognition('Did you misunderstand me?');
      assert.equal(isReadOnlyCognition(intent), true);
    });
  });

  describe('WorkspaceEngine integration', () => {
    let engine;
    let mission;
    let runtime;

    beforeEach(() => {
      engine = amo.createAcquisitionMissionEngine();
      runtime = installTestAmoRuntime({ engine });
      mission = engine.create({
        tenantId: '10',
        objective: OBJECTIVE,
        targetSegment: 'Law Firms',
      });
    });

    async function seedMissionAwaitingPrioritization() {
      await advancePlanAfterApproval({
        engine,
        mission,
        tenantId: '10',
        question: 'Approved.',
      });
      const { SPECIALISTS, CONTRIBUTION_KINDS } = amo;
      engine.contribute(
        mission.id,
        {
          specialist: SPECIALISTS.SCOUT,
          kind: CONTRIBUTION_KINDS.DISCOVERY,
          payload: {
            companies: [{ name: 'Harbor Law Group', icpScore: 82 }],
            prospects: [{ name: 'Jordan Lee', company: 'Harbor Law Group' }],
            complete: true,
          },
        },
        { tenantId: '10' }
      );
      const updated = engine.get(mission.id, '10');
      updated.pendingOperatorDecision = {
        stage: STAGES.DISCOVER,
        kind: OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL,
        prompt: 'Approve prioritization?',
      };
      engine.store.putMission(updated);
    }

    it('reflect turn bypasses active mission and does not mutate mission', async () => {
      await seedMissionAwaitingPrioritization();
      const before = engine.get(mission.id, '10');

      const workspace = createWorkspaceEngine({
        missionsEnabled: true,
        resolverEnabled: true,
        missionEngine: {
          activeMissionResolver: {
            resolveActiveMission: async () => null,
            resolve: async () => ({ action: 'intelligence' }),
            clearActiveMission: async () => {},
          },
        },
        acquisitionMissionRuntime: runtime,
      });

      const opened = await workspace.open({ tenantId: '10', missionId: mission.id });

      await workspace.ask({
        sessionId: opened.sessionId,
        question: 'What should we focus on today?',
        context: { tenantId: '10', missionId: mission.id },
      });

      const result = await workspace.ask({
        sessionId: opened.sessionId,
        question: 'What did you think I was asking?',
        context: { tenantId: '10', missionId: mission.id },
      });

      assert.equal(result.conversationIntent.intent, THINKING_MODES.REFLECT);
      assert.equal(result.workspaceOwnership.owner, WORKSPACE_OWNERS.REFLECTION);
      assert.match(result.prose, /classified|interpret|previous|strategy|focus/i);
      assert.doesNotMatch(result.prose, /Harbor Law|Approve prioritization/i);

      const after = engine.get(mission.id, '10');
      assert.equal(after.version, before.version);
      assert.equal(after.stage, before.stage);
    });

    it('EXPLAIN on active mission still uses mission inspection (SPEC-146 preserved)', async () => {
      await seedMissionAwaitingPrioritization();

      const workspace = createWorkspaceEngine({
        missionsEnabled: true,
        resolverEnabled: true,
        missionEngine: {
          activeMissionResolver: {
            resolveActiveMission: async () => null,
            resolve: async () => ({ action: 'intelligence' }),
            clearActiveMission: async () => {},
          },
        },
        acquisitionMissionRuntime: runtime,
      });

      const opened = await workspace.open({ tenantId: '10', missionId: mission.id });
      const result = await workspace.ask({
        sessionId: opened.sessionId,
        question: 'Why did Scout stop?',
        context: { tenantId: '10', missionId: mission.id },
      });

      assert.equal(result.conversationIntent.intent, THINKING_MODES.EXPLAIN);
      assert.match(result.prose, /Scout|prioritization|operator/i);
    });
  });

  describe('classifyReflectQuestion', () => {
    it('maps representative questions to reflect kinds', () => {
      assert.equal(
        classifyReflectQuestion('What did you think I was asking?'),
        'intent_misinterpretation'
      );
      assert.equal(classifyReflectQuestion('Why did you answer like that?'), 'answer_style');
      assert.equal(
        classifyReflectQuestion("Why didn't Scout execute?"),
        'scout_not_executed'
      );
    });
  });
});
