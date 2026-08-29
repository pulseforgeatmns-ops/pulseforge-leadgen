'use strict';

/**
 * SPEC-209 — Typo-Tolerant Mission Continuation.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const {
  STAGES,
  EXECUTION_INTENTS,
  OPERATOR_DECISION_KINDS,
  resolveMissionContinuation,
} = amo;
const {
  classifyOperatorCognition,
  THINKING_MODES,
} = require('../../operatorCognition');
const { analyzeOperatorIntent } = require('../OperatorIntent');
const {
  normalizeConversationalControlLanguage,
  normalizePendingDecisionTypos,
  resolveContinuationControlTypo,
} = require('../BoundedTypoNormalization');
const {
  resolvePendingOperatorDecision,
} = require('../PendingDecisionResolver');
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  advancePrioritizationAfterApproval,
  advanceMaxPrioritization,
} = require('../AmoOperatorApproval');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

async function seedUnderstandMaxComplete(engine, mission) {
  await advancePlanAfterApproval({
    engine,
    mission,
    tenantId: '10',
    question: 'Approved.',
  });
  await advanceDiscoveryAfterApproval({
    engine,
    mission: engine.get(mission.id, '10'),
    tenantId: '10',
    question: 'Approved. Begin Discovery.',
    allowFixtureFallback: true,
  });
  await advancePrioritizationAfterApproval({
    engine,
    mission: engine.get(mission.id, '10'),
    tenantId: '10',
    question: 'Approved prioritization.',
  });
  await advanceMaxPrioritization({
    engine,
    mission: engine.get(mission.id, '10'),
    tenantId: '10',
    allowFixtureFallback: true,
  });
  return engine.inspect(mission.id, { tenantId: '10' });
}

describe('SPEC-209 — Typo-Tolerant Mission Continuation', () => {
  describe('BoundedTypoNormalization — continuation control vocabulary', () => {
    const shouldNormalize = ['continuee', 'contine', 'continur'];
    const shouldNotNormalize = ['continuous', 'continued', 'continuation', 'container'];

    for (const token of shouldNormalize) {
      it(`normalizes "${token}" to continue`, () => {
        assert.equal(resolveContinuationControlTypo(token), 'continue');
        assert.equal(normalizeConversationalControlLanguage(token), 'continue');
      });
    }

    for (const token of shouldNotNormalize) {
      it(`does not normalize "${token}" to continue`, () => {
        assert.equal(resolveContinuationControlTypo(token), null);
        assert.equal(normalizeConversationalControlLanguage(token), token);
      });
    }

    it('strips trailing punctuation before normalization', () => {
      assert.equal(normalizeConversationalControlLanguage('Continue.'), 'continue');
      assert.equal(normalizeConversationalControlLanguage('continuee!'), 'continue');
    });

    it('normalizes continuation typos inside short multi-word phrases', () => {
      assert.equal(
        normalizeConversationalControlLanguage('continuee this mission'),
        'continue this mission'
      );
    });
  });

  describe('classifyOperatorCognition — regression matrix', () => {
    let snapshot;

    beforeEach(async () => {
      const engine = amo.createAcquisitionMissionEngine();
      const mission = engine.create({
        tenantId: '10',
        objective: OBJECTIVE,
        targetSegment: 'Law Firms',
      });
      snapshot = await seedUnderstandMaxComplete(engine, mission);
    });

    const continuationCases = [
      'continue',
      'Continue.',
      'continuee',
      'contine',
      'continue this mission',
    ];

    for (const utterance of continuationCases) {
      it(`"${utterance}" resolves to mission continuation`, () => {
        const intent = classifyOperatorCognition(utterance, {
          mission: snapshot.mission,
          snapshot,
        });

        assert.equal(intent.intent, THINKING_MODES.EXECUTE);
        assert.equal(intent.via, 'mission_continuation');
        assert.equal(
          intent.missionContinuation.intent,
          EXECUTION_INTENTS.GENERATE_VARIANTS
        );
      });
    }

    it('no active mission + continuee stays read-only without arbitrary execution', () => {
      const intent = classifyOperatorCognition('continuee');
      assert.equal(intent.intent, THINKING_MODES.INSPECT);
      assert.equal(intent.via, 'conversational_continue');
      assert.equal(intent.missionContinuation, null);
    });

    it('two eligible actions + continuee returns mission-grounded clarification', () => {
      const ambiguousSnapshot = {
        mission: {
          id: 'm-ambiguous',
          stage: STAGES.DISCOVER,
          planCancelled: false,
          pendingOperatorDecision: null,
          blockers: [{ kind: 'paused', reason: 'Blocked for test', label: 'Blocked' }],
        },
        contributions: [],
      };

      assert.equal(resolveMissionContinuation(ambiguousSnapshot).kind, 'ambiguous');

      const intent = classifyOperatorCognition('continuee', {
        mission: ambiguousSnapshot.mission,
        snapshot: ambiguousSnapshot,
      });

      assert.equal(intent.intent, THINKING_MODES.INSPECT);
      assert.equal(intent.via, 'mission_continuation_ambiguous');
      assert.ok(intent.missionContinuationAmbiguity);
    });
  });

  describe('SPEC-202 preservation — pending decision typos', () => {
    const mission = {
      id: 'm-discovery',
      stage: STAGES.DISCOVER,
      structuredMissionApproved: true,
      pendingOperatorDecision: {
        kind: OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL,
        prompt: 'Approve discovery?',
      },
    };

    it('still normalizes apprroved for pending decisions', () => {
      const resolution = resolvePendingOperatorDecision('apprroved', mission);
      assert.equal(resolution.resolved, true);
      assert.equal(resolution.action, 'approve_discovery');
    });

    it('still normalizes continuee for pending investigation continuation', () => {
      const investigationMission = {
        id: 'm-investigation',
        pendingOperatorDecision: {
          kind: OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION,
          prompt: 'Continue investigation?',
        },
      };
      const resolution = resolvePendingOperatorDecision('continuee', investigationMission);
      assert.equal(resolution.resolved, true);
      assert.equal(resolution.action, 'continue_investigation');
    });

    it('pending typo primitive remains separate from conversational control guard', () => {
      assert.equal(normalizePendingDecisionTypos('continuee'), 'continue');
      assert.equal(normalizeConversationalControlLanguage('continued'), 'continued');
    });
  });

  describe('analyzeOperatorIntent — end-to-end typo continuation', () => {
    it('continuee requests GENERATE_VARIANTS when one eligible progression exists', async () => {
      const engine = amo.createAcquisitionMissionEngine();
      const mission = engine.create({
        tenantId: '10',
        objective: OBJECTIVE,
        targetSegment: 'Law Firms',
      });
      const snapshot = await seedUnderstandMaxComplete(engine, mission);

      const intent = await analyzeOperatorIntent({
        question: 'continuee',
        mission: snapshot.mission,
        snapshot,
        resolveMission: false,
      });

      assert.equal(intent.executionRequested, true);
      assert.equal(intent.conversationIntent.via, 'mission_continuation');
      assert.equal(
        intent.conversationIntent.missionContinuation.intent,
        EXECUTION_INTENTS.GENERATE_VARIANTS
      );
    });

    it('continuee without mission context does not request execution', async () => {
      const intent = await analyzeOperatorIntent({
        question: 'continuee',
        resolveMission: false,
        session: { id: 'spec-209-none', context: {} },
      });

      assert.equal(intent.executionRequested, false);
      assert.equal(intent.conversationIntent.via, 'conversational_continue');
    });
  });
});
