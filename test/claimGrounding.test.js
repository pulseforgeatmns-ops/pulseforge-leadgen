'use strict';

/**
 * SPEC-108 — claim-grounding transfer at the shared layer.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  classifyCognitiveMode,
  COGNITIVE_MODES,
} = require('../packages/max/specialistDelegation/CognitiveMode');
const { shouldInvokeSpecialist } = require('../packages/max/specialistDelegation/RetrievalGate');
const { shouldClaimClientIntelligenceTurn } = require('../packages/max/workspace/ClientIntelligenceContext');
const { isOperatorOperatingUpdate } = require('../packages/max/workspace/OperatorOperatingUpdate');
const {
  isClaimChallenge,
  isOperatorClaimCorrection,
  identifyChallengedClaim,
  evaluateClaim,
} = require('../packages/max/workspace/RecommendationClaimChallenge');
const {
  composeEvidenceGroundedRecommendation,
} = require('../packages/max/workspace/OperatingStateRecommendation');
const {
  SUPPORT,
  VERDICT,
  TOPICS,
  evaluateOperatingStateClaim,
} = require('../packages/max/workspace/ClaimGrounding');
const training = require('../packages/max/training');

const CHALLENGES = [
  "That isn't true.",
  "That's incorrect.",
  "That's not true.",
  'You said the follow-up already occurred. That isn\'t true.',
  'You said outreach has begun. That isn\'t true.',
  'You said we are expanding our commercial business. That isn\'t true.',
];

describe('SPEC-108 shared classification', () => {
  it('routes generic claim challenges away from inventory, CIE, specialists, and SPEC-106', () => {
    for (const question of CHALLENGES) {
      const mode = classifyCognitiveMode(question);
      assert.equal(isClaimChallenge(question), true, question);
      assert.equal(isOperatorClaimCorrection(question), false, question);
      assert.equal(isOperatorOperatingUpdate(question), false, question);
      assert.equal(mode.kind, COGNITIVE_MODES.EXPLANATION, question);
      assert.equal(mode.via, 'claim_challenge', question);
      assert.equal(shouldInvokeSpecialist(question), false, question);
      assert.equal(
        shouldClaimClientIntelligenceTurn(question, null, { approvedBlueprint: true }),
        false,
        question
      );
    }
  });
});

describe('SPEC-108 shared claim evaluation', () => {
  it('Scenario 1 — unsupported current email is retracted', () => {
    const evaluation = evaluateOperatingStateClaim(
      { topic: TOPICS.EMAIL_MOTION, text: 'An outbound email motion is already active.' },
      { emailMotion: { kind: 'disabled', current: false } }
    );
    assert.equal(evaluation.support, SUPPORT.UNSUPPORTED);
    assert.equal(evaluation.verdict, VERDICT.RETRACT);
  });

  it('Scenario 2 — planned is not completed', () => {
    const evaluation = evaluateClaim(
      { topic: TOPICS.FOLLOW_UP, text: 'Follow-up occurred.' },
      { campaignName: 'Campaign 001', followUp: { kind: 'planned_future', expectedAt: '2026-08-18', executed: false } }
    );
    assert.equal(evaluation.support, SUPPORT.UNSUPPORTED);
    assert.equal(evaluation.verdict, VERDICT.RETRACT);
    assert.match(evaluation.distinction, /planned work as completed/i);
  });

  it('Scenario 3 — inventory is not outreach', () => {
    const evaluation = evaluateOperatingStateClaim(
      { topic: TOPICS.OUTREACH_BEGUN, text: 'Outreach has begun.' },
      { prospects: { total: 67, qualified: 40 }, scout: { matched: 67 }, emailMotion: { current: false } }
    );
    assert.equal(evaluation.support, SUPPORT.UNSUPPORTED);
    assert.equal(evaluation.verdict, VERDICT.RETRACT);
    assert.match(evaluation.distinction, /inventory as execution/i);
  });

  it('Scenario 4 — a Blueprint goal is not observed expansion', () => {
    const evaluation = evaluateOperatingStateClaim(
      { topic: TOPICS.COMMERCIAL_EXPANSION, text: 'You are expanding your commercial business.' },
      {
        objectives: [{ title: 'Acquire twenty commercial clients' }],
        outcomes: { jobs: 0, payments: 0 },
      }
    );
    assert.equal(evaluation.support, SUPPORT.PARTIALLY_SUPPORTED);
    assert.equal(evaluation.verdict, VERDICT.QUALIFY);
    assert.match(evaluation.detail, /stated objective/i);
    assert.match(evaluation.detail, /Acquire twenty commercial clients/i);
  });

  it('Scenario 5 — campaign complete with delivery evidence stays supported', () => {
    const evaluation = evaluateOperatingStateClaim(
      { topic: TOPICS.MAIL, text: 'Campaign 001 was mailed.' },
      {
        campaignName: 'Campaign 001',
        mailExecuted: true,
        activity: {
          touchpoints: [{ channel: 'mail', action_type: 'delivery_log', status: 'delivered' }],
          activity: [],
        },
      }
    );
    assert.equal(evaluation.support, SUPPORT.SUPPORTED);
    assert.equal(evaluation.verdict, VERDICT.CONFIRM);
    assert.match(evaluation.detail, /mailed/i);
  });

  it('does not default a named follow-up challenge to email', () => {
    const claim = identifyChallengedClaim("You said the follow-up already occurred. That isn't true.", {
      premises: [
        { id: 'email_motion', topic: 'email_motion', text: 'An outbound email motion is already active.' },
        { id: 'follow_up', topic: 'follow_up', text: 'Follow-up is planned.' },
      ],
    });
    assert.equal(claim.topic, TOPICS.FOLLOW_UP);
  });

  it('recommendations may not depend on unsupported outreach or expansion claims', () => {
    const composed = composeEvidenceGroundedRecommendation(
      {
        prospects: { counts: { total: 67, qualified: 40 } },
        scout: { intelligence: { counts: { matched: 67 } } },
        activity: { touchpoints: [], activity: [] },
        missions: [],
        objectives: [{ title: 'Acquire twenty commercial clients' }],
        capability: { available: true, enabled_agents: ['scout'], autosend_enabled: false },
      },
      { now: new Date('2026-08-17T15:00:00.000Z') }
    );
    assert.doesNotMatch(composed.prose, /outreach has begun/i);
    assert.doesNotMatch(composed.prose, /you are expanding your commercial business/i);
    const outreach = composed.premises.find((p) => p.topic === TOPICS.OUTREACH_BEGUN);
    const expansion = composed.premises.find((p) => p.topic === TOPICS.COMMERCIAL_EXPANSION);
    assert.equal(outreach.support, SUPPORT.UNSUPPORTED);
    assert.equal(expansion.support, SUPPORT.PARTIALLY_SUPPORTED);
    assert.notEqual(composed.decision.bottleneck, 'outcomes');
  });
});

describe('SPEC-108 competency registry', () => {
  it('registers claim_grounding as a graduated competency', () => {
    const competency = training.getCompetency('claim_grounding');
    assert.ok(competency);
    assert.equal(competency.stage, training.STAGES.GRADUATED);
    assert.ok(competency.specRefs.includes('SPEC-108'));
    assert.ok(competency.regressionTests.some((p) => p.includes('claimGrounding.test.js')));
  });
});
