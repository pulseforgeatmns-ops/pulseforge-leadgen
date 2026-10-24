'use strict';

/**
 * Canonical LEARN → IMPROVE progression (post AUDIT-075).
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { createAcquisitionMissionEngine } = require('../Engine');
const { STAGES, SPECIALISTS, EVENT_KINDS } = require('../types');
const { canEnter, specialistContext } = require('../Lifecycle');
const {
  hasMeaningfulLearning,
  isMeaningfulOutcomeLearningRow,
  isMeaningfulSegmentLearningRow,
} = require('../MeaningfulLearning');
const { shouldProgressToImprove, tryProgressToImprove } = require('../ImproveProgression');
const { createEvent } = require('../Timeline');
const {
  ACCURACY_LABELS,
  LEARNING_OBJECT_KINDS,
  PREDICTION_STATUS,
} = require('../OutcomeLearning');

function engine() {
  return createAcquisitionMissionEngine();
}

function learnMission(amoEngine, overrides = {}) {
  const mission = amoEngine.create({
    tenantId: '10',
    objective: 'Acquire commercial cleaning customers in Manchester NH for law firms.',
    targetSegment: 'Law Firms',
  });
  const missionId = overrides.id || mission.id;
  amoEngine.store.putMission({
    ...mission,
    ...overrides,
    id: missionId,
    stage: STAGES.LEARN,
    pendingOperatorDecision: null,
    executionSummary: overrides.executionSummary || {
      total: 1,
      sent: 1,
      failed: 0,
      blocked: 0,
      queued: 0,
      attempted: 0,
      complete: true,
    },
  });
  amoEngine.store.addOutcome({
    id: 'out-interested',
    missionId,
    tenantId: '10',
    type: 'interested',
    at: '2026-08-25T12:00:00.000Z',
    payload: { source: 'riley_reply_interpretation', interpretationId: 'interp_1' },
  });
  return amoEngine.get(missionId, '10');
}

describe('meaningful learning predicates', () => {
  it('accepts mission-bound OutcomeLearning with substantive statement', () => {
    assert.equal(isMeaningfulOutcomeLearningRow({
      missionId: 'm1',
      kind: LEARNING_OBJECT_KINDS.STRATEGY,
      statement: 'Vendor stability heuristic over-weighted for this segment.',
      evaluationId: 'eval_1',
    }, new Map([['eval_1', { id: 'eval_1', accuracy: ACCURACY_LABELS.INCORRECT }]])), true);
  });

  it('rejects OutcomeLearning without mission binding', () => {
    assert.equal(isMeaningfulOutcomeLearningRow({
      kind: LEARNING_OBJECT_KINDS.STRATEGY,
      statement: 'Some lesson',
    }), false);
  });

  it('rejects empty OutcomeLearning bookkeeping rows', () => {
    assert.equal(isMeaningfulOutcomeLearningRow({
      missionId: 'm1',
      kind: LEARNING_OBJECT_KINDS.STRATEGY,
      statement: '',
    }), false);
  });

  it('rejects OutcomeLearning tied to inconclusive evaluation', () => {
    assert.equal(isMeaningfulOutcomeLearningRow({
      missionId: 'm1',
      kind: LEARNING_OBJECT_KINDS.STRATEGY,
      statement: 'Insufficient signal to evaluate prediction accuracy.',
      evaluationId: 'eval_inc',
    }, new Map([['eval_inc', { id: 'eval_inc', accuracy: ACCURACY_LABELS.INCONCLUSIVE }]])), false);
  });

  it('allows autoApplied false OutcomeLearning', () => {
    assert.equal(isMeaningfulOutcomeLearningRow({
      missionId: 'm1',
      kind: LEARNING_OBJECT_KINDS.OPPORTUNITY_RULE,
      statement: 'Opportunity prediction accuracy improved.',
      autoApplied: false,
    }), true);
  });

  it('accepts meaningful segment learning with statement', () => {
    assert.equal(isMeaningfulSegmentLearningRow({
      missionId: 'm1',
      segment: 'law_firm',
      sends: 0,
      replies: 0,
      statement: 'Commercial firms responding better to operational messaging.',
    }, 'm1'), true);
  });

  it('rejects empty segment learning rows', () => {
    assert.equal(isMeaningfulSegmentLearningRow({
      missionId: 'm1',
      segment: 'law_firm',
      sends: 0,
      replies: 0,
      statement: null,
    }, 'm1'), false);
  });
});

describe('IMPROVE gate semantics', () => {
  it('blocks IMPROVE without meaningful learning in LEARN', () => {
    const amoEngine = engine();
    const mission = learnMission(amoEngine);
    const extra = { hasMeaningfulLearning: false, hasLearning: false };
    const ctx = specialistContext([], extra);
    const gate = canEnter(STAGES.IMPROVE, { ...ctx, ...extra });
    assert.equal(gate.ok, false);
    assert.match(gate.reason, /Meaningful learning/i);
    assert.equal(amoEngine.get(mission.id, '10').stage, STAGES.LEARN);
  });

  it('allows IMPROVE when meaningful OutcomeLearning exists', () => {
    const amoEngine = engine();
    const mission = learnMission(amoEngine);
    amoEngine.store.addOutcomeLearning({
      id: 'olearn_1',
      tenantId: '10',
      missionId: mission.id,
      kind: LEARNING_OBJECT_KINDS.STRATEGY,
      statement: 'Buying signals overestimated for law firm segment.',
      autoApplied: false,
    });
    const extra = { hasMeaningfulLearning: true, hasLearning: true };
    const ctx = specialistContext([], extra);
    assert.equal(canEnter(STAGES.IMPROVE, { ...ctx, ...extra }).ok, true);
    assert.equal(hasMeaningfulLearning(amoEngine.store, mission), true);
  });
});

describe('LEARN → IMPROVE auto-progression', () => {
  it('auto-progresses to IMPROVE for meaningful OutcomeLearning alone', () => {
    const amoEngine = engine();
    const mission = learnMission(amoEngine);
    amoEngine.store.addOutcomeLearning({
      id: 'olearn_1',
      tenantId: '10',
      missionId: mission.id,
      kind: LEARNING_OBJECT_KINDS.OPPORTUNITY_RULE,
      statement: 'Opportunity prediction accuracy improved for ABC Law.',
      autoApplied: false,
    });

    const result = tryProgressToImprove(amoEngine, mission.id, { tenantId: '10' });
    assert.equal(result.progressed, true);
    assert.equal(result.mission.stage, STAGES.IMPROVE);

    const transitions = amoEngine.store.listEvents(mission.id)
      .filter((row) => row.kind === EVENT_KINDS.STAGE_TRANSITION);
    const improveTransition = transitions.find((row) => row.payload && row.payload.to === STAGES.IMPROVE);
    assert.ok(improveTransition);
    assert.equal(improveTransition.specialist, SPECIALISTS.MAX);
    assert.equal(improveTransition.payload.from, STAGES.LEARN);
  });

  it('auto-progresses via recordOutcome chain: OutcomeLearning → LEARN → IMPROVE', () => {
    const amoEngine = engine();
    const mission = amoEngine.create({
      tenantId: '10',
      objective: 'Acquire law firm cleaning contracts',
      targetSegment: 'law_firm',
    });
    amoEngine.store.putMission({
      ...mission,
      stage: STAGES.OBSERVE,
      pendingOperatorDecision: null,
      executionSummary: { total: 1, sent: 1, complete: true },
    });
    amoEngine.store.addEvent(createEvent({
      missionId: mission.id,
      kind: EVENT_KINDS.LAUNCHED,
      specialist: SPECIALISTS.EMMETT,
      label: 'Campaign launched',
    }));
    amoEngine.capturePrediction(mission.id, {
      recommendation: { summary: 'Prioritize ABC Law', confidence: 0.72 },
      expectedOutcome: { kind: 'walkthrough', label: 'Walkthrough booked', probability: 0.72 },
      opportunityId: 'opp-abc',
    });

    amoEngine.recordOutcome(mission.id, { type: 'walkthrough_booked', at: '2026-08-25T14:00:00.000Z' });

    const updated = amoEngine.get(mission.id, '10');
    assert.equal(updated.stage, STAGES.IMPROVE);
    const learnings = amoEngine.store.listOutcomeLearnings('10', mission.id);
    assert.ok(learnings.length > 0);
    assert.equal(learnings.every((row) => row.autoApplied === false), true);
  });

  it('supports meaningful segment learning without OutcomeLearning', () => {
    const amoEngine = engine();
    const mission = learnMission(amoEngine);
    amoEngine.recordLearning(mission.id, {
      segment: 'law_firm',
      sends: 18,
      replies: 3,
      statement: 'Operational messaging outperformed relationship hooks.',
    });
    assert.equal(amoEngine.get(mission.id, '10').stage, STAGES.IMPROVE);
  });

  it('remains LEARN when no meaningful learning exists', () => {
    const amoEngine = engine();
    const mission = learnMission(amoEngine);
    assert.equal(shouldProgressToImprove(mission, amoEngine.store), false);
    const snapshot = amoEngine.inspect(mission.id);
    assert.equal(snapshot.lifecycleLearning.awaitingMeaningfulLearning, true);
    assert.equal(snapshot.lifecycleLearning.improveEligible, false);
  });

  it('does not count empty segment learning rows', () => {
    const amoEngine = engine();
    const mission = learnMission(amoEngine);
    amoEngine.store.addLearning({
      id: 'learn_empty',
      tenantId: '10',
      missionId: mission.id,
      segment: 'law_firm',
      sends: 0,
      replies: 0,
      statement: null,
      autoApplied: false,
    });
    assert.equal(hasMeaningfulLearning(amoEngine.store, mission), false);
    assert.equal(amoEngine.get(mission.id, '10').stage, STAGES.LEARN);
  });

  it('does not unlock IMPROVE from inconclusive OutcomeLearning generation', () => {
    const amoEngine = engine();
    const mission = learnMission(amoEngine);
    amoEngine.store.addEvaluation({
      id: 'eval_inc',
      tenantId: '10',
      missionId: mission.id,
      accuracy: ACCURACY_LABELS.INCONCLUSIVE,
    });
    amoEngine.store.addOutcomeLearning({
      id: 'olearn_inc',
      tenantId: '10',
      missionId: mission.id,
      evaluationId: 'eval_inc',
      kind: LEARNING_OBJECT_KINDS.STRATEGY,
      statement: 'Insufficient signal to evaluate prediction accuracy.',
      autoApplied: false,
    });
    assert.equal(hasMeaningfulLearning(amoEngine.store, mission), false);
    assert.equal(amoEngine.get(mission.id, '10').stage, STAGES.LEARN);
  });

  it('evaluates IMPROVE when OutcomeLearning is added after entering LEARN', () => {
    const amoEngine = engine();
    const mission = learnMission(amoEngine);
    assert.equal(amoEngine.get(mission.id, '10').stage, STAGES.LEARN);

    amoEngine.capturePrediction(mission.id, {
      recommendation: { summary: 'Call XYZ Law', confidence: 0.6 },
      expectedOutcome: { kind: 'walkthrough', probability: 0.6 },
    });
    amoEngine.evaluateOutcomeLearning(mission.id, {
      actualOutcome: 'not_interested',
      primaryCause: 'Buying signals overestimated',
      lesson: 'Vendor stability heuristic over-weighted.',
    });

    assert.equal(amoEngine.get(mission.id, '10').stage, STAGES.IMPROVE);
  });
});

describe('tenant isolation for learning eligibility', () => {
  it('does not let mission A learning unlock mission B IMPROVE', () => {
    const amoEngine = engine();
    const missionA = learnMission(amoEngine, { id: 'mission_a' });
    const missionB = learnMission(amoEngine, { id: 'mission_b' });

    amoEngine.store.addOutcomeLearning({
      id: 'olearn_a',
      tenantId: '10',
      missionId: missionA.id,
      kind: LEARNING_OBJECT_KINDS.STRATEGY,
      statement: 'Mission A learned something substantive.',
      autoApplied: false,
    });

    assert.equal(hasMeaningfulLearning(amoEngine.store, missionA), true);
    assert.equal(hasMeaningfulLearning(amoEngine.store, missionB), false);
    assert.equal(amoEngine.get(missionB.id, '10').stage, STAGES.LEARN);
  });

  it('ignores tenant-wide segment learning without mission binding', () => {
    const amoEngine = engine();
    const mission = learnMission(amoEngine);
    amoEngine.store.addLearning({
      id: 'learn_tenant',
      tenantId: '10',
      missionId: null,
      segment: 'law_firm',
      sends: 50,
      replies: 7,
      statement: 'Tenant-wide rollup — not mission-bound.',
      autoApplied: false,
    });
    assert.equal(hasMeaningfulLearning(amoEngine.store, mission), false);
  });
});

describe('regression: OBSERVE → LEARN preserved', () => {
  it('still auto-progresses OBSERVE → LEARN without OutcomeLearning', () => {
    const amoEngine = engine();
    const mission = amoEngine.create({
      tenantId: '10',
      objective: 'Acquire law firm cleaning contracts',
      targetSegment: 'law_firm',
    });
    amoEngine.store.putMission({
      ...mission,
      stage: STAGES.OBSERVE,
      pendingOperatorDecision: null,
      executionSummary: { total: 1, sent: 1, complete: true },
    });
    amoEngine.store.addEvent(createEvent({
      missionId: mission.id,
      kind: EVENT_KINDS.LAUNCHED,
      specialist: SPECIALISTS.EMMETT,
      label: 'Campaign launched',
    }));

    amoEngine.recordOutcome(mission.id, {
      type: 'not_now',
      payload: { source: 'riley_reply_interpretation', interpretationId: 'i1' },
    });

    assert.equal(amoEngine.get(mission.id, '10').stage, STAGES.LEARN);
  });

  it('preserves OutcomeLearning autoApplied false on terminal outcomes', () => {
    const amoEngine = engine();
    const mission = amoEngine.create({
      tenantId: '10',
      objective: 'Acquire law firm cleaning contracts',
      targetSegment: 'law_firm',
    });
    amoEngine.store.putMission({
      ...mission,
      stage: STAGES.OBSERVE,
      pendingOperatorDecision: null,
      executionSummary: { total: 1, sent: 1, complete: true },
    });
    amoEngine.store.addEvent(createEvent({
      missionId: mission.id,
      kind: EVENT_KINDS.LAUNCHED,
      specialist: SPECIALISTS.EMMETT,
      label: 'Campaign launched',
    }));
    amoEngine.capturePrediction(mission.id, {
      recommendation: { summary: 'Prioritize ABC Law', confidence: 0.72 },
      expectedOutcome: { kind: 'walkthrough', label: 'Walkthrough booked', probability: 0.72 },
      opportunityId: 'opp-abc',
    });

    amoEngine.recordOutcome(mission.id, { type: 'walkthrough_booked' });

    const learnings = amoEngine.store.listOutcomeLearnings('10', mission.id);
    assert.ok(learnings.length > 0);
    assert.equal(learnings.every((row) => row.autoApplied === false), true);
    assert.equal(amoEngine.store.listPredictions(mission.id)[0].status, PREDICTION_STATUS.RESOLVED);
  });
});
