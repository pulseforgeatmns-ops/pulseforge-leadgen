'use strict';

/**
 * Canonical OBSERVE → LEARN progression (post AUDIT-074).
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const { createAcquisitionMissionEngine } = require('../Engine');
const { STAGES, SPECIALISTS, EVENT_KINDS } = require('../types');
const { canEnter, specialistContext } = require('../Lifecycle');
const {
  hasMeaningfulBusinessOutcome,
  isMeaningfulBusinessOutcomeRow,
} = require('../ObservationInterpretation');
const { shouldProgressToLearn, tryProgressToLearn } = require('../LearnProgression');
const { createEvent } = require('../Timeline');
const { PREDICTION_STATUS } = require('../OutcomeLearning');

function engine() {
  return createAcquisitionMissionEngine();
}

function observeMission(amoEngine, overrides = {}) {
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
    stage: STAGES.OBSERVE,
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
  amoEngine.store.addEvent(createEvent({
    missionId,
    kind: EVENT_KINDS.LAUNCHED,
    specialist: SPECIALISTS.EMMETT,
    label: 'Campaign launched',
  }));
  return amoEngine.get(missionId, '10');
}

describe('meaningful business outcome predicate', () => {
  it('rejects transport-only outcome rows', () => {
    for (const type of ['queued', 'sent', 'open']) {
      assert.equal(isMeaningfulBusinessOutcomeRow({ type }), false);
    }
    assert.equal(hasMeaningfulBusinessOutcome([
      { type: 'sent' },
      { type: 'open' },
      { type: 'queued' },
    ]), false);
  });

  it('rejects generic reply without interpretation provenance', () => {
    assert.equal(isMeaningfulBusinessOutcomeRow({ type: 'reply' }), false);
    assert.equal(isMeaningfulBusinessOutcomeRow({
      type: 'reply',
      payload: { source: 'legacy_webhook' },
    }), false);
  });

  it('accepts interpreted reply provenance', () => {
    assert.equal(isMeaningfulBusinessOutcomeRow({
      type: 'reply',
      payload: {
        interpretationId: 'interp_1',
        source: 'riley_reply_interpretation',
      },
    }), true);
  });

  it('accepts canonical business outcome types', () => {
    for (const type of [
      'interested',
      'not_now',
      'not_interested',
      'unsubscribe',
      'walkthrough_booked',
      'walkthrough_requested',
    ]) {
      assert.equal(isMeaningfulBusinessOutcomeRow({ type }), true, type);
    }
  });
});

describe('LEARN gate semantics', () => {
  it('blocks LEARN for transport-only outcomes in OBSERVE', () => {
    const amoEngine = engine();
    const mission = observeMission(amoEngine);
    amoEngine.recordOutcome(mission.id, { type: 'sent' });
    amoEngine.recordOutcome(mission.id, { type: 'open' });
    amoEngine.recordOutcome(mission.id, { type: 'reply' });

    const outcomes = amoEngine.store.listOutcomes(mission.id);
    const extra = {
      hasOutcomes: outcomes.length > 0,
      hasMeaningfulBusinessOutcome: hasMeaningfulBusinessOutcome(outcomes),
    };
    const ctx = specialistContext(amoEngine.store.listContributions(mission.id), extra);
    const gate = canEnter(STAGES.LEARN, { ...ctx, ...extra });

    assert.equal(gate.ok, false);
    assert.match(gate.reason, /Meaningful business outcomes/i);
    assert.equal(amoEngine.get(mission.id, '10').stage, STAGES.OBSERVE);
  });

  it('allows LEARN when meaningful business outcome exists', () => {
    const amoEngine = engine();
    const mission = observeMission(amoEngine);
    amoEngine.recordOutcome(mission.id, {
      type: 'interested',
      payload: { source: 'riley_reply_interpretation', interpretationId: 'interp_1' },
    });

    const outcomes = amoEngine.store.listOutcomes(mission.id);
    const extra = {
      hasOutcomes: true,
      hasMeaningfulBusinessOutcome: hasMeaningfulBusinessOutcome(outcomes),
    };
    const ctx = specialistContext([], extra);
    assert.equal(canEnter(STAGES.LEARN, { ...ctx, ...extra }).ok, true);
  });
});

describe('OBSERVE → LEARN auto-progression', () => {
  it('remains OBSERVE for transport-only evidence', () => {
    const amoEngine = engine();
    const mission = observeMission(amoEngine);
    amoEngine.recordOutcome(mission.id, { type: 'sent' });
    amoEngine.recordOutcome(mission.id, { type: 'open' });
    assert.equal(amoEngine.get(mission.id, '10').stage, STAGES.OBSERVE);
    assert.equal(shouldProgressToLearn(amoEngine.get(mission.id, '10'), amoEngine.store), false);
  });

  it('auto-progresses to LEARN for intermediate interested outcome', () => {
    const amoEngine = engine();
    const mission = observeMission(amoEngine);
    amoEngine.applyRileyReplyInterpretation({
      missionId: mission.id,
      prospectId: 'co-harbor',
      classification: 'interested',
      observationId: 'obs-1',
    });
    assert.equal(amoEngine.get(mission.id, '10').stage, STAGES.LEARN);
  });

  it('auto-progresses to LEARN for negative not_interested outcome', () => {
    const amoEngine = engine();
    const mission = observeMission(amoEngine);
    amoEngine.applyRileyReplyInterpretation({
      missionId: mission.id,
      prospectId: 'co-harbor',
      classification: 'negative',
    });
    assert.equal(amoEngine.get(mission.id, '10').stage, STAGES.LEARN);
  });

  it('auto-progresses to LEARN for terminal walkthrough_booked', () => {
    const amoEngine = engine();
    const mission = observeMission(amoEngine);
    amoEngine.applyBookingInterpretation({
      missionId: mission.id,
      prospectId: 'co-harbor',
      bookingRef: 'cal-event-1',
    });
    assert.equal(amoEngine.get(mission.id, '10').stage, STAGES.LEARN);
  });

  it('uses Max-owned progress path with lifecycle validation', () => {
    const amoEngine = engine();
    const mission = observeMission(amoEngine);
    const result = tryProgressToLearn(amoEngine, mission.id, { tenantId: '10' });
    assert.equal(result.progressed, false);

    amoEngine.recordOutcome(mission.id, { type: 'not_now', payload: { source: 'riley_reply_interpretation', interpretationId: 'i1' } });
    const updated = amoEngine.get(mission.id, '10');
    assert.equal(updated.stage, STAGES.LEARN);

    const transitions = amoEngine.store.listEvents(mission.id)
      .filter((row) => row.kind === EVENT_KINDS.STAGE_TRANSITION);
    const learnTransition = transitions.find((row) => row.payload && row.payload.to === STAGES.LEARN);
    assert.ok(learnTransition);
    assert.equal(learnTransition.specialist, SPECIALISTS.MAX);
    assert.equal(learnTransition.payload.from, STAGES.OBSERVE);
  });

  it('preserves OutcomeLearning on terminal outcomes while advancing to LEARN', () => {
    const amoEngine = engine();
    const mission = observeMission(amoEngine);
    amoEngine.capturePrediction(mission.id, {
      recommendation: { summary: 'Prioritize ABC Law', confidence: 0.72 },
      expectedOutcome: { kind: 'walkthrough', label: 'Walkthrough booked', probability: 0.72 },
    });

    amoEngine.recordOutcome(mission.id, { type: 'walkthrough_booked', at: '2026-08-25T14:00:00.000Z' });

    const snapshot = amoEngine.inspect(mission.id);
    assert.equal(snapshot.mission.stage, STAGES.LEARN);
    assert.equal(snapshot.outcomeLearning.predictions.resolved, 1);
    const predictions = amoEngine.store.listPredictions(mission.id);
    assert.equal(predictions[0].status, PREDICTION_STATUS.RESOLVED);
  });
});
