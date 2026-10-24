'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  createObserveReaction,
  createObserveReactionReevaluation,
  buildObserveReactionId,
  buildObserveReactionReevaluationId,
  listEffectiveObserveReactions,
  pickEffectiveObserveReaction,
  EVALUATION_KINDS,
} = require('../packages/acquisition-mission/ObserveReaction');
const { evaluateObserveReaction } = require('../packages/acquisition-mission/ObserveEvaluator');
const { OBSERVATION_KINDS } = require('../packages/acquisition-mission/CommunicationObservation');

function communicationObservation(overrides = {}) {
  return {
    id: overrides.id || 'obs_human_open_1',
    missionId: 'mission_backus',
    tenantId: '10',
    prospectId: 'prospect_backus',
    kind: OBSERVATION_KINDS.COMMUNICATION_EVIDENCE,
    category: 'engagement',
    eventType: 'opened',
    occurredAt: '2026-09-14T12:25:57.000Z',
    at: '2026-09-14T12:25:57.000Z',
    specialist: 'emmett',
    observation: 'human open',
    ...overrides,
  };
}

describe('SPEC-251 observe reaction cadence re-evaluation supersession', () => {
  it('builds deterministic re-evaluation ids from observation + trigger', () => {
    const id = buildObserveReactionReevaluationId('obs_1', 'cadence_ann_abc');
    assert.match(id, /^obsrx_obs_1_reeval_[a-f0-9]{16}$/);
    assert.equal(id, buildObserveReactionReevaluationId('obs_1', 'cadence_ann_abc'));
  });

  it('createObserveReactionReevaluation links initial reaction without mutating id shape', () => {
    const initial = createObserveReaction({
      observationId: 'obs_1',
      missionId: 'mission_1',
      tenantId: '10',
      prospectId: 'p1',
      evidenceType: 'human_open',
      evidenceStrength: 'engagement',
      updatedDisposition: 'seen',
      recommendedNextAction: 'wait',
      recommendedTiming: {
        kind: 'unresolved',
        waitDays: null,
        cadenceSource: 'unresolved',
      },
    });
    assert.equal(initial.id, buildObserveReactionId('obs_1'));
    assert.equal(initial.evaluationKind, EVALUATION_KINDS.INITIAL);

    const reeval = createObserveReactionReevaluation({
      ...initial,
      reevaluationTriggerId: 'cadence_ann_test',
      reevaluationTriggerKind: 'historical_cadence_annotation',
      recommendedTiming: {
        kind: 'wait_until',
        waitDays: 4,
        dueAt: '2026-09-18T12:25:57.000Z',
        cadenceSource: 'prepared_sequence',
        cadenceProvenance: 'historical_annotation',
        reconstructed: true,
      },
      cadenceSource: 'prepared_sequence',
    });

    assert.notEqual(reeval.id, initial.id);
    assert.equal(reeval.evaluationKind, EVALUATION_KINDS.CADENCE_REEVALUATION);
    assert.equal(reeval.supersedesReactionId, initial.id);
    assert.equal(reeval.recommendedTiming.waitDays, 4);
    assert.equal(reeval.recommendedTiming.kind, 'wait_until');
  });

  it('listEffectiveObserveReactions prefers re-evaluation over initial per observation', () => {
    const initial = createObserveReaction({
      observationId: 'obs_1',
      missionId: 'mission_1',
      tenantId: '10',
      evidenceType: 'human_open',
      evidenceStrength: 'engagement',
      updatedDisposition: 'seen',
      recommendedNextAction: 'wait',
      recommendedTiming: { kind: 'unresolved', waitDays: null, cadenceSource: 'unresolved' },
      at: '2026-09-14T12:25:57.000Z',
    });
    const reeval = createObserveReactionReevaluation({
      ...initial,
      reevaluationTriggerId: 'cadence_ann_test',
      recommendedTiming: {
        kind: 'wait_until',
        waitDays: 4,
        dueAt: '2026-09-18T12:25:57.000Z',
        cadenceSource: 'prepared_sequence',
      },
      cadenceSource: 'prepared_sequence',
      at: '2026-09-14T12:25:57.000Z',
    });

    const effective = listEffectiveObserveReactions([initial, reeval], 'mission_1');
    assert.equal(effective.length, 1);
    assert.equal(effective[0].id, reeval.id);
    assert.equal(effective[0].recommendedTiming.waitDays, 4);
  });

  it('evaluateObserveReaction resolves waitDays=4 from historical preparedCadence', () => {
    const observation = communicationObservation();
    const evaluated = evaluateObserveReaction({
      mission: { id: 'mission_backus', tenantId: '10' },
      observation,
      priorState: { disposition: 'reached', sequenceStepSent: 0 },
      store: {},
      outcomes: [],
      preparedCadence: {
        steps: [{ step: 0, day: 0 }, { step: 1, day: 4 }, { step: 2, day: 8 }],
        cadenceSource: 'prepared_sequence',
        cadenceProvenance: 'historical_annotation',
        reconstructed: true,
      },
      now: new Date('2026-09-14T12:26:00.000Z'),
    });

    assert.equal(evaluated.reaction.recommendedTiming.kind, 'wait_until');
    assert.equal(evaluated.reaction.recommendedTiming.waitDays, 4);
    assert.equal(evaluated.reaction.recommendedTiming.cadenceSource, 'prepared_sequence');
    assert.equal(evaluated.candidateState.recommendedTiming.waitDays, 4);
  });

  it('pickEffectiveObserveReaction keeps both rows but selects re-eval for reads', () => {
    const rows = [
      {
        id: 'obsrx_obs_a',
        observationId: 'obs_a',
        evaluationSequence: 0,
        at: '2026-09-14T10:00:00.000Z',
        recommendedTiming: { kind: 'unresolved' },
      },
      {
        id: 'obsrx_obs_a_reeval_x',
        observationId: 'obs_a',
        evaluationSequence: 1,
        at: '2026-09-14T10:00:00.000Z',
        recommendedTiming: { kind: 'wait_until', waitDays: 4 },
      },
    ];
    assert.equal(pickEffectiveObserveReaction(rows).id, 'obsrx_obs_a_reeval_x');
  });
});
