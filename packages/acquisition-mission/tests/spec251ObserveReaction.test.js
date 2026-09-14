'use strict';

/**
 * SPEC-251 — Max OBSERVE Reaction Policy (first slice).
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../index');
const {
  STAGES,
  SPECIALISTS,
  EVENT_KINDS,
  OBSERVATION_KINDS,
  EVIDENCE_TYPES,
  EVIDENCE_STRENGTH,
  DISPOSITIONS,
  NEXT_ACTIONS,
  WORKSPACE_MODES,
  deriveWorkspaceMode,
  evaluateObserveReaction,
  resolveObserveCadence,
  buildObserveReactionId,
  tryProgressToLearn,
} = amo;
const { createEvent } = require('../Timeline');
const { INTERPRETATION_TYPES } = require('../ObservationInterpretation');
const BACKUS = Object.freeze({
  missionId: 'mission_ad7753b0-6def-441d-bb1a-3764656f5750',
  tenantId: '10',
  executionId: 'amo_send_37a03a00-2686-4804-8360-9cf93edb52ba',
  prospectId: '7adbb294-b94c-45c0-85df-e040f027ece0',
});

function communicationObservation(missionId, eventType, overrides = {}) {
  const id = overrides.id || `obs_test_${eventType}_${overrides.suffix || '1'}`;
  return {
    id,
    missionId,
    tenantId: BACKUS.tenantId,
    prospectId: BACKUS.prospectId,
    kind: OBSERVATION_KINDS.COMMUNICATION_EVIDENCE,
    category: eventType === 'sent' || eventType === 'delivered' ? 'delivery' : 'engagement',
    eventType,
    occurredAt: overrides.occurredAt || '2026-09-14T12:25:00.000Z',
    at: overrides.at || overrides.occurredAt || '2026-09-14T12:25:00.000Z',
    specialist: 'emmett',
    observation: `${eventType} observation`,
    evidence: {
      executionRecordId: BACKUS.executionId,
      preparedArtifactRevision: 'rev-backus-capacity-1',
    },
    ...overrides,
  };
}

function setupObserveMission(engine, overrides = {}) {
  const mission = engine.create({
    tenantId: BACKUS.tenantId,
    objective: 'Acquire commercial cleaning law firm clients in Manchester NH.',
    targetSegment: 'Law Firms',
    confidence: 0.82,
  });
  const missionId = overrides.id || mission.id;
  engine.store.putMission({
    ...mission,
    ...overrides,
    id: missionId,
    stage: STAGES.OBSERVE,
    confidence: overrides.confidence ?? 0.82,
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
  engine.store.addEvent(createEvent({
    missionId,
    kind: EVENT_KINDS.LAUNCHED,
    specialist: SPECIALISTS.EMMETT,
    label: 'Sent to Backus',
    payload: { prospectId: BACKUS.prospectId },
  }));
  return engine.get(missionId, BACKUS.tenantId);
}

describe('SPEC-251 Observe Reaction Policy', () => {
  let engine;

  beforeEach(() => {
    engine = amo.createAcquisitionMissionEngine();
  });

  it('evidence matrix maps transport and engagement observations', () => {
    const cases = [
      ['sent', EVIDENCE_TYPES.SENT, EVIDENCE_STRENGTH.TRANSPORT_ATTEMPTED, DISPOSITIONS.REACHED],
      ['delivered', EVIDENCE_TYPES.DELIVERED, EVIDENCE_STRENGTH.TRANSPORT_CONFIRMED, DISPOSITIONS.REACHED],
      ['opened_proxy', EVIDENCE_TYPES.PROXY_OPEN, EVIDENCE_STRENGTH.WEAK_ENGAGEMENT, DISPOSITIONS.POSSIBLY_SEEN],
      ['opened', EVIDENCE_TYPES.HUMAN_OPEN, EVIDENCE_STRENGTH.ENGAGEMENT, DISPOSITIONS.SEEN],
      ['clicked', EVIDENCE_TYPES.CLICKED, EVIDENCE_STRENGTH.STRONG_ENGAGEMENT, DISPOSITIONS.ENGAGED],
      ['soft_bounce', EVIDENCE_TYPES.SOFT_BOUNCE, EVIDENCE_STRENGTH.TRANSPORT_ATTEMPTED, DISPOSITIONS.UNREACHED],
      ['hard_bounce', EVIDENCE_TYPES.HARD_BOUNCE, EVIDENCE_STRENGTH.TERMINAL_NEGATIVE, DISPOSITIONS.UNREACHABLE],
      ['unsubscribed', EVIDENCE_TYPES.UNSUBSCRIBE, EVIDENCE_STRENGTH.TERMINAL_NEGATIVE, DISPOSITIONS.REJECTED],
    ];

    const mission = setupObserveMission(engine);
    for (const [eventType, evidenceType, strength, disposition] of cases) {
      const obs = communicationObservation(mission.id, eventType, { suffix: eventType });
      const result = evaluateObserveReaction({
        mission,
        observation: obs,
        priorState: {},
        store: engine.store,
        outcomes: [],
      });
      assert.equal(result.reaction.evidenceType, evidenceType, eventType);
      assert.equal(result.reaction.evidenceStrength, strength, eventType);
      assert.equal(result.reaction.updatedDisposition, disposition, eventType);
      assert.equal(result.reaction.externalActionPermitted, false, eventType);
    }
  });

  it('human open is stronger than proxy open and is not interest', () => {
    const mission = setupObserveMission(engine);
    const proxy = evaluateObserveReaction({
      mission,
      observation: communicationObservation(mission.id, 'opened_proxy', { suffix: 'proxy' }),
      priorState: { disposition: DISPOSITIONS.REACHED },
      store: engine.store,
      outcomes: [],
    });
    const human = evaluateObserveReaction({
      mission,
      observation: communicationObservation(mission.id, 'opened', { suffix: 'human' }),
      priorState: { disposition: DISPOSITIONS.REACHED },
      store: engine.store,
      outcomes: [],
    });

    assert.ok(
      human.reaction.evidenceStrength !== proxy.reaction.evidenceStrength,
      'human vs proxy strength differs'
    );
    assert.match(human.reaction.rationale, /not buying intent/i);
    assert.match(proxy.reaction.rationale, /not buying intent|weak engagement/i);
    assert.notEqual(human.reaction.updatedDisposition, DISPOSITIONS.INTERESTED);
  });

  it('open does not create business outcomes or mutate mission confidence', () => {
    const mission = setupObserveMission(engine, { confidence: 0.82 });
    const beforeConfidence = engine.get(mission.id, BACKUS.tenantId).confidence;
    const priorOutcomes = engine.store.listOutcomes(mission.id).length;

    engine.applyObserveReaction({
      missionId: mission.id,
      observation: communicationObservation(mission.id, 'opened', { suffix: 'open-no-interest' }),
      interpretation: { type: INTERPRETATION_TYPES.HUMAN_OPEN, confidence: 1 },
    }, { tenantId: BACKUS.tenantId });

    const after = engine.get(mission.id, BACKUS.tenantId);
    assert.equal(after.stage, STAGES.OBSERVE);
    assert.equal(after.confidence, beforeConfidence);
    assert.equal(engine.store.listOutcomes(mission.id).length, priorOutcomes);
  });

  it('idempotent duplicate observation does not duplicate reaction', () => {
    const mission = setupObserveMission(engine);
    const obs = communicationObservation(mission.id, 'delivered', { suffix: 'dup' });
    const first = engine.applyObserveReaction({
      missionId: mission.id,
      observation: obs,
      interpretation: { type: INTERPRETATION_TYPES.TRANSPORT_SUCCESS },
    }, { tenantId: BACKUS.tenantId });
    const second = engine.applyObserveReaction({
      missionId: mission.id,
      observation: obs,
      interpretation: { type: INTERPRETATION_TYPES.TRANSPORT_SUCCESS },
    }, { tenantId: BACKUS.tenantId });

    assert.equal(first.duplicate, false);
    assert.equal(second.duplicate, true);
    assert.equal(engine.store.listObserveReactions(mission.id).length, 1);
    assert.equal(buildObserveReactionId(obs.id), first.reaction.id);
  });

  it('cadence derived from prepared sequence steps when present', () => {
    const mission = setupObserveMission(engine);
    engine.store.addContribution({
      id: 'contrib-emmett-cap',
      missionId: mission.id,
      specialist: SPECIALISTS.EMMETT,
      kind: 'capacity',
      at: '2026-09-01T00:00:00.000Z',
      payload: {
        steps: [{ day: 0 }, { day: 4 }, { day: 8 }],
      },
    });

    const cadence = resolveObserveCadence({
      mission,
      store: engine.store,
      sequenceStepSent: 0,
      clockStart: '2026-09-14T12:25:00.000Z',
      now: new Date('2026-09-14T12:26:00.000Z'),
    });

    assert.equal(cadence.cadenceSource, 'prepared_sequence');
    assert.equal(cadence.waitDays, 4);
    assert.equal(cadence.kind, 'wait_until');
    assert.ok(cadence.dueAt);
  });

  it('cadence unresolved when no sequence artifact exists', () => {
    const mission = setupObserveMission(engine);
    const cadence = resolveObserveCadence({
      mission,
      store: engine.store,
      sequenceStepSent: 0,
      clockStart: '2026-09-14T12:25:00.000Z',
    });
    assert.equal(cadence.cadenceSource, 'unresolved');
    assert.equal(cadence.waitDays, null);
    assert.equal(cadence.kind, 'unresolved');
  });

  it('Backus sent + delivered + human_open yields seen / wait without external action', () => {
    const mission = setupObserveMission(engine);
    let prior = {};

    for (const [eventType, at] of [
      ['sent', '2026-09-14T12:25:00.000Z'],
      ['delivered', '2026-09-14T12:25:30.000Z'],
      ['opened', '2026-09-14T12:40:00.000Z'],
    ]) {
      const obs = communicationObservation(mission.id, eventType, {
        suffix: eventType,
        occurredAt: at,
        id: `obs_backus_${eventType}`,
      });
      const evaluated = evaluateObserveReaction({
        mission,
        observation: obs,
        priorState: prior,
        store: engine.store,
        outcomes: [],
        now: new Date(at),
      });
      engine.store.addObserveReaction(evaluated.reaction);
      engine.store.putCandidateObserveState({
        missionId: mission.id,
        tenantId: BACKUS.tenantId,
        prospectId: BACKUS.prospectId,
        ...evaluated.candidateState,
      });
      prior = engine.store.getCandidateObserveState(mission.id, BACKUS.prospectId);
    }

    const state = engine.store.getCandidateObserveState(mission.id, BACKUS.prospectId);
    const reaction = engine.store.listObserveReactions(mission.id).slice(-1)[0];

    assert.equal(state.disposition, DISPOSITIONS.SEEN);
    assert.equal(reaction.evidenceType, EVIDENCE_TYPES.HUMAN_OPEN);
    assert.equal(reaction.evidenceStrength, EVIDENCE_STRENGTH.ENGAGEMENT);
    assert.equal(reaction.recommendedNextAction, NEXT_ACTIONS.WAIT);
    assert.equal(reaction.externalActionPermitted, false);
    assert.match(reaction.rationale, /not buying intent/i);
    assert.equal(engine.get(mission.id, BACKUS.tenantId).confidence, 0.82);
  });

  it('OBSERVE workspace mode is observing, not Complete', () => {
    const mission = setupObserveMission(engine);
    const snapshot = engine.inspect(mission.id, { tenantId: BACKUS.tenantId });
    const mode = deriveWorkspaceMode({ missionId: mission.id, snapshot });
    assert.equal(mode, WORKSPACE_MODES.OBSERVING);
    assert.notEqual(mode, WORKSPACE_MODES.COMPLETE);
  });

  it('SPEC-122 inspection exposes latest observation reaction and recommendation', () => {
    const mission = setupObserveMission(engine);
    engine.applyObserveReaction({
      missionId: mission.id,
      observation: communicationObservation(mission.id, 'opened', {
        suffix: 'inspect',
        id: 'obs_backus_inspect_open',
      }),
      interpretation: { type: INTERPRETATION_TYPES.HUMAN_OPEN },
    }, { tenantId: BACKUS.tenantId });

    const snapshot = engine.inspect(mission.id, { tenantId: BACKUS.tenantId });
    assert.ok(snapshot.observeAssessment);
    assert.equal(snapshot.observeAssessment.spec, 'SPEC-251');
    assert.ok(snapshot.observeReactions.length >= 1);

    const next = engine.answerOperator('What happens next?', {
      tenantId: BACKUS.tenantId,
      missionId: mission.id,
      silentInspection: true,
    });
    assert.equal(next.inspection.property, 'next');
    assert.match(next.prose, /Wait|wait|OBSERVE/i);
    assert.match(next.prose, /false|not permitted|Human approval/i);

    const meaning = engine.answerOperator('What does the open mean?', {
      tenantId: BACKUS.tenantId,
      missionId: mission.id,
      silentInspection: true,
    });
    assert.equal(meaning.inspection.property, 'observe_meaning');
    assert.match(meaning.prose, /engagement|not buying intent/i);
  });

  it('meaningful business outcomes still advance LEARN (regression)', () => {
    const mission = setupObserveMission(engine);
    engine.applyRileyReplyInterpretation({
      missionId: mission.id,
      prospectId: BACKUS.prospectId,
      classification: 'interested',
      replyText: 'Yes, tell me more.',
      observationId: 'obs-riley-interested-1',
    }, { tenantId: BACKUS.tenantId });

    assert.equal(engine.get(mission.id, BACKUS.tenantId).stage, STAGES.LEARN);
  });

  it('hard bounce creates terminal disposition without DNC mutation hook', () => {
    const mission = setupObserveMission(engine);
    const result = evaluateObserveReaction({
      mission,
      observation: communicationObservation(mission.id, 'hard_bounce', { suffix: 'bounce' }),
      priorState: {},
      store: engine.store,
      outcomes: [],
    });
    assert.equal(result.reaction.updatedDisposition, DISPOSITIONS.UNREACHABLE);
    assert.equal(result.reaction.recommendedNextAction, NEXT_ACTIONS.PROPOSE_END_CANDIDATE);
    assert.equal(result.reaction.externalActionPermitted, false);
  });
});
