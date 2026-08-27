'use strict';

/**
 * Canonical observation interpretation & business outcomes (AUDIT-073 repair).
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../packages/acquisition-mission');
const {
  STAGES,
  SPECIALISTS,
  EVENT_KINDS,
  OBSERVATION_KINDS,
  INTERPRETATION_TYPES,
  EVIDENCE_ONLY_EVENT_TYPES,
  isEvidenceOnlyEventType,
  isBusinessOutcomeType,
  interpretMissionObservation,
  interpretRileyReply,
  shouldCreateOutcome,
  buildOutcomePayload,
  detectWalkthroughIntent,
} = amo;
const { createEvent } = require('../packages/acquisition-mission/Timeline');
const {
  consumeMissionProviderEvent,
} = require('../services/acquisitionMissionProviderObservation');
const {
  correlateRileyReplyToMission,
  consumeRileyReplyInterpretation,
} = require('../services/acquisitionMissionRileyInterpretation');
const {
  createAcquisitionMissionRuntime,
  resetAcquisitionMissionRuntime,
  setAcquisitionMissionRuntimeForTests,
} = require('../services/acquisitionMissionRuntime');

function sampleProviderEvent(overrides = {}) {
  return {
    id: 'amo_pe_test001',
    dedupeKey: 'dedupe-test-1',
    missionId: 'mission-interp-1',
    tenantId: '10',
    prospectId: 'co-harbor',
    executionRecordId: 'amo_send_test1',
    preparedArtifactRevision: 'rev-hash-1',
    provider: 'brevo',
    providerMessageId: 'brevo-msg-123',
    eventType: 'delivered',
    eventCategory: 'delivery',
    rawEventType: 'delivered',
    providerEventId: 'brevo-event-1',
    occurredAt: '2026-08-27T12:00:00.000Z',
    payload: {},
    createdAt: '2026-08-27T12:00:01.000Z',
    ...overrides,
  };
}

function setupObserveMission(engine, overrides = {}) {
  const mission = engine.create({
    tenantId: '10',
    objective: 'Acquire commercial cleaning customers in Manchester NH for law firms.',
    targetSegment: 'Law Firms',
  });
  const missionId = overrides.id || mission.id;
  engine.store.putMission({
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
  engine.store.addEvent(createEvent({
    missionId,
    kind: EVENT_KINDS.LAUNCHED,
    specialist: SPECIALISTS.EMMETT,
    label: 'Sent to co-harbor',
    payload: {
      prospectId: 'co-harbor',
      providerMessageId: 'brevo-msg-123',
      preparedArtifactRevision: 'rev-hash-1',
    },
  }));
  return engine.get(missionId, '10');
}

function structuredObservation(eventType, overrides = {}) {
  return {
    id: `obs_${eventType}_001`,
    missionId: 'mission-interp-1',
    prospectId: 'co-harbor',
    kind: OBSERVATION_KINDS.COMMUNICATION_EVIDENCE,
    category: ['delivered', 'sent', 'deferred', 'hard_bounce'].includes(eventType) ? 'delivery' : 'engagement',
    eventType,
    occurredAt: '2026-08-27T12:00:00.000Z',
    evidence: {
      provider: 'brevo',
      providerEventId: 'brevo-event-1',
      providerMessageId: 'brevo-msg-123',
      executionRecordId: 'amo_send_test1',
      preparedArtifactRevision: 'rev-hash-1',
    },
    payload: {},
    source: 'provider_webhook',
    specialist: 'emmett',
    observation: `brevo ${eventType} for prospect co-harbor`,
    at: '2026-08-27T12:00:00.000Z',
    ...overrides,
  };
}

describe('Outcome taxonomy', () => {
  it('marks transport/engagement events as evidence-only', () => {
    for (const eventType of ['queued', 'sent', 'delivered', 'opened', 'opened_proxy', 'clicked', 'deferred']) {
      assert.equal(isEvidenceOnlyEventType(eventType), true, eventType);
    }
    assert.equal(isEvidenceOnlyEventType('replied'), false);
  });

  it('recognizes business outcome types', () => {
    assert.equal(isBusinessOutcomeType('interested'), true);
    assert.equal(isBusinessOutcomeType('walkthrough_booked'), true);
    assert.equal(isBusinessOutcomeType('unsubscribe'), true);
    assert.equal(isBusinessOutcomeType('delivered'), false);
    assert.equal(isBusinessOutcomeType('open'), false);
  });
});

describe('Provider observation interpretation', () => {
  let engine;

  beforeEach(() => {
    engine = amo.createAcquisitionMissionEngine();
    setupObserveMission(engine, { id: 'mission-interp-1' });
  });

  it('delivered observation → transport_success interpretation → no business outcome', () => {
    const obs = structuredObservation('delivered');
    const result = interpretMissionObservation({ observation: obs });
    assert.equal(result.interpretation.type, INTERPRETATION_TYPES.TRANSPORT_SUCCESS);
    assert.equal(result.recommendedOutcome, null);

    const applied = engine.applyCommunicationObservationInterpretation('mission-interp-1', obs);
    assert.ok(applied.interpretation);
    assert.equal(applied.outcome, null);
    assert.equal(engine.store.listOutcomes('mission-interp-1').length, 0);
  });

  it('human open → human_open interpretation → no business outcome', () => {
    const obs = structuredObservation('opened');
    const result = interpretMissionObservation({ observation: obs });
    assert.equal(result.interpretation.type, INTERPRETATION_TYPES.HUMAN_OPEN);
    assert.equal(result.recommendedOutcome, null);

    engine.applyCommunicationObservationInterpretation('mission-interp-1', obs);
    assert.equal(engine.store.listOutcomes('mission-interp-1').length, 0);
  });

  it('replied observation without semantic content → no automatic business outcome', () => {
    const obs = structuredObservation('replied');
    const result = interpretMissionObservation({ observation: obs });
    assert.equal(result.interpretation.type, INTERPRETATION_TYPES.REPLY_RECEIVED);
    assert.equal(result.recommendedOutcome, null);

    engine.applyCommunicationObservationInterpretation('mission-interp-1', obs);
    assert.equal(engine.store.listOutcomes('mission-interp-1').length, 0);
  });

  it('provider unsubscribed → unsubscribe outcome with provenance', () => {
    const obs = structuredObservation('unsubscribed');
    const applied = engine.applyCommunicationObservationInterpretation('mission-interp-1', obs);
    assert.ok(applied.outcome);
    assert.equal(applied.outcome.type, 'unsubscribe');
    assert.equal(applied.outcome.payload.source, 'provider_observation_interpretation');
    assert.ok(applied.outcome.payload.interpretationId);
    assert.deepEqual(applied.outcome.payload.observationIds, [obs.id]);
  });

  it('hard_bounce → bounce outcome with provenance', () => {
    const obs = structuredObservation('hard_bounce', { category: 'delivery' });
    const applied = engine.applyCommunicationObservationInterpretation('mission-interp-1', obs);
    assert.ok(applied.outcome);
    assert.equal(applied.outcome.type, 'bounce');
    assert.ok(applied.outcome.payload.interpretationId);
  });
});

describe('Riley reply interpretation', () => {
  let engine;

  beforeEach(() => {
    engine = amo.createAcquisitionMissionEngine();
    setupObserveMission(engine, { id: 'mission-interp-1' });
  });

  it('interested classification → positive_intent interpretation', () => {
    const result = interpretRileyReply({
      missionId: 'mission-interp-1',
      prospectId: 'co-harbor',
      classification: 'interested',
      replyText: 'Yes, tell me more about your services.',
    });
    assert.equal(result.interpretation.type, INTERPRETATION_TYPES.POSITIVE_INTENT);
    assert.equal(result.recommendedOutcome.type, 'interested');
    assert.equal(result.recommendedOutcome.terminal, false);
  });

  it('walkthrough intent in reply → walkthrough_requested intermediate outcome', () => {
    assert.equal(detectWalkthroughIntent('Can you come Tuesday?'), true);
    const result = interpretRileyReply({
      missionId: 'mission-interp-1',
      prospectId: 'co-harbor',
      classification: 'interested',
      replyText: 'Can you come Tuesday? We are available after 2pm.',
    });
    assert.equal(result.interpretation.type, INTERPRETATION_TYPES.WALKTHROUGH_INTENT);
    assert.equal(result.recommendedOutcome.type, 'walkthrough_requested');
  });

  it('unsubscribe → canonical unsubscribe outcome with provenance', () => {
    const applied = engine.applyRileyReplyInterpretation({
      missionId: 'mission-interp-1',
      prospectId: 'co-harbor',
      classification: 'unsubscribe',
      correlation: { executionRecordId: 'amo_send_test1', providerMessageId: 'brevo-msg-123' },
    });
    assert.equal(applied.outcome.type, 'unsubscribe');
    assert.equal(applied.outcome.payload.source, 'riley_reply_interpretation');
    assert.equal(applied.outcome.payload.executionRecordId, 'amo_send_test1');
  });

  it('negative → not_interested outcome when threshold met', () => {
    const applied = engine.applyRileyReplyInterpretation({
      missionId: 'mission-interp-1',
      prospectId: 'co-harbor',
      classification: 'negative',
    });
    assert.equal(applied.outcome.type, 'not_interested');
  });

  it('unknown classification → interpretation only, no automatic outcome', () => {
    const applied = engine.applyRileyReplyInterpretation({
      missionId: 'mission-interp-1',
      prospectId: 'co-harbor',
      classification: 'unknown',
    });
    assert.ok(applied.interpretation);
    assert.equal(applied.interpretation.requiresHumanConfirmation, true);
    assert.equal(applied.outcome, null);
  });
});

describe('Booking evidence interpretation', () => {
  it('booking evidence → walkthrough_booked outcome with mission binding', () => {
    const engine = amo.createAcquisitionMissionEngine();
    setupObserveMission(engine, { id: 'mission-interp-1' });
    const applied = engine.applyBookingInterpretation({
      missionId: 'mission-interp-1',
      prospectId: 'co-harbor',
      bookingRef: 'cal-event-abc123',
      correlation: { executionRecordId: 'amo_send_test1' },
    });
    assert.equal(applied.outcome.type, 'walkthrough_booked');
    assert.equal(applied.outcome.payload.source, 'booking_evidence');
  });
});

describe('Provider event consumer integration', () => {
  let runtime;
  let engine;

  beforeEach(() => {
    resetAcquisitionMissionRuntime();
    runtime = createAcquisitionMissionRuntime({ persist: false, production: false });
    setAcquisitionMissionRuntimeForTests(runtime);
    engine = runtime.engine();
  });

  it('delivered/clicked/opened/replied still do not create business outcomes via webhook path', async () => {
    setupObserveMission(engine, { id: 'mission-interp-1', stage: STAGES.EXECUTE });
    for (const eventType of ['delivered', 'opened', 'clicked', 'replied']) {
      await consumeMissionProviderEvent(
        {
          event: sampleProviderEvent({
            missionId: 'mission-interp-1',
            id: `amo_pe_${eventType}`,
            eventType,
            eventCategory: eventType === 'delivered' ? 'delivery' : 'engagement',
            providerEventId: `brevo-${eventType}`,
          }),
          inserted: true,
          duplicate: false,
        },
        null,
        { runtime, persist: false }
      );
    }
    assert.equal(engine.store.listOutcomes('mission-interp-1').length, 0);
    assert.ok(engine.store.listInterpretations('mission-interp-1').length >= 4);
  });

  it('does not auto-progress OBSERVE → LEARN after outcomes created', async () => {
    setupObserveMission(engine, { id: 'mission-interp-1', stage: STAGES.EXECUTE });
    await consumeMissionProviderEvent(
      {
        event: sampleProviderEvent({
          missionId: 'mission-interp-1',
          eventType: 'unsubscribed',
          eventCategory: 'engagement',
        }),
        inserted: true,
        duplicate: false,
      },
      null,
      { runtime, persist: false }
    );
    const mission = engine.get('mission-interp-1', '10');
    assert.equal(mission.stage, STAGES.OBSERVE);
    assert.ok(engine.store.listOutcomes('mission-interp-1').length > 0);
  });
});

describe('Riley mission correlation', () => {
  it('legacy reply correlation returns missionBound=false without fabricated mission IDs', async () => {
    const pool = {
      async query(sql, params) {
        if (/acquisition_mission_outbound_executions/i.test(sql)) return { rows: [] };
        if (/agent_log/i.test(sql) && /email_sent/i.test(sql)) {
          return {
            rows: [{
              payload: { message_id: params[0] },
              client_id: 10,
              prospect_id: 42,
              ran_at: '2026-08-27T10:00:00.000Z',
            }],
          };
        }
        if (/prospects p/i.test(sql)) return { rows: [] };
        return { rows: [] };
      },
    };

    const { correlation, missionBound } = await correlateRileyReplyToMission({
      email: { inReplyTo: '<legacy-msg-id@mail.example>' },
      prospectId: 42,
      clientId: 10,
    }, pool);

    assert.equal(missionBound, false);
    assert.equal(correlation.missionId, null);
    assert.equal(correlation.source, 'legacy_agent_log');
  });

  it('consumeRileyReplyInterpretation skips when no mission binding', async () => {
    const pool = { async query() { return { rows: [] }; } };
    const result = await consumeRileyReplyInterpretation({
      prospect: { id: 99 },
      email: { inReplyTo: null },
      classification: 'interested',
      clientId: 10,
    }, pool, { persist: false });
    assert.equal(result.skipped, true);
    assert.equal(result.reason, 'no_mission_binding');
  });
});
