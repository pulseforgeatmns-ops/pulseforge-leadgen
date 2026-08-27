'use strict';

/**
 * Canonical communication observation + OBSERVE entry (AUDIT-072 repair).
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../packages/acquisition-mission');
const {
  STAGES,
  SPECIALISTS,
  EVENT_KINDS,
  createCommunicationObservation,
  buildCommunicationObservationId,
  isCommunicationEvidenceEventType,
  OBSERVATION_KINDS,
} = amo;
const { createEvent } = require('../packages/acquisition-mission/Timeline');
const {
  consumeMissionProviderEvent,
  shouldProgressToObserve,
} = require('../services/acquisitionMissionProviderObservation');
const {
  createAcquisitionMissionRuntime,
  resetAcquisitionMissionRuntime,
  setAcquisitionMissionRuntimeForTests,
} = require('../services/acquisitionMissionRuntime');
const { persistMissionProviderEvent } = require('../services/acquisitionMissionOutboundPersistence');

function sampleProviderEvent(overrides = {}) {
  return {
    id: 'amo_pe_delivered001',
    dedupeKey: 'dedupe-delivered-1',
    missionId: 'mission-observe-1',
    tenantId: '10',
    prospectId: 'co-harbor',
    executionRecordId: 'amo_send_test1',
    preparedArtifactRevision: 'rev-hash-1',
    provider: 'brevo',
    providerMessageId: 'brevo-msg-123',
    eventType: 'delivered',
    eventCategory: 'delivery',
    rawEventType: 'delivered',
    providerEventId: 'brevo-event-delivered-1',
    occurredAt: '2026-08-27T12:00:00.000Z',
    payload: { open_source: null },
    createdAt: '2026-08-27T12:00:01.000Z',
    ...overrides,
  };
}

function setupExecuteMission(engine, overrides = {}) {
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
    stage: STAGES.EXECUTE,
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

describe('Communication observation contract', () => {
  it('maps delivery provider events to structured communication observations', () => {
    const row = createCommunicationObservation(sampleProviderEvent());
    assert.equal(row.kind, OBSERVATION_KINDS.COMMUNICATION_EVIDENCE);
    assert.equal(row.category, 'delivery');
    assert.equal(row.eventType, 'delivered');
    assert.equal(row.source, 'provider_webhook');
    assert.equal(row.missionId, 'mission-observe-1');
    assert.equal(row.prospectId, 'co-harbor');
    assert.equal(row.evidence.executionRecordId, 'amo_send_test1');
    assert.equal(row.evidence.preparedArtifactRevision, 'rev-hash-1');
    assert.equal(row.evidence.providerMessageId, 'brevo-msg-123');
    assert.equal(row.evidence.providerEventId, 'brevo-event-delivered-1');
    assert.equal(row.evidence.missionProviderEventId, 'amo_pe_delivered001');
    assert.match(row.observation, /brevo delivered/i);
  });

  it('maps engagement provider events (clicked) to structured observations', () => {
    const row = createCommunicationObservation(sampleProviderEvent({
      id: 'amo_pe_clicked001',
      eventType: 'clicked',
      eventCategory: 'engagement',
      providerEventId: 'brevo-event-click-1',
      payload: { link: 'https://example.com/walkthrough' },
    }));
    assert.equal(row.category, 'engagement');
    assert.equal(row.eventType, 'clicked');
    assert.equal(row.payload.link, 'https://example.com/walkthrough');
  });

  it('rejects unsupported provider event types (invalid/error)', () => {
    assert.equal(isCommunicationEvidenceEventType('invalid'), false);
    assert.equal(isCommunicationEvidenceEventType('error'), false);
    assert.equal(createCommunicationObservation(sampleProviderEvent({ eventType: 'invalid' })), null);
  });

  it('derives deterministic observation ids from mission provider event rows', () => {
    const providerEvent = sampleProviderEvent();
    assert.equal(
      buildCommunicationObservationId(providerEvent),
      `obs_${providerEvent.id}`
    );
  });
});

describe('Provider event consumer → communication observation', () => {
  let runtime;
  let engine;

  beforeEach(() => {
    resetAcquisitionMissionRuntime();
    runtime = createAcquisitionMissionRuntime({ persist: false, production: false });
    setAcquisitionMissionRuntimeForTests(runtime);
    engine = runtime.engine();
  });

  it('delivered webhook path: provider event → communication observation', async () => {
    const mission = setupExecuteMission(engine, { id: 'mission-observe-1' });
    const providerEvent = sampleProviderEvent({ missionId: mission.id });

    const result = await consumeMissionProviderEvent(
      { event: providerEvent, inserted: true, duplicate: false },
      null,
      { runtime, persist: false }
    );

    assert.ok(result.observation);
    assert.equal(result.observation.kind, OBSERVATION_KINDS.COMMUNICATION_EVIDENCE);
    assert.equal(result.observation.eventType, 'delivered');
    assert.equal(result.duplicate, false);
    assert.equal(engine.store.listObservations(mission.id).length, 1);
  });

  it('clicked webhook path: provider event → engagement observation', async () => {
    const mission = setupExecuteMission(engine, { id: 'mission-observe-1' });
    const providerEvent = sampleProviderEvent({
      missionId: mission.id,
      eventType: 'clicked',
      eventCategory: 'engagement',
    });

    const result = await consumeMissionProviderEvent(
      { event: providerEvent, inserted: true, duplicate: false },
      null,
      { runtime, persist: false }
    );

    assert.equal(result.observation.category, 'engagement');
    assert.equal(result.observation.eventType, 'clicked');
  });

  it('preserves full provenance on the observation', async () => {
    const mission = setupExecuteMission(engine, { id: 'mission-observe-1' });
    const providerEvent = sampleProviderEvent({ missionId: mission.id });

    const result = await consumeMissionProviderEvent(
      { event: providerEvent, inserted: true, duplicate: false },
      null,
      { runtime, persist: false }
    );

    const obs = result.observation;
    assert.equal(obs.missionId, mission.id);
    assert.equal(obs.prospectId, 'co-harbor');
    assert.equal(obs.evidence.executionRecordId, 'amo_send_test1');
    assert.equal(obs.evidence.preparedArtifactRevision, 'rev-hash-1');
    assert.equal(obs.evidence.providerMessageId, 'brevo-msg-123');
    assert.equal(obs.evidence.providerEventId, 'brevo-event-delivered-1');
    assert.equal(obs.evidence.provider, 'brevo');
  });

  it('duplicate webhook: same provider event → one observation', async () => {
    const mission = setupExecuteMission(engine, { id: 'mission-observe-1' });
    const providerEvent = sampleProviderEvent({ missionId: mission.id });
    const payload = { event: providerEvent, inserted: true, duplicate: false };

    const first = await consumeMissionProviderEvent(payload, null, { runtime, persist: false });
    const second = await consumeMissionProviderEvent(
      { event: providerEvent, inserted: false, duplicate: true },
      null,
      { runtime, persist: false }
    );

    assert.equal(first.duplicate, false);
    assert.equal(second.duplicate, true);
    assert.equal(engine.store.listObservations(mission.id).length, 1);
    assert.equal(first.observation.id, second.observation.id);
  });

  it('does not create business outcomes for opened/clicked/delivered/replied events', async () => {
    const mission = setupExecuteMission(engine, { id: 'mission-observe-1' });
    for (const eventType of ['delivered', 'opened', 'clicked', 'replied']) {
      await consumeMissionProviderEvent(
        {
          event: sampleProviderEvent({
            missionId: mission.id,
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
    assert.equal(engine.store.listOutcomes(mission.id).length, 0);
  });

  it('eligible EXECUTE mission with launch evidence progresses to OBSERVE on provider observation', async () => {
    const mission = setupExecuteMission(engine, { id: 'mission-observe-1' });
    const providerEvent = sampleProviderEvent({ missionId: mission.id });

    const result = await consumeMissionProviderEvent(
      { event: providerEvent, inserted: true, duplicate: false },
      null,
      { runtime, persist: false }
    );

    assert.equal(result.progressed, true);
    assert.equal(result.stage, STAGES.OBSERVE);
    assert.equal(engine.get(mission.id, '10').stage, STAGES.OBSERVE);
  });

  it('does not progress to OBSERVE while executionSummary.complete is false', async () => {
    const mission = setupExecuteMission(engine, {
      id: 'mission-observe-1',
      executionSummary: {
        total: 2,
        sent: 1,
        failed: 0,
        blocked: 0,
        queued: 1,
        attempted: 0,
        complete: false,
      },
    });
    const providerEvent = sampleProviderEvent({ missionId: mission.id });

    const result = await consumeMissionProviderEvent(
      { event: providerEvent, inserted: true, duplicate: false },
      null,
      { runtime, persist: false }
    );

    assert.equal(result.progressed, false);
    assert.equal(result.observeBlocked, 'execution_incomplete');
    assert.equal(engine.get(mission.id, '10').stage, STAGES.EXECUTE);
    assert.equal(engine.store.listObservations(mission.id).length, 1);
  });

  it('skips uncorrelated unsupported event types without fabricated observations', async () => {
    setupExecuteMission(engine, { id: 'mission-observe-1' });
    const result = await consumeMissionProviderEvent(
      {
        event: sampleProviderEvent({ eventType: 'invalid' }),
        inserted: true,
        duplicate: false,
      },
      null,
      { runtime, persist: false }
    );
    assert.equal(result.skipped, true);
    assert.equal(result.reason, 'unsupported_event_type');
  });
});

describe('Cross-process hydration', () => {
  it('webhook consumer hydrates persisted mission without prior in-memory engine state', async () => {
    const pool = createAmoMemoryPool();

    const seedRuntime = createAcquisitionMissionRuntime({ pool, persist: true, production: false });
    const seedEngine = seedRuntime.engine();
    const mission = setupExecuteMission(seedEngine, { id: 'mission-hydrate-1' });
    await seedRuntime.persistMissionState(mission.id, { pool, persist: true });

    resetAcquisitionMissionRuntime();
    const webhookRuntime = createAcquisitionMissionRuntime({ pool, persist: true, production: false });
    setAcquisitionMissionRuntimeForTests(webhookRuntime);

    assert.equal(webhookRuntime.engine().get(mission.id, '10'), null);

    const providerEvent = sampleProviderEvent({
      missionId: mission.id,
      id: 'amo_pe_hydrate001',
    });

    const result = await consumeMissionProviderEvent(
      { event: providerEvent, inserted: true, duplicate: false },
      pool,
      { runtime: webhookRuntime, pool, persist: true }
    );

    assert.ok(result.observation);
    assert.equal(result.progressed, true);
    assert.equal(result.stage, STAGES.OBSERVE);
    assert.equal(webhookRuntime.engine().get(mission.id, '10').stage, STAGES.OBSERVE);
  });
});

describe('Engine recordCommunicationObservation', () => {
  it('preserves free-text observation compatibility', () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester NH for law firms.',
    });
    const row = engine.recordObservation(mission.id, {
      specialist: 'scout',
      observation: 'Found 12 law firms in Manchester.',
    });
    assert.equal(row.observation, 'Found 12 law firms in Manchester.');
    assert.equal(row.kind, undefined);
  });
});

function createAmoMemoryPool() {
  const tables = {
    acquisition_missions: new Map(),
    acquisition_mission_events: new Map(),
    acquisition_mission_contributions: new Map(),
    acquisition_mission_observations: new Map(),
    acquisition_mission_outcomes: new Map(),
    acquisition_mission_execution_audit: new Map(),
  };
  let txnBackup = null;

  function cloneTables() {
    return Object.fromEntries(
      Object.entries(tables).map(([name, map]) => [name, new Map(map)])
    );
  }

  function restoreTables(backup) {
    for (const [name, map] of Object.entries(backup)) {
      tables[name] = map;
    }
  }

  const pool = {
    tables,
    async query(sql, params = []) {
      const trimmed = sql.trim();
      if (/^CREATE TABLE|^CREATE INDEX|^ALTER TABLE/i.test(trimmed)) return { rows: [] };
      if (trimmed === 'BEGIN') {
        txnBackup = cloneTables();
        return { rows: [] };
      }
      if (trimmed === 'COMMIT') {
        txnBackup = null;
        return { rows: [] };
      }
      if (trimmed === 'ROLLBACK') {
        if (txnBackup) restoreTables(txnBackup);
        txnBackup = null;
        return { rows: [] };
      }

      if (/INSERT INTO acquisition_missions/i.test(sql)) {
        const mission = params[14];
        tables.acquisition_missions.set(params[0], {
          id: params[0],
          tenant_id: String(params[1]),
          client_id: params[2],
          stage: params[3],
          status: params[4],
          objective: params[5],
          target_segment: params[6],
          campaign: params[7],
          title: params[8],
          priority: params[9],
          confidence: params[10],
          owner: params[11],
          created_by: params[12],
          orchestration_mission_id: params[13],
          payload: mission,
          created_at: params[15],
          updated_at: params[16],
        });
        return { rows: [] };
      }

      if (/INSERT INTO acquisition_mission_events/i.test(sql)) {
        const event = params[6];
        tables.acquisition_mission_events.set(params[0], {
          id: params[0],
          mission_id: params[1],
          tenant_id: String(params[2]),
          kind: params[3],
          specialist: params[4],
          label: params[5],
          payload: event,
          at: params[7],
        });
        return { rows: [] };
      }

      if (/INSERT INTO acquisition_mission_observations/i.test(sql)) {
        tables.acquisition_mission_observations.set(params[0], {
          id: params[0],
          mission_id: params[1],
          tenant_id: String(params[2]),
          specialist: params[3],
          observation: params[4],
          payload: params[5],
          at: params[6],
        });
        return { rows: [] };
      }

      if (/SELECT \* FROM acquisition_missions WHERE tenant_id/i.test(sql)) {
        const tenantId = String(params[0]);
        return {
          rows: [...tables.acquisition_missions.values()].filter(
            (row) => String(row.tenant_id) === tenantId
          ),
        };
      }

      if (/SELECT payload, id, mission_id, kind, specialist, label, at FROM acquisition_mission_events/i.test(sql)) {
        const tenantId = String(params[0]);
        return {
          rows: [...tables.acquisition_mission_events.values()]
            .filter((row) => String(row.tenant_id) === tenantId)
            .map((row) => ({
              id: row.id,
              mission_id: row.mission_id,
              kind: row.kind,
              specialist: row.specialist,
              label: row.label,
              at: row.at,
              payload: row.payload,
            })),
        };
      }

      if (/SELECT id, mission_id, specialist, observation, payload, at FROM acquisition_mission_observations/i.test(sql)) {
        const tenantId = String(params[0]);
        return {
          rows: [...tables.acquisition_mission_observations.values()].filter(
            (row) => String(row.tenant_id) === tenantId
          ),
        };
      }

      if (/SELECT payload FROM acquisition_mission_contributions/i.test(sql)) {
        return { rows: [] };
      }

      if (/SELECT payload FROM acquisition_mission_outcomes/i.test(sql)) {
        return { rows: [] };
      }

      if (/SELECT payload FROM acquisition_mission_learning/i.test(sql)) {
        return { rows: [] };
      }

      if (/SELECT payload FROM acquisition_mission_predictions/i.test(sql)) {
        return { rows: [] };
      }

      if (/SELECT payload FROM acquisition_mission_outcome_evaluations/i.test(sql)) {
        return { rows: [] };
      }

      if (/SELECT payload FROM acquisition_mission_outcome_learnings/i.test(sql)) {
        return { rows: [] };
      }

      if (/INSERT INTO acquisition_mission_execution_audit/i.test(sql)) {
        return { rows: [] };
      }

      if (/SELECT COUNT\(\*\)/i.test(sql)) {
        return { rows: [{ count: 0 }] };
      }

      return { rows: [] };
    },
  };

  if (typeof pool.connect !== 'function') {
    pool.connect = async () => ({
      query: pool.query.bind(pool),
      release() {},
    });
  }

  return pool;
}
