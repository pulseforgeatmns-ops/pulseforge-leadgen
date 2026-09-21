'use strict';

/**
 * Backus-style Anchor outbound path: Brevo webhook → provider event → mission observation.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const {
  DEFAULTS,
  queryObservations,
  summarizeObservation,
} = require('../scripts/auditAnchorOutboundEvidence');
const { insertBrevoEvent } = require('../utils/brevoEvents');
const {
  consumeMissionProviderEvent,
  backfillMissionObservationsFromProviderEvents,
} = require('../services/acquisitionMissionProviderObservation');
const {
  resetAcquisitionMissionRuntime,
  setAcquisitionMissionRuntimeForTests,
  createAcquisitionMissionRuntime,
} = require('../services/acquisitionMissionRuntime');
const { OBSERVATION_KINDS } = require('../packages/acquisition-mission/CommunicationObservation');
const { STAGES, SPECIALISTS, EVENT_KINDS } = require('../packages/acquisition-mission/types');
const { createEvent } = require('../packages/acquisition-mission/Timeline');

const BACKUS = Object.freeze({
  missionId: DEFAULTS.MISSION_ID,
  executionId: DEFAULTS.EXECUTION_ID,
  tenantId: DEFAULTS.TENANT_ID,
  clientId: DEFAULTS.CLIENT_ID,
  prospectId: '7adbb294-b94c-45c0-85df-e040f027ece0',
  recipientEmail: DEFAULTS.RECIPIENT_EMAIL,
  brevoMessageId: DEFAULTS.BREVO_MESSAGE_ID,
  sendingDomain: 'backusmeyer.com',
  preparedArtifactRevision: 'rev-backus-capacity-1',
});

function backusProviderEvent(overrides = {}) {
  return {
    id: overrides.id || 'amo_pe_backus_sent',
    dedupeKey: overrides.dedupeKey || 'dedupe-backus-sent',
    missionId: BACKUS.missionId,
    tenantId: BACKUS.tenantId,
    prospectId: BACKUS.prospectId,
    executionRecordId: BACKUS.executionId,
    preparedArtifactRevision: BACKUS.preparedArtifactRevision,
    provider: 'brevo',
    providerMessageId: BACKUS.brevoMessageId,
    eventType: overrides.eventType || 'sent',
    eventCategory: overrides.eventCategory || 'delivery',
    rawEventType: overrides.rawEventType || 'request',
    providerEventId: overrides.providerEventId || 'brevo-backus-sent-1',
    occurredAt: overrides.occurredAt || '2026-09-14T12:25:00.000Z',
    payload: overrides.payload || { open_source: null },
    createdAt: overrides.createdAt || '2026-09-14T12:25:01.000Z',
    ...overrides,
  };
}

function createBackusPool() {
  const tables = {
    clients: new Map([[BACKUS.clientId, { id: BACKUS.clientId, sending_domain: BACKUS.sendingDomain }]]),
    prospects: new Map([[BACKUS.prospectId, {
      id: BACKUS.prospectId,
      client_id: BACKUS.clientId,
      email: BACKUS.recipientEmail,
      vertical: 'law_firm',
    }]]),
    executions: new Map([[BACKUS.executionId, {
      id: BACKUS.executionId,
      mission_id: BACKUS.missionId,
      tenant_id: BACKUS.tenantId,
      prospect_id: BACKUS.prospectId,
      prepared_artifact_revision: BACKUS.preparedArtifactRevision,
      provider: 'brevo',
      provider_message_id: BACKUS.brevoMessageId,
      status: 'sent',
      execution_identity: 'identity-backus',
      attempted_at: '2026-09-14T12:25:00.000Z',
      sent_at: '2026-09-14T12:25:00.000Z',
      payload: {},
      created_at: '2026-09-14T12:25:00.000Z',
      updated_at: '2026-09-14T12:25:00.000Z',
    }]]),
    providerEvents: new Map(),
    observations: new Map(),
    emailEvents: [],
    agentLog: [],
    acquisition_missions: new Map(),
    acquisition_mission_events: new Map(),
    acquisition_mission_contributions: new Map(),
    acquisition_mission_observations: new Map(),
    acquisition_mission_outcomes: new Map(),
    acquisition_mission_execution_audit: new Map(),
  };

  let txnBackup = null;
  function cloneTables() {
    return Object.fromEntries(Object.entries(tables).map(([name, value]) => {
      if (value instanceof Map) return [name, new Map(value)];
      if (Array.isArray(value)) return [name, [...value]];
      return [name, value];
    }));
  }
  function restoreTables(backup) {
    for (const [name, value] of Object.entries(backup)) {
      tables[name] = value;
    }
  }

  const pool = {
    tables,
    async query(sql, params = []) {
      const text = String(sql).replace(/\s+/g, ' ').trim();
      if (/^(CREATE TABLE|CREATE INDEX|CREATE TYPE|ALTER TABLE|DO \$\$)/i.test(text)) {
        return { rows: [], rowCount: 0 };
      }
      if (text === 'BEGIN') {
        txnBackup = cloneTables();
        return { rows: [] };
      }
      if (text === 'COMMIT') {
        txnBackup = null;
        return { rows: [] };
      }
      if (text === 'ROLLBACK') {
        if (txnBackup) restoreTables(txnBackup);
        txnBackup = null;
        return { rows: [] };
      }

      if (/to_regclass/i.test(text)) {
        return { rows: [{ present: true }] };
      }

      if (/FROM clients WHERE id/i.test(text)) {
        const row = tables.clients.get(Number(params[0]));
        return { rows: row ? [{ sending_domain: row.sending_domain }] : [] };
      }

      if (/FROM prospects/i.test(text)) {
        if (/WHERE id = \$1/i.test(text)) {
          const row = tables.prospects.get(String(params[0]));
          return { rows: row ? [row] : [] };
        }
        const email = String(params[0] || '').toLowerCase();
        const row = [...tables.prospects.values()].find((p) => String(p.email).toLowerCase() === email);
        return { rows: row ? [row] : [] };
      }

      if (/FROM acquisition_mission_outbound_executions/i.test(text) && /provider_message_id = \$1/i.test(text)) {
        const match = [...tables.executions.values()].find((row) => row.provider_message_id === params[0]);
        return { rows: match ? [match] : [] };
      }

      if (/FROM acquisition_mission_outbound_executions/i.test(text) && /WHERE id = \$1/i.test(text)) {
        const row = tables.executions.get(params[0]);
        return { rows: row ? [row] : [] };
      }

      if (/INSERT INTO acquisition_mission_provider_events/i.test(text)) {
        const dedupeKey = params[1];
        if (tables.providerEvents.has(dedupeKey)) {
          return { rows: [], rowCount: 0 };
        }
        const row = {
          id: params[0],
          dedupe_key: dedupeKey,
          mission_id: params[2],
          tenant_id: params[3],
          prospect_id: params[4],
          execution_record_id: params[5],
          prepared_artifact_revision: params[6],
          provider: params[7],
          provider_message_id: params[8],
          event_type: params[9],
          event_category: params[10],
          raw_event_type: params[11],
          provider_event_id: params[12],
          occurred_at: params[13],
          payload: params[14] || {},
          created_at: new Date().toISOString(),
          inserted: true,
        };
        tables.providerEvents.set(dedupeKey, row);
        return { rows: [{ ...row, inserted: true }], rowCount: 1 };
      }

      if (/FROM acquisition_mission_provider_events WHERE dedupe_key/i.test(text)) {
        const row = tables.providerEvents.get(params[0]);
        return { rows: row ? [row] : [] };
      }

      if (/FROM acquisition_mission_provider_events/i.test(text) && /ORDER BY occurred_at/i.test(text)) {
        const rows = [...tables.providerEvents.values()].filter((row) => {
          let paramIdx = 0;
          if (/mission_id = \$\d+/i.test(text)) {
            if (row.mission_id !== params[paramIdx]) return false;
            paramIdx += 1;
          }
          if (/execution_record_id = \$\d+/i.test(text)) {
            if (row.execution_record_id !== params[paramIdx]) return false;
            paramIdx += 1;
          }
          if (/tenant_id = \$\d+/i.test(text)) {
            if (String(row.tenant_id) !== String(params[paramIdx])) return false;
          }
          return true;
        });
        return { rows };
      }

      if (/SELECT id FROM acquisition_mission_observations WHERE id = \$1/i.test(text)) {
        const row = tables.acquisition_mission_observations.get(params[0]);
        return { rows: row ? [{ id: row.id }] : [] };
      }

      if (/INSERT INTO acquisition_mission_observations/i.test(text)) {
        tables.acquisition_mission_observations.set(params[0], {
          id: params[0],
          mission_id: params[1],
          tenant_id: String(params[2]),
          specialist: params[3],
          observation: params[4],
          payload: params[5],
          at: params[6],
        });
        return { rows: [], rowCount: 1 };
      }

      if (/INSERT INTO email_events/i.test(text)) {
        tables.emailEvents.push({ event_id: params[0], event_type: params[5] });
        return {
          rowCount: 1,
          rows: [{ id: tables.emailEvents.length, open_source: 'unknown', open_source_reason: null, inserted: true }],
        };
      }

      if (/INSERT INTO agent_log/i.test(text)) {
        tables.agentLog.push(params);
        return { rows: [], rowCount: 1 };
      }

      if (/FROM email_events/i.test(text)) {
        return { rows: [] };
      }

      if (/INSERT INTO acquisition_missions/i.test(text)) {
        const mission = params[14];
        tables.acquisition_missions.set(params[0], {
          id: params[0],
          tenant_id: String(params[1]),
          payload: mission,
          stage: params[3],
        });
        return { rows: [] };
      }

      if (/SELECT \* FROM acquisition_missions WHERE tenant_id/i.test(text)) {
        return {
          rows: [...tables.acquisition_missions.values()].filter(
            (row) => String(row.tenant_id) === String(params[0])
          ),
        };
      }

      if (/SELECT payload, id, mission_id, kind, specialist, label, at FROM acquisition_mission_events/i.test(text)) {
        return { rows: [] };
      }

      if (/FROM acquisition_mission_observations/i.test(text) && /SELECT id, mission_id/i.test(text)) {
        if (/payload->'evidence'->>'executionRecordId'/i.test(text)) {
          const [missionId, executionId, messageIds] = params;
          const rows = [...tables.acquisition_mission_observations.values()].filter((row) => {
            if (row.mission_id !== missionId) return false;
            const payload = row.payload && typeof row.payload === 'object' ? row.payload : {};
            const evidence = payload.evidence && typeof payload.evidence === 'object' ? payload.evidence : {};
            return evidence.executionRecordId === executionId
              || (Array.isArray(messageIds) && messageIds.includes(evidence.providerMessageId));
          });
          return { rows };
        }
        const tenantId = String(params[0]);
        return {
          rows: [...tables.acquisition_mission_observations.values()].filter(
            (row) => String(row.tenant_id) === tenantId
          ),
        };
      }

      if (/SELECT payload FROM acquisition_mission_contributions/i.test(text)) {
        return { rows: [] };
      }

      if (/SELECT payload FROM acquisition_mission_outcomes/i.test(text)) {
        return { rows: [] };
      }

      if (/SELECT payload FROM acquisition_mission_learning/i.test(text)) {
        return { rows: [] };
      }

      if (/SELECT payload FROM acquisition_mission_predictions/i.test(text)) {
        return { rows: [] };
      }

      if (/SELECT payload FROM acquisition_mission_outcome_evaluations/i.test(text)) {
        return { rows: [] };
      }

      if (/SELECT payload FROM acquisition_mission_outcome_learnings/i.test(text)) {
        return { rows: [] };
      }

      if (/INSERT INTO acquisition_mission_execution_audit/i.test(text)) {
        return { rows: [] };
      }

      if (/SELECT COUNT\(\*\)/i.test(text)) {
        return { rows: [{ count: 0 }] };
      }

      if (/pg_try_advisory_lock/i.test(text)) {
        return { rows: [{ locked: true }] };
      }

      if (/pg_advisory_unlock/i.test(text)) {
        return { rows: [{ unlocked: true }] };
      }

      return { rows: [], rowCount: 0 };
    },
  };

  pool.connect = async () => ({
    query: pool.query.bind(pool),
    release() {},
  });

  return pool;
}

function seedExecuteMission(pool, runtime) {
  const engine = runtime.engine();
  const mission = engine.create({
    id: BACKUS.missionId,
    tenantId: BACKUS.tenantId,
    objective: 'Acquire commercial cleaning customers in Manchester NH for law firms.',
    targetSegment: 'Law Firms',
  });
  engine.store.putMission({
    ...mission,
    id: BACKUS.missionId,
    stage: STAGES.EXECUTE,
    pendingOperatorDecision: null,
    executionSummary: {
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
    missionId: BACKUS.missionId,
    kind: EVENT_KINDS.LAUNCHED,
    specialist: SPECIALISTS.EMMETT,
    label: 'Sent to Backus',
    payload: {
      prospectId: BACKUS.prospectId,
      providerMessageId: BACKUS.brevoMessageId,
      preparedArtifactRevision: BACKUS.preparedArtifactRevision,
    },
  }));
  return mission;
}

describe('Anchor Backus provider observation path', () => {
  beforeEach(() => {
    resetAcquisitionMissionRuntime();
  });

  it('persists mission observations when mission hydration is unavailable', async () => {
    const pool = createBackusPool();
    const sent = backusProviderEvent();

    const result = await consumeMissionProviderEvent(
      { event: sent, inserted: true, duplicate: false },
      pool,
      { persist: true }
    );

    assert.equal(result.skipped, undefined);
    assert.ok(result.observation);
    assert.equal(result.persisted, true);
    assert.equal(result.duplicate, false);
    assert.equal(pool.tables.acquisition_mission_observations.size, 1);
    const row = [...pool.tables.acquisition_mission_observations.values()][0];
    assert.equal(row.id, `obs_${sent.id}`);
    assert.equal(row.payload.kind, OBSERVATION_KINDS.COMMUNICATION_EVIDENCE);
    assert.equal(row.payload.eventType, 'sent');
    assert.equal(row.payload.evidence.executionRecordId, BACKUS.executionId);
    assert.equal(row.payload.evidence.missionProviderEventId, sent.id);
    assert.equal(row.payload.prospectId, BACKUS.prospectId);
  });

  it('insertBrevoEvent sent/opened webhooks create durable mission observations (Backus defaults)', async () => {
    const pool = createBackusPool();
    const runtime = createAcquisitionMissionRuntime({ pool, persist: true, production: false });
    setAcquisitionMissionRuntimeForTests(runtime);
    seedExecuteMission(pool, runtime);
    await runtime.persistMissionState(BACKUS.missionId, { pool, persist: true });

    resetAcquisitionMissionRuntime();
    setAcquisitionMissionRuntimeForTests(createAcquisitionMissionRuntime({ pool, persist: true, production: false }));

    const basePayload = {
      email: BACKUS.recipientEmail,
      client_id: BACKUS.clientId,
      'message-id': BACKUS.brevoMessageId,
      subject: 'Anchor cleaning walkthrough',
      sender: `hello@${BACKUS.sendingDomain}`,
      tags: [
        `mission:${BACKUS.missionId}`,
        `prospect:${BACKUS.prospectId}`,
        `revision:${BACKUS.preparedArtifactRevision}`,
      ],
    };

    const sent = await insertBrevoEvent({
      ...basePayload,
      event: 'request',
      date: '2026-09-14T12:25:00.000Z',
    }, pool);
    assert.ok(sent.mission_provider_event?.event);
    assert.ok(sent.mission_provider_observation?.observation);

    const opened = await insertBrevoEvent({
      ...basePayload,
      event: 'opened',
      date: '2026-09-14T12:35:00.000Z',
    }, pool);
    assert.ok(opened.mission_provider_observation?.observation);

    assert.equal(pool.tables.acquisition_mission_observations.size, 2);
    const observations = [...pool.tables.acquisition_mission_observations.values()];
    const eventTypes = observations.map((row) => row.payload.eventType).sort();
    assert.deepEqual(eventTypes, ['opened', 'sent']);
    for (const row of observations) {
      assert.equal(row.mission_id, BACKUS.missionId);
      assert.equal(row.payload.evidence.executionRecordId, BACKUS.executionId);
      assert.equal(row.payload.prospectId, BACKUS.prospectId);
    }
  });

  it('webhook replay does not duplicate mission observations', async () => {
    const pool = createBackusPool();
    const event = backusProviderEvent();

    const first = await consumeMissionProviderEvent({ event, inserted: true, duplicate: false }, pool, { persist: true });
    const second = await consumeMissionProviderEvent({ event, inserted: false, duplicate: true }, pool, { persist: true });

    assert.equal(first.duplicate, false);
    assert.equal(second.duplicate, true);
    assert.equal(pool.tables.acquisition_mission_observations.size, 1);
    assert.equal(first.observation.id, second.observation.id);
  });

  it('backfills when provider events have NULL tenant_id (production shape)', async () => {
    const pool = createBackusPool();
    pool.tables.executions.set(BACKUS.executionId, {
      ...pool.tables.executions.get(BACKUS.executionId),
      tenant_id: BACKUS.tenantId,
    });
    pool.tables.providerEvents.set('dedupe-backus-sent-null-tenant', {
      id: 'amo_pe_backus_sent_null_tenant',
      dedupe_key: 'dedupe-backus-sent-null-tenant',
      mission_id: BACKUS.missionId,
      tenant_id: null,
      prospect_id: BACKUS.prospectId,
      execution_record_id: BACKUS.executionId,
      prepared_artifact_revision: BACKUS.preparedArtifactRevision,
      provider: 'brevo',
      provider_message_id: BACKUS.brevoMessageId,
      event_type: 'sent',
      event_category: 'delivery',
      raw_event_type: 'request',
      provider_event_id: 'brevo-backus-sent-null-tenant',
      occurred_at: '2026-09-14T12:25:00.000Z',
      payload: {},
      created_at: '2026-09-14T12:25:01.000Z',
    });

    const report = await backfillMissionObservationsFromProviderEvents({
      missionId: BACKUS.missionId,
      executionRecordId: BACKUS.executionId,
      tenantId: BACKUS.tenantId,
    }, pool, { persist: true, skipStageSideEffects: true });

    assert.equal(report.providerEventCount, 1);
    assert.equal(report.observationsCreated, 1);
    const row = pool.tables.acquisition_mission_observations.get('obs_amo_pe_backus_sent_null_tenant');
    assert.ok(row);
    assert.equal(row.tenant_id, BACKUS.tenantId);
  });

  it('audit queryObservations finds backfilled rows by execution and message id', async () => {
    const pool = createBackusPool();
    const sent = backusProviderEvent({ id: 'amo_pe_audit_sent', dedupeKey: 'dedupe-audit-sent' });
    await consumeMissionProviderEvent({ event: sent, inserted: false, duplicate: true }, pool, { persist: true });

    const opened = backusProviderEvent({
      id: 'amo_pe_audit_opened',
      dedupeKey: 'dedupe-audit-opened',
      eventType: 'opened',
      eventCategory: 'engagement',
      rawEventType: 'opened',
      providerEventId: 'brevo-audit-opened',
      occurredAt: '2026-09-14T12:35:00.000Z',
    });
    await consumeMissionProviderEvent({ event: opened, inserted: false, duplicate: true }, pool, { persist: true });

    const rows = await queryObservations(pool, {
      missionId: BACKUS.missionId,
      executionId: BACKUS.executionId,
      messageIds: { variants: [BACKUS.brevoMessageId, BACKUS.brevoMessageId.replace(/^<|>$/g, '')] },
    });

    assert.equal(rows.length, 2);
    const summaries = rows.map(summarizeObservation);
    assert.deepEqual(summaries.map((row) => row.event_type).sort(), ['opened', 'sent']);
    for (const summary of summaries) {
      assert.equal(summary.execution_record_id, BACKUS.executionId);
      assert.equal(summary.provider_message_id, BACKUS.brevoMessageId);
    }
  });

  it('backfills observations from existing provider events without resending mail', async () => {
    const pool = createBackusPool();
    pool.tables.providerEvents.set('dedupe-backus-sent', {
      id: 'amo_pe_backus_sent',
      dedupe_key: 'dedupe-backus-sent',
      mission_id: BACKUS.missionId,
      tenant_id: BACKUS.tenantId,
      prospect_id: BACKUS.prospectId,
      execution_record_id: BACKUS.executionId,
      prepared_artifact_revision: BACKUS.preparedArtifactRevision,
      provider: 'brevo',
      provider_message_id: BACKUS.brevoMessageId,
      event_type: 'sent',
      event_category: 'delivery',
      raw_event_type: 'request',
      provider_event_id: 'brevo-backus-sent-1',
      occurred_at: '2026-09-14T12:25:00.000Z',
      payload: {},
      created_at: '2026-09-14T12:25:01.000Z',
    });
    pool.tables.providerEvents.set('dedupe-backus-opened', {
      id: 'amo_pe_backus_opened',
      dedupe_key: 'dedupe-backus-opened',
      mission_id: BACKUS.missionId,
      tenant_id: BACKUS.tenantId,
      prospect_id: BACKUS.prospectId,
      execution_record_id: BACKUS.executionId,
      prepared_artifact_revision: BACKUS.preparedArtifactRevision,
      provider: 'brevo',
      provider_message_id: BACKUS.brevoMessageId,
      event_type: 'opened',
      event_category: 'engagement',
      raw_event_type: 'opened',
      provider_event_id: 'brevo-backus-opened-1',
      occurred_at: '2026-09-14T12:35:00.000Z',
      payload: { open_source: 'human' },
      created_at: '2026-09-14T12:35:01.000Z',
    });

    const report = await backfillMissionObservationsFromProviderEvents({
      missionId: BACKUS.missionId,
      executionRecordId: BACKUS.executionId,
      tenantId: BACKUS.tenantId,
    }, pool, { persist: true });

    assert.equal(report.providerEventCount, 2);
    assert.equal(report.observationsCreated, 2);
    assert.equal(pool.tables.acquisition_mission_observations.size, 2);
  });
});
