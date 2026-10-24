'use strict';

/**
 * Canonical provider event correlation — durable execution records + webhook correlation.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('crypto');

const {
  CORRELATION_SOURCES,
  correlateBrevoSend,
  parseBrevoTags,
  tagsContradictRecord,
} = require('../utils/brevoSendCorrelation');
const {
  persistOutboundExecution,
  findOutboundExecutionByProviderMessageId,
  persistMissionProviderEvent,
  deriveProviderEventDedupeKey,
  providerEventCategory,
} = require('../services/acquisitionMissionOutboundPersistence');
const { buildExecutionRecord, EXECUTION_RECORD_STATUS } = require('../packages/acquisition-mission/OutboundExecution');

function createMemoryPool() {
  const store = {
    executions: new Map(),
    providerEvents: new Map(),
    agentLog: [],
    prospects: new Map(),
  };

  function executionKey(id) {
    return String(id);
  }

  const pool = {
    get executions() { return store.executions; },
    set executions(value) { store.executions = value; },
    get agentLog() { return store.agentLog; },
    get prospects() { return store.prospects; },
    get providerEvents() { return store.providerEvents; },
    async query(sql, params = []) {
      const text = String(sql).replace(/\s+/g, ' ').trim();
      const executions = store.executions;
      const providerEvents = store.providerEvents;
      const agentLog = store.agentLog;
      const prospects = store.prospects;

      if (text.startsWith('CREATE TABLE') || text.startsWith('CREATE UNIQUE INDEX') || text.startsWith('CREATE INDEX') || text.startsWith('DO $$')) {
        return { rows: [] };
      }

      if (text.includes('INSERT INTO acquisition_mission_outbound_executions')) {
        const [
          id, missionId, tenantId, prospectId, revision,
          approvalId, provider, providerMessageId, status,
        ] = params;
        const row = {
          id,
          mission_id: missionId,
          tenant_id: tenantId,
          prospect_id: prospectId,
          prepared_artifact_revision: revision,
          execution_approval_contribution_id: approvalId,
          provider,
          provider_message_id: providerMessageId,
          status,
          provider_error_code: params[9] || null,
          provider_error_message: params[10] || null,
          execution_request_id: params[11] || null,
          transaction_id: params[12] || null,
          execution_identity: params[13],
          idempotency_key: params[14] || null,
          attempted_at: params[15] || null,
          sent_at: params[16] || null,
          payload: params[17] || {},
          created_at: params[18] || new Date().toISOString(),
          updated_at: params[19] || new Date().toISOString(),
        };
        executions.set(executionKey(id), row);
        return { rows: [row], rowCount: 1 };
      }

      if (text.includes('FROM acquisition_mission_outbound_executions') && text.includes('provider_message_id = $1')) {
        const messageId = params[0];
        const match = [...executions.values()].find((row) => row.provider_message_id === messageId);
        return { rows: match ? [match] : [] };
      }

      if (
        text.includes('FROM acquisition_mission_outbound_executions')
        && text.includes('mission_id = $1')
        && text.includes('prospect_id = $2')
      ) {
        const [missionId, prospectId, revision] = params;
        const match = [...executions.values()].find((row) =>
          row.mission_id === missionId
          && row.prospect_id === prospectId
          && row.prepared_artifact_revision === revision
          && row.status === 'sent');
        return { rows: match ? [match] : [] };
      }

      if (text.includes('INSERT INTO acquisition_mission_provider_events')) {
        const dedupeKey = params[1];
        if (providerEvents.has(dedupeKey)) {
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
        providerEvents.set(dedupeKey, row);
        return { rows: [row], rowCount: 1 };
      }

      if (text.includes('FROM acquisition_mission_provider_events WHERE dedupe_key = $1')) {
        const row = providerEvents.get(params[0]);
        return { rows: row ? [row] : [] };
      }

      if (text.includes('FROM agent_log') && text.includes("payload->>'message_id' = $1")) {
        const messageId = params[0];
        const match = agentLog.find((row) => row.payload?.message_id === messageId);
        return { rows: match ? [match] : [] };
      }

      if (text.includes('FROM agent_log al') && text.includes('JOIN prospects p')) {
        const [email] = params;
        const match = agentLog.find((row) => {
          const prospect = prospects.get(`${row.client_id}:${row.prospect_id}`);
          return prospect && prospect.email === email;
        });
        return { rows: match ? [match] : [] };
      }

      if (text.includes('INSERT INTO agent_log') && text.includes('brevo_correlation_failed')) {
        agentLog.push({ kind: 'correlation_failure', params });
        return { rows: [] };
      }

      return { rows: [], rowCount: 0 };
    },
  };

  return pool;
}

function sampleExecutionRecord(overrides = {}) {
  return buildExecutionRecord({
    id: 'amo_send_test1',
    missionId: 'mission-abc',
    tenantId: '10',
    prospectId: 'co-harbor',
    preparedArtifactRevision: 'rev-hash-1',
    executionApprovalContributionId: 'approval-1',
    provider: 'brevo',
    providerMessageId: 'brevo-msg-123',
    status: EXECUTION_RECORD_STATUS.SENT,
    executionIdentity: crypto.createHash('sha256').update('identity').digest('hex'),
    idempotencyKey: 'exec_identity',
    transactionId: 'txn-1',
    attemptedAt: '2026-08-27T10:00:00.000Z',
    sentAt: '2026-08-27T10:00:01.000Z',
    ...overrides,
  });
}

describe('Canonical provider event correlation', () => {
  let pool;

  beforeEach(() => {
    pool = createMemoryPool();
  });

  it('persists durable execution record and resolves providerMessageId in a fresh pool instance', async () => {
    const record = sampleExecutionRecord();
    await persistOutboundExecution(record, pool, { skipEnsure: true });

    const freshPool = createMemoryPool();
    freshPool.executions = new Map(pool.executions);

    const loaded = await findOutboundExecutionByProviderMessageId('brevo-msg-123', freshPool, { skipEnsure: true });
    assert.ok(loaded, 'expected durable execution record after cross-store reload');
    assert.equal(loaded.missionId, 'mission-abc');
    assert.equal(loaded.prospectId, 'co-harbor');
    assert.equal(loaded.preparedArtifactRevision, 'rev-hash-1');
    assert.equal(loaded.providerMessageId, 'brevo-msg-123');
    assert.equal(loaded.status, 'sent');
  });

  it('correlates Brevo providerMessageId to canonical execution record and recovers missionId', async () => {
    await persistOutboundExecution(sampleExecutionRecord(), pool, { skipEnsure: true });

    const correlation = await correlateBrevoSend({
      messageId: 'brevo-msg-123',
      email: 'alex@harborlaw.com',
      tags: ['mission:mission-abc', 'prospect:co-harbor', 'revision:rev-hash-1'],
    }, pool);

    assert.equal(correlation.source, CORRELATION_SOURCES.CANONICAL_EXECUTION);
    assert.equal(correlation.missionId, 'mission-abc');
    assert.equal(correlation.executionRecordId, 'amo_send_test1');
    assert.equal(correlation.preparedArtifactRevision, 'rev-hash-1');
  });

  it('prefers canonical execution record over legacy agent_log when both match', async () => {
    await persistOutboundExecution(sampleExecutionRecord(), pool, { skipEnsure: true });
    pool.agentLog.push({
      payload: { message_id: 'brevo-msg-123', subject: 'Hello' },
      client_id: 10,
      prospect_id: 999,
      ran_at: new Date().toISOString(),
    });

    const correlation = await correlateBrevoSend({
      messageId: 'brevo-msg-123',
      email: 'alex@harborlaw.com',
    }, pool);

    assert.equal(correlation.source, CORRELATION_SOURCES.CANONICAL_EXECUTION);
    assert.equal(correlation.prospectId, 'co-harbor');
    assert.notEqual(correlation.prospectId, '999');
  });

  it('falls back to legacy agent_log when no canonical record exists', async () => {
    pool.agentLog.push({
      payload: { message_id: 'legacy-msg-1', subject: 'Legacy hello' },
      client_id: 1,
      prospect_id: 42,
      ran_at: new Date().toISOString(),
    });

    const correlation = await correlateBrevoSend({
      messageId: 'legacy-msg-1',
      email: 'legacy@example.com',
    }, pool);

    assert.equal(correlation.source, CORRELATION_SOURCES.LEGACY_AGENT_LOG);
    assert.equal(correlation.prospectId, '42');
    assert.equal(correlation.missionId, null);
  });

  it('uses tags for recovery but rejects contradictory durable record identity', async () => {
    await persistOutboundExecution(sampleExecutionRecord({
      providerMessageId: 'other-msg',
    }), pool, { skipEnsure: true });

    const tagBinding = parseBrevoTags([
      'mission:mission-abc',
      'prospect:co-harbor',
      'revision:rev-hash-1',
    ]);
    assert.equal(tagsContradictRecord({
      missionId: 'mission-wrong',
      prospectId: 'co-harbor',
      preparedArtifactRevision: 'rev-hash-1',
    }, sampleExecutionRecord()), true);

    const correlation = await correlateBrevoSend({
      messageId: 'brevo-msg-123',
      email: 'alex@harborlaw.com',
      tags: tagBinding.missionId
        ? [`mission:${tagBinding.missionId}`, `prospect:${tagBinding.prospectId}`, `revision:${tagBinding.preparedArtifactRevision}`]
        : [],
    }, pool);

    assert.equal(correlation.source, CORRELATION_SOURCES.NONE);
    assert.equal(correlation.missionId, null);
  });

  it('persists multiple provider events against the same execution record', async () => {
    const record = sampleExecutionRecord();
    await persistOutboundExecution(record, pool, { skipEnsure: true });

    const base = {
      missionId: record.missionId,
      tenantId: record.tenantId,
      prospectId: record.prospectId,
      executionRecordId: record.id,
      preparedArtifactRevision: record.preparedArtifactRevision,
      providerMessageId: record.providerMessageId,
    };

    const delivered = await persistMissionProviderEvent({
      ...base,
      eventType: 'delivered',
      providerEventId: 'evt-delivered-1',
      occurredAt: '2026-08-27T10:05:00.000Z',
    }, pool, { skipEnsure: true });
    const opened = await persistMissionProviderEvent({
      ...base,
      eventType: 'opened',
      providerEventId: 'evt-open-1',
      occurredAt: '2026-08-27T10:10:00.000Z',
    }, pool, { skipEnsure: true });
    const clicked = await persistMissionProviderEvent({
      ...base,
      eventType: 'clicked',
      providerEventId: 'evt-click-1',
      occurredAt: '2026-08-27T10:12:00.000Z',
    }, pool, { skipEnsure: true });

    assert.equal(delivered.inserted, true);
    assert.equal(opened.inserted, true);
    assert.equal(clicked.inserted, true);
    assert.equal(pool.providerEvents.size, 3);
    assert.equal(providerEventCategory('delivered'), 'delivery');
    assert.equal(providerEventCategory('opened'), 'engagement');
  });

  it('dedupes duplicate webhook delivery for the same provider event', async () => {
    const record = sampleExecutionRecord();
    await persistOutboundExecution(record, pool, { skipEnsure: true });

    const input = {
      missionId: record.missionId,
      tenantId: record.tenantId,
      prospectId: record.prospectId,
      executionRecordId: record.id,
      preparedArtifactRevision: record.preparedArtifactRevision,
      providerMessageId: record.providerMessageId,
      eventType: 'opened',
      providerEventId: 'evt-open-dup',
      occurredAt: '2026-08-27T10:10:00.000Z',
    };

    const first = await persistMissionProviderEvent(input, pool, { skipEnsure: true });
    const second = await persistMissionProviderEvent(input, pool, { skipEnsure: true });

    assert.equal(first.inserted, true);
    assert.equal(second.duplicate, true);
    assert.equal(pool.providerEvents.size, 1);
    assert.equal(
      deriveProviderEventDedupeKey(input),
      deriveProviderEventDedupeKey(input)
    );
  });

  it('does not fabricate mission association for unknown message ID', async () => {
    const correlation = await correlateBrevoSend({
      messageId: 'unknown-msg-999',
      email: 'nobody@example.com',
    }, pool);

    assert.equal(correlation.source, CORRELATION_SOURCES.NONE);
    assert.equal(correlation.failed, true);
    assert.equal(correlation.missionId, null);
    assert.equal(correlation.executionRecordId, null);
  });
});

console.log('canonicalProviderCorrelation tests defined');
