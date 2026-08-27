'use strict';

/**
 * SPEC-071 / canonical provider correlation — durable outbound execution records
 * and mission-bound provider events.
 */

const crypto = require('crypto');

function defaultPool() {
  return require('../db');
}

const DELIVERY_EVENT_TYPES = new Set([
  'sent',
  'delivered',
  'deferred',
  'soft_bounce',
  'hard_bounce',
  'blocked',
  'spam',
  'invalid',
  'error',
]);

const ENGAGEMENT_EVENT_TYPES = new Set([
  'opened',
  'opened_proxy',
  'clicked',
  'replied',
  'unsubscribed',
]);

function providerEventCategory(eventType) {
  const type = String(eventType || '').toLowerCase();
  if (DELIVERY_EVENT_TYPES.has(type)) return 'delivery';
  if (ENGAGEMENT_EVENT_TYPES.has(type)) return 'engagement';
  return 'engagement';
}

function executionRecordFromRow(row) {
  if (!row) return null;
  return {
    id: row.id,
    missionId: row.mission_id,
    tenantId: row.tenant_id,
    prospectId: row.prospect_id,
    preparedArtifactRevision: row.prepared_artifact_revision,
    executionApprovalContributionId: row.execution_approval_contribution_id,
    provider: row.provider,
    providerMessageId: row.provider_message_id,
    status: row.status,
    providerErrorCode: row.provider_error_code,
    providerErrorMessage: row.provider_error_message,
    executionRequestId: row.execution_request_id,
    transactionId: row.transaction_id,
    executionIdentity: row.execution_identity,
    idempotencyKey: row.idempotency_key,
    attemptedAt: row.attempted_at,
    sentAt: row.sent_at,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    payload: row.payload && typeof row.payload === 'object' ? row.payload : {},
  };
}

function providerEventFromRow(row) {
  if (!row) return null;
  return {
    id: row.id,
    dedupeKey: row.dedupe_key,
    missionId: row.mission_id,
    tenantId: row.tenant_id,
    prospectId: row.prospect_id,
    executionRecordId: row.execution_record_id,
    preparedArtifactRevision: row.prepared_artifact_revision,
    provider: row.provider,
    providerMessageId: row.provider_message_id,
    eventType: row.event_type,
    eventCategory: row.event_category,
    rawEventType: row.raw_event_type,
    providerEventId: row.provider_event_id,
    occurredAt: row.occurred_at,
    payload: row.payload && typeof row.payload === 'object' ? row.payload : {},
    createdAt: row.created_at,
  };
}

async function ensureOutboundExecutionSchema(pool = defaultPool()) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS acquisition_mission_outbound_executions (
      id TEXT PRIMARY KEY,
      mission_id TEXT NOT NULL,
      tenant_id TEXT,
      prospect_id TEXT NOT NULL,
      prepared_artifact_revision TEXT NOT NULL,
      execution_approval_contribution_id TEXT,
      provider TEXT NOT NULL DEFAULT 'brevo',
      provider_message_id TEXT,
      status TEXT NOT NULL,
      provider_error_code TEXT,
      provider_error_message TEXT,
      execution_request_id TEXT,
      transaction_id TEXT,
      execution_identity TEXT NOT NULL,
      idempotency_key TEXT,
      attempted_at TIMESTAMPTZ,
      sent_at TIMESTAMPTZ,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS acquisition_mission_outbound_executions_identity_sent_idx
      ON acquisition_mission_outbound_executions (execution_identity)
      WHERE status = 'sent'
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS acquisition_mission_outbound_executions_mission_idx
      ON acquisition_mission_outbound_executions (mission_id, attempted_at DESC)
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS acquisition_mission_outbound_executions_prospect_idx
      ON acquisition_mission_outbound_executions (prospect_id, mission_id)
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS acquisition_mission_outbound_executions_provider_msg_idx
      ON acquisition_mission_outbound_executions (provider_message_id)
      WHERE provider_message_id IS NOT NULL
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS acquisition_mission_provider_events (
      id TEXT PRIMARY KEY,
      dedupe_key TEXT NOT NULL UNIQUE,
      mission_id TEXT NOT NULL,
      tenant_id TEXT,
      prospect_id TEXT NOT NULL,
      execution_record_id TEXT NOT NULL REFERENCES acquisition_mission_outbound_executions(id),
      prepared_artifact_revision TEXT NOT NULL,
      provider TEXT NOT NULL DEFAULT 'brevo',
      provider_message_id TEXT,
      event_type TEXT NOT NULL,
      event_category TEXT NOT NULL,
      raw_event_type TEXT,
      provider_event_id TEXT,
      occurred_at TIMESTAMPTZ NOT NULL,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS acquisition_mission_provider_events_execution_idx
      ON acquisition_mission_provider_events (execution_record_id, occurred_at DESC)
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS acquisition_mission_provider_events_mission_idx
      ON acquisition_mission_provider_events (mission_id, occurred_at DESC)
  `);
}

async function persistOutboundExecution(record, pool = defaultPool(), opts = {}) {
  if (!record?.id) return null;
  if (opts.skipEnsure !== true) await ensureOutboundExecutionSchema(pool);

  await pool.query(
    `INSERT INTO acquisition_mission_outbound_executions (
       id, mission_id, tenant_id, prospect_id, prepared_artifact_revision,
       execution_approval_contribution_id, provider, provider_message_id, status,
       provider_error_code, provider_error_message, execution_request_id, transaction_id,
       execution_identity, idempotency_key, attempted_at, sent_at, payload, created_at, updated_at
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
     ON CONFLICT (id) DO UPDATE SET
       provider_message_id = COALESCE(EXCLUDED.provider_message_id, acquisition_mission_outbound_executions.provider_message_id),
       status = EXCLUDED.status,
       provider_error_code = EXCLUDED.provider_error_code,
       provider_error_message = EXCLUDED.provider_error_message,
       sent_at = COALESCE(EXCLUDED.sent_at, acquisition_mission_outbound_executions.sent_at),
       payload = EXCLUDED.payload,
       updated_at = EXCLUDED.updated_at`,
    [
      record.id,
      record.missionId,
      record.tenantId != null ? String(record.tenantId) : null,
      String(record.prospectId),
      record.preparedArtifactRevision,
      record.executionApprovalContributionId || null,
      record.provider || 'brevo',
      record.providerMessageId || null,
      record.status,
      record.providerErrorCode || null,
      record.providerErrorMessage || null,
      record.executionRequestId || null,
      record.transactionId || null,
      record.executionIdentity,
      record.idempotencyKey || null,
      record.attemptedAt || null,
      record.sentAt || null,
      record.payload || {},
      record.createdAt || new Date().toISOString(),
      record.updatedAt || new Date().toISOString(),
    ]
  );
  return record;
}

async function findOutboundExecutionByProviderMessageId(providerMessageId, pool = defaultPool(), opts = {}) {
  if (!providerMessageId) return null;
  if (opts.skipEnsure !== true) await ensureOutboundExecutionSchema(pool);
  const result = await pool.query(
    `SELECT * FROM acquisition_mission_outbound_executions
     WHERE provider_message_id = $1
     ORDER BY sent_at DESC NULLS LAST, updated_at DESC
     LIMIT 1`,
    [String(providerMessageId)]
  );
  return executionRecordFromRow(result.rows[0]);
}

async function findOutboundExecutionByMissionBinding(
  { missionId, prospectId, preparedArtifactRevision },
  pool = defaultPool(),
  opts = {}
) {
  if (!missionId || !prospectId || !preparedArtifactRevision) return null;
  if (opts.skipEnsure !== true) await ensureOutboundExecutionSchema(pool);
  const result = await pool.query(
    `SELECT * FROM acquisition_mission_outbound_executions
     WHERE mission_id = $1
       AND prospect_id = $2
       AND prepared_artifact_revision = $3
       AND status = 'sent'
     ORDER BY sent_at DESC NULLS LAST
     LIMIT 1`,
    [String(missionId), String(prospectId), String(preparedArtifactRevision)]
  );
  return executionRecordFromRow(result.rows[0]);
}

function deriveProviderEventDedupeKey(input = {}) {
  const providerEventId = input.providerEventId || input.provider_event_id;
  if (providerEventId) {
    return crypto.createHash('sha256').update([
      input.executionRecordId || '',
      input.eventType || '',
      String(providerEventId),
    ].join('|')).digest('hex');
  }
  return crypto.createHash('sha256').update([
    input.executionRecordId || '',
    input.eventType || '',
    input.providerMessageId || '',
    input.occurredAt || '',
    input.rawEventType || '',
    input.link || '',
  ].join('|')).digest('hex');
}

function buildProviderEventId(input = {}) {
  const dedupeKey = deriveProviderEventDedupeKey(input);
  return `amo_pe_${dedupeKey.slice(0, 24)}`;
}

async function persistMissionProviderEvent(input = {}, pool = defaultPool(), opts = {}) {
  if (!input.executionRecordId || !input.missionId || !input.eventType) return null;
  if (opts.skipEnsure !== true) await ensureOutboundExecutionSchema(pool);

  const dedupeKey = deriveProviderEventDedupeKey(input);
  const id = input.id || buildProviderEventId(input);
  const eventCategory = input.eventCategory || providerEventCategory(input.eventType);

  const result = await pool.query(
    `INSERT INTO acquisition_mission_provider_events (
       id, dedupe_key, mission_id, tenant_id, prospect_id, execution_record_id,
       prepared_artifact_revision, provider, provider_message_id, event_type,
       event_category, raw_event_type, provider_event_id, occurred_at, payload
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
     ON CONFLICT (dedupe_key) DO NOTHING
     RETURNING *, (xmax = 0) AS inserted`,
    [
      id,
      dedupeKey,
      input.missionId,
      input.tenantId != null ? String(input.tenantId) : null,
      String(input.prospectId),
      input.executionRecordId,
      input.preparedArtifactRevision,
      input.provider || 'brevo',
      input.providerMessageId || null,
      input.eventType,
      eventCategory,
      input.rawEventType || null,
      input.providerEventId || null,
      input.occurredAt || new Date().toISOString(),
      input.payload || {},
    ]
  );

  if (!result.rows[0]) {
    const existing = await pool.query(
      'SELECT * FROM acquisition_mission_provider_events WHERE dedupe_key = $1 LIMIT 1',
      [dedupeKey]
    );
    return {
      event: providerEventFromRow(existing.rows[0]),
      inserted: false,
      duplicate: true,
    };
  }

  return {
    event: providerEventFromRow(result.rows[0]),
    inserted: result.rows[0].inserted === true,
    duplicate: false,
  };
}

async function logCorrelationFailure(input = {}, pool = defaultPool()) {
  try {
    await pool.query(`
      INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, ran_at, client_id)
      VALUES ('riley', 'brevo_correlation_failed', $1, $2::jsonb, 'skipped', NOW(), $3)
    `, [
      input.prospectId || null,
      JSON.stringify({
        reason: input.reason || 'unknown',
        provider_message_id: input.providerMessageId || null,
        recipient_email: input.recipientEmail || null,
        event_type: input.eventType || null,
      }),
      input.clientId || null,
    ]);
  } catch (err) {
    console.warn('[brevo] correlation failure log skipped:', err.message);
  }
}

module.exports = {
  DELIVERY_EVENT_TYPES,
  ENGAGEMENT_EVENT_TYPES,
  providerEventCategory,
  ensureOutboundExecutionSchema,
  executionRecordFromRow,
  providerEventFromRow,
  persistOutboundExecution,
  findOutboundExecutionByProviderMessageId,
  findOutboundExecutionByMissionBinding,
  deriveProviderEventDedupeKey,
  buildProviderEventId,
  persistMissionProviderEvent,
  logCorrelationFailure,
};
