const crypto = require('crypto');
const pool = require('../db');
const { evaluateProspectShadow } = require('./maxOrchestration');
const { recordMaxMetric, logMaxOrchestrationFailure } = require('./maxOrchestrationObservability');

const MEANINGFUL_EVALUATION_SIGNALS = new Set([
  'company_signal_detected', 'icp_score_changed', 'email_human_opened', 'email_clicked',
  'email_replied', 'email_meaningful_reply', 'email_positive_reply', 'email_negative_reply',
  'email_unsubscribed', 'email_hard_bounced', 'email_hard_bounced_confirmed_invalid',
  'email_soft_bounced', 'contact_invalid', 'enrichment_succeeded', 'enrichment_failed',
  'phone_found', 'email_verified', 'operator_marked_warm', 'operator_marked_hot',
  'email_proxy_reclassified',
  'prospect_qualified', 'meeting_booked', 'meeting_cancelled', 'meeting_showed', 'meeting_no_showed',
]);

function stableSignalId({ source, sourceRecordId, eventType, prospectId }) {
  const digest = crypto.createHash('sha256')
    .update(`${source}:${sourceRecordId}:${eventType}:${prospectId}`)
    .digest('hex');
  return `maxsig_${digest}`;
}

function signalTimestamp(value) {
  if (value === undefined || value === null || value === '') return new Date();
  if (typeof value === 'number' || /^\d+$/.test(String(value))) {
    const number = Number(value);
    const date = new Date(number < 1e12 ? number * 1000 : number);
    if (!Number.isNaN(date.getTime())) return date;
  }
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? new Date() : date;
}

function validateSignal(signal) {
  for (const key of ['prospect_id', 'client_id', 'event_type', 'source', 'source_record_id']) {
    if (signal[key] === undefined || signal[key] === null || signal[key] === '') {
      throw new Error(`Normalized signal requires ${key}`);
    }
  }
}

async function loadClientOrchestrationConfig(db, clientId) {
  const result = await db.query(`
    SELECT id, vertical_tiers, max_orchestration_config
    FROM clients WHERE id = $1
  `, [clientId]);
  return result.rows[0] || { id: clientId, vertical_tiers: {}, max_orchestration_config: {} };
}

async function persistNormalizedSignal(signal, db = pool) {
  validateSignal(signal);
  const id = signal.id || stableSignalId({
    source: signal.source,
    sourceRecordId: signal.source_record_id,
    eventType: signal.event_type,
    prospectId: signal.prospect_id,
  });
  const result = await db.query(`
    INSERT INTO prospect_signal_events
      (id, client_id, prospect_id, company_id, event_type, event_timestamp, source, source_record_id, metadata)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)
    ON CONFLICT DO NOTHING
    RETURNING id, created_at
  `, [
    id, signal.client_id, signal.prospect_id, signal.company_id || null, signal.event_type,
    signalTimestamp(signal.event_timestamp), signal.source, String(signal.source_record_id),
    JSON.stringify(signal.metadata || {}),
  ]);
  return { id, inserted: result.rows.length > 0, created_at: result.rows[0]?.created_at || null };
}

async function ingestNormalizedSignal(signal, { db = pool, env = process.env, evaluate = true, evaluateProspectFn = evaluateProspectShadow } = {}) {
  const persisted = await persistNormalizedSignal(signal, db);
  const meaningful = MEANINGFUL_EVALUATION_SIGNALS.has(signal.event_type);
  if (!persisted.inserted) {
    const existingDecision = await db.query(`
      SELECT id FROM max_decisions
      WHERE client_id = $1 AND trigger_event_id = $2
      ORDER BY created_at DESC LIMIT 1
    `, [signal.client_id, persisted.id]).catch(() => ({ rows: [] }));
    if (!meaningful || existingDecision.rows.length || !evaluate) {
      await recordMaxMetric('max_duplicate_events_suppressed_total', {
        db, clientId: signal.client_id, prospectId: signal.prospect_id,
        signalEventId: persisted.id, dimensions: { source: signal.source, event_type: signal.event_type },
      }).catch(() => {});
      return { inserted: false, duplicate: true, evaluated: false, signal_id: persisted.id };
    }
  }
  if (!meaningful || !evaluate) {
    return { inserted: persisted.inserted, duplicate: !persisted.inserted, evaluated: false, signal_id: persisted.id };
  }

  const clientConfig = await loadClientOrchestrationConfig(db, signal.client_id);
  const triggerEvent = {
    id: persisted.id,
    event_type: signal.event_type,
    event_timestamp: signal.event_timestamp || new Date(),
    source: signal.source,
    source_record_id: String(signal.source_record_id),
    metadata: signal.metadata || {},
  };
  const result = await evaluateProspectFn({
    db,
    prospectId: signal.prospect_id,
    clientId: signal.client_id,
    triggerEvent,
    clientConfig,
    env,
    now: new Date(),
  });
  if (!result.skipped && !result.duplicate) {
    const decisionId = result.decision?.id || null;
    await Promise.allSettled([
      recordMaxMetric('signal_to_decision_duration', {
        db, clientId: signal.client_id,
        value: Math.max(0, Date.now() - signalTimestamp(signal.event_timestamp).getTime()),
        prospectId: signal.prospect_id, signalEventId: persisted.id, decisionId,
      }),
    ]);
  }
  return { ...result, inserted: persisted.inserted, signal_id: persisted.id, evaluated: !result.skipped };
}

async function safeIngestNormalizedSignal(signal, options = {}) {
  try {
    return await ingestNormalizedSignal(signal, options);
  } catch (error) {
    await logMaxOrchestrationFailure({
      db: options.db || pool,
      clientId: signal?.client_id || null,
      prospectId: signal?.prospect_id || null,
      action: 'signal_ingestion_failed',
      error,
      payload: { source: signal?.source, source_record_id: signal?.source_record_id, event_type: signal?.event_type },
    }).catch(() => {});
    console.error('[max_orchestration] signal ingestion failed:', error.message);
    return { failed: true, error: error.message };
  }
}

function normalizeBrevoSignal(result, payload = {}) {
  if (!result?.prospect_id || !result?.client_id || !result?.event_id) return null;
  const type = String(result.event_type || '');
  let eventType = ({
    sent: 'email_sent', delivered: 'email_delivered', clicked: 'email_clicked',
    replied: 'email_replied', soft_bounce: 'email_soft_bounced', hard_bounce: 'email_hard_bounced',
    unsubscribed: 'email_unsubscribed', invalid: 'email_hard_bounced_confirmed_invalid',
  })[type] || null;
  if (['opened', 'open', 'opened_proxy'].includes(type)) {
    eventType = result.open_source === 'human' ? 'email_human_opened'
      : result.open_source === 'proxy' ? 'email_proxy_opened' : 'email_unknown_opened';
  }
  if (!eventType) return null;
  return {
    client_id: result.client_id,
    prospect_id: result.prospect_id,
    event_type: eventType,
    event_timestamp: payload.ts || payload.date || payload.timestamp || new Date(),
    source: 'brevo',
    source_record_id: result.event_id,
    metadata: {
      brevo_event_type: type,
      message_id: payload.messageId || payload.message_id || payload['message-id'] || null,
      open_source: result.open_source || null,
      open_source_reason: result.open_source_reason || null,
      verified: eventType === 'email_clicked' && result.has_corresponding_send !== false,
    },
  };
}

async function safeIngestBrevoSignal(result, payload, options = {}) {
  const signal = normalizeBrevoSignal(result, payload);
  const primary = signal ? await safeIngestNormalizedSignal(signal, options) : { skipped: true, reason: 'unmapped_brevo_event' };
  const corrections = [];
  for (const row of result?.reclassified_proxy_events || []) {
    corrections.push(await safeIngestNormalizedSignal({
      client_id: row.client_id,
      prospect_id: row.prospect_id,
      event_type: 'email_proxy_reclassified',
      event_timestamp: row.event_at,
      source: 'brevo_open_gate',
      source_record_id: `${row.event_id}:batch_proxy`,
      metadata: { original_event_id: row.event_id, open_source: 'proxy', reason: 'batch_fire' },
    }, options));
  }
  return { primary, corrections };
}

function rileyReplyEventType(classification) {
  return ({
    interested: 'email_positive_reply',
    not_now: 'email_meaningful_reply',
    wrong_person: 'email_meaningful_reply',
    negative: 'email_negative_reply',
    unsubscribe: 'email_unsubscribed',
    out_of_office: 'email_out_of_office',
    unknown: 'email_replied',
  })[classification] || 'email_replied';
}

async function safeIngestRileyReplySignal({ prospect, email, classification, clientId }, options = {}) {
  if (!prospect?.id || !email?.id) return { skipped: true, reason: 'missing_prospect_or_message' };
  return safeIngestNormalizedSignal({
    client_id: clientId,
    prospect_id: prospect.id,
    company_id: prospect.company_id || null,
    event_type: rileyReplyEventType(classification),
    event_timestamp: email.date || new Date(),
    source: 'riley_gmail',
    source_record_id: email.id,
    metadata: { classification, gmail_thread_id: email.threadId || null },
  }, options);
}

async function safeIngestIcpScoreChange({ prospectId, clientId, historyId, oldScore, newScore, createdAt }, options = {}) {
  if (!historyId) return { skipped: true, reason: 'missing_history_id' };
  return safeIngestNormalizedSignal({
    client_id: clientId,
    prospect_id: prospectId,
    event_type: 'icp_score_changed',
    event_timestamp: createdAt || new Date(),
    source: 'icp_score_history',
    source_record_id: historyId,
    metadata: { old_score: oldScore, new_score: newScore, delta: Number(newScore || 0) - Number(oldScore || 0) },
  }, options);
}

async function safeIngestEnrichmentOutcome({ prospectId, clientId, sourceRecordId, eventTimestamp, status, payload = {} }, options = {}) {
  if (!prospectId || !sourceRecordId) return { skipped: true, reason: 'missing_enrichment_identity' };
  const found = payload.found || {};
  const updated = payload.updated || {};
  const successful = status === 'success' && (updated.email || updated.phone || payload.resolved || payload.verified_email);
  const signals = [{
    event_type: successful ? 'enrichment_succeeded' : 'enrichment_failed',
    metadata: { provider: payload.provider || payload.source || null, status, result: payload.result || null },
  }];
  if (updated.phone || found.phone || payload.phone_found) signals.push({ event_type: 'phone_found', metadata: { provider: payload.provider || payload.source || null } });
  if (payload.email_verified || payload.verified_email) signals.push({ event_type: 'email_verified', metadata: { provider: payload.provider || payload.source || null } });
  const results = [];
  for (const item of signals) {
    results.push(await safeIngestNormalizedSignal({
      client_id: clientId,
      prospect_id: prospectId,
      event_type: item.event_type,
      event_timestamp: eventTimestamp || new Date(),
      source: 'enrichment',
      source_record_id: String(sourceRecordId),
      metadata: item.metadata,
    }, options));
  }
  return results;
}

module.exports = {
  MEANINGFUL_EVALUATION_SIGNALS,
  ingestNormalizedSignal,
  loadClientOrchestrationConfig,
  normalizeBrevoSignal,
  persistNormalizedSignal,
  rileyReplyEventType,
  safeIngestBrevoSignal,
  safeIngestEnrichmentOutcome,
  safeIngestIcpScoreChange,
  safeIngestNormalizedSignal,
  safeIngestRileyReplySignal,
  signalTimestamp,
  stableSignalId,
  validateSignal,
};
