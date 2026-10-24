'use strict';

/**
 * Canonical Brevo send correlation — providerMessageId → execution record first.
 */

const pool = require('../db');
const {
  findOutboundExecutionByProviderMessageId,
  findOutboundExecutionByMissionBinding,
} = require('../services/acquisitionMissionOutboundPersistence');

const CORRELATION_SOURCES = Object.freeze({
  CANONICAL_EXECUTION: 'canonical_execution',
  LEGACY_AGENT_LOG: 'legacy_agent_log',
  EMAIL_SUBJECT_FALLBACK: 'email_subject_fallback',
  NONE: 'none',
});

function parseBrevoTags(tags = []) {
  const parsed = {
    missionId: null,
    prospectId: null,
    preparedArtifactRevision: null,
  };
  for (const raw of tags || []) {
    const tag = String(raw || '');
    if (tag.startsWith('mission:')) parsed.missionId = tag.slice('mission:'.length) || null;
    else if (tag.startsWith('prospect:')) parsed.prospectId = tag.slice('prospect:'.length) || null;
    else if (tag.startsWith('revision:')) parsed.preparedArtifactRevision = tag.slice('revision:'.length) || null;
  }
  return parsed;
}

function tagsContradictRecord(tagBinding, record) {
  if (!record || !tagBinding) return false;
  if (tagBinding.missionId && tagBinding.missionId !== record.missionId) return true;
  if (tagBinding.prospectId && tagBinding.prospectId !== String(record.prospectId)) return true;
  if (
    tagBinding.preparedArtifactRevision
    && tagBinding.preparedArtifactRevision !== record.preparedArtifactRevision
  ) {
    return true;
  }
  return false;
}

function correlationFromExecutionRecord(record, source = CORRELATION_SOURCES.CANONICAL_EXECUTION) {
  return {
    source,
    executionRecordId: record.id,
    missionId: record.missionId,
    tenantId: record.tenantId,
    prospectId: record.prospectId,
    preparedArtifactRevision: record.preparedArtifactRevision,
    executionApprovalContributionId: record.executionApprovalContributionId || null,
    executionIdentity: record.executionIdentity,
    provider: record.provider || 'brevo',
    providerMessageId: record.providerMessageId || null,
    clientId: record.tenantId != null ? Number(record.tenantId) : null,
    legacyPayload: null,
    legacyRanAt: record.sentAt || record.attemptedAt || null,
  };
}

function correlationFromLegacySend(sendRow) {
  return {
    source: CORRELATION_SOURCES.LEGACY_AGENT_LOG,
    executionRecordId: null,
    missionId: null,
    tenantId: sendRow?.client_id != null ? String(sendRow.client_id) : null,
    prospectId: sendRow?.prospect_id != null ? String(sendRow.prospect_id) : null,
    preparedArtifactRevision: null,
    executionApprovalContributionId: null,
    executionIdentity: null,
    provider: 'brevo',
    providerMessageId: sendRow?.payload?.message_id || null,
    clientId: sendRow?.client_id != null ? Number(sendRow.client_id) : null,
    legacyPayload: sendRow?.payload || null,
    legacyRanAt: sendRow?.ran_at || null,
  };
}

async function findLegacyAgentLogSend({ email, clientId, messageId, subject }, db = pool) {
  if (messageId) {
    const byMessage = await db.query(`
      SELECT payload, client_id, prospect_id, ran_at
      FROM agent_log
      WHERE agent_name = 'emmett'
        AND action = 'email_sent'
        AND payload->>'message_id' = $1
      ORDER BY ran_at DESC
      LIMIT 1
    `, [messageId]);
    if (byMessage.rows.length) return byMessage.rows[0];
  }

  const params = [email, subject || null];
  if (clientId) params.push(clientId);
  const byEmail = await db.query(`
    SELECT al.payload, al.client_id, al.prospect_id, al.ran_at
    FROM agent_log al
    JOIN prospects p ON p.id = al.prospect_id AND p.client_id = al.client_id
    WHERE al.agent_name = 'emmett'
      AND al.action = 'email_sent'
      AND LOWER(p.email) = $1
      AND ($2::text IS NULL OR al.payload->>'subject' = $2)
      ${clientId ? 'AND al.client_id = $3' : ''}
    ORDER BY al.ran_at DESC
    LIMIT 1
  `, params);
  return byEmail.rows[0] || null;
}

/**
 * Resolve send correlation with canonical-first precedence.
 * @returns {Promise<object>}
 */
async function correlateBrevoSend(input = {}, db = pool) {
  const {
    messageId,
    email,
    clientId,
    subject,
    tags,
  } = input;

  const tagBinding = parseBrevoTags(Array.isArray(tags) ? tags : []);

  if (messageId) {
    const canonical = await findOutboundExecutionByProviderMessageId(messageId, db, { skipEnsure: true });
    if (canonical) {
      if (tagsContradictRecord(tagBinding, canonical)) {
        console.warn(
          `[Brevo] Tag binding contradicts canonical execution record id=${canonical.id}; record wins.`
        );
      }
      return correlationFromExecutionRecord(canonical);
    }
  }

  if (tagBinding.missionId && tagBinding.prospectId && tagBinding.preparedArtifactRevision) {
    const fromTags = await findOutboundExecutionByMissionBinding(tagBinding, db, { skipEnsure: true });
    if (fromTags) {
      if (
        messageId
        && fromTags.providerMessageId
        && fromTags.providerMessageId !== messageId
      ) {
        console.warn(
          `[Brevo] Tag-recovered execution record id=${fromTags.id} has conflicting providerMessageId; skipping tag recovery.`
        );
      } else if (!messageId || !fromTags.providerMessageId || fromTags.providerMessageId === messageId) {
        return correlationFromExecutionRecord(fromTags);
      }
    }
  }

  const legacy = await findLegacyAgentLogSend({ email, clientId, messageId, subject }, db);
  if (legacy) {
    const legacyCorrelation = correlationFromLegacySend(legacy);
    if (
      tagBinding.missionId
      && legacyCorrelation.missionId == null
      && tagBinding.prospectId
      && String(legacy.prospect_id) !== tagBinding.prospectId
    ) {
      return {
        source: CORRELATION_SOURCES.NONE,
        failed: true,
        reason: 'legacy_prospect_contradicts_tags',
        executionRecordId: null,
        missionId: null,
        tenantId: null,
        prospectId: null,
        preparedArtifactRevision: null,
        executionApprovalContributionId: null,
        executionIdentity: null,
        provider: 'brevo',
        providerMessageId: messageId || null,
        clientId: legacy.client_id != null ? Number(legacy.client_id) : null,
        legacyPayload: legacy.payload || null,
        legacyRanAt: legacy.ran_at || null,
      };
    }
    return legacyCorrelation;
  }

  return {
    source: CORRELATION_SOURCES.NONE,
    failed: messageId ? true : false,
    reason: messageId ? 'no_matching_send' : 'missing_message_id',
    executionRecordId: null,
    missionId: null,
    tenantId: null,
    prospectId: null,
    preparedArtifactRevision: null,
    executionApprovalContributionId: null,
    executionIdentity: null,
    provider: 'brevo',
    providerMessageId: messageId || null,
    clientId: clientId || null,
    legacyPayload: null,
    legacyRanAt: null,
  };
}

function isCanonicalCorrelation(correlation = {}) {
  return correlation.source === CORRELATION_SOURCES.CANONICAL_EXECUTION;
}

function legacySendMatchFromCorrelation(correlation = {}) {
  if (correlation.source === CORRELATION_SOURCES.LEGACY_AGENT_LOG) {
    return {
      payload: correlation.legacyPayload,
      client_id: correlation.clientId,
      prospect_id: correlation.prospectId,
      ran_at: correlation.legacyRanAt,
    };
  }
  if (isCanonicalCorrelation(correlation)) {
    return {
      payload: {
        message_id: correlation.providerMessageId,
        mission_id: correlation.missionId,
        prepared_artifact_revision: correlation.preparedArtifactRevision,
      },
      client_id: correlation.clientId,
      prospect_id: correlation.prospectId,
      ran_at: correlation.legacyRanAt,
    };
  }
  return null;
}

module.exports = {
  CORRELATION_SOURCES,
  parseBrevoTags,
  tagsContradictRecord,
  correlateBrevoSend,
  isCanonicalCorrelation,
  legacySendMatchFromCorrelation,
  findLegacyAgentLogSend,
  correlationFromExecutionRecord,
};
