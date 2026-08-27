'use strict';

/**
 * Mission-bound Riley reply interpretation.
 * inbound reply → execution correlation → missionId → Riley classification → interpretation → outcome.
 */

const {
  CORRELATION_SOURCES,
  correlateBrevoSend,
  isCanonicalCorrelation,
} = require('../utils/brevoSendCorrelation');
const {
  findOutboundExecutionByProviderMessageId,
} = require('./acquisitionMissionOutboundPersistence');
const { getAcquisitionMissionRuntime } = require('./acquisitionMissionRuntime');

function defaultPool() {
  return require('../db');
}

function extractMessageIds(headerValue) {
  if (!headerValue) return [];
  return String(headerValue)
    .split(/\s+/)
    .map((part) => part.replace(/^<|>$/g, '').trim().toLowerCase())
    .filter(Boolean);
}

function correlationFromExecutionRecord(record) {
  if (!record) return null;
  return {
    source: CORRELATION_SOURCES.CANONICAL_EXECUTION,
    missionId: record.missionId,
    tenantId: record.tenantId,
    prospectId: record.prospectId != null ? String(record.prospectId) : null,
    executionRecordId: record.id,
    providerMessageId: record.providerMessageId || null,
    preparedArtifactRevision: record.preparedArtifactRevision || null,
  };
}

function correlationFromLegacySend(sendRow) {
  if (!sendRow) return null;
  return {
    source: CORRELATION_SOURCES.LEGACY_AGENT_LOG,
    missionId: null,
    tenantId: sendRow.client_id != null ? String(sendRow.client_id) : null,
    prospectId: sendRow.prospect_id != null ? String(sendRow.prospect_id) : null,
    executionRecordId: null,
    providerMessageId: sendRow.payload?.message_id || null,
    preparedArtifactRevision: null,
  };
}

/**
 * Resolve mission binding for an inbound Riley reply.
 * Canonical execution record first; legacy agent_log fallback without fabricated mission IDs.
 */
async function correlateRileyReplyToMission(input = {}, db = defaultPool()) {
  const {
    email,
    prospectId,
    clientId,
    inReplyToHeader,
  } = input;

  const inReplyToIds = extractMessageIds(inReplyToHeader || email?.inReplyTo);

  for (const messageId of inReplyToIds) {
    const canonical = await findOutboundExecutionByProviderMessageId(messageId, db, { skipEnsure: true });
    if (canonical) {
      const correlation = correlationFromExecutionRecord(canonical);
      if (!prospectId || String(correlation.prospectId) === String(prospectId)) {
        return { correlation, missionBound: true };
      }
    }
  }

  if (inReplyToIds.length) {
    const brevoCorrelation = await correlateBrevoSend({
      messageId: inReplyToIds[0],
      email: email?.from || input.senderEmail,
      clientId,
      subject: email?.subject,
      tags: email?.tags,
    }, db);

    if (isCanonicalCorrelation(brevoCorrelation) && brevoCorrelation.missionId) {
      return {
        correlation: {
          source: brevoCorrelation.source,
          missionId: brevoCorrelation.missionId,
          tenantId: brevoCorrelation.tenantId,
          prospectId: brevoCorrelation.prospectId != null ? String(brevoCorrelation.prospectId) : null,
          executionRecordId: brevoCorrelation.executionRecordId,
          providerMessageId: brevoCorrelation.providerMessageId,
          preparedArtifactRevision: brevoCorrelation.preparedArtifactRevision,
        },
        missionBound: true,
      };
    }

    if (brevoCorrelation.source === CORRELATION_SOURCES.LEGACY_AGENT_LOG) {
      return {
        correlation: {
          source: brevoCorrelation.source,
          missionId: null,
          tenantId: brevoCorrelation.tenantId,
          prospectId: brevoCorrelation.prospectId != null ? String(brevoCorrelation.prospectId) : null,
          executionRecordId: null,
          providerMessageId: brevoCorrelation.providerMessageId,
          preparedArtifactRevision: null,
        },
        missionBound: false,
      };
    }
  }

  return {
    correlation: {
      source: CORRELATION_SOURCES.NONE,
      missionId: null,
      tenantId: clientId != null ? String(clientId) : null,
      prospectId: prospectId != null ? String(prospectId) : null,
      executionRecordId: null,
      providerMessageId: null,
      preparedArtifactRevision: null,
    },
    missionBound: false,
  };
}

/**
 * Apply Riley reply classification to a mission-bound canonical interpretation.
 */
async function consumeRileyReplyInterpretation(input = {}, db = defaultPool(), opts = {}) {
  const {
    prospect,
    email,
    classification,
    clientId,
    replyText,
  } = input;

  if (!prospect?.id || !classification) {
    return { skipped: true, reason: 'missing_prospect_or_classification' };
  }

  const { correlation, missionBound } = await correlateRileyReplyToMission({
    email,
    prospectId: prospect.id,
    clientId,
    inReplyToHeader: email?.inReplyTo,
  }, db);

  if (!missionBound || !correlation.missionId) {
    return {
      skipped: true,
      reason: 'no_mission_binding',
      missionBound: false,
      correlation,
    };
  }

  const runtime = opts.runtime || getAcquisitionMissionRuntime({
    pool: opts.pool || db,
    persist: opts.persist !== false,
    production: false,
  });

  await runtime.hydrate(correlation.tenantId, { pool: opts.pool || db, persist: opts.persist });

  const engine = runtime.engine();
  const mission = engine.get(correlation.missionId, correlation.tenantId);
  if (!mission) {
    return { skipped: true, reason: 'mission_not_found', correlation };
  }

  const priorObservations = engine.store.listObservations(mission.id);
  const repliedObservation = priorObservations.find(
    (row) => row.prospectId === String(prospect.id) && row.eventType === 'replied'
  );

  const result = engine.applyRileyReplyInterpretation({
    missionId: mission.id,
    prospectId: String(prospect.id),
    classification,
    replyText: replyText || email?.body || email?.snippet || '',
    observationId: repliedObservation?.id || null,
    correlation,
  }, { tenantId: correlation.tenantId });

  if (opts.persist !== false) {
    await runtime.persistMissionState(mission.id, { pool: opts.pool || db, persist: opts.persist });
  }

  return {
    ...result,
    missionBound: true,
    correlation,
    missionId: mission.id,
  };
}

module.exports = {
  correlateRileyReplyToMission,
  consumeRileyReplyInterpretation,
  extractMessageIds,
  correlationFromExecutionRecord,
  correlationFromLegacySend,
};
