'use strict';

/**
 * Canonical communication observation — structured provider evidence on missions.
 * Provider telemetry is evidence, not business judgment (no outcomes here).
 */

const { asText, nowIso, clone } = require('./types');

const OBSERVATION_KINDS = Object.freeze({
  COMMUNICATION_EVIDENCE: 'communication_evidence',
});

const COMMUNICATION_SOURCES = Object.freeze({
  PROVIDER_WEBHOOK: 'provider_webhook',
});

/** Spec taxonomy — only these event types become communication observations. */
const COMMUNICATION_DELIVERY_TYPES = new Set([
  'sent',
  'delivered',
  'deferred',
  'soft_bounce',
  'hard_bounce',
  'blocked',
  'spam',
]);

const COMMUNICATION_ENGAGEMENT_TYPES = new Set([
  'opened',
  'opened_proxy',
  'clicked',
  'replied',
  'unsubscribed',
]);

const COMMUNICATION_EVENT_TYPES = new Set([
  ...COMMUNICATION_DELIVERY_TYPES,
  ...COMMUNICATION_ENGAGEMENT_TYPES,
]);

function communicationCategory(eventType) {
  const type = asText(eventType).toLowerCase();
  if (COMMUNICATION_DELIVERY_TYPES.has(type)) return 'delivery';
  if (COMMUNICATION_ENGAGEMENT_TYPES.has(type)) return 'engagement';
  return null;
}

function isCommunicationEvidenceEventType(eventType) {
  return COMMUNICATION_EVENT_TYPES.has(asText(eventType).toLowerCase());
}

function buildCommunicationObservationId(providerEvent = {}) {
  const providerEventRowId = asText(providerEvent.id);
  if (providerEventRowId) return `obs_${providerEventRowId}`;
  const providerEventId = asText(providerEvent.providerEventId);
  if (providerEventId) {
    return `obs_pe_${providerEventId.replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 48)}`;
  }
  return null;
}

function formatObservationSummary(providerEvent = {}) {
  const eventType = asText(providerEvent.eventType).toLowerCase();
  const provider = asText(providerEvent.provider) || 'provider';
  const prospectId = asText(providerEvent.prospectId);
  const prospect = prospectId ? `prospect ${prospectId}` : 'prospect';
  return `${provider} ${eventType} for ${prospect}`;
}

/**
 * @param {object} providerEvent — row from acquisition_mission_provider_events
 * @returns {object|null}
 */
function createCommunicationObservation(providerEvent = {}) {
  const eventType = asText(providerEvent.eventType).toLowerCase();
  if (!isCommunicationEvidenceEventType(eventType)) return null;

  const category = communicationCategory(eventType);
  if (!category) return null;

  const id = buildCommunicationObservationId(providerEvent);
  if (!id) return null;

  const payload = providerEvent.payload && typeof providerEvent.payload === 'object'
    ? providerEvent.payload
    : {};

  return {
    id,
    missionId: asText(providerEvent.missionId),
    prospectId: providerEvent.prospectId != null ? String(providerEvent.prospectId) : null,
    kind: OBSERVATION_KINDS.COMMUNICATION_EVIDENCE,
    category,
    eventType,
    occurredAt: nowIso(providerEvent.occurredAt || providerEvent.createdAt),
    evidence: {
      provider: asText(providerEvent.provider) || 'brevo',
      providerEventId: providerEvent.providerEventId || providerEvent.id || null,
      providerMessageId: providerEvent.providerMessageId || null,
      executionRecordId: providerEvent.executionRecordId || null,
      preparedArtifactRevision: providerEvent.preparedArtifactRevision || null,
      missionProviderEventId: providerEvent.id || null,
    },
    payload: {
      openSource: payload.open_source || payload.openSource || null,
      link: payload.link || null,
      rawEventType: providerEvent.rawEventType || null,
    },
    source: COMMUNICATION_SOURCES.PROVIDER_WEBHOOK,
    specialist: 'emmett',
    observation: formatObservationSummary(providerEvent),
    at: nowIso(providerEvent.occurredAt || providerEvent.createdAt),
  };
}

function isCommunicationObservation(row = {}) {
  return asText(row.kind).toLowerCase() === OBSERVATION_KINDS.COMMUNICATION_EVIDENCE;
}

module.exports = {
  OBSERVATION_KINDS,
  COMMUNICATION_SOURCES,
  COMMUNICATION_DELIVERY_TYPES,
  COMMUNICATION_ENGAGEMENT_TYPES,
  COMMUNICATION_EVENT_TYPES,
  communicationCategory,
  isCommunicationEvidenceEventType,
  buildCommunicationObservationId,
  createCommunicationObservation,
  isCommunicationObservation,
  formatObservationSummary,
};
