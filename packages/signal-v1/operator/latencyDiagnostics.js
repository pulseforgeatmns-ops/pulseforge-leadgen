'use strict';

const { knowledgeAt } = require('../prospective/knowledgeClock');

function msBetween(later, earlier) {
  const a = Date.parse(later);
  const b = Date.parse(earlier);
  if (!Number.isFinite(a) || !Number.isFinite(b) || a < b) return null;
  return a - b;
}

function buildLatencyDiagnostics(evidence, timestamps = {}) {
  const occurredAt = evidence?.occurredAt;
  const ingestedAt = evidence?.ingestedAt;
  const knowledge = knowledgeAt(occurredAt, ingestedAt);
  const pulseForgeReceivedAt = timestamps.pulseForgeReceivedAt || null;
  const callPersistedAt = timestamps.callPersistedAt || null;
  const alertCreatedAt = timestamps.alertCreatedAt || timestamps.createdAt || null;
  const transportSentAt = timestamps.transportSentAt || null;

  return {
    telegramPostAt: occurredAt ? new Date(occurredAt).toISOString() : null,
    callerServiceIngestedAt: ingestedAt ? new Date(ingestedAt).toISOString() : null,
    signalKnowledgeAt: Number.isFinite(knowledge.getTime()) ? knowledge.toISOString() : null,
    pulseForgeReceivedAt: pulseForgeReceivedAt ? new Date(pulseForgeReceivedAt).toISOString() : null,
    callPersistedAt: callPersistedAt ? new Date(callPersistedAt).toISOString() : null,
    alertCreatedAt: alertCreatedAt ? new Date(alertCreatedAt).toISOString() : null,
    transportSentAt: transportSentAt ? new Date(transportSentAt).toISOString() : null,
    telegramToCallerServiceMs: msBetween(ingestedAt, occurredAt),
    callerServiceToPulseForgeMs: pulseForgeReceivedAt ? msBetween(pulseForgeReceivedAt, ingestedAt) : null,
    pulseForgeToAlertMs: alertCreatedAt && pulseForgeReceivedAt
      ? msBetween(alertCreatedAt, pulseForgeReceivedAt) : null,
    telegramToAlertSentMs: transportSentAt && occurredAt ? msBetween(transportSentAt, occurredAt) : null,
  };
}

module.exports = {
  buildLatencyDiagnostics,
  msBetween,
};
