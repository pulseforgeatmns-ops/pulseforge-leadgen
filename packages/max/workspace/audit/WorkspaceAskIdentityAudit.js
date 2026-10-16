'use strict';

/**
 * AUDIT-022 — Workspace ask request/response identity fingerprints.
 * Stable checksum over fields that must survive browser → Express → DOM.
 */

const crypto = require('crypto');

function stableStringify(value) {
  if (value == null) return 'null';
  if (typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableStringify(item)).join(',')}]`;
  }
  const keys = Object.keys(value).sort();
  return `{${keys
    .map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`)
    .join(',')}}`;
}

function responseHash(payload = {}) {
  const fingerprint = {
    sessionId: payload.sessionId || null,
    missionId:
      (payload.mission && payload.mission.id) ||
      (payload.context && payload.context.missionId) ||
      null,
    stage:
      (payload.mission && payload.mission.stage) ||
      (payload.structured &&
        payload.structured.metadata &&
        payload.structured.metadata.missionCommunicationPayload &&
        payload.structured.metadata.missionCommunicationPayload.stage) ||
      null,
    headline:
      (payload.structured &&
        payload.structured.metadata &&
        payload.structured.metadata.missionCommunicationPayload &&
        payload.structured.metadata.missionCommunicationPayload.headline) ||
      null,
    action:
      (payload.resolution && payload.resolution.reason) ||
      (payload.structured &&
        payload.structured.metadata &&
        payload.structured.metadata.executionAction) ||
      null,
    proseHeadline: proseHeadline(payload.prose),
  };
  return crypto.createHash('sha256').update(stableStringify(fingerprint)).digest('hex');
}

function proseHeadline(prose) {
  const line = String(prose || '')
    .split(/\r?\n/)
    .map((row) => row.trim())
    .find(Boolean);
  return line || null;
}

function identitySnapshot(payload = {}, extra = {}) {
  return {
    requestId: extra.requestId || payload.requestId || null,
    sessionId: payload.sessionId || extra.sessionId || null,
    missionId:
      extra.missionId ||
      (payload.mission && payload.mission.id) ||
      (payload.context && payload.context.missionId) ||
      null,
    stage:
      extra.stage ||
      (payload.mission && payload.mission.stage) ||
      null,
    headline:
      extra.headline ||
      (payload.structured &&
        payload.structured.metadata &&
        payload.structured.metadata.missionCommunicationPayload &&
        payload.structured.metadata.missionCommunicationPayload.headline) ||
      proseHeadline(payload.prose),
    executionAction: extra.executionAction || null,
    responseHash: responseHash(payload),
    proseHeadline: proseHeadline(payload.prose),
  };
}

function assertIdentityEqual(expected, actual, label) {
  const mismatches = [];
  for (const key of [
    'requestId',
    'sessionId',
    'missionId',
    'stage',
    'headline',
    'executionAction',
    'responseHash',
    'proseHeadline',
  ]) {
    if (expected[key] !== actual[key]) {
      mismatches.push({ field: key, expected: expected[key], actual: actual[key] });
    }
  }
  if (mismatches.length) {
    const err = new Error(`AUDIT-022 identity mismatch at ${label}`);
    err.code = 'AUDIT_022_IDENTITY_MISMATCH';
    err.label = label;
    err.mismatches = mismatches;
    throw err;
  }
}

module.exports = {
  stableStringify,
  responseHash,
  proseHeadline,
  identitySnapshot,
  assertIdentityEqual,
};
