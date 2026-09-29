'use strict';

/**
 * SPEC-118 — cross-capability memory. Observations attach to the mission.
 */

const { asText, nowIso, newId, clone } = require('./types');

function createObservation(input = {}) {
  const row = {
    id: asText(input.id) || newId('obs'),
    missionId: asText(input.missionId),
    specialist: asText(input.specialist),
    observation: asText(input.observation || input.text),
    at: nowIso(input.at || input.now),
  };
  if (input.kind) row.kind = asText(input.kind);
  if (input.prospectId != null) row.prospectId = String(input.prospectId);
  if (input.category) row.category = asText(input.category);
  if (input.eventType) row.eventType = asText(input.eventType);
  if (input.occurredAt) row.occurredAt = nowIso(input.occurredAt);
  if (input.evidence && typeof input.evidence === 'object') row.evidence = clone(input.evidence);
  if (input.payload && typeof input.payload === 'object') row.payload = clone(input.payload);
  if (input.source) row.source = asText(input.source);
  return row;
}

function formatMemory(observations = []) {
  return observations.map((row) => ({
    ...clone(row),
    line: `${capitalize(row.specialist)}   ${row.observation}`,
  }));
}

function capitalize(value) {
  const text = String(value || '');
  return text ? text.charAt(0).toUpperCase() + text.slice(1) : text;
}

module.exports = {
  createObservation,
  formatMemory,
};
