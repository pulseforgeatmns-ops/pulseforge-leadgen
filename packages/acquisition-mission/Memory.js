'use strict';

/**
 * SPEC-118 — cross-capability memory. Observations attach to the mission.
 */

const { asText, nowIso, newId, clone } = require('./types');

function createObservation(input = {}) {
  return {
    id: asText(input.id) || newId('obs'),
    missionId: asText(input.missionId),
    specialist: asText(input.specialist),
    observation: asText(input.observation || input.text),
    at: nowIso(input.at || input.now),
  };
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
