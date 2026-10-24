'use strict';

/**
 * SPEC-149 — Message Type vocabulary (ADR-069).
 * Communicative purpose of an operator message, classified before cognition.
 */

const MESSAGE_TYPES = Object.freeze({
  QUESTION: 'question',
  COMMAND: 'command',
  SESSION_CONFIGURATION: 'session_configuration',
  SESSION_INSPECTION: 'session_inspection',
  MISSION_CREATION: 'mission_creation',
  MISSION_EXECUTION: 'mission_execution',
  INFORMATION: 'information',
  FEEDBACK: 'feedback',
  CORRECTION: 'correction',
  APPROVAL: 'approval',
  REJECTION: 'rejection',
  SYSTEM_CONFIGURATION: 'system_configuration',
  UNKNOWN: 'unknown',
});

/**
 * @typedef {object} MessageClassification
 * @property {string} type — primary MESSAGE_TYPES value
 * @property {number} confidence — 0..1
 * @property {string[]} evidence — matched signals
 * @property {boolean} mutatesSession — session state should update this turn
 * @property {boolean} mutatesMission — mission runtime may execute this turn
 */

function buildMessageClassification(type, confidence, evidence = [], extras = {}) {
  return {
    type,
    confidence,
    evidence: Array.isArray(evidence) ? evidence : [evidence].filter(Boolean),
    mutatesSession: Boolean(extras.mutatesSession),
    mutatesMission: Boolean(extras.mutatesMission),
    via: extras.via || null,
  };
}

module.exports = {
  MESSAGE_TYPES,
  buildMessageClassification,
};
