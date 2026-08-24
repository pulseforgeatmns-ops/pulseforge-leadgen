'use strict';

/**
 * SPEC-149 — Acknowledgement for SESSION_CONFIGURATION turns.
 * No reasoning pipeline; session mutations occur before downstream handlers.
 */

const { buildStructuredResponse } = require('./WorkspaceTypes');
const { formatSessionInspection } = require('./SessionStateManager');

/**
 * @param {object|null} state
 * @param {object} [input]
 * @param {boolean} [input.changed]
 * @returns {string}
 */
function formatSessionConfigurationAcknowledgement(state, input = {}) {
  if (input.changed === false) {
    return 'Acknowledged.';
  }
  const inspection = formatSessionInspection(state);
  return `Acknowledged.\n\n${inspection.replace(/^Current Session\n\n/, '')}`;
}

/**
 * @param {object} input
 * @param {object|null} input.sessionState
 * @param {boolean} [input.changed]
 * @param {import('./MessageType').MessageClassification} [input.messageClassification]
 * @returns {{ handled: boolean, prose: string, structured: object, reason: string }}
 */
function buildSessionConfigurationResponse(input = {}) {
  const sessionState = input.sessionState || null;
  const prose = formatSessionConfigurationAcknowledgement(sessionState, {
    changed: input.changed,
  });

  const structured = buildStructuredResponse({
    answer: prose,
    answerKind: 'session_configuration',
    reasoning: ['SPEC-149 — SESSION_CONFIGURATION; no reasoning pipeline.'],
    sources: [],
    recommendedActions: [{ id: 'acknowledge', type: 'review', label: 'Continue' }],
  });

  return {
    handled: true,
    prose,
    structured,
    reason: 'session_configuration_acknowledged',
    messageClassification: input.messageClassification || null,
  };
}

module.exports = {
  formatSessionConfigurationAcknowledgement,
  buildSessionConfigurationResponse,
};
