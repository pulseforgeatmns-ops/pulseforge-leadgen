'use strict';

/**
 * SPEC-149 — Acknowledgement for SESSION_CONFIGURATION turns.
 * No reasoning pipeline; session mutations occur before downstream handlers.
 *
 * P0 response integrity: this turn never dispatches a specialist, so it must
 * never read like work was done. When the operator's words also command
 * execution ("let's run it", "go ahead and launch the campaign"), the reply
 * says "Not executed" explicitly instead of a bare "Acknowledged.".
 */

const { buildStructuredResponse } = require('./WorkspaceTypes');
const {
  formatSessionInspection,
  formatExecutionPolicyLabel,
} = require('./SessionStateManager');
const { hasExecutionCommandDirective } = require('./SessionDirectiveRegistry');
const { stripNegatedDirectives } = require('./NegatedDirective');
const { detectExecutionCommand } = require('./RoutingFallbackIntegrity');

/**
 * Defense in depth: operational execution commands are routed away from this
 * path by the classifier (P0 execution-intent routing). If one still arrives
 * here, it is answered as "Not executed", never as an acknowledgement.
 * @param {string} question
 * @returns {string|null} signal naming why execution was expected
 */
function detectUnexecutedCommand(question) {
  const text = String(question || '');
  if (!text.trim()) return null;
  const affirmative = text
    .split(/(?<=[.!?])\s+|\n+/)
    .map((s) => stripNegatedDirectives(s))
    .join('\n');
  if (hasExecutionCommandDirective(affirmative)) return 'execution_command';
  return detectExecutionCommand(text);
}

/**
 * @param {object|null} state
 * @param {object} [input]
 * @param {boolean} [input.changed]
 * @param {string|null} [input.unexecutedCommand]
 * @returns {string}
 */
function formatSessionConfigurationAcknowledgement(state, input = {}) {
  if (input.unexecutedCommand) {
    const policy = formatExecutionPolicyLabel(state && state.executionPolicy);
    const settings = input.changed === false
      ? `Session settings are unchanged (execution policy: ${policy}).`
      : `Session settings were updated (execution policy: ${policy}).`;
    return (
      'Not executed: this message was handled as a session setting, so nothing was run. ' +
      'No specialist was dispatched and no mission, prospect or outbound state changed. ' +
      `${settings} To run work, name it — for example "execute the mission" — or approve the pending step.`
    );
  }
  if (input.changed === false) {
    return 'Acknowledged. No session settings changed. No operational work was executed.';
  }
  const inspection = formatSessionInspection(state);
  return `Acknowledged. Session settings updated. No operational work was executed.\n\n${inspection.replace(/^Current Session\n\n/, '')}`;
}

/**
 * @param {object} input
 * @param {object|null} input.sessionState
 * @param {boolean} [input.changed]
 * @param {string} [input.question]
 * @param {import('./MessageType').MessageClassification} [input.messageClassification]
 * @returns {{ handled: boolean, prose: string, structured: object, reason: string }}
 */
function buildSessionConfigurationResponse(input = {}) {
  const sessionState = input.sessionState || null;
  const unexecutedCommand = detectUnexecutedCommand(input.question);
  const prose = formatSessionConfigurationAcknowledgement(sessionState, {
    changed: input.changed,
    unexecutedCommand,
  });

  const structured = buildStructuredResponse({
    answer: prose,
    reasoning: ['SPEC-149 — SESSION_CONFIGURATION; no reasoning pipeline.'],
    recommendedActions: [{ id: 'acknowledge', type: 'review', label: 'Continue' }],
    metadata: {
      sessionConfiguration: true,
      sessionStateRead: true,
      businessIntelligenceUsed: false,
      sourcesUsed: {
        briefing: false,
        reasoning: false,
        memory: false,
        policy: true,
        knowledge: false,
      },
    },
  });
  // Assigned after construction: buildStructuredResponse whitelists metadata keys.
  // A session-configuration turn never dispatches; say so in machine-readable form.
  Object.assign(structured.metadata, {
    executionState: 'not_executed',
    executionVerified: false,
    ...(unexecutedCommand
      ? { blocker: 'session_configuration_only', executionExpected: true, executionSignal: unexecutedCommand }
      : {}),
  });

  return {
    handled: true,
    prose,
    structured,
    reason: unexecutedCommand
      ? 'session_configuration_not_executed'
      : 'session_configuration_acknowledged',
    messageClassification: input.messageClassification || null,
  };
}

module.exports = {
  formatSessionConfigurationAcknowledgement,
  buildSessionConfigurationResponse,
  detectUnexecutedCommand,
};
