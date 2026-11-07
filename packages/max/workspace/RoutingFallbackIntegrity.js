'use strict';

/**
 * P0 Max reliability — routing fallback integrity.
 *
 * When an actionable owner (active mission, mission creation, Scout / Paige /
 * Cal / direction specialists) declines a turn, the engine falls back to
 * reasoning. Late pipeline handlers may still legitimately own the turn
 * (active-work continuation, SPEC-022 mission routes), so the fallback itself
 * is not an error. But if the turn would end in a GENERIC answer while the
 * operator plainly asked for something to be executed, Max must say it did
 * not execute — never compose a general reply that reads like work was done.
 *
 * Every fallback is recorded (`routingFallback`) so the trace shows which owner
 * claimed the turn, why it declined, and whether execution was expected.
 * Tenant-independent and purely lexical over the operator's affirmative text.
 */

const { stripNegatedDirectives } = require('./NegatedDirective');
const { isMissionExecutionCommand } = require('./ExecutionLanguageDetection');
const { buildStructuredResponse } = require('./WorkspaceTypes');
const { EXECUTION_COMMAND_RE } = require('./SessionDirectiveRegistry');

const ACTIONABLE_OWNERS = new Set([
  'active_mission',
  'mission_creation',
  'specialist_scout',
  'specialist_paige',
  'specialist_cal',
  'specialist_direction',
]);

/** Questions ask about work; they do not command it. */
const INTERROGATIVE_RE =
  /^\s*(?:should|shall|can|could|would|will|what|how|why|when|where|which|who|is|are|was|were|do|does|did|has|have|may|might)\b/i;

const IMPERATIVE_EXECUTION_RE =
  /\b(?:run|launch|start|execute|trigger|kick\s+off|begin|activate)\s+(?:the\s+|a\s+|this\s+)?(?:scout|paige|emmett|penny|discovery|outreach|campaign|sequence|send|mission|research)\b|\b(?:send|mail|e-?mail|text|call)\s+(?:the\s+|these\s+|them|all\s+)?(?:emails?|letters?|messages?|campaign|outreach|sequence|prospects?|leads?|them)\b|\bapprove\s+(?:the\s+)?(?:discovery|execution|plan|campaign|send|sends|outreach)\b/i;

const SCOUT_DISCOVERY_RE =
  /\b(?:find|search\s+for|identify|discover|locate|pull|source)\b.{0,40}\b(?:prospects?|leads?|companies|businesses|accounts|firms|offices)\b/i;

function affirmativeSentences(question) {
  return String(question || '')
    .split(/(?<=[.!?])\s+|\n+/)
    .map((s) => stripNegatedDirectives(s))
    .map((s) => s.replace(/^[\s.,;:!-]+|[\s.,;:-]+$/g, ''))
    .filter(Boolean);
}

/**
 * Owner-independent: does the operator's affirmative (non-negated, non-question)
 * text command work to be executed? Used where a turn is answered without any
 * dispatch, so the reply can say plainly that nothing ran.
 * @param {string} question
 * @returns {string|null} signal
 */
function detectExecutionCommand(question) {
  for (const sentence of affirmativeSentences(question)) {
    if (INTERROGATIVE_RE.test(sentence) || /\?\s*$/.test(sentence)) continue;
    if (IMPERATIVE_EXECUTION_RE.test(sentence)) return 'imperative_execution';
    if (isMissionExecutionCommand(sentence)) return 'mission_execution_command';
    if (EXECUTION_COMMAND_RE.test(sentence)) return 'execution_command';
  }
  return null;
}

/**
 * @param {{ claimedOwner: string, reason: string, question: string }} input
 * @returns {{ claimedOwner: string, reason: string, executionExpected: boolean, signal: string|null }}
 */
function assessFallback(input = {}) {
  const claimedOwner = String(input.claimedOwner || '');
  const reason = String(input.reason || '');
  const base = { claimedOwner, reason, executionExpected: false, signal: null };
  if (!ACTIONABLE_OWNERS.has(claimedOwner)) return base;
  for (const sentence of affirmativeSentences(input.question)) {
    if (INTERROGATIVE_RE.test(sentence) || /\?\s*$/.test(sentence)) continue;
    if (IMPERATIVE_EXECUTION_RE.test(sentence)) return { ...base, executionExpected: true, signal: 'imperative_execution' };
    if (isMissionExecutionCommand(sentence)) return { ...base, executionExpected: true, signal: 'mission_execution_command' };
    if (EXECUTION_COMMAND_RE.test(sentence)) return { ...base, executionExpected: true, signal: 'execution_command' };
    if (claimedOwner === 'specialist_scout' && SCOUT_DISCOVERY_RE.test(sentence)) {
      return { ...base, executionExpected: true, signal: 'scout_discovery_command' };
    }
  }
  return base;
}

const OWNER_LABELS = {
  active_mission: 'the active mission',
  mission_creation: 'mission creation',
  specialist_scout: 'Scout',
  specialist_paige: 'Paige',
  specialist_cal: 'Cal',
  specialist_direction: 'specialist direction',
};

/**
 * Explicit, traceable "not executed" turn.
 * @param {{ claimedOwner: string, reason: string, signal: string|null }} fallback
 */
function buildFallbackBlockedResponse(fallback) {
  const label = OWNER_LABELS[fallback.claimedOwner] || fallback.claimedOwner;
  const lead = fallback.reason === 'no_mission_for_execution_request'
    ? 'Not executed: there is no mission in this workspace that can carry out this request.'
    : `Not executed: this request was routed to ${label}, which could not carry it out ` +
      `(${fallback.reason.replace(/_/g, ' ')}).`;
  const answer =
    `${lead} No specialist was dispatched and no action was taken. ` +
    'Check that the mission, client workspace and approvals this needs are in place, then ask again.';
  const structured = buildStructuredResponse({
    answer,
    reasoning: [`Routing fallback ${fallback.reason}; execution was expected (${fallback.signal}).`],
  });
  // Assigned after construction: buildStructuredResponse whitelists metadata keys.
  Object.assign(structured.metadata, {
    executionState: 'blocked',
    executionVerified: false,
    blocker: 'routing_fallback',
    routingFallback: { ...fallback },
  });
  return structured;
}

module.exports = {
  ACTIONABLE_OWNERS,
  assessFallback,
  detectExecutionCommand,
  buildFallbackBlockedResponse,
};
