'use strict';

/**
 * P0 Max reliability — constraint-aware conversational refusals.
 *
 * Durable mission restrictions (`mission.authorizationConstraints`) are
 * enforced by the execution router and TME. Chat only builds an execution
 * request when a command resolves the *current* pending decision, so a
 * prohibited command that names a different action ("Send the emails." while
 * a plan clarification or discovery approval is pending) never reached the
 * router: it fell through to a generic "not executed" reply or a yes/no
 * re-prompt that never named the restriction.
 *
 * This guard runs before pending-decision capture in the active-mission
 * pipeline. When the operator's affirmative text commands an action whose
 * class the active mission prohibits, it returns the canonical
 * constraint-blocked turn. It never builds or dispatches an execution request,
 * never mutates the mission, and never relaxes anything: it can only refuse.
 *
 * Commands that target the pending decision's own action keep their existing
 * path (the router enforces the same constraints there, with its audit row).
 * Negated directives and questions are never commands.
 */

const amo = require('../../acquisition-mission');
const { recordRouterAudit } = require('../../acquisition-mission/ExecutionRouter');
const { stripNegatedDirectives } = require('./NegatedDirective');
const { ACTION_TARGETS, KIND_TARGETS } = require('./PendingDecisionResolver');
const { buildConstraintBlockedTurn } = require('./AcquisitionMissionExecution');
const { resolveAcquisitionActiveMission } = require('./ActiveMissionGuard');

const { EXECUTION_INTENTS } = amo;

/** Questions ask about work; they do not command it. */
const INTERROGATIVE_RE =
  /^\s*(?:should|shall|can|could|would|will|what|how|why|when|where|which|who|is|are|was|were|do|does|did|has|have|may|might)\b/i;

/** A command verb must be present; naming an object alone is not a command. */
const COMMAND_VERB_RE =
  /\b(?:approve[sd]?|authori[sz]e[sd]?|run|launch|start|begin|execute|trigger|kick\s+off|activate|send|mail|e-?mail|text|call|draft|write|generate|create|produce|publish|post|buy|go\s+ahead|proceed|do\s+it)\b/i;

const APPROVAL_VERB_RE = /\b(?:approve[sd]?|authori[sz]e[sd]?)\b/i;
const CONTENT_VERB_RE = /\b(?:draft|write|generate|create|produce)\b/i;

const CONTENT_TARGET_RE = /\b(?:content|copy|copywriting|drafts?|variants?|posts?)\b/i;
/** Outreach channels the shared outbound target does not name (calls, texts, mail). */
const OUTREACH_CHANNEL_RE =
  /\b(?:call(?:s|ing)?|phone|texts?|texting|sms|mail(?:ing)?|letters?|messages?)\b/i;
const PAID_MEDIA_TARGET_RE =
  /\b(?:ads?|advertis\w*|paid\s+(?:media|search|social|acquisition)|ppc)\b/i;

function affirmativeCommandSentences(question) {
  return String(question || '')
    .split(/(?<=[.!?])\s+|\n+/)
    .map((sentence) => ({ raw: sentence, text: stripNegatedDirectives(sentence) }))
    .filter(({ raw, text }) => text && !/\?\s*$/.test(raw) && !INTERROGATIVE_RE.test(text))
    .map(({ text }) => text.replace(/^[\s.,;:!-]+|[\s.,;:-]+$/g, ''))
    .filter((text) => text && COMMAND_VERB_RE.test(text));
}

/**
 * Execution intents commanded by the operator's affirmative text, in the order
 * they should be explained. Pure and tenant-independent.
 * @param {string} question
 * @returns {{ target: string, intent: string }[]}
 */
function commandedIntents(question) {
  const out = [];
  const push = (target, intent) => {
    if (!out.some((row) => row.intent === intent)) out.push({ target, intent });
  };
  for (const sentence of affirmativeCommandSentences(question)) {
    const content = CONTENT_TARGET_RE.test(sentence) ||
      (CONTENT_VERB_RE.test(sentence) && ACTION_TARGETS.outbound.test(sentence));
    // "Draft the emails" asks for content, not a send; explain that first.
    if (content && CONTENT_VERB_RE.test(sentence)) push('content', EXECUTION_INTENTS.GENERATE_VARIANTS);
    const paidMedia = PAID_MEDIA_TARGET_RE.test(sentence);
    // "Launch paid ads" names paid media; "launch" alone is not an outreach object.
    if (paidMedia) push('paid_media', EXECUTION_INTENTS.ASSESS_PAID_ACQUISITION);
    const outboundText = paidMedia ? sentence.replace(/\blaunch\w*\b/gi, ' ') : sentence;
    if (ACTION_TARGETS.outbound.test(outboundText) || OUTREACH_CHANNEL_RE.test(outboundText)) {
      push('outbound', APPROVAL_VERB_RE.test(sentence)
        ? EXECUTION_INTENTS.APPROVE_EXECUTION
        : EXECUTION_INTENTS.EXECUTE_OUTBOUND);
    }
    if (ACTION_TARGETS.discovery.test(sentence)) push('discovery', EXECUTION_INTENTS.APPROVE_DISCOVERY);
    if (content) push('content', EXECUTION_INTENTS.GENERATE_VARIANTS);
  }
  return out;
}

/**
 * Prohibited commands for this mission, excluding those the pending decision
 * itself executes under the same action class (those keep the router path,
 * which enforces the same constraint).
 * @param {object} mission
 * @param {string} question
 */
function prohibitedCommands(mission, question) {
  if (!mission || !mission.authorizationConstraints) return [];
  const pending = mission.pendingOperatorDecision || null;
  const ownTargets = (pending && KIND_TARGETS[pending.kind]) || [];
  const pendingIntent = amo.intentFromPendingDecision(pending);
  const pendingClass = pendingIntent ? amo.intentActionClass(pendingIntent) : null;
  // A command is left to the pending decision only when executing that
  // decision enforces the same action class. "Approve discovery" while plan
  // approval is pending would otherwise approve the plan (planning) instead of
  // refusing the discovery the mission forbids.
  const ownsCommand = (row) => ownTargets.includes(row.target) &&
    pendingClass === amo.intentActionClass(row.intent);
  return commandedIntents(question)
    .filter((row) => !ownsCommand(row))
    .map((row) => ({ ...row, verdict: amo.evaluateAction(mission, amo.intentActionClass(row.intent)) }))
    .filter((row) => !row.verdict.allowed);
}

/**
 * @param {object} input same input as maybeHandlePendingDecisionTurn
 * @returns {Promise<object|null>} constraint-blocked turn, or null to continue routing
 */
async function maybeHandleProhibitedCommandTurn(input = {}) {
  const question = String(input.question || '');
  if (!commandedIntents(question).length) return null;
  let mission = null;
  try {
    ({ mission } = await resolveAcquisitionActiveMission(input));
  } catch (_) {
    // Fail to the existing path: the router and TME still enforce constraints.
    return null;
  }
  const blocked = prohibitedCommands(mission, question);
  if (!blocked.length) return null;

  const [first] = blocked;
  for (const row of blocked) {
    recordRouterAudit({
      source: amo.EXECUTION_SOURCES.CHAT,
      intent: row.intent,
      missionId: mission.id,
      outcome: 'blocked_by_constraint',
      executionState: 'blocked',
      errorCode: amo.AUTHORIZATION_CONSTRAINT_BLOCKED,
    });
  }
  const turn = buildConstraintBlockedTurn({
    err: { code: null, cause: null },
    mission,
    action: 'prohibited_command',
    intent: first.intent,
  });
  const classes = [...new Set(blocked.map((row) => row.verdict.actionClass))];
  if (classes.length > 1) {
    turn.structured.reasoning.push(
      `Every commanded action is forbidden here: ${classes.map((c) => c.replace(/_/g, ' ')).join(', ')}.`
    );
  }
  if (mission.pendingOperatorDecision && mission.pendingOperatorDecision.prompt) {
    turn.structured.reasoning.push(
      `The pending decision is unchanged: ${mission.pendingOperatorDecision.prompt}`
    );
  }
  Object.assign(turn.structured.metadata, {
    prohibitedCommandGuard: true,
    blockedActionClasses: classes,
    pendingDecisionKind: (mission.pendingOperatorDecision && mission.pendingOperatorDecision.kind) || null,
  });
  return turn;
}

module.exports = {
  commandedIntents,
  prohibitedCommands,
  maybeHandleProhibitedCommandTurn,
};
