'use strict';

/**
 * Lexical TurnFrame producer (PROTOTYPE, NOT WIRED).
 *
 * Maps the lexical signals production ALREADY computes into a TurnFrame. It
 * adds no new phrase rules: every decision below is a call into an existing
 * production helper. It exists for two reasons:
 *   1. It is the degraded-mode producer when the model is unavailable, so
 *      degraded behaviour equals today's behaviour, never something new.
 *   2. It measures today's interpretation quality on the held-out set, so the
 *      model producer has a baseline to beat.
 */

const {
  classifyAuthorizationPolarity,
  isConstraintQualifiedRequest,
} = require('../../workspace/NegatedDirective');
const { isExplicitMissionExit } = require('../../workspace/ActiveMissionGuard');
const {
  isInterrogative,
  detectMissionFacets,
  resolveMissionReferences,
} = require('../../workspace/OperationalQuestionFrame');
const { validateTurnFrame } = require('../TurnFrame');

const FACET_MAP = Object.freeze({
  classification: 'status',
  reconsideration: 'next_step',
  relevance: 'relevance',
  abandonment: 'recommendation',
  history: 'history',
  comparison: 'comparison',
  evidence: 'cause',
});

function missionRecord(candidates) {
  return {
    missions: candidates
      .filter((c) => c.kind === 'mission')
      .map((c) => ({ id: c.id, title: c.label || '', objective: c.objective || '' })),
  };
}

/**
 * @param {{ text: string, context: import('../TurnFrame').FrameContext & { focusIds?: string[] } }} input
 */
function produceLexicalFrame({ text, context }) {
  const q = String(text || '');
  const candidates = Array.isArray(context.candidates) ? context.candidates : [];
  const polarity = classifyAuthorizationPolarity(q);
  const exit = isExplicitMissionExit(q);
  const interrogative = isInterrogative(q);
  const constraintQualified = isConstraintQualifiedRequest(q);
  const refs = resolveMissionReferences(q, missionRecord(candidates));
  const facets = [...new Set(detectMissionFacets(q).map((f) => FACET_MAP[f]).filter(Boolean))];

  let intent = 'other';
  let verb = 'none';
  let actionPolarity = 'none';
  if (exit.explicit) {
    // Today's routing treats these as leaving the mission conversation (#1094).
    intent = 'instruction';
    verb = 'leave_conversation';
    actionPolarity = 'affirmative';
  } else if ((polarity.negatedAuthorization || polarity.negatedCancel) && !constraintQualified && !polarity.affirmativeCancel) {
    intent = 'prohibition';
    verb = polarity.negatedCancel ? 'cancel' : 'approve';
    actionPolarity = 'negated';
  } else if (interrogative || constraintQualified) {
    intent = 'question';
  } else if (polarity.affirmativeCancel) {
    intent = context.pendingDecisionId ? 'authorization' : 'instruction';
    verb = 'cancel';
    actionPolarity = 'affirmative';
  } else if (polarity.affirmativeAuthorization) {
    intent = context.pendingDecisionId ? 'authorization' : 'instruction';
    verb = 'approve';
    actionPolarity = 'affirmative';
  }

  const ambiguities = refs.candidates.map((c) => ({ kind: 'referent', candidateIds: c.missionIds }));
  let referentIds = refs.ids;
  // Today: an anaphor binds to the existing analytical focus.
  if (!referentIds.length && Array.isArray(context.focusIds) && context.focusIds.length && interrogative) {
    referentIds = context.focusIds;
  }
  const target = verb !== 'none' && verb !== 'leave_conversation'
    ? (intent === 'authorization' ? context.pendingDecisionId || null : referentIds[0] || null)
    : null;

  return validateTurnFrame({
    intent,
    subjectKind: referentIds.length ? 'mission' : (intent === 'authorization' ? 'pending_decision' : 'none'),
    relationToPrior: referentIds.length && context.focusIds && referentIds === context.focusIds ? 'follow_up' : 'none',
    referents: referentIds.map((id, i) => ({ candidateId: id, role: i === 0 ? 'primary' : 'comparison' })),
    evidenceNeeds: referentIds.length ? ['mission_lifecycle'] : [],
    facets,
    requestedOutcome: intent === 'question' ? 'explanation' : (verb !== 'none' ? 'action' : 'acknowledgement'),
    requestedAction: { verb, targetCandidateId: target, polarity: actionPolarity },
    scope: referentIds.length ? 'focused' : 'portfolio',
    foreignTenantReference: false,
    ambiguities,
    confidence: intent === 'other' ? 0.3 : 0.9,
  }, { ...context, producer: 'lexical' });
}

module.exports = { produceLexicalFrame, FACET_MAP };
