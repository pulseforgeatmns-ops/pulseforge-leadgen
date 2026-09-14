'use strict';

/**
 * SPEC-251 — Observe reaction types, disposition fold, and reaction row factory.
 */

const { asText, nowIso, clone } = require('./types');
const { INTERPRETATION_TYPES } = require('./ObservationInterpretation');

const EVIDENCE_TYPES = Object.freeze({
  SENT: 'sent',
  DELIVERED: 'delivered',
  PROXY_OPEN: 'proxy_open',
  HUMAN_OPEN: 'human_open',
  CLICKED: 'clicked',
  SOFT_BOUNCE: 'soft_bounce',
  HARD_BOUNCE: 'hard_bounce',
  REPLY_NEUTRAL: 'reply_neutral',
  REPLY_POSITIVE: 'reply_positive',
  REPLY_NEGATIVE: 'reply_negative',
  UNSUBSCRIBE: 'unsubscribe',
  BOOKING: 'booking',
});

const EVIDENCE_STRENGTH = Object.freeze({
  TRANSPORT_ATTEMPTED: 'transport_attempted',
  TRANSPORT_CONFIRMED: 'transport_confirmed',
  WEAK_ENGAGEMENT: 'weak_engagement',
  ENGAGEMENT: 'engagement',
  STRONG_ENGAGEMENT: 'strong_engagement',
  BUSINESS_SIGNAL: 'business_signal',
  TERMINAL_NEGATIVE: 'terminal_negative',
  TERMINAL_POSITIVE: 'terminal_positive',
});

const EVIDENCE_STRENGTH_ORDER = Object.freeze([
  EVIDENCE_STRENGTH.TRANSPORT_ATTEMPTED,
  EVIDENCE_STRENGTH.TRANSPORT_CONFIRMED,
  EVIDENCE_STRENGTH.WEAK_ENGAGEMENT,
  EVIDENCE_STRENGTH.ENGAGEMENT,
  EVIDENCE_STRENGTH.STRONG_ENGAGEMENT,
  EVIDENCE_STRENGTH.BUSINESS_SIGNAL,
  EVIDENCE_STRENGTH.TERMINAL_NEGATIVE,
  EVIDENCE_STRENGTH.TERMINAL_POSITIVE,
]);

const DISPOSITIONS = Object.freeze({
  UNREACHED: 'unreached',
  REACHED: 'reached',
  POSSIBLY_SEEN: 'possibly_seen',
  SEEN: 'seen',
  ENGAGED: 'engaged',
  CONVERSING: 'conversing',
  INTERESTED: 'interested',
  NOT_NOW: 'not_now',
  BOOKED: 'booked',
  REJECTED: 'rejected',
  UNREACHABLE: 'unreachable',
  EXHAUSTED: 'exhausted',
});

const ACTIVE_DISPOSITIONS = Object.freeze([
  DISPOSITIONS.UNREACHED,
  DISPOSITIONS.REACHED,
  DISPOSITIONS.POSSIBLY_SEEN,
  DISPOSITIONS.SEEN,
  DISPOSITIONS.ENGAGED,
  DISPOSITIONS.CONVERSING,
  DISPOSITIONS.INTERESTED,
  DISPOSITIONS.NOT_NOW,
]);

const TERMINAL_DISPOSITIONS = Object.freeze([
  DISPOSITIONS.BOOKED,
  DISPOSITIONS.REJECTED,
  DISPOSITIONS.UNREACHABLE,
  DISPOSITIONS.EXHAUSTED,
]);

const NEXT_ACTIONS = Object.freeze({
  WAIT: 'wait',
  PROPOSE_FOLLOW_UP: 'propose_follow_up',
  REVIEW_REPLY: 'review_reply',
  PROPOSE_END_CANDIDATE: 'propose_end_candidate',
  NONE: 'none',
});

const EVIDENCE_TYPE_TO_STRENGTH = Object.freeze({
  [EVIDENCE_TYPES.SENT]: EVIDENCE_STRENGTH.TRANSPORT_ATTEMPTED,
  [EVIDENCE_TYPES.DELIVERED]: EVIDENCE_STRENGTH.TRANSPORT_CONFIRMED,
  [EVIDENCE_TYPES.PROXY_OPEN]: EVIDENCE_STRENGTH.WEAK_ENGAGEMENT,
  [EVIDENCE_TYPES.HUMAN_OPEN]: EVIDENCE_STRENGTH.ENGAGEMENT,
  [EVIDENCE_TYPES.CLICKED]: EVIDENCE_STRENGTH.STRONG_ENGAGEMENT,
  [EVIDENCE_TYPES.SOFT_BOUNCE]: EVIDENCE_STRENGTH.TRANSPORT_ATTEMPTED,
  [EVIDENCE_TYPES.HARD_BOUNCE]: EVIDENCE_STRENGTH.TERMINAL_NEGATIVE,
  [EVIDENCE_TYPES.REPLY_NEUTRAL]: EVIDENCE_STRENGTH.BUSINESS_SIGNAL,
  [EVIDENCE_TYPES.REPLY_POSITIVE]: EVIDENCE_STRENGTH.BUSINESS_SIGNAL,
  [EVIDENCE_TYPES.REPLY_NEGATIVE]: EVIDENCE_STRENGTH.TERMINAL_NEGATIVE,
  [EVIDENCE_TYPES.UNSUBSCRIBE]: EVIDENCE_STRENGTH.TERMINAL_NEGATIVE,
  [EVIDENCE_TYPES.BOOKING]: EVIDENCE_STRENGTH.TERMINAL_POSITIVE,
});

function strengthIndex(strength) {
  const idx = EVIDENCE_STRENGTH_ORDER.indexOf(asText(strength));
  return idx < 0 ? -1 : idx;
}

function maxEvidenceStrength(current, incoming) {
  const a = strengthIndex(current);
  const b = strengthIndex(incoming);
  if (a < 0) return incoming;
  if (b < 0) return current;
  return b > a ? incoming : current;
}

function isTerminalDisposition(disposition) {
  return TERMINAL_DISPOSITIONS.includes(asText(disposition));
}

function isTerminalStrength(strength) {
  return strength === EVIDENCE_STRENGTH.TERMINAL_NEGATIVE
    || strength === EVIDENCE_STRENGTH.TERMINAL_POSITIVE;
}

function evidenceTypeFromObservation(observation = {}, interpretation = null) {
  const eventType = asText(observation.eventType).toLowerCase();
  const interpType = asText(interpretation?.type || interpretation?.interpretation?.type).toLowerCase();

  if (interpType) {
    switch (interpType) {
      case INTERPRETATION_TYPES.HUMAN_OPEN:
        return EVIDENCE_TYPES.HUMAN_OPEN;
      case INTERPRETATION_TYPES.PROXY_OPEN:
        return EVIDENCE_TYPES.PROXY_OPEN;
      case INTERPRETATION_TYPES.LINK_ENGAGEMENT:
        return EVIDENCE_TYPES.CLICKED;
      case INTERPRETATION_TYPES.TRANSPORT_SUCCESS:
        return eventType === 'delivered' ? EVIDENCE_TYPES.DELIVERED : EVIDENCE_TYPES.SENT;
      case INTERPRETATION_TYPES.TRANSPORT_DEFERRED:
        return EVIDENCE_TYPES.SENT;
      case INTERPRETATION_TYPES.TRANSPORT_FAILURE:
        return ['soft_bounce'].includes(eventType)
          ? EVIDENCE_TYPES.SOFT_BOUNCE
          : EVIDENCE_TYPES.HARD_BOUNCE;
      case INTERPRETATION_TYPES.UNSUBSCRIBE_INTENT:
        return EVIDENCE_TYPES.UNSUBSCRIBE;
      case INTERPRETATION_TYPES.POSITIVE_INTENT:
      case INTERPRETATION_TYPES.WALKTHROUGH_INTENT:
        return EVIDENCE_TYPES.REPLY_POSITIVE;
      case INTERPRETATION_TYPES.NEGATIVE_INTENT:
      case INTERPRETATION_TYPES.WRONG_PERSON:
        return EVIDENCE_TYPES.REPLY_NEGATIVE;
      case INTERPRETATION_TYPES.NOT_NOW:
        return EVIDENCE_TYPES.REPLY_NEUTRAL;
      case INTERPRETATION_TYPES.OUT_OF_OFFICE:
      case INTERPRETATION_TYPES.AMBIGUOUS_REPLY:
      case INTERPRETATION_TYPES.REPLY_RECEIVED:
        return EVIDENCE_TYPES.REPLY_NEUTRAL;
      case INTERPRETATION_TYPES.BOOKING_CONFIRMED:
        return EVIDENCE_TYPES.BOOKING;
      default:
        break;
    }
  }

  switch (eventType) {
    case 'sent':
    case 'deferred':
      return EVIDENCE_TYPES.SENT;
    case 'delivered':
      return EVIDENCE_TYPES.DELIVERED;
    case 'opened_proxy':
      return EVIDENCE_TYPES.PROXY_OPEN;
    case 'opened':
      return EVIDENCE_TYPES.HUMAN_OPEN;
    case 'clicked':
      return EVIDENCE_TYPES.CLICKED;
    case 'soft_bounce':
      return EVIDENCE_TYPES.SOFT_BOUNCE;
    case 'hard_bounce':
    case 'blocked':
    case 'spam':
      return EVIDENCE_TYPES.HARD_BOUNCE;
    case 'unsubscribed':
      return EVIDENCE_TYPES.UNSUBSCRIBE;
    case 'replied':
      return EVIDENCE_TYPES.REPLY_NEUTRAL;
    default:
      return null;
  }
}

function dispositionFromEvidence(evidenceType, priorDisposition = DISPOSITIONS.UNREACHED, opts = {}) {
  const prior = asText(priorDisposition) || DISPOSITIONS.UNREACHED;
  if (isTerminalDisposition(prior)) return prior;

  switch (asText(evidenceType)) {
    case EVIDENCE_TYPES.SENT:
    case EVIDENCE_TYPES.DELIVERED:
      return prior === DISPOSITIONS.UNREACHED ? DISPOSITIONS.REACHED : prior;
    case EVIDENCE_TYPES.PROXY_OPEN:
      return [DISPOSITIONS.UNREACHED, DISPOSITIONS.REACHED].includes(prior)
        ? DISPOSITIONS.POSSIBLY_SEEN
        : prior;
    case EVIDENCE_TYPES.HUMAN_OPEN:
      return [DISPOSITIONS.UNREACHED, DISPOSITIONS.REACHED, DISPOSITIONS.POSSIBLY_SEEN].includes(prior)
        ? DISPOSITIONS.SEEN
        : prior;
    case EVIDENCE_TYPES.CLICKED:
      return DISPOSITIONS.ENGAGED;
    case EVIDENCE_TYPES.SOFT_BOUNCE:
      return prior === DISPOSITIONS.UNREACHED ? DISPOSITIONS.UNREACHED : prior;
    case EVIDENCE_TYPES.HARD_BOUNCE:
      return DISPOSITIONS.UNREACHABLE;
    case EVIDENCE_TYPES.REPLY_NEUTRAL:
      if (opts.rileyClassification === 'not_now') return DISPOSITIONS.NOT_NOW;
      return DISPOSITIONS.CONVERSING;
    case EVIDENCE_TYPES.REPLY_POSITIVE:
      return DISPOSITIONS.INTERESTED;
    case EVIDENCE_TYPES.REPLY_NEGATIVE:
    case EVIDENCE_TYPES.UNSUBSCRIBE:
      return DISPOSITIONS.REJECTED;
    case EVIDENCE_TYPES.BOOKING:
      return DISPOSITIONS.BOOKED;
    default:
      return prior;
  }
}

function foldCandidateObserveState(priorState = {}, reaction = {}) {
  const priorDisposition = priorState.disposition || DISPOSITIONS.UNREACHED;
  const priorStrength = priorState.evidenceStrength || null;

  if (isTerminalDisposition(priorDisposition)) {
    return {
      disposition: priorDisposition,
      evidenceStrength: priorStrength || reaction.evidenceStrength,
      lastObservationId: reaction.observationId || priorState.lastObservationId,
      lastEvidenceType: reaction.evidenceType || priorState.lastEvidenceType,
      lastReactionId: reaction.id || priorState.lastReactionId,
      recommendedNextAction: reaction.recommendedNextAction || priorState.recommendedNextAction,
      recommendedTiming: reaction.recommendedTiming || priorState.recommendedTiming,
      sequenceStepSent: reaction.sequenceStepSent ?? priorState.sequenceStepSent ?? null,
    };
  }

  const updatedDisposition = reaction.updatedDisposition || priorDisposition;
  const updatedStrength = isTerminalStrength(reaction.evidenceStrength)
    ? reaction.evidenceStrength
    : maxEvidenceStrength(priorStrength, reaction.evidenceStrength);

  return {
    disposition: updatedDisposition,
    evidenceStrength: updatedStrength,
    lastObservationId: reaction.observationId || priorState.lastObservationId,
    lastEvidenceType: reaction.evidenceType || priorState.lastEvidenceType,
    lastReactionId: reaction.id || priorState.lastReactionId,
    recommendedNextAction: reaction.recommendedNextAction || priorState.recommendedNextAction,
    recommendedTiming: reaction.recommendedTiming || priorState.recommendedTiming,
    sequenceStepSent: reaction.sequenceStepSent ?? priorState.sequenceStepSent ?? null,
  };
}

function buildObserveReactionId(observationId) {
  return `obsrx_${asText(observationId)}`;
}

function createObserveReaction(input = {}) {
  const observationId = asText(input.observationId);
  if (!observationId) return null;

  const payload = clone(input);
  return {
    id: buildObserveReactionId(observationId),
    observationId,
    missionId: input.missionId,
    tenantId: input.tenantId != null ? String(input.tenantId) : null,
    prospectId: input.prospectId != null ? String(input.prospectId) : null,
    evidenceType: input.evidenceType,
    evidenceStrength: input.evidenceStrength,
    interpretationType: input.interpretationType || null,
    priorDisposition: input.priorDisposition ?? null,
    updatedDisposition: input.updatedDisposition,
    missionEvidenceTier: input.missionEvidenceTier || input.evidenceStrength,
    recommendedNextAction: input.recommendedNextAction || NEXT_ACTIONS.WAIT,
    recommendedTiming: input.recommendedTiming || {
      kind: 'unresolved',
      dueAt: null,
      waitDays: null,
      cadenceSource: 'unresolved',
      clockStart: null,
      businessDays: false,
    },
    rationale: input.rationale || '',
    humanApprovalRequired: input.humanApprovalRequired === true,
    externalActionPermitted: false,
    cadenceSource: input.cadenceSource || input.recommendedTiming?.cadenceSource || 'unresolved',
    sequenceStepSent: input.sequenceStepSent ?? null,
    at: input.at || nowIso(),
    payload,
  };
}

function buildMissionObserveAssessment(input = {}) {
  const {
    mission = {},
    candidateStates = [],
    latestReaction = null,
  } = input;

  const active = candidateStates.filter(
    (row) => row && !isTerminalDisposition(row.disposition)
  );
  const terminal = candidateStates.filter(
    (row) => row && isTerminalDisposition(row.disposition)
  );

  let evidenceTier = null;
  for (const row of candidateStates) {
    if (!row?.evidenceStrength) continue;
    evidenceTier = maxEvidenceStrength(evidenceTier, row.evidenceStrength);
  }

  const planningConfidence = mission.confidence != null ? mission.confidence : null;
  let confidenceBasis = 'Planning confidence from Scout discovery; no observe evidence yet.';
  if (latestReaction?.rationale) {
    confidenceBasis = `${latestReaction.rationale}; not buying intent.`;
  } else if (evidenceTier) {
    confidenceBasis = `Observe evidence tier: ${evidenceTier}; planning confidence unchanged.`;
  }

  return {
    spec: 'SPEC-251',
    evidenceTier,
    planningConfidence,
    confidenceBasis,
    recommendedNextAction: latestReaction?.recommendedNextAction
      || active[0]?.recommendedNextAction
      || NEXT_ACTIONS.WAIT,
    recommendedTiming: latestReaction?.recommendedTiming
      || active[0]?.recommendedTiming
      || { kind: 'unresolved', dueAt: null, waitDays: null, cadenceSource: 'unresolved' },
    humanApprovalRequired: latestReaction?.humanApprovalRequired === true,
    externalActionPermitted: false,
    activeCandidateCount: active.length,
    terminalCandidateCount: terminal.length,
    latestReactionId: latestReaction?.id || null,
    latestObservationId: latestReaction?.observationId || null,
  };
}

module.exports = {
  EVIDENCE_TYPES,
  EVIDENCE_STRENGTH,
  EVIDENCE_STRENGTH_ORDER,
  DISPOSITIONS,
  ACTIVE_DISPOSITIONS,
  TERMINAL_DISPOSITIONS,
  NEXT_ACTIONS,
  EVIDENCE_TYPE_TO_STRENGTH,
  strengthIndex,
  maxEvidenceStrength,
  isTerminalDisposition,
  isTerminalStrength,
  evidenceTypeFromObservation,
  dispositionFromEvidence,
  foldCandidateObserveState,
  buildObserveReactionId,
  createObserveReaction,
  buildMissionObserveAssessment,
};
