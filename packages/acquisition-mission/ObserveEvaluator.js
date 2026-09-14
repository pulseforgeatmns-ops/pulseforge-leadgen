'use strict';

/**
 * SPEC-251 — Deterministic Max OBSERVE reaction evaluator.
 */

const { asText, nowIso } = require('./types');
const { INTERPRETATION_TYPES } = require('./ObservationInterpretation');
const { isCommunicationObservation } = require('./CommunicationObservation');
const { resolveObserveCadence } = require('./ObserveCadence');
const { CADENCE_PROVENANCE } = require('./PreparedOutreachSequence');
const {
  EVIDENCE_TYPES,
  EVIDENCE_TYPE_TO_STRENGTH,
  DISPOSITIONS,
  NEXT_ACTIONS,
  evidenceTypeFromObservation,
  dispositionFromEvidence,
  createObserveReaction,
  buildMissionObserveAssessment,
} = require('./ObserveReaction');

function rileyClassificationFromInterpretation(interpretation = {}) {
  const type = asText(interpretation?.type || interpretation?.interpretation?.type).toLowerCase();
  if (type === INTERPRETATION_TYPES.NOT_NOW) return 'not_now';
  if (type === INTERPRETATION_TYPES.OUT_OF_OFFICE) return 'out_of_office';
  if (type === INTERPRETATION_TYPES.AMBIGUOUS_REPLY) return 'ambiguous';
  return null;
}

function clockStartForEvidence(evidenceType, observation = {}, priorState = {}) {
  const occurredAt = observation.occurredAt || observation.at || null;
  const eventType = asText(observation.eventType).toLowerCase();

  if ([
    EVIDENCE_TYPES.PROXY_OPEN,
    EVIDENCE_TYPES.HUMAN_OPEN,
    EVIDENCE_TYPES.CLICKED,
  ].includes(evidenceType)) {
    return occurredAt;
  }

  if ([EVIDENCE_TYPES.SENT, EVIDENCE_TYPES.DELIVERED].includes(evidenceType)) {
    return occurredAt || priorState.lastClockStart || null;
  }

  if (eventType === 'delivered' || eventType === 'sent') {
    return occurredAt;
  }

  return priorState.recommendedTiming?.clockStart || occurredAt || null;
}

function hasReplyOutcome(outcomes = [], prospectId) {
  const pid = prospectId != null ? String(prospectId) : null;
  return (outcomes || []).some((row) => {
    if (pid && row.prospectId != null && String(row.prospectId) !== pid) return false;
    const type = asText(row.type).toLowerCase();
    return ['interested', 'not_now', 'not_interested', 'unsubscribe', 'wrong_person',
      'walkthrough_requested', 'walkthrough_booked', 'meeting_booked', 'reply'].includes(type);
  });
}

function engagementWithoutReply(evidenceType) {
  return [
    EVIDENCE_TYPES.PROXY_OPEN,
    EVIDENCE_TYPES.HUMAN_OPEN,
    EVIDENCE_TYPES.CLICKED,
  ].includes(evidenceType);
}

function buildRationale(evidenceType, updatedDisposition, opts = {}) {
  switch (evidenceType) {
    case EVIDENCE_TYPES.SENT:
      return 'Provider confirmed send — transport attempted; candidate marked reached.';
    case EVIDENCE_TYPES.DELIVERED:
      return 'Provider confirmed delivery — inbox reached; awaiting engagement or reply.';
    case EVIDENCE_TYPES.PROXY_OPEN:
      return 'Proxy/batch open detected — weak engagement evidence; not buying intent.';
    case EVIDENCE_TYPES.HUMAN_OPEN:
      return 'Human open detected — engagement evidence stronger than delivery; not buying intent.';
    case EVIDENCE_TYPES.CLICKED:
      return 'Link click detected — stronger engagement than open; still not buying intent without a reply.';
    case EVIDENCE_TYPES.SOFT_BOUNCE:
      return 'Soft bounce reported — delivery concern; remain observing without proposing follow-up email.';
    case EVIDENCE_TYPES.HARD_BOUNCE:
      return 'Hard bounce / blocked delivery — candidate unreachable.';
    case EVIDENCE_TYPES.REPLY_POSITIVE:
      return 'Positive reply classified — interested signal; lifecycle owns next stage.';
    case EVIDENCE_TYPES.REPLY_NEUTRAL:
      if (opts.rileyClassification === 'not_now') {
        return 'Prospect asked to defer — not_now disposition; no automatic follow-up send.';
      }
      if (opts.rileyClassification === 'out_of_office') {
        return 'Out-of-office reply — remain observing; wait for return.';
      }
      if (opts.rileyClassification === 'ambiguous') {
        return 'Reply semantics ambiguous — operator review recommended.';
      }
      return 'Reply received — active conversation without clear intent signal.';
    case EVIDENCE_TYPES.REPLY_NEGATIVE:
      return 'Negative or wrong-person reply — candidate rejected.';
    case EVIDENCE_TYPES.UNSUBSCRIBE:
      return 'Unsubscribe signal — terminal negative disposition; no outbound action.';
    case EVIDENCE_TYPES.BOOKING:
      return 'Booking confirmed — success signal; lifecycle owns next stage.';
    default:
      return `Observe evidence ${evidenceType} on candidate ${updatedDisposition}.`;
  }
}

function timingFromCadence(cadence = {}, overrides = {}) {
  return {
    kind: overrides.kind ?? cadence.kind ?? 'unresolved',
    dueAt: overrides.dueAt ?? cadence.dueAt ?? null,
    waitDays: overrides.waitDays ?? cadence.waitDays ?? null,
    cadenceSource: overrides.cadenceSource ?? cadence.cadenceSource ?? 'unresolved',
    clockStart: overrides.clockStart ?? cadence.clockStart ?? null,
    businessDays: false,
    cadenceProvenance: overrides.cadenceProvenance ?? cadence.cadenceProvenance ?? null,
    reconstructed: overrides.reconstructed ?? cadence.reconstructed === true,
  };
}

function resolveNextActionAndTiming(input = {}) {
  const {
    evidenceType,
    updatedDisposition,
    cadence = {},
    hasReply = false,
    rileyClassification = null,
  } = input;

  if ([
    EVIDENCE_TYPES.REPLY_POSITIVE,
    EVIDENCE_TYPES.BOOKING,
  ].includes(evidenceType)) {
    return {
      recommendedNextAction: NEXT_ACTIONS.NONE,
      recommendedTiming: timingFromCadence(cadence, { kind: 'none', waitDays: null, dueAt: null }),
      humanApprovalRequired: false,
    };
  }

  if ([
    EVIDENCE_TYPES.REPLY_NEGATIVE,
    EVIDENCE_TYPES.UNSUBSCRIBE,
    EVIDENCE_TYPES.HARD_BOUNCE,
  ].includes(evidenceType)) {
    return {
      recommendedNextAction: NEXT_ACTIONS.PROPOSE_END_CANDIDATE,
      recommendedTiming: timingFromCadence(cadence, { kind: 'none', waitDays: null, dueAt: null }),
      humanApprovalRequired: false,
    };
  }

  if (evidenceType === EVIDENCE_TYPES.REPLY_NEUTRAL) {
    if (rileyClassification === 'ambiguous') {
      return {
        recommendedNextAction: NEXT_ACTIONS.REVIEW_REPLY,
        recommendedTiming: timingFromCadence(cadence, { kind: 'none', waitDays: null, dueAt: null }),
        humanApprovalRequired: false,
      };
    }
    if (rileyClassification === 'not_now') {
      return {
        recommendedNextAction: NEXT_ACTIONS.NONE,
        recommendedTiming: timingFromCadence(cadence, { kind: 'none', waitDays: null, dueAt: null }),
        humanApprovalRequired: false,
      };
    }
    return {
      recommendedNextAction: NEXT_ACTIONS.WAIT,
      recommendedTiming: timingFromCadence(cadence, { kind: 'wait_until' }),
      humanApprovalRequired: false,
    };
  }

  if (cadence.sequenceExhausted && engagementWithoutReply(evidenceType) && !hasReply) {
    return {
      recommendedNextAction: NEXT_ACTIONS.PROPOSE_END_CANDIDATE,
      recommendedTiming: timingFromCadence(cadence, { kind: 'none', waitDays: null, dueAt: null }),
      humanApprovalRequired: false,
    };
  }

  if (engagementWithoutReply(evidenceType) && !hasReply) {
    if (cadence.cadenceSource === 'unresolved') {
      return {
        recommendedNextAction: NEXT_ACTIONS.WAIT,
        recommendedTiming: timingFromCadence(cadence, {
          kind: 'unresolved',
          dueAt: null,
          waitDays: null,
          cadenceSource: 'unresolved',
        }),
        humanApprovalRequired: false,
      };
    }
    if (cadence.cadenceElapsed) {
      return {
        recommendedNextAction: NEXT_ACTIONS.PROPOSE_FOLLOW_UP,
        recommendedTiming: timingFromCadence(cadence, { kind: 'due' }),
        humanApprovalRequired: true,
      };
    }
    return {
      recommendedNextAction: NEXT_ACTIONS.WAIT,
      recommendedTiming: timingFromCadence(cadence, { kind: 'wait_until' }),
      humanApprovalRequired: false,
    };
  }

  if ([EVIDENCE_TYPES.SENT, EVIDENCE_TYPES.DELIVERED, EVIDENCE_TYPES.SOFT_BOUNCE].includes(evidenceType)) {
    return {
      recommendedNextAction: NEXT_ACTIONS.WAIT,
      recommendedTiming: timingFromCadence(cadence, {
        kind: cadence.cadenceSource === 'unresolved' ? 'unresolved' : 'wait_until',
      }),
      humanApprovalRequired: false,
    };
  }

  return {
    recommendedNextAction: NEXT_ACTIONS.WAIT,
    recommendedTiming: timingFromCadence(cadence, { kind: 'wait_until' }),
    humanApprovalRequired: false,
  };
}

function appendCadenceProvenanceRationale(baseRationale, cadence = {}) {
  if (cadence.cadenceProvenance !== CADENCE_PROVENANCE.HISTORICAL_ANNOTATION
    && cadence.reconstructed !== true) {
    return baseRationale;
  }
  return `${baseRationale} Follow-up timing was reconstructed post-execution from the client template catalog; it was not operator-approved at the original send time.`;
}

/**
 * Pure deterministic evaluator for one canonical observation.
 */
function evaluateObserveReaction(input = {}) {
  const {
    mission = {},
    observation,
    interpretation = null,
    priorState = {},
    store = {},
    outcomes = [],
    executionRecord = null,
    preparedCadence = null,
    now = new Date(),
  } = input;

  if (!observation || !isCommunicationObservation(observation)) {
    return { skipped: true, reason: 'invalid_observation' };
  }

  const evidenceType = evidenceTypeFromObservation(observation, interpretation);
  if (!evidenceType) {
    return { skipped: true, reason: 'unsupported_evidence_type' };
  }

  const evidenceStrength = EVIDENCE_TYPE_TO_STRENGTH[evidenceType];
  const rileyClassification = rileyClassificationFromInterpretation(interpretation);
  const priorDisposition = priorState.disposition || DISPOSITIONS.UNREACHED;
  const updatedDisposition = dispositionFromEvidence(evidenceType, priorDisposition, {
    rileyClassification,
  });

  const sequenceStepSent = priorState.sequenceStepSent ?? 0;
  const clockStart = clockStartForEvidence(evidenceType, observation, priorState);
  const preparedArtifactRevision = observation.evidence?.preparedArtifactRevision
    || executionRecord?.preparedArtifactRevision
    || null;

  const cadence = resolveObserveCadence({
    mission,
    store,
    executionRecord,
    preparedArtifactRevision,
    preparedCadence,
    sequenceStepSent,
    clockStart,
    now,
  });

  const hasReply = hasReplyOutcome(outcomes, observation.prospectId);
  const actionBundle = resolveNextActionAndTiming({
    evidenceType,
    updatedDisposition,
    cadence,
    hasReply,
    rileyClassification,
  });

  const interpretationType = interpretation?.type
    || interpretation?.interpretation?.type
    || null;

  const reaction = createObserveReaction({
    observationId: observation.id,
    missionId: mission.id || observation.missionId,
    tenantId: mission.tenantId || observation.tenantId,
    prospectId: observation.prospectId,
    evidenceType,
    evidenceStrength,
    interpretationType,
    priorDisposition,
    updatedDisposition,
    missionEvidenceTier: evidenceStrength,
    recommendedNextAction: actionBundle.recommendedNextAction,
    recommendedTiming: actionBundle.recommendedTiming,
    rationale: appendCadenceProvenanceRationale(
      buildRationale(evidenceType, updatedDisposition, { rileyClassification }),
      cadence
    ),
    humanApprovalRequired: actionBundle.humanApprovalRequired,
    externalActionPermitted: false,
    cadenceSource: actionBundle.recommendedTiming?.cadenceSource || cadence.cadenceSource,
    sequenceStepSent,
    at: nowIso(now),
  });

  return {
    reaction,
    candidateState: {
      missionId: reaction.missionId,
      prospectId: reaction.prospectId,
      disposition: updatedDisposition,
      evidenceStrength,
      lastObservationId: reaction.observationId,
      lastEvidenceType: evidenceType,
      lastReactionId: reaction.id,
      recommendedNextAction: reaction.recommendedNextAction,
      recommendedTiming: reaction.recommendedTiming,
      sequenceStepSent,
      updatedAt: reaction.at,
    },
  };
}

function buildObserveAssessmentForMission(mission, store = {}) {
  const candidateStates = store.listCandidateObserveStates
    ? store.listCandidateObserveStates(mission.id)
    : [];
  const reactions = store.listObserveReactions
    ? store.listObserveReactions(mission.id)
    : [];
  const latestReaction = reactions.length ? reactions[reactions.length - 1] : null;
  return buildMissionObserveAssessment({
    mission,
    candidateStates,
    latestReaction,
  });
}

module.exports = {
  evaluateObserveReaction,
  buildObserveAssessmentForMission,
  resolveNextActionAndTiming,
  timingFromCadence,
  appendCadenceProvenanceRationale,
  buildRationale,
  clockStartForEvidence,
  rileyClassificationFromInterpretation,
};
