'use strict';

/**
 * Canonical observation interpretation — semantic layer between evidence and business outcomes.
 * raw evidence → interpreted signal → business outcome (via Engine.recordOutcome).
 *
 * Provider telemetry and transport events never directly create business outcomes.
 * Riley owns semantic reply classification; Max owns mission-level interpretation orchestration.
 */

const { asText, nowIso, newId, clone } = require('./types');
const { SPECIALISTS, CONTRIBUTION_KINDS } = require('./types');
const { isCommunicationObservation } = require('./CommunicationObservation');

const INTERPRETATION_TYPES = Object.freeze({
  TRANSPORT_SUCCESS: 'transport_success',
  TRANSPORT_DEFERRED: 'transport_deferred',
  TRANSPORT_FAILURE: 'transport_failure',
  HUMAN_OPEN: 'human_open',
  PROXY_OPEN: 'proxy_open',
  LINK_ENGAGEMENT: 'link_engagement',
  REPLY_RECEIVED: 'reply_received',
  POSITIVE_INTENT: 'positive_intent',
  WALKTHROUGH_INTENT: 'walkthrough_intent',
  NEGATIVE_INTENT: 'negative_intent',
  NOT_NOW: 'not_now',
  UNSUBSCRIBE_INTENT: 'unsubscribe_intent',
  WRONG_PERSON: 'wrong_person',
  OUT_OF_OFFICE: 'out_of_office',
  AMBIGUOUS_REPLY: 'ambiguous_reply',
  BOOKING_CONFIRMED: 'booking_confirmed',
});

/** Events that are evidence-only — must never become business outcomes directly. */
const EVIDENCE_ONLY_EVENT_TYPES = Object.freeze([
  'queued',
  'sent',
  'delivered',
  'opened',
  'opened_proxy',
  'clicked',
  'deferred',
]);

/** Interpreted intent / intermediate business outcomes. */
const INTERMEDIATE_OUTCOME_TYPES = Object.freeze([
  'interested',
  'not_now',
  'walkthrough_requested',
  'qualified_conversation',
  'proposal_requested',
]);

/** Terminal positive business outcomes. */
const TERMINAL_POSITIVE_OUTCOME_TYPES = Object.freeze([
  'walkthrough_booked',
  'meeting_booked',
  'won',
  'closed_won',
]);

/** Terminal negative business outcomes. */
const TERMINAL_NEGATIVE_OUTCOME_TYPES = Object.freeze([
  'not_interested',
  'unsubscribe',
  'wrong_person',
  'lost',
  'closed_lost',
  'bounce',
]);

const BUSINESS_OUTCOME_TYPES = Object.freeze([
  ...INTERMEDIATE_OUTCOME_TYPES,
  ...TERMINAL_POSITIVE_OUTCOME_TYPES,
  ...TERMINAL_NEGATIVE_OUTCOME_TYPES,
]);

/** Legacy transport/engagement types — must not satisfy canonical business-outcome semantics. */
const LEGACY_TRANSPORT_OUTCOME_TYPES = Object.freeze([
  'queued',
  'sent',
  'open',
  'reply',
]);

/**
 * Automatic outcome creation policy.
 * Requires typed interpretation + evidence provenance + sufficient confidence.
 */
const AUTOMATIC_OUTCOME_POLICY = Object.freeze({
  unsubscribe: {
    minConfidence: 0.9,
    sources: ['riley_reply_interpretation', 'provider_observation_interpretation', 'provider_unsubscribe'],
  },
  wrong_person: { minConfidence: 0.85, sources: ['riley_reply_interpretation'] },
  not_interested: { minConfidence: 0.85, sources: ['riley_reply_interpretation'] },
  interested: { minConfidence: 0.8, sources: ['riley_reply_interpretation'] },
  not_now: { minConfidence: 0.8, sources: ['riley_reply_interpretation'] },
  bounce: { minConfidence: 1.0, sources: ['provider_observation_interpretation'] },
  walkthrough_booked: { minConfidence: 1.0, sources: ['booking_evidence'] },
  meeting_booked: { minConfidence: 1.0, sources: ['booking_evidence'] },
});

const WALKTHROUGH_INTENT_PATTERNS = [
  /\b(can you|could you|would you)\s+(come|visit|stop by|swing by)\b/i,
  /\b(schedule|book|set up|arrange)\s+(a\s+)?(walk\s*through|walkthrough|visit|meeting|call|demo)\b/i,
  /\b(tuesday|wednesday|thursday|friday|monday|tomorrow|next week)\b.*\b(work|good|available|free)\b/i,
  /\bwhat time(s)?\s+(work|are you available)\b/i,
  /\blet'?s\s+(meet|schedule|book)\b/i,
];

function isEvidenceOnlyEventType(eventType) {
  return EVIDENCE_ONLY_EVENT_TYPES.includes(asText(eventType).toLowerCase());
}

function isBusinessOutcomeType(type) {
  return BUSINESS_OUTCOME_TYPES.includes(asText(type).toLowerCase());
}

function isLegacyTransportOutcomeType(type) {
  return LEGACY_TRANSPORT_OUTCOME_TYPES.includes(asText(type).toLowerCase());
}

function buildMissionInterpretationContext(mission = {}, store = {}) {
  const contributions = store.listContributions
    ? store.listContributions(mission.id)
    : (store.contributions || []).filter((row) => row.missionId === mission.id);

  const scoutDiscovery = contributions.find(
    (row) => row.specialist === SPECIALISTS.SCOUT && row.kind === CONTRIBUTION_KINDS.DISCOVERY
  );
  const maxPrioritization = contributions.find(
    (row) => row.specialist === SPECIALISTS.MAX && row.kind === CONTRIBUTION_KINDS.PRIORITIZATION
  );
  const paigeArtifact = contributions.find(
    (row) => row.specialist === SPECIALISTS.PAIGE && row.kind === CONTRIBUTION_KINDS.VARIANTS
  );

  const executionRecords = store.listExecutionRecords
    ? store.listExecutionRecords(mission.id)
    : (store.executionRecords || []).filter((row) => row.missionId === mission.id);

  const priorObservations = store.listObservations
    ? store.listObservations(mission.id)
    : (store.observations || []).filter((row) => row.missionId === mission.id);

  return {
    missionId: mission.id,
    tenantId: mission.tenantId,
    structuredMission: mission.structuredMission || null,
    scoutDiscovery: scoutDiscovery ? scoutDiscovery.payload : null,
    maxPrioritization: maxPrioritization ? maxPrioritization.payload : null,
    paigeArtifact: paigeArtifact ? paigeArtifact.payload : null,
    executionRecords,
    priorObservations,
  };
}

function interpretTransportObservation(observation = {}) {
  const eventType = asText(observation.eventType).toLowerCase();
  if (eventType === 'delivered' || eventType === 'sent') {
    return {
      type: INTERPRETATION_TYPES.TRANSPORT_SUCCESS,
      confidence: 1,
      rationale: `Provider confirmed ${eventType} — transport evidence only.`,
    };
  }
  if (eventType === 'deferred') {
    return {
      type: INTERPRETATION_TYPES.TRANSPORT_DEFERRED,
      confidence: 1,
      rationale: 'Delivery deferred by provider — transport evidence only.',
    };
  }
  if (['hard_bounce', 'soft_bounce', 'blocked', 'spam'].includes(eventType)) {
    return {
      type: INTERPRETATION_TYPES.TRANSPORT_FAILURE,
      confidence: 1,
      rationale: `Provider reported ${eventType} — deliverability failure.`,
    };
  }
  return null;
}

function interpretEngagementObservation(observation = {}) {
  const eventType = asText(observation.eventType).toLowerCase();
  if (eventType === 'opened') {
    return {
      type: INTERPRETATION_TYPES.HUMAN_OPEN,
      confidence: 1,
      rationale: 'Human open detected — engagement evidence only.',
    };
  }
  if (eventType === 'opened_proxy') {
    return {
      type: INTERPRETATION_TYPES.PROXY_OPEN,
      confidence: 1,
      rationale: 'Proxy/batch open detected — engagement evidence only.',
    };
  }
  if (eventType === 'clicked') {
    return {
      type: INTERPRETATION_TYPES.LINK_ENGAGEMENT,
      confidence: 1,
      rationale: 'Link click detected — engagement evidence only.',
    };
  }
  if (eventType === 'replied') {
    return {
      type: INTERPRETATION_TYPES.REPLY_RECEIVED,
      confidence: 1,
      rationale: 'Reply event received without semantic content — awaiting Riley interpretation.',
    };
  }
  if (eventType === 'unsubscribed') {
    return {
      type: INTERPRETATION_TYPES.UNSUBSCRIBE_INTENT,
      confidence: 0.95,
      rationale: 'Provider unsubscribe event — semantic unsubscribe intent.',
    };
  }
  return null;
}

function recommendedOutcomeForTransportFailure(eventType) {
  if (['hard_bounce', 'blocked', 'spam'].includes(asText(eventType).toLowerCase())) {
    return { type: 'bounce', terminal: true };
  }
  return null;
}

function recommendedOutcomeForProviderObservation(interpretation, observation = {}) {
  const eventType = asText(observation.eventType).toLowerCase();

  if (interpretation.type === INTERPRETATION_TYPES.UNSUBSCRIBE_INTENT) {
    return { type: 'unsubscribe', terminal: true };
  }
  if (interpretation.type === INTERPRETATION_TYPES.TRANSPORT_FAILURE) {
    return recommendedOutcomeForTransportFailure(eventType);
  }

  // Evidence-only — no business outcome
  if (isEvidenceOnlyEventType(eventType)) {
    return null;
  }
  return null;
}

function detectWalkthroughIntent(replyText = '') {
  const text = asText(replyText);
  if (!text) return false;
  return WALKTHROUGH_INTENT_PATTERNS.some((pattern) => pattern.test(text));
}

/**
 * Riley reply classification → canonical interpretation.
 * Riley provides semantic evidence; does not advance mission lifecycle.
 */
function interpretRileyReply(input = {}) {
  const {
    missionId,
    prospectId,
    classification,
    replyText,
    observationId,
    correlation = {},
    confidence: inputConfidence,
  } = input;

  const normalized = asText(classification).toLowerCase();
  const hasWalkthroughIntent = detectWalkthroughIntent(replyText);

  const evidence = [];
  if (observationId) {
    evidence.push({ kind: 'communication_observation', ref: observationId });
  }
  if (correlation.executionRecordId) {
    evidence.push({ kind: 'execution_record', ref: correlation.executionRecordId });
  }
  if (correlation.providerMessageId) {
    evidence.push({ kind: 'provider_message', ref: correlation.providerMessageId });
  }
  evidence.push({ kind: 'riley_classification', ref: normalized });

  let interpretation;
  let recommendedOutcome = null;
  let requiresHumanConfirmation = false;

  switch (normalized) {
    case 'interested':
      if (hasWalkthroughIntent) {
        interpretation = {
          type: INTERPRETATION_TYPES.WALKTHROUGH_INTENT,
          confidence: inputConfidence ?? 0.91,
          rationale: 'Prospect explicitly requested scheduling options.',
        };
        recommendedOutcome = { type: 'walkthrough_requested', terminal: false };
      } else {
        interpretation = {
          type: INTERPRETATION_TYPES.POSITIVE_INTENT,
          confidence: inputConfidence ?? 0.85,
          rationale: 'Riley classified reply as interested.',
        };
        recommendedOutcome = { type: 'interested', terminal: false };
      }
      break;

    case 'not_now':
      interpretation = {
        type: INTERPRETATION_TYPES.NOT_NOW,
        confidence: inputConfidence ?? 0.85,
        rationale: 'Riley classified reply as not now.',
      };
      recommendedOutcome = { type: 'not_now', terminal: false };
      break;

    case 'negative':
      interpretation = {
        type: INTERPRETATION_TYPES.NEGATIVE_INTENT,
        confidence: inputConfidence ?? 0.88,
        rationale: 'Riley classified reply as negative.',
      };
      recommendedOutcome = { type: 'not_interested', terminal: true };
      break;

    case 'unsubscribe':
      interpretation = {
        type: INTERPRETATION_TYPES.UNSUBSCRIBE_INTENT,
        confidence: inputConfidence ?? 0.95,
        rationale: 'Riley classified reply as unsubscribe.',
      };
      recommendedOutcome = { type: 'unsubscribe', terminal: true };
      break;

    case 'wrong_person':
      interpretation = {
        type: INTERPRETATION_TYPES.WRONG_PERSON,
        confidence: inputConfidence ?? 0.9,
        rationale: 'Riley classified reply as wrong person.',
      };
      recommendedOutcome = { type: 'wrong_person', terminal: true };
      break;

    case 'out_of_office':
      interpretation = {
        type: INTERPRETATION_TYPES.OUT_OF_OFFICE,
        confidence: inputConfidence ?? 0.95,
        rationale: 'Riley classified reply as out of office.',
      };
      recommendedOutcome = null;
      break;

    case 'unknown':
    default:
      interpretation = {
        type: INTERPRETATION_TYPES.AMBIGUOUS_REPLY,
        confidence: inputConfidence ?? 0.5,
        rationale: 'Reply semantics ambiguous — operator review recommended.',
      };
      requiresHumanConfirmation = true;
      recommendedOutcome = null;
      break;
  }

  return {
    missionId,
    prospectId,
    observationId: observationId || null,
    interpretation,
    recommendedOutcome,
    evidence,
    requiresHumanConfirmation,
    source: 'riley_reply_interpretation',
  };
}

/**
 * Booking evidence → walkthrough_booked interpretation.
 */
function interpretBookingEvidence(input = {}) {
  const {
    missionId,
    prospectId,
    bookingRef,
    observationId,
    correlation = {},
  } = input;

  const evidence = [];
  if (observationId) evidence.push({ kind: 'communication_observation', ref: observationId });
  if (bookingRef) evidence.push({ kind: 'booking', ref: bookingRef });
  if (correlation.executionRecordId) {
    evidence.push({ kind: 'execution_record', ref: correlation.executionRecordId });
  }

  return {
    missionId,
    prospectId,
    observationId: observationId || null,
    interpretation: {
      type: INTERPRETATION_TYPES.BOOKING_CONFIRMED,
      confidence: 1,
      rationale: 'Calendar/booking integration confirmed walkthrough.',
    },
    recommendedOutcome: { type: 'walkthrough_booked', terminal: true },
    evidence,
    requiresHumanConfirmation: false,
    source: 'booking_evidence',
  };
}

/**
 * Mission-bound interpretation of a communication observation.
 * Max orchestrates; Emmett observations supply evidence only.
 */
function interpretMissionObservation(input = {}) {
  const {
    missionId,
    prospectId,
    observation,
    missionContext = {},
  } = input;

  if (!observation || !isCommunicationObservation(observation)) {
    return null;
  }

  const observationId = observation.id;
  const evidence = [{ kind: 'communication_observation', ref: observationId }];
  if (observation.evidence?.executionRecordId) {
    evidence.push({ kind: 'execution_record', ref: observation.evidence.executionRecordId });
  }
  if (observation.evidence?.providerMessageId) {
    evidence.push({ kind: 'provider_message', ref: observation.evidence.providerMessageId });
  }

  let interpretation = null;
  if (observation.category === 'delivery') {
    interpretation = interpretTransportObservation(observation);
  } else if (observation.category === 'engagement') {
    interpretation = interpretEngagementObservation(observation);
  }

  if (!interpretation) return null;

  const recommendedOutcome = recommendedOutcomeForProviderObservation(interpretation, observation);

  return {
    missionId: missionId || observation.missionId,
    prospectId: prospectId || observation.prospectId,
    observationId,
    interpretation,
    recommendedOutcome,
    evidence,
    requiresHumanConfirmation: false,
    source: 'provider_observation_interpretation',
    missionContext: {
      hasStructuredMission: Boolean(missionContext.structuredMission),
      priorObservationCount: (missionContext.priorObservations || []).length,
    },
  };
}

function resolveOutcomeSource(result = {}) {
  const explicit = asText(result.source);
  if (explicit) return explicit;
  if ((result.evidence || []).some((row) => row.kind === 'riley_classification')) {
    return 'riley_reply_interpretation';
  }
  if ((result.evidence || []).some((row) => row.kind === 'booking')) {
    return 'booking_evidence';
  }
  return 'provider_observation_interpretation';
}

/**
 * Decide whether an interpretation should produce a canonical business outcome.
 */
function shouldCreateOutcome(result = {}, opts = {}) {
  if (!result.recommendedOutcome || !result.recommendedOutcome.type) return false;
  if (result.requiresHumanConfirmation && !opts.humanConfirmed) return false;

  const outcomeType = asText(result.recommendedOutcome.type).toLowerCase();
  if (isLegacyTransportOutcomeType(outcomeType)) return false;
  if (isEvidenceOnlyEventType(outcomeType)) return false;

  const policy = AUTOMATIC_OUTCOME_POLICY[outcomeType];
  if (!policy) return false;

  const confidence = Number(result.interpretation?.confidence ?? 0);
  if (confidence < policy.minConfidence) return false;

  const source = resolveOutcomeSource(result);
  if (!policy.sources.includes(source)) return false;

  return true;
}

function buildOutcomePayload(result = {}, interpretationRow = {}) {
  return {
    interpretationId: interpretationRow.id,
    observationIds: result.observationId ? [result.observationId] : [],
    executionRecordId: (result.evidence || []).find((row) => row.kind === 'execution_record')?.ref || null,
    providerMessageId: (result.evidence || []).find((row) => row.kind === 'provider_message')?.ref || null,
    source: resolveOutcomeSource(result),
    confidence: result.interpretation?.confidence ?? null,
    rationale: result.interpretation?.rationale || null,
    evidence: clone(result.evidence || []),
    requiresHumanConfirmation: result.requiresHumanConfirmation === true,
  };
}

function createInterpretationRecord(result = {}) {
  return {
    id: newId('interp'),
    missionId: result.missionId,
    prospectId: result.prospectId || null,
    observationId: result.observationId || null,
    type: result.interpretation?.type,
    confidence: result.interpretation?.confidence ?? null,
    rationale: result.interpretation?.rationale || null,
    recommendedOutcome: result.recommendedOutcome ? clone(result.recommendedOutcome) : null,
    evidence: clone(result.evidence || []),
    requiresHumanConfirmation: result.requiresHumanConfirmation === true,
    source: resolveOutcomeSource(result),
    at: nowIso(),
    payload: {
      interpretation: clone(result.interpretation || {}),
    },
  };
}

module.exports = {
  INTERPRETATION_TYPES,
  EVIDENCE_ONLY_EVENT_TYPES,
  INTERMEDIATE_OUTCOME_TYPES,
  TERMINAL_POSITIVE_OUTCOME_TYPES,
  TERMINAL_NEGATIVE_OUTCOME_TYPES,
  BUSINESS_OUTCOME_TYPES,
  LEGACY_TRANSPORT_OUTCOME_TYPES,
  AUTOMATIC_OUTCOME_POLICY,
  isEvidenceOnlyEventType,
  isBusinessOutcomeType,
  isLegacyTransportOutcomeType,
  buildMissionInterpretationContext,
  interpretMissionObservation,
  interpretRileyReply,
  interpretBookingEvidence,
  shouldCreateOutcome,
  buildOutcomePayload,
  createInterpretationRecord,
  detectWalkthroughIntent,
  resolveOutcomeSource,
};
