'use strict';

/**
 * SPEC-117 — outbound outcomes persist and route into learning sinks.
 * Paige / Scout / Max / Emmett. Learnings never auto-mutate campaigns.
 */

const { OUTCOME_TYPES, LEARNING_SINKS, newId, nowIso, clone } = require('./types');

const EVENT_TYPE_MAP = Object.freeze({
  sent: OUTCOME_TYPES.DELIVERY,
  delivered: OUTCOME_TYPES.DELIVERY,
  delivery: OUTCOME_TYPES.DELIVERY,
  opened: OUTCOME_TYPES.OPEN,
  open: OUTCOME_TYPES.OPEN,
  replied: OUTCOME_TYPES.REPLY,
  reply: OUTCOME_TYPES.REPLY,
  inbound_reply: OUTCOME_TYPES.REPLY,
  hard_bounce: OUTCOME_TYPES.BOUNCE,
  soft_bounce: OUTCOME_TYPES.BOUNCE,
  bounce: OUTCOME_TYPES.BOUNCE,
  blocked: OUTCOME_TYPES.BOUNCE,
  unsubscribed: OUTCOME_TYPES.UNSUBSCRIBE,
  unsubscribe: OUTCOME_TYPES.UNSUBSCRIBE,
  spam: OUTCOME_TYPES.SPAM_COMPLAINT,
  spam_complaint: OUTCOME_TYPES.SPAM_COMPLAINT,
  meeting_booked: OUTCOME_TYPES.MEETING_BOOKED,
  booked: OUTCOME_TYPES.MEETING_BOOKED,
  opportunity_created: OUTCOME_TYPES.OPPORTUNITY_CREATED,
  revenue: OUTCOME_TYPES.REVENUE,
});

function normalizeOutcomeType(eventType) {
  const key = String(eventType || '').toLowerCase();
  return EVENT_TYPE_MAP[key] || null;
}

function sinksFor(outcomeType) {
  switch (outcomeType) {
    case OUTCOME_TYPES.OPEN:
    case OUTCOME_TYPES.REPLY:
    case OUTCOME_TYPES.UNSUBSCRIBE:
    case OUTCOME_TYPES.SPAM_COMPLAINT:
      return [LEARNING_SINKS.PAIGE, LEARNING_SINKS.MAX, LEARNING_SINKS.EMMETT];
    case OUTCOME_TYPES.BOUNCE:
      return [LEARNING_SINKS.SCOUT, LEARNING_SINKS.EMMETT, LEARNING_SINKS.MAX];
    case OUTCOME_TYPES.MEETING_BOOKED:
    case OUTCOME_TYPES.OPPORTUNITY_CREATED:
    case OUTCOME_TYPES.REVENUE:
      return [LEARNING_SINKS.SCOUT, LEARNING_SINKS.MAX, LEARNING_SINKS.PAIGE];
    case OUTCOME_TYPES.DELIVERY:
      return [LEARNING_SINKS.EMMETT, LEARNING_SINKS.MAX];
    default:
      return [LEARNING_SINKS.EMMETT];
  }
}

function recordOutcome(input = {}, now) {
  const type = input.type || normalizeOutcomeType(input.eventType);
  if (!type) return null;
  const sinks = sinksFor(type);
  return {
    id: input.id || newId('out'),
    kind: 'outbound_outcome',
    spec: 'SPEC-117',
    type,
    tenantId: String(input.tenantId || input.clientId || ''),
    clientId: input.clientId || null,
    prospectId: input.prospectId || null,
    inboxId: input.inboxId || null,
    vertical: input.vertical || null,
    subject: input.subject || null,
    amount: input.amount || null,
    eventAt: input.eventAt || nowIso(now),
    sinks,
    autoMutatesCampaign: false,
    payload: clone(input.payload || {}),
  };
}

function learningRecords(outcome) {
  if (!outcome) return [];
  return (outcome.sinks || []).map((sink) => ({
    id: newId('learn'),
    kind: 'outbound_learning',
    spec: 'SPEC-117',
    sink,
    outcomeId: outcome.id,
    outcomeType: outcome.type,
    tenantId: outcome.tenantId,
    clientId: outcome.clientId,
    statement: statementFor(sink, outcome),
    autoApplied: false,
    createdAt: outcome.eventAt,
  }));
}

function statementFor(sink, outcome) {
  const type = outcome.type.replace(/_/g, ' ');
  if (sink === LEARNING_SINKS.PAIGE) return `Paige: ${type} evidence for messaging and creative. Do not auto-rewrite live copy.`;
  if (sink === LEARNING_SINKS.SCOUT) return `Scout: ${type} evidence for ICP and buying-signal weighting. Do not auto-rescope the market.`;
  if (sink === LEARNING_SINKS.MAX) return `Max: ${type} evidence for prioritization and recommendations.`;
  return `Emmett: ${type} evidence for capacity estimation and deliverability decisions.`;
}

module.exports = {
  EVENT_TYPE_MAP,
  normalizeOutcomeType,
  sinksFor,
  recordOutcome,
  learningRecords,
};
