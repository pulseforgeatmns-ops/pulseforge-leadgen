'use strict';

/**
 * Operational event types that must dual-write to Knowledge (SPEC-014).
 * Downstream consumers observe; none reconstruct.
 */
const OPERATIONAL_EVENTS = Object.freeze({
  // Prospect discovery
  COMPANY_DISCOVERED: 'prospect.company_discovered',
  CONTACT_DISCOVERED: 'prospect.contact_discovered',
  DUPLICATE_MERGED: 'prospect.duplicate_merged',
  COMPANY_ENRICHED: 'prospect.company_enriched',
  CONTACT_ENRICHED: 'prospect.contact_enriched',

  // Communications
  EMAIL_DRAFTED: 'comm.email_drafted',
  EMAIL_SENT: 'comm.email_sent',
  EMAIL_DELIVERED: 'comm.email_delivered',
  EMAIL_OPENED: 'comm.email_opened',
  LINK_CLICKED: 'comm.link_clicked',
  REPLY_RECEIVED: 'comm.reply_received',
  BOUNCE: 'comm.bounce',
  UNSUBSCRIBE: 'comm.unsubscribe',

  // Calls
  CALL_SCHEDULED: 'call.scheduled',
  CALL_COMPLETED: 'call.completed',
  CALL_OUTCOME: 'call.outcome',
  VOICEMAIL: 'call.voicemail',
  CALLBACK_REQUESTED: 'call.callback_requested',

  // Meetings
  MEETING_BOOKED: 'meeting.booked',
  MEETING_COMPLETED: 'meeting.completed',
  MEETING_CANCELLED: 'meeting.cancelled',

  // Signals
  SIGNAL_HIRING: 'signal.hiring',
  SIGNAL_WEBSITE_CHANGE: 'signal.website_change',
  SIGNAL_TECHNOLOGY_CHANGE: 'signal.technology_change',
  SIGNAL_OVERFLOW: 'signal.overflow',
  SIGNAL_LOCATION_EXPANSION: 'signal.location_expansion',
  SIGNAL_DECISION_MAKER_CHANGE: 'signal.decision_maker_change',

  // Recommendations
  REC_GENERATED: 'recommendation.generated',
  REC_REVIEWED: 'recommendation.reviewed',
  REC_APPROVED: 'recommendation.approved',
  REC_REJECTED: 'recommendation.rejected',
  REC_EXECUTED: 'recommendation.executed',

  // Outcomes
  OUTCOME_SUCCESS: 'outcome.success',
  OUTCOME_FAILURE: 'outcome.failure',
  OUTCOME_INCONCLUSIVE: 'outcome.inconclusive',
});

const FLIGHT_STAGES = Object.freeze({
  PROSPECT_DISCOVERED: 'prospect_discovered',
  KNOWLEDGE_WRITTEN: 'knowledge_written',
  REASONING_GENERATED: 'reasoning_generated',
  MEMORY_UPDATED: 'memory_updated',
  BRIEFING_UPDATED: 'briefing_updated',
  COMMAND_DECK_REFRESHED: 'command_deck_refreshed',
  VIEWED_BY_OPERATOR: 'viewed_by_operator',
  OUTCOME_RECORDED: 'outcome_recorded',
});

const FLIGHT_STAGE_ORDER = Object.freeze([
  FLIGHT_STAGES.PROSPECT_DISCOVERED,
  FLIGHT_STAGES.KNOWLEDGE_WRITTEN,
  FLIGHT_STAGES.REASONING_GENERATED,
  FLIGHT_STAGES.MEMORY_UPDATED,
  FLIGHT_STAGES.BRIEFING_UPDATED,
  FLIGHT_STAGES.COMMAND_DECK_REFRESHED,
  FLIGHT_STAGES.VIEWED_BY_OPERATOR,
  FLIGHT_STAGES.OUTCOME_RECORDED,
]);

/**
 * Normalize a producer payload into the SPEC-014 KnowledgeEvent contract.
 * No anonymous events — tenantId + source + id required.
 */
function normalizeKnowledgeEvent(input = {}) {
  if (input.tenantId == null || input.tenantId === '') {
    throw new Error('KnowledgeEvent.tenantId is required');
  }
  if (!input.eventType) {
    throw new Error('KnowledgeEvent.eventType is required');
  }
  if (!input.source) {
    throw new Error('KnowledgeEvent.source is required');
  }
  const id =
    input.id ||
    require('crypto').randomUUID();
  return {
    id: String(id),
    tenantId: String(input.tenantId),
    entityId: input.entityId != null ? String(input.entityId) : null,
    entityType: input.entityType || null,
    eventType: String(input.eventType),
    timestamp: input.timestamp || new Date().toISOString(),
    source: String(input.source),
    payload: input.payload && typeof input.payload === 'object' ? input.payload : {},
    evidence:
      input.evidence && typeof input.evidence === 'object' ? input.evidence : {},
  };
}

module.exports = {
  OPERATIONAL_EVENTS,
  FLIGHT_STAGES,
  FLIGHT_STAGE_ORDER,
  normalizeKnowledgeEvent,
};
