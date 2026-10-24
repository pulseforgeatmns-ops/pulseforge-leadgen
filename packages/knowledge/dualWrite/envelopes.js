'use strict';

const { KNOWLEDGE_EVENTS } = require('../events/KnowledgeEventBus');
const {
  mapCompanyRow,
  mapProspectRow,
  mapTouchpointRow,
  mapEntityMutation,
} = require('../sync/mappers');
const {
  companyNodeId,
  personNodeId,
  interactionNodeId,
  stableEvidenceId,
  syncIdempotencyKey,
} = require('../sync/stableIds');
const { SYNC_ENTITY_KINDS } = require('../sync/syncEvents');
const {
  OPERATIONAL_EVENTS,
  normalizeKnowledgeEvent,
} = require('./operationalEvents');

/**
 * Build a GraphSyncEngine envelope from a CRM company row + operational context.
 */
function envelopeForCompany(row, options = {}) {
  const event = mapCompanyRow(row, {
    sourceType: options.sourceType || 'crm_company',
    confidence: options.confidence,
    revision: options.revision,
  });
  if (options.operationalEventType) {
    event.payload.knowledgeEvent.payload.metadata =
      event.payload.knowledgeEvent.payload.metadata || {};
    event.payload.knowledgeEvent.payload.metadata.operationalEventType =
      options.operationalEventType;
  }
  return event;
}

/**
 * Build a GraphSyncEngine envelope from a CRM prospect row.
 */
function envelopeForProspect(row, options = {}) {
  const event = mapProspectRow(row, {
    sourceType: options.sourceType || 'crm_prospect',
    confidence: options.confidence,
    revision: options.revision,
  });
  if (options.operationalEventType) {
    event.payload.knowledgeEvent.payload.metadata =
      event.payload.knowledgeEvent.payload.metadata || {};
    event.payload.knowledgeEvent.payload.metadata.operationalEventType =
      options.operationalEventType;
  }
  return event;
}

/**
 * Build a GraphSyncEngine envelope from a CRM touchpoint row.
 */
function envelopeForTouchpoint(row, options = {}) {
  const event = mapTouchpointRow(row, {
    revision: options.revision,
  });
  if (options.operationalEventType) {
    event.payload.knowledgeEvent.payload.metadata =
      event.payload.knowledgeEvent.payload.metadata || {};
    event.payload.knowledgeEvent.payload.metadata.operationalEventType =
      options.operationalEventType;
  }
  return event;
}

/**
 * Map a generic operational event (signal / recommendation / outcome / call / meeting)
 * into an evidence + optional interaction sync envelope.
 */
function envelopeForOperationalEvent(knowledgeEvent, options = {}) {
  const evt = normalizeKnowledgeEvent(knowledgeEvent);
  const tenantId = evt.tenantId;
  const revision = options.revision || evt.timestamp || 'v1';
  const entityKind = options.entityKind || evt.entityType || 'operational';
  const entityId = evt.entityId || evt.id;
  const sourceType = options.sourceType || `ops_${evt.eventType}`;
  const sourceId = evt.evidence.sourceId || `${evt.eventType}:${evt.id}`;
  const evidenceId =
    evt.evidence.id ||
    stableEvidenceId(tenantId, sourceType, sourceId);

  const participantId =
    evt.entityType === 'prospect' || evt.entityType === 'person'
      ? personNodeId(tenantId, entityId)
      : null;
  const companyId =
    evt.entityType === 'company'
      ? companyNodeId(tenantId, entityId)
      : evt.payload.companyId
        ? companyNodeId(tenantId, evt.payload.companyId)
        : null;

  // Prefer interaction node when channel/action present; else evidence-only claim path.
  const wantsInteraction =
    options.asInteraction === true ||
    evt.payload.channel ||
    evt.payload.actionType ||
    String(evt.eventType).startsWith('comm.') ||
    String(evt.eventType).startsWith('call.') ||
    String(evt.eventType).startsWith('meeting.');

  if (wantsInteraction) {
    const interactionId = interactionNodeId(
      tenantId,
      evt.payload.touchpointId || evt.id
    );
    return mapEntityMutation({
      tenantId,
      entityKind: SYNC_ENTITY_KINDS.TOUCHPOINT || 'touchpoint',
      entityId: String(evt.payload.touchpointId || evt.id),
      revision: String(revision),
      id: syncIdempotencyKey(
        tenantId,
        entityKind,
        entityId,
        `${evt.eventType}:${revision}`
      ),
      occurredAt: evt.timestamp,
      knowledgeEvent: {
        type: KNOWLEDGE_EVENTS.INTERACTION_RECORDED,
        tenantId,
        payload: {
          nodeId: interactionId,
          channel: evt.payload.channel || inferChannel(evt.eventType),
          actionType: evt.payload.actionType || evt.eventType,
          summary:
            evt.payload.summary ||
            evt.evidence.summary ||
            `${evt.eventType} (${evt.source})`,
          occurredAt: evt.timestamp,
          participantId,
          metadata: {
            operationalEventType: evt.eventType,
            source: evt.source,
            evidenceId,
            companyId,
            ...(evt.payload.metadata || {}),
          },
        },
      },
    });
  }

  return mapEntityMutation({
    tenantId,
    entityKind,
    entityId: String(entityId),
    revision: String(revision),
    id: syncIdempotencyKey(
      tenantId,
      entityKind,
      entityId,
      `${evt.eventType}:${revision}`
    ),
    occurredAt: evt.timestamp,
    knowledgeEvent: {
      type: KNOWLEDGE_EVENTS.EVIDENCE_RECORDED,
      tenantId,
      payload: {
        nodeId: evidenceId,
        evidenceId,
        sourceType,
        sourceId,
        summary:
          evt.evidence.summary ||
          evt.payload.summary ||
          `${evt.eventType} from ${evt.source}`,
        confidence:
          evt.evidence.confidence == null ? 0.75 : Number(evt.evidence.confidence),
        aboutNodeId: participantId || companyId || null,
        subjectId: participantId || companyId || null,
        payload: {
          operationalEventType: evt.eventType,
          entityId: evt.entityId,
          entityType: evt.entityType,
          source: evt.source,
          ...(evt.payload || {}),
        },
      },
    },
  });
}

function inferChannel(eventType) {
  if (String(eventType).startsWith('comm.')) return 'email';
  if (String(eventType).startsWith('call.')) return 'phone';
  if (String(eventType).startsWith('meeting.')) return 'meeting';
  if (String(eventType).startsWith('signal.')) return 'signal';
  return 'ops';
}

/**
 * Infer operational event type from a Brevo / touchpoint action.
 */
function operationalEventFromTouchpoint(channel, actionType) {
  const action = String(actionType || '').toLowerCase();
  const ch = String(channel || '').toLowerCase();
  if (ch === 'email' || ch === 'brevo') {
    if (action.includes('draft')) return OPERATIONAL_EVENTS.EMAIL_DRAFTED;
    if (action.includes('sent') || action === 'send') return OPERATIONAL_EVENTS.EMAIL_SENT;
    if (action.includes('deliver')) return OPERATIONAL_EVENTS.EMAIL_DELIVERED;
    if (action.includes('open')) return OPERATIONAL_EVENTS.EMAIL_OPENED;
    if (action.includes('click')) return OPERATIONAL_EVENTS.LINK_CLICKED;
    if (action.includes('reply')) return OPERATIONAL_EVENTS.REPLY_RECEIVED;
    if (action.includes('bounce')) return OPERATIONAL_EVENTS.BOUNCE;
    if (action.includes('unsub')) return OPERATIONAL_EVENTS.UNSUBSCRIBE;
  }
  if (ch === 'phone' || ch === 'call' || ch === 'bland') {
    if (action.includes('schedul')) return OPERATIONAL_EVENTS.CALL_SCHEDULED;
    if (action.includes('voicemail') || action === 'vm') return OPERATIONAL_EVENTS.VOICEMAIL;
    if (action.includes('callback')) return OPERATIONAL_EVENTS.CALLBACK_REQUESTED;
    if (action.includes('complet')) return OPERATIONAL_EVENTS.CALL_COMPLETED;
    return OPERATIONAL_EVENTS.CALL_OUTCOME;
  }
  if (ch === 'meeting' || ch === 'calendar') {
    if (action.includes('cancel')) return OPERATIONAL_EVENTS.MEETING_CANCELLED;
    if (action.includes('complet') || action.includes('show')) {
      return OPERATIONAL_EVENTS.MEETING_COMPLETED;
    }
    return OPERATIONAL_EVENTS.MEETING_BOOKED;
  }
  return `ops.touchpoint.${ch || 'unknown'}.${action || 'unknown'}`;
}

module.exports = {
  envelopeForCompany,
  envelopeForProspect,
  envelopeForTouchpoint,
  envelopeForOperationalEvent,
  operationalEventFromTouchpoint,
  OPERATIONAL_EVENTS,
};
