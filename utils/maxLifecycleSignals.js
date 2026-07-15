'use strict';

const { safeIngestNormalizedSignal } = require('./maxSignalIngestion');

const SCOUT_EVENT_TYPES = new Set([
  'prospect_discovered','prospect_qualified','prospect_disqualified_before_outreach','prospect_enrichment_queued',
]);
const MEETING_EVENT_TYPES = new Set([
  'meeting_booked','meeting_cancelled','meeting_showed','meeting_no_showed',
]);

async function safeIngestScoutLifecycleSignal({ prospectId, clientId, eventType, sourceRecordId, eventTimestamp, metadata = {} }, options = {}) {
  if (!SCOUT_EVENT_TYPES.has(eventType)) throw new Error(`Unsupported Scout lifecycle event: ${eventType}`);
  return safeIngestNormalizedSignal({
    prospect_id: prospectId, client_id: clientId, event_type: eventType,
    event_timestamp: eventTimestamp || new Date(), source: 'scout',
    source_record_id: sourceRecordId || `${prospectId}:${eventType}`, metadata,
  }, options);
}

async function safeIngestMeetingSignal({ prospectId, clientId, eventType, source, sourceRecordId, eventTimestamp, metadata = {} }, options = {}) {
  if (!MEETING_EVENT_TYPES.has(eventType)) throw new Error(`Unsupported meeting event: ${eventType}`);
  if (!sourceRecordId) throw new Error('Meeting signals require a canonical source record ID');
  return safeIngestNormalizedSignal({
    prospect_id: prospectId, client_id: clientId, event_type: eventType,
    event_timestamp: eventTimestamp || new Date(), source: source || 'meeting_contract',
    source_record_id: String(sourceRecordId), metadata,
  }, options);
}

module.exports = { MEETING_EVENT_TYPES, SCOUT_EVENT_TYPES, safeIngestMeetingSignal, safeIngestScoutLifecycleSignal };
