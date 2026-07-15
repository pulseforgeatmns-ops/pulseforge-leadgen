'use strict';

const { normalizeEventTimestamp } = require('./maxTimestamp');

const EVENT_TYPES = new Set(['meeting_cancelled','meeting_showed','meeting_no_showed']);
const CONFIDENCE_LEVELS = new Set(['confirmed_provider','confirmed_operator']);

function normalizeMeetingOutcomeEvent(input = {}) {
  for (const field of ['client_id','prospect_id','event_type','source','source_record_id','event_timestamp','confidence']) {
    if (input[field] === undefined || input[field] === null || input[field] === '') {
      throw new Error(`Meeting outcome contract requires ${field}`);
    }
  }
  if (!EVENT_TYPES.has(input.event_type)) throw new Error(`Unsupported meeting outcome: ${input.event_type}`);
  if (!CONFIDENCE_LEVELS.has(input.confidence)) throw new Error(`Unsupported meeting outcome confidence: ${input.confidence}`);
  if (!String(input.source_record_id).trim()) throw new Error('Meeting outcome source_record_id must be stable and non-empty');
  return {
    client_id: Number(input.client_id),
    prospect_id: String(input.prospect_id),
    company_id: input.company_id || null,
    event_type: input.event_type,
    source: String(input.source),
    source_record_id: String(input.source_record_id),
    event_timestamp: normalizeEventTimestamp(input.event_timestamp,{source:String(input.source),field:'event_timestamp'}),
    original_event_timestamp: input.original_event_timestamp == null ? null
      : normalizeEventTimestamp(input.original_event_timestamp,{source:String(input.source),field:'original_event_timestamp'}),
    confidence: input.confidence,
    correction_of_event_id: input.correction_of_event_id || null,
    metadata: input.metadata && typeof input.metadata === 'object' ? input.metadata : {},
  };
}

module.exports = { CONFIDENCE_LEVELS, EVENT_TYPES, normalizeMeetingOutcomeEvent };
