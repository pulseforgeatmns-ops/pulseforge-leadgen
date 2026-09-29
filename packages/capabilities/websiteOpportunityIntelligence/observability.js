'use strict';

const { WEB_EVENT_TYPES, CAPABILITY_VERSION } = require('./types');

function buildWebEvent(type, partial = {}) {
  if (!WEB_EVENT_TYPES[type] && !Object.values(WEB_EVENT_TYPES).includes(type)) {
    throw new Error(`Unknown web event type: ${type}`);
  }
  return {
    event_type: Object.values(WEB_EVENT_TYPES).includes(type) ? type : WEB_EVENT_TYPES[type],
    tenant_id: partial.tenant_id ?? partial.client_id ?? null,
    mission_id: partial.mission_id ?? null,
    prospect_id: partial.prospect_id ?? null,
    domain: partial.domain ?? null,
    timestamp: partial.timestamp || new Date().toISOString(),
    capability_version: partial.capability_version || CAPABILITY_VERSION,
    evidence_counts: partial.evidence_counts ?? null,
    score: partial.score ?? null,
    confidence: partial.confidence ?? null,
    recommended_action: partial.recommended_action ?? null,
    payload: partial.payload || {},
  };
}

async function emitWebEvent(type, partial = {}, deps = {}) {
  const event = buildWebEvent(type, partial);
  if (typeof deps.onEvent === 'function') {
    await deps.onEvent(event);
  }
  if (deps.pool) {
    const { ensureWebsiteOpportunitySchema, insertWebsiteOpportunityEvent } = require('../../../services/websiteOpportunityPersistence');
    await ensureWebsiteOpportunitySchema(deps.pool);
    await insertWebsiteOpportunityEvent(deps.pool, event);
  }
  return event;
}

module.exports = {
  WEB_EVENT_TYPES,
  buildWebEvent,
  emitWebEvent,
};
