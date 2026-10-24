'use strict';

const { trimHistory } = require('./aoConversationContext');
const { redactObject } = require('./aoConversationRedaction');

function buildReportTranscript(payload) {
  return trimHistory(payload?.messages || [], 20).map(msg => ({
    role: msg.role,
    content: msg.content,
    intent: msg.intent || null,
    ts: msg.ts || null,
  }));
}

function buildReportContext(session, payload, extra = {}) {
  return redactObject({
    session_id: session.id,
    mode: session.mode,
    client_id: session.client_id,
    ao_owner_id: session.ao_owner_id,
    created_at: session.created_at,
    updated_at: session.updated_at,
    last_intent: payload?.last_intent || null,
    selected_account: payload?.selected_account || null,
    prioritized_accounts: payload?.prioritized_accounts || null,
    route: '/ao',
    field_mode: true,
    ...extra,
  });
}

module.exports = {
  buildReportTranscript,
  buildReportContext,
};
