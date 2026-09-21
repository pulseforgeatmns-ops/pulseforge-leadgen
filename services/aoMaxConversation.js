'use strict';

const pool = require('../db');
const {
  trimHistory,
  resolveConversationIntent,
  buildWhyPrioritizedReply,
  buildAccountContactsReply,
  mergeConversationContext,
  MAX_HISTORY_TURNS,
} = require('../utils/aoConversationContext');
const { redactObject } = require('../utils/aoConversationRedaction');
const {
  buildReportTranscript,
  buildReportContext,
} = require('../utils/aoConversationReport');
const {
  buildAccountPrioritizationReply,
  buildAccountBriefingReply,
  findAssignedLeadById,
  findAssignedLeadByName,
  listAssignedAccountSummaries,
} = require('./aoAccountIntelligence');
const { buildCoachingReply } = require('../utils/aoAccountPrioritization');

async function createConversationSession({ aoOwnerId, clientId, initialPayload = {} }) {
  const payload = {
    messages: [],
    ...initialPayload,
  };
  const { rows } = await pool.query(`
    INSERT INTO ao_max_sessions (ao_owner_id, client_id, mode, step_index, payload)
    VALUES ($1, $2, 'conversation', 0, $3::jsonb)
    RETURNING *
  `, [aoOwnerId, clientId, JSON.stringify(payload)]);
  return rows[0];
}

async function getActiveConversationSession(sessionId, aoOwnerId) {
  const { rows } = await pool.query(`
    SELECT * FROM ao_max_sessions
    WHERE id = $1
      AND ao_owner_id = $2
      AND mode = 'conversation'
      AND completed = false
    LIMIT 1
  `, [sessionId, aoOwnerId]);
  return rows[0] || null;
}

async function persistConversationPayload(sessionId, payload) {
  await pool.query(`
    UPDATE ao_max_sessions
    SET payload = $2::jsonb, updated_at = NOW()
    WHERE id = $1
  `, [sessionId, JSON.stringify(payload)]);
}

async function completeConversationSession(sessionId) {
  await pool.query(`
    UPDATE ao_max_sessions
    SET completed = true, updated_at = NOW()
    WHERE id = $1
  `, [sessionId]);
}

function appendMessage(payload, { role, content, intent = null, meta = null }) {
  const messages = trimHistory([
    ...(payload.messages || []),
    {
      role,
      content,
      intent,
      meta,
      ts: new Date().toISOString(),
    },
  ], MAX_HISTORY_TURNS);

  return { ...payload, messages };
}

async function resolveIntentWithAssignedAccounts({ aoOwnerId, clientId, message, context }) {
  let resolved = resolveConversationIntent(message, context);

  const needsAccountLookup = resolved?.intent === 'account_briefing' || resolved?.intent === 'account_contacts';
  if (needsAccountLookup && !resolved.account && !resolved.ambiguous) {
    const assignedAccounts = await listAssignedAccountSummaries({ aoOwnerId, clientId });
    resolved = resolveConversationIntent(message, context, assignedAccounts);
  }

  return resolved;
}

async function executeConversationIntent({ aoOwnerId, clientId, message, context }) {
  const resolved = await resolveIntentWithAssignedAccounts({ aoOwnerId, clientId, message, context });

  if (resolved.ambiguous && resolved.ambiguityReply) {
    return {
      intent: resolved.intent,
      reply: resolved.ambiguityReply,
      account: null,
      accounts: null,
    };
  }

  if (resolved.intent === 'why_prioritized') {
    const reply = buildWhyPrioritizedReply(resolved.account, context);
    return {
      intent: 'why_prioritized',
      reply,
      account: resolved.account || null,
      accounts: null,
    };
  }

  if (resolved.intent === 'account_prioritization') {
    return buildAccountPrioritizationReply({ aoOwnerId, clientId });
  }

  if (resolved.intent === 'account_briefing') {
    if (!resolved.account?.business_name) {
      return {
        intent: 'account_briefing',
        reply: `I couldn't find "${resolved.briefingTarget || message}" in your assigned accounts. Double-check the name or open Queue to browse your list.`,
        account: null,
      };
    }

    const result = await buildAccountBriefingReply({
      aoOwnerId,
      clientId,
      businessNameQuery: resolved.account.business_name,
      leadId: resolved.account.lead_id || null,
    });
    return result;
  }

  if (resolved.intent === 'account_contacts') {
    const lead = resolved.account?.lead_id
      ? await findAssignedLeadById({
        aoOwnerId,
        clientId,
        leadId: resolved.account.lead_id,
      })
      : resolved.account?.business_name
        ? await findAssignedLeadByName({
          aoOwnerId,
          clientId,
          businessNameQuery: resolved.account.business_name,
        })
        : null;

    return {
      intent: 'account_contacts',
      reply: buildAccountContactsReply(lead),
      account: lead
        ? { business_name: lead.business_name, lead_id: lead.id }
        : resolved.account || null,
    };
  }

  const coachingMessage = resolved.account?.business_name
    ? `${message} (Context: currently discussing ${resolved.account.business_name}.)`
    : message;

  return buildCoachingReply(coachingMessage);
}

async function handleConversationTurn({ sessionId, aoOwnerId, clientId, message }) {
  let session = sessionId
    ? await getActiveConversationSession(sessionId, aoOwnerId)
    : null;

  if (!session) {
    session = await createConversationSession({ aoOwnerId, clientId });
  }

  let payload = { ...(session.payload || {}) };
  payload = appendMessage(payload, { role: 'user', content: message });

  const turnResult = await executeConversationIntent({
    aoOwnerId,
    clientId,
    message,
    context: payload,
  });

  payload = mergeConversationContext(payload, turnResult);
  payload = appendMessage(payload, {
    role: 'max',
    content: turnResult.reply,
    intent: turnResult.intent,
    meta: turnResult.account
      ? { account: turnResult.account }
      : turnResult.accounts
        ? { account_count: turnResult.accounts.length }
        : null,
  });

  await persistConversationPayload(session.id, payload);

  return {
    session_id: session.id,
    mode: 'conversation',
    completed: false,
    intent: turnResult.intent,
    reply: turnResult.reply,
    accounts: turnResult.accounts || null,
    account: turnResult.account || null,
    escalate: turnResult.escalate || false,
    escalation_reason: turnResult.escalation_reason || null,
  };
}

async function startNewConversation({ aoOwnerId, clientId, previousSessionId = null }) {
  if (previousSessionId) {
    const previous = await getActiveConversationSession(previousSessionId, aoOwnerId);
    if (previous) {
      await completeConversationSession(previousSessionId);
    }
  }

  const session = await createConversationSession({ aoOwnerId, clientId });
  return {
    session_id: session.id,
    mode: 'conversation',
    completed: false,
    reply: 'New conversation started. What do you need help with?',
  };
}

async function appendConversationEvent({
  sessionId,
  aoOwnerId,
  role,
  content,
  intent = null,
  meta = null,
}) {
  const session = await getActiveConversationSession(sessionId, aoOwnerId);
  if (!session) return null;

  let payload = { ...(session.payload || {}) };
  payload = appendMessage(payload, { role, content, intent, meta });

  if (meta?.account?.business_name) {
    payload.selected_account = {
      business_name: meta.account.business_name,
      lead_id: meta.account.lead_id || meta.account.id || null,
    };
  }

  if (meta?.prioritized_accounts?.length) {
    payload.prioritized_accounts = meta.prioritized_accounts;
  }

  await persistConversationPayload(session.id, payload);
  return { session_id: session.id, messages: payload.messages.length };
}

async function reportConversation({
  sessionId,
  aoOwnerId,
  clientId,
  note = '',
  category = 'user_report',
}) {
  const session = await getActiveConversationSession(sessionId, aoOwnerId);
  if (!session) {
    return { error: 'Active conversation not found', status: 404 };
  }

  if (Number(session.client_id) !== Number(clientId)) {
    return { error: 'Conversation tenant mismatch', status: 403 };
  }

  const payload = session.payload || {};
  const transcript = redactObject(buildReportTranscript(payload));
  const context = buildReportContext(session, payload);

  const { rows } = await pool.query(`
    INSERT INTO ao_max_conversation_reports (
      client_id, ao_owner_id, session_id, category, note, transcript_json, context_json
    )
    VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb)
    RETURNING id, created_at, status
  `, [
    clientId,
    aoOwnerId,
    sessionId,
    category,
    String(note || '').trim() || null,
    JSON.stringify(transcript),
    JSON.stringify(context),
  ]);

  return {
    ok: true,
    report_id: rows[0].id,
    created_at: rows[0].created_at,
    status: rows[0].status,
  };
}

async function listConversationReports({ clientId = null, limit = 50 } = {}) {
  const params = [];
  let where = '';
  if (clientId) {
    params.push(clientId);
    where = `WHERE r.client_id = $${params.length}`;
  }
  params.push(limit);

  const { rows } = await pool.query(`
    SELECT
      r.id,
      r.client_id,
      r.ao_owner_id,
      r.session_id,
      r.category,
      r.note,
      r.status,
      r.created_at,
      u.name AS ao_name,
      u.email AS ao_email
    FROM ao_max_conversation_reports r
    LEFT JOIN users u ON u.id = r.ao_owner_id
    ${where}
    ORDER BY r.created_at DESC
    LIMIT $${params.length}
  `, params);

  return rows;
}

async function getConversationReport(reportId, { clientId = null } = {}) {
  const params = [reportId];
  let where = 'WHERE r.id = $1';
  if (clientId) {
    params.push(clientId);
    where += ` AND r.client_id = $${params.length}`;
  }

  const { rows } = await pool.query(`
    SELECT
      r.*,
      u.name AS ao_name,
      u.email AS ao_email
    FROM ao_max_conversation_reports r
    LEFT JOIN users u ON u.id = r.ao_owner_id
    ${where}
    LIMIT 1
  `, params);

  return rows[0] || null;
}

module.exports = {
  createConversationSession,
  getActiveConversationSession,
  handleConversationTurn,
  startNewConversation,
  appendConversationEvent,
  reportConversation,
  listConversationReports,
  getConversationReport,
};
