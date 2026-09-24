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
const { isActiveConversationStatus } = require('../utils/aoRoutingIssueTypes');
const { logAoAuditEvent } = require('../utils/aoAuditEvents');
const { requestProspectBrief } = require('./aoProspectBriefService');

function conversationPreview(payload = {}) {
  const messages = payload.messages || [];
  const last = messages[messages.length - 1];
  if (!last) return '';
  const text = String(last.content || '').trim();
  return text.length > 120 ? `${text.slice(0, 117)}…` : text;
}

function prospectLabel(payload = {}, session = {}) {
  const account = payload.selected_account;
  if (account?.business_name) return account.business_name;
  if (session.prospect_id) return `Prospect ${session.prospect_id}`;
  return 'General conversation';
}

async function createConversationSession({
  aoOwnerId,
  clientId,
  initialPayload = {},
  prospectId = null,
  missionId = null,
}) {
  const payload = {
    messages: [],
    ...initialPayload,
  };
  const { rows } = await pool.query(`
    INSERT INTO ao_max_sessions (
      ao_owner_id, client_id, mode, step_index, payload, status, prospect_id, mission_id
    )
    VALUES ($1, $2, 'conversation', 0, $3::jsonb, 'active', $4, $5)
    RETURNING *
  `, [aoOwnerId, clientId, JSON.stringify(payload), prospectId, missionId]);
  return rows[0];
}

async function getConversationSession(sessionId, aoOwnerId, { clientId = null } = {}) {
  const params = [sessionId, aoOwnerId];
  let where = `
    WHERE id = $1
      AND ao_owner_id = $2
      AND mode = 'conversation'
  `;
  if (clientId != null) {
    params.push(clientId);
    where += ` AND client_id = $${params.length}`;
  }

  const { rows } = await pool.query(`
    SELECT * FROM ao_max_sessions
    ${where}
    LIMIT 1
  `, params);
  return rows[0] || null;
}

async function getActiveConversationSession(sessionId, aoOwnerId, options = {}) {
  const session = await getConversationSession(sessionId, aoOwnerId, options);
  if (!session) return null;
  if (!isActiveConversationStatus(session.status) || session.completed) return null;
  return session;
}

async function persistConversationPayload(sessionId, payload) {
  await pool.query(`
    UPDATE ao_max_sessions
    SET payload = $2::jsonb, updated_at = NOW()
    WHERE id = $1
  `, [sessionId, JSON.stringify(payload)]);
}

async function markConversationDone({ sessionId, aoOwnerId, clientId, closedBy }) {
  const session = await getConversationSession(sessionId, aoOwnerId, { clientId });
  if (!session) return { error: 'Conversation not found', status: 404 };
  if (Number(session.client_id) !== Number(clientId)) {
    return { error: 'Conversation tenant mismatch', status: 403 };
  }

  await pool.query(`
    UPDATE ao_max_sessions
    SET
      completed = true,
      status = 'done',
      closed_at = NOW(),
      closed_by = $2,
      updated_at = NOW()
    WHERE id = $1
  `, [sessionId, String(closedBy)]);

  return {
    ok: true,
    session_id: sessionId,
    status: 'done',
    previous_status: session.status,
  };
}

async function completeConversationSession(sessionId, closedBy = null) {
  await pool.query(`
    UPDATE ao_max_sessions
    SET
      completed = true,
      status = 'done',
      closed_at = COALESCE(closed_at, NOW()),
      closed_by = COALESCE(closed_by, $2),
      updated_at = NOW()
    WHERE id = $1
  `, [sessionId, closedBy != null ? String(closedBy) : null]);
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
    ? await getActiveConversationSession(sessionId, aoOwnerId, { clientId })
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
    const previous = await getActiveConversationSession(previousSessionId, aoOwnerId, { clientId });
    if (previous) {
      await completeConversationSession(previousSessionId, aoOwnerId);
    }
  }

  const session = await createConversationSession({ aoOwnerId, clientId });
  return {
    session_id: session.id,
    mode: 'conversation',
    completed: false,
    status: 'active',
    reply: 'New conversation started. What do you need help with?',
  };
}

async function reopenConversation({ sessionId, aoOwnerId, clientId, reopenedBy }) {
  const session = await getConversationSession(sessionId, aoOwnerId, { clientId });
  if (!session) return { error: 'Conversation not found', status: 404 };
  if (Number(session.client_id) !== Number(clientId)) {
    return { error: 'Conversation tenant mismatch', status: 403 };
  }

  const previousStatus = session.status;
  await pool.query(`
    UPDATE ao_max_sessions
    SET
      completed = false,
      status = 'reopened',
      reopened_at = NOW(),
      reopened_by = $2,
      updated_at = NOW()
    WHERE id = $1
  `, [sessionId, String(reopenedBy)]);

  await logAoAuditEvent({
    event: 'AO_CONVERSATION_REOPENED',
    clientId,
    aoUserId: reopenedBy,
    prospectId: session.prospect_id || null,
    missionId: session.mission_id || null,
    payload: {
      conversation_id: sessionId,
      previous_status: previousStatus,
      reason: 'manual_reopen',
    },
  });

  const payload = session.payload || {};
  return {
    ok: true,
    session_id: sessionId,
    status: 'reopened',
    previous_status: previousStatus,
    messages: payload.messages || [],
    prospect_label: prospectLabel(payload, session),
    reply: 'Conversation reopened. I still have the prior context — what do you need next?',
  };
}

async function listConversations({ aoOwnerId, clientId, status = null, limit = 50 }) {
  const params = [aoOwnerId, clientId];
  let where = `
    WHERE ao_owner_id = $1
      AND client_id = $2
      AND mode = 'conversation'
  `;
  if (status) {
    params.push(status);
    where += ` AND status = $${params.length}`;
  }
  params.push(Math.min(Number(limit) || 50, 200));

  const { rows } = await pool.query(`
    SELECT id, status, prospect_id, mission_id, payload, created_at, updated_at, closed_at, reopened_at
    FROM ao_max_sessions
    ${where}
    ORDER BY updated_at DESC
    LIMIT $${params.length}
  `, params);

  return rows.map(row => ({
    conversation_id: row.id,
    status: row.status,
    prospect_id: row.prospect_id,
    mission_id: row.mission_id,
    prospect_label: prospectLabel(row.payload || {}, row),
    last_message_preview: conversationPreview(row.payload || {}),
    message_count: (row.payload?.messages || []).length,
    created_at: row.created_at,
    updated_at: row.updated_at,
    closed_at: row.closed_at,
    reopened_at: row.reopened_at,
  }));
}

async function getConversationDetail({ sessionId, aoOwnerId, clientId }) {
  const session = await getConversationSession(sessionId, aoOwnerId, { clientId });
  if (!session) return null;

  const payload = session.payload || {};
  return {
    conversation_id: session.id,
    session_id: session.id,
    status: session.status,
    messages: payload.messages || [],
    prospect_id: session.prospect_id,
    mission_id: session.mission_id,
    prospect_label: prospectLabel(payload, session),
    selected_account: payload.selected_account || null,
    created_at: session.created_at,
    updated_at: session.updated_at,
    closed_at: session.closed_at,
    reopened_at: session.reopened_at,
  };
}

async function getActiveOrRestorableConversation({ aoOwnerId, clientId }) {
  const { rows } = await pool.query(`
    SELECT *
    FROM ao_max_sessions
    WHERE ao_owner_id = $1
      AND client_id = $2
      AND mode = 'conversation'
      AND status IN ('active', 'reopened')
      AND completed = false
    ORDER BY updated_at DESC
    LIMIT 1
  `, [aoOwnerId, clientId]);

  const session = rows[0];
  if (!session) return null;

  const payload = session.payload || {};
  return {
    session_id: session.id,
    status: session.status,
    messages: payload.messages || [],
    prospect_id: session.prospect_id,
    mission_id: session.mission_id,
    prospect_label: prospectLabel(payload, session),
    selected_account: payload.selected_account || null,
  };
}

async function handleProspectBriefAction({
  sessionId = null,
  aoOwnerId,
  clientId,
  prospectId = null,
  leadId = null,
  source = 'prospect_card_brief_button',
}) {
  const briefResult = await requestProspectBrief({
    clientId,
    aoOwnerId,
    prospectId,
    leadId,
    source,
  });
  if (briefResult.status) return briefResult;

  let session = sessionId
    ? await getActiveConversationSession(sessionId, aoOwnerId, { clientId })
    : null;

  if (!session) {
    session = await createConversationSession({
      aoOwnerId,
      clientId,
      prospectId: briefResult.prospect_id,
      missionId: briefResult.mission_id,
      initialPayload: {},
    });
  }

  let payload = { ...(session.payload || {}) };
  if (briefResult.lead_id || briefResult.business_name) {
    payload.selected_account = {
      business_name: briefResult.business_name || 'Assigned account',
      lead_id: briefResult.lead_id || null,
      prospect_id: briefResult.prospect_id || null,
    };
  }
  if (briefResult.prospect_id) {
    payload.prospect_id = briefResult.prospect_id;
  }
  if (briefResult.mission_id) {
    payload.mission_id = briefResult.mission_id;
  }

  payload = appendMessage(payload, {
    role: 'user',
    content: 'Give me the AO brief for this prospect.',
    intent: 'prospect_brief',
    meta: {
      action: 'prospect_brief',
      source,
      prospect_id: briefResult.prospect_id,
      lead_id: briefResult.lead_id,
    },
  });
  payload = appendMessage(payload, {
    role: 'max',
    content: briefResult.brief,
    intent: 'prospect_brief',
    meta: {
      prospect_id: briefResult.prospect_id,
      lead_id: briefResult.lead_id,
    },
  });

  await pool.query(`
    UPDATE ao_max_sessions
    SET
      payload = $2::jsonb,
      prospect_id = COALESCE($3, prospect_id),
      mission_id = COALESCE($4, mission_id),
      updated_at = NOW()
    WHERE id = $1
  `, [
    session.id,
    JSON.stringify(payload),
    briefResult.prospect_id || null,
    briefResult.mission_id || null,
  ]);

  return {
    ok: true,
    session_id: session.id,
    mode: 'conversation',
    completed: false,
    status: session.status,
    intent: 'prospect_brief',
    reply: briefResult.brief,
    action: 'prospect_brief',
    prospect_id: briefResult.prospect_id,
    lead_id: briefResult.lead_id,
    mission_id: briefResult.mission_id,
    account: payload.selected_account || null,
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
  const session = await getActiveConversationSession(sessionId, aoOwnerId, {});
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
  const session = await getConversationSession(sessionId, aoOwnerId, { clientId });
  if (!session) {
    return { error: 'Conversation not found', status: 404 };
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
  getConversationSession,
  getActiveConversationSession,
  handleConversationTurn,
  handleProspectBriefAction,
  startNewConversation,
  markConversationDone,
  reopenConversation,
  listConversations,
  getActiveOrRestorableConversation,
  getConversationDetail,
  appendConversationEvent,
  reportConversation,
  listConversationReports,
  getConversationReport,
  conversationPreview,
  prospectLabel,
};
