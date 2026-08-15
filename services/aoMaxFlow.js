const pool = require('../db');
const { safeGuidance, shouldEscalate } = require('../utils/aoMessageTemplates');
const {
  createVisitRecord,
  notifyJakeEscalation,
  depositEscalationAction,
} = require('./aoFieldService');

const LOG_VISIT_STEPS = [
  { key: 'business_name', question: 'What business did you visit?' },
  { key: 'address', question: 'Where is it? (address or cross streets)' },
  { key: 'business_type', question: 'What type of business is it?' },
  { key: 'contact_name', question: 'Who did you talk to?' },
  { key: 'contact_title', question: "What's their role?" },
  { key: 'contact_phone', question: 'Did you get a phone number? (or say "none")' },
  { key: 'contact_email', question: 'Did you get an email? (or say "none")' },
  { key: 'visit_note', question: 'What happened on the visit?' },
  { key: 'interest_level', question: 'How interested did they seem? (low / medium / high)' },
  { key: 'next_action', question: 'What should happen next?' },
];

const DEBRIEF_STEPS = [
  { key: 'visits_count', question: 'How many businesses did you visit today?' },
  { key: 'strong_opportunities', question: 'Any strong opportunities worth flagging?' },
  { key: 'walkthrough_requests', question: 'Any walkthrough requests?' },
  { key: 'need_help', question: 'Anything you need help with?' },
  { key: 'revisit_places', question: 'Any places to revisit?' },
];

function normalizeNone(value) {
  const v = String(value || '').trim();
  if (!v || /^none$|^n\/a$|^no$/i.test(v)) return null;
  return v;
}

function parseInterest(value) {
  const v = String(value || '').trim().toLowerCase();
  if (v.startsWith('h')) return 'high';
  if (v.startsWith('l')) return 'low';
  return 'medium';
}

function inferDueDate(nextAction) {
  const text = String(nextAction || '').toLowerCase();
  const date = new Date();
  if (/tomorrow/.test(text)) {
    date.setDate(date.getDate() + 1);
  } else if (/next week/.test(text)) {
    date.setDate(date.getDate() + 7);
  } else if (/2 day|two day/.test(text)) {
    date.setDate(date.getDate() + 2);
  } else {
    date.setDate(date.getDate() + 1);
  }
  return date.toISOString().slice(0, 10);
}

function inferStatus(payload) {
  if (/walkthrough/.test(String(payload.next_action || '').toLowerCase())) return 'walkthrough_requested';
  if (/manager|owner|decision|not there|absent/.test(String(payload.visit_note || '').toLowerCase())) {
    return 'decision_maker_absent';
  }
  return 'needs_follow_up';
}

function detectEscalation(payload) {
  const blob = [
    payload.visit_note,
    payload.next_action,
    payload.interest_level === 'high' ? 'high interest' : '',
  ].filter(Boolean).join(' ');
  const guidance = safeGuidance(blob);
  if (guidance.escalate) return guidance;
  if (payload.interest_level === 'high') {
    return { escalate: true, reason: 'high_interest', guidance: guidance.guidance };
  }
  return { escalate: false, guidance: guidance.guidance };
}

async function createSession({ aoOwnerId, clientId, mode }) {
  const { rows } = await pool.query(`
    INSERT INTO ao_max_sessions (ao_owner_id, client_id, mode, step_index, payload)
    VALUES ($1, $2, $3, 0, '{}'::jsonb)
    RETURNING *
  `, [aoOwnerId, clientId, mode]);
  return rows[0];
}

async function getSession(sessionId, aoOwnerId) {
  const { rows } = await pool.query(`
    SELECT * FROM ao_max_sessions
    WHERE id = $1 AND ao_owner_id = $2 AND completed = false
    LIMIT 1
  `, [sessionId, aoOwnerId]);
  return rows[0] || null;
}

function getSteps(mode) {
  if (mode === 'log_visit') return LOG_VISIT_STEPS;
  if (mode === 'daily_debrief') return DEBRIEF_STEPS;
  if (mode === 'ask_for_help') return [{ key: 'question', question: 'What do you need help with?' }];
  if (mode === 'book_walkthrough') {
    return [
      { key: 'business_name', question: 'Which business wants a walkthrough?' },
      { key: 'address', question: 'Where is it?' },
      { key: 'contact_name', question: 'Best contact name?' },
      { key: 'contact_phone', question: 'Best phone number?' },
      { key: 'notes', question: 'Any timing preferences or notes for Jake?' },
    ];
  }
  return [];
}

async function startMode({ aoOwnerId, clientId, mode, aoName }) {
  if (mode === 'follow_up') {
    return {
      completed: false,
      reply: 'Open your follow-up queue below — tap any task to mark done, reschedule, or escalate to Jake.',
      mode,
      show_queue: true,
    };
  }

  const session = await createSession({ aoOwnerId, clientId, mode });
  const steps = getSteps(mode);
  const greeting = mode === 'log_visit'
    ? `Hey ${aoName || 'there'} — let's log that visit.`
    : mode === 'daily_debrief'
      ? 'End-of-day debrief time.'
      : mode === 'book_walkthrough'
        ? 'Walkthrough handoff — Jake will take it from here.'
        : 'How can I help?';

  return {
    session_id: session.id,
    mode,
    step: 0,
    total_steps: steps.length,
    reply: `${greeting}\n\n${steps[0].question}`,
    completed: false,
  };
}

async function respondToSession({ sessionId, aoOwnerId, clientId, aoName, message }) {
  const session = await getSession(sessionId, aoOwnerId);
  if (!session) return { error: 'Session not found or already completed', status: 404 };

  const steps = getSteps(session.mode);
  const step = steps[session.step_index];
  if (!step) return { error: 'Invalid session step', status: 400 };

  const payload = { ...(session.payload || {}), [step.key]: message };
  const nextIndex = session.step_index + 1;

  if (session.mode === 'ask_for_help') {
    const guidance = safeGuidance(message);
    await pool.query(`
      UPDATE ao_max_sessions SET completed = true, payload = $2, updated_at = NOW()
      WHERE id = $1
    `, [sessionId, payload]);
    return {
      session_id: sessionId,
      mode: session.mode,
      completed: true,
      reply: guidance.guidance,
      escalate: guidance.escalate,
      escalation_reason: guidance.reason || null,
    };
  }

  if (nextIndex < steps.length) {
    await pool.query(`
      UPDATE ao_max_sessions SET step_index = $2, payload = $3, updated_at = NOW()
      WHERE id = $1
    `, [sessionId, nextIndex, payload]);
    return {
      session_id: sessionId,
      mode: session.mode,
      step: nextIndex,
      total_steps: steps.length,
      reply: steps[nextIndex].question,
      completed: false,
    };
  }

  await pool.query(`
    UPDATE ao_max_sessions SET step_index = $2, payload = $3, completed = true, updated_at = NOW()
    WHERE id = $1
  `, [sessionId, nextIndex, payload]);

  if (session.mode === 'log_visit' || session.mode === 'book_walkthrough') {
    const interestLevel = parseInterest(payload.interest_level || 'high');
    const escalationInfo = session.mode === 'book_walkthrough'
      ? { escalate: true, reason: 'walkthrough_request', guidance: 'Jake will coordinate the walkthrough.' }
      : detectEscalation({ ...payload, interest_level: interestLevel });

    const result = await createVisitRecord({
      clientId,
      aoOwnerId,
      aoName,
      businessName: payload.business_name,
      address: payload.address,
      businessType: payload.business_type || null,
      contactName: payload.contact_name,
      contactTitle: payload.contact_title,
      phone: normalizeNone(payload.contact_phone),
      email: normalizeNone(payload.contact_email),
      isDecisionMaker: !/assistant|reception|front desk/i.test(String(payload.contact_title || '')),
      visitNote: payload.visit_note || payload.notes,
      interestLevel,
      nextAction: payload.next_action || (session.mode === 'book_walkthrough' ? 'Book walkthrough' : 'Follow up'),
      dueDate: inferDueDate(payload.next_action || payload.notes),
      status: session.mode === 'book_walkthrough' ? 'walkthrough_requested' : inferStatus(payload),
      escalate: escalationInfo.escalate,
      escalationReason: escalationInfo.reason,
      escalationSummary: [
        payload.visit_note || payload.notes,
        escalationInfo.reason ? `Reason: ${escalationInfo.reason}` : null,
      ].filter(Boolean).join('\n'),
    });

    if (result.escalation) {
      await depositEscalationAction(result.escalation, result.lead, clientId);
      await notifyJakeEscalation(result.escalation, result.lead, aoName).catch(err => {
        console.error('[ao] Jake notification failed:', err.message);
      });
    }

    const lines = [
      `Got it — logged ${result.lead.business_name}.`,
      result.task.suggested_message ? `\nSuggested message:\n"${result.task.suggested_message}"` : null,
      result.escalation ? '\nEscalated to Jake — he\'ll follow up.' : `\n${escalationInfo.guidance}`,
    ].filter(Boolean);

    return {
      session_id: sessionId,
      mode: session.mode,
      completed: true,
      reply: lines.join('\n'),
      lead: result.lead,
      task: result.task,
      escalated: Boolean(result.escalation),
    };
  }

  if (session.mode === 'daily_debrief') {
    const strong = String(payload.strong_opportunities || '').trim();
    const walkthroughs = String(payload.walkthrough_requests || '').trim();
    const revisit = String(payload.revisit_places || '').trim();
    const parts = [
      `Logged your debrief — ${payload.visits_count || 0} visits today.`,
      strong && strong.toLowerCase() !== 'none' ? `Strong opportunities noted: ${strong}` : null,
      walkthroughs && walkthroughs.toLowerCase() !== 'none' ? `Walkthrough requests: ${walkthroughs}` : null,
      revisit && revisit.toLowerCase() !== 'none' ? `Revisit list: ${revisit}` : null,
      payload.need_help && payload.need_help.toLowerCase() !== 'none'
        ? `Help requested: ${payload.need_help}`
        : 'Solid day — keep the momentum.',
    ].filter(Boolean);

    return {
      session_id: sessionId,
      mode: session.mode,
      completed: true,
      reply: parts.join('\n'),
      debrief: payload,
    };
  }

  return { session_id: sessionId, mode: session.mode, completed: true, reply: 'Done.' };
}

module.exports = {
  LOG_VISIT_STEPS,
  startMode,
  respondToSession,
};
