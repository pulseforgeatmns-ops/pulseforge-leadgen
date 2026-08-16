const pool = require('../db');
const {
  safeGuidance,
  buildCompletionReply,
  buildDirectMailOpening,
  parseContactRole,
  normalizeNextAction,
  parseInterestLevel,
  resolveNextActionOwner,
  formatProbeAnswers,
  resolveCompletionType,
  formatDecisionMakerStatus,
} = require('../utils/aoMessageTemplates');
const {
  initProbeState,
  currentProbe,
  startProbeModeIfNeeded,
  advanceAfterBaseStep,
  processVisitNoteAnswer,
} = require('../utils/aoVisitFlow');
const { DIRECT_MAIL_FOLLOW_UP_STEPS } = require('../utils/aoDirectMailFlow');
const {
  createVisitRecord,
  completeDirectMailFollowUp,
  getTaskForFollowUp,
  notifyJakeEscalation,
  depositEscalationAction,
} = require('./aoFieldService');
const { advanceRouteAfterVisit } = require('./aoRouteService');

const LOG_VISIT_STEPS = [
  { key: 'business_name', question: 'What business did you visit?' },
  { key: 'address', question: 'Where is it? (address or cross streets)' },
  { key: 'business_type', question: 'What type of business is it?' },
  { key: 'contact_name', question: 'Who did you talk to?' },
  { key: 'contact_title', question: "What's their role?" },
  { key: 'contact_role', question: 'Are they the decision-maker, a gatekeeper, or unknown?' },
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

function inferStatus(payload, nextActionOwner) {
  if (nextActionOwner === 'not_a_fit') return 'not_a_fit';
  if (nextActionOwner === 'walkthrough') return 'walkthrough_requested';
  if (/manager|owner|decision|not there|absent/.test(String(payload.visit_note || '').toLowerCase())) {
    return 'decision_maker_absent';
  }
  if (nextActionOwner === 'none') return 'new_visit';
  return 'needs_follow_up';
}

function detectEscalation(payload, nextActionOwner) {
  const contactRole = parseContactRole(payload.contact_role);
  const blob = [
    payload.visit_note,
    payload.next_action,
    payload.interest_level === 'high' ? 'high interest' : '',
  ].filter(Boolean).join(' ');
  const guidance = safeGuidance(blob);

  if (nextActionOwner === 'jake' || nextActionOwner === 'walkthrough') {
    return {
      escalate: true,
      reason: nextActionOwner === 'walkthrough' ? 'walkthrough_request' : (guidance.reason || 'high_interest'),
      guidance: guidance.guidance || 'Jake will follow up.',
      contactRole,
    };
  }
  if (guidance.escalate) return { ...guidance, contactRole };
  if (payload.interest_level === 'high' && contactRole === 'decision_maker') {
    return { escalate: true, reason: 'high_interest', guidance: 'Strong lead — Jake should follow up.', contactRole };
  }
  return { escalate: false, guidance: guidance.guidance, contactRole };
}

async function createSession({ aoOwnerId, clientId, mode, initialPayload = {} }) {
  const { rows } = await pool.query(`
    INSERT INTO ao_max_sessions (ao_owner_id, client_id, mode, step_index, payload)
    VALUES ($1, $2, $3, 0, $4::jsonb)
    RETURNING *
  `, [aoOwnerId, clientId, mode, JSON.stringify(initialPayload)]);
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
  if (mode === 'direct_mail_follow_up') return DIRECT_MAIL_FOLLOW_UP_STEPS;
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

async function startMode({ aoOwnerId, clientId, mode, aoName, taskId }) {
  if (mode === 'follow_up') {
    return {
      completed: false,
      reply: 'Open your follow-up queue below — tap any direct mail task to log the visit with Max.',
      mode,
      show_queue: true,
    };
  }

  if (mode === 'direct_mail_follow_up') {
    if (!taskId) {
      return { error: 'task_id required for direct mail follow-up', status: 400 };
    }
    const task = await getTaskForFollowUp(taskId, aoOwnerId);
    if (!task) return { error: 'Follow-up task not found', status: 404 };
    if (task.attribution_source !== 'direct_mail_campaign') {
      return { error: 'This task is not a direct mail follow-up', status: 400 };
    }

    const session = await createSession({
      aoOwnerId,
      clientId,
      mode,
      initialPayload: {
        task_id: taskId,
        lead_id: task.lead_id,
        business_name: task.business_name,
        campaign_name: task.campaign_name,
      },
    });
    const opening = buildDirectMailOpening(aoName);
    const steps = getSteps(mode);
    return {
      session_id: session.id,
      mode,
      task_id: taskId,
      business_name: task.business_name,
      step: 0,
      total_steps: steps.length,
      reply: [
        `Direct mail follow-up — ${task.business_name}${task.campaign_name ? ` (${task.campaign_name})` : ''}.`,
        '',
        `Opening to use in person:`,
        `"${opening}"`,
        '',
        steps[0].question,
      ].join('\n'),
      completed: false,
    };
  }

  const session = await createSession({ aoOwnerId, clientId, mode });
  const steps = getSteps(mode);
  const greeting = mode === 'log_visit'
    ? 'Let\'s log that visit.'
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

async function persistSessionProgress(sessionId, { stepIndex, payload }) {
  await pool.query(`
    UPDATE ao_max_sessions SET step_index = $2, payload = $3, updated_at = NOW()
    WHERE id = $1
  `, [sessionId, stepIndex, payload]);
}

async function completeSession(sessionId, { stepIndex, payload }) {
  await pool.query(`
    UPDATE ao_max_sessions SET step_index = $2, payload = $3, completed = true, updated_at = NOW()
    WHERE id = $1
  `, [sessionId, stepIndex, payload]);
}

function extractPreferredTiming(payload) {
  const answers = payload.probe_answers || {};
  return answers.probe_walkthrough_timing
    || (String(payload.notes || '').trim() || null);
}

async function finalizeVisitSession({
  session,
  payload,
  sessionId,
  aoOwnerId,
  clientId,
  aoName,
}) {
  const interestLevel = parseInterestLevel(payload.interest_level || 'high');
  const normalizedNextAction = normalizeNextAction(
    payload.next_action || (session.mode === 'book_walkthrough' ? 'Book walkthrough' : 'Follow up'),
  );
  const nextActionOwner = session.mode === 'book_walkthrough'
    ? 'walkthrough'
    : resolveNextActionOwner(normalizedNextAction, payload);

  const escalationInfo = session.mode === 'book_walkthrough'
    ? { escalate: true, reason: 'walkthrough_request', guidance: 'Jake will coordinate the walkthrough.' }
    : detectEscalation({ ...payload, interest_level: interestLevel, next_action: normalizedNextAction }, nextActionOwner);

  const contactRole = parseContactRole(payload.contact_role);
  const status = session.mode === 'book_walkthrough'
    ? 'walkthrough_requested'
    : inferStatus(payload, nextActionOwner);
  const probeAnswers = payload.probe_answers || {};
  const probeSummary = formatProbeAnswers(probeAnswers);
  const escalationSummary = [
    payload.visit_note || payload.notes,
    probeSummary || null,
    escalationInfo.reason ? `Reason: ${escalationInfo.reason}` : null,
    payload.next_action ? `Next step: ${normalizedNextAction}` : null,
  ].filter(Boolean).join('\n');

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
    contactRole,
    isDecisionMaker: contactRole === 'decision_maker',
    visitNote: payload.visit_note || payload.notes,
    interestLevel,
    nextAction: normalizedNextAction,
    nextActionOwner,
    dueDate: inferDueDate(normalizedNextAction || payload.notes),
    status,
    escalate: escalationInfo.escalate,
    escalationReason: escalationInfo.reason,
    escalationSummary,
    probeAnswers,
    skipTask: nextActionOwner === 'none' || nextActionOwner === 'not_a_fit',
  });

  if (result.escalation) {
    await depositEscalationAction(result.escalation, result.lead, clientId, {
      probeAnswers,
      suggestedMessage: result.task?.suggested_message,
      nextAction: normalizedNextAction,
      interestLevel,
      aoName,
      contactRole,
      decisionMakerStatus: formatDecisionMakerStatus({
        contactRole,
        contactTitle: payload.contact_title,
      }),
    });
    await notifyJakeEscalation(result.escalation, result.lead, aoName).catch(err => {
      console.error('[ao] Jake notification failed:', err.message);
    });
  }

  const completionType = resolveCompletionType({
    nextActionOwner,
    escalated: Boolean(result.escalation),
    status,
  });

  const reply = buildCompletionReply({
    businessName: result.lead.business_name,
    completionType,
    suggestedMessage: result.task?.suggested_message,
    preferredTiming: extractPreferredTiming(payload),
    escalated: Boolean(result.escalation),
  });

  let routeAdvance = null;
  if (payload.task_id) {
    routeAdvance = await advanceRouteAfterVisit({
      aoOwnerId,
      taskId: payload.task_id,
      aoName,
    }).catch(err => {
      console.error('[ao] route advance failed:', err.message);
      return null;
    });
  }

  return {
    session_id: sessionId,
    mode: session.mode,
    completed: true,
    reply: routeAdvance?.next_stop_debrief ? reply + routeAdvance.next_stop_debrief : reply,
    lead: result.lead,
    task: result.task,
    escalated: Boolean(result.escalation),
    next_action_owner: nextActionOwner,
    route: routeAdvance?.route || null,
    next_stop: routeAdvance?.next_stop || null,
  };
}

async function finalizeDirectMailSession({
  session,
  payload,
  sessionId,
  aoOwnerId,
  clientId,
  aoName,
}) {
  const nextAction = normalizeNextAction(payload.next_step || payload.visit_note || 'Follow up');
  const nextActionOwner = resolveNextActionOwner(nextAction, payload);
  const interestLevel = parseInterestLevel(
    /yes|interested|walkthrough/i.test(String(payload.walkthrough_interest || '')) ? 'high'
      : /not a fit|no fit|hard no/i.test(String(payload.next_step || '')) ? 'low'
        : 'medium',
  );

  const result = await completeDirectMailFollowUp(payload.task_id, aoOwnerId, {
    clientId,
    aoName,
    answers: payload,
    visitNote: payload.visit_note,
    nextAction,
    interestLevel,
    contactName: payload.cleaning_decision_maker,
    contactRole: parseContactRole(payload.reached_decision_maker),
  });

  if (!result) return { error: 'Follow-up task not found', status: 404 };

  if (result.escalation) {
    await depositEscalationAction(result.escalation, result.lead, clientId, {
      probeAnswers: payload,
      nextAction,
      interestLevel,
      aoName,
      contactRole: parseContactRole(payload.reached_decision_maker),
      decisionMakerStatus: formatDecisionMakerStatus({
        contactRole: parseContactRole(payload.reached_decision_maker),
      }),
    });
    await notifyJakeEscalation(result.escalation, result.lead, aoName).catch(err => {
      console.error('[ao] Jake notification failed:', err.message);
    });
  }

  const completionType = resolveCompletionType({
    nextActionOwner,
    escalated: Boolean(result.escalation),
    status: result.lead.status,
  });

  const reply = buildCompletionReply({
    businessName: result.lead.business_name,
    completionType,
    preferredTiming: payload.walkthrough_interest,
    escalated: Boolean(result.escalation),
  });

  let routeAdvance = null;
  if (payload.task_id) {
    routeAdvance = await advanceRouteAfterVisit({
      aoOwnerId,
      taskId: payload.task_id,
      aoName,
    }).catch(err => {
      console.error('[ao] route advance failed:', err.message);
      return null;
    });
  }

  return {
    session_id: sessionId,
    mode: session.mode,
    completed: true,
    reply: routeAdvance?.next_stop_debrief ? reply + routeAdvance.next_stop_debrief : reply,
    lead: result.lead,
    task: result.task,
    escalated: Boolean(result.escalation),
    route: routeAdvance?.route || null,
    next_stop: routeAdvance?.next_stop || null,
  };
}

async function respondToSession({ sessionId, aoOwnerId, clientId, aoName, message }) {
  const session = await getSession(sessionId, aoOwnerId);
  if (!session) return { error: 'Session not found or already completed', status: 404 };

  const steps = getSteps(session.mode);
  let payload = { ...(session.payload || {}) };
  let stepIndex = session.step_index;
  let probeState = initProbeState(payload);

  if (session.mode === 'ask_for_help') {
    const guidance = safeGuidance(message);
    await completeSession(sessionId, { stepIndex: stepIndex + 1, payload: { ...payload, question: message } });
    return {
      session_id: sessionId,
      mode: session.mode,
      completed: true,
      reply: guidance.guidance,
      escalate: guidance.escalate,
      escalation_reason: guidance.reason || null,
    };
  }

  if (session.mode === 'log_visit' && probeState.in_probe_mode) {
    const probe = currentProbe(probeState);
    if (probe) {
      probeState.probe_answers = {
        ...probeState.probe_answers,
        [probe.key]: message,
      };
      probeState.probe_index += 1;
      payload = { ...payload, ...probeState };

      const nextProbe = startProbeModeIfNeeded(probeState);
      payload = { ...payload, ...probeState };

      if (nextProbe) {
        await persistSessionProgress(sessionId, { stepIndex, payload });
        return {
          session_id: sessionId,
          mode: session.mode,
          step: stepIndex,
          total_steps: steps.length,
          reply: nextProbe.question,
          completed: false,
          probing: true,
        };
      }

      payload.in_probe_mode = false;
      if (stepIndex < steps.length) {
        await persistSessionProgress(sessionId, { stepIndex, payload });
        return {
          session_id: sessionId,
          mode: session.mode,
          step: stepIndex,
          total_steps: steps.length,
          reply: steps[stepIndex].question,
          completed: false,
        };
      }

      await completeSession(sessionId, { stepIndex, payload });
      return finalizeVisitSession({ session, payload, sessionId, aoOwnerId, clientId, aoName });
    }
  }

  const step = steps[stepIndex];
  if (!step) return { error: 'Invalid session step', status: 400 };

  let completedStepKey = step.key;

  if (session.mode === 'log_visit' && step.key === 'visit_note') {
    const visitNoteResult = processVisitNoteAnswer(payload, message);
    payload = visitNoteResult.payload;

    if (!visitNoteResult.completed) {
      await persistSessionProgress(sessionId, { stepIndex, payload });
      return {
        session_id: sessionId,
        mode: session.mode,
        step: stepIndex,
        total_steps: steps.length,
        reply: visitNoteResult.clarifyQuestion,
        completed: false,
        clarifying: true,
      };
    }

    stepIndex += 1;
  } else {
    payload = { ...payload, [step.key]: message };
    stepIndex += 1;
  }

  if (session.mode === 'log_visit' && stepIndex <= steps.length) {
    const { state, nextProbe } = advanceAfterBaseStep(payload, completedStepKey);
    payload = { ...payload, ...state };

    if (nextProbe) {
      await persistSessionProgress(sessionId, { stepIndex, payload });
      return {
        session_id: sessionId,
        mode: session.mode,
        step: stepIndex,
        total_steps: steps.length,
        reply: nextProbe.question,
        completed: false,
        probing: true,
      };
    }
  }

  if (stepIndex < steps.length) {
    await persistSessionProgress(sessionId, { stepIndex, payload });
    return {
      session_id: sessionId,
      mode: session.mode,
      step: stepIndex,
      total_steps: steps.length,
      reply: steps[stepIndex].question,
      completed: false,
    };
  }

  if (session.mode === 'log_visit') {
    const { state, nextProbe } = advanceAfterBaseStep(payload, completedStepKey);
    payload = { ...payload, ...state };
    if (nextProbe) {
      await persistSessionProgress(sessionId, { stepIndex, payload });
      return {
        session_id: sessionId,
        mode: session.mode,
        step: stepIndex,
        total_steps: steps.length,
        reply: nextProbe.question,
        completed: false,
        probing: true,
      };
    }
  }

  await completeSession(sessionId, { stepIndex, payload });

  if (session.mode === 'log_visit' || session.mode === 'book_walkthrough') {
    return finalizeVisitSession({ session, payload, sessionId, aoOwnerId, clientId, aoName });
  }

  if (session.mode === 'direct_mail_follow_up') {
    return finalizeDirectMailSession({ session, payload, sessionId, aoOwnerId, clientId, aoName });
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
  DIRECT_MAIL_FOLLOW_UP_STEPS,
  startMode,
  respondToSession,
  detectEscalation,
  finalizeDirectMailSession,
};
