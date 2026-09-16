'use strict';

const {
  deriveOperationalState,
  buildRelationshipIntel,
  OUTCOME_LABELS,
} = require('./aoOperationalState');
const { safeGuidance } = require('./aoMessageTemplates');

const TASK_PRIORITY_WEIGHT = Object.freeze({
  warm: 3,
  high: 2,
  normal: 1,
});

const INTEREST_WEIGHT = Object.freeze({
  high: 3,
  medium: 2,
  low: 1,
});

const STATE_ACTION_PRIORITY = Object.freeze({
  walkthrough_requested: 1,
  jake_action_needed: 2,
  decision_maker_reached: 3,
  follow_up_needed: 4,
  contact_identified: 5,
  gatekeeper_reached: 6,
  visited: 7,
  no_contact: 8,
  not_started: 9,
  disqualified: 99,
  converted_to_crm: 99,
});

function todayISO(date = new Date()) {
  return date.toISOString().slice(0, 10);
}

function formatStatusLabel(state, row) {
  const interest = String(row.interest_level || 'medium');
  const parts = [OUTCOME_LABELS[state] || state.replace(/_/g, ' ')];
  parts.push(`${interest} interest`);
  if (row.attribution_source === 'direct_mail_campaign') {
    parts.push(row.campaign_name ? `direct mail · ${row.campaign_name}` : 'direct mail');
  }
  if (row.waiting_on_jake) parts.push('waiting on Jake');
  return parts.join(' · ');
}

function buildNextAction(row, state) {
  if (row.suggested_message) {
    return `Use your suggested follow-up: "${row.suggested_message}"`;
  }

  const nextAction = String(row.next_action || row.open_next_action || '').trim();
  if (/phone_follow_up/i.test(nextAction)) {
    const phone = row.contact_phone ? ` at ${row.contact_phone}` : '';
    return `Call${phone} and log the outcome with Max.`;
  }
  if (/walkthrough|tour/i.test(nextAction) || state === 'walkthrough_requested') {
    return 'Confirm walkthrough interest and capture best contact/time for Jake.';
  }
  if (/in_person_revisit|revisit/i.test(nextAction)) {
    return 'Stop by in person and log the visit with Max.';
  }
  if (state === 'not_started') {
    return 'Make first contact — use your direct mail opening and log the visit.';
  }
  if (state === 'gatekeeper_reached' || state === 'decision_maker_absent') {
    return 'Ask for the cleaning decision-maker and best time to reach them.';
  }
  if (state === 'decision_maker_reached') {
    return 'Follow up on your last conversation and ask for a quick walkthrough if they seem open.';
  }
  if (nextAction) {
    return nextAction.charAt(0).toUpperCase() + nextAction.slice(1);
  }
  return 'Log the visit or follow-up outcome with Max.';
}

function buildWhyNow(row, state, today) {
  const dueDate = row.due_date || row.open_task_due || null;
  if (dueDate && dueDate < today) {
    return `Follow-up is overdue (due ${dueDate}).`;
  }
  if (dueDate && dueDate === today) {
    return 'Follow-up is due today.';
  }
  if (state === 'walkthrough_requested') {
    return 'Walkthrough opportunity needs your follow-up.';
  }
  if (row.priority === 'warm') {
    return 'Warm direct-mail lead with active follow-up.';
  }
  if (String(row.interest_level || '').toLowerCase() === 'high') {
    return 'High-interest conversation needs your next touch.';
  }
  if (row.last_interaction_summary) {
    return `Recent activity: ${row.last_interaction_summary}`;
  }
  if (state === 'not_started') {
    return 'Assigned account with no visit logged yet.';
  }
  if (dueDate) {
    return `Next follow-up due ${dueDate}.`;
  }
  return 'Assigned account in your queue.';
}

function computeRankScore(row, state, today) {
  let score = 0;
  const dueDate = row.due_date || row.open_task_due || null;

  if (dueDate && dueDate < today) score += 10000;
  else if (dueDate && dueDate === today) score += 5000;

  const stateRank = STATE_ACTION_PRIORITY[state] ?? 50;
  score += (100 - stateRank) * 10;

  score += (TASK_PRIORITY_WEIGHT[row.priority] || 1) * 100;
  score += (INTEREST_WEIGHT[String(row.interest_level || 'medium').toLowerCase()] || 1) * 20;

  if (row.waiting_on_jake) score -= 500;

  return score;
}

function mapAccountRow(row, today) {
  const state = deriveOperationalState({
    status: row.lead_status || row.status,
    interest_level: row.interest_level,
    original_visit_note: row.original_visit_note,
    probe_answers: row.probe_answers,
    contact_name: row.contact_name,
    is_decision_maker: row.is_decision_maker,
    waiting_on_jake: row.waiting_on_jake,
    open_escalation_id: row.open_escalation_id,
    open_escalation_status: row.open_escalation_status,
    open_task_status: row.task_status || row.open_task_status,
    attribution_source: row.attribution_source,
    open_next_action: row.next_action || row.open_next_action,
    last_interaction_summary: row.last_interaction_summary,
  });

  return {
    lead_id: row.lead_id || row.id,
    task_id: row.task_id,
    business_name: row.business_name,
    address: row.address,
    due_date: row.due_date || row.open_task_due || null,
    priority: row.priority,
    interest_level: row.interest_level,
    next_action: row.next_action || row.open_next_action,
    suggested_message: row.suggested_message,
    waiting_on_jake: row.waiting_on_jake,
    operational_state: state,
    status_label: formatStatusLabel(state, row),
    why_now: buildWhyNow(row, state, today),
    next_step: buildNextAction(row, state),
    rank_score: computeRankScore(row, state, today),
    ao_owner_id: row.ao_owner_id,
    client_id: row.client_id,
  };
}

function formatPrioritizationResponse(accounts, { hasOverdue = false } = {}) {
  if (!accounts.length) {
    return [
      "You don't currently have any assigned accounts in your queue.",
      '',
      'Check with Jake if you should receive new direct-mail targets, or use Log Visit to add a business you stopped by.',
    ].join('\n');
  }

  const lines = [];
  if (!hasOverdue) {
    lines.push('No overdue follow-ups — here are your highest-priority assigned accounts:');
    lines.push('');
  } else {
    lines.push('Here are your top accounts to work today:');
    lines.push('');
  }

  accounts.forEach((account, index) => {
    lines.push(`${index + 1}. ${account.business_name}`);
    lines.push(`   Why: ${account.why_now}`);
    lines.push(`   Status: ${account.status_label}`);
    lines.push(`   Next: ${account.next_step}`);
    lines.push('');
  });

  const first = accounts[0];
  lines.push(`Start with ${first.business_name} because ${first.why_now.replace(/\.$/, '').toLowerCase()}.`);

  return lines.join('\n');
}

function formatAccountBriefing(lead) {
  const state = deriveOperationalState({
    status: lead.status,
    interest_level: lead.interest_level,
    original_visit_note: lead.original_visit_note,
    probe_answers: lead.probe_answers,
    contact_name: lead.contact_name,
    is_decision_maker: lead.is_decision_maker,
    waiting_on_jake: lead.waiting_on_jake,
    open_escalation_id: lead.open_escalation_id,
    open_escalation_status: lead.open_escalation_status,
    open_task_status: lead.open_task_status,
    attribution_source: lead.attribution_source,
    open_next_action: lead.open_next_action,
    last_interaction_summary: lead.last_interaction_summary,
  });
  const intel = buildRelationshipIntel({
    ...lead,
    open_next_action: lead.open_next_action,
  });

  const lines = [`Briefing — ${lead.business_name}`, ''];

  if (lead.address) lines.push(`Address: ${lead.address}`);
  lines.push(`Status: ${formatStatusLabel(state, lead)}`);

  if (intel.contact_name) {
    const role = intel.contact_role === 'decision_maker' ? 'decision-maker' : intel.contact_role;
    lines.push(`Contact: ${intel.contact_name}${intel.contact_title ? ` (${intel.contact_title})` : ''} · ${role}`);
  } else {
    lines.push('Contact: No contact captured yet.');
  }

  if (intel.decision_maker_name && intel.decision_maker_name !== intel.contact_name) {
    lines.push(`Decision-maker to ask for: ${intel.decision_maker_name}`);
  }

  const lastNote = intel.latest_ao_note;
  if (lastNote) {
    lines.push('');
    lines.push(`Last interaction: ${lastNote}`);
  } else if (lead.original_visit_note) {
    lines.push('');
    lines.push(`Last interaction: ${lead.original_visit_note}`);
  }

  if (intel.current_pain) lines.push(`Pain point: ${intel.current_pain}`);
  if (intel.current_vendor) lines.push(`Current vendor: ${intel.current_vendor}`);

  if (lead.open_task_due) {
    lines.push(`Follow-up due: ${lead.open_task_due}`);
  }

  lines.push('');
  lines.push(`Next: ${buildNextAction({
    next_action: lead.open_next_action,
    suggested_message: lead.suggested_message,
    contact_phone: lead.contact_phone,
  }, state)}`);

  return lines.join('\n');
}

function buildCoachingReply(message) {
  const guidance = safeGuidance(message);
  return {
    intent: 'coaching',
    reply: guidance.guidance,
    escalate: guidance.escalate,
    escalation_reason: guidance.reason || null,
  };
}

module.exports = {
  todayISO,
  mapAccountRow,
  computeRankScore,
  formatPrioritizationResponse,
  formatAccountBriefing,
  buildCoachingReply,
};
