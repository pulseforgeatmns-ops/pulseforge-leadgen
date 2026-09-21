'use strict';

const { deriveOperationalState } = require('./aoOperationalState');
const { observedInterestRankWeight } = require('./aoInterestLevel');
const { safeGuidance } = require('./aoMessageTemplates');
const { normalizeDueDate } = require('./aoQueueFormat');
const {
  todayISOInZone,
  DEFAULT_AO_TIMEZONE,
  buildAoAccountView,
  formatAccountBriefing,
  formatPrioritizationResponse,
} = require('./aoAccountBriefing');

const NO_DUE_DATE_SORT_KEY = '9999-12-31';

const TASK_PRIORITY_WEIGHT = Object.freeze({
  warm: 3,
  high: 2,
  normal: 1,
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

function todayISO(date = new Date(), timeZone = DEFAULT_AO_TIMEZONE) {
  return todayISOInZone(date, timeZone);
}

function dueDateSortKey(value) {
  return normalizeDueDate(value) || NO_DUE_DATE_SORT_KEY;
}

function comparePrioritizedAccounts(a, b) {
  if (b.rank_score !== a.rank_score) return b.rank_score - a.rank_score;
  const dueA = dueDateSortKey(a.due_date);
  const dueB = dueDateSortKey(b.due_date);
  if (dueA !== dueB) return dueA.localeCompare(dueB);
  return String(a.business_name || '').localeCompare(String(b.business_name || ''));
}

function computeRankScore(row, state, today) {
  let score = 0;
  const dueDate = normalizeDueDate(row.due_date || row.open_task_due || null);

  if (dueDate && dueDate < today) score += 10000;
  else if (dueDate && dueDate === today) score += 5000;

  const stateRank = STATE_ACTION_PRIORITY[state] ?? 50;
  score += (100 - stateRank) * 10;

  score += (TASK_PRIORITY_WEIGHT[row.priority] || 1) * 100;
  score += observedInterestRankWeight(row.interest_level) * 20;

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
  const view = buildAoAccountView(row, { today });

  return {
    lead_id: row.lead_id || row.id,
    task_id: row.task_id,
    business_name: row.business_name,
    address: row.address,
    due_date: normalizeDueDate(row.due_date || row.open_task_due || null),
    priority: row.priority,
    interest_level: row.interest_level,
    next_action: row.next_action || row.open_next_action,
    suggested_message: row.suggested_message,
    waiting_on_jake: row.waiting_on_jake,
    operational_state: state,
    status_label: view.status_line,
    why_now: view.why_it_matters,
    next_step: view.next_action,
    rank_score: computeRankScore(row, state, today),
    ao_owner_id: row.ao_owner_id,
    client_id: row.client_id,
  };
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
  dueDateSortKey,
  comparePrioritizedAccounts,
  mapAccountRow,
  computeRankScore,
  formatPrioritizationResponse,
  formatAccountBriefing,
  buildCoachingReply,
};
