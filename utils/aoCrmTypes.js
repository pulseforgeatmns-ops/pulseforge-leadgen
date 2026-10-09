'use strict';

const AO_CRM_OUTCOMES = Object.freeze([
  { value: 'no_answer', label: 'No answer', internal: 'called_no_answer' },
  { value: 'left_voicemail', label: 'Left voicemail', internal: 'left_voicemail' },
  { value: 'spoke_gatekeeper', label: 'Spoke with gatekeeper', internal: 'spoke_gatekeeper' },
  { value: 'spoke_decision_maker', label: 'Spoke with decision-maker', internal: 'spoke_decision_maker' },
  { value: 'asked_to_follow_up', label: 'Asked to follow up', internal: 'asked_to_follow_up' },
  { value: 'interested', label: 'Interested', internal: 'interested' },
  { value: 'not_interested', label: 'Not interested', internal: 'not_interested' },
  { value: 'bad_fit', label: 'Bad fit', internal: 'bad_fit' },
  { value: 'booked_walkthrough', label: 'Booked walkthrough', internal: 'booked_walkthrough' },
  { value: 'needs_jake', label: 'Needs Jake', internal: 'needs_jake' },
  { value: 'sent_email', label: 'Sent email', internal: 'sent_email' },
  { value: 'other', label: 'Other', internal: 'other' },
]);

const AO_CRM_STATUSES = Object.freeze([
  'researching', 'ready_to_call', 'call_attempted', 'contacted', 'gatekeeper_reached',
  'decision_maker_reached', 'follow_up_needed', 'warm', 'walkthrough_target',
  'walkthrough_booked', 'proposal_needed', 'proposal_sent', 'won', 'lost',
  'not_a_fit', 'dead', 'application_in_progress',
]);

const AO_CRM_NEXT_ACTIONS = Object.freeze([
  'research_contact', 'call', 'visit', 'email', 'follow_up', 'ask_jake',
  'book_walkthrough', 'send_information', 'prepare_proposal', 'check_back_later',
  'disqualify', 'no_action',
]);

const OUTCOME_TO_STATUS = Object.freeze({
  no_answer: 'call_attempted',
  left_voicemail: 'call_attempted',
  sent_email: 'contacted',
  spoke_gatekeeper: 'gatekeeper_reached',
  spoke_decision_maker: 'decision_maker_reached',
  asked_to_follow_up: 'follow_up_needed',
  interested: 'warm',
  not_interested: 'follow_up_needed',
  bad_fit: 'not_a_fit',
  booked_walkthrough: 'walkthrough_booked',
  needs_jake: 'follow_up_needed',
  other: 'contacted',
});

const OUTCOME_TO_TOUCH_TYPE = Object.freeze({
  no_answer: 'call',
  left_voicemail: 'call',
  sent_email: 'email',
  spoke_gatekeeper: 'call',
  spoke_decision_maker: 'call',
  asked_to_follow_up: 'call',
  interested: 'call',
  not_interested: 'call',
  bad_fit: 'call',
  booked_walkthrough: 'call',
  needs_jake: 'call',
  other: 'note',
});

const CRM_NEXT_TO_LEGACY = Object.freeze({
  research_contact: 'NEEDS_RESEARCH',
  call: 'AO_FOLLOW_UP',
  visit: 'AO_FOLLOW_UP',
  email: 'SEND_INFO',
  follow_up: 'AO_FOLLOW_UP',
  ask_jake: 'JAKE_REVIEW',
  book_walkthrough: 'BOOK_ASSESSMENT',
  send_information: 'SEND_INFO',
  prepare_proposal: 'JAKE_REVIEW',
  check_back_later: 'NURTURE',
  disqualify: 'SUPPRESS',
  no_action: null,
});

const CLOSED_STATUSES = new Set(['won', 'lost', 'not_a_fit', 'dead']);

function resolveInternalOutcome(crmOutcome) {
  const row = AO_CRM_OUTCOMES.find(o => o.value === crmOutcome);
  return row ? row.internal : null;
}

function isValidCrmOutcome(value) {
  return AO_CRM_OUTCOMES.some(o => o.value === value);
}

function isValidCrmStatus(value) {
  return AO_CRM_STATUSES.includes(value);
}

function isValidCrmNextAction(value) {
  return AO_CRM_NEXT_ACTIONS.includes(value);
}

function deriveDefaultStatus(prospect) {
  if (prospect.ao_current_status) return prospect.ao_current_status;
  if (prospect.advisory_stage === 'closed') return 'dead';
  const debrief = prospect.last_debrief_status || prospect.ao_last_outcome;
  if (debrief === 'booked_assessment' || debrief === 'booked_walkthrough') return 'walkthrough_booked';
  if (debrief === 'spoke_decision_maker') return 'decision_maker_reached';
  if (debrief === 'spoke_gatekeeper') return 'gatekeeper_reached';
  if (debrief === 'called_no_answer' || debrief === 'left_voicemail') return 'call_attempted';
  if (prospect.assigned_ao_id && !debrief) return 'ready_to_call';
  return 'researching';
}

function accountIsActive(prospect) {
  const status = deriveDefaultStatus(prospect);
  return !CLOSED_STATUSES.has(status) && !prospect.ao_paused;
}

module.exports = {
  AO_CRM_OUTCOMES,
  AO_CRM_STATUSES,
  AO_CRM_NEXT_ACTIONS,
  OUTCOME_TO_STATUS,
  OUTCOME_TO_TOUCH_TYPE,
  CRM_NEXT_TO_LEGACY,
  CLOSED_STATUSES,
  resolveInternalOutcome,
  isValidCrmOutcome,
  isValidCrmStatus,
  isValidCrmNextAction,
  deriveDefaultStatus,
  accountIsActive,
};
