'use strict';

const AO_OUTCOME_TYPES = Object.freeze([
  'called_no_answer',
  'left_voicemail',
  'sent_email',
  'spoke_gatekeeper',
  'spoke_decision_maker',
  'booked_assessment',
  'not_interested',
  'not_fit',
  'follow_up_later',
  'needs_research',
  'other',
]);

const OUTCOME_TO_NEXT_ACTION = Object.freeze({
  booked_assessment: 'BOOK_ASSESSMENT',
  follow_up_later: 'AO_FOLLOW_UP',
  needs_research: 'NEEDS_RESEARCH',
  not_fit: 'SUPPRESS',
  not_interested: 'NURTURE',
  sent_email: 'AO_FOLLOW_UP',
});

const OUTCOME_TO_ADVISORY_STAGE = Object.freeze({
  called_no_answer: 'contact_attempted',
  left_voicemail: 'contact_attempted',
  sent_email: 'contact_attempted',
  spoke_gatekeeper: 'in_progress',
  spoke_decision_maker: 'in_progress',
  booked_assessment: 'debrief_complete',
  not_interested: 'closed',
  not_fit: 'closed',
  follow_up_later: 'in_progress',
  needs_research: 'in_progress',
});

function isValidOutcomeType(value) {
  return AO_OUTCOME_TYPES.includes(value);
}

module.exports = {
  AO_OUTCOME_TYPES,
  OUTCOME_TO_NEXT_ACTION,
  OUTCOME_TO_ADVISORY_STAGE,
  isValidOutcomeType,
};
