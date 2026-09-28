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
  'interested',
  'asked_to_follow_up',
  'bad_fit',
  'booked_walkthrough',
  'needs_jake',
]);

const OUTCOME_TO_NEXT_ACTION = Object.freeze({
  booked_assessment: 'BOOK_ASSESSMENT',
  booked_walkthrough: 'BOOK_ASSESSMENT',
  follow_up_later: 'AO_FOLLOW_UP',
  asked_to_follow_up: 'AO_FOLLOW_UP',
  needs_research: 'NEEDS_RESEARCH',
  not_fit: 'SUPPRESS',
  bad_fit: 'SUPPRESS',
  not_interested: 'NURTURE',
  sent_email: 'AO_FOLLOW_UP',
  interested: 'AO_FOLLOW_UP',
  needs_jake: 'JAKE_REVIEW',
});

const OUTCOME_TO_ADVISORY_STAGE = Object.freeze({
  called_no_answer: 'in_progress',
  left_voicemail: 'in_progress',
  sent_email: 'in_progress',
  spoke_gatekeeper: 'in_progress',
  spoke_decision_maker: 'in_progress',
  booked_assessment: 'debrief_complete',
  booked_walkthrough: 'debrief_complete',
  not_interested: 'closed',
  not_fit: 'closed',
  bad_fit: 'closed',
  follow_up_later: 'in_progress',
  asked_to_follow_up: 'in_progress',
  needs_research: 'in_progress',
  interested: 'in_progress',
  needs_jake: 'in_progress',
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
