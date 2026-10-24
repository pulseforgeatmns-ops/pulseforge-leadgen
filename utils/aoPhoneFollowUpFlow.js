const PHONE_FOLLOW_UP_STEPS = Object.freeze([
  { key: 'contact_reached', question: 'Who did you reach?' },
  { key: 'reached_decision_maker', question: 'Were they the decision-maker?' },
  { key: 'call_summary', question: 'What did they say?' },
  { key: 'mailer_received', question: 'Did they receive the mailer? (yes / no / n/a)' },
  { key: 'walkthrough_interest', question: 'Are they interested in a walkthrough or quote?' },
  { key: 'next_step', question: 'What should happen next?' },
]);

module.exports = {
  PHONE_FOLLOW_UP_STEPS,
};
