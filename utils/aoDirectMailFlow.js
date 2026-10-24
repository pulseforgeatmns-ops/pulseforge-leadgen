const DIRECT_MAIL_FOLLOW_UP_STEPS = Object.freeze([
  { key: 'mailer_remembered', question: 'Did they remember receiving the mailer?' },
  { key: 'cleaning_decision_maker', question: 'Who handles cleaning decisions?' },
  { key: 'reached_decision_maker', question: 'Did you reach that person?' },
  { key: 'outside_cleaner', question: 'Are they using an outside cleaner now?' },
  { key: 'cleaner_issues', question: 'Any issues with consistency, quality, communication, or scheduling?' },
  { key: 'walkthrough_interest', question: 'Any interest in a walkthrough?' },
  { key: 'next_step', question: 'Should you revisit, should Jake follow up, or should we mark this as not a fit?' },
  { key: 'visit_note', question: 'Anything else Max should know about this stop?' },
]);

module.exports = {
  DIRECT_MAIL_FOLLOW_UP_STEPS,
};
