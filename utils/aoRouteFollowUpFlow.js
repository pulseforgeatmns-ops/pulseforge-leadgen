const ROUTE_FOLLOW_UP_STEPS = Object.freeze([
  { key: 'contact_name', question: 'Who did you talk to?' },
  { key: 'contact_role', question: 'Are they the decision-maker, a gatekeeper, or unknown?' },
  { key: 'visit_note', question: 'What happened on this stop?' },
  { key: 'next_step', question: 'What should happen next? (revisit, Jake follow up, not a fit, or done)' },
]);

module.exports = {
  ROUTE_FOLLOW_UP_STEPS,
};
