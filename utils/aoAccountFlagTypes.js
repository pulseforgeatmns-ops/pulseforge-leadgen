'use strict';

const AO_ACCOUNT_FLAG_REASONS = Object.freeze([
  { value: 'pricing_or_scope', label: 'Pricing or scope question' },
  { value: 'decision_maker_block', label: 'Can\'t reach decision-maker' },
  { value: 'incumbent_vendor', label: 'Incumbent vendor in the way' },
  { value: 'walkthrough_or_proposal', label: 'Walkthrough or proposal help' },
  { value: 'account_intel_gap', label: 'Missing account intel' },
  { value: 'other', label: 'Something else' },
  { value: 'conversation_flag', label: 'Conversation flag' },
]);

const CONVERSATION_ESCALATION_REASON = 'conversation_flag';

function recommendJakeActionForFlag(reason, companyName = null) {
  const who = companyName ? `${companyName}` : 'this account';
  switch (reason) {
    case 'pricing_or_scope':
      return `Review pricing/scope with the AO, then call ${who} with a clear recommendation.`;
    case 'decision_maker_block':
      return `Help the AO map the DM path for ${who} — name, title, and best intro route.`;
    case 'incumbent_vendor':
      return `Coach the AO on a Diagnose-first angle vs the incumbent at ${who}.`;
    case 'walkthrough_or_proposal':
      return `Confirm walkthrough/proposal next step for ${who} and unblock the AO today.`;
    case 'account_intel_gap':
      return `Fill the intel gap on ${who} (contacts, vendor, pain) so the AO can move.`;
    case 'conversation_flag':
      return `Open the flagged Max conversation${companyName ? ` for ${who}` : ''} and reply with a concrete next move.`;
    default:
      return `Read the AO note on ${who} and reply with a concrete next move.`;
  }
}

module.exports = {
  AO_ACCOUNT_FLAG_REASONS,
  CONVERSATION_ESCALATION_REASON,
  recommendJakeActionForFlag,
};
