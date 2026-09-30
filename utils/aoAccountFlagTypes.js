'use strict';

const AO_ACCOUNT_FLAG_DEFAULT_REASON = 'needs_owner_help';

const AO_ACCOUNT_FLAG_REASONS = Object.freeze([
  { value: 'needs_owner_help', label: 'Need Jake / owner help' },
  { value: 'pricing_or_scope', label: 'Pricing or scope question' },
  { value: 'decision_maker_block', label: 'Can\'t reach decision-maker' },
  { value: 'incumbent_vendor', label: 'Incumbent vendor in the way' },
  { value: 'walkthrough_or_proposal', label: 'Walkthrough or proposal help' },
  { value: 'account_intel_gap', label: 'Missing account intel' },
  { value: 'other', label: 'Something else' },
]);

function recommendJakeActionForFlag(reason, companyName = null) {
  const who = companyName ? `${companyName}` : 'this account';
  switch (reason) {
    case 'needs_owner_help':
      return `Read what the AO needs on ${who} and reply with a concrete next move today.`;
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
    default:
      return `Read the AO note on ${who} and reply with a concrete next move.`;
  }
}

module.exports = {
  AO_ACCOUNT_FLAG_DEFAULT_REASON,
  AO_ACCOUNT_FLAG_REASONS,
  recommendJakeActionForFlag,
};
