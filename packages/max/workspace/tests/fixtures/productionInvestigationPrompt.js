'use strict';
// The exact operator prompt from the failed post-#1120 production acceptance
// run (tenant 21), verbatim, including its read-only prohibition. It contains
// no personal data. `classifyOperationalRead` once rejected it as
// "execution_language" because of "Do not send messages, assign prospects".
const PRODUCTION_INVESTIGATION_PROMPT = [
  'Review the six current contact_now prospects for LA Endeavors and rank them by investigation priority using only recorded evidence and the approved Blueprint.',
  '',
  'For each prospect, show:',
  '',
  '- Investigation position and evidence coverage',
  '- Recorded but unverified financing, deal-stage, security, exit and closing-date claims',
  '- Missing information and reasons for the ranking',
  '- Financing-fit comparison only where approved Blueprint criteria support it',
  '- Outreach eligibility',
  '',
  'This is a read-only investigation. Do not send messages, assign prospects, modify records, or delegate execution.',
].join('\n');

module.exports = { PRODUCTION_INVESTIGATION_PROMPT };
