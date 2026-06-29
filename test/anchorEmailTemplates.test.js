const assert = require('assert');
const { ANCHOR_DRAFT_SEQUENCES } = require('../utils/anchorEmailTemplates');
const { CLIENT_SEQUENCE_MAP } = require('../utils/sendingReadiness');

assert.deepStrictEqual(CLIENT_SEQUENCE_MAP[10], {
  law_firm: 'anchor_law_firm_draft',
  accounting: 'anchor_accounting_draft',
}, 'Anchor sequences must be explicitly mapped for client 10 only');

const allowedTokens = new Set(['first_name', 'business_name']);
for (const [name, steps] of Object.entries(ANCHOR_DRAFT_SEQUENCES)) {
  assert.deepStrictEqual(steps.map(step => step.day), [0, 4, 8, 13], `${name} cadence`);
  for (const step of steps) {
    assert(step.subject && step.body, `${name} step ${step.day} must be complete`);
    assert(!step.body.includes('—'), `${name} step ${step.day} contains an em dash`);
    assert(!/Pulseforge/i.test(`${step.subject}\n${step.body}`), `${name} contains Pulseforge branding`);
    assert(/Anchor Cleaning/.test(step.body), `${name} step ${step.day} lacks Anchor branding`);
    assert(/jacob@goanchorcleaning\.com/.test(step.body), `${name} step ${step.day} lacks sender email`);
    assert(/\(603\) 420-2430/.test(step.body), `${name} step ${step.day} lacks phone`);
    const tokens = [...`${step.subject}\n${step.body}`.matchAll(/{{([^}]+)}}/g)].map(match => match[1]);
    tokens.forEach(token => assert(allowedTokens.has(token), `${name} has unsupported token ${token}`));
    const businessNameMentions = tokens.filter(token => token === 'business_name').length;
    assert(
      businessNameMentions <= 1,
      `${name} step ${step.day} mentions business_name ${businessNameMentions} times`
    );
  }
}

console.log('Anchor email template tests passed');
