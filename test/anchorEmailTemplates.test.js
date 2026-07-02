const assert = require('assert');
const { ANCHOR_DRAFT_SEQUENCES } = require('../utils/anchorEmailTemplates');
const { CLIENT_SEQUENCE_MAP } = require('../utils/sendingReadiness');

assert.deepStrictEqual(CLIENT_SEQUENCE_MAP[10], {
  law_firm: 'anchor_law_firm_draft',
  accounting: 'anchor_accounting_draft',
}, 'Anchor sequences must be explicitly mapped for client 10 only');

const allowedTokens = new Set(['first_name', 'business_name_short']);
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
    const businessNameMentions = tokens.filter(token => token === 'business_name_short').length;
    assert(
      businessNameMentions <= 1,
      `${name} step ${step.day} mentions business_name_short ${businessNameMentions} times`
    );
    for (const segment of step.protectedSegments || []) {
      assert(step.body.includes(segment), `${name} step ${step.day} drops protected segment: ${segment}`);
    }
  }
}

const lawFirmDay4 = ANCHOR_DRAFT_SEQUENCES.anchor_law_firm_draft.find(step => step.day === 4);
assert.deepStrictEqual(lawFirmDay4.protectedSegments, [
  "I've run service businesses for over a decade",
], 'Anchor law-firm day 4 must protect the owner-operator credibility claim');
assert(/service businesses/i.test(lawFirmDay4.body), 'protected claim must say the owner ran service businesses');
assert(/over a decade/i.test(lawFirmDay4.body), 'protected claim must preserve roughly a decade of ownership experience');

const accountingDay8 = ANCHOR_DRAFT_SEQUENCES.anchor_accounting_draft.find(step => step.day === 8);
assert.deepStrictEqual(accountingDay8.protectedSegments, [
  "I spent years running restaurant crews before I ever ran cleaning crews, and the standard only held when a specific person answered for it.",
], 'Anchor accounting day 8 must protect the owner-operator credibility claim');

console.log('Anchor email template tests passed');
