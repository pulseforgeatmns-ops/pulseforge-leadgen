'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
  buildDirectMailOpening,
  suggestTemplate,
  TEMPLATES,
} = require('../utils/aoMessageTemplates');
const { DIRECT_MAIL_FOLLOW_UP_STEPS } = require('../utils/aoDirectMailFlow');
const { DIRECT_MAIL_TARGETS, CAMPAIGN_NAME } = require('../scripts/data/anchorDirectMailTargets');

test('buildDirectMailOpening uses Anchor direct mail script', () => {
  const opening = buildDirectMailOpening('Mike');
  assert.match(opening, /Mike with Anchor Cleaning/i);
  assert.match(opening, /recently sent some info over about commercial cleaning/i);
  assert.match(opening, /Who usually handles cleaning or facility vendors here/i);
});

test('suggestTemplate picks direct mail revisit for campaign follow-ups', () => {
  const template = suggestTemplate({
    attributionSource: 'direct_mail_campaign',
    nextAction: 'in_person_revisit',
  });
  assert.equal(template.id, TEMPLATES.direct_mail_revisit.id);
});

test('DIRECT_MAIL_FOLLOW_UP_STEPS covers Campaign 001 logging prompts', () => {
  const questions = DIRECT_MAIL_FOLLOW_UP_STEPS.map(s => s.question).join(' ');
  assert.match(questions, /remember receiving the mailer/i);
  assert.match(questions, /handles cleaning decisions/i);
  assert.match(questions, /reach that person/i);
  assert.match(questions, /outside cleaner/i);
  assert.match(questions, /consistency, quality, communication, or scheduling/i);
  assert.match(questions, /walkthrough/i);
  assert.match(questions, /revisit|Jake follow up|not a fit/i);
});

test('seed manifest has 20 Campaign 001 businesses', () => {
  assert.equal(DIRECT_MAIL_TARGETS.length, 20);
  assert.equal(CAMPAIGN_NAME, 'Campaign 001');
  assert.ok(DIRECT_MAIL_TARGETS.some(t => t.business_name === 'Gamache Properties'));
  assert.ok(DIRECT_MAIL_TARGETS.some(t => t.business_name === 'Lot 202 LLC'));
});

test('aoFieldService exports direct mail helper names', () => {
  const fs = require('fs');
  const source = fs.readFileSync(require.resolve('../services/aoFieldService.js'), 'utf8');
  assert.match(source, /createDirectMailFollowUpLead/);
  assert.match(source, /completeDirectMailFollowUp/);
  assert.match(source, /endOfBusinessWeekISO/);
});

test('aoFieldSchema defines warm priority and direct_mail_follow_up mode', () => {
  const fs = require('fs');
  const source = fs.readFileSync(require.resolve('../utils/aoFieldSchema.js'), 'utf8');
  assert.match(source, /'warm'/);
  assert.match(source, /direct_mail_follow_up/);
  assert.match(source, /direct_mail_campaign/);
});
