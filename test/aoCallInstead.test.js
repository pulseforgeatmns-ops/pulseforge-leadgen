'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
  buildTelUrl,
  normalizePhoneForTel,
  buildPhoneFollowUpDebrief,
  buildNextStopDebrief,
} = require('../utils/aoRoutePlanner');
const { PHONE_FOLLOW_UP_STEPS } = require('../utils/aoPhoneFollowUpFlow');
const { formatPhoneConversionNote } = require('../services/aoFieldService');

test('normalizePhoneForTel strips formatting', () => {
  assert.equal(normalizePhoneForTel('(603) 555-1212'), '6035551212');
  assert.equal(normalizePhoneForTel(''), null);
  assert.equal(normalizePhoneForTel('123'), null);
});

test('buildTelUrl produces tel deep link', () => {
  assert.equal(buildTelUrl('603-555-1212'), 'tel:6035551212');
  assert.equal(buildTelUrl(''), null);
});

test('PHONE_FOLLOW_UP_STEPS covers call logging questions', () => {
  const keys = PHONE_FOLLOW_UP_STEPS.map(s => s.key);
  assert.ok(keys.includes('contact_reached'));
  assert.ok(keys.includes('reached_decision_maker'));
  assert.ok(keys.includes('call_summary'));
  assert.ok(keys.includes('mailer_received'));
  assert.ok(keys.includes('walkthrough_interest'));
  assert.ok(keys.includes('next_step'));
});

test('buildPhoneFollowUpDebrief includes call actions', () => {
  const msg = buildPhoneFollowUpDebrief({
    business_name: 'Lodgism',
    contact_phone: '6035551212',
    contact_name: 'Pat',
    attribution_source: 'direct_mail_campaign',
    campaign_name: 'Campaign 001',
  });
  assert.match(msg, /Lodgism/);
  assert.match(msg, /6035551212/);
  assert.match(msg, /Log Call With Max/);
});

test('buildNextStopDebrief mentions Call Instead', () => {
  const msg = buildNextStopDebrief({
    business_name: 'Lodgism',
    address: '100 Main St',
    attribution_source: 'direct_mail_campaign',
    campaign_name: 'Campaign 001',
  }, 'Mike');
  assert.match(msg, /Call Instead/);
});

test('formatPhoneConversionNote is exported from aoFieldService', () => {
  assert.equal(typeof formatPhoneConversionNote, 'function');
  assert.match(formatPhoneConversionNote(), /phone follow-up/i);
});
