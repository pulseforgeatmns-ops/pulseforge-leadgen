'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
  buildSuggestedMessage,
  normalizeNextAction,
  parseContactRole,
  sanitizeUserFacingText,
  buildCompletionReply,
} = require('../utils/aoMessageTemplates');

test('parseContactRole detects decision-maker and gatekeeper', () => {
  assert.equal(parseContactRole('decision-maker'), 'decision_maker');
  assert.equal(parseContactRole('gatekeeper'), 'gatekeeper');
  assert.equal(parseContactRole('unknown'), 'unknown');
});

test('normalizeNextAction replaces Pulseforge admin phrasing', () => {
  assert.equal(normalizeNextAction('Pulseforge admin should call'), 'Jake should call');
});

test('buildSuggestedMessage uses visit context when contact is known', () => {
  const msg = buildSuggestedMessage({
    contactName: 'Sarah',
    businessName: 'Test Dental Office',
    aoName: 'Alex',
    visitNote: 'Current cleaner is inconsistent, they want more info',
    contactRole: 'decision_maker',
    interestLevel: 'high',
    nextAction: 'Send info and Jake should call',
    status: 'needs_follow_up',
  });
  assert.match(msg, /Sarah/);
  assert.match(msg, /Test Dental Office/);
  assert.doesNotMatch(msg, /whoever handles your office cleaning/i);
});

test('buildSuggestedMessage falls back to gatekeeper template', () => {
  const msg = buildSuggestedMessage({
    contactName: 'Front desk',
    businessName: 'Test Office',
    contactRole: 'gatekeeper',
    interestLevel: 'medium',
    nextAction: 'Follow up',
    status: 'decision_maker_absent',
  });
  assert.match(msg, /whoever manages your office cleaning/i);
});

test('buildCompletionReply includes clear done state', () => {
  const reply = buildCompletionReply({
    businessName: 'Test Dental Office',
    escalated: true,
    suggestedMessage: 'Hi Sarah...',
  });
  assert.match(reply, /Logged Test Dental Office/);
  assert.match(reply, /Logged and escalated/);
  assert.match(reply, /Log another visit or check your queue/);
  assert.doesNotMatch(reply, /Pulseforge admin/i);
});

test('sanitizeUserFacingText cleans admin phrasing', () => {
  assert.equal(sanitizeUserFacingText('Pulseforge admin should call tomorrow'), 'Jake should call tomorrow');
});
