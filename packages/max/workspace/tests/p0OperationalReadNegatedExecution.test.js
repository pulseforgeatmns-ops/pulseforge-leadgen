'use strict';
// P0 (post-#1120 production acceptance): a prohibition is not an execution
// request. classifyOperationalRead ran EXECUTION_RE on the raw prompt, so
// "Do not send messages, assign prospects, modify records, or delegate
// execution" classified a read-only investigation as execution. The turn then
// skipped operational-read recovery and fell into General Conversation.
require('./workspaceTestHarness');
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { classifyOperationalRead } = require('../OperationalReadIntent');
const { PRODUCTION_INVESTIGATION_PROMPT } = require('./fixtures/productionInvestigationPrompt');

const read = q => classifyOperationalRead(q);

test('the exact production prompt is an investigation read, not execution', () => {
  const r = read(PRODUCTION_INVESTIGATION_PROMPT);
  assert.equal(r.isRead, true, r.reason);
  assert.equal(r.reason, 'operational_read');
  assert.equal(r.reviewState, 'contact_now');
  assert.equal(r.priorityMode, 'investigation');
  assert.equal(r.wantsRanking, true);
});

test('affirmative execution requests remain execution, including mixed read + execution requests', () => {
  for (const q of [
    'Rank these prospects and send the top one an email.',
    'Rank my contact_now prospects and send the top one an email.',
    'Send them the email.',
    'Assign these contact_now prospects to Levi.',
    'Email them today.',
    'List my prospects and assign them to Levi.',
    'Do not send anything yet. Assign my contact_now prospects to Levi.',
    'Rank my contact_now prospects. Do not send messages; assign the top one to Levi.',
  ]) {
    const r = read(q);
    assert.equal(r.isRead, false, q);
    assert.equal(r.reason, 'execution_language', q);
  }
});

test('prohibited actions are ignored for execution classification', () => {
  for (const q of [
    'Review my contact_now prospects. Do not send anything.',
    'Rank my contact_now prospects for investigation. Do not assign anyone.',
    'Rank my contact_now prospects for investigation. Do not delegate execution.',
    "Rank my contact_now prospects for investigation. Don't send messages, assign prospects or modify records.",
    'List my prospects; do not assign or email anyone.',
    'Brief me on my prospects. Never send or schedule anything from this.',
  ]) {
    const r = read(q);
    assert.equal(r.isRead, true, `${q} -> ${r.reason}`);
  }
});

// Documented conservative behaviour. A negated clause runs to the next
// sentence boundary (a comma does not end it), so stripping alone would hide
// the affirmative command in these. They stay execution, as before the fix:
// they are never answered as a read-only investigation.
test('adversarial negation: a negated clause that turns affirmative keeps its execution meaning', () => {
  for (const q of [
    'Do not wait, assign my contact_now prospects to Levi and rank them.',
    'Rank my contact_now prospects — do not send, but assign the top one to Levi.',
    'Never mind the ranking, send my contact_now prospects an email.',
    'Rank my contact_now prospects. Do not hesitate to email them.',
  ]) {
    const r = read(q);
    assert.equal(r.isRead, false, q);
    assert.equal(r.reason, 'execution_language', q);
  }
  // Over-conservative by design: two prohibitions joined by "but" also stay
  // out of the read path (no read answer, and still nothing is executed).
  assert.equal(read('Rank my contact_now prospects. Do not send messages, but do not assign anyone either.').reason, 'execution_language');
  // Without a tenant record object an idiomatic negation is not a read at all.
  assert.equal(read('Do not wait, assign these prospects to Levi.').isRead, false);
});

test('desk workflows and discovery remain non-reads with a prohibition attached', () => {
  // Desk workflows and persistence keep their own handlers.
  assert.equal(read('Draft the preparation package for my prospects. Do not send it.').isRead, false);
  // Discovery is still not a read.
  assert.equal(read('Find new prospects in Example County. Do not email them.').isRead, false);
});
