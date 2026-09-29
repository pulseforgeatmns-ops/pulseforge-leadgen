'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
  resolveAccountReference,
  normalizeAccountName,
  buildAmbiguityReply,
} = require('../utils/aoAccountResolution');
const { resolveConversationIntent, mergeConversationContext } = require('../utils/aoConversationContext');

const productionPrioritizedContext = {
  prioritized_accounts: [
    { business_name: 'Anagnost Companies', lead_id: 'lead-1', why_now: 'Follow-up is due today.' },
    { business_name: 'Brady Sullivan Properties', lead_id: 'lead-2', why_now: 'High-interest conversation.' },
    { business_name: 'CPManagement', lead_id: 'lead-3', why_now: 'Needs revisit.' },
    { business_name: 'Evergreen Management Group', lead_id: 'lead-4', why_now: 'Warm signal.' },
    { business_name: 'Farley White Management Company', lead_id: 'lead-5', why_now: 'Due this week.' },
  ],
  selected_account: {
    business_name: 'Anagnost Companies',
    lead_id: 'lead-1',
  },
};

test('normalizeAccountName strips punctuation and common suffixes', () => {
  assert.equal(normalizeAccountName('Brady Sullivan Properties, LLC'), 'brady sullivan');
  assert.equal(normalizeAccountName('Anagnost Companies'), 'anagnost');
});

test('typo resolves aganost to Anagnost Companies within prioritized context', () => {
  const resolution = resolveAccountReference({
    query: 'aganost',
    message: 'brief me on aganost',
    context: productionPrioritizedContext,
  });
  assert.equal(resolution.status, 'resolved');
  assert.equal(resolution.account.business_name, 'Anagnost Companies');
  assert.equal(resolution.method, 'typo');
});

test('exact normalized match resolves anagnost', () => {
  const resolution = resolveAccountReference({
    query: 'anagnost',
    message: 'brief me on anagnost',
    context: productionPrioritizedContext,
  });
  assert.equal(resolution.status, 'resolved');
  assert.equal(resolution.account.business_name, 'Anagnost Companies');
  assert.equal(resolution.method, 'exact');
});

test('prefix match resolves brady to Brady Sullivan Properties', () => {
  const resolution = resolveAccountReference({
    query: 'brady',
    message: 'brief me on brady',
    context: productionPrioritizedContext,
  });
  assert.equal(resolution.status, 'resolved');
  assert.equal(resolution.account.business_name, 'Brady Sullivan Properties');
});

test('ordinal reference resolves the first prioritized account', () => {
  const resolution = resolveAccountReference({
    message: 'brief me on the first one',
    context: productionPrioritizedContext,
  });
  assert.equal(resolution.status, 'resolved');
  assert.equal(resolution.account.business_name, 'Anagnost Companies');
  assert.equal(resolution.method, 'context_reference');
});

test('resolveConversationIntent maps typo briefing to canonical account', () => {
  const resolved = resolveConversationIntent('brief me on aganost', productionPrioritizedContext);
  assert.equal(resolved.intent, 'account_briefing');
  assert.equal(resolved.briefingTarget, 'Anagnost Companies');
  assert.equal(resolved.account.lead_id, 'lead-1');
});

test('resolveConversationIntent keeps brady off selected_account context', () => {
  const resolved = resolveConversationIntent('brief me on brady', productionPrioritizedContext);
  assert.equal(resolved.intent, 'account_briefing');
  assert.equal(resolved.account.business_name, 'Brady Sullivan Properties');
});

test('contact follow-up uses selected account without restating company', () => {
  const resolved = resolveConversationIntent('Who should I ask for there?', productionPrioritizedContext);
  assert.equal(resolved.intent, 'account_contacts');
  assert.equal(resolved.account.business_name, 'Anagnost Companies');
});

test('contact follow-up without there still uses selected account', () => {
  const resolved = resolveConversationIntent('Who should I ask for?', productionPrioritizedContext);
  assert.equal(resolved.intent, 'account_contacts');
  assert.equal(resolved.account.business_name, 'Anagnost Companies');
});

test('ambiguous prefix match asks clarifying question', () => {
  const ambiguousContext = {
    prioritized_accounts: [
      { business_name: 'Brady Sullivan Properties', lead_id: 'lead-2' },
      { business_name: 'Brady Realty Group', lead_id: 'lead-4' },
    ],
  };

  const resolution = resolveAccountReference({
    query: 'brady',
    message: 'brief me on brady',
    context: ambiguousContext,
  });
  assert.equal(resolution.status, 'ambiguous');
  const reply = buildAmbiguityReply(resolution.query, resolution.candidates);
  assert.match(reply, /Did you mean/i);
  assert.match(reply, /Brady Sullivan Properties/);
  assert.match(reply, /Brady Realty Group/);

  const resolved = resolveConversationIntent('brief me on brady', ambiguousContext);
  assert.equal(resolved.intent, 'account_briefing');
  assert.equal(resolved.ambiguous, true);
  assert.match(resolved.ambiguityReply, /Did you mean/i);
});

test('typo match stays within assigned account pool only', () => {
  const zachAccounts = [
    { business_name: 'Anagnost Companies', lead_id: 'zach-lead-1' },
  ];
  const otherAoAccounts = [
    { business_name: 'Anagnost Holdings', lead_id: 'other-lead-1' },
  ];

  const resolution = resolveAccountReference({
    query: 'aganost',
    message: 'brief me on aganost',
    context: { prioritized_accounts: zachAccounts },
    assignedAccounts: zachAccounts,
  });
  assert.equal(resolution.status, 'resolved');
  assert.equal(resolution.account.lead_id, 'zach-lead-1');
  assert.notEqual(resolution.account.lead_id, 'other-lead-1');

  const crossResolution = resolveAccountReference({
    query: 'aganost',
    message: 'brief me on aganost',
    context: { prioritized_accounts: zachAccounts },
    assignedAccounts: otherAoAccounts,
  });
  assert.notEqual(crossResolution.account?.lead_id, 'zach-lead-1');
});

test('low-confidence typo remains unresolved', () => {
  const resolution = resolveAccountReference({
    query: 'xyzzy',
    message: 'brief me on xyzzy',
    context: productionPrioritizedContext,
  });
  assert.equal(resolution.status, 'unresolved');
});

test('successful briefing merge updates selected_account', () => {
  const merged = mergeConversationContext({}, {
    intent: 'account_briefing',
    account: { business_name: 'Brady Sullivan Properties', lead_id: 'lead-2' },
  });
  assert.equal(merged.selected_account.business_name, 'Brady Sullivan Properties');
  assert.equal(merged.selected_account.lead_id, 'lead-2');
});

test('new conversation payload starts without prior account context', () => {
  const freshPayload = { messages: [] };
  assert.equal(freshPayload.prioritized_accounts, undefined);
  assert.equal(freshPayload.selected_account, undefined);
});
