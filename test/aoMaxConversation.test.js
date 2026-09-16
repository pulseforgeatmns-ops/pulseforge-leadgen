'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
  resolveConversationIntent,
  buildWhyPrioritizedReply,
  trimHistory,
  mergeConversationContext,
} = require('../utils/aoConversationContext');
const { redactObject, redactString } = require('../utils/aoConversationRedaction');
const {
  buildReportTranscript,
  buildReportContext,
} = require('../utils/aoConversationReport');
const fs = require('node:fs');
const path = require('node:path');

const sampleContext = {
  prioritized_accounts: [
    {
      business_name: 'Anagnost Companies',
      lead_id: 'lead-1',
      why_now: 'Follow-up is due today.',
    },
    {
      business_name: 'Beacon Law',
      lead_id: 'lead-2',
      why_now: 'High-interest conversation needs your next touch.',
    },
  ],
  selected_account: {
    business_name: 'Anagnost Companies',
    lead_id: 'lead-1',
  },
};

test('resolveConversationIntent handles why-first follow-up from prioritization context', () => {
  const resolved = resolveConversationIntent('Why Anagnost first?', sampleContext);
  assert.equal(resolved.intent, 'why_prioritized');
  assert.equal(resolved.account.business_name, 'Anagnost Companies');
});

test('resolveConversationIntent handles pronoun briefing follow-up', () => {
  const resolved = resolveConversationIntent('Brief me on them.', sampleContext);
  assert.equal(resolved.intent, 'account_briefing');
  assert.equal(resolved.briefingTarget, 'Anagnost Companies');
});

test('resolveConversationIntent handles contact follow-up without restating company', () => {
  const resolved = resolveConversationIntent('Who should I ask for?', sampleContext);
  assert.equal(resolved.intent, 'account_contacts');
  assert.equal(resolved.briefingTarget, 'Anagnost Companies');
});

test('buildWhyPrioritizedReply explains top account ranking', () => {
  const reply = buildWhyPrioritizedReply(sampleContext.prioritized_accounts[0], sampleContext);
  assert.match(reply, /Anagnost Companies is first/i);
  assert.match(reply, /Follow-up is due today/i);
});

test('mergeConversationContext stores prioritization and selected account', () => {
  const merged = mergeConversationContext({}, {
    intent: 'account_prioritization',
    accounts: sampleContext.prioritized_accounts,
  });
  assert.equal(merged.last_intent, 'account_prioritization');
  assert.equal(merged.prioritized_accounts.length, 2);
});

test('trimHistory bounds message count', () => {
  const messages = Array.from({ length: 30 }, (_, i) => ({ role: 'user', content: String(i) }));
  const trimmed = trimHistory(messages, 20);
  assert.equal(trimmed.length, 20);
  assert.equal(trimmed[0].content, '10');
});

test('redactObject removes obvious secrets from transcript payloads', () => {
  const redacted = redactObject({
    note: 'password=supersecret token=abc123',
    authorization: 'Bearer xyz',
    safe: 'Why Anagnost first?',
  });
  assert.match(redacted.note, /\[REDACTED\]/);
  assert.equal(redacted.authorization, '[REDACTED]');
  assert.equal(redacted.safe, 'Why Anagnost first?');
});

test('redactString masks bearer tokens inline', () => {
  const out = redactString('Use bearer sk-live-abcdefghij token here');
  assert.doesNotMatch(out, /sk-live/);
  assert.match(out, /\[REDACTED\]/);
});

test('buildReportTranscript includes only conversation messages', () => {
  const transcript = buildReportTranscript({
    messages: [
      { role: 'user', content: 'What accounts should I focus on today?', intent: null, ts: '2026-09-16T10:00:00Z' },
      { role: 'max', content: 'Start with Anagnost Companies…', intent: 'account_prioritization', ts: '2026-09-16T10:00:01Z' },
    ],
    prioritized_accounts: sampleContext.prioritized_accounts,
  });
  assert.equal(transcript.length, 2);
  assert.equal(transcript[0].role, 'user');
  assert.equal(transcript[1].intent, 'account_prioritization');
});

test('buildReportContext includes tenant-scoped session metadata', () => {
  const session = {
    id: 'sess-1',
    mode: 'conversation',
    client_id: 10,
    ao_owner_id: 42,
    created_at: '2026-09-16T10:00:00Z',
    updated_at: '2026-09-16T10:05:00Z',
  };
  const context = buildReportContext(session, sampleContext);
  assert.equal(context.session_id, 'sess-1');
  assert.equal(context.client_id, 10);
  assert.equal(context.ao_owner_id, 42);
  assert.equal(context.field_mode, true);
  assert.equal(context.selected_account.business_name, 'Anagnost Companies');
});

test('askMax routes through conversation turn handler', () => {
  const aoMaxFlowSrc = fs.readFileSync(path.join(__dirname, '..', 'services', 'aoMaxFlow.js'), 'utf8');
  const aoMaxConversationSrc = fs.readFileSync(path.join(__dirname, '..', 'services', 'aoMaxConversation.js'), 'utf8');
  assert.match(aoMaxFlowSrc, /handleConversationTurn/);
  assert.match(aoMaxConversationSrc, /executeConversationIntent/);
});

test('ao routes expose conversation report endpoints', () => {
  const aoRoutesSrc = fs.readFileSync(path.join(__dirname, '..', 'routes', 'ao.js'), 'utf8');
  assert.match(aoRoutesSrc, /\/api\/max\/new-conversation/);
  assert.match(aoRoutesSrc, /\/api\/max\/report/);
  assert.match(aoRoutesSrc, /\/api\/max\/reports/);
});

test('AO dashboard renders flag conversation control', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'ao-dashboard.html'), 'utf8');
  assert.match(html, /flagConversationBtn/);
  assert.match(html, /Flag conversation/);
  assert.match(html, /newConversationBtn/);
});

test('MAX_MODES includes conversation mode', () => {
  const schemaSrc = fs.readFileSync(path.join(__dirname, '..', 'utils', 'aoFieldSchema.js'), 'utf8');
  assert.match(schemaSrc, /'conversation'/);
  assert.match(schemaSrc, /ao_max_conversation_reports/);
});
