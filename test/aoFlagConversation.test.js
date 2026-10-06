'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const { buildConversationDeepLink, createAoEscalation } = require('../services/aoEscalationService');
const { CONVERSATION_ESCALATION_REASON } = require('../utils/aoAccountFlagTypes');
const { resetForTests } = require('../utils/aoFlagMetrics');

test('conversation deep link uses session query param', () => {
  const id = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee';
  assert.equal(
    buildConversationDeepLink(id, 3),
    `/ao/field?session=${encodeURIComponent(id)}&message=3`
  );
});

test('reportConversation path creates canonical escalation', () => {
  const src = fs.readFileSync(path.join(__dirname, '..', 'services', 'aoMaxConversation.js'), 'utf8');
  assert.match(src, /createAoEscalation/);
  assert.match(src, /canonical_flag_id/);
  assert.match(src, /flagged_for_jake/);
});

test('conversation reports schema links canonical_flag_id', () => {
  const migration = fs.readFileSync(
    path.join(__dirname, '..', 'migrations', '2026-10-06-ao-flag-inbox-002-conversation.sql'),
    'utf8'
  );
  assert.match(migration, /canonical_flag_id/);
});

test('AO dashboard shows Flagged for Jake success copy', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'ao-dashboard.html'), 'utf8');
  assert.match(html, /Flagged for Jake/);
  assert.match(html, /flag_id/);
});

function makeMockDb(state) {
  return {
    query: async (sql, params) => {
      if (/FROM users/.test(sql)) return { rows: [state.assignee] };
      if (/INSERT INTO ao_account_flags/.test(sql)) {
        state.flag = {
          id: 'flag-conv-1',
          client_id: params[0],
          account_id: params[1],
          ao_id: params[2],
          reason: params[3],
          note: params[4],
          status: 'open',
          source_type: params[5],
          source_id: params[6],
          conversation_id: params[7],
          unread: true,
          created_at: new Date().toISOString(),
          source_context: {},
        };
        return { rows: [state.flag] };
      }
      if (/INSERT INTO agent_log/.test(sql)) return { rows: [] };
      if (/SELECT name FROM users/.test(sql)) return { rows: [{ name: 'Tony' }] };
      if (/INSERT INTO ao_flag_notifications/.test(sql)) {
        state.notificationCreated = true;
        return { rows: [{ id: 'n1' }] };
      }
      if (/SELECT \* FROM ao_account_flags/.test(sql)) return { rows: state.duplicate ? [state.duplicateRow] : [] };
      return { rows: [] };
    },
  };
}

test('CF1 Tony conversation flag creates notification', async () => {
  resetForTests();
  const sessionId = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee';
  const state = {
    assignee: { id: 1, role: 'admin' },
    duplicate: false,
  };
  const db = makeMockDb(state);
  const result = await createAoEscalation({
    clientId: 10,
    createdByUserId: 99,
    sourceType: 'conversation',
    sourceId: sessionId,
    conversationId: sessionId,
    reason: CONVERSATION_ESCALATION_REASON,
    note: 'Decision-maker question',
    companyName: 'Exeter Phillips',
    db,
  });
  assert.equal(result.ok, true);
  assert.equal(result.flag.source_type, 'conversation');
  assert.equal(state.notificationCreated, true);
});

test('CF2 Jake self-flag conversation skips notification', async () => {
  resetForTests();
  const sessionId = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee';
  const state = { assignee: { id: 1, role: 'admin' } };
  const db = makeMockDb(state);
  const result = await createAoEscalation({
    clientId: 10,
    createdByUserId: 1,
    creatorRole: 'admin',
    sourceType: 'conversation',
    sourceId: sessionId,
    conversationId: sessionId,
    reason: CONVERSATION_ESCALATION_REASON,
    note: 'Self review',
    db,
  });
  assert.equal(result.notification_skipped, true);
  assert.equal(state.notificationCreated, undefined);
});
