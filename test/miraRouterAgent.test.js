const assert = require('node:assert/strict');
const test = require('node:test');

const dbPath = require.resolve('../db');
const routerPath = require.resolve('../miraRouterAgent');
const queries = [];
const mockPool = {
  async query(sql, params = []) {
    queries.push({ sql, params });
    return { rows: [] };
  },
};

require.cache[dbPath] = {
  id: dbPath,
  filename: dbPath,
  loaded: true,
  exports: mockPool,
};

delete require.cache[routerPath];
const { routeCapture } = require('../miraRouterAgent');

test.beforeEach(() => {
  queries.length = 0;
});

test('a reminder without remind_at is sent to manual review without an insert', async () => {
  const result = await routeCapture({
    id: 9001,
    raw_text: 'Remind me about the proposal',
    classification: 'reminder',
    raw_metadata: { suggested_routing: {} },
  });

  assert.deepEqual(result, {
    id: 9001,
    status: 'skipped',
    reason: 'reminder_missing_valid_remind_at',
    routed_to_table: 'manual_review',
  });
  assert.equal(queries.some(({ sql }) => /INSERT INTO reminders/.test(sql)), false);
  const update = queries.find(({ sql }) => /UPDATE capture_inbox/.test(sql));
  assert.deepEqual(update.params, [
    'review_needed',
    'manual_review',
    'reminder_missing_valid_remind_at',
    9001,
  ]);
});

test('a historical Telegram slash command is skipped before routing', async () => {
  const result = await routeCapture({
    id: 9002,
    raw_text: '/start',
    classification: 'reminder',
    raw_metadata: { suggested_routing: {} },
  });

  assert.equal(result.status, 'skipped');
  assert.equal(result.reason, 'telegram_control_command');
  assert.equal(queries.some(({ sql }) => /INSERT INTO reminders/.test(sql)), false);
  const update = queries.find(({ sql }) => /UPDATE capture_inbox/.test(sql));
  assert.deepEqual(update.params, ['routed', 'skipped', 'telegram_control_command', 9002]);
});
