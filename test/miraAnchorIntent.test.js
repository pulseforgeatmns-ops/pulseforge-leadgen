const assert = require('node:assert/strict');
const test = require('node:test');

const dbPath = require.resolve('../db');
const anchorPath = require.resolve('../utils/miraAnchor');
const queries = [];
const mockPool = {
  async query(sql, params = []) {
    queries.push({ sql, params });
    if (/INSERT INTO daily_anchors/.test(sql)) {
      return {
        rows: [{
          id: 42,
          client_id: params[0],
          anchor_date: params[1],
          primary_anchor: params[2],
          secondary_anchors: params[3],
          completion_notes: params[4],
        }],
      };
    }
    return { rows: [] };
  },
};

require.cache[dbPath] = {
  id: dbPath,
  filename: dbPath,
  loaded: true,
  exports: mockPool,
};

delete require.cache[anchorPath];
const {
  parseAnchorSetIntent,
  setCurrentAnchor,
  clearCurrentAnchor,
  clientNameForAnchor,
} = require('../utils/miraAnchor');

test.beforeEach(() => {
  queries.length = 0;
});

test('anchor-set intent recognizes slash command and natural phrases', () => {
  assert.deepEqual(parseAnchorSetIntent('/anchor Follow up from opens'), {
    matched: true,
    action: 'set',
    anchorText: 'Follow up from opens',
  });
  assert.deepEqual(parseAnchorSetIntent("today's anchor is Warm signals in Providence"), {
    matched: true,
    action: 'set',
    anchorText: 'Warm signals in Providence',
  });
  assert.deepEqual(parseAnchorSetIntent('anchor: Review reply gap'), {
    matched: true,
    action: 'set',
    anchorText: 'Review reply gap',
  });
});

test('anchor-set intent recognizes clear commands and ignores normal notes', () => {
  assert.deepEqual(parseAnchorSetIntent('/anchor clear'), {
    matched: true,
    action: 'clear',
    anchorText: 'clear',
  });
  assert.deepEqual(parseAnchorSetIntent('clear anchor'), {
    matched: true,
    action: 'clear',
    anchorText: '',
  });
  assert.equal(parseAnchorSetIntent('remember to follow up with Providence opens'), null);
});

test('setCurrentAnchor writes client-scoped daily anchor without capture pipeline', async () => {
  const row = await setCurrentAnchor({
    client_id: 1,
    primary_anchor: 'Warm signals up, replies flat.',
  });

  assert.equal(row.primary_anchor, 'Warm signals up, replies flat.');
  const insert = queries.find(({ sql }) => /INSERT INTO daily_anchors/.test(sql));
  assert.match(insert.sql, /ON CONFLICT \(client_id, anchor_date\) DO UPDATE/);
  assert.equal(insert.params[0], 1);
  assert.equal(insert.params[2], 'Warm signals up, replies flat.');
  assert.equal(queries.some(({ sql }) => /INSERT INTO capture_inbox/.test(sql)), false);
});

test('clearCurrentAnchor nulls the client-scoped field', async () => {
  const row = await clearCurrentAnchor(1);

  assert.equal(row.primary_anchor, null);
  const insert = queries.find(({ sql }) => /INSERT INTO daily_anchors/.test(sql));
  assert.equal(insert.params[0], 1);
  assert.equal(insert.params[2], null);
});

test('clientNameForAnchor labels Pulseforge client', () => {
  assert.equal(clientNameForAnchor(1), 'Pulseforge');
});
