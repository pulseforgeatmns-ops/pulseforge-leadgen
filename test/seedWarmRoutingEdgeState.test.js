const assert = require('node:assert/strict');
const test = require('node:test');

const dbPath = require.resolve('../db');
const seedPath = require.resolve('../scripts/seedWarmRoutingEdgeState');
const queries = [];

const client = {
  async query(sql, params = []) {
    queries.push({ sql, params });
    if (/projected_first_run_fires/.test(sql) && /WITH effective_touch/.test(sql)) {
      return { rows: [{ projected_first_run_fires: 0, leaks: [] }] };
    }
    return { rows: [] };
  },
  release() {},
};

require.cache[dbPath] = {
  id: dbPath,
  filename: dbPath,
  loaded: true,
  exports: {
    async connect() {
      return client;
    },
  },
};

delete require.cache[seedPath];
const { seedWarmRoutingEdgeState, SEED_VERSION } = require('../scripts/seedWarmRoutingEdgeState');

test.beforeEach(() => {
  queries.length = 0;
  process.env.WARM_ROUTING_ENABLED = '';
  process.env.WARM_ROUTING_SEED_CONFIRM = SEED_VERSION;
});

test('seed maps a missing eligibility touch to is_active=false, never NULL', async () => {
  await seedWarmRoutingEdgeState();

  const eligibilityInsert = queries.find(({ sql }) =>
    /INSERT INTO warm_signal_state/.test(sql) && /'ICP_ELIGIBILITY'/.test(sql)
  );

  assert.ok(eligibilityInsert, 'expected the ICP_ELIGIBILITY seed insert');
  assert.match(eligibilityInsert.sql, /COALESCE\([\s\S]*touch\.latest_touch[\s\S]*FALSE[\s\S]*\)/);
  assert.match(eligibilityInsert.sql, /signal_type, is_active, last_observed_value/);
});
