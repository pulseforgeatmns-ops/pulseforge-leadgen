const assert = require('node:assert/strict');
const { computeDailyHealth, formatDailyHealthMessage } = require('../utils/dailyHealth');

function mockQuery(overrides = {}) {
  const data = {
    email: [{
      send_count_today: 90,
      send_count_baseline_7d: 100,
      bounce_count_today: 1,
      reply_count_today: 3,
      opened_count_today: 20,
      opened_proxy_count_today: 7,
    }],
    scout: [{ scout_prospects_added_today: 10, scout_baseline_7d: 9 }],
    warm_signals: [{ warm_signals_fired_today: 2 }],
    agent_errors: [],
    autosend: [],
    clients: [{
      client_id: 1,
      client_name: 'Pulseforge Manchester',
      active_prospect_count: 80,
      send_count_today: 90,
      send_count_baseline_7d_total: 700,
    }],
    ...overrides,
  };

  return async sql => {
    const match = sql.match(/daily_health:([a-z_]+)/);
    return { rows: match ? (data[match[1]] || []) : [] };
  };
}

async function run() {
  const now = new Date('2026-06-22T10:00:00.000Z');

  // More than 50% below a baseline over 10 triggers the strict `< -50` rule.
  const low = await computeDailyHealth({
    now,
    query: mockQuery({ email: [{ send_count_today: 49, send_count_baseline_7d: 100 }] }),
  });
  assert.ok(low.health_flags.some(flag => flag.code === 'SEND_VOLUME_LOW'));

  const bounce = await computeDailyHealth({
    now,
    query: mockQuery({ email: [{ send_count_today: 100, send_count_baseline_7d: 100, bounce_count_today: 5 }] }),
  });
  assert.ok(bounce.health_flags.some(flag => flag.code === 'BOUNCE_RATE_HIGH'));

  const dry = await computeDailyHealth({
    now,
    query: mockQuery({ scout: [{ scout_prospects_added_today: 0, scout_baseline_7d: 4 }] }),
  });
  assert.ok(dry.health_flags.some(flag => flag.code === 'SCOUT_DRY'));

  const healthy = await computeDailyHealth({ now, query: mockQuery() });
  assert.deepEqual(healthy.health_flags, [{ severity: 'green', code: 'HEALTHY', msg: 'All systems normal' }]);
  assert.equal(healthy.opened_count_today, 20);
  assert.equal(healthy.opened_proxy_count_today, 7);

  const zeroSendsWithCollapsedBaseline = await computeDailyHealth({
    now,
    query: mockQuery({
      email: [{ send_count_today: 0, send_count_baseline_7d: 0 }],
      clients: [{
        client_id: 1,
        client_name: 'Pulseforge Manchester',
        active_prospect_count: 80,
        send_count_today: 0,
        send_count_baseline_7d_total: 0,
      }],
    }),
  });
  assert.ok(zeroSendsWithCollapsedBaseline.health_flags.some(flag =>
    flag.code === 'CLIENT_DARK' && flag.severity === 'red'
  ));
  assert.ok(!zeroSendsWithCollapsedBaseline.health_flags.some(flag => flag.code === 'HEALTHY'));

  const missing = await computeDailyHealth({
    now,
    query: async () => { throw new Error('relation email_events does not exist'); },
  });
  assert.equal(missing.send_count_today, 0);
  assert.equal(missing.bounce_count_today, 0);
  assert.equal(missing.reply_count_today, 0);
  assert.equal(missing.scout_prospects_added_today, 0);
  assert.deepEqual(missing.agent_error_count_today, {});

  const formatted = formatDailyHealthMessage({
    ...healthy,
    health_flags: [
      { severity: 'red', code: 'BOUNCE_RATE_HIGH', msg: 'BOUNCE RATE HIGH: 5.0% (threshold 4%)' },
      { severity: 'yellow', code: 'CLIENT_DARK', msg: 'CLIENT DARK: MSHI (2) had 0 sends today', client_id: 2 },
    ],
    per_client_send_details: [{ client_id: 2, client_name: 'MSHI', send_count_today: 0 }],
  });
  assert.match(formatted, /🚨 BOUNCE RATE HIGH/);
  assert.match(formatted, /⚠️ CLIENT DARK/);

  console.log('Daily health tests passed');
}

run().catch(err => {
  console.error(err);
  process.exitCode = 1;
});
