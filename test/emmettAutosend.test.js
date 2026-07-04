const assert = require('node:assert/strict');
const {
  autorun,
  businessDaysInclusive,
  envEnabled,
  isBusinessWindow,
  rampCap,
} = require('../utils/emmettAutosend');

async function run() {
  assert.equal(envEnabled('true'), true);
  assert.equal(envEnabled('ON'), true);
  assert.equal(envEnabled(undefined), false);

  assert.equal(businessDaysInclusive('2026-07-06', '2026-07-06'), 1);
  assert.equal(businessDaysInclusive('2026-07-03', '2026-07-06'), 2);
  assert.equal(businessDaysInclusive('2026-07-04', '2026-07-05'), 0);

  const stages = [
    { business_day_start: 1, daily_cap: 5 },
    { business_day_start: 6, daily_cap: 10 },
    { business_day_start: 11, daily_cap: 20 },
    { business_day_start: 16, daily_cap: 35 },
    { business_day_start: 21, daily_cap: 50 },
  ];
  assert.equal(rampCap(stages, 1), 5);
  assert.equal(rampCap(stages, 10), 10);
  assert.equal(rampCap(stages, 21), 50);

  assert.equal(isBusinessWindow(new Date('2026-07-06T13:00:00Z')), true); // 9am ET
  assert.equal(isBusinessWindow(new Date('2026-07-06T20:00:00Z')), false); // 4pm ET
  assert.equal(isBusinessWindow(new Date('2026-07-05T14:00:00Z')), false); // Sunday

  const logged = [];
  const query = async (sql, params = []) => {
    if (/^SELECT id FROM clients/.test(sql.trim())) return { rows: [{ id: 10 }] };
    if (/SELECT id, active, autosend_enabled/.test(sql)) {
      return { rows: [{ id: 10, active: true, autosend_enabled: true, warmup_start_date: null }] };
    }
    if (/UPDATE clients\s+SET warmup_start_date/.test(sql)) {
      return { rows: [{ warmup_start_date: '2026-07-06' }] };
    }
    if (/SELECT business_day_start, daily_cap/.test(sql)) return { rows: stages };
    if (/AS sent_today/.test(sql)) return { rows: [{ sent_today: 1, failed_today: 0 }] };
    if (/AS bounced_today/.test(sql)) return { rows: [{ bounced_today: 0 }] };
    if (/SELECT p.email/.test(sql)) {
      return { rows: [{ email: 'one@example.net' }, { email: 'two@example.net' }] };
    }
    if (/SELECT COUNT\(\*\)::int AS count/.test(sql)) return { rows: [{ count: 3 }] };
    if (/INSERT INTO agent_log/.test(sql)) {
      logged.push(JSON.parse(params[0]));
      return { rows: [] };
    }
    return { rows: [] };
  };
  const lockClient = {
    async query(sql) {
      if (sql.includes('pg_try_advisory_lock')) return { rows: [{ locked: true }] };
      return { rows: [{ pg_advisory_unlock: true }] };
    },
  };
  const result = await autorun(10, {
    now: new Date('2026-07-06T14:00:00Z'),
    globalEnabled: true,
    query,
    lockClient,
    runChild: async context => {
      assert.equal(context.maxSends, 3);
      assert.equal(context.dailyCapOverride, 5);
      assert.deepEqual(context.targetEmails, ['one@example.net', 'two@example.net']);
      return { successes: 2, skipped: 1 };
    },
  });
  assert.deepEqual(result, {
    client_id: 10,
    sent: 2,
    skipped: 1,
    cap: 5,
    sent_today_after: 3,
    halted_reason: null,
  });
  assert.equal(logged.at(-1).cap, 5);

  const safetyQuery = ({ sentToday, bouncedToday }) => async (sql) => {
    if (/^SELECT id FROM clients/.test(sql.trim())) return { rows: [{ id: 10 }] };
    if (/SELECT id, active, autosend_enabled/.test(sql)) {
      return { rows: [{ id: 10, active: true, autosend_enabled: true, warmup_start_date: '2026-07-06' }] };
    }
    if (/SELECT business_day_start, daily_cap/.test(sql)) return { rows: stages };
    if (/AS sent_today/.test(sql)) return { rows: [{ sent_today: sentToday, failed_today: 0 }] };
    if (/AS bounced_today/.test(sql)) return { rows: [{ bounced_today: bouncedToday }] };
    return { rows: [] };
  };
  const neverRunChild = async () => { throw new Error('send child should not run'); };
  const capReached = await autorun(10, {
    now: new Date('2026-07-06T14:00:00Z'),
    globalEnabled: true,
    query: safetyQuery({ sentToday: 5, bouncedToday: 0 }),
    lockClient,
    runChild: neverRunChild,
  });
  assert.equal(capReached.halted_reason, 'cap_reached');

  const bounceBreaker = await autorun(10, {
    now: new Date('2026-07-06T14:00:00Z'),
    globalEnabled: true,
    query: safetyQuery({ sentToday: 10, bouncedToday: 1 }),
    lockClient,
    runChild: neverRunChild,
  });
  assert.equal(bounceBreaker.halted_reason, 'bounce_breaker');

  console.log('Emmett autosend tests passed');
}

run().catch(err => {
  console.error(err);
  process.exitCode = 1;
});
