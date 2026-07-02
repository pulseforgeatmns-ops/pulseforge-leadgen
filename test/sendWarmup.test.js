const assert = require('assert');
const {
  getWarmupProgress,
  resolveWarmupDailyCap,
} = require('../utils/sendWarmup');

const pulseforgeStages = [
  { afterSendDays: 0, dailyCap: 10 },
  { afterSendDays: 2, dailyCap: 15 },
  { afterSendDays: 4, dailyCap: 25 },
  { afterSendDays: 22, dailyCap: 100 },
];
assert.strictEqual(resolveWarmupDailyCap(pulseforgeStages, 0), 10);
assert.strictEqual(resolveWarmupDailyCap(pulseforgeStages, 1), 10);
assert.strictEqual(resolveWarmupDailyCap(pulseforgeStages, 2), 15);
assert.strictEqual(resolveWarmupDailyCap(pulseforgeStages, 5), 25);
assert.strictEqual(resolveWarmupDailyCap(pulseforgeStages, 22), 100);

async function run() {
  const now = new Date('2026-07-02T14:00:00Z');
  const stalePool = {
    async query() {
      return {
        rows: [{
          last_sent_at: '2026-06-19T14:00:00Z',
          restarted_at: '2026-06-01T14:00:00Z',
          active_send_days: 12,
        }],
      };
    },
  };
  const reset = await getWarmupProgress(stalePool, 1, 7, now);
  assert.strictEqual(reset.reset, true);
  assert.strictEqual(reset.activeSendDays, 0);

  const activePool = {
    async query() {
      return {
        rows: [{
          last_sent_at: '2026-06-30T14:00:00Z',
          restarted_at: '2026-06-30T14:00:00Z',
          active_send_days: 1,
        }],
      };
    },
  };
  const active = await getWarmupProgress(activePool, 10, 7, now);
  assert.strictEqual(active.reset, false);
  assert.strictEqual(active.activeSendDays, 1);

  console.log('sendWarmup tests passed');
}

run().catch(err => {
  console.error(err);
  process.exit(1);
});
