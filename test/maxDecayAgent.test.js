const assert = require('node:assert/strict');
const test = require('node:test');
const { findDecayCandidates, run } = require('../maxDecayAgent');

test('decay candidate query is bounded and targets expiring windows', async () => {
  let captured;
  const db = { async query(sql, params) { captured = { sql, params }; return { rows: [] }; } };
  await findDecayCandidates(db, { clientId: 2, afterId: null, limit: 50 });
  assert.match(captured.sql, /prospect_signal_events/);
  assert.match(captured.sql, /16 days/);
  assert.match(captured.sql, /downgrade_candidate_since/);
  assert.equal(captured.params[2], 50);
});

test('dry-run decay reports hysteresis without persistence or side effects', async () => {
  const db = {
    calls: [],
    async query(sql, params = []) {
      this.calls.push({ sql, params });
      if (/SELECT p.id, p.client_id/.test(sql)) return { rows: [{ id: 'p1', client_id: 1 }] };
      return { rows: [] };
    },
  };
  const report = await run({ dry_run: true, limit: 10 }, db, {
    loadClientOrchestrationConfig: async () => ({}),
    calculateProspectShadow: async () => ({ decision: { reason_codes: ['DOWNGRADE_STABILIZING'] } }),
  });
  assert.equal(report.mode, 'dry-run');
  assert.equal(report.downgrade_candidates, 1);
  assert.deepEqual(report.side_effects, { status_updates: 0, messages: 0, sequence_changes: 0, enrichment_retries: 0, tasks: 0 });
  assert.equal(db.calls.some(call => /INSERT|UPDATE/.test(call.sql)), false);
});
