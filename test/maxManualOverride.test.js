const assert = require('node:assert/strict');
const test = require('node:test');
const { applyManualLifecycleOverride } = require('../utils/maxManualOverride');

function overrideDb(state = 'warm') {
  const calls = [];
  return {
    calls,
    async query(sql, params = []) {
      calls.push({ sql, params });
      if (/SELECT id, client_id, company_id/.test(sql)) {
        return { rows: [{ id: 'p1', client_id: 1, company_id: null, lifecycle_state: state, status: 'cold', do_not_contact: false }] };
      }
      return { rows: [] };
    },
  };
}

test('manual override updates canonical lifecycle only and writes immutable audits', async () => {
  const db = overrideDb('warm');
  const result = await applyManualLifecycleOverride({
    db, prospectId: 'p1', clientId: 1, requestedState: 'engaged',
    reason: 'Operator confirmed an active conversation', operator: { id: 7, email: 'operator@example.com' },
    now: new Date('2026-07-15T12:00:00Z'),
  });
  assert.equal(result.lifecycle_state, 'engaged');
  assert.ok(db.calls.some(call => /INSERT INTO manual_lifecycle_overrides/.test(call.sql)));
  assert.ok(db.calls.some(call => /INSERT INTO prospect_state_transitions/.test(call.sql)));
  const update = db.calls.find(call => /UPDATE prospects/.test(call.sql));
  assert.match(update.sql, /lifecycle_state/);
  assert.doesNotMatch(update.sql, /\bstatus\s*=/);
  assert.equal(db.calls.some(call => /email_sent|agent_actions|cal_queue/.test(call.sql)), false);
});

test('terminal restoration requires explicit confirmation and reason', async () => {
  const db = overrideDb('disqualified');
  await assert.rejects(() => applyManualLifecycleOverride({
    db, prospectId: 'p1', clientId: 1, requestedState: 'warm', reason: 'Valid restore reason',
  }), /confirm_terminal_restore/);
  await assert.rejects(() => applyManualLifecycleOverride({
    db: overrideDb('warm'), prospectId: 'p1', clientId: 1, requestedState: 'hot', reason: 'no',
  }), /at least 5 characters/);
});
