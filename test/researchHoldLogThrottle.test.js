'use strict';
// SPEC-SCOUT-P0-005: bounded research-hold diagnostics (acceptance 8 and 9).
const test = require('node:test'), assert = require('node:assert/strict');
const { createResearchHoldLogThrottle, HEARTBEAT_MS } = require('../services/researchHoldLogThrottle');
const {
  dispatchGovernedOutboundControl, whenControlDispatchSettled, resetControlDispatchForTests,
} = require('../services/governedOutboundControlDispatch');

const MISMATCH = d => ({ halted: 'research_only_revision_mismatch', researchOnly: { status: 'revision_mismatch', diagnostic: d } });
const D1 = { grantId: 'a'.repeat(64), pinnedRevision: '1'.repeat(40), deployedRevision: '2'.repeat(40) };

test('heartbeat interval is exactly 60 minutes', () => assert.equal(HEARTBEAT_MS, 3600000));

test('unchanged mismatch: one initial diagnostic, then one heartbeat per interval with the suppressed count', () => {
  let t = 0; const decide = createResearchHoldLogThrottle({ now: () => t });
  assert.deepEqual(decide('10', MISMATCH(D1)), { emit: true, kind: 'initial', suppressed: 0 });
  // Both production schedulers tick roughly every 7.5 minutes combined.
  let emitted = 0;
  for (let i = 1; i < 8; i++) { t = i * 450000; if (decide('10', MISMATCH(D1)).emit) emitted++; }
  assert.equal(emitted, 0, 'no per-tick logging inside the interval');
  t = HEARTBEAT_MS - 1; assert.equal(decide('10', MISMATCH(D1)).emit, false, 'one millisecond early is still suppressed');
  t = HEARTBEAT_MS; assert.deepEqual(decide('10', MISMATCH(D1)), { emit: true, kind: 'heartbeat', suppressed: 8 });
  t = HEARTBEAT_MS + 1; assert.equal(decide('10', MISMATCH(D1)).emit, false);
  t = 2 * HEARTBEAT_MS; assert.equal(decide('10', MISMATCH(D1)).kind, 'heartbeat');
});

test('a changed grant, pinned revision, deployed revision or reason emits a fresh diagnostic immediately', () => {
  let t = 0; const decide = createResearchHoldLogThrottle({ now: () => t });
  decide('10', MISMATCH(D1)); t = 1000;
  assert.equal(decide('10', MISMATCH({ ...D1, deployedRevision: '3'.repeat(40) })).kind, 'initial', 'new deploy');
  assert.equal(decide('10', MISMATCH({ ...D1, deployedRevision: '3'.repeat(40), grantId: 'b'.repeat(64) })).kind, 'initial', 'successor grant');
  assert.equal(decide('10', MISMATCH({ ...D1, pinnedRevision: '4'.repeat(40) })).kind, 'initial', 'different pin');
  assert.equal(decide('10', { halted: 'research_only_awaiting_cycle_release', researchOnly: {} }).kind, 'initial', 'different reason');
});

test('any non-steady outcome is always logged and restarts the sequence', () => {
  let t = 0; const decide = createResearchHoldLogThrottle({ now: () => t });
  decide('10', MISMATCH(D1)); t = 1;
  assert.deepEqual(decide('10', { halted: null }), { emit: true, kind: 'event', suppressed: 0 });
  assert.deepEqual(decide('10', { halted: 'research_only_claim_failed' }), { emit: true, kind: 'event', suppressed: 0 });
  assert.equal(decide('10', MISMATCH(D1)).kind, 'initial', 'a recurrence after a change is reported again');
  assert.equal(decide('11', MISMATCH(D1)).kind, 'initial', 'tenants are independent');
});

test('two scheduler paths share one throttle, and every tick still runs the control loop', async () => {
  resetControlDispatchForTests({ now: () => 0 });
  const locks = new Set();
  const pool = { async connect() { return { async query(sql, a) {
    if (sql.includes('pg_try_advisory_lock')) { const k = a.join(':'); if (locks.has(k)) return { rows: [{ locked: false }] }; locks.add(k); return { rows: [{ locked: true }] }; }
    if (sql.includes('pg_advisory_unlock')) { locks.delete(a.join(':')); return { rows: [] }; }
    throw Error(sql); }, release() {} }; } };
  const lines = []; const logger = { log: (tag, body) => { if (tag.includes('cycle finished')) lines.push(JSON.parse(body)); }, error() {} };
  let runs = 0;
  const runControl = async () => { runs++; return MISMATCH(D1); };
  for (let i = 0; i < 6; i++) {
    // Alternate "HTTP cron" and "in-process clock" callers of the same dispatcher.
    await dispatchGovernedOutboundControl({ pool, logger, runControl, tenantIds: ['10'], execute: true });
    await whenControlDispatchSettled();
  }
  assert.equal(runs, 6, 'log throttling never skips an authorization-checking control run');
  assert.equal(lines.length, 1);
  assert.equal(lines[0].holdDiagnostic, 'initial');
  assert.deepEqual(lines[0].diagnostic, D1);
  resetControlDispatchForTests();
});
