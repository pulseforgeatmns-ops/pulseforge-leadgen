const assert = require('node:assert/strict');
const test = require('node:test');
const { DECAY_LOCK_KEY, DECAY_LOCK_NAMESPACE, findDecayCandidates, parseArgs, run } = require('../maxDecayAgent');

function lockHarness() {
  let held = false;
  const calls = [];
  return {
    calls,
    get held() { return held; },
    client: {
      async query(sql, params) {
        calls.push({ sql, params });
        if (/pg_try_advisory_lock/.test(sql)) {
          if (held) return { rows: [{ locked: false }] };
          held = true;
          return { rows: [{ locked: true }] };
        }
        if (/pg_advisory_unlock/.test(sql)) {
          const unlocked = held;
          held = false;
          return { rows: [{ unlocked }] };
        }
        return { rows: [] };
      },
    },
  };
}

test('decay candidate query is bounded and targets expiring windows', async () => {
  let captured;
  const db = { async query(sql, params) { captured = { sql, params }; return { rows: [] }; } };
  await findDecayCandidates(db, { clientId: 2, afterId: null, limit: 50 });
  assert.match(captured.sql, /prospect_signal_events/);
  assert.match(captured.sql, /16 days/);
  assert.match(captured.sql, /downgrade_candidate_since/);
  assert.equal(captured.params[2], 50);
});

test('scheduled decay resume is explicit and apply remains explicit', () => {
  assert.deepEqual(parseArgs(['--apply','--resume','--client-id=10','--limit=250']), {
    client_id: 10, after_id: null, limit: 250, dry_run: false, resume: true,
  });
});

test('dry-run decay reports hysteresis without persistence or side effects', async () => {
  const lock = lockHarness();
  const events = [];
  const db = {
    calls: [],
    async query(sql, params = []) {
      this.calls.push({ sql, params });
      if (/SELECT p.id, p.client_id/.test(sql)) return { rows: [{ id: 'p1', client_id: 1, warmth_score: 1 }] };
      return { rows: [] };
    },
  };
  const report = await run({ dry_run: true, limit: 10 }, db, {
    loadClientOrchestrationConfig: async () => ({}),
    calculateProspectShadow: async () => ({ scoreResult: { score: 1 }, decision: { reason_codes: ['DOWNGRADE_STABILIZING'] } }),
    lockClient: lock.client,
    recordDecayRunEvent: async (_db, event) => events.push(event),
  });
  assert.equal(report.mode, 'dry-run');
  assert.equal(report.downgrade_candidates, 1);
  assert.deepEqual(report.side_effects, { status_updates: 0, messages: 0, sequence_changes: 0, enrichment_retries: 0, tasks: 0 });
  assert.equal(db.calls.some(call => /UPDATE prospects|INSERT INTO max_decisions|INSERT INTO max_actions/.test(call.sql)), false);
  assert.equal(events.at(-1).status, 'completed');
  assert.equal(events.at(-1).details.lock_released, true);
});

test('global advisory lock prevents concurrent client or batch runs and records overlap', async () => {
  const lock = lockHarness();
  const events = [];
  let unblock;
  let started;
  const startedPromise = new Promise(resolve => { started = resolve; });
  const blocker = new Promise(resolve => { unblock = resolve; });
  const db = { async query(sql) {
    if (/SELECT p.id, p.client_id/.test(sql)) return { rows: [{ id: 'p1', client_id: 10, warmth_score: 0 }] };
    return { rows: [] };
  } };
  const dependencies = {
    lockClient: lock.client,
    recordDecayRunEvent: async (_db, event) => events.push(event),
    loadClientOrchestrationConfig: async () => ({}),
    calculateProspectShadow: async () => { started(); await blocker; return { scoreResult: { score: 0 }, decision: { reason_codes: [] } }; },
  };
  const first = run({ dry_run: true, client_id: 10, limit: 10 }, db, dependencies);
  await startedPromise;
  const second = await run({ dry_run: true, client_id: 1, limit: 20 }, db, dependencies);
  assert.equal(second.status, 'skipped_overlap');
  assert.equal(second.lock_acquired, false);
  assert.equal(events.some(event => event.status === 'skipped_overlap'), true);
  unblock();
  const completed = await first;
  assert.equal(completed.status, 'completed');
  assert.equal(lock.held, false);
  assert.deepEqual(lock.calls[0].params, [DECAY_LOCK_NAMESPACE, DECAY_LOCK_KEY]);
});

test('lock releases after prospect failure and a later run can recover', async () => {
  const lock = lockHarness();
  const events = [];
  const db = { async query(sql) {
    if (/SELECT p.id, p.client_id/.test(sql)) return { rows: [{ id: 'p1', client_id: 10, warmth_score: 0 }] };
    return { rows: [] };
  } };
  const base = {
    lockClient: lock.client,
    recordDecayRunEvent: async (_db, event) => events.push(event),
    loadClientOrchestrationConfig: async () => ({}),
  };
  const failed = await run({ dry_run: true, limit: 10 }, db, {
    ...base, calculateProspectShadow: async () => { throw Object.assign(new Error('fixture failure'), { code: 'FIXTURE' }); },
  });
  assert.equal(failed.status, 'failed');
  assert.equal(lock.held, false);
  const recovered = await run({ dry_run: true, limit: 10 }, db, {
    ...base, calculateProspectShadow: async () => ({ scoreResult: { score: 0 }, decision: { reason_codes: [] } }),
  });
  assert.equal(recovered.status, 'completed');
  assert.equal(lock.held, false);
  const failureEvent = events.find(event => event.status === 'failed');
  assert.equal(failureEvent.retryable, false);
  assert.match(failureEvent.error_summary, /1 decay evaluation/);
});

test('resume cursor comes from completed history and wraps after a full sweep', async () => {
  const lock = lockHarness();
  const cursor = '1000c166-c9c3-4bab-adef-d4cbdf14ab18';
  const candidateQueries = [];
  const db = { async query(sql, params = []) {
    if (/SELECT end_cursor/.test(sql)) return { rows: [{ end_cursor: cursor }] };
    if (/SELECT p.id, p.client_id/.test(sql)) {
      candidateQueries.push(params);
      if (params[1]) return { rows: [] };
      return { rows: [{ id: '2000c166-c9c3-4bab-adef-d4cbdf14ab18', client_id: 10, warmth_score: 0 }] };
    }
    return { rows: [] };
  } };
  const events = [];
  const result = await run({ dry_run: true, resume: true, client_id: 10, limit: 10 }, db, {
    lockClient: lock.client,
    recordDecayRunEvent: async (_db, event) => events.push(event),
    loadClientOrchestrationConfig: async () => ({}),
    calculateProspectShadow: async () => ({ scoreResult: { score: 0 }, decision: { reason_codes: [] } }),
  });
  assert.equal(result.start_cursor, cursor);
  assert.equal(result.cursor_wrapped, true);
  assert.equal(candidateQueries.length, 2);
  assert.equal(candidateQueries[0][1], cursor);
  assert.equal(candidateQueries[1][1], null);
  assert.equal(events.at(-1).details.cursor_wrapped, true);
});
