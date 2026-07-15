'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
  AGENT_ACTION_PROSPECT_SQL,
  OPERATIONAL_FIELDS,
  REQUIRED_INVARIANT_COLUMNS,
  run,
  validateInvariantSources,
} = require('../scripts/smokeMaxShadow');

const PROSPECT_ID = '5128ba03-dc0b-44fe-aeb1-f9419142d3e3';

function productionColumnRows() {
  const rows = [];
  for (const [table, columns] of Object.entries(REQUIRED_INVARIANT_COLUMNS)) {
    for (const column of columns) {
      rows.push({
        table_name: table,
        column_name: column,
        data_type: table === 'agent_actions' && column === 'payload' ? 'jsonb' : 'text',
        udt_name: table === 'agent_actions' && column === 'payload' ? 'jsonb' : 'text',
      });
    }
  }
  return rows;
}

function baseProspect(overrides = {}) {
  return { ...Object.fromEntries(OPERATIONAL_FIELDS.map(field => [field, field === 'status' ? 'cold' : null])), ...overrides };
}

function baseCounts() {
  return {
    agent_actions: 3,
    cal_queue: 1,
    touchpoint_sends: 2,
    email_event_sends: 2,
    agent_log_sends: 2,
    enrichment_activity: 1,
    max_signals: 0,
    max_decisions: 0,
    max_transitions: 0,
    max_actions: 0,
  };
}

function smokeDb({ mutateStatus = false, auditActions = 2, residuals = null } = {}) {
  const state = { transaction: false, ingested: false, rolledBack: false, calls: [] };
  const countsForState = () => {
    const counts = baseCounts();
    if (state.transaction && state.ingested) {
      counts.max_signals = 1;
      counts.max_decisions = 1;
      counts.max_transitions = 1;
      counts.max_actions = 2;
    }
    return counts;
  };
  const query = async (sql, params = []) => {
    state.calls.push({ sql, params, transaction: state.transaction });
    if (/^\s*BEGIN\s*$/i.test(sql)) { state.transaction = true; return { rows: [] }; }
    if (/^\s*ROLLBACK\s*$/i.test(sql)) {
      state.transaction = false;
      state.ingested = false;
      state.rolledBack = true;
      return { rows: [] };
    }
    if (/SELECT to_jsonb\(p\) AS row/.test(sql)) {
      const changed = state.transaction && state.ingested && mutateStatus;
      return { rows: [{ row: baseProspect(changed ? { status: 'warm' } : {}) }] };
    }
    if (/FROM agent_actions aa/.test(sql) && /AS max_actions/.test(sql)) {
      return { rows: [countsForState()] };
    }
    if (/SELECT d\.id, d\.is_shadow/.test(sql)) {
      return { rows: [{ id: 'decision-1', is_shadow: true, warmth_score: 82, action_count: auditActions, all_actions_skipped: auditActions > 0 }] };
    }
    if (/AS transition_count/.test(sql)) {
      return { rows: [{ transition_count: 1, all_shadow_unapplied: true }] };
    }
    if (/WITH smoke_signals AS/.test(sql)) {
      return { rows: [residuals || { signals: 0, decisions: 0, transitions: 0, actions: 0 }] };
    }
    throw new Error(`Unexpected smoke test query: ${sql}`);
  };
  const client = { query, release() { state.released = true; } };
  return {
    state,
    async connect() { return client; },
    query,
  };
}

function dependencies(db) {
  return {
    validateSchema: async () => ({ valid: true }),
    validateInvariantSources: async () => ({ available: true }),
    loadClientOrchestrationConfig: async () => ({ max_orchestration_config: {} }),
    loadMaxOrchestrationConfig: () => ({
      enabled: true,
      flags: {
        max_scoring_enabled: true,
        max_shadow_mode: true,
        max_state_transitions_enabled: false,
        max_enrichment_actions_enabled: false,
        max_warm_sequence_enabled: false,
        max_call_tasks_enabled: false,
        max_hot_escalations_enabled: false,
        max_recycle_actions_enabled: false,
        max_sequence_actions_enabled: false,
        max_operator_tasks_enabled: false,
        max_enrichment_retry_enabled: false,
        max_prospect_actions_enabled: false,
      },
    }),
    randomUUID: () => '00000000-0000-4000-8000-000000000001',
    ingestNormalizedSignal: async () => {
      db.state.ingested = true;
      return {
        signal_id: 'signal-1',
        score: { score: 82 },
        decision: { id: 'decision-1' },
      };
    },
  };
}

test('production agent_actions fixture requires JSON payload but no prospect_id column', async () => {
  const rows = productionColumnRows();
  assert.equal(rows.some(row => row.table_name === 'agent_actions' && row.column_name === 'prospect_id'), false);
  const db = { async query() { return { rows }; } };
  const result = await validateInvariantSources(db);
  assert.equal(result.available, true);
});

test('agent action attribution uses only the confirmed top-level JSON prospect_id path', () => {
  assert.match(AGENT_ACTION_PROSPECT_SQL, /jsonb_typeof\(aa\.payload::jsonb\)/);
  assert.match(AGENT_ACTION_PROSPECT_SQL, /payload::jsonb ->> 'prospect_id'/);
  assert.match(AGENT_ACTION_PROSPECT_SQL, /ELSE NULL/);
  assert.doesNotMatch(AGENT_ACTION_PROSPECT_SQL, /aa\.prospect_id/);
  assert.doesNotMatch(AGENT_ACTION_PROSPECT_SQL, /payload::jsonb -> 'prospect'/);
});

test('missing required invariant source fails clearly instead of silently skipping', async () => {
  const rows = productionColumnRows().filter(row => row.table_name !== 'agent_actions');
  const db = { async query() { return { rows }; } };
  await assert.rejects(() => validateInvariantSources(db), /UNAVAILABLE_INVARIANT: agent_actions: table missing/);
});

test('smoke test rolls back on success and independently verifies no residual records', async () => {
  const db = smokeDb();
  const report = await run({ clientId: 10, prospectId: PROSPECT_ID }, db, {}, dependencies(db));
  assert.equal(report.valid, true);
  assert.equal(report.rolled_back, true);
  assert.equal(report.synthetic_records_remaining, 0);
  assert.equal(report.actions_skipped_with_shadow_mode, true);
  assert.equal(report.shadow_score_generated, true);
  assert.equal(db.state.rolledBack, true);
  assert.ok(db.state.calls.some(call => /^\s*ROLLBACK\s*$/i.test(call.sql)));
});

test('smoke test rolls back on assertion failure and verifies zero residual records', async () => {
  const db = smokeDb({ auditActions: 0 });
  await assert.rejects(
    () => run({ clientId: 10, prospectId: PROSPECT_ID }, db, {}, dependencies(db)),
    error => {
      assert.match(error.message, /recommended actions were not all skipped/);
      assert.equal(error.smoke_failure.rolled_back, true);
      assert.equal(error.smoke_failure.synthetic_records_remaining, 0);
      return true;
    },
  );
  assert.equal(db.state.rolledBack, true);
});

test('smoke test detects an unexpected operational mutation and still rolls back cleanly', async () => {
  const db = smokeDb({ mutateStatus: true });
  await assert.rejects(
    () => run({ clientId: 10, prospectId: PROSPECT_ID }, db, {}, dependencies(db)),
    error => {
      assert.match(error.message, /operational prospect state changed/);
      assert.equal(error.smoke_failure.state_restored, true);
      assert.equal(error.smoke_failure.synthetic_records_remaining, 0);
      return true;
    },
  );
});

test('smoke test fails if rollback leaves a synthetic signal, decision, action, or transition', async () => {
  const db = smokeDb({ residuals: { signals: 1, decisions: 1, transitions: 1, actions: 1 } });
  await assert.rejects(
    () => run({ clientId: 10, prospectId: PROSPECT_ID }, db, {}, dependencies(db)),
    /synthetic smoke records remain after rollback/,
  );
});
