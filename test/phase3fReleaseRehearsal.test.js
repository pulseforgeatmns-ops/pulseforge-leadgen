'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { allPhase3dObjectsPresent, counts, phase3dObjectState } = require('../scripts/runSetterReleaseRehearsal');

function state({ phase3d = false } = {}) {
  const tables = ['clients', 'prospects', 'call_dispositions', 'setter_follow_up_drafts'];
  if (phase3d) tables.push('setter_callbacks');
  const columns = [
    { table_name: 'prospects', column_name: 'do_not_contact' },
    ...(phase3d ? [
      ...['setter_pipeline_v2_enabled', 'setter_pipeline_v2_configured_at', 'setter_review_sample_percent'].map(column_name => ({ table_name: 'clients', column_name })),
      ...['is_synthetic', 'synthetic_label', 'callback_completed_at', 'assigned_setter_id'].map(column_name => ({ table_name: 'prospects', column_name })),
      ...['structured_notes', 'activity_result', 'next_action', 'suppression_state', 'lifecycle_result', 'is_synthetic', 'review_required', 'review_status', 'idempotency_key'].map(column_name => ({ table_name: 'call_dispositions', column_name })),
    ] : []),
  ];
  return {
    tables: tables.map(table_name => ({ table_name })),
    columns,
    indexes: phase3d ? ['call_dispositions_idempotency_idx', 'setter_callbacks_one_pending_idx', 'setter_callbacks_due_idx'].map(indexname => ({ indexname })) : [],
    triggers: phase3d ? ['prospects_synthetic_suppression', 'prospects_suppression_cleanup'].map(trigger_name => ({ trigger_name })) : [],
    constraints: phase3d ? [{ constraint_name: 'setter_callbacks_status_check', constraint_type: 'CHECK' }] : [],
  };
}

test('pre-Phase 3D snapshots record missing setter tables without querying them', async () => {
  const preMigration = state();
  const queried = [];
  const db = { query: async sql => {
    queried.push(sql);
    if (/setter_callbacks/.test(sql)) throw new Error('pre-migration setter_callbacks must not be queried');
    if (/FROM prospects WHERE do_not_contact/.test(sql)) return { rows: [{ count: 0 }] };
    return { rows: [{ count: 3 }] };
  } };
  const result = await counts(db, preMigration);
  assert.deepEqual(result.tables.setter_callbacks, { exists: false, row_count: null });
  assert.deepEqual(result.prospect_filters.synthetic_prospects, { exists: false, row_count: null });
  assert.equal(queried.some(sql => /setter_callbacks/.test(sql)), false);
  assert.equal(phase3dObjectState(preMigration).tables.setter_callbacks, false);
});

test('forward, logical rollback, and reapply snapshots preserve the intended Phase 3D schema contract', () => {
  const before = phase3dObjectState(state());
  const forward = phase3dObjectState(state({ phase3d: true }));
  const rollback = phase3dObjectState(state({ phase3d: true }));
  const reapply = phase3dObjectState(state({ phase3d: true }));
  assert.equal(before.tables.setter_callbacks, false);
  assert.equal(allPhase3dObjectsPresent(forward), true);
  // The approved rollback is a UI/feature-flag rollback and intentionally
  // retains safety/history schema rather than dropping operational records.
  assert.equal(allPhase3dObjectsPresent(rollback), true);
  assert.equal(allPhase3dObjectsPresent(reapply), true);
});
