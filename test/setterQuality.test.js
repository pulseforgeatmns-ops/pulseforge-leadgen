'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const {
  DISPOSITION_CONTRACTS,
  callbackSla,
  dispositionContract,
  shouldSampleCall,
  syntheticContactProhibited,
  validateStructuredNotes,
} = require('../utils/setterQuality');
const { DISPOSITION_VALUES, applyProspectDisposition } = require('../utils/callDispositions');

const root = path.join(__dirname, '..');
const migration = fs.readFileSync(path.join(root, 'migrations', '2026-07-19-setter-pilot-quality-control.sql'), 'utf8');
const rollback = fs.readFileSync(path.join(root, 'migrations', '2026-07-19-setter-pilot-quality-control.rollback.sql'), 'utf8');
const setterRoute = fs.readFileSync(path.join(root, 'routes', 'setter.js'), 'utf8');

test('every disposition has one complete activity, next-action, suppression, and lifecycle contract', () => {
  assert.deepEqual([...DISPOSITION_VALUES].sort(), Object.keys(DISPOSITION_CONTRACTS).sort());
  for (const disposition of DISPOSITION_VALUES) {
    const value = dispositionContract(disposition);
    assert.ok(value.activity, `${disposition} activity`);
    assert.equal(typeof value.connected, 'boolean', `${disposition} connected`);
    assert.equal(typeof value.decision_maker_conversation, 'boolean', `${disposition} DM conversation`);
    assert.ok(value.next_action, `${disposition} next action`);
    assert.ok(value.suppression_state, `${disposition} suppression`);
    assert.ok(value.lifecycle_result, `${disposition} lifecycle`);
  }
});

test('each disposition generates the expected prospect lifecycle mutation', async () => {
  for (const disposition of DISPOSITION_VALUES) {
    let statement;
    const db = { query: async (sql, params) => {
      statement = { sql, params };
      return { rows: [{ id: params[0], client_id: params[1] }] };
    } };
    await applyProspectDisposition(db, {
      prospectId: '11111111-1111-1111-1111-111111111111',
      clientId: 10,
      disposition,
      callbackAt: new Date('2026-07-20T14:00:00Z'),
    });
    assert.match(statement.sql, /WHERE id = \$1 AND client_id = \$2/);
    assert.match(statement.sql, /setter_updated_at = NOW\(\)/);
  }
});

test('callback SLA states are mutually exclusive at their boundaries', () => {
  const now = new Date('2026-07-19T12:00:00Z');
  assert.equal(callbackSla('2026-07-20T11:00:00Z', now), 'due_soon');
  assert.equal(callbackSla('2026-07-19T12:10:00Z', now), 'due_now');
  assert.equal(callbackSla('2026-07-19T11:40:00Z', now), 'overdue');
  assert.equal(callbackSla('2026-07-22T12:00:00Z', now), 'scheduled');
});

test('interested, callback, qualified, and disqualified outcomes require structured notes', () => {
  for (const disposition of ['answered_interested', 'answered_callback', 'qualified']) {
    assert.throws(() => validateStructuredNotes(disposition, null), { code: 'STRUCTURED_NOTES_REQUIRED' });
    const value = validateStructuredNotes(disposition, { summary: 'Spoke with owner', next_step: 'Call Tuesday' });
    assert.equal(value.summary, 'Spoke with owner');
  }
  assert.throws(() => validateStructuredNotes('disqualified', { summary: 'Not a fit' }), { code: 'STRUCTURED_NOTES_REQUIRED' });
  assert.equal(validateStructuredNotes('disqualified', { summary: 'Not a fit', reason: 'Outside service area' }).reason, 'Outside service area');
  assert.throws(() => validateStructuredNotes('answered_not_interested', { summary: 'Declined' }), { code: 'STRUCTURED_NOTES_REQUIRED' });
  assert.equal(validateStructuredNotes('no_answer', null), null);
});

test('manager review sampling is deterministic and synthetic contact is fail-closed', () => {
  const sample = shouldSampleCall({ clientId: 10, dispositionId: 42, samplePercent: 20 });
  assert.equal(shouldSampleCall({ clientId: 10, dispositionId: 42, samplePercent: 20 }), sample);
  assert.equal(shouldSampleCall({ clientId: 10, dispositionId: 42, samplePercent: 0 }), false);
  assert.equal(shouldSampleCall({ clientId: 10, dispositionId: 42, samplePercent: 100 }), true);
  assert.equal(syntheticContactProhibited({ is_synthetic: true, do_not_contact: false }), true);
});

test('schema and routes enforce one pending callback, synthetic DNC, tenant scope, and UI rollback flag', () => {
  assert.match(migration, /prospects_synthetic_suppression/);
  assert.match(migration, /setter_callbacks_one_pending_idx/);
  assert.match(migration, /setter_pipeline_v2_enabled/);
  assert.match(rollback, /setter_pipeline_v2_enabled = false/);
  assert.match(setterRoute, /client_id = \$1 AND cd\.idempotency_key = \$2/);
  assert.match(setterRoute, /COALESCE\(is_synthetic, false\) = false/);
  assert.match(setterRoute, /setter_pipeline_v2_configured_at = NOW\(\)/);
  assert.match(setterRoute, /requireRole\('admin', 'manager'\)/);
});
