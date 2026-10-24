'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');
const {
  classifyProvenUnsent,
  reconcileUncertainItemFromEvidence,
} = require('../services/governedUncertainSendReconciliation');

const ITEM = {
  id: 'daily_fixture_0', tenant_id: '10', envelope_id: 'daily_fixture',
  program_id: 'program_fixture', mission_id: 'mission_fixture',
  status: 'uncertain', reason: 'provider_acceptance_unknown',
  provider_message_id: null, candidate_id: 'prospect_fixture',
  prospect_id: 'prospect_fixture', email: 'fixture@example.invalid',
  attempted_at: '2026-10-16T17:08:03.000Z',
  attempt_token: '2026-10-16 17:08:03+00',
};
const currentAttemptProof = item => ({ payload: {
  itemId: item.id, envelopeId: item.envelope_id, attemptToken: item.attempt_token,
  providerBoundaryCrossed: false, reason: 'ak_observed_provenance_required',
} });
const emptyEvidence = () => ({
  schedules: [], mailboxMessages: [], executions: [], events: [],
  emmettReservation: [], tickBlocks: [],
});

function readOnlyPool(item) {
  const calls = [];
  return {
    calls,
    async query(sql) {
      calls.push(String(sql));
      assert.match(String(sql).trim(), /^SELECT\b/i, 'inspection must issue only SELECTs');
      if (/to_regclass/.test(sql)) return { rows: [{ control_table: null }] };
      if (/FROM acquisition_outbound_items i/.test(sql)) {
        assert.match(sql, /e\.tenant_id\s*=\s*i\.tenant_id/, 'envelope join must preserve tenant scope');
        assert.match(sql, /i\.tenant_id\s*=\s*\$2/, 'item lookup must bind the requested tenant');
      }
      return { rows: /FROM acquisition_outbound_items i/.test(sql) ? (item ? [item] : [])
        : /WHERE tenant_id = \$1 AND item_id = \$2/.test(sql) && item ? [currentAttemptProof(item)] : [] };
    },
    async connect() { assert.fail('inspection must not acquire a mutation lock'); },
    async end() {},
  };
}

function loadScript(relativePath, mocks = {}) {
  const filename = path.resolve(__dirname, '..', relativePath);
  const nativeRequire = createRequire(filename);
  const module = { exports: {} };
  const mockedRequire = id => Object.hasOwn(mocks, id) ? mocks[id] : nativeRequire(id);
  vm.runInNewContext(fs.readFileSync(filename, 'utf8'), {
    module, exports: module.exports, require: mockedRequire,
    process: { env: {}, argv: [] }, console,
  }, { filename });
  return module.exports;
}

for (const reason of ['ak_observed_provenance_required', 'provider_acceptance_unknown', 'provider_or_persistence_error']) {
  test(`canonical dry-run is SELECT-only for ${reason}`, async () => {
    const item = { ...ITEM, reason };
    const pool = readOnlyPool(item);
    const result = await reconcileUncertainItemFromEvidence(pool, '10', item.id, { dryRun: true });
    assert.equal(result.dryRun, true);
    assert.equal(result.applied, false);
    assert.equal(result.classification.outcome, reason === 'ak_observed_provenance_required' ? 'PROVEN_UNSENT' : 'UNKNOWN');
    assert.equal(pool.calls.length, 8);
  });
}

test('dry-run leaves missing and already-reconciled items untouched', async () => {
  for (const item of [null, { ...ITEM, status: 'pending', reason: 'reconciled_not_sent' }]) {
    const pool = readOnlyPool(item);
    const result = await reconcileUncertainItemFromEvidence(pool, '10', ITEM.id, { dryRun: true });
    assert.equal(result.skipped, true);
    assert.equal(pool.calls.length, 1);
  }
});

for (const reason of ['provider_acceptance_unknown', 'provider_or_persistence_error']) {
  test(`${reason} stays unknown despite a nearby unrelated tenant failure`, () => {
    const evidence = emptyEvidence();
    evidence.tickBlocks.push({ payload: { reason: 'ak_observed_provenance_required' } });
    evidence.events.push({ payload: { reason: 'ak_observed_provenance_required' } });
    assert.equal(classifyProvenUnsent({ ...ITEM, reason }, evidence).outcome, 'UNKNOWN');
  });
}

test('a tenant-wide tick failure cannot prove non-acceptance for an unbound item', () => {
  const evidence = emptyEvidence();
  evidence.tickBlocks.push({ payload: { reason: 'ak_observed_provenance_required' } });
  assert.equal(classifyProvenUnsent({ ...ITEM, reason: '' }, evidence).outcome, 'UNKNOWN');
  evidence.tickBlocks[0].payload.itemId = ITEM.id;
  evidence.tickBlocks[0].payload.envelopeId = ITEM.envelope_id;
  assert.equal(classifyProvenUnsent({ ...ITEM, reason: '' }, evidence).outcome, 'UNKNOWN');
  evidence.tickBlocks[0].payload.attemptToken = ITEM.attempt_token;
  evidence.tickBlocks[0].payload.providerBoundaryCrossed = false;
  assert.equal(classifyProvenUnsent({ ...ITEM, reason: '' }, evidence).outcome, 'PROVEN_UNSENT');
});

test('old-attempt pre-provider events cannot release a later abandoned or database-error attempt', () => {
  for (const reason of ['abandoned_attempt', '23502', 'ak_observed_provenance_required']) {
    const evidence = emptyEvidence();
    evidence.events.push({ ...currentAttemptProof(ITEM), created_at: '2026-10-16T12:00:00Z' });
    const later = { ...ITEM, reason, attempted_at: '2026-10-16T18:00:00Z', attempt_token: '2026-10-16 18:00:00+00' };
    assert.equal(classifyProvenUnsent(later, evidence).outcome, 'UNKNOWN');
  }
});

test('bare pre-provider reason and unversioned same-item events are not current-attempt proof', () => {
  const evidence = emptyEvidence();
  evidence.events.push({ payload: { itemId: ITEM.id, envelopeId: ITEM.envelope_id, reason: 'ak_observed_provenance_required' } });
  assert.equal(classifyProvenUnsent({ ...ITEM, reason: 'ak_observed_provenance_required' }, evidence).outcome, 'UNKNOWN');
});

test('a durable mailbox receipt holds every mailbox status', () => {
  for (const status of ['failed', 'queued', 'pending', 'sent']) {
    const evidence = emptyEvidence();
    evidence.events.push(currentAttemptProof(ITEM));
    evidence.mailboxMessages.push({ status, provider_message_id: 'provider-accepted-receipt' });
    assert.equal(classifyProvenUnsent({ ...ITEM, reason: 'ak_observed_provenance_required' }, evidence).outcome, 'SKIP');
  }
});

test('canonical sent_at prevents release even when status is failed and provider ID is missing', () => {
  const evidence = emptyEvidence();
  evidence.events.push(currentAttemptProof(ITEM));
  evidence.executions.push({ status: 'failed', provider_message_id: null, sent_at: '2026-10-16T17:08:04Z' });
  assert.equal(classifyProvenUnsent({ ...ITEM, reason: 'ak_observed_provenance_required' }, evidence).outcome, 'SKIP');
});

test('provider acceptance evidence never becomes retryable', () => {
  for (const evidence of [
    { ...emptyEvidence(), executions: [{ status: 'sent', provider_message_id: 'receipt' }] },
    { ...emptyEvidence(), mailboxMessages: [{ status: 'sent', provider_message_id: 'receipt' }] },
    { ...emptyEvidence(), schedules: [{ status: 'SENT', outbound_message_id: 'message' }] },
  ]) {
    assert.equal(classifyProvenUnsent({ ...ITEM, reason: 'ak_observed_provenance_required' }, evidence).outcome, 'SKIP');
  }
});

test('a stored database SQLSTATE cannot establish which side of the provider boundary failed', () => {
  assert.equal(classifyProvenUnsent({ ...ITEM, reason: '23502' }, emptyEvidence()).outcome, 'UNKNOWN');
  const evidence = emptyEvidence();
  evidence.tickBlocks.push({ payload: { itemId: ITEM.id, envelopeId: ITEM.envelope_id, reason: '23502' } });
  assert.equal(classifyProvenUnsent({ ...ITEM, reason: '' }, evidence).outcome, 'UNKNOWN');
});

test('an item-bound provider receipt event blocks release despite a pre-provider reason', () => {
  const evidence = emptyEvidence();
  evidence.events.push({ payload: { providerMessageId: 'durable-provider-receipt' } });
  assert.equal(classifyProvenUnsent({ ...ITEM, reason: 'ak_observed_provenance_required' }, evidence).outcome, 'SKIP');
});

test('an item-bound crossed-boundary event keeps contradictory pre-provider reasons unknown', () => {
  const evidence = emptyEvidence();
  evidence.events.push({ payload: { providerBoundaryCrossed: true } });
  assert.equal(classifyProvenUnsent({ ...ITEM, reason: 'ak_observed_provenance_required' }, evidence).outcome, 'UNKNOWN');
});

test('legacy Anchor inspection is SELECT-only and empty receipts remain unknown', async () => {
  const pool = readOnlyPool({ ...ITEM, provider_message_id: null });
  const script = loadScript('scripts/reconcileAnchorUncertainSend.js', {
    '../db': pool, axios: { get() { assert.fail('no provider request without a key'); } },
    dotenv: { config() {} },
  });
  const result = await script.run(['--confirm-production']);
  assert.equal(result.applied, false);
  assert.equal(result.providerOutcome, 'PROVIDER_STATE_UNKNOWN');
  assert.equal(pool.calls.length, 3);
});

test('legacy apply rejects missing operator before reads or evidence writes', async () => {
  const pool = readOnlyPool(ITEM);
  const script = loadScript('scripts/reconcileAnchorUncertainSend.js', { '../db': pool, dotenv: { config() {} } });
  await assert.rejects(script.run(['--confirm-production', '--apply']), /operator is required/);
  assert.equal(pool.calls.length, 0);
});

test('legacy apply refuses unknown acceptance without writing evidence', async () => {
  const pool = readOnlyPool(ITEM);
  const script = loadScript('scripts/reconcileAnchorUncertainSend.js', { '../db': pool, dotenv: { config() {} } });
  await assert.rejects(script.run(['--confirm-production', '--apply', '--operator', 'operator_fixture']),
    error => error.code === 'provider_outcome_blocks_apply');
  assert.equal(pool.calls.length, 3);
});

test('canonical CLI defaults to inspection, requires explicit apply, and rejects conflicting modes', async () => {
  const pool = readOnlyPool(ITEM);
  const script = loadScript('scripts/reconcileGovernedUncertainFromEvidence.js', { '../db': pool, dotenv: { config() {} } });
  assert.equal(script.parseArgs(['--tenant', '10', '--item', ITEM.id]).dryRun, true);
  assert.equal(script.parseArgs(['--tenant', '10', '--item', ITEM.id, '--apply']).dryRun, false);
  assert.throws(() => script.parseArgs(['--dry-run', '--apply']), error => error.code === 'conflicting_modes');
  const result = await script.run(['--tenant', '10', '--item', ITEM.id]);
  assert.equal(result.applied, false);
  assert.equal(result.dryRun, true);
  assert.equal(pool.calls.length, 8);
});

test('apply reports lock overlap truthfully without reading or writing evidence', async () => {
  let calls = 0;
  const pool = { query() { calls += 1; assert.fail('overlap must not inspect or mutate'); } };
  const service = loadScript('services/governedUncertainSendReconciliation.js', {
    './governedOutboundStore': { GovernedOutboundStore: class {
      async lock() { return { halted: 'overlap' }; }
      async releaseUnsent() { assert.fail('overlap must not release'); }
    } },
  });
  const result = await service.reconcileUncertainItemFromEvidence(pool, '10', ITEM.id);
  assert.equal(result.applied, false);
  assert.equal(result.retryAllowed, false);
  assert.equal(result.halted, 'overlap');
  assert.equal(calls, 0);
});

for (const changedItem of [
  { ...ITEM, provider_message_id: 'receipt-arrived-before-lock' },
  { ...ITEM, reason: 'provider_acceptance_unknown' },
]) {
  test(`apply classifies only after acquiring the lock: ${changedItem.provider_message_id || changedItem.reason}`, async () => {
    let locked = false;
    let item = { ...ITEM, reason: 'ak_observed_provenance_required' };
    let releaseCalls = 0;
    const pool = { async query(sql) {
      assert.equal(locked, true, 'all apply inspection and writes must be under the lock');
      if (/to_regclass/.test(sql)) return { rows: [{ control_table: null }] };
      return { rows: /FROM acquisition_outbound_items i/.test(sql) ? [item] : [] };
    } };
    const service = loadScript('services/governedUncertainSendReconciliation.js', {
      './tenantOutreachExecutorLock': { withTenantOutreachExecutorLock: async (_pool, fn) => fn() },
      './governedOutboundStore': { GovernedOutboundStore: class {
        async lock(fn) {
          item = changedItem;
          locked = true;
          try { return await fn(); } finally { locked = false; }
        }
        async releaseUnsent() { releaseCalls += 1; assert.fail('fresh receipt or ambiguity must not release'); }
      } },
    });
    const result = await service.reconcileUncertainItemFromEvidence(pool, '10', ITEM.id);
    assert.equal(result.applied, false);
    assert.equal(result.retryAllowed, false);
    assert.equal(result.classification.outcome, changedItem.provider_message_id ? 'SKIP' : 'UNKNOWN');
    assert.equal(releaseCalls, 0);
  });
}
