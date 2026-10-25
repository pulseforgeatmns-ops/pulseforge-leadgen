'use strict';

// Local synthetic diagnosis, not a production-cause assertion or recovery path.
// Exercise native authenticated grant/review, actual PostgreSQL transactions,
// tenant-wide hydration and all canonical specialists. No hydration mocks,
// provider discovery, schema/business-behavior repair, retry or refund mechanism.
const test = require('node:test'), assert = require('node:assert/strict');
const { startHarness } = require('./helpers/anchorPreparationApprovalBrowserHarness');
const approval = require('../services/anchorCrmPreparationApproval');
const action = require('../services/anchorCrmPreparationAction');
const runtime = require('../services/acquisitionMissionRuntime');

const HISTORY_ID = 'synthetic-unrelated-historical-ready-mission';
const FAILURE = { code: 'anchor_crm_action_rejected', status: 'blocked_review_required',
  writesMayHaveOccurred: true, retryAllowed: false };
const effects = children => ({ consumed: 1, children, reservations: 0, execution_approvals: 0 });

async function authorize(h) {
  const revision = process.env.RAILWAY_GIT_COMMIT_SHA;
  const actor = { id: '3', role: 'admin', sessionId: h.f.sessionId };
  const intent = h.intent();
  const preview = await approval.previewPreparationApproval(h.pool, { revision, intent,
    review: { action: 'initialize_null_review_false', approvalReference: 'synthetic-exact-diagnostic-review',
      prospectIds: intent.admissions.map(a => a.prospectId) } }, actor);
  assert.equal(preview.installable, true);
  const installed = await approval.installPreparationApproval(h.pool, { revision,
    proposal: preview.proposal, manifestHash: preview.manifestHash,
    reviewInitialization: preview.reviewInitialization, reviewConfirmationHash: preview.reviewConfirmationHash }, actor);
  assert.equal(installed.initialized, 1);
  return { actor, preview,
    input: { revision, manifestHash: preview.manifestHash, sourceMissionHash: preview.sourceMissionHash },
    receiptInput: { revision, receipt: { manifestId: preview.proposal.id, manifestHash: preview.manifestHash } } };
}

async function missionRows(pool, id) {
  return { mission: (await pool.query('SELECT * FROM acquisition_missions WHERE id=$1', [id])).rows[0],
    contributions: (await pool.query('SELECT * FROM acquisition_mission_contributions WHERE mission_id=$1 ORDER BY id', [id])).rows };
}

async function insertHistory(pool, history, tenantId = '10') {
  const saved = structuredClone(history);
  saved.mission.tenant_id = tenantId;
  saved.mission.client_id = Number(tenantId);
  saved.mission.payload.tenantId = tenantId;
  saved.mission.payload.clientId = Number(tenantId);
  await pool.query('INSERT INTO acquisition_missions SELECT * FROM jsonb_populate_record(NULL::acquisition_missions,$1)', [saved.mission]);
  for (const row of saved.contributions) {
    row.tenant_id = tenantId;
    await pool.query('INSERT INTO acquisition_mission_contributions SELECT * FROM jsonb_populate_record(NULL::acquisition_mission_contributions,$1)', [row]);
  }
}

async function corruptContribution(pool, specialist, mode) {
  if (mode === 'deleted') {
    await pool.query('DELETE FROM acquisition_mission_contributions WHERE mission_id=$1 AND specialist=$2', [HISTORY_ID, specialist]);
  } else {
    // Production-shaped nested specialist payload, not a fake runtime result.
    const field = mode === 'superseded' ? 'superseded' : 'rolledBack';
    await pool.query(`UPDATE acquisition_mission_contributions
      SET payload=jsonb_set(payload,ARRAY['payload',$3], 'true'::jsonb, true)
      WHERE mission_id=$1 AND specialist=$2`, [HISTORY_ID, specialist, field]);
  }
}

test('synthetic legacy history and first-stage failures retain consumed uncertainty without changing public authority', { timeout: 240000 }, async t => {
  const h = await startHarness(); t.after(() => h.close());
  let diagnosticLines = [], history;
  const originalError = console.error;
  t.mock.method(console, 'error', (...args) => {
    if (args[0] === '[anchor-crm-preparation]') diagnosticLines.push(args);
    else originalError(...args);
  });
  const clearDiagnostics = () => { diagnosticLines = []; };
  function diagnostic(expected) {
    assert.equal(diagnosticLines.length, 1, 'one bounded terminal preparation event');
    assert.equal(diagnosticLines[0].length, 2);
    const event = JSON.parse(diagnosticLines[0][1]);
    for (const [key, value] of Object.entries(expected)) assert.deepEqual(event[key], value, key);
    assert.equal(event.retryAllowed, false);
    assert.match(event.operationId, /^[a-f0-9]{64}$/);
    const serialized = JSON.stringify(event);
    for (const privateValue of [HISTORY_ID, h.f.sessionId, h.f.manifest.admissions[0].recipientEmail,
      'synthetic-commit-ack-private-message', 'synthetic_fail_first_stage', 'SELECT', 'INSERT', 'stack']) {
      assert.ok(!serialized.includes(privateValue), 'diagnostic excludes raw identifiers, SQL and errors');
    }
    t.diagnostic(serialized);
    return event;
  }
  async function reset() {
    await h.reset(); runtime.resetAcquisitionMissionRuntime(); clearDiagnostics();
  }
  async function receipt(auth) {
    const before = await h.snapshot();
    const r = await approval.previewPreparationApproval(h.pool, auth.receiptInput, auth.actor);
    assert.deepEqual(await h.snapshot(), before, 'exact readback is SELECT-only');
    return r;
  }
  async function assertConsumedUnknown(auth, { repeat = true } = {}) {
    assert.deepEqual(await h.effects(), effects(0));
    for (const table of ['acquisition_mission_events', 'acquisition_mission_execution_audit']) {
      assert.equal((await h.pool.query(`SELECT count(*)::int AS n FROM ${table}
        WHERE mission_id LIKE 'mission_crm_%'`)).rows[0].n, 0,
      'no child-specific event or stage audit survives the failure/first-stage rollback');
    }
    const r = await receipt(auth);
    assert.equal(r.preparationOutcome.status, 'consumed_outcome_unknown');
    assert.deepEqual(r.preparationOutcome.drafts, []);
    assert.equal(r.preparationOutcome.retryAllowed, false);
    const claims = (await h.pool.query('SELECT * FROM anchor_scoped_resume_consumption ORDER BY operation_id')).rows;
    assert.equal(claims.length, 1);
    if (repeat) {
      // An explicit adversarial second call is refused; no automatic retry is
      // added by diagnostics and no second claim/child/provider work is created.
      const before = await h.snapshot();
      await assert.rejects(action.prepareDrafts(h.pool, auth.input, auth.actor),
        { code: 'anchor_crm_action_rejected', status: 'already_consumed', retryAllowed: false });
      assert.deepEqual(await h.snapshot(), before, 'duplicate action does not alter persisted state');
    }
    const revoked = await approval.revokePreparationApproval(h.pool, { revision: auth.input.revision,
      manifestId: auth.preview.proposal.id, manifestHash: auth.preview.manifestHash,
      approvalReference: 'synthetic-explicit-diagnostic-revocation' }, auth.actor);
    assert.equal(revoked.revoked, true);
    assert.deepEqual((await h.pool.query('SELECT * FROM anchor_scoped_resume_consumption ORDER BY operation_id')).rows, claims,
      'revocation preserves the durable claim; it does not refund');
    assert.equal(await h.reviewed(), false, 'revocation does not reverse the committed exact review');
    const afterRevoke = await receipt(auth);
    assert.equal(afterRevoke.status, 'revoked');
    assert.equal(afterRevoke.preparationOutcome.status, 'consumed_outcome_unknown');
    assert.deepEqual(afterRevoke.preparationOutcome.drafts, []);
    assert.deepEqual(await h.effects(), effects(0));
    assert.equal(h.deny.attempts(), 0);
  }

  await t.test('generate the historical fixture from a real complete guarded READY preparation', async () => {
    const auth = await authorize(h), protectedBefore = await h.protectedState();
    const prepared = await action.prepareDrafts(h.pool, auth.input, auth.actor);
    assert.equal(prepared.status, 'ready_for_review');
    assert.equal(prepared.providerCalls, 0); assert.equal(prepared.sent, 0);
    assert.deepEqual(await h.effects(), effects(1));
    assert.deepEqual(await h.protectedState(), protectedBefore);
    assert.equal((await receipt(auth)).preparationOutcome.status, 'ready_for_review');
    const complete = await missionRows(h.pool, prepared.missionId);
    assert.equal(complete.mission.stage, 'ready');
    assert.equal(complete.mission.payload.pendingOperatorDecision.kind, 'execution_approval');
    for (const specialist of ['scout', 'max', 'paige', 'emmett']) {
      assert.ok(complete.contributions.some(c => c.specialist === specialist));
    }
    // Preserve the actual artifact/contribution chain. Only the parent mission
    // ID is remapped throughout the snapshot to make it unrelated history.
    history = JSON.parse(JSON.stringify(complete).replaceAll(prepared.missionId, HISTORY_ID));
    assert.equal(diagnosticLines.length, 0, 'successful preparation emits no failure diagnostic');
    assert.equal(h.deny.attempts(), 0);
  });

  await t.test('unchanged complete unrelated history hydrates and permits a new guarded READY child', async () => {
    await reset(); await insertHistory(h.pool, history);
    const historyBefore = await missionRows(h.pool, HISTORY_ID), auth = await authorize(h);
    const protectedBefore = await h.protectedState();
    const prepared = await action.prepareDrafts(h.pool, auth.input, auth.actor);
    assert.equal(prepared.status, 'ready_for_review');
    assert.notEqual(prepared.missionId, HISTORY_ID);
    assert.equal((await receipt(auth)).preparationOutcome.status, 'ready_for_review');
    assert.deepEqual(await missionRows(h.pool, HISTORY_ID), historyBefore);
    assert.deepEqual(await h.protectedState(), protectedBefore);
    assert.deepEqual(await h.effects(), effects(1));
    assert.equal(diagnosticLines.length, 0); assert.equal(h.deny.attempts(), 0);
  });

  const cases = ['paige', 'emmett', 'scout', 'max'].flatMap(specialist =>
    ['deleted', 'superseded', 'rolled_back'].map(mode => ({ name: `${specialist} ${mode}`,
      mutate: () => corruptContribution(h.pool, specialist, mode) })));
  cases.push({ name: 'SQL stage disagrees with the persisted pending-decision stage',
    mutate: () => h.pool.query("UPDATE acquisition_missions SET stage='observe' WHERE id=$1", [HISTORY_ID]) });
  for (const c of cases) await t.test(`unrelated history with ${c.name} blocks before any new child persistence`, async () => {
    await reset(); await insertHistory(h.pool, history); await c.mutate();
    const historyBefore = await missionRows(h.pool, HISTORY_ID), auth = await authorize(h);
    const protectedBefore = await h.protectedState();
    await assert.rejects(action.prepareDrafts(h.pool, auth.input, auth.actor), FAILURE);
    diagnostic({ phase: 'canonical_hydration', claimOutcome: 'committed', errorCode: 'MISSION_STATE_INCONSISTENT',
      executionErrorClass: null, step: null, intent: null });
    assert.deepEqual(await h.protectedState(), protectedBefore);
    assert.deepEqual(await missionRows(h.pool, HISTORY_ID), historyBefore, 'diagnosis never repairs history');
    // Independently establish the native underlying error from the same actual
    // persisted rows, without replacing hydrate or its consistency checker.
    runtime.resetAcquisitionMissionRuntime();
    const beforeHydration = await h.snapshot();
    await assert.rejects(runtime.getAcquisitionMissionRuntime({ pool: h.pool, persist: true }).hydrate('10',
      { pool: h.pool, persist: true, preinitializedSchema: true }), { code: 'MISSION_STATE_INCONSISTENT' });
    assert.deepEqual(await h.snapshot(), beforeHydration);
    runtime.resetAcquisitionMissionRuntime();
    await assertConsumedUnknown(auth);
  });

  await t.test('corrupt history belonging to another tenant does not contaminate Anchor hydration', async () => {
    await reset(); await insertHistory(h.pool, history, '11'); await corruptContribution(h.pool, 'paige', 'deleted');
    const auth = await authorize(h), historyBefore = await missionRows(h.pool, HISTORY_ID);
    const prepared = await action.prepareDrafts(h.pool, auth.input, auth.actor);
    assert.equal(prepared.status, 'ready_for_review');
    assert.equal((await receipt(auth)).preparationOutcome.status, 'ready_for_review');
    assert.deepEqual(await missionRows(h.pool, HISTORY_ID), historyBefore);
    assert.deepEqual(await h.effects(), effects(1));
    assert.equal(diagnosticLines.length, 0); assert.equal(h.deny.attempts(), 0);
  });

  await t.test('actual first-stage PostgreSQL constraint rollback has the same public symptom but a distinct diagnostic phase', async () => {
    await reset(); await insertHistory(h.pool, history);
    await h.pool.query(`ALTER TABLE acquisition_missions ADD CONSTRAINT synthetic_fail_first_stage
      CHECK (id NOT LIKE 'mission_crm_%') NOT VALID`);
    const auth = await authorize(h), protectedBefore = await h.protectedState();
    await assert.rejects(action.prepareDrafts(h.pool, auth.input, auth.actor), FAILURE);
    const event = diagnostic({ phase: 'stage_execution', claimOutcome: 'committed', errorCode: 'tme_persistence',
      executionErrorClass: 'persistence', step: 0, intent: 'APPROVE_DISCOVERY' });
    assert.ok(event.causeCodes.includes('23514'), 'bounded cause chain retains the actual database constraint code');
    assert.deepEqual(await h.protectedState(), protectedBefore);
    assert.equal((await h.pool.query("SELECT count(*)::int AS n FROM acquisition_mission_contributions WHERE mission_id LIKE 'mission_crm_%'")).rows[0].n, 0);
    runtime.resetAcquisitionMissionRuntime();
    await runtime.getAcquisitionMissionRuntime({ pool: h.pool, persist: true }).hydrate('10',
      { pool: h.pool, persist: true, preinitializedSchema: true });
    await assertConsumedUnknown(auth);
  });

  await t.test('lost acknowledgement after a real claim COMMIT stays unknown and is never retried or refunded', async () => {
    await reset();
    const auth = await authorize(h); let actualCommits = 0;
    const faultPool = { query: h.pool.query.bind(h.pool), async connect() {
      const db = await h.pool.connect(); let claiming = false;
      return { release: destroy => db.release(destroy), async query(q, values) {
        const sql = typeof q === 'string' ? q : q.text;
        if (/INSERT INTO anchor_scoped_resume_consumption/.test(sql)) claiming = true;
        const result = await db.query(q, values);
        if (claiming && sql === 'COMMIT') {
          claiming = false; actualCommits++;
          throw new Error('synthetic-commit-ack-private-message');
        }
        return result;
      } };
    } };
    await assert.rejects(action.prepareDrafts(faultPool, auth.input, auth.actor), FAILURE);
    diagnostic({ phase: 'claim', claimOutcome: 'unknown', errorCode: 'unknown',
      executionErrorClass: null, step: null, intent: null });
    assert.equal(actualCommits, 1, 'the fault follows, rather than replaces, the actual commit');
    await assertConsumedUnknown(auth, { repeat: false });
    assert.equal(actualCommits, 1, 'readback and revoke never repeat the ambiguous action');
  });
});
