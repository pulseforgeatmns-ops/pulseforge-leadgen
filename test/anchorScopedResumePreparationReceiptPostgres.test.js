'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const { startHarness } = require('./helpers/anchorPreparationApprovalBrowserHarness');
const { hash } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const projection = require('../services/anchorCrmPreparationReceipt');
const service = require('../services/anchorCrmPreparationApproval');
const action = require('../services/anchorCrmPreparationAction');

test('exact preparation receipt is SELECT-only and proves actual durable draft provenance without execution', { timeout: 120000 }, async t => {
  const h = await startHarness(); t.after(() => h.close());
  const f = h.f, revision = process.env.RAILWAY_GIT_COMMIT_SHA;
  const actor = { id: '3', role: 'admin', sessionId: f.sessionId };
  await h.pool.query('UPDATE prospects SET ao_outreach_review_required=false WHERE id::text=$1', [f.manifest.admissions[0].prospectId]);
  const preview = await service.previewPreparationApproval(h.pool, { revision, intent: h.intent() }, actor);
  assert.equal(preview.installable, true);
  await service.installPreparationApproval(h.pool, { revision, proposal: preview.proposal, manifestHash: preview.manifestHash }, actor);
  const input = { revision, receipt: { manifestId: f.manifest.id, manifestHash: preview.manifestHash } };
  const queries = []; let raw;
  const readPool = { async connect() {
    const db = await h.pool.connect();
    return { release: destroy => db.release(destroy), async query(q, params) {
      const sql = typeof q === 'string' ? q : q.text; queries.push(sql);
      assert.match(sql, /^(SELECT|BEGIN READ ONLY|SET LOCAL|COMMIT|ROLLBACK)/, 'receipt may not bootstrap or write');
      const result = await db.query(q, params);
      if (sql.startsWith('SELECT g.manifest,')) raw = structuredClone(result.rows[0]);
      return result;
    } };
  } };
  async function receipt(who = actor) {
    const before = await h.snapshot();
    const result = await service.previewPreparationApproval(readPool, input, who);
    assert.deepEqual(await h.snapshot(), before, 'receipt changes no persisted row, including session');
    const serialized = JSON.stringify(result);
    assert.ok(!serialized.includes(who.sessionId));
    assert.ok(!serialized.includes(require('../services/anchorCrmPreparationIdentity').sessionBindingHash(actor.sessionId)));
    assert.equal(h.deny.attempts(), 0);
    return result;
  }
  await t.test('unconsumed receipt supplies only stored pins and same-session display evidence', async () => {
    const r = await receipt();
    assert.equal(r.preparationOutcome.status, 'not_recorded');
    assert.deepEqual(r.preparationOutcome.drafts, []);
    assert.deepEqual(r.preparationIntent.proposal, preview.proposal);
    assert.equal(r.preparationIntent.originalSessionMatches, true);
    assert.equal(r.preparationIntent.sourceMissionHash, preview.sourceMissionHash);
    assert.equal(r.preparationAvailability, 'not_asserted');
  });
  const prepared = await action.prepareDrafts(h.pool, { revision, manifestHash: preview.manifestHash, sourceMissionHash: preview.sourceMissionHash }, actor);
  let ready;
  await t.test('real guarded READY child yields exact current draft body, not just a charged claim', async () => {
    ready = await receipt();
    assert.equal(ready.preparationOutcome.status, 'ready_for_review');
    const o = ready.preparationOutcome;
    assert.equal(o.operationId, prepared.operationId); assert.equal(o.missionId, prepared.missionId);
    assert.equal(o.preparedArtifactRevision, prepared.preparedArtifactRevision);
    assert.equal(o.candidateCount, 1); assert.equal(o.draftContentHash, hash(o.drafts));
    assert.equal(o.drafts[0].recipientEmail, preview.proposal.admissions[0].recipientEmail);
    assert.ok(o.drafts[0].body.length > 20); assert.equal(o.executionApproved, false); assert.equal(o.envelopeAuthorized, false);
    assert.equal(o.retryAllowed, false); assert.equal(o.sendOutcome, 'not_checked');
    assert.equal(queries.filter(q => q.startsWith('SELECT g.manifest,')).length, 2);
    assert.deepEqual(await h.effects(), { consumed: 1, children: 1, reservations: 0, execution_approvals: 0 });
  });
  const pristine = structuredClone(raw);
  for (const [column, value] of [['stage', 'observe'], ['status', 'cancelled'], ['client_id', 11], ['orchestration_mission_id', 'other-source']])
    await t.test('persisted mission column-only ' + column + ' drift cannot borrow stale READY payload', async () => {
      const original = pristine.preparation_snapshot[0].mission[column];
      await h.pool.query(`UPDATE acquisition_missions SET ${column}=$1 WHERE id=$2`, [value, prepared.missionId]);
      const r = await receipt(); assert.equal(r.preparationOutcome.status, 'consumed_outcome_unknown');
      assert.deepEqual(r.preparationOutcome.drafts, []);
      await h.pool.query(`UPDATE acquisition_missions SET ${column}=$1 WHERE id=$2`, [original, prepared.missionId]);
    });
  for (const [name, change] of [
    ['partial claim without child', s => { s.mission = null; }],
    ['missing final ready event', s => { s.readyEvents = []; }],
    ['conflicting ready events', s => { s.readyEvents.push(structuredClone(s.readyEvents[0])); }],
    ['foreign claim tenant', s => { s.claim.tenant_id = '11'; }],
    ['wrong claim manifest', s => { s.claim.manifest_id = 'other'; }],
    ['wrong claim identities', s => { s.claim.identities[0].recipientEmail = 'other@example.test'; }],
    ['wrong operation ID', s => { s.claim.operation_id = '0'.repeat(64); }],
    ['wrong mission ownership', s => { s.mission.tenant_id = '11'; }],
    ['wrong child source hash', s => { s.mission.payload.crmPreparation.sourceMissionHash = '0'.repeat(64); }],
    ['wrong child manifest hash', s => { s.mission.payload.crmPreparation.manifestHash = '0'.repeat(64); }],
    ['partial child stage', s => { s.mission.payload.stage = 'prepare'; }],
    ['foreign contribution tenant', s => { s.contributions[0].tenant_id = '11'; }],
    ['contribution mission mismatch', s => { s.contributions[0].payload.missionId = 'other'; }],
    ['contribution overflow', s => { s.contributions = Array(101).fill(s.contributions[0]); }],
    ['missing Scout evidence', s => { s.contributions = s.contributions.filter(c => c.specialist !== 'scout'); }],
    ['missing Max prioritization', s => { s.contributions = s.contributions.filter(c => c.specialist !== 'max'); }],
    ['stale decision revision', s => { s.mission.payload.pendingOperatorDecision.executionReview.preparedArtifactRevision = '0'.repeat(16); }],
    ['stale decision queue text', s => { s.mission.payload.pendingOperatorDecision.executionReview.infrastructure.queue[0].paige.body += ' changed'; }],
    ['changed canonical sender name', s => { s.contributions.find(c => c.specialist === 'emmett').payload.payload.senderIdentity.senderName = 'Other Sender'; }],
    ['stale queue text differs from Paige', s => { s.contributions.find(c => c.specialist === 'emmett').payload.payload.queue.items[0].paige.body += ' changed'; }],
    ['wrong ready event hash', s => { s.readyEvents[0].id = '0'.repeat(64); }],
    ['wrong ready event source', s => { s.readyEvents[0].payload.sourceMissionHash = '0'.repeat(64); }],
    ['provider evidence contradiction', s => { s.readyEvents[0].payload.providerCalls = 1; }],
    ['existing envelope', s => { s.envelopes = 1; }],
    ['existing direct execution', s => { s.executions = 1; }],
    ['malformed evidence', s => { s.contributions = null; }],
  ]) await t.test(name + ' remains consumed outcome unknown with no draft disclosure', () => {
    const g = structuredClone(pristine); change(g.preparation_snapshot[0]);
    const o = projection.preparationOutcome(g);
    assert.equal(o.status, 'consumed_outcome_unknown'); assert.deepEqual(o.drafts, []); assert.equal(o.retryAllowed, false);
  });
  await t.test('overflowing claims never masquerade as one successful preparation', () => {
    const g = structuredClone(pristine); g.total_operations = 2; g.preparation_snapshot.push(structuredClone(g.preparation_snapshot[0]));
    assert.equal(projection.preparationOutcome(g).status, 'consumed_outcome_unknown');
  });
  await t.test('a fresh same-admin session reads history but is not the original preparation session', async () => {
    const sessionId = 'synthetic-fresh-read-only-recovery-session';
    await h.pool.query('INSERT INTO session(sid,sess,expire) SELECT $1,sess,expire FROM session WHERE sid=$2', [sessionId, actor.sessionId]);
    const r = await receipt({ ...actor, sessionId });
    assert.equal(r.preparationIntent.originalSessionMatches, false);
    assert.equal(r.preparationOutcome.status, 'ready_for_review');
  });
  await t.test('wrong exact hash and revoked live identity cannot disclose the prepared artifact', async () => {
    await assert.rejects(service.previewPreparationApproval(readPool, { ...input, receipt: { ...input.receipt, manifestHash: '0'.repeat(64) } }, actor));
    await h.pool.query("UPDATE users SET role='ao' WHERE id='3'");
    await assert.rejects(service.previewPreparationApproval(readPool, input, actor));
    assert.equal(h.deny.attempts(), 0);
  });
});
