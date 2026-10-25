'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const approval = require('../services/anchorCrmPreparationApproval');
const revision = 'a'.repeat(40), env = { RAILWAY_GIT_COMMIT_SHA: revision };
const actor = { id: '3', role: 'admin', sessionId: 'synthetic-no-live-bearer' };
const pool = { connect() { assert.fail('Invalid requests must never connect'); } };
const intent = { id: 'bounded-approval', programId: 'historical-program', sourceMissionId: 'approved-source',
  validFrom: '2026-10-24T20:00:00Z', expiresAt: '2026-10-24T21:00:00Z', approvalReference: 'exact-owner-approval',
  preparationApprovalReference: 'exact-draft-only-approval', dailyOperationCap: 1, dailyItemCap: 1,
  admissions: [{ prospectId: 'candidate-1', approvalReference: 'separate-reviewed-identity-approval' }] };
test('preparation approval adapters reject unapproved shapes before accessing data', async t => {
  for (const [label, mutation] of [
    ['review initialization', i => { i.reviewInitialization = {}; }],
    ['caller actor', i => { i.operatorId = '19'; }],
    ['caller session', i => { i.sessionId = 'untrusted'; }],
    ['caller manifest', i => { i.manifest = {}; }],
    ['empty identities', i => { i.intent.admissions = []; }],
    ['duplicate identities', i => { i.intent.admissions.push(i.intent.admissions[0]); }],
    ['recipient injection', i => { i.intent.admissions[0].recipientEmail = 'x@example.invalid'; }],
    ['caller tenant', i => { i.intent.tenantId = '11'; }],
    ['caller binding hash', i => { i.intent.programBindingHash = 'a'.repeat(64); }],
    ['invalid revision', i => { i.revision = 'b'.repeat(40); }],
    ['unbounded window', i => { delete i.intent.expiresAt; }],
    ['empty review selection', i => { i.review = { action: 'initialize_null_review_false', approvalReference: 'separate-review', prospectIds: [] }; }],
    ['unadmitted review selection', i => { i.review = { action: 'initialize_null_review_false', approvalReference: 'separate-review', prospectIds: ['other'] }; }],
    ['duplicate review selection', i => { i.review = { action: 'initialize_null_review_false', approvalReference: 'separate-review', prospectIds: ['candidate-1','candidate-1'] }; }],
    ['release true hold', i => { i.review = { action: 'release_true_review', approvalReference: 'separate-review', prospectIds: ['candidate-1'] }; }],
    ['review without approval reference', i => { i.review = { action: 'initialize_null_review_false', prospectIds: ['candidate-1'] }; }],
    ['review caller identity pin', i => { i.review = { action: 'initialize_null_review_false', approvalReference: 'separate-review', prospectIds: ['candidate-1'], identityFingerprint: 'a'.repeat(64) }; }],
  ]) await t.test(label, async () => {
    const input = { revision, intent: structuredClone(intent) }; mutation(input);
    await assert.rejects(approval.previewPreparationApproval(pool, input, actor, { env }));
  });
  for (const role of ['ao','manager',undefined]) await t.test(`reject ${role || 'missing'} role`, async () => {
    await assert.rejects(approval.previewPreparationApproval(pool, { revision, intent }, { ...actor, role }, { env }));
  });
  for (const method of ['installPreparationApproval','revokePreparationApproval']) await t.test(`${method} strict shape`, async () => {
    await assert.rejects(approval[method](pool, { revision, reviewInitialization: {}, manifestHash: 'a'.repeat(64) }, actor, { env }));
  });
});
test('approval adapters have no provider, invocation, activation or direct review writes', () => {
  const fs = require('node:fs');
  const source = fs.readFileSync(require.resolve('../services/anchorCrmPreparationApproval'), 'utf8');
  for (const forbidden of ['prepareExistingCrm(', 'prepareDrafts(', 'UPDATE prospects', 'process.env[', 'process.env.ANCHOR', 'fetch(', 'axios', 'pool.end('])
    assert.ok(!source.includes(forbidden), forbidden);
  assert.match(source, /BEGIN READ ONLY/);
  assert.match(source, /preparationAvailability: 'not_asserted'/);
  assert.match(source, /control\.installScopedResume\(/);
  assert.match(source, /anchor_preparation_review_confirmation_v1/);
  assert.match(source, /currentReviewState: 'not_checked'/);
});
