'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { fixture } = require('./helpers/paigeSocialFixture');
const { approvalHash } = require('../packages/capabilities/contentPublication/approvalBinding');
const { resolveSocialAccount } = require('../packages/capabilities/contentPublication/socialAccounts');

test('unapproved and legacy approval flag without a binding cannot publish', async () => {
  const f = await fixture(); assert.equal((await f.publish()).status, 'failed');
  await f.store.updateArtifactMetadata(f.artifact.id, '10', 10, { approvalState: 'APPROVED' });
  assert.equal((await f.publish()).errors[0].message, 'approval_binding_required'); assert.equal(f.counts().sends, 0);
});

test('human preview hash and actor are required; stale preview is rejected', async () => {
  const f = await fixture();
  const input = { ...f.scope, artifactId: f.artifact.id, decision: 'approve', accountId: f.account.id };
  await assert.rejects(f.approval.recordDecision(input), /explicit_artifact_approval_required/);
  await assert.rejects(f.approval.recordDecision({ ...input, approvedBy: 'operator:7', expectedApprovalHash: 'stale' }), /approval_preview_changed/);
  assert.equal((await f.current()).approvalState, 'PENDING_APPROVAL');
});

test('content, media, objective, mission, campaign, account mutations invalidate approved artifact', async () => {
  for (const mutate of [a => { a.body += ' changed'; }, a => { a.mediaRefs = ['https://example.com/pic']; }, a => { a.contentObjective = 'other'; },
    a => { a.missionId = 'other'; }, a => { a.meta.campaignId = 'other'; }, a => { a.platform = 'facebook_page'; }]) {
    const f = await fixture(); await f.approve(); const a = await f.current(); mutate(a); await f.store.insertBatch([a]);
    assert.equal((await f.publish()).status, 'failed'); assert.equal(f.counts().sends, 0);
  }
  const f = await fixture(); await f.approve(); f.account.externalAccountId = 'other-account';
  assert.equal((await f.publish()).errors[0].message, 'approved_artifact_or_account_changed'); assert.equal(f.counts().sends, 0);
});

test('canonical tenant and account checks fail closed without using global credentials', async () => {
  const f = await fixture(); await f.approve();
  await assert.rejects(f.approval.preview({ clientId: 11, tenantId: '11', artifactId: f.artifact.id }), /artifact_not_found/);
  const r = await f.cap.execute({ tenantId: '11', clientId: 11, inputs: { artifactId: f.artifact.id } }); assert.equal(r.status, 'failed');
  assert.throws(() => resolveSocialAccount({ clientId: 10, tenantId: '10', platform: 'linkedin_page' }, { BUFFER_ACCESS_TOKEN: 'global' }), /not_connected/);
  const env = { PAIGE_SOCIAL_ACCOUNTS: JSON.stringify([{ ...f.account, credentialEnv: { accessToken: 'ANCHOR_TOKEN' } }]), ANCHOR_TOKEN: 'test' };
  assert.throws(() => resolveSocialAccount({ clientId: 11, tenantId: '11', platform: 'linkedin_page', accountId: f.account.id }, env), /not_connected/);
  assert.throws(() => resolveSocialAccount({ clientId: 10, tenantId: '11', platform: 'linkedin_page' }, env), /tenant_scope_required/);
  const resolved = resolveSocialAccount({ ...f.scope, platform: 'linkedin_page' }, env);
  assert.equal(resolved.account.externalAccountId, 'channel-10'); assert.doesNotMatch(JSON.stringify(resolved.account), /test|TOKEN/);
});

test('Anchor doctrine blocks AI tells, generic closers, body em dashes and walkthrough terminology', async () => {
  for (const body of ['I wanted to reach out about a facilities assessment.', 'Cleaning — made easy.', 'Would you be open to a quick call?', 'Book a walkthrough.']) {
    const f = await fixture(); const a = await f.current(); a.body = body; await f.store.insertBatch([a]);
    await assert.rejects(f.approval.preview({ ...f.scope, artifactId: a.id }), /anchor_copy_doctrine_violation/);
    assert.notEqual(approvalHash(a, f.account), approvalHash(f.artifact, f.account));
  }
});

test('approval repeat is idempotent and rejection cannot restore or cancel an in-flight post', async () => {
  const f = await fixture(); await f.approve(); const timestamp = (await f.current()).approvedAt;
  assert.equal((await f.approve()).idempotent, true); assert.equal((await f.current()).approvedAt, timestamp);
  await f.publish(); await assert.rejects(f.approval.recordDecision({ ...f.scope, artifactId: f.artifact.id, decision: 'reject' }), /cannot_reject_published_artifact/);
  const g = await fixture(); await g.approval.recordDecision({ ...g.scope, artifactId: g.artifact.id, decision: 'reject' });
  await assert.rejects(g.approve(), /rejected_artifact_requires_restore/);
});
