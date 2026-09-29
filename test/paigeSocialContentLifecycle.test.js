'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { fixture } = require('./helpers/paigeSocialFixture');
const { createInMemorySocialContentStore } = require('../packages/capabilities/contentGeneration/SocialContentStore');

test('approval binds exact content, destination, actor, campaign and mission; publication verifies and replays', async () => {
  const f = await fixture(); await f.approve();
  const approved = await f.current();
  assert.equal(approved.approvalBinding.approvedBy, 'operator:7');
  assert.equal(approved.approvalBinding.account.externalAccountId, 'channel-10');
  const result = await f.publish();
  assert.equal(result.status, 'completed');
  const a = await f.current();
  assert.equal(a.publishState, 'PUBLISHED');
  assert.equal(a.publication.providerPostId, 'provider-10');
  assert.equal(a.publication.missionId, 'max-mission');
  assert.equal(a.publication.campaignId, 'anchor-campaign');
  assert.equal(a.publication.contentHash, a.approvalBinding.contentHash);
  assert.ok(a.publication.verifiedAt);
  assert.equal((await f.publish()).outputs.idempotent, true);
  assert.deepEqual(f.counts(), { sends: 1, reads: 1 });
});

test('concurrent publication performs one provider create', async () => {
  let release; const wait = new Promise(r => { release = r; });
  const f = await fixture({ send: async () => { await wait; return { success: true, externalPostId: 'one' }; } }); await f.approve();
  const first = f.publish();
  while (!f.counts().sends) await new Promise(r => setImmediate(r));
  const second = await f.publish(); release(); await first;
  assert.equal(second.status, 'failed');
  assert.match(second.errors[0].message, /publish_in_progress/);
  assert.equal(f.counts().sends, 1);
});

test('definitive provider rejection is logged and safe to retry', async () => {
  const f = await fixture({ send: async (_, n) => n === 1 ? { success: false, definitelyNotPublished: true, retryable: true, errorCode: 'provider_http_429' } : { success: true, externalPostId: 'retry-ok' } });
  await f.approve(); assert.equal((await f.publish()).status, 'failed');
  assert.equal((await f.current()).publishState, 'FAILED');
  assert.equal((await f.current()).publication.safeToRetry, true);
  assert.equal((await f.publish()).status, 'completed');
  assert.equal((await f.current()).publication.attempts.length, 2);
});

test('timeout and void success are UNKNOWN, never fresh-send retried', async () => {
  for (const send of [async () => { throw new Error('socket timed out secret-token'); }, async () => ({ success: true })]) {
    const f = await fixture({ send }); await f.approve(); await f.publish();
    assert.equal((await f.current()).publishState, 'UNKNOWN');
    assert.equal((await f.publish()).errors[0].message, 'publish_requires_reconciliation');
    assert.equal(f.counts().sends, 1);
    assert.doesNotMatch(JSON.stringify(await f.current()), /secret-token/);
  }
});

test('accepted provider queue is VERIFYING; retry reads the same ID without creating again', async () => {
  const f = await fixture({ read: async (i, n) => ({ externalPostId: i.receipt.externalPostId, externalAccountId: 'channel-10', body: i.artifact.body,
    platformMatches: true, published: n > 1, status: n > 1 ? 'sent' : 'scheduled', publishedAt: n > 1 ? '2026-09-24T03:00:00Z' : null }) });
  await f.approve(); await f.publish(); assert.equal((await f.current()).publishState, 'VERIFYING');
  await f.publish(); assert.equal((await f.current()).publishState, 'PUBLISHED');
  assert.deepEqual(f.counts(), { sends: 1, reads: 2 });
});

test('read-back failure or wrong content/account/post/platform cannot mark published', async () => {
  for (const read of [async () => { throw new Error('read timeout'); },
    ...[{ body: 'changed' }, { externalAccountId: 'other' }, { externalPostId: 'other' }, { platformMatches: false }].map(patch => async i => ({
      externalPostId: i.receipt.externalPostId, externalAccountId: 'channel-10', body: i.artifact.body, platformMatches: true,
      status: 'sent', published: true, publishedAt: '2026-09-24T03:00:00Z', ...patch }))]) {
    const f = await fixture({ read }); await f.approve(); await f.publish(); await f.publish();
    assert.equal((await f.current()).publishState, 'VERIFYING'); assert.equal(f.counts().sends, 1);
  }
});

test('durable receipt survives restarted publisher and failed outcome sync', async () => {
  let failures = 0;
  const f = await fixture({ syncOutcome: async () => { if (!failures++) throw new Error('DB down'); return 'outcome-10'; } });
  await f.approve(); await f.publish();
  assert.equal((await f.current()).publication.outcomeError, 'outcome_sync_failed');
  await f.publish(); assert.equal((await f.current()).publication.outcomeId, 'outcome-10');
  assert.equal(f.counts().sends, 1);
});

test('database receipt-commit failure stays PUBLISHING and forbids resend', async () => {
  const store = createInMemorySocialContentStore(); const original = store.updateArtifactMetadata;
  const f = await fixture({ store }); await f.approve();
  store.updateArtifactMetadata = async function(...args) { if (args[3]?.publication?.receipt) throw new Error('disk failure'); return original.apply(this, args); };
  await f.publish(); store.updateArtifactMetadata = original;
  assert.equal((await f.current()).publishState, 'PUBLISHING');
  assert.equal((await f.publish()).errors[0].message, 'publish_in_progress'); assert.equal(f.counts().sends, 1);
});

test('unknown attempt can reconcile a human-supplied existing ID, with read-back only', async () => {
  const f = await fixture({ send: async () => { throw new Error('timeout'); } }); await f.approve(); await f.publish();
  assert.equal((await f.publish({ reconcilePostId: 'known' })).status, 'failed');
  const r = await f.publish({ reconcilePostId: 'known', reconciledBy: 'operator:7', reconciliationReason: 'Found in Buffer' });
  assert.equal(r.status, 'completed'); assert.equal(f.counts().sends, 1);
  assert.equal((await f.current()).publication.reconciledBy, 'operator:7');
});

test('disabled publishing and dry-run never call provider or change publication state', async () => {
  const f = await fixture({ enabled: false }); await f.approve();
  assert.equal((await f.publish()).errors[0].message, 'social_publication_disabled');
  assert.equal((await f.publish({ dryRun: true })).status, 'completed');
  assert.equal((await f.current()).publishState, 'NOT_PUBLISHED'); assert.equal(f.counts().sends, 0);
});
