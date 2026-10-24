'use strict';
const assert = require('node:assert/strict');
const { test } = require('node:test');
const { createSocialPlatformAdapters, providerFailure } = require('../packages/capabilities/contentPublication/adapters/SocialPlatformAdapters');
const { createPublicationService } = require('../packages/capabilities/contentPublication/PublicationService');
const { PlatformAdapterRegistry } = require('../packages/capabilities/contentPublication/PlatformAdapterRegistry');
const { fixture } = require('./helpers/paigeSocialFixture');

test('Buffer uses variables, exact approved text, bound account, and distinct read-back query', async () => {
  const calls = [];
  const http = { post: async (url, data, config) => { calls.push({ url, data, config }); return data.query.startsWith('mutation')
    ? { data: { data: { createPost: { post: { id: 'buffer-id' } } } } }
    : { data: { data: { post: { id: 'buffer-id', text: 'Exact "copy"\nnext', status: 'sent', sentAt: '2026-09-24T03:00:00Z', externalLink: 'https://linkedin.com/post', channel: { id: 'bound', service: 'linkedin' } } } } }; } };
  const adapter = createSocialPlatformAdapters({ http })[0];
  const input = { artifact: { body: 'Exact "copy"\nnext' }, account: { externalAccountId: 'bound' }, credentials: { accessToken: 'test-token' } };
  const receipt = await adapter.publish(input); const verify = await adapter.readBack({ ...input, receipt });
  assert.equal(receipt.externalPostId, 'buffer-id'); assert.equal(verify.published, true); assert.equal(verify.platformMatches, true);
  assert.equal(calls[0].data.variables.input.text, input.artifact.body); assert.equal(calls[0].data.variables.input.channelId, 'bound');
  assert.equal(calls[0].data.variables.input.mode, 'shareNow'); assert.equal(calls[1].data.variables.input.id, 'buffer-id');
  assert.equal(calls[0].config.timeout, 30000); assert.equal(calls[0].config.headers.Authorization, 'Bearer test-token');
});

test('Buffer mutation errors are definite rejection; GraphQL transport errors remain ambiguous', async () => {
  for (const [data, definite] of [[{ data: { createPost: { message: 'Rejected' } } }, true], [{ errors: [{ message: 'server failed' }] }, false]]) {
    const adapter = createSocialPlatformAdapters({ http: { post: async () => ({ data }) } })[0];
    const result = await adapter.publish({ artifact: { body: 'text' }, account: { externalAccountId: 'a' }, credentials: { accessToken: 'secret' } });
    assert.equal(result.success, false); assert.equal(result.definitelyNotPublished === true, definite); assert.doesNotMatch(JSON.stringify(result), /secret|server failed/);
  }
});

test('Facebook uses exact text and page-specific credentials and verifies page/post/text/status', async () => {
  const calls = []; const http = {
    post: async (...args) => { calls.push(args); return { data: { id: '10_20' } }; },
    get: async (...args) => { calls.push(args); return { data: { id: '10_20', from: { id: '10' }, message: 'Exact copy', is_published: true, created_time: '2026-09-24T03:00:00Z', permalink_url: 'https://facebook.com/10_20' } }; },
  };
  const adapter = createSocialPlatformAdapters({ http }).find(a => a.platform === 'facebook_page');
  const input = { artifact: { body: 'Exact copy' }, account: { externalAccountId: '10', apiVersion: 'v25.0' }, credentials: { accessToken: 'token-10' } };
  const receipt = await adapter.publish(input); const verify = await adapter.readBack({ ...input, receipt });
  assert.equal(verify.externalAccountId, '10'); assert.equal(verify.published, true); assert.deepEqual(calls[0][1], { message: 'Exact copy' });
  assert.equal(calls[0][0], 'https://graph.facebook.com/v25.0/10/feed');
  await assert.rejects(adapter.readBack({ ...input, receipt: { externalPostId: '../other' } }), /invalid_provider_post_id/);
});

test('GBP uses bound location and never treats PROCESSING as LIVE', async () => {
  const calls = []; const name = 'accounts/10/locations/20/localPosts/30';
  const adapter = createSocialPlatformAdapters({ googleAuthFactory: () => ({ setCredentials: c => calls.push(c), request: async r => {
    calls.push(r); return { data: { name, summary: 'Exact copy', state: 'PROCESSING', createTime: '2026-09-24T03:00:00Z' } };
  } }) }).find(a => a.platform === 'google_business');
  const input = { artifact: { body: 'Exact copy' }, account: { externalAccountId: 'accounts/10/locations/20' }, credentials: { clientId: 'google', clientSecret: 'test', refreshToken: 'refresh' } };
  const receipt = await adapter.publish(input); const verify = await adapter.readBack({ ...input, receipt });
  assert.equal(verify.published, false); assert.equal(verify.status, 'PROCESSING'); assert.equal(calls[1].retry, false);
  assert.equal(calls[1].data.summary, 'Exact copy');
  await assert.rejects(adapter.readBack({ ...input, receipt: { externalPostId: 'accounts/11/locations/20/localPosts/30' } }), /invalid_provider_post_id/);
});

test('unknown errors are not retryable, and no error body or token is retained', () => {
  assert.equal(providerFailure({ response: { status: 503, data: 'secret' } }).definitelyNotPublished, false);
  assert.equal(providerFailure({ response: { status: 429 } }).definitelyNotPublished, true);
  assert.doesNotMatch(JSON.stringify(providerFailure({ message: 'secret' })), /secret/);
});

test('service refuses legacy adapters without read-back and validates approval even when called directly', async () => {
  const f = await fixture(); const registry = new PlatformAdapterRegistry(); let sent = false;
  registry.register({ platform: 'linkedin_page', publish: async () => { sent = true; } });
  const service = createPublicationService({ adapterRegistry: registry, accountResolver: f.accountResolver });
  await assert.rejects(service.publishApprovedArtifact({ ...f.scope, artifact: await f.current() }), /requires_approved/);
  await f.approve(); await assert.rejects(service.publishApprovedArtifact({ ...f.scope, artifact: await f.current() }), /verified_publish_adapter_required/);
  assert.equal(sent, false);
});
