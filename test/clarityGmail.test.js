'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { GMAIL_READONLY_SCOPE, PILOT_MAILBOX, createOAuthAuthorization, completeOAuthAuthorization, createClarityGmail, encryptRefreshToken, decryptRefreshToken } = require('../services/clarityGmail');

const NOW = Date.parse('2026-10-20T00:00:00Z');
function env(overrides = {}) { return { CLARITY_ENABLED: 'true', CLARITY_INGESTION_ENABLED: 'true', CLARITY_GOOGLE_CLIENT_ID: 'dedicated-client', CLARITY_GOOGLE_CLIENT_SECRET: 'secret-client-fixture', CLARITY_TOKEN_ENCRYPTION_KEY: Buffer.alloc(32, 1).toString('base64'), ...overrides }; }
function integration(overrides = {}, environment = env()) {
  const value = { client_id: 10, mailbox_email: PILOT_MAILBOX, digest_sender: 'fixture@microsoft.com', project_id: 'fixture976', enabled: true, consent_status: 'authorized', consented_at: '2026-10-19T00:00:00.000Z', oauth_client_id: 'dedicated-client', oauth_scopes: [GMAIL_READONLY_SCOPE], token_key_id: 'v1', ...overrides };
  value.refresh_token_encrypted = encryptRefreshToken('refresh-secret-fixture', value, environment);
  return value;
}
function response(data, status = 200) { return { ok: status >= 200 && status < 300, status, json: async () => data }; }
function authorization(environment = env()) { return createOAuthAuthorization({ clientId: 10, mailboxEmail: PILOT_MAILBOX, digestSender: 'fixture@microsoft.com', projectId: 'fixture976', redirectUri: 'http://127.0.0.1:8787/oauth/callback', env: environment, now: NOW }); }

test('enrollment requests a dedicated readonly grant with offline access, state, PKCE and no legacy combined consent', () => {
  const { authorizationUrl, pending } = authorization();
  const url = new URL(authorizationUrl);
  assert.equal(url.origin, 'https://accounts.google.com');
  assert.equal(url.searchParams.get('scope'), GMAIL_READONLY_SCOPE);
  assert.equal(url.searchParams.get('access_type'), 'offline');
  assert.equal(url.searchParams.get('include_granted_scopes'), 'false');
  assert.equal(url.searchParams.get('login_hint'), PILOT_MAILBOX);
  assert.equal(url.searchParams.get('code_challenge_method'), 'S256');
  assert.equal(url.searchParams.get('state'), pending.state);
  assert.equal(url.searchParams.get('client_id'), 'dedicated-client');
  assert.ok(!authorizationUrl.includes('secret-client-fixture'));
  assert.ok(!authorizationUrl.includes(pending.codeVerifier));
});

test('enrollment validates Google-reported mailbox/scope, encrypts refresh token, persists disabled and consumes state once', async () => {
  const { pending } = authorization();
  const calls = [];
  let saved;
  const fetchImpl = async (url, options) => {
    calls.push(url);
    assert.equal(options.redirect, 'error');
    if (url.endsWith('/token')) {
      assert.equal(options.body.get('code_verifier'), pending.codeVerifier);
      assert.equal(options.body.get('grant_type'), 'authorization_code');
      return response({ access_token: 'access-fixture', refresh_token: 'refresh-secret-fixture', scope: GMAIL_READONLY_SCOPE });
    }
    assert.equal(options.headers.Authorization, 'Bearer access-fixture');
    return response({ emailAddress: PILOT_MAILBOX });
  };
  const store = { async saveIntegration(value) { saved = value; } };
  const result = await completeOAuthAuthorization({ pending, code: 'code-fixture', state: pending.state, store, env: env(), fetchImpl, now: NOW });
  assert.equal(saved.clientId, 10);
  assert.equal(saved.integration.enabled, false);
  assert.equal(saved.integration.oauth_client_id, 'dedicated-client');
  assert.equal(saved.integration.project_id, 'fixture976');
  assert.ok(!JSON.stringify(saved).includes('refresh-secret-fixture'));
  assert.equal(decryptRefreshToken(saved.integration, env()), 'refresh-secret-fixture');
  assert.equal(result.enabled, false);
  assert.equal(calls.length, 2);
  await assert.rejects(completeOAuthAuthorization({ pending, code: 'code-fixture', state: pending.state, store, env: env(), fetchImpl, now: NOW }), { code: 'clarity_oauth_state_invalid' });
  assert.equal(calls.length, 2);
});

for (const [name, tokens, mailbox, code] of [
  ['full-mail grant', { scope: 'https://mail.google.com/', refresh_token: 'refresh-fixture' }, PILOT_MAILBOX, 'clarity_scope_not_readonly'],
  ['mixed broad grant', { scope: `${GMAIL_READONLY_SCOPE} https://www.googleapis.com/auth/gmail.send`, refresh_token: 'refresh-fixture' }, PILOT_MAILBOX, 'clarity_scope_not_readonly'],
  ['missing scope', { refresh_token: 'refresh-fixture' }, PILOT_MAILBOX, 'clarity_scope_not_readonly'],
  ['missing refresh token', { scope: GMAIL_READONLY_SCOPE }, PILOT_MAILBOX, 'clarity_oauth_refresh_token_missing'],
  ['wrong account', { scope: GMAIL_READONLY_SCOPE, refresh_token: 'refresh-fixture' }, 'other@example.com', 'clarity_mailbox_mismatch'],
]) test(`OAuth rejects ${name} without writing integration`, async () => {
  const { pending } = authorization();
  let saves = 0;
  const fetchImpl = async (url) => response(url.endsWith('/token') ? { access_token: 'access-fixture', ...tokens } : { emailAddress: mailbox });
  await assert.rejects(completeOAuthAuthorization({ pending, code: 'code-fixture', state: pending.state, store: { async saveIntegration() { saves += 1; } }, env: env(), fetchImpl, now: NOW }), { code });
  assert.equal(saves, 0);
});

test('invalid/expired OAuth state, unauthorized tenant and non-loopback redirect fail before network', async () => {
  const { pending } = authorization();
  const fetchImpl = async () => assert.fail('no network');
  await assert.rejects(completeOAuthAuthorization({ pending, code: 'code', state: 'wrong', env: env(), fetchImpl, now: NOW }), { code: 'clarity_oauth_state_invalid' });
  await assert.rejects(completeOAuthAuthorization({ pending, code: 'code', state: pending.state, env: env(), fetchImpl, now: NOW + 600001 }), { code: 'clarity_oauth_state_invalid' });
  assert.throws(() => createOAuthAuthorization({ clientId: 11, mailboxEmail: PILOT_MAILBOX, digestSender: 'fixture@microsoft.com', projectId: 'fixture976', redirectUri: 'http://127.0.0.1:8787/oauth/callback', env: env() }), { code: 'clarity_tenant_not_authorized' });
  assert.throws(() => createOAuthAuthorization({ clientId: 10, mailboxEmail: PILOT_MAILBOX, digestSender: 'fixture@microsoft.com', projectId: 'fixture976', redirectUri: 'https://attacker.example/oauth/callback', env: env() }), { code: 'clarity_redirect_invalid' });
});

test('AES-GCM uses randomized ciphertext and authenticated tenant/mailbox/consent/client/sender bindings', () => {
  const first = integration();
  const second = integration();
  assert.notEqual(first.refresh_token_encrypted, second.refresh_token_encrypted);
  assert.equal(decryptRefreshToken(first, env()), 'refresh-secret-fixture');
  for (const changes of [{ client_id: 11 }, { mailbox_email: 'other@example.com' }, { consented_at: '2026-10-18T00:00:00Z' }, { oauth_client_id: 'different-client' }, { digest_sender: 'different@microsoft.com' }, { project_id: 'different976' }, { oauth_scopes: ['https://mail.google.com/'] }, { consent_status: 'revoked' }]) {
    assert.throws(() => decryptRefreshToken({ ...first, ...changes }, env()));
  }
  assert.throws(() => decryptRefreshToken(first, env({ CLARITY_TOKEN_ENCRYPTION_KEY: Buffer.alloc(32, 2).toString('base64') })), { code: 'clarity_token_decryption_failed' });
  assert.throws(() => decryptRefreshToken({ ...first, token_key_id: 'v2' }, env()), { code: 'clarity_encryption_key_version' });
});

test('Gmail REST validates profile and confines query to verified sender/time bounds, never IMAP', async () => {
  const calls = [];
  const fetchImpl = async (url, options) => {
    calls.push({ url, options });
    if (url.endsWith('/token')) return response({ access_token: 'access-fixture', expires_in: 3600, scope: GMAIL_READONLY_SCOPE });
    if (url.endsWith('/profile')) return response({ emailAddress: PILOT_MAILBOX });
    if (url.includes('/messages?')) return response({ messages: [{ id: 'm1' }], nextPageToken: 'page2' });
    return response({ id: 'm1', payload: {} });
  };
  const gmail = createClarityGmail({ env: env(), fetchImpl, now: () => NOW });
  const grant = integration();
  const result = await gmail.listMessages({ clientId: 10, integration: grant, after: '2026-10-01T00:00:00Z', before: '2026-10-20T00:00:00Z', maxResults: 25 });
  assert.equal(result.messages[0].id, 'm1');
  assert.equal(result.nextPageToken, 'page2');
  assert.equal(new URL(calls[2].url).searchParams.get('q'), 'from:fixture@microsoft.com subject:Clarity after:1790812800 before:1792454400');
  await gmail.getMessage({ clientId: 10, integration: grant, messageId: 'm1' });
  assert.equal(calls.filter((call) => call.url.endsWith('/token')).length, 1);
  assert.equal(calls.filter((call) => call.url.endsWith('/profile')).length, 1);
  assert.match(calls.at(-1).url, /\/messages\/m1\?format=full$/);
  assert.ok(calls.every((call) => !call.url.includes('access-fixture') && !call.url.includes('refresh-secret-fixture')));
});

test('both dark flags, disabled grant, missing consent, mismatched tenant/scope/client fail without any fetch', async () => {
  const valid = integration();
  const attempts = [
    { environment: env({ CLARITY_ENABLED: 'false' }), grant: valid },
    { environment: env({ CLARITY_INGESTION_ENABLED: 'false' }), grant: valid },
    { grant: { ...valid, enabled: false } },
    { grant: { ...valid, consent_status: 'revoked' } },
    { grant: { ...valid, client_id: 11 } },
    { grant: { ...valid, oauth_scopes: ['https://mail.google.com/'] } },
    { grant: { ...valid, oauth_client_id: 'wrong' } },
  ];
  for (const attempt of attempts) {
    const gmail = createClarityGmail({ env: attempt.environment || env(), fetchImpl: async () => assert.fail('no network'), now: () => NOW });
    await assert.rejects(gmail.getMessage({ clientId: 10, integration: attempt.grant, messageId: 'm1' }));
  }
});

test('legacy shared OAuth env is never reused and actual refreshed account/scope are checked', async () => {
  const grant = integration();
  const legacyEnv = env({ CLARITY_GOOGLE_CLIENT_ID: undefined, GOOGLE_CLIENT_ID: 'legacy', GOOGLE_CLIENT_SECRET: 'legacy-secret', ANCHOR_GOOGLE_REFRESH_TOKEN: 'legacy-refresh' });
  await assert.rejects(createClarityGmail({ env: legacyEnv, fetchImpl: async () => assert.fail('no network') }).getMessage({ clientId: 10, integration: grant, messageId: 'm1' }), { code: 'clarity_oauth_client_missing' });
  for (const [scope, mailbox, code] of [[GMAIL_READONLY_SCOPE, 'wrong@example.com', 'clarity_mailbox_mismatch'], ['https://mail.google.com/', PILOT_MAILBOX, 'clarity_scope_not_readonly']]) {
    const gmail = createClarityGmail({ env: env(), fetchImpl: async (url) => response(url.endsWith('/token') ? { access_token: 'test-token', scope } : { emailAddress: mailbox }) });
    await assert.rejects(gmail.getMessage({ clientId: 10, integration: grant, messageId: 'm1' }), { code });
  }
});

test('provider errors are bounded, classified and contain no remote body or secrets', async () => {
  for (const [status, retryable] of [[400, false], [429, true], [500, true]]) {
    const gmail = createClarityGmail({ env: env(), fetchImpl: async () => response({ error: 'bad_request', error_description: 'secret-client-fixture refresh-secret-fixture EMAIL BODY' }, status) });
    await assert.rejects(gmail.getMessage({ clientId: 10, integration: integration(), messageId: 'm1' }), (error) => error.code === 'clarity_google_request_failed' && error.retryable === retryable && !JSON.stringify(error).includes('fixture') && !error.message.includes('EMAIL'));
  }
});

test('no arbitrary Gmail queries/URLs or unlimited backfill are accepted', async () => {
  const gmail = createClarityGmail({ env: env(), fetchImpl: async () => assert.fail('no network') });
  const grant = integration();
  await assert.rejects(gmail.listMessages({ clientId: 10, integration: grant, after: '2020-01-01', before: '2026-01-01' }), { code: 'clarity_search_bounds_invalid' });
  await assert.rejects(gmail.getMessage({ clientId: 10, integration: grant, messageId: '../profile' }), { code: 'clarity_message_id_invalid' });
});

test('verified project binding is mandatory before OAuth and cannot be changed under encrypted grant', () => {
  assert.throws(() => createOAuthAuthorization({ clientId: 10, mailboxEmail: PILOT_MAILBOX, digestSender: 'fixture@microsoft.com', redirectUri: 'http://127.0.0.1:8787/oauth/callback', env: env() }), { code: 'clarity_project_id_missing' });
  const original = integration();
  assert.throws(() => decryptRefreshToken({ ...original, project_id: 'otherproject976' }, env()), { code: 'clarity_token_decryption_failed' });
});

test('only a structured Gmail pageToken error is classified for bounded cursor recovery', async () => {
  const grant = integration();
  const gmail = createClarityGmail({ env: env(), fetchImpl: async (url) => {
    if (url.endsWith('/token')) return response({ access_token: 'access-fixture', expires_in: 3600 });
    if (url.endsWith('/profile')) return response({ emailAddress: PILOT_MAILBOX });
    return response({ error: { errors: [{ reason: 'invalidArgument', location: 'pageToken', message: 'invalid sensitive token content never copied' }] } }, 400);
  } });
  await assert.rejects(gmail.listMessages({ clientId: 10, integration: grant, after: '2026-10-01T00:00:00Z', before: '2026-10-20T00:00:00Z', pageToken: 'stale' }), { code: 'clarity_page_token_rejected' });
});

test('exact 90-day search boundary is accepted, one extra second fails before reads', async () => {
  let calls = 0;
  const gmail = createClarityGmail({ env: env(), now: () => NOW, fetchImpl: async (url) => {
    calls += 1;
    if (url.endsWith('/token')) return response({ access_token: 'access-fixture', expires_in: 3600 });
    if (url.endsWith('/profile')) return response({ emailAddress: PILOT_MAILBOX });
    return response({ messages: [] });
  } });
  const options = { clientId: 10, integration: integration(), after: new Date(NOW - 90 * 86400000).toISOString(), before: new Date(NOW).toISOString() };
  assert.deepEqual((await gmail.listMessages(options)).messages, []);
  const previous = calls;
  await assert.rejects(gmail.listMessages({ ...options, before: new Date(NOW + 1000).toISOString() }), { code: 'clarity_search_bounds_invalid' });
  assert.equal(calls, previous);
});
