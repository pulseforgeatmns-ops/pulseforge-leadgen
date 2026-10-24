'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { createClarityIngestion, LIMITS } = require('../services/clarityIngestion');
const { GMAIL_READONLY_SCOPE, clarityError } = require('../services/clarityGmail');

const NOW = Date.parse('2026-10-20T00:00:00Z');
const ENV = { CLARITY_ENABLED: 'true', CLARITY_INGESTION_ENABLED: 'true', CLARITY_GOOGLE_CLIENT_ID: 'fixture-client', CLARITY_GOOGLE_CLIENT_SECRET: 'fixture-secret' };
function message(id = 'm1', body = 'Clarity weekly digest\nOct 11 - 17, 2026\nSessions: 232 +180%\nPages per session: 1.22 +11.7%') { return { id, internalDate: String(NOW), payload: { mimeType: 'text/plain', headers: [{ name: 'Subject', value: 'Clarity weekly digest' }, { name: 'From', value: 'fixture@microsoft.com' }, { name: 'Authentication-Results', value: 'mx.google.com; dkim=pass header.i=@microsoft.com; dmarc=pass header.from=microsoft.com' }], body: { data: Buffer.from(body + '\nhttps://clarity.microsoft.com/projects/view/fixture976/dashboard').toString('base64url') } } }; }
function harness({ grant = {}, pages, messageProvider } = {}) {
  const state = { held: false, lockCalls: 0, listCalls: [], fetchCalls: [], runs: [], snapshots: [], callbacks: [], sleeps: [], grant: { client_id: 10, mailbox_email: 'jacob@goanchorcleaning.com', digest_sender: 'fixture@microsoft.com', project_id: 'fixture976', enabled: true, consent_status: 'authorized', consented_at: '2026-10-19T00:00:00Z', oauth_client_id: 'fixture-client', oauth_scopes: [GMAIL_READONLY_SCOPE], ...grant } };
  const store = {
    async withIngestionLock({ clientId }, fn) {
      assert.equal(clientId, 10);
      state.lockCalls += 1;
      if (state.held) return { skipped: true, reason: 'already_running' };
      state.held = true;
      try { return await fn(store); } finally { state.held = false; }
    },
    async getIntegration({ clientId }) { assert.equal(clientId, 10); return state.grant; },
    async listDueIntegrations() { return [state.grant]; },
    async saveSnapshot({ clientId, snapshot }) {
      assert.equal(clientId, 10);
      const prior = state.snapshots.find((s) => s.window_start === snapshot.window_start && s.window_end === snapshot.window_end);
      if (prior) return { inserted: false, snapshot: prior };
      const row = { id: `snapshot-${state.snapshots.length + 1}`, client_id: clientId, ...snapshot };
      state.snapshots.push(row);
      return { inserted: true, snapshot: row };
    },
    async recordIngestionRun(run) { state.runs.push(run); state.grant.next_run_at = run.nextRunAt; state.grant.ingestion_cursor = run.cursor; },
  };
  const gmail = {
    async listMessages(options) { state.listCalls.push(options); return pages ? pages(options, state) : { messages: [{ id: 'm1' }], nextPageToken: null }; },
    async getMessage(options) { state.fetchCalls.push(options); return messageProvider ? messageProvider(options, state) : message(options.messageId); },
  };
  function service(overrides = {}) { return createClarityIngestion({ store, gmail, env: ENV, now: () => NOW, sleep: async (ms) => state.sleeps.push(ms), onSnapshot: async (entry) => state.callbacks.push(entry), ...overrides }); }
  return { state, store, gmail, service };
}

test('dark flags prohibit database access, OAuth and network even for scheduler', async () => {
  for (const flags of [{}, { CLARITY_ENABLED: 'true' }, { CLARITY_INGESTION_ENABLED: 'true' }]) {
    const boom = new Proxy({}, { get() { assert.fail('dark mode must not access a dependency'); } });
    const service = createClarityIngestion({ store: boom, gmail: boom, env: flags });
    assert.deepEqual(await service.run({ clientId: 10 }), { skipped: true, reason: 'disabled' });
    assert.deepEqual(await service.runDue(), { skipped: true, reason: 'disabled', runs: [] });
  }
});

test('explicit pilot tenant and bounded backfill mandatory', async () => {
  const h = harness();
  for (const clientId of [undefined, null, '10', 1, 11]) await assert.rejects(h.service().run({ clientId }), { code: 'clarity_tenant_not_authorized' });
  for (const backfillDays of [0, 6, 91, 1.5, '21']) await assert.rejects(h.service().run({ clientId: 10, backfillDays }), { code: 'clarity_backfill_bounds_invalid' });
  assert.equal(h.state.lockCalls, 0);
});

test('ingestion persists aggregates once and recommendation retry repairs partial work without duplicate snapshots', async () => {
  const h = harness();
  let failOnce = true;
  const service = h.service({ onSnapshot: async ({ snapshot }) => { assert.equal(snapshot.id, 'snapshot-1'); if (failOnce) { failOnce = false; throw new Error('private failure contents'); } h.state.callbacks.push(snapshot); } });
  const failed = await service.run({ clientId: 10 });
  assert.equal(failed.status, 'failed');
  assert.equal(failed.error_code, 'clarity_ingestion_failed');
  assert.equal(h.state.snapshots.length, 1);
  const repaired = await service.run({ clientId: 10, force: true });
  assert.equal(repaired.status, 'completed');
  assert.equal(repaired.counts.duplicate, 1);
  assert.equal(repaired.counts.inserted, 0);
  assert.equal(h.state.snapshots.length, 1);
  assert.equal(h.state.callbacks.length, 1);
  assert.ok(!JSON.stringify(h.state.runs).includes('private failure contents'));
});

test('deduplicates repeated message delivery inside a page and reporting interval across deliveries', async () => {
  const h = harness({ pages: () => ({ messages: [{ id: 'm1' }, { id: 'm1' }, { id: 'm2' }] }) });
  const result = await h.service().run({ clientId: 10 });
  assert.equal(result.status, 'completed');
  assert.equal(result.counts.inserted, 1);
  assert.equal(result.counts.duplicate, 2);
  assert.equal(h.state.fetchCalls.length, 2);
  assert.equal(h.state.callbacks.length, 2);
  assert.equal(h.state.snapshots.length, 1);
  assert.equal(h.state.runs[0].nextRunAt, '2026-10-27T00:00:00.000Z');
});

test('transient network/429/5xx error retries at most three times with finite exponential delay', async () => {
  let failures = 0;
  const h = harness({ pages: () => { failures += 1; if (failures < 3) throw clarityError('clarity_google_request_failed', { retryable: true, httpStatus: 429 }); return { messages: [] }; } });
  const result = await h.service().run({ clientId: 10 });
  assert.equal(result.status, 'no_digest');
  assert.deepEqual(h.state.sleeps, [250, 500]);
  assert.equal(h.state.listCalls.length, 3);
  assert.equal(result.next_run_at, '2026-10-21T00:00:00.000Z');
});

test('exhausted transient failure schedules persisted retry rather than claiming completion', async () => {
  const h = harness({ pages: () => { throw clarityError('clarity_google_network_error', { retryable: true }); } });
  const result = await h.service().run({ clientId: 10 });
  assert.equal(result.status, 'failed');
  assert.equal(result.error_code, 'clarity_google_network_error');
  assert.equal(h.state.listCalls.length, LIMITS.attempts);
  assert.equal(result.next_run_at, '2026-10-20T01:00:00.000Z');
  assert.ok(h.state.runs[0].cursor);
});

test('revoked/non-readonly grant never reaches provider; permanent provider failure is not retried', async () => {
  const denied = harness({ grant: { consent_status: 'revoked' } });
  assert.equal((await denied.service().run({ clientId: 10 })).error_code, 'clarity_consent_missing');
  assert.equal(denied.state.listCalls.length, 0);
  const permanent = harness({ pages: () => { throw clarityError('clarity_oauth_reauthorization_required'); } });
  assert.equal((await permanent.service().run({ clientId: 10 })).status, 'failed');
  assert.equal(permanent.state.listCalls.length, 1);
  assert.equal(permanent.state.sleeps.length, 0);
});

test('malformed/non-digest records preserve source/hash/error provenance, not bodies or secrets', async () => {
  const body = 'Clarity weekly digest\nOct 11 - 17, 2026\nSessions: malformed EMAIL PRIVATE CONTENT\nPages per session: 1.22';
  const h = harness({ messageProvider: () => message('m1', body) });
  const result = await h.service().run({ clientId: 10 });
  assert.equal(result.counts.rejected, 1);
  assert.equal(h.state.snapshots.length, 0);
  assert.equal(h.state.callbacks.length, 0);
  const rejection = h.state.runs[0].rejections[0];
  assert.equal(rejection.code, 'metric_invalid_sessions');
  assert.equal(rejection.provenance.gmail_message_id, 'm1');
  assert.match(rejection.provenance.content_sha256, /^[a-f0-9]{64}$/);
  assert.ok(!JSON.stringify(h.state.runs).includes('EMAIL PRIVATE'));
});

test('bounded pagination persists cursor and resumes exact query, rather than losing old digest pages', async () => {
  const h = harness({ pages: ({ pageToken }) => ({ messages: [], nextPageToken: pageToken === 'page3' ? null : pageToken === 'page2' ? 'page3' : 'page2' }) });
  const first = await h.service().run({ clientId: 10, backfillDays: 90 });
  assert.equal(first.status, 'partial');
  assert.equal(h.state.listCalls.length, 2);
  assert.equal(h.state.grant.ingestion_cursor.page_token, 'page3');
  const saved = { ...h.state.grant.ingestion_cursor };
  const second = await h.service().run({ clientId: 10, force: true });
  assert.equal(second.status, 'no_digest');
  assert.equal(h.state.listCalls[2].pageToken, 'page3');
  assert.equal(h.state.listCalls[2].after, saved.after);
  assert.equal(h.state.listCalls[2].before, saved.before);
  assert.equal(h.state.grant.ingestion_cursor, null);
});

test('repeated page token and forged cursor cannot create infinite or unbounded reads', async () => {
  const repeat = harness({ pages: () => ({ messages: [], nextPageToken: 'same' }) });
  assert.equal((await repeat.service().run({ clientId: 10 })).error_code, 'clarity_page_token_invalid');
  assert.equal(repeat.state.listCalls.length, 2);
  const forged = harness({ grant: { ingestion_cursor: { after: '2020-01-01', before: '2026-10-20', page_token: null } } });
  assert.equal((await forged.service().run({ clientId: 10 })).error_code, 'clarity_cursor_invalid');
  assert.equal(forged.state.listCalls.length, 0);
});

test('scheduled cadence, disabled integration, concurrency lock all skip without Gmail', async () => {
  for (const [grant, expected] of [[{ next_run_at: '2026-10-21T00:00:00Z' }, 'not_due'], [{ enabled: false }, 'integration_disabled']]) {
    const h = harness({ grant });
    assert.equal((await h.service().run({ clientId: 10 })).reason, expected);
    assert.equal(h.state.listCalls.length, 0);
  }
  const h = harness();
  h.state.held = true;
  assert.equal((await h.service().run({ clientId: 10 })).reason, 'already_running');
  assert.equal(h.state.listCalls.length, 0);
});

test('runDue loads only bounded due integrations and rechecks authorization inside lock', async () => {
  const h = harness();
  const result = await h.service().runDue();
  assert.equal(result.runs[0].client_id, 10);
  assert.equal(result.runs[0].status, 'completed');
  await assert.rejects(h.service().runDue({ limit: 11 }), { code: 'clarity_scheduler_limit_invalid' });
  h.store.listDueIntegrations = async () => [{ client_id: 11 }];
  await assert.rejects(h.service().runDue(), { code: 'clarity_tenant_not_authorized' });
});

test('run deadline is bounded and saved for retry even when provider keeps stalling', async () => {
  const h = harness();
  let clockCalls = 0;
  const service = h.service({ now: () => NOW + (clockCalls++ < 1 ? 0 : LIMITS.runtimeMs + 1) });
  const result = await service.run({ clientId: 10 });
  assert.equal(result.status, 'failed');
  assert.equal(result.error_code, 'clarity_run_deadline');
  assert.equal(h.state.listCalls.length, 0);
});

test('different projects in same mailbox/week never collide as valid evidence', async () => {
  const h = harness({ pages: () => ({ messages: [{ id: 'wrong-project' }, { id: 'correct-project' }] }), messageProvider: ({ messageId }) => {
    const mail = message(messageId);
    if (messageId === 'wrong-project') mail.payload.body.data = Buffer.from(Buffer.from(mail.payload.body.data, 'base64url').toString().replace('projects/view/fixture976', 'projects/view/otherfixture976')).toString('base64url');
    return mail;
  } });
  const result = await h.service().run({ clientId: 10 });
  assert.equal(result.counts.rejected, 1);
  assert.equal(result.counts.inserted, 1);
  assert.equal(h.state.snapshots[0].provenance.gmail_message_id, 'correct-project');
  assert.equal(h.state.runs[0].rejections[0].code, 'digest_project_mismatch');
});

test('mid-run revocation halts subsequent provider calls and records original consent CAS', async () => {
  const h = harness({ pages: (_options, state) => { state.grant = { ...state.grant, consent_status: 'revoked' }; return { messages: [{ id: 'm1' }] }; } });
  const result = await h.service().run({ clientId: 10 });
  assert.equal(result.error_code, 'clarity_consent_missing');
  assert.equal(h.state.fetchCalls.length, 0);
  assert.equal(h.state.runs[0].expectedConsentAt, '2026-10-19T00:00:00Z');
});

test('mid-run replaced grant halts rather than continuing with captured credentials', async () => {
  const h = harness({ pages: (_options, state) => { state.grant = { ...state.grant, consented_at: '2026-10-20T00:00:00Z' }; return { messages: [{ id: 'm1' }] }; } });
  const result = await h.service().run({ clientId: 10 });
  assert.equal(result.error_code, 'clarity_grant_changed');
  assert.equal(h.state.fetchCalls.length, 0);
});

test('positively invalid cursor restarts the same bounded window, never retains an unusable page token', async () => {
  const cursor = { after: '2026-10-01T00:00:00Z', before: '2026-10-20T00:00:00Z', page_token: 'expired' };
  const h = harness({ grant: { ingestion_cursor: cursor }, pages: () => { throw clarityError('clarity_page_token_rejected'); } });
  assert.equal((await h.service().run({ clientId: 10 })).status, 'failed');
  assert.deepEqual(h.state.grant.ingestion_cursor, { ...cursor, page_token: null });
  h.gmail.listMessages = async (options) => { assert.equal(options.pageToken, undefined); assert.equal(options.after, cursor.after); assert.equal(options.before, cursor.before); return { messages: [] }; };
  assert.equal((await h.service().run({ clientId: 10, force: true })).status, 'no_digest');
});

test('recommendation callback receives remaining deadline and overruns do not claim success', async () => {
  const h = harness();
  let current = NOW;
  const result = await h.service({ now: () => current, onSnapshot: async ({ remainingMs }) => { assert.equal(remainingMs, LIMITS.runtimeMs); current = NOW + LIMITS.runtimeMs + 1; } }).run({ clientId: 10 });
  assert.equal(result.status, 'failed');
  assert.equal(result.error_code, 'clarity_run_deadline');
});
