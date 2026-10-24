'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
const guard = require('../utils/callEligibility');
const boundary = require('../services/governedOutboundProviderBoundary');

function sender(pool, { featureEnabled = true, commitError = false } = {}) {
  let requests = 0;
  const db = commitError ? { async connect() {
    const client = await pool.connect();
    return { release: client.release.bind(client), async query(config) {
      if (config.text === 'COMMIT') throw Object.assign(new Error('Synthetic lost COMMIT response'), { code: '08006' });
      return client.query(config);
    } };
  } } : pool;
  const provider = { exports: {} };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../packages/providers/brevo/sendEmail.js'), 'utf8'), {
    module: provider, exports: provider.exports, process: { env: {} },
    require(id) {
      if (id === 'axios') return { post: async () => { requests++; return { data: { messageId: 'fixture-receipt' } }; } };
      if (id === '../../../utils/callEligibility') return guard;
      throw new Error('Unexpected dependency');
    },
  });
  const adapter = { exports: {} };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../services/governedOutboundAdapters.js'), 'utf8'), {
    module: adapter, exports: adapter.exports,
    process: { env: featureEnabled ? { ANCHOR_BREVO_BOUNDARY_DIAGNOSTICS_ENABLED: 'true' } : {} },
    require(id) { return id === '../packages/providers/brevo/sendEmail' ? provider.exports : {}; },
  });
  const send = adapter.exports.adapters(db, { governedContext: { tenantId: '10', clientId: 10, transport: { usesBrevoTransport: true } } })
    .sendFor({}, { item: { prospect_id: 'contact-fixture' } });
  return { async execute() {
    const tracker = boundary.createProviderBoundaryTracker();
    boundary.markLeafProviderSend(send, tracker);
    const result = await send({ toEmail: 'fixture@example.test', subject: 'Synthetic', body: 'Synthetic', apiKey: 'fixture-only',
      providerBoundary: tracker, outreachAuthorization: { pool: db, prospectId: 'contact-fixture', clientId: 10 } });
    return { result, crossed: tracker.crossed, requests };
  } };
}

test('real PostgreSQL preserves original 42703 boundary and post-dispatch uncertainty', { timeout: 60000 }, async t => {
  const instance = await startDisposablePostgres('anchor-boundary-');
  assert.equal(new URL(instance.connectionString).hostname, '127.0.0.1');
  const pool = createTrackedPostgresPool({ connectionString: instance.connectionString });
  t.after(() => closePostgresFixture(pool, instance));
  await pool.query('CREATE TABLE prospects(id text, client_id integer, do_not_contact boolean, is_synthetic boolean); INSERT INTO prospects VALUES(\'contact-fixture\',10,false,false)');
  await t.test('exact original missing call-only column is pre-provider when scoped instrumentation is enabled', async () => {
    const actual = await sender(pool).execute();
    assert.equal(actual.result.providerErrorCode, '42703');
    assert.match(actual.result.providerErrorMessage, /column "ao_call_suppressed" does not exist/);
    assert.equal(actual.result.failureStage, 'authorization');
    assert.equal(actual.result.providerBoundaryCrossed, false);
    assert.equal(actual.crossed, false);
    assert.equal(actual.requests, 0);
  });
  await t.test('default-disabled path preserves existing conservative tracker', async () => {
    const actual = await sender(pool, { featureEnabled: false }).execute();
    assert.equal(actual.result.providerErrorCode, '42703');
    assert.equal(actual.crossed, true);
    assert.equal(actual.requests, 0);
  });
  await pool.query('ALTER TABLE prospects ADD COLUMN ao_call_suppressed boolean NOT NULL DEFAULT false');
  await t.test('missing mandatory review column remains a zero-send schema failure', async () => {
    const actual = await sender(pool).execute();
    assert.equal(actual.result.providerErrorCode, '42703');
    assert.match(actual.result.providerErrorMessage, /column "ao_outreach_review_required" does not exist/);
    assert.equal(actual.crossed, false);
    assert.equal(actual.requests, 0);
  });
  await pool.query('ALTER TABLE prospects ADD COLUMN ao_outreach_review_required boolean NOT NULL DEFAULT false');
  await t.test('existing reviewed fixture can reach only the stub provider', async () => {
    const actual = await sender(pool).execute();
    assert.equal(actual.result.success, true);
    assert.equal(actual.crossed, true);
    assert.equal(actual.requests, 1);
  });
  await t.test('post-HTTP database COMMIT error stays crossed and uncertain', async () => {
    const actual = await sender(pool, { commitError: true }).execute();
    assert.equal(actual.result.success, false);
    assert.equal(actual.result.providerErrorCode, '08006');
    assert.equal(actual.result.failureStage, 'authorization_commit');
    assert.equal(actual.result.providerBoundaryCrossed, true);
    assert.equal(actual.crossed, true);
    assert.equal(actual.requests, 1);
    assert.equal(boundary.isPreProviderOutboundFailure({ code: '08006' }, { crossed: actual.crossed }), false);
  });
});

