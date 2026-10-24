'use strict';
// Actual Express routing, JSON parser, auth/identity middleware and action service.
// Only DB and downstream preparation/recovery boundaries are replaced. No providers.
const test = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const express = require('express');
const { hash, missionScope } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const PATH = '/api/v1/tenant-outreach/anchor-program/prepare-drafts';
const ORIGIN = 'https://drafts.example.invalid';
const REVISION = 'a'.repeat(40);
function stub(path, exports) {
  const id = require.resolve(path), previous = require.cache[id];
  require.cache[id] = { id, filename: id, loaded: true, exports };
  return () => { delete require.cache[id]; if (previous) require.cache[id] = previous; };
}
function request(server, { headers = {}, raw, body, method = 'POST' } = {}) {
  const payload = raw === undefined ? JSON.stringify(body) : raw;
  return new Promise((resolve, reject) => {
    const req = http.request({ hostname: '127.0.0.1', port: server.address().port,
      path: PATH, method, headers: { origin: ORIGIN, 'content-type': 'application/json', ...headers } }, res => {
      let text = ''; res.setEncoding('utf8'); res.on('data', chunk => { text += chunk; });
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, text, body: JSON.parse(text) }));
    });
    req.on('error', reject); req.end(payload);
  });
}

test('admin draft preparation HTTP boundary fails closed and returns only no-send review metadata', async t => {
  const source = { id: 'source-1', tenantId: '10', stage: 'discover', structuredMission: { immutable: true } };
  const manifest = { id: 'manifest-1', programId: 'program-1', sourceMissionId: source.id,
    operatorId: '71', operatorRole: 'admin', scopeHash: hash(missionScope(source)), admissions: [],
    scopes: { email: { enabled: false }, research: { enabled: false }, preparation: { enabled: true, operations: ['prepare_existing_crm'] } } };
  const validInput = { revision: REVISION, manifestHash: hash(manifest), sourceMissionHash: hash(source) };
  const safeResult = { missionId: 'draft-mission', operationId: 'operation-1', preparedArtifactRevision: 2,
    candidateCount: 2, readyForReview: true, executionApproved: false, envelopeAuthorized: false, sent: 0, providerCalls: 0 };
  let session, calls, connections, error, result;
  const pool = { connect: async () => {
    connections++;
    return { query: async query => { const sql = typeof query === 'string' ? query : query.text;
      return { rows: sql.startsWith('SELECT id FROM users') ? [{ id: '71' }]
        : sql.startsWith('SELECT payload') ? [{ payload: source }] : [] }; }, release() {} };
  }, query: async () => { throw Error('Unexpected shared pool query'); } };
  const restores = [
    stub('../db', pool),
    stub('../services/tenantOutreachScheduler', {}),
    stub('../services/governedMissionExecutionStatus', {}),
    stub('../services/pilotOnboarding', { FAILURE: { PASSWORD_CHANGE_REQUIRED: { code: 'password_change_required', message: 'Password change required' } } }),
    stub('../services/anchorEmailRecovery', { loadRecoveryState: async () => ({ mode: 'scoped_resume', manifest }) }),
    stub('../services/anchorScopedResumeCrmPreparation', { prepareExistingCrm: async (_pool, input, options) => {
      calls.push({ input, options }); if (error) throw error; return result;
    } }),
  ];
  const modulePaths = ['../routes/tenantOutreach', '../middleware/auth', '../services/anchorCrmPreparationAction', '../packages/max/workspace/TenantContextResolver'];
  const priorModules = modulePaths.map(path => [require.resolve(path), require.cache[require.resolve(path)]]);
  for (const [id] of priorModules) delete require.cache[id];
  const envKeys = ['APP_URL', 'RAILWAY_GIT_COMMIT_SHA', 'ANCHOR_EMAIL_SCOPED_RESUME_ID'];
  const envBefore = Object.fromEntries(envKeys.map(key => [key, process.env[key]]));
  Object.assign(process.env, { APP_URL: ORIGIN, RAILWAY_GIT_COMMIT_SHA: REVISION, ANCHOR_EMAIL_SCOPED_RESUME_ID: manifest.id });
  let server;
  try {
    const router = require('../routes/tenantOutreach');
    const app = express();
    app.use(express.json({ strict: true }));
    app.use((req, res, next) => { req.session = structuredClone(session); next(); });
    app.use(router);
    app.use((err, req, res, next) => res.status(400).json({ error: 'invalid_json' }));
    server = app.listen(0, '127.0.0.1');
    await new Promise((resolve, reject) => { server.once('listening', resolve); server.once('error', reject); });
    function reset() {
      session = { user: { id: 71, role: 'admin', active: true, client_id: 10 }, active_client_id: 10 };
      calls = []; connections = 0; error = null; result = { ...safeResult, secret: 'DO_NOT_EXPOSE', subject: 'PRIVATE_COPY', body: 'PRIVATE_BODY' };
    }
    async function denied(status, options = {}) {
      const response = await request(server, { body: validInput, ...options });
      assert.equal(response.status, status, response.text);
      assert.equal(calls.length, 0); assert.equal(connections, 0);
      return response;
    }
    const identities = [
      ['unauthenticated', () => { session = {}; }, 401],
      ['legacy admin has no real user id', () => { session = { authenticated: true, active_client_id: 10 }; }, 403],
      ...['manager', 'client', 'viewer', 'ao'].map(role => [role, () => { session.user.role = role; }, 403]),
      ['inactive admin', () => { session.user.active = false; }, 403],
      ['password reset required', () => { session.user.password_change_required = true; }, 403],
      ['impersonating admin', () => { session.impersonation = { active: true, effectiveUser: { id: 72, role: 'admin', client_id: 10 } }; }, 403],
      ['malformed active impersonation', () => { session.impersonation = { active: true }; }, 403],
      ['array-valued tenant', () => { session.active_client_id = [10]; }, 403],
      ['object-valued tenant', () => { session.active_client_id = { id: 10 }; }, 403],
      ['missing tenant', () => { delete session.active_client_id; delete session.user.client_id; }, 403],
      ['wrong tenant', () => { session.active_client_id = 11; }, 403],
      ['malformed tenant', () => { session.active_client_id = '10junk'; }, 403],
    ];
    for (const [name, mutate, status] of identities) await t.test(name, async () => { reset(); mutate(); await denied(status); });
    for (const [name, headers] of [
      ['missing Origin', { origin: '' }], ['foreign Origin', { origin: 'https://evil.example.invalid' }],
      ['Origin lookalike', { origin: ORIGIN + '.evil.invalid' }], ['cross-site fetch', { 'sec-fetch-site': 'cross-site' }],
      ['form content type', { 'content-type': 'application/x-www-form-urlencoded' }],
      ['text content type', { 'content-type': 'text/plain' }],
    ]) await t.test(name, async () => { reset(); await denied(403, { headers }); });
    for (const url of ['', 'not-a-url', 'file:///tmp/drafts']) await t.test(`invalid configured app origin ${url}`, async () => {
      reset(); process.env.APP_URL = url; try { await denied(403); } finally { process.env.APP_URL = ORIGIN; }
    });
    for (const raw of ['null', 'true', '"string"', '42', '{']) await t.test(`strict JSON parser rejects ${raw}`, async () => {
      reset(); await denied(400, { raw });
    });
    for (const body of [{}, [], { ...validInput, send: true }, { ...validInput, tenantId: 10 },
      { ...validInput, operatorId: 'other' }, { ...validInput, revision: 'bad' },
      { ...validInput, manifestHash: 'bad' }, { ...validInput, sourceMissionHash: 'bad' }]) {
      await t.test(`input rejected: ${JSON.stringify(body)}`, async () => { reset(); await denied(409, { body }); });
    }
    await t.test('verified admin action returns sanitized preparation-only receipt', async () => {
      reset(); const response = await request(server, { body: validInput });
      assert.equal(response.status, 200); assert.equal(response.headers['cache-control'], 'no-store');
      assert.deepEqual(response.body, { ...safeResult, status: 'ready_for_review', providerReadiness: 'not_checked', retryAllowed: false });
      assert.equal(calls.length, 1); assert.equal(connections, 1);
      assert.deepEqual(calls[0].options.actionAuthority, { actorId: '71', manifestHash: validInput.manifestHash, revision: REVISION });
      assert.equal(response.text.includes('PRIVATE'), false); assert.equal(response.text.includes('DO_NOT_EXPOSE'), false);
    });
    await t.test('consumed claim is not success and does not disclose downstream errors', async () => {
      reset(); error = Object.assign(Error('SECRET_DATABASE_DETAILS'), { code: 'anchor_scoped_resume_operation_consumed' });
      const response = await request(server, { body: validInput });
      assert.equal(response.status, 409); assert.equal(response.headers['cache-control'], 'no-store');
      assert.deepEqual(response.body, { error: 'anchor_crm_action_rejected', status: 'already_consumed', writesMayHaveOccurred: true, retryAllowed: false });
      assert.equal(calls.length, 1); assert.equal(response.text.includes('SECRET'), false);
    });
    await t.test('unexpected downstream failure requires review, not retry', async () => {
      reset(); error = Error('SECRET_PROVIDER_TOKEN'); const response = await request(server, { body: validInput });
      assert.equal(response.status, 409); assert.deepEqual(response.body, { error: 'anchor_crm_action_rejected', status: 'blocked_review_required', writesMayHaveOccurred: true, retryAllowed: false });
      assert.equal(calls.length, 1); assert.equal(response.text.includes('SECRET'), false);
    });
    for (const unsafe of [{ sent: 1 }, { providerCalls: 1 }, { executionApproved: true }, { envelopeAuthorized: true }, { readyForReview: false }]) {
      await t.test(`unsafe downstream receipt is never presented as success ${JSON.stringify(unsafe)}`, async () => {
        reset(); Object.assign(result, unsafe); const response = await request(server, { body: validInput });
        assert.equal(response.status, 409); assert.equal(response.body.status, 'blocked_review_required'); assert.equal(response.body.retryAllowed, false);
      });
    }
  } finally {
    if (server) await new Promise(resolve => server.close(resolve));
    for (const key of envKeys) { if (envBefore[key] === undefined) delete process.env[key]; else process.env[key] = envBefore[key]; }
    for (const [id, previous] of priorModules) { delete require.cache[id]; if (previous) require.cache[id] = previous; }
    for (const restore of restores.reverse()) restore();
  }
});
