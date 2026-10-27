'use strict';

// Real routes and authentication with inert business services. No database,
// provider SDK, socket, migration, or production module initialization is used.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const express = require('express');
const identity = require('../utils/requestIdentity');
const { resolveMaxOperationalScope } = require('../utils/maxOperationalAuthorization');

function source(relative, dependencies) {
  const module = { exports: {} };
  vm.compileFunction(fs.readFileSync(path.join(__dirname, '..', relative), 'utf8'),
    ['module', 'require', 'console'], { filename: relative })(module, name => {
    assert.ok(Object.hasOwn(dependencies, name), `Unexpected dependency ${name}`);
    return dependencies[name];
  }, { error() {}, warn() {} });
  return module.exports;
}

const paths = [
  ['evaluate', 'POST', '/api/v1/max/decisions/evaluate'],
  ['scan', 'POST', '/api/v1/max/decisions/scan-expectations'],
  ['receipt', 'GET', '/api/v1/max/decisions/decision-1/receipt'],
  ['understand', 'POST', '/api/v1/max/understand'],
  ['ingest', 'POST', '/api/v1/max/ingest'],
  ['overdue', 'GET', '/api/v1/max/ingest/expectations/overdue'],
  ['composer', 'POST', '/api/v1/max/composer'],
  ['voice', 'POST', '/api/v1/max/voice/transcribe'],
  ['retry', 'POST', '/api/v1/max/voice/recordings/recording-1/transcribe'],
];

function fixture() {
  const users = [
    { id: 3, role: 'admin', client_id: 1, active: true },
    { id: 5, role: 'manager', client_id: 10, active: true },
    { id: 7, role: 'ao', client_id: 10, active: true },
    { id: 8, role: 'manager', client_id: 17, active: true },
  ];
  const calls = [];
  let available = true, tenantActive = true;
  const db = { async query(sql, args) {
    assert.match(sql, /^SELECT /);
    if (!available) throw new Error('private credential or database detail');
    if (sql.includes('FROM users')) return { rows: users.filter(user => args[0].includes(user.id)) };
    if (sql.includes('FROM clients')) return { rows: tenantActive ? [{ id: args[0] }] : [] };
    assert.match(sql, /FROM max_operational_decisions WHERE id = \$1 AND client_id = \$2/);
    calls.push(['receipt', args[1]]);
    return { rows: [{ id: args[0] }] };
  } };
  const auth = source('middleware/auth.js', { bcryptjs: {}, '../db': db,
    '../utils/userRoles': require('../utils/userRoles'), '../utils/usersRoleConstraint': {},
    '../utils/requestIdentity': identity });
  const scoped = { resolveMaxOperationalScope };
  const decision = source('routes/maxDecisionExecution.js', { express, '../middleware/auth': auth,
    '../db': db, '../utils/maxOperationalAuthorization': scoped,
    '../services/maxDecisionExecutionService': {
      evaluateDecision: async id => { calls.push(['evaluate', id]); return {}; },
      runExpectationDecisionScan: async id => { calls.push(['scan', id]); return []; },
    } });
  const passthrough = () => (_req, _res, next) => next();
  const multer = () => ({ any: passthrough, fields: passthrough });
  multer.memoryStorage = () => ({});
  const state = source('routes/maxStateIngestion.js', {
    express, multer, '../middleware/auth': auth, '../utils/maxOperationalAuthorization': scoped,
    '../db': db, '../utils/requestIdentity': identity,
    './accountReview': { createAccountReviewRouter: () => express.Router() },
    '../services/aoImpersonationService': { logImpersonationAction: async () => {} },
    '../utils/maxSpreadsheetAuthorization': { resolveSpreadsheetScope: async () => assert.fail('no spreadsheet call') },
    '../services/maxSpreadsheetService': {
      hasSpreadsheetInput: () => false, errorStatus: error => error.statusCode || 500,
      publicError: error => ({ error: error.code }),
    },
    '../services/maxStateIngestionService': {
      ingestOperationalEvidence: async (id, body) => { calls.push(['ingest', id, body]); return { commit_blocked: true }; },
      listOverdueExpectationPrompts: async id => { calls.push(['overdue', id]); return []; },
    },
    '../services/maxComposerService': { submitMaxComposerTurn: async (id, body) => {
      calls.push(['composer', id, body]); return { ok: true, preview_only: true };
    } },
    '../services/maxDecisionExecutionService': { afterIngestionDecisions: async () => assert.fail('preview only') },
    '../packages/max/understanding': {
      interpretConversationalInput: () => assert.fail('durable context expected'),
      interpretWithDurableConversationContext: async input => { calls.push(['understand', input.clientId, input]); return {}; },
      PostgresConversationMemoryRepository: class { async init() {} },
    },
    '../packages/max/composer': { createMaxAttachment: input => input },
    '../packages/max/composer/limits': { LIMITS: { maxFileBytes: 1000, maxAttachmentsPerTurn: 2, maxVoiceDurationMs: 1000 } },
    '../services/maxVoiceService': {
      createVoiceTranscriptionAdapter: () => { calls.push(['adapter']); return {}; },
      uploadAndTranscribeVoice: async (id, body) => { calls.push(['voice', id, body]); return { recording: { id: 'recording-1' } }; },
      retryTranscription: async (id, recordingId) => { calls.push(['retry', id, recordingId]); return { recording: { id: recordingId } }; },
    },
  });
  const router = express.Router(); router.use(decision); router.use(state);
  async function request(route, { body = {}, query = {}, userId = 3, session, activeClient = 10 } = {}) {
    const [, method, url] = route;
    const req = { method, url, originalUrl: url, path: url, query,
      body: { text: 'Fixture statement', conversation_id: 'conversation-1', ...body },
      files: [{ fieldname: 'voice', buffer: Buffer.from('inert fixture'), mimetype: 'audio/wav' }],
      session: session || { user: { ...users.find(user => user.id === userId) }, active_client_id: activeClient },
      is: () => false, get: () => 'application/json' };
    return new Promise((resolve, reject) => {
      const res = { statusCode: 200, status(value) { this.statusCode = value; return this; },
        json(body) { resolve({ status: this.statusCode, body }); } };
      router.handle(req, res, error => reject(error || new Error('unhandled route')));
    });
  }
  return { users, calls, request, db, fail: () => { available = false; },
    deactivateTenant: () => { tenantActive = false; } };
}

for (const route of paths) {
  test(`${route[0]}: omitted selectors use selected admin tenant, never implicit tenant 1`, async () => {
    const f = fixture();
    const result = await f.request(route);
    assert.equal(result.status, 200, JSON.stringify(result));
    assert.equal(f.calls.find(call => call[0] === route[0])?.[1], 10);
  });
  test(`${route[0]}: same-tenant aliases agree; explicit cross-tenant and conflicting aliases fail before work`, async () => {
    const f = fixture();
    const good = await f.request(route, { userId: 5, activeClient: 1,
      body: { client_id: 10, clientId: '10' }, query: { client_id: '10', clientId: 10 } });
    assert.equal(good.status, 200);
    const before = f.calls.length;
    for (const overrides of [{ body: { client_id: 17 } }, { query: { clientId: 17 } },
      { body: { client_id: 10, clientId: 17 } }, { body: { client_id: 10 }, query: { client_id: 17 } }]) {
      const result = await f.request(route, { userId: 5, ...overrides });
      assert.equal(result.status, 403); assert.equal(result.body.error, 'tenant_scope_mismatch');
    }
    assert.equal(f.calls.length, before);
  });
  test(`${route[0]}: malformed selectors fail closed, never normalize or ignore conflicting aliases`, async () => {
    const f = fixture();
    for (const value of ['', null, true, [10], {}, '10x', '01', 0, -1, 1.5, '9007199254740992']) {
      const result = await f.request(route, { body: { client_id: value } });
      assert.equal(result.status, 400, JSON.stringify(value));
      assert.equal(result.body.error, 'invalid_client_id');
    }
    assert.deepEqual(f.calls, []);
  });
  test(`${route[0]}: revoked actor, stale role, legacy identity, inactive tenant and DB failure cannot act`, async () => {
    for (const kind of ['revoked', 'demoted', 'legacy', 'inactiveTenant', 'unavailable']) {
      const f = fixture();
      const session = { user: { id: 3, role: 'admin', client_id: 10 }, active_client_id: 10 };
      if (kind === 'revoked') f.users[0].active = false;
      if (kind === 'demoted') f.users[0].role = 'viewer';
      if (kind === 'legacy') { delete session.user; session.authenticated = true; }
      if (kind === 'inactiveTenant') f.deactivateTenant();
      if (kind === 'unavailable') f.fail();
      const result = await f.request(route, { session });
      assert.equal(result.status, kind === 'unavailable' ? 503 : 403, kind);
      assert.doesNotMatch(JSON.stringify(result.body), /private credential|database detail/);
      assert.deepEqual(f.calls, []);
    }
  });
  test(`${route[0]}: impersonation uses current effective binding and cannot inherit admin authority`, async () => {
    const f = fixture();
    const session = { user: { id: 3, role: 'admin' }, active_client_id: 10,
      impersonation: { active: true, effectiveUser: { id: 8, role: 'admin', client_id: 10 } } };
    assert.equal((await f.request(route, { session, body: { client_id: 10 } })).status, 403);
    assert.equal((await f.request(route, { session, body: { client_id: 17 } })).status, 200);
    assert.equal(f.calls.find(call => call[0] === route[0])?.[1], 17);
    const before = f.calls.length;
    session.user = { id: 5, role: 'manager' };
    assert.equal((await f.request(route, { session, body: { client_id: 17 } })).status, 403);
    assert.equal(f.calls.length, before);
  });
}

test('composer and voice retain current AO access; decision and ingestion do not gain AO authority', async () => {
  for (const route of paths) {
    const f = fixture();
    const allowed = ['composer', 'voice', 'retry'].includes(route[0]);
    const result = await f.request(route, { userId: 7, activeClient: 17 });
    assert.equal(result.status, allowed ? 200 : 403, route[0]);
    if (allowed) assert.equal(f.calls.find(call => call[0] === route[0])?.[1], 10);
    else assert.deepEqual(f.calls, []);
  }
});

test('ingestion and composer provenance is current server identity, not supplied actor or stale session role', async () => {
  for (const name of ['ingest', 'composer']) {
    const f = fixture();
    const session = { user: { id: 5, role: 'admin', client_id: 17 }, active_client_id: 17 };
    const result = await f.request(paths.find(route => route[0] === name), { session,
      body: { actor: { userId: 3, role: 'admin' }, source_actor: 3, sourceActor: 3 } });
    assert.equal(result.status, 200);
    const call = f.calls.find(call => call[0] === name);
    assert.equal(call[1], 10); assert.equal(call[2].actor.userId, 5);
    assert.equal(call[2].actor.role, 'manager');
    if (name === 'ingest') assert.equal(call[2].source_actor, 5);
  }
});
