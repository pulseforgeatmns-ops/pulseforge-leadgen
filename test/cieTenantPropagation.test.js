'use strict';

/**
 * CIE tenant propagation — admin operator session.active_client_id must
 * drive every CIE write/read; route/query/body client hints cannot override.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const express = require('express');
const session = require('express-session');

const uiPath = path.join(__dirname, '..', 'public', 'client-intel.html');
const cieAuthPath = path.join(__dirname, '..', 'utils', 'cieAuth.js');
const routePath = path.join(__dirname, '..', 'routes', 'clientIntelligence.js');
const uiSource = fs.readFileSync(uiPath, 'utf8');
const cieAuthSource = fs.readFileSync(cieAuthPath, 'utf8');
const routeSource = fs.readFileSync(routePath, 'utf8');

const {
  createMemoryStore,
  startClientInterview,
  postInterviewMessage,
  approveBlueprint,
  resolveClientOnboardingState,
  auditClientBlueprintLifecycle,
  getApprovedClientBlueprint,
  getInterview,
  ClientIntelligenceError,
} = require('../services/clientIntelligenceInterview');
const {
  resolveCieClientId,
  resolveCieCanonicalClientId,
  assertCieClientAccess,
} = require('../utils/cieAuth');

const ANCHOR_ID = 10;
const PULSEFORGE_ID = 1;

const ANSWERS = [
  'Anchor Cleaning — commercial office cleaning.',
  'Nightly office cleans and periodic deep cleans.',
  'Law firms and accounting practices in Manchester NH.',
  'Multi-tenant towers and national firms.',
  'Manchester, Bedford, Goffstown, Hooksett.',
  'Reliable crews without chasing the team.',
  'Professional and trustworthy voice.',
  'Book qualified walkthroughs in the pilot cluster.',
  'Signed recurring contracts within 90 days.',
];

function adminReq(activeClientId) {
  return {
    user: { id: 3, role: 'admin', client_id: null },
    session: { active_client_id: activeClientId, user: { role: 'admin' } },
  };
}

function listen(app) {
  const server = app.listen(0, '127.0.0.1');
  return new Promise((resolve) => {
    server.on('listening', () => {
      const { port } = server.address();
      resolve({
        server,
        base: `http://127.0.0.1:${port}`,
        async close() {
          await new Promise((r) => server.close(r));
        },
      });
    });
  });
}

async function request(base, method, urlPath, body) {
  const url = new URL(urlPath, base);
  const res = await fetch(url, {
    method,
    headers: {
      Accept: 'application/json',
      ...(body == null ? {} : { 'Content-Type': 'application/json' }),
    },
    body: body == null ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch (_) {
    json = null;
  }
  return { status: res.status, json, text };
}

function mountTenantAwareStartRoute(activeClientId, store) {
  const app = express();
  app.use(express.json());
  app.use(
    session({
      secret: 'cie-tenant-propagation-test',
      resave: false,
      saveUninitialized: false,
    })
  );
  app.use((req, _res, next) => {
    req.user = { id: 3, role: 'admin', client_id: null };
    req.session.user = req.user;
    req.session.active_client_id = activeClientId;
    next();
  });
  app.post('/api/v1/clients/:id/interview/start', async (req, res) => {
    try {
      const clientId = resolveCieClientId(req, req.params.id);
      const result = await startClientInterview(
        { clientId, restart: Boolean(req.body && req.body.restart) },
        { store, useMemoryPlaybookStore: true }
      );
      return res.status(result.resumedExisting ? 200 : 201).json(result);
    } catch (err) {
      const status = err instanceof ClientIntelligenceError ? err.status || 400 : 500;
      return res.status(status).json({
        error: err.code || 'failed',
        message: err.message,
      });
    }
  });
  app.get('/api/v1/client-intel/onboarding', async (req, res) => {
    try {
      const clientId = resolveCieClientId(req, req.query.clientId || req.query.client_id);
      const result = await resolveClientOnboardingState(clientId, {
        store,
        useMemoryPlaybookStore: true,
      });
      return res.json(result);
    } catch (err) {
      const status = err instanceof ClientIntelligenceError ? err.status || 400 : 500;
      return res.status(status).json({
        error: err.code || 'failed',
        message: err.message,
      });
    }
  });
  app.get('/api/v1/clients/:id/cie-lifecycle-audit', async (req, res) => {
    try {
      const clientId = resolveCieClientId(req, req.params.id);
      const report = await auditClientBlueprintLifecycle(clientId, {
        store,
        useMemoryPlaybookStore: true,
      });
      return res.json(report);
    } catch (err) {
      const status = err instanceof ClientIntelligenceError ? err.status || 400 : 500;
      return res.status(status).json({
        error: err.code || 'failed',
        message: err.message,
      });
    }
  });
  return app;
}

async function runInterviewToBlueprint(store, clientId) {
  const opts = { store, useMemoryPlaybookStore: true };
  const started = await startClientInterview({ clientId }, opts);
  let turn = started;
  for (const answer of ANSWERS) {
    turn = await postInterviewMessage(started.interviewId, answer, opts);
  }
  assert.ok(turn.blueprint, 'expected blueprint');
  return { started, turn, interviewId: started.interviewId, blueprint: turn.blueprint };
}

describe('CIE tenant propagation — auth helpers', () => {
  it('admin with active_client_id=10 resolves canonical tenant 10', () => {
    const req = adminReq(ANCHOR_ID);
    assert.equal(resolveCieCanonicalClientId(req), ANCHOR_ID);
    assert.equal(resolveCieClientId(req, ANCHOR_ID), ANCHOR_ID);
  });

  it('route client_id=1 rejects when session active_client_id=10', () => {
    const req = adminReq(ANCHOR_ID);
    assert.throws(
      () => resolveCieClientId(req, PULSEFORGE_ID),
      (err) =>
        err instanceof ClientIntelligenceError && err.code === 'tenant_mismatch'
    );
  });

  it('query client_id=1 rejects when session active_client_id=10', () => {
    const req = {
      ...adminReq(ANCHOR_ID),
      query: { client_id: PULSEFORGE_ID },
    };
    assert.throws(
      () => resolveCieClientId(req, PULSEFORGE_ID),
      (err) =>
        err instanceof ClientIntelligenceError && err.code === 'tenant_mismatch'
    );
  });

  it('missing tenant context fails closed instead of defaulting to client 1', () => {
    const req = adminReq(null);
    delete req.session.active_client_id;
    assert.throws(
      () => resolveCieCanonicalClientId(req),
      (err) =>
        err instanceof ClientIntelligenceError &&
        err.code === 'tenant_context_required'
    );
  });

  it('admin cannot access another tenant interview without switching session', () => {
    const req = adminReq(ANCHOR_ID);
    assert.throws(
      () => assertCieClientAccess(req, PULSEFORGE_ID),
      (err) =>
        err instanceof ClientIntelligenceError &&
        err.code === 'forbidden_client_scope'
    );
  });
});

describe('CIE tenant propagation — service lifecycle', () => {
  it('admin session tenant 10 persists session, turns, blueprint, and audit under client 10', async () => {
    const store = createMemoryStore();
    const opts = { store, useMemoryPlaybookStore: true };

    const { started, turn, blueprint } = await runInterviewToBlueprint(
      store,
      ANCHOR_ID
    );
    assert.equal(Number(started.clientId), ANCHOR_ID);

    const sessionRow = await store.getSession(started.interviewId);
    assert.equal(Number(sessionRow.client_id), ANCHOR_ID);

    const detail = await getInterview(started.interviewId, opts);
    assert.equal(Number(detail.clientId), ANCHOR_ID);

    assert.equal(Number(turn.clientId), ANCHOR_ID);
    assert.equal(Number(blueprint.clientId), ANCHOR_ID);

    const approved = await approveBlueprint(blueprint.id, opts);
    assert.equal(Number(approved.clientId || approved.blueprint.clientId), ANCHOR_ID);

    const audit = await auditClientBlueprintLifecycle(ANCHOR_ID, opts);
    assert.equal(Number(audit.clientId), ANCHOR_ID);
    assert.ok(Array.isArray(audit.sessions) && audit.sessions.length > 0);
    assert.ok(Array.isArray(audit.blueprints) && audit.blueprints.length > 0);
    assert.ok(audit.currentBlueprint);
    assert.equal(String(audit.currentBlueprint.status).toLowerCase(), 'approved');

    const maxBlueprint = await getApprovedClientBlueprint(ANCHOR_ID, opts);
    assert.equal(Number(maxBlueprint.clientId), ANCHOR_ID);
    assert.equal(String(maxBlueprint.id), String(blueprint.id));
  });

  it('client 1 lifecycle audit does not see client 10 CIE artifacts', async () => {
    const store = createMemoryStore();
    const opts = { store, useMemoryPlaybookStore: true };
    await runInterviewToBlueprint(store, ANCHOR_ID);

    const clientOneAudit = await auditClientBlueprintLifecycle(PULSEFORGE_ID, opts);
    assert.equal(Number(clientOneAudit.clientId), PULSEFORGE_ID);
    assert.equal(clientOneAudit.sessions.length, 0);
    assert.equal(clientOneAudit.blueprints.length, 0);
    assert.equal(clientOneAudit.onboarding?.onboardingState, 'none');

    await assert.rejects(
      () => getApprovedClientBlueprint(PULSEFORGE_ID, opts),
      (err) => err instanceof ClientIntelligenceError && err.status === 404
    );
  });

  it('one start produces one active interview session', async () => {
    const store = createMemoryStore();
    const opts = { store, useMemoryPlaybookStore: true };
    const first = await startClientInterview({ clientId: ANCHOR_ID }, opts);
    const second = await startClientInterview({ clientId: ANCHOR_ID }, opts);
    assert.equal(second.interviewId, first.interviewId);
    assert.equal(second.resumedExisting, true);
  });
});

describe('CIE tenant propagation — HTTP route resolution', () => {
  it('admin active_client_id=10 start via matching route persists client_id=10', async () => {
    const store = createMemoryStore();
    const app = mountTenantAwareStartRoute(ANCHOR_ID, store);
    const { base, close } = await listen(app);
    try {
      const start = await request(
        base,
        'POST',
        `/api/v1/clients/${ANCHOR_ID}/interview/start`,
        {}
      );
      assert.equal(start.status, 201);
      assert.equal(Number(start.json.clientId), ANCHOR_ID);

      const sessionRow = await store.getSession(start.json.interviewId);
      assert.equal(Number(sessionRow.client_id), ANCHOR_ID);
    } finally {
      await close();
    }
  });

  it('admin active_client_id=10 rejects start on route client_id=1', async () => {
    const store = createMemoryStore();
    const app = mountTenantAwareStartRoute(ANCHOR_ID, store);
    const { base, close } = await listen(app);
    try {
      const start = await request(
        base,
        'POST',
        `/api/v1/clients/${PULSEFORGE_ID}/interview/start`,
        {}
      );
      assert.equal(start.status, 403);
      assert.equal(start.json.error, 'tenant_mismatch');
    } finally {
      await close();
    }
  });

  it('onboarding and lifecycle audit resolve session tenant without query override', async () => {
    const store = createMemoryStore();
    const opts = { store, useMemoryPlaybookStore: true };
    const { blueprint } = await runInterviewToBlueprint(store, ANCHOR_ID);
    await approveBlueprint(blueprint.id, opts);

    const app = mountTenantAwareStartRoute(ANCHOR_ID, store);
    const { base, close } = await listen(app);
    try {
      const onboarding = await request(
        base,
        'GET',
        `/api/v1/client-intel/onboarding?clientId=${PULSEFORGE_ID}`
      );
      assert.equal(onboarding.status, 403);
      assert.equal(onboarding.json.error, 'tenant_mismatch');

      const audit = await request(
        base,
        'GET',
        `/api/v1/clients/${ANCHOR_ID}/cie-lifecycle-audit`
      );
      assert.equal(audit.status, 200);
      assert.ok(audit.json.sessions.length > 0);
      assert.ok(audit.json.blueprints.length > 0);
      assert.equal(Number(audit.json.clientId), ANCHOR_ID);
    } finally {
      await close();
    }
  });

  it('concurrent duplicate start requests return one active session', async () => {
    const store = createMemoryStore();
    const app = mountTenantAwareStartRoute(ANCHOR_ID, store);
    const { base, close } = await listen(app);
    try {
      const [a, b, c] = await Promise.all([
        request(base, 'POST', `/api/v1/clients/${ANCHOR_ID}/interview/start`, {}),
        request(base, 'POST', `/api/v1/clients/${ANCHOR_ID}/interview/start`, {}),
        request(base, 'POST', `/api/v1/clients/${ANCHOR_ID}/interview/start`, {}),
      ]);
      const ids = new Set(
        [a, b, c].map((res) => res.json && res.json.interviewId).filter(Boolean)
      );
      assert.equal(ids.size, 1);
      for (const res of [a, b, c]) {
        assert.equal(Number(res.json.clientId), ANCHOR_ID);
      }
    } finally {
      await close();
    }
  });
});

describe('CIE tenant propagation — frontend markers', () => {
  it('syncs operator tenant from session and blocks duplicate interview starts', () => {
    assert.match(cieAuthSource, /resolveCieCanonicalClientId/);
    assert.match(cieAuthSource, /tenant_context_required/);
    assert.match(cieAuthSource, /tenant_mismatch/);
    assert.doesNotMatch(cieAuthSource, /getRequestClientId/);

    assert.match(routeSource, /req\.query\.client_id/);

    assert.match(uiSource, /activeClientId/);
    assert.match(uiSource, /syncActiveTenantFromContext/);
    assert.match(uiSource, /pulseforge:tenant-changed/);
    assert.match(uiSource, /postInterviewStart/);
    assert.match(uiSource, /interviewStartInFlight/);
    assert.match(uiSource, /function resolvedClientId\(\)[\s\S]{0,400}activeClientId/);
    assert.doesNotMatch(
      uiSource,
      /function resolvedClientId\(\)[\s\S]{0,220}\|\|\s*'1'/
    );
    assert.doesNotMatch(uiSource, /id="clientId" value="1"/);
  });
});
