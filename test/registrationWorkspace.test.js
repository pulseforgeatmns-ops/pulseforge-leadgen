'use strict';

/**
 * SPEC-115 — Client Registration & Workspace Provisioning.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('path');
const express = require('express');

const training = require('../packages/max/training');
const {
  resolveMaxPromptContext,
  buildRegistrationGreeting,
  greetingForWorkspace,
  NO_WORKSPACE,
} = require('../packages/max/workspace/TenantContextResolver');
const { buildOpeningState } = require('../packages/max/workspace/OpeningStateBuilder');
const { PAGE_TYPES } = require('../packages/max/workspace/WorkspaceTypes');
const { LIFECYCLE, deriveWorkspaceLifecycle } = require('../services/workspaceLifecycle');
const { initialWorkspaceStatus } = require('../services/tenantWorkspace');
const {
  validateAccountInput,
  validateWorkspaceInput,
  createMemoryRegistrationStore,
  registerCustomer,
  verifyRegistrationToken,
  establishRegisteredSession,
  assertClientWorkspace,
} = require('../services/registration');

const SIGNUP = {
  name: 'Ada Founder',
  email: 'ada@northstar.example',
  password: 'secure-pass-1',
  phone: '+1-555-0100',
  companyName: 'North Star Studio',
  vertical: 'business_coaching',
  country: 'United States',
  timezone: 'America/New_York',
  website: 'https://northstar.example',
  teamSize: '2-10',
};

function signup(overrides = {}) {
  return { ...SIGNUP, ...overrides };
}

async function registerWithMailer(overrides = {}) {
  const store = createMemoryRegistrationStore();
  const sent = [];
  const mailer = async (payload) => {
    sent.push(payload);
    return { sent: true };
  };
  const result = await registerCustomer({
    store,
    input: signup(overrides),
    mailer,
    appUrl: 'https://app.pulseforge.test',
  });
  const token = new URL(sent[0].verifyUrl).searchParams.get('token');
  return { store, sent, result, token };
}

describe('SPEC-115 competency', () => {
  it('registers client_registration_workspace as a graduated competency', () => {
    const competency = training.getCompetency('client_registration_workspace');
    assert.equal(competency.stage, training.STAGES.GRADUATED);
    assert.ok(competency.specRefs.includes('SPEC-115'));
    assert.match(competency.exercises[0].generalLesson, /workspace is created first/i);
  });
});

describe('SPEC-115 validation', () => {
  it('requires account name, email, and password', () => {
    assert.throws(
      () => validateAccountInput({ email: 'ada@example.com' }),
      (err) => err.code === 'registration_validation' && err.missing.includes('name')
    );
  });

  it('requires workspace company, vertical, country, and timezone', () => {
    assert.throws(
      () => validateWorkspaceInput({ companyName: 'North Star' }),
      (err) => err.code === 'registration_validation' && err.missing.includes('vertical')
    );
  });
});

describe('SPEC-115 register + provision', () => {
  it('creates the workspace before binding the user', async () => {
    const { result } = await registerWithMailer();
    assert.equal(result.provisioned, true);
    assert.equal(result.client.name, 'North Star Studio');
    assert.equal(result.user.role, 'client');
    assert.equal(result.user.client_id, result.client.id);
    assert.equal(result.user.email_verified, false);
    assert.equal(result.workspace.origin, 'self_service');
    assert.ok(result.workspace.knowledge_namespace.includes(`tenant:${result.client.id}:knowledge`));
    assert.ok(result.workspace.campaign_namespace.includes(`tenant:${result.client.id}:campaign`));
    assert.ok(result.workspace.memory_namespace.includes(`tenant:${result.client.id}:memory`));
  });

  it('starts every registered workspace from the same empty foundation', async () => {
    const { result } = await registerWithMailer();
    assert.deepEqual(result.status, initialWorkspaceStatus());
    assert.equal(result.status.clientIntelligence.status, 'Not Started');
    assert.equal(result.status.aim.status, 'No Published AIM');
    assert.equal(result.status.prospects.count, 0);
    assert.equal(result.status.campaigns.count, 0);
    assert.equal(result.status.knowledge.count, 0);
    assert.equal(result.status.outcomes.count, 0);
    assert.equal(result.lifecycle.stage, LIFECYCLE.PROVISIONED);
  });

  it('rejects a duplicate email without creating a second operator', async () => {
    const store = createMemoryRegistrationStore();
    const mailer = async () => ({ sent: true });
    await registerCustomer({ store, input: signup(), mailer });
    await assert.rejects(
      () => registerCustomer({ store, input: signup(), mailer }),
      (err) => err.code === 'email_taken'
    );
    assert.equal(store._users.size, 1);
  });
});

describe('SPEC-115 verification + session', () => {
  it('requires verification before a session is established', async () => {
    const { result, token, store } = await registerWithMailer();
    assert.throws(
      () => establishRegisteredSession({}, result.user),
      (err) => err.code === 'email_unverified'
    );
    await assert.rejects(
      () => verifyRegistrationToken({ store, token: 'not-this-one' }),
      (err) => err.code === 'verification_invalid'
    );
    const ok = await verifyRegistrationToken({ store, token });
    assert.equal(ok.user.email_verified, true);
    const established = establishRegisteredSession({}, ok.user);
    assert.equal(established.active_client_id, ok.user.client_id);
    assert.equal(established.user.role, 'client');
  });

  it('fail-closes a client-role user without a workspace', () => {
    assert.throws(
      () => assertClientWorkspace({ role: 'client', client_id: null }),
      (err) => err.code === 'no_workspace' && err.message === NO_WORKSPACE
    );
    const closed = resolveMaxPromptContext({
      user: { role: 'client', client_id: null },
      publishedAim: { id: 'should-not-leak' },
    });
    assert.equal(closed.ok, false);
    assert.equal(closed.error, 'no_workspace');
    assert.equal(closed.publishedAim, null);
  });
});

describe('SPEC-115 Max greeting', () => {
  it('uses the Client Intelligence opening for self-service workspaces', () => {
    const greeting = buildRegistrationGreeting();
    assert.equal(greeting.greeting, 'Welcome to PulseForge.');
    assert.match(greeting.fullText, /grounded in what you teach me/);
    assert.equal(greeting.cta, 'Begin Client Intelligence');
    const opening = buildOpeningState({
      tenantId: '88',
      tenantName: 'North Star Studio',
      tenantWorkspace: { needsOnboarding: true, origin: 'self_service' },
      workspace: { origin: 'self_service' },
      page: PAGE_TYPES.COMMAND_DECK,
    });
    assert.match(opening.fullText, /Welcome to PulseForge/);
    assert.equal(opening.cta, 'Begin Client Intelligence');
    assert.equal(opening.onboarding, true);
    assert.equal(
      greetingForWorkspace({ origin: 'operator' }, 'Fedir').greeting,
      'Welcome, Fedir.'
    );
  });
});

describe('SPEC-115 lifecycle', () => {
  it('advances only when artifacts are earned', () => {
    const empty = initialWorkspaceStatus();
    assert.equal(deriveWorkspaceLifecycle(empty, LIFECYCLE.PROVISIONED), LIFECYCLE.PROVISIONED);
    assert.equal(
      deriveWorkspaceLifecycle(
        { ...empty, clientIntelligence: { present: true, status: 'In Progress' } },
        LIFECYCLE.PROVISIONED
      ),
      LIFECYCLE.CLIENT_INTELLIGENCE_IN_PROGRESS
    );
    assert.equal(
      deriveWorkspaceLifecycle(
        { ...empty, clientIntelligence: { present: true, approved: true, status: 'Approved' } },
        LIFECYCLE.PROVISIONED
      ),
      LIFECYCLE.BLUEPRINT_APPROVED
    );
    assert.equal(
      deriveWorkspaceLifecycle(
        { ...empty, aim: { published: true, status: 'Published AIM' }, prospects: { count: 2 } },
        LIFECYCLE.PROVISIONED
      ),
      LIFECYCLE.PROSPECTING_ACTIVE
    );
  });
});

describe('SPEC-115 public surfaces', () => {
  it('exposes signup, verify, and Begin Client Intelligence without SQL', () => {
    const signup = fs.readFileSync(path.join(__dirname, '../public/signup.html'), 'utf8');
    const dashboard = fs.readFileSync(path.join(__dirname, '../public/dashboard.html'), 'utf8');
    const login = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
    const routes = fs.readFileSync(path.join(__dirname, '../routes/registration.js'), 'utf8');
    assert.match(signup, /Create your workspace/);
    assert.match(signup, /\/api\/register/);
    assert.match(login, /\/signup/);
    assert.match(login, /error=unverified/);
    assert.match(login, /role === 'client'/);
    assert.match(dashboard, /Begin Client Intelligence/);
    assert.match(dashboard, /\/api\/v1\/workspace\/me/);
    assert.match(routes, /router\.post\('\/api\/register'/);
    assert.doesNotMatch(signup, /INSERT INTO/i);
  });

  it('POST /api/register provisions and verify opens a session', async () => {
    const store = createMemoryRegistrationStore();
    const sent = [];
    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => {
      req.session = {};
      next();
    });
    app.post('/api/register', async (req, res) => {
      try {
        const result = await registerCustomer({
          store,
          input: req.body,
          mailer: async (payload) => {
            sent.push(payload);
            return { sent: true };
          },
        });
        res.status(201).json({
          ok: true,
          user: result.user,
          status: result.status,
          lifecycle: result.lifecycle,
        });
      } catch (err) {
        res.status(err.status || 500).json({ error: err.code, message: err.message });
      }
    });
    app.post('/api/register/verify', async (req, res) => {
      try {
        const verified = await verifyRegistrationToken({ store, token: req.body.token });
        const session = establishRegisteredSession(req.session, verified.user);
        res.json(session);
      } catch (err) {
        res.status(err.status || 500).json({ error: err.code, message: err.message });
      }
    });

    const server = await new Promise((resolve) => {
      const s = app.listen(0, '127.0.0.1', () => resolve(s));
    });
    const port = server.address().port;
    const base = `http://127.0.0.1:${port}`;
    try {
      const createdRes = await fetch(`${base}/api/register`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(signup()),
      });
      assert.equal(createdRes.status, 201);
      const created = await createdRes.json();
      assert.equal(created.status.aim.status, 'No Published AIM');
      assert.equal(created.lifecycle.stage, 'provisioned');
      assert.equal(created.user.email_verified, false);

      const token = new URL(sent[0].verifyUrl).searchParams.get('token');
      const verifyRes = await fetch(`${base}/api/register/verify`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token }),
      });
      const session = await verifyRes.json();
      assert.equal(session.ok, true);
      assert.equal(session.active_client_id, created.user.client_id);
    } finally {
      await new Promise((r) => server.close(r));
    }
  });
});
