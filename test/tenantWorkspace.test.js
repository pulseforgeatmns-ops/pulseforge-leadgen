'use strict';

/**
 * SPEC-114 — Client Tenant Creation & Activation.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const express = require('express');

const training = require('../packages/max/training');
const {
  resolveActiveTenantId,
  resolveMaxPromptContext,
  buildTenantGreeting,
  NO_ACTIVE_CLIENT,
} = require('../packages/max/workspace/TenantContextResolver');
const { buildOpeningState } = require('../packages/max/workspace/OpeningStateBuilder');
const { PAGE_TYPES } = require('../packages/max/workspace/WorkspaceTypes');
const {
  validateCreateClientInput,
  createMemoryTenantStore,
  createAndProvisionTenant,
  getTenantWorkspace,
  getPublishedAimForTenant,
  activateTenant,
  slugify,
  initialWorkspaceStatus,
} = require('../services/tenantWorkspace');

const FEDIR_INPUT = {
  companyName: 'Fedir',
  primaryContact: 'Fedir',
  email: 'hello@fedir.example',
  vertical: 'business_coaching',
  country: 'United States',
  timezone: 'America/New_York',
};

function fedirInput(overrides = {}) {
  return { ...FEDIR_INPUT, ...overrides };
}

describe('SPEC-114 competency', () => {
  it('registers client_tenant_creation as a graduated competency', () => {
    const competency = training.getCompetency('client_tenant_creation');
    assert.equal(competency.stage, training.STAGES.GRADUATED);
    assert.ok(competency.specRefs.includes('SPEC-114'));
    assert.match(competency.exercises[0].generalLesson, /provisioned before intelligence/i);
  });

  it('documents the product brief numbering remap', () => {
    const spec = fs.readFileSync(
      path.join(__dirname, '../docs/specs/SPEC-114_Client_Tenant_Creation.md'),
      'utf8'
    );
    assert.match(spec, /product brief called this SPEC-112/i);
    assert.match(spec, /Create \*\*Fedir\*\*/);
    assert.match(spec, /No active client selected/);
  });
});

describe('SPEC-114 create + provision', () => {
  it('rejects missing required fields', () => {
    assert.throws(
      () => validateCreateClientInput({ companyName: 'Fedir' }),
      (err) => err.code === 'tenant_validation' && err.missing.includes('email')
    );
  });

  it('creates Fedir without developer SQL and provisions isolated namespaces', async () => {
    const store = createMemoryTenantStore();
    const result = await createAndProvisionTenant({ store, input: fedirInput() });
    assert.equal(result.client.name, 'Fedir');
    assert.equal(result.client.slug, 'fedir');
    assert.equal(result.client.vertical, 'business_coaching');
    assert.equal(result.provisioned, true);
    assert.ok(result.workspace.knowledge_namespace.includes(`tenant:${result.client.id}:knowledge`));
    assert.ok(result.workspace.aim_namespace.includes(`tenant:${result.client.id}:aim:fedir`));
    assert.equal(result.workspace.platform_knowledge_isolated, true);
    assert.deepEqual(result.client.enabled_agents, ['max']);
  });

  it('starts every tenant from the same empty foundation', async () => {
    const store = createMemoryTenantStore();
    const result = await createAndProvisionTenant({ store, input: fedirInput() });
    const empty = initialWorkspaceStatus();
    assert.deepEqual(result.status, empty);
    assert.equal(result.status.clientIntelligence.status, 'Not Started');
    assert.equal(result.status.aim.status, 'No Published AIM');
    assert.equal(result.status.missions.status, 'Empty');
    assert.equal(result.status.prospects.status, 'Empty');
    assert.equal(result.status.outcomes.status, 'Empty');
    assert.equal(result.status.knowledge.status, 'Empty');
    assert.equal(result.status.needsOnboarding, true);
  });

  it('does not attach the hand-authored Fedir AIM seed to a new Fedir tenant', async () => {
    const store = createMemoryTenantStore();
    const created = await createAndProvisionTenant({ store, input: fedirInput() });
    const published = await getPublishedAimForTenant({
      store,
      clientId: created.client.id,
      aimLookup: () => ({ id: 'aim-fedir', clientKey: 'fedir', status: 'complete', client_id: null }),
    });
    const snapshot = await getTenantWorkspace({
      store,
      clientId: created.client.id,
      aimLookup: () => ({ id: 'aim-fedir', clientKey: 'fedir', status: 'complete' }),
    });
    assert.equal(snapshot.status.aim.present, false);
    assert.equal(snapshot.status.aim.status, 'No Published AIM');
    assert.equal(snapshot.publishedAim, null);
    assert.equal(published, null);
  });

  it('workspace is available immediately after create', async () => {
    const store = createMemoryTenantStore();
    const created = await createAndProvisionTenant({ store, input: fedirInput() });
    const snapshot = await getTenantWorkspace({ store, clientId: created.client.id });
    assert.equal(snapshot.client.name, 'Fedir');
    assert.ok(snapshot.workspace);
    assert.ok(snapshot.greeting.fullText.includes('Welcome, Fedir.'));
  });

  it('unique-slugs colliding names', async () => {
    const store = createMemoryTenantStore();
    const a = await createAndProvisionTenant({ store, input: fedirInput() });
    const b = await createAndProvisionTenant({ store, input: fedirInput({ email: 'two@fedir.example' }) });
    assert.equal(a.client.slug, 'fedir');
    assert.equal(b.client.slug, 'fedir-2');
    assert.notEqual(a.client.id, b.client.id);
  });

  it('slugify keeps operator names readable', () => {
    assert.equal(slugify('Fedir'), 'fedir');
    assert.equal(slugify("O'Connor Growth"), 'oconnor-growth');
  });
});

describe('SPEC-114 isolation', () => {
  it('keeps prospects, knowledge, and AIM out of the other tenant', async () => {
    const store = createMemoryTenantStore();
    const fedir = await createAndProvisionTenant({ store, input: fedirInput() });
    const other = await createAndProvisionTenant({
      store,
      input: fedirInput({
        companyName: 'North Loop',
        email: 'hello@northloop.example',
      }),
    });
    store.putNamespace('prospects', fedir.client.id, [{ id: 'p-1', name: 'Founder A' }]);
    store.putNamespace('knowledge', fedir.client.id, [{ id: 'k-1' }]);
    store.putNamespace('aim', fedir.client.id, [{ id: 'aim-1', status: 'published', client_id: fedir.client.id }]);

    const fedirView = await getTenantWorkspace({ store, clientId: fedir.client.id });
    const otherView = await getTenantWorkspace({ store, clientId: other.client.id });

    assert.equal(fedirView.status.prospects.count, 1);
    assert.equal(fedirView.status.aim.present, true);
    assert.equal(otherView.status.prospects.count, 0);
    assert.equal(otherView.status.knowledge.count, 0);
    assert.equal(otherView.status.aim.present, false);
    assert.notEqual(fedir.workspace.prospect_namespace, other.workspace.prospect_namespace);
    assert.notEqual(fedir.workspace.aim_namespace, other.workspace.aim_namespace);
  });
});

describe('SPEC-114 activate + Max context', () => {
  it('activates the tenant on the session', () => {
    const session = {};
    const result = activateTenant(session, 88);
    assert.equal(result.active_client_id, 88);
    assert.equal(session.active_client_id, 88);
  });

  it('Max fail-closes when no tenant is selected', () => {
    const closed = resolveMaxPromptContext({});
    assert.equal(closed.ok, false);
    assert.equal(closed.error, 'no_active_client');
    assert.equal(closed.message, NO_ACTIVE_CLIENT);
    assert.equal(resolveActiveTenantId({ session: {}, user: { role: 'admin' } }), null);
  });

  it('Max recognizes the active Fedir tenant and does not invent intelligence', async () => {
    const store = createMemoryTenantStore();
    const created = await createAndProvisionTenant({ store, input: fedirInput() });
    const session = {};
    activateTenant(session, created.client.id);
    const req = { session, user: { role: 'admin' } };
    assert.equal(resolveActiveTenantId(req), created.client.id);

    const context = resolveMaxPromptContext({
      tenant: created.client,
      tenantId: created.client.id,
      blueprint: null,
      publishedAim: null,
      knowledge: [],
      mission: null,
    });
    assert.equal(context.ok, true);
    assert.equal(context.tenantId, String(created.client.id));
    assert.equal(context.reasoning.hasBlueprint, false);
    assert.equal(context.reasoning.hasPublishedAim, false);
    assert.equal(context.reasoning.readyForOnboarding, true);
  });

  it('locks client-role users to their assigned tenant', () => {
    assert.equal(
      resolveActiveTenantId({
        user: { role: 'client', client_id: 11 },
        session: { active_client_id: 1 },
      }),
      11
    );
  });
});

describe('SPEC-114 Max greeting', () => {
  it('greets the new tenant with the onboarding script', () => {
    const greeting = buildTenantGreeting('Fedir');
    assert.equal(greeting.greeting, 'Welcome, Fedir.');
    assert.match(greeting.fullText, /understanding your business/);
    assert.match(greeting.fullText, /Client Intelligence/);
    assert.match(greeting.fullText, /prospecting, reasoning/);
    assert.doesNotMatch(greeting.fullText, /Shall we start/);
  });

  it('OpeningStateBuilder uses the onboarding greeting for an empty workspace', () => {
    const opening = buildOpeningState({
      tenantId: '80',
      page: PAGE_TYPES.COMMAND_DECK,
      tenantName: 'Fedir',
      tenantWorkspace: { needsOnboarding: true },
    });
    assert.match(opening.fullText, /Welcome, Fedir/);
    assert.equal(opening.onboarding, true);
  });
});

describe('SPEC-114 operator APIs', () => {
  it('POST /api/clients provisions and GET workspace returns the empty foundation', async () => {
    const store = createMemoryTenantStore();
    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => {
      req.user = { id: 1, role: 'admin' };
      req.session = { active_client_id: null, user: req.user };
      next();
    });
    app.post('/api/clients', async (req, res) => {
      try {
        const result = await createAndProvisionTenant({ store, input: req.body, actor: req.user });
        res.status(201).json(result);
      } catch (err) {
        res.status(err.status || 500).json({ error: err.code, message: err.message });
      }
    });
    app.get('/api/clients/:id/workspace', async (req, res) => {
      const snapshot = await getTenantWorkspace({ store, clientId: req.params.id });
      res.json(snapshot);
    });
    app.post('/api/v1/tenant/activate', (req, res) => {
      activateTenant(req.session, req.body.client_id);
      res.json({ ok: true, active_client_id: req.session.active_client_id });
    });
    app.get('/api/v1/tenant/context', (req, res) => {
      const tenantId = resolveActiveTenantId(req);
      if (tenantId == null) {
        return res.status(400).json({ error: 'no_active_client', message: NO_ACTIVE_CLIENT });
      }
      return res.json({ ok: true, tenantId });
    });

    const server = await new Promise((resolve) => {
      const s = app.listen(0, '127.0.0.1', () => resolve(s));
    });
    const port = server.address().port;
    const base = `http://127.0.0.1:${port}`;
    try {
      const createdRes = await fetch(`${base}/api/clients`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(fedirInput()),
      });
      assert.equal(createdRes.status, 201);
      const created = await createdRes.json();
      assert.equal(created.client.name, 'Fedir');
      assert.equal(created.status.aim.status, 'No Published AIM');

      const wsRes = await fetch(`${base}/api/clients/${created.client.id}/workspace`);
      const workspace = await wsRes.json();
      assert.equal(workspace.status.clientIntelligence.status, 'Not Started');

      const noTenant = await fetch(`${base}/api/v1/tenant/context`);
      assert.equal(noTenant.status, 400);
      const closed = await noTenant.json();
      assert.equal(closed.message, NO_ACTIVE_CLIENT);

      const activateRes = await fetch(`${base}/api/v1/tenant/activate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ client_id: created.client.id }),
      });
      assert.equal((await activateRes.json()).active_client_id, created.client.id);
    } finally {
      await new Promise((r) => server.close(r));
    }
  });

  it('shell and admin surfaces expose create-client without SQL', () => {
    const shell = fs.readFileSync(path.join(__dirname, '../public/shared/shell.js'), 'utf8');
    const admin = fs.readFileSync(path.join(__dirname, '../public/admin-clients.html'), 'utf8');
    const routes = fs.readFileSync(path.join(__dirname, '../routes/tenantWorkspace.js'), 'utf8');
    assert.match(shell, /\/admin\/clients/);
    assert.match(admin, /Create tenant/);
    assert.match(routes, /router\.post\('\/api\/clients'/);
    assert.doesNotMatch(admin, /INSERT INTO clients/i);
  });
});
