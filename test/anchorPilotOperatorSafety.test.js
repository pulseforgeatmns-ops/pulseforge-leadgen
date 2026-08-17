'use strict';

/**
 * Anchor Pilot 0 — operator-path safety (tenant context + enabled_agents dispatch).
 */

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const express = require('express');

const root = path.join(__dirname, '..');
const shellSource = fs.readFileSync(path.join(root, 'public', 'shared', 'shell.js'), 'utf8');
const shellCss = fs.readFileSync(path.join(root, 'public', 'shared', 'shell.css'), 'utf8');
const apiSource = fs.readFileSync(path.join(root, 'routes', 'api.js'), 'utf8');
const cronSource = fs.readFileSync(path.join(root, 'routes', 'cron.js'), 'utf8');
const maxWsSource = fs.readFileSync(path.join(root, 'routes', 'maxWorkspace.js'), 'utf8');
const deckJs = fs.readFileSync(path.join(root, 'public', 'command-deck', 'command-deck.js'), 'utf8');

const {
  assertAuthorizedClientSwitch,
  filterClientsForUser,
} = require('../utils/tenantAuthorization');

const ANCHOR_ID = 10;
const PULSEFORGE_ID = 1;

function mockDbClients(clientsById) {
  const dbPath = require.resolve('../db');
  require.cache[dbPath] = {
    id: dbPath,
    filename: dbPath,
    loaded: true,
    exports: {
      query: async (sql, params = []) => {
        const normalized = String(sql).trim();
        if (/INSERT INTO agent_log/i.test(normalized)) {
          return { rows: [{ id: 1 }] };
        }
        if (/SELECT id, enabled_agents FROM clients/i.test(normalized)) {
          const id = Number(params[0]);
          const row = clientsById[id];
          return { rows: row ? [row] : [] };
        }
        return { rows: [] };
      },
    },
  };
}

describe('Anchor Pilot 0 — tenant authorization', () => {
  it('admin may switch to any client', () => {
    const auth = assertAuthorizedClientSwitch(
      { role: 'admin', client_id: null },
      ANCHOR_ID
    );
    assert.equal(auth.ok, true);
  });

  it('client-bound manager cannot switch to unauthorized tenant', () => {
    const auth = assertAuthorizedClientSwitch(
      { role: 'manager', client_id: ANCHOR_ID },
      PULSEFORGE_ID
    );
    assert.equal(auth.ok, false);
    assert.equal(auth.status, 403);
    assert.equal(auth.error, 'forbidden_client_scope');
  });

  it('client-bound manager may switch only to assigned tenant', () => {
    const auth = assertAuthorizedClientSwitch(
      { role: 'manager', client_id: ANCHOR_ID },
      ANCHOR_ID
    );
    assert.equal(auth.ok, true);
  });

  it('filterClientsForUser constrains non-admin users with client_id', () => {
    const all = [
      { id: 1, name: 'Pulseforge' },
      { id: 10, name: 'Anchor Cleaning' },
    ];
    const filtered = filterClientsForUser(all, { role: 'manager', client_id: 10 });
    assert.equal(filtered.length, 1);
    assert.equal(filtered[0].id, 10);
  });
});

describe('Anchor Pilot 0 — enabled_agents dispatch policy', () => {
  before(() => {
    mockDbClients({
      10: { id: 10, enabled_agents: ['scout'] },
      1: { id: 1, enabled_agents: ['scout', 'emmett', 'sam', 'paige', 'cal'] },
    });
  });

  after(() => {
    delete require.cache[require.resolve('../db')];
    delete require.cache[require.resolve('../utils/agentDispatchPolicy')];
  });

  it('client 10 + scout → allowed', async () => {
    const { isAgentEnabledForClient } = require('../utils/agentDispatchPolicy');
    const gate = await isAgentEnabledForClient(10, 'scout');
    assert.equal(gate.allowed, true);
  });

  it('client 10 + emmett → blocked before run', async () => {
    const { isAgentEnabledForClient, BLOCK_REASON } = require('../utils/agentDispatchPolicy');
    const gate = await isAgentEnabledForClient(10, 'emmett');
    assert.equal(gate.allowed, false);
    assert.equal(gate.reason, BLOCK_REASON);
  });

  it('client 10 + sam → blocked', async () => {
    const { isAgentEnabledForClient } = require('../utils/agentDispatchPolicy');
    const gate = await isAgentEnabledForClient(10, 'sam');
    assert.equal(gate.allowed, false);
  });

  it('client 10 + cal_batch → blocked (normalized to cal)', async () => {
    const { isAgentEnabledForClient } = require('../utils/agentDispatchPolicy');
    const gate = await isAgentEnabledForClient(10, 'cal_batch');
    assert.equal(gate.allowed, false);
  });

  it('client 10 + paige → blocked at dispatch boundary', async () => {
    const { isAgentEnabledForClient } = require('../utils/agentDispatchPolicy');
    const gate = await isAgentEnabledForClient(10, 'paige');
    assert.equal(gate.allowed, false);
  });

  it('unknown policy state → fail closed', async () => {
    const dbPath = require.resolve('../db');
    require.cache[dbPath].exports.query = async () => ({ rows: [] });
    delete require.cache[require.resolve('../utils/agentDispatchPolicy')];
    const { isAgentEnabledForClient } = require('../utils/agentDispatchPolicy');
    const gate = await isAgentEnabledForClient(10, 'scout');
    assert.equal(gate.allowed, false);
    assert.equal(gate.reason, 'client_not_found');
  });

  it('other clients retain configured enabled-agent behavior', async () => {
    delete require.cache[require.resolve('../utils/agentDispatchPolicy')];
    mockDbClients({
      1: { id: 1, enabled_agents: ['scout', 'emmett', 'sam', 'paige', 'cal'] },
    });
    const { isAgentEnabledForClient } = require('../utils/agentDispatchPolicy');
    assert.equal((await isAgentEnabledForClient(1, 'emmett')).allowed, true);
    assert.equal((await isAgentEnabledForClient(1, 'scout')).allowed, true);
  });

  it('accepts client object with enabled_agents', async () => {
    delete require.cache[require.resolve('../utils/agentDispatchPolicy')];
    const { isAgentEnabledForClient } = require('../utils/agentDispatchPolicy');
    const gate = await isAgentEnabledForClient(
      { id: 10, enabled_agents: ['scout'] },
      'scout'
    );
    assert.equal(gate.allowed, true);
  });
});

describe('Anchor Pilot 0 — route and shell wiring', () => {
  it('shared shell exposes tenant selector using existing /api/clients/active', () => {
    assert.match(shellSource, /pf-nav-tenant-select/);
    assert.match(shellSource, /\/api\/clients\/active/);
    assert.match(shellSource, /pulseforge:tenant-changed/);
    assert.match(shellSource, /pf-nav-tenant-wrap/);
    assert.match(shellCss, /\.pf-nav-tenant-select/);
  });

  it('api/run and cron dispatch enforce isAgentEnabledForClient before mod.run', () => {
    assert.match(apiSource, /isAgentEnabledForClient/);
    assert.match(apiSource, /agent_not_enabled_for_client|BLOCK_REASON/);
    assert.match(apiSource, /status\(403\)/);
    assert.match(cronSource, /isAgentEnabledForClient/);
    assert.match(cronSource, /skipped:\s*true/);
    assert.doesNotMatch(
      cronSource,
      /isAgentEnabledForClient[\s\S]{0,400}require\(CRON_MODULES/
    );
  });

  it('POST /api/clients/active checks tenant authorization', () => {
    assert.match(apiSource, /assertAuthorizedClientSwitch/);
    assert.match(apiSource, /filterClientsForUser/);
  });

  it('max workspace ask rejects stale cross-tenant session reuse', () => {
    assert.match(maxWsSource, /tenant_mismatch/);
    assert.match(maxWsSource, /existing\.context\.tenantId/);
  });

  it('command deck invalidates workspace on tenant change', () => {
    assert.match(deckJs, /pulseforge:tenant-changed/);
    assert.match(deckJs, /invalidateWorkspaceForTenantChange/);
    assert.match(deckJs, /workspaceSessionId = null/);
    assert.match(deckJs, /loadDeck\(\)/);
  });
});

describe('Anchor Pilot 0 — cron dispatch HTTP', () => {
  let server;
  let base;
  const prevSecret = process.env.CRON_SECRET;

  before(async () => {
    mockDbClients({
      10: { id: 10, enabled_agents: ['scout'] },
      1: { id: 1, enabled_agents: ['scout', 'emmett'] },
    });
    delete require.cache[require.resolve('../routes/cron')];
    process.env.CRON_SECRET = 'anchor-pilot-test-secret';
    const cronRouter = require('../routes/cron');
    const app = express();
    app.use(express.json());
    app.use(cronRouter);
    server = app.listen(0, '127.0.0.1');
    await new Promise((resolve) => server.on('listening', resolve));
    const { port } = server.address();
    base = `http://127.0.0.1:${port}`;
  });

  after(async () => {
    process.env.CRON_SECRET = prevSecret;
    delete require.cache[require.resolve('../routes/cron')];
    delete require.cache[require.resolve('../db')];
    delete require.cache[require.resolve('../utils/agentDispatchPolicy')];
    if (server) {
      await new Promise((resolve) => server.close(resolve));
    }
  });

  it('cron blocks emmett for client 10 with skipped result', async () => {
    const url = `${base}/cron/emmett?client_id=10&secret=anchor-pilot-test-secret`;
    const res = await fetch(url);
    const body = await res.json();
    assert.equal(res.status, 200);
    assert.equal(body.skipped, true);
    assert.equal(body.reason, 'agent_not_enabled_for_client');
    assert.equal(body.client_id, 10);
  });

  it('cron allows scout for client 10', async () => {
    const url = `${base}/cron/scout?client_id=10&secret=anchor-pilot-test-secret&dryRun=1`;
    const res = await fetch(url);
    const body = await res.json();
    assert.equal(res.status, 200);
    assert.equal(body.success, true);
    assert.notEqual(body.skipped, true);
    assert.equal(body.client_id, 10);
  });

  it('cron retains enabled behavior for other clients', async () => {
    const url = `${base}/cron/emmett?client_id=1&secret=anchor-pilot-test-secret`;
    const res = await fetch(url);
    const body = await res.json();
    assert.equal(res.status, 200);
    assert.equal(body.success, true);
    assert.notEqual(body.skipped, true);
  });
});
