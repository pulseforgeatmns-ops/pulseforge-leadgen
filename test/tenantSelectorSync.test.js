'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const clientContextSource = fs.readFileSync(
  path.join(__dirname, '..', 'utils', 'clientContext.js'),
  'utf8'
);
const apiSource = fs.readFileSync(path.join(__dirname, '..', 'routes', 'api.js'), 'utf8');
const shellSource = fs.readFileSync(path.join(__dirname, '..', 'public', 'shared', 'shell.js'), 'utf8');
const coordinatorSource = fs.readFileSync(
  path.join(__dirname, '..', 'public', 'shared', 'tenantCoordinator.js'),
  'utf8'
);
const dashboardSource = fs.readFileSync(
  path.join(__dirname, '..', 'public', 'dashboard.html'),
  'utf8'
);
const { reconcileOperatorActiveClient } = require('../utils/tenantAuthorization');

describe('operator tenant list — Maynard Web exclusion', () => {
  it('defines operator_switchable and hides maynard-web from switcher queries', () => {
    assert.match(clientContextSource, /operator_switchable/);
    assert.match(clientContextSource, /getOperatorSwitchableClients/);
    assert.match(clientContextSource, /operator_switchable, true\) = true/);
    assert.match(clientContextSource, /operator_switchable = false WHERE slug = 'maynard-web'/);
  });

  it('/api/clients uses operator-switchable tenant source', () => {
    assert.match(apiSource, /getOperatorSwitchableClients/);
    assert.doesNotMatch(apiSource, /getActiveClients\(\)/);
  });

  it('reconciles session when active tenant is not operator-switchable', () => {
    assert.match(apiSource, /reconcileOperatorActiveClient/);
    const session = { active_client_id: 99 };
    const clients = [{ id: 1, name: 'Pulseforge' }, { id: 10, name: 'Anchor Cleaning' }];
    const activeId = reconcileOperatorActiveClient(session, clients);
    assert.equal(activeId, 1);
    assert.equal(session.active_client_id, 1);
  });
});

describe('tenant selector synchronization', () => {
  it('shared coordinator binds all data-pf-tenant-select controls to one switch path', () => {
    assert.match(coordinatorSource, /data-pf-tenant-select/);
    assert.match(coordinatorSource, /\/api\/clients\/active/);
    assert.match(coordinatorSource, /pulseforge:tenant-changed/);
    assert.match(coordinatorSource, /syncAllSelects/);
  });

  it('shell nav select registers with coordinator instead of a private switch handler', () => {
    assert.match(shellSource, /PulseforgeTenantCoordinator/);
    assert.match(shellSource, /bindTenantSelect/);
    assert.doesNotMatch(shellSource, /async function handleTenantSwitch/);
  });

  it('dashboard sidebar selector uses coordinator and reloads tenant-scoped data on switch', () => {
    assert.match(dashboardSource, /data-pf-tenant-select/);
    assert.match(dashboardSource, /tenantCoordinator\.js/);
    assert.match(dashboardSource, /registerReloadHandler\(reloadDashboardTenantScopedData\)/);
    assert.match(dashboardSource, /bindTenantSelect\(selector\)/);
    assert.doesNotMatch(dashboardSource, /selector\.onchange = async/);
  });
});
