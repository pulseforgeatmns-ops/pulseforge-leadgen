'use strict';

/**
 * SPEC-114 — Client tenant creation, activation, and Max context APIs.
 *
 * POST /api/clients
 * GET  /api/clients/:id/workspace
 * GET  /api/v1/tenant/context
 * GET  /api/v1/tenant/greeting
 * GET  /admin/clients
 */

const express = require('express');
const path = require('path');
const pool = require('../db');
const { requireAuth, requireRole } = require('../middleware/auth');
const {
  assertAuthorizedClientSwitch,
  filterClientsForUser,
} = require('../utils/tenantAuthorization');
const { getActiveClients } = require('../utils/clientContext');
const {
  createAndProvisionTenant,
  getTenantWorkspace,
  activateTenant,
  ensureTenantWorkspaceSchema,
} = require('../services/tenantWorkspace');
const {
  listCanonicalBusinessVerticals,
} = require('../utils/canonicalVerticals');
const {
  resolveActiveTenantId,
  resolveMaxPromptContext,
  NO_ACTIVE_CLIENT,
} = require('../packages/max/workspace/TenantContextResolver');

const router = express.Router();
const requireOperator = [requireAuth, requireRole('admin', 'manager')];

function noStore(res) {
  res.set('Cache-Control', 'no-store');
}

function sendTenantError(res, err) {
  const status = err.status || (err.code === 'tenant_not_found' ? 404 : 500);
  if (status >= 500) console.error('[tenant]', err);
  return res.status(status).json({
    error: err.code || 'tenant_failed',
    message: err.message || 'Tenant request failed',
    missing: err.missing || undefined,
  });
}

router.get('/admin/clients', requireOperator, (req, res) => {
  res.sendFile(path.join(__dirname, '..', 'public', 'admin-clients.html'));
});

router.get('/api/canonical-verticals', (req, res) => {
  noStore(res);
  return res.json({
    ok: true,
    spec: 'PEC-116',
    verticals: listCanonicalBusinessVerticals(),
  });
});

router.post('/api/clients', requireOperator, async (req, res) => {
  try {
    const result = await createAndProvisionTenant({
      pool,
      input: req.body || {},
      actor: req.user,
    });
    noStore(res);
    return res.status(201).json({
      ok: true,
      spec: 'SPEC-114',
      ...result,
    });
  } catch (err) {
    return sendTenantError(res, err);
  }
});

router.get('/api/clients/:id/workspace', requireOperator, async (req, res) => {
  try {
    const clientId = Number(req.params.id);
    const auth = assertAuthorizedClientSwitch(req.user, clientId);
    if (!auth.ok) {
      return res.status(auth.status).json({
        error: auth.error,
        message: auth.message || 'Tenant not authorized',
      });
    }
    const snapshot = await getTenantWorkspace({ pool, clientId });
    noStore(res);
    return res.json({
      ok: true,
      spec: 'SPEC-114',
      ...snapshot,
    });
  } catch (err) {
    return sendTenantError(res, err);
  }
});

router.get('/api/v1/tenant/context', requireOperator, async (req, res) => {
  try {
    const tenantId = resolveActiveTenantId(req);
    if (tenantId == null) {
      noStore(res);
      return res.status(400).json({
        ok: false,
        error: 'no_active_client',
        message: NO_ACTIVE_CLIENT,
      });
    }
    const snapshot = await getTenantWorkspace({ pool, clientId: tenantId });
    const context = resolveMaxPromptContext({
      tenant: snapshot.client,
      tenantId,
      blueprint: snapshot.status.clientIntelligence.present ? { present: true } : null,
      publishedAim: snapshot.publishedAim,
      knowledge: snapshot.status.knowledge.count
        ? { count: snapshot.status.knowledge.count }
        : [],
      mission: snapshot.status.missions.count
        ? { count: snapshot.status.missions.count }
        : null,
      workspace: snapshot.workspace,
    });
    noStore(res);
    return res.json({
      spec: 'SPEC-114',
      active_client_id: tenantId,
      greeting: snapshot.greeting,
      status: snapshot.status,
      ...context,
    });
  } catch (err) {
    return sendTenantError(res, err);
  }
});

router.get('/api/v1/tenant/greeting', requireOperator, async (req, res) => {
  try {
    const tenantId = resolveActiveTenantId(req);
    if (tenantId == null) {
      noStore(res);
      return res.status(400).json({
        ok: false,
        error: 'no_active_client',
        message: NO_ACTIVE_CLIENT,
      });
    }
    const snapshot = await getTenantWorkspace({ pool, clientId: tenantId });
    noStore(res);
    return res.json({
      ok: true,
      spec: 'SPEC-114',
      active_client_id: tenantId,
      client: snapshot.client,
      status: snapshot.status,
      greeting: snapshot.greeting,
    });
  } catch (err) {
    return sendTenantError(res, err);
  }
});

router.post('/api/v1/tenant/activate', requireOperator, async (req, res) => {
  try {
    const clientId = Number(req.body?.client_id || req.body?.clientId || req.query.client_id);
    const auth = assertAuthorizedClientSwitch(req.user, clientId);
    if (!auth.ok) {
      return res.status(auth.status).json({
        error: auth.error,
        message: auth.message || 'Tenant switch not authorized',
      });
    }
    const allowed = filterClientsForUser(await getActiveClients(), req.user);
    const active = allowed.find((c) => Number(c.id) === Number(clientId));
    if (!active) {
      return res.status(404).json({ error: 'tenant_not_found', message: 'Client not found' });
    }
    activateTenant(req.session, clientId);
    const snapshot = await getTenantWorkspace({ pool, clientId });
    noStore(res);
    return res.json({
      ok: true,
      spec: 'SPEC-114',
      active_client_id: clientId,
      ...snapshot,
    });
  } catch (err) {
    return sendTenantError(res, err);
  }
});

ensureTenantWorkspaceSchema(pool).catch((err) => {
  console.error('[startup] tenant workspace schema:', err.message);
});

module.exports = router;
