'use strict';

/**
 * SPEC-115 — Public registration, email verification, and workspace-me APIs.
 *
 * GET  /signup
 * GET  /verify-email
 * POST /api/register
 * POST /api/register/verify
 * GET  /api/v1/workspace/me
 */

const express = require('express');
const path = require('path');
const pool = require('../db');
const { requireAuth, requireRole } = require('../middleware/auth');
const {
  registerCustomer,
  verifyRegistrationToken,
  establishRegisteredSession,
  assertClientWorkspace,
  ensureRegistrationSchema,
} = require('../services/registration');
const {
  resolveActiveTenantId,
  resolveMaxPromptContext,
  NO_ACTIVE_CLIENT,
  NO_WORKSPACE,
} = require('../packages/max/workspace/TenantContextResolver');
const { getTenantWorkspace } = require('../services/tenantWorkspace');

const router = express.Router();
const requireWorkspaceReader = [
  requireAuth,
  requireRole('admin', 'manager', 'viewer', 'client'),
];

function noStore(res) {
  res.set('Cache-Control', 'no-store');
}

function sendError(res, err) {
  const status = err.status || 500;
  if (status >= 500) console.error('[registration]', err);
  return res.status(status).json({
    error: err.code || 'registration_failed',
    message: err.message || 'Registration failed',
    missing: err.missing || undefined,
  });
}

router.get('/signup', (req, res) => {
  if (req.session?.user) return res.redirect('/dashboard');
  res.sendFile(path.join(__dirname, '..', 'public', 'signup.html'));
});

router.get('/verify-email', async (req, res) => {
  const token = String(req.query.token || '').trim();
  if (!token) {
    return res.redirect('/login?error=verify');
  }
  try {
    const result = await verifyRegistrationToken({ pool, token });
    if (req.session) {
      establishRegisteredSession(req.session, result.user);
      return res.redirect('/dashboard');
    }
    return res.redirect('/login?verified=1');
  } catch (_err) {
    return res.redirect('/login?error=verify');
  }
});

router.post('/api/register', async (req, res) => {
  try {
    if (req.session?.user) {
      return res.status(409).json({
        error: 'already_authenticated',
        message: 'Sign out before creating a new workspace.',
      });
    }
    const result = await registerCustomer({
      pool,
      input: req.body || {},
    });
    noStore(res);
    return res.status(201).json({
      ok: true,
      spec: 'SPEC-115',
      user: result.user,
      client: result.client,
      workspace: result.workspace,
      status: result.status,
      lifecycle: result.lifecycle,
      greeting: result.greeting,
      provisioned: true,
      verification: result.verification,
    });
  } catch (err) {
    return sendError(res, err);
  }
});

router.post('/api/register/verify', async (req, res) => {
  try {
    const result = await verifyRegistrationToken({
      pool,
      token: req.body?.token || req.query.token,
    });
    const session = establishRegisteredSession(req.session, result.user);
    noStore(res);
    return res.json({
      ok: true,
      spec: 'SPEC-115',
      verified: true,
      user: session.user,
      active_client_id: session.active_client_id,
    });
  } catch (err) {
    return sendError(res, err);
  }
});

router.get('/api/v1/workspace/me', requireWorkspaceReader, async (req, res) => {
  try {
    const user = req.user || req.session?.user;
    let clientId;
    if (user?.role === 'client') {
      clientId = assertClientWorkspace(user).clientId;
    } else {
      clientId = resolveActiveTenantId(req);
    }
    if (clientId == null) {
      noStore(res);
      return res.status(400).json({
        ok: false,
        error: user?.role === 'client' ? 'no_workspace' : 'no_active_client',
        message: user?.role === 'client' ? NO_WORKSPACE : NO_ACTIVE_CLIENT,
      });
    }
    const snapshot = await getTenantWorkspace({ pool, clientId });
    const context = resolveMaxPromptContext({
      user,
      tenant: snapshot.client,
      tenantId: clientId,
      workspace: snapshot.workspace,
      blueprint: snapshot.status.clientIntelligence.present
        ? { present: true, approved: snapshot.status.clientIntelligence.approved }
        : null,
      publishedAim: snapshot.publishedAim,
      knowledge: snapshot.status.knowledge.count
        ? { count: snapshot.status.knowledge.count }
        : [],
      mission: snapshot.status.missions.count
        ? { count: snapshot.status.missions.count }
        : null,
      requireWorkspace: user?.role === 'client',
    });
    noStore(res);
    return res.json({
      ok: true,
      spec: 'SPEC-115',
      active_client_id: clientId,
      greeting: snapshot.greeting,
      status: snapshot.status,
      lifecycle: snapshot.lifecycle,
      runtime: {
        workspace: 'Provisioned',
        blueprint: snapshot.status.clientIntelligence.status,
        aim: snapshot.status.aim.status,
        prospects: snapshot.status.prospects.count,
        campaigns: snapshot.status.campaigns.count,
        knowledge: snapshot.status.knowledge.count,
        outcomes: snapshot.status.outcomes.count,
      },
      cta: {
        label: 'Begin Client Intelligence',
        href: '/client-intel',
      },
      ...context,
    });
  } catch (err) {
    return sendError(res, err);
  }
});

ensureRegistrationSchema(pool).catch((err) => {
  console.error('[startup] registration schema:', err.message);
});

module.exports = router;
