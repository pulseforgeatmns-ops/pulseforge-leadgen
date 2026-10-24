'use strict';

/**
 * SPEC-115 — Pilot 0 onboarding surfaces.
 *
 * GET  /change-password
 * POST /api/me/password
 * GET  /aim
 * GET  /api/v1/onboarding
 * POST /api/v1/onboarding/scout
 * GET  /api/v1/onboarding/prospects
 */

const express = require('express');
const path = require('path');
const pool = require('../db');
const { requireAuth, requireRole, bcrypt } = require('../middleware/auth');
const {
  resolveActiveTenantId,
} = require('../packages/max/workspace/TenantContextResolver');
const { getTenantWorkspace } = require('../services/tenantWorkspace');
const {
  buildOnboardingGreeting,
  publicOnboardingState,
  firstName,
  FAILURE,
} = require('../services/pilotOnboarding');
const { loadPublishedAimForClient } = require('../services/aicPersistence');
const { hydrateClientWorkspaces } = require('../services/acquisitionIntelligenceCompiler');
const { runPilotScout, listTenantProspects } = require('../services/pilotScout');

const router = express.Router();
const requireReader = [requireAuth, requireRole('admin', 'manager', 'viewer', 'client')];
const requireOperator = [requireAuth, requireRole('admin', 'manager', 'client')];

function noStore(res) {
  res.set('Cache-Control', 'no-store');
}

function sendError(res, err) {
  const status = err.status || (err.code === 'password_change_required' ? 403 : 500);
  if (status >= 500) console.error('[pilot-onboarding]', err);
  return res.status(status).json({
    error: err.code || 'onboarding_failed',
    message: err.message || 'Onboarding failed',
  });
}

function tenantIdFor(req) {
  return resolveActiveTenantId(req);
}

router.get('/change-password', requireAuth, (req, res) => {
  if (!req.user?.password_change_required) return res.redirect('/dashboard');
  res.sendFile(path.join(__dirname, '..', 'public', 'change-password.html'));
});

router.post('/api/me/password', requireAuth, async (req, res) => {
  try {
    const userId = req.user && req.user.id;
    if (!userId) {
      return res.status(400).json({ error: 'no_user', message: 'Sign in before changing your password.' });
    }
    const next = String(req.body?.password || req.body?.new_password || '').trim();
    if (next.length < 8) {
      return res.status(400).json({
        error: 'password_too_short',
        message: 'New password must be at least 8 characters.',
      });
    }
    const hash = await bcrypt.hash(next, 12);
    await pool.query(
      'UPDATE users SET password_hash = $1, password_change_required = FALSE WHERE id = $2',
      [hash, userId]
    );
    if (req.session && req.session.user) {
      req.session.user.password_change_required = false;
    }
    if (req.user) req.user.password_change_required = false;
    noStore(res);
    return res.json({
      ok: true,
      spec: 'SPEC-115',
      password_change_required: false,
      message: 'Password Updated',
      continue: '/dashboard',
    });
  } catch (err) {
    return sendError(res, err);
  }
});

router.get('/aim', ...requireOperator, (req, res) => {
  res.sendFile(path.join(__dirname, '..', 'public', 'aim.html'));
});

router.get('/api/v1/onboarding', ...requireReader, async (req, res) => {
  try {
    const clientId = tenantIdFor(req);
    if (clientId == null) {
      noStore(res);
      return res.status(400).json({
        ok: false,
        error: FAILURE.NO_TENANT.code,
        message: FAILURE.NO_TENANT.message,
      });
    }
    const snapshot = await getTenantWorkspace({ pool, clientId });
    const aicRows = await hydrateClientWorkspaces(clientId);
    const aic = aicRows[0] || null;
    const publishedAim = snapshot.publishedAim || (await loadPublishedAimForClient(clientId, pool));
    const gates = publicOnboardingState({
      tenantId: clientId,
      hasTenant: true,
      passwordChangeRequired: Boolean(req.user && req.user.password_change_required),
      clientIntelligence: snapshot.status && snapshot.status.clientIntelligence,
      aim: {
        published: Boolean(publishedAim),
        status: publishedAim ? 'published' : (aic && aic.status),
        documentCount: aic && aic.documents ? aic.documents.length : 0,
        compiled: Boolean(aic && ['in_review', 'approved', 'published'].includes(aic.status)),
      },
      prospectApproved: false,
      domainHealthy: false,
      sendingCapacityAvailable: false,
      campaignApproved: false,
    });
    const name = firstName((req.user && req.user.name) || (snapshot.client && snapshot.client.name));
    noStore(res);
    return res.json({
      ok: true,
      spec: 'SPEC-115',
      active_client_id: clientId,
      greeting: snapshot.status && snapshot.status.needsOnboarding
        ? buildOnboardingGreeting(name)
        : null,
      status: snapshot.status,
      lifecycle: snapshot.lifecycle,
      gates,
      aic: aic
        ? { id: aic.id, status: aic.status, documentCount: (aic.documents || []).length }
        : null,
      publishedAim: publishedAim
        ? { id: publishedAim.id, status: publishedAim.status, client_id: publishedAim.client_id || publishedAim.clientId }
        : null,
    });
  } catch (err) {
    return sendError(res, err);
  }
});

router.post('/api/v1/onboarding/scout', ...requireOperator, async (req, res) => {
  try {
    const clientId = tenantIdFor(req);
    if (clientId == null) {
      return res.status(400).json({ error: FAILURE.NO_TENANT.code, message: FAILURE.NO_TENANT.message });
    }
    const aim = await loadPublishedAimForClient(clientId, pool);
    const result = await runPilotScout({
      pool,
      clientId,
      aim,
      question: req.body?.question || 'Find founders struggling with founder dependency.',
    });
    noStore(res);
    return res.json(result);
  } catch (err) {
    if (err.code === FAILURE.NO_AIM.code) {
      return res.status(403).json({ error: err.code, message: err.message });
    }
    return sendError(res, err);
  }
});

router.get('/api/v1/onboarding/prospects', ...requireOperator, async (req, res) => {
  try {
    const clientId = tenantIdFor(req);
    if (clientId == null) {
      return res.status(400).json({ error: FAILURE.NO_TENANT.code, message: FAILURE.NO_TENANT.message });
    }
    const prospects = await listTenantProspects(pool, clientId);
    noStore(res);
    return res.json({
      ok: true,
      spec: 'SPEC-115',
      client_id: Number(clientId),
      count: prospects.length,
      prospects,
    });
  } catch (err) {
    return sendError(res, err);
  }
});

module.exports = router;
