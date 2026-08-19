'use strict';

/**
 * Max Intelligence Workspace API — SPEC-009 / ADR-005.
 *
 * POST /api/v1/max/workspace/open
 * POST /api/v1/max/workspace/ask
 *
 * Legacy dashboard chat remains at POST /api/max/ask (routes/maxChat.js).
 */

const express = require('express');
const Anthropic = require('@anthropic-ai/sdk');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const {
  normalizeClientId,
} = require('../utils/clientContext');
const { getMaxRuntime } = require('../utils/maxRuntime');
const { presentMaxResultForClient } = require('../utils/clientFacingPresentation');
const {
  resolveActiveTenantId,
  NO_ACTIVE_CLIENT,
} = require('../packages/max/workspace/TenantContextResolver');
const { getTenantWorkspace } = require('../services/tenantWorkspace');
const {
  maxAcquisitionReply,
} = require('../services/pilotOnboarding');
const { loadPublishedAimForClient } = require('../services/aicPersistence');

const requireDashboardRead = [
  requireAuth,
  requireRole('admin', 'manager', 'viewer', 'client'),
];

let anthropicClient = null;
function getAnthropic() {
  if (!process.env.ANTHROPIC_API_KEY) return null;
  if (!anthropicClient) anthropicClient = new Anthropic();
  return anthropicClient;
}

/**
 * POST /api/v1/max/workspace/open
 * Body: MaxContext
 */
router.post(
  '/api/v1/max/workspace/open',
  requireDashboardRead,
  async (req, res) => {
    try {
      const tenantId = resolveTenantId(req);
      if (tenantId == null) {
        return res.status(400).json({
          error: 'no_active_client',
          message: NO_ACTIVE_CLIENT,
        });
      }

      const envelope = {
        ...(req.body || {}),
        tenantId: String(tenantId),
      };
      try {
        const snapshot = await getTenantWorkspace({ clientId: tenantId });
        envelope.tenantWorkspace = snapshot.status;
        envelope.tenant = snapshot.client;
        envelope.tenantName = snapshot.client && snapshot.client.name;
      } catch (_err) {
        /* workspace snapshot is additive */
      }
      if (
        req.body &&
        req.body.tenantId != null &&
        String(req.body.tenantId) !== String(tenantId)
      ) {
        return res.status(403).json({
          error: 'tenant_mismatch',
          message: 'Context tenantId does not match session client',
        });
      }

      const max = await getWorkspaceRuntime();
      let result = await max.openWorkspace(envelope);
      if (isClientRole(req)) {
        result = presentMaxResultForClient(result);
      }
      res.set('Cache-Control', 'no-store');
      return res.json(result);
    } catch (err) {
      console.error('[max-workspace] open:', err);
      const status = /required|must be one of/i.test(err.message) ? 400 : 500;
      return res.status(status).json({
        error: 'workspace_open_failed',
        message: err && err.message ? String(err.message) : 'open failed',
      });
    }
  }
);

/**
 * POST /api/v1/max/workspace/ask
 * Body: { sessionId?, question, context? }
 */
router.post(
  '/api/v1/max/workspace/ask',
  requireDashboardRead,
  async (req, res) => {
    try {
      const tenantId = resolveTenantId(req);
      if (tenantId == null) {
        return res.status(400).json({
          error: 'no_active_client',
          message: NO_ACTIVE_CLIENT,
        });
      }

      const question = String(req.body?.question || '').trim();
      if (!question) {
        return res.status(400).json({ error: 'Question is required' });
      }

      if (isClientRole(req)) {
        const snapshot = await getTenantWorkspace({ clientId: tenantId }).catch(() => null);
        const publishedAim = (snapshot && snapshot.publishedAim)
          || await loadPublishedAimForClient(tenantId).catch(() => null);
        const lock = maxAcquisitionReply({
          tenantId,
          hasTenant: true,
          passwordChangeRequired: Boolean(req.user && req.user.password_change_required),
          clientIntelligence: snapshot && snapshot.status && snapshot.status.clientIntelligence,
          aim: publishedAim || (snapshot && snapshot.status && snapshot.status.aim) || {},
        });
        if (lock) {
          res.set('Cache-Control', 'no-store');
          return res.json({
            ok: false,
            spec: 'SPEC-115',
            error: lock.code,
            message: lock.message,
            reply: lock.message,
            answer: lock.message,
          });
        }
      }

      console.info('[mission-objective-len]', {
        stage: 'api',
        chars: question.length,
        newlines: (question.match(/\n/g) || []).length,
        bodyKeys: req.body && typeof req.body === 'object' ? Object.keys(req.body) : [],
      });

      let context = req.body?.context || null;
      if (context) {
        if (
          context.tenantId != null &&
          String(context.tenantId) !== String(tenantId)
        ) {
          return res.status(403).json({
            error: 'tenant_mismatch',
            message: 'Context tenantId does not match session client',
          });
        }
        context = { ...context, tenantId: String(tenantId) };
      }

      const max = await getWorkspaceRuntime();

      if (req.body?.sessionId) {
        const existing = max.workspace?.sessions?.get(req.body.sessionId);
        if (
          existing &&
          existing.context &&
          existing.context.tenantId != null &&
          String(existing.context.tenantId) !== String(tenantId)
        ) {
          return res.status(403).json({
            error: 'tenant_mismatch',
            message:
              'Workspace session belongs to a different tenant — open a new workspace',
          });
        }
      }

      let result = await max.askWorkspace({
        sessionId: req.body?.sessionId || null,
        question,
        context,
      });
      if (isClientRole(req)) {
        result = presentMaxResultForClient(result);
      }

      res.set('Cache-Control', 'no-store');
      return res.json(result);
    } catch (err) {
      console.error('[max-workspace] ask:', err);
      const status = /required|Unknown workspace/i.test(err.message)
        ? 400
        : 500;
      return res.status(status).json({
        error: 'workspace_ask_failed',
        message: err && err.message ? String(err.message) : 'ask failed',
      });
    }
  }
);

async function getWorkspaceRuntime() {
  const max = await getMaxRuntime();
  // Ensure presentation engine can use Claude when key is present
  if (max.workspace && max.workspace._presentation) {
    const client = getAnthropic();
    if (client) {
      max.workspace._presentation._anthropic = client;
      max.workspace._presentation._disableLlm = false;
    }
  }
  return max;
}

function resolveTenantId(req) {
  const active = resolveActiveTenantId(req);
  if (active != null) return active;
  // Query/body client_id is allowed only when the session already has no
  // tenant — still fail closed rather than defaulting to Pulseforge (id=1).
  if (req.query.client_id != null && req.query.client_id !== '') {
    const role =
      (req.session && req.session.user && req.session.user.role) || null;
    if (role === 'admin' || role === 'manager') {
      return normalizeClientId(req.query.client_id);
    }
  }
  if (req.body && req.body.client_id != null && req.body.client_id !== '') {
    const role =
      (req.session && req.session.user && req.session.user.role) || null;
    if (role === 'admin' || role === 'manager') {
      return normalizeClientId(req.body.client_id);
    }
  }
  return null;
}

function isClientRole(req) {
  const role =
    (req.user && req.user.role) ||
    (req.session && req.session.user && req.session.user.role) ||
    null;
  return role === 'client';
}

module.exports = router;
