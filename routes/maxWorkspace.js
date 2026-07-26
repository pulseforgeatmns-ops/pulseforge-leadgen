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
  getRequestClientId,
  normalizeClientId,
} = require('../utils/clientContext');
const { getMaxRuntime } = require('../utils/maxRuntime');

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
          error: 'client_id required',
          message: 'Max workspace requires an active client context',
        });
      }

      const envelope = {
        ...(req.body || {}),
        tenantId: String(tenantId),
      };
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
      const result = max.openWorkspace(envelope);
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
          error: 'client_id required',
          message: 'Max workspace requires an active client context',
        });
      }

      const question = String(req.body?.question || '').trim();
      if (!question) {
        return res.status(400).json({ error: 'Question is required' });
      }

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
      const result = await max.askWorkspace({
        sessionId: req.body?.sessionId || null,
        question,
        context,
      });

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
  return getRequestClientId(req);
}

module.exports = router;
