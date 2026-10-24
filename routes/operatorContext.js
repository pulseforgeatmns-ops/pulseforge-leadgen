'use strict';

/**
 * SPEC-104 — Operator Context API.
 *
 * GET  /api/v1/operator-context — load persisted context + generated brief
 * POST /api/v1/operator-context/rebuild — manual rebuild
 */

const express = require('express');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const {
  getRequestClientId,
  normalizeClientId,
} = require('../utils/clientContext');
const {
  loadOperatorContext,
  rebuildOperatorContext,
  generateSessionBrief,
} = require('../services/operatorContext');
const { loadOperatorContextForSession } = require('../packages/max/workspace/OperatorContextLoader');

const requireDashboardRead = [
  requireAuth,
  requireRole('admin', 'manager', 'viewer', 'client'),
];

const requireOperator = [
  requireAuth,
  requireRole('admin', 'manager'),
];

function resolveClientContext(req) {
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

router.get('/api/v1/operator-context', requireDashboardRead, async (req, res) => {
  try {
    const clientId = resolveClientContext(req);
    if (clientId == null) {
      return res.status(400).json({
        error: 'client_id required',
        message: 'Operator context requires an active client',
      });
    }

    const attachment = await loadOperatorContextForSession({
      tenantId: String(clientId),
      clientId,
      rebuildIfMissing: req.query.rebuild === '1',
    });

    res.set('Cache-Control', 'no-store');
    return res.json({
      clientId,
      operatorContext: attachment.operatorContext,
      sessionBrief: attachment.sessionBrief,
      reviewedBeforeArrival: attachment.reviewedBeforeArrival,
    });
  } catch (err) {
    console.error('[operator-context] get:', err);
    return res.status(500).json({
      error: 'operator_context_load_failed',
      message: err && err.message ? String(err.message) : 'load failed',
    });
  }
});

router.post(
  '/api/v1/operator-context/rebuild',
  requireOperator,
  async (req, res) => {
    try {
      const clientId = resolveClientContext(req);
      if (clientId == null) {
        return res.status(400).json({
          error: 'client_id required',
          message: 'Operator context rebuild requires a client',
        });
      }

      const row = await rebuildOperatorContext({
        tenantId: String(clientId),
        clientId,
        trigger: (req.body && req.body.trigger) || 'manual_rebuild',
        metadata: (req.body && req.body.metadata) || {},
      });

      const sessionBrief = generateSessionBrief(row);

      res.set('Cache-Control', 'no-store');
      return res.json({
        clientId,
        operatorContext: {
          version: row.version,
          lastRebuildAt: row.lastRebuildAt,
          lastRebuildTrigger: row.lastRebuildTrigger,
          document: row.context,
        },
        sessionBrief,
        reviewedBeforeArrival: true,
      });
    } catch (err) {
      console.error('[operator-context] rebuild:', err);
      return res.status(500).json({
        error: 'operator_context_rebuild_failed',
        message: err && err.message ? String(err.message) : 'rebuild failed',
      });
    }
  }
);

module.exports = router;
