'use strict';

/**
 * Command Deck API — SPEC-007.
 *
 * GET /api/v1/command-deck → CommandDeckModel
 *
 * One API. One payload. One render.
 * The browser never orchestrates intelligence.
 */

const express = require('express');
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

/**
 * GET /api/v1/command-deck
 * Query: period=daily|weekly|monthly, asOf=ISO, client_id (optional override for admin)
 */
router.get('/api/v1/command-deck', requireDashboardRead, async (req, res) => {
  try {
    const clientId = resolveTenantId(req);
    if (clientId == null) {
      return res.status(400).json({
        error: 'client_id required',
        message: 'Command Deck requires an active client context',
      });
    }

    const period = normalizePeriod(req.query.period);
    const asOf =
      typeof req.query.asOf === 'string' && req.query.asOf.trim()
        ? req.query.asOf.trim()
        : undefined;

    const max = await getMaxRuntime();
    const model = await max.compose({
      tenantId: String(clientId),
      period,
      asOf,
      operator:
        (req.session && req.session.user && req.session.user.email) || null,
    });

    res.set('Cache-Control', 'no-store');
    return res.json(model);
  } catch (err) {
    console.error('[command-deck]', err);
    return res.status(500).json({
      error: 'command_deck_compose_failed',
      message: err && err.message ? String(err.message) : 'compose failed',
    });
  }
});

function resolveTenantId(req) {
  if (req.query.client_id != null && req.query.client_id !== '') {
    const role =
      (req.session && req.session.user && req.session.user.role) || null;
    if (role === 'admin' || role === 'manager') {
      return normalizeClientId(req.query.client_id);
    }
  }
  return getRequestClientId(req);
}

function normalizePeriod(value) {
  const v = String(value || 'daily').toLowerCase();
  if (v === 'weekly' || v === 'monthly' || v === 'daily') return v;
  return 'daily';
}

module.exports = router;
