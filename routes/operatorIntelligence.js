'use strict';

/**
 * Operator Intelligence APIs (SPEC-012 / ADR-007).
 *
 * POST /api/v1/operator/events
 * POST /api/v1/operator/outcomes
 * GET  /api/v1/operator/learning/:recommendationId
 * GET  /api/v1/operator/quality
 * GET  /api/v1/operator/preferences
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

const requireInternalQuality = [
  requireAuth,
  requireRole('admin', 'manager'),
];

/**
 * POST /api/v1/operator/events
 * Body: InteractionEvent | InteractionEvent[]
 */
router.post(
  '/api/v1/operator/events',
  requireDashboardRead,
  async (req, res) => {
    try {
      const clientId = resolveTenantId(req);
      if (clientId == null) {
        return res.status(400).json({
          error: 'client_id required',
          message: 'Operator events require an active client context',
        });
      }

      const raw = req.body;
      const items = Array.isArray(raw)
        ? raw
        : Array.isArray(raw && raw.events)
          ? raw.events
          : raw
            ? [raw]
            : [];
      if (!items.length) {
        return res.status(400).json({ error: 'events required' });
      }

      const operatorId =
        (req.session && req.session.user && req.session.user.email) ||
        (req.session && req.session.user && req.session.user.id) ||
        null;

      const max = await getMaxRuntime();
      const payload = items.map((item) => ({
        ...item,
        tenantId: String(clientId),
        operatorId:
          item.operatorId != null ? item.operatorId : operatorId,
      }));

      const result = max.trackOperator(payload);
      res.set('Cache-Control', 'no-store');
      return res.status(201).json({
        recorded: result.events.length,
        events: result.events,
        learnings: result.learnings,
      });
    } catch (err) {
      console.error('[operator-events]', err);
      const status = /requires|Unknown interaction/i.test(
        err && err.message ? err.message : ''
      )
        ? 400
        : 500;
      return res.status(status).json({
        error: 'operator_events_failed',
        message: err && err.message ? String(err.message) : 'failed',
      });
    }
  }
);

/**
 * POST /api/v1/operator/outcomes
 * Body: { recommendationId, outcome, reason? }
 */
router.post(
  '/api/v1/operator/outcomes',
  requireDashboardRead,
  async (req, res) => {
    try {
      const clientId = resolveTenantId(req);
      if (clientId == null) {
        return res.status(400).json({ error: 'client_id required' });
      }
      const recommendationId = String(
        (req.body && req.body.recommendationId) || ''
      ).trim();
      const outcome = String((req.body && req.body.outcome) || '')
        .trim()
        .toLowerCase();
      if (!recommendationId || !outcome) {
        return res.status(400).json({
          error: 'recommendationId and outcome required',
        });
      }

      const max = await getMaxRuntime();
      const result = max.operatorOutcome({
        tenantId: String(clientId),
        recommendationId,
        outcome,
        reason: req.body && req.body.reason,
        force: req.body && req.body.force === true,
      });
      res.set('Cache-Control', 'no-store');
      return res.json(result);
    } catch (err) {
      console.error('[operator-outcomes]', err);
      const status = /Invalid outcome|Unknown outcome|requires/i.test(
        err && err.message ? err.message : ''
      )
        ? 400
        : 500;
      return res.status(status).json({
        error: 'operator_outcome_failed',
        message: err && err.message ? String(err.message) : 'failed',
      });
    }
  }
);

/**
 * GET /api/v1/operator/learning/:recommendationId
 */
router.get(
  '/api/v1/operator/learning/:recommendationId',
  requireDashboardRead,
  async (req, res) => {
    try {
      const clientId = resolveTenantId(req);
      if (clientId == null) {
        return res.status(400).json({ error: 'client_id required' });
      }
      const recommendationId = String(
        req.params.recommendationId || ''
      ).trim();
      if (!recommendationId) {
        return res.status(400).json({ error: 'recommendationId required' });
      }
      const max = await getMaxRuntime();
      const learning = max.operatorLearning(
        String(clientId),
        recommendationId
      );
      res.set('Cache-Control', 'no-store');
      return res.json(learning);
    } catch (err) {
      console.error('[operator-learning]', err);
      return res.status(500).json({
        error: 'operator_learning_failed',
        message: err && err.message ? String(err.message) : 'failed',
      });
    }
  }
);

/**
 * GET /api/v1/operator/quality
 * Internal Intelligence Quality Dashboard — admin/manager only.
 */
router.get(
  '/api/v1/operator/quality',
  requireInternalQuality,
  async (req, res) => {
    try {
      const clientId = resolveTenantId(req);
      if (clientId == null) {
        return res.status(400).json({ error: 'client_id required' });
      }
      const max = await getMaxRuntime();
      const dashboard = max.operatorQuality(String(clientId));
      res.set('Cache-Control', 'no-store');
      return res.json({
        ...dashboard,
        internal: true,
        customerFacing: false,
      });
    } catch (err) {
      console.error('[operator-quality]', err);
      return res.status(500).json({
        error: 'operator_quality_failed',
        message: err && err.message ? String(err.message) : 'failed',
      });
    }
  }
);

/**
 * GET /api/v1/operator/preferences
 */
router.get(
  '/api/v1/operator/preferences',
  requireDashboardRead,
  async (req, res) => {
    try {
      const clientId = resolveTenantId(req);
      if (clientId == null) {
        return res.status(400).json({ error: 'client_id required' });
      }
      const max = await getMaxRuntime();
      const prefs = max.operator.preferences.snapshot(String(clientId));
      res.set('Cache-Control', 'no-store');
      return res.json(prefs);
    } catch (err) {
      console.error('[operator-preferences]', err);
      return res.status(500).json({
        error: 'operator_preferences_failed',
        message: err && err.message ? String(err.message) : 'failed',
      });
    }
  }
);

function resolveTenantId(req) {
  const fromQuery =
    req.query.client_id != null ? normalizeClientId(req.query.client_id) : null;
  if (fromQuery != null) return fromQuery;
  const fromBody =
    req.body && req.body.client_id != null
      ? normalizeClientId(req.body.client_id)
      : null;
  if (fromBody != null) return fromBody;
  return getRequestClientId(req);
}

module.exports = router;
