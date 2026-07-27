'use strict';

/**
 * Outcome Intelligence APIs (SPEC-013 / ADR-008).
 * Internal evaluation layer — never mutates customer-facing reasoning.
 *
 * POST /api/v1/outcome/records
 * POST /api/v1/outcome/lifecycle
 * GET  /api/v1/outcome/:recommendationId
 * GET  /api/v1/outcome/calibration
 * GET  /api/v1/outcome/strategies
 * GET  /api/v1/outcome/drift
 * GET  /api/v1/outcome/review
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

const requireInternal = [requireAuth, requireRole('admin', 'manager')];

/**
 * POST /api/v1/outcome/records
 * Body: RecommendationOutcome | { records: RecommendationOutcome[] }
 */
router.post(
  '/api/v1/outcome/records',
  requireDashboardRead,
  async (req, res) => {
    try {
      const clientId = resolveTenantId(req);
      if (clientId == null) {
        return res.status(400).json({ error: 'client_id required' });
      }

      const raw = req.body;
      const items = Array.isArray(raw)
        ? raw
        : Array.isArray(raw && raw.records)
          ? raw.records
          : raw
            ? [raw]
            : [];
      if (!items.length) {
        return res.status(400).json({ error: 'records required' });
      }

      const max = await getMaxRuntime();
      const recorded = items.map((item) =>
        max.recordOutcome({
          ...item,
          tenantId: String(clientId),
        })
      );

      // SPEC-014 Flight Recorder + Knowledge evidence for outcomes
      try {
        const {
          safeRecordFlightStage,
          safeWriteOperational,
          FLIGHT_STAGES,
          OPERATIONAL_EVENTS,
        } = require('../utils/knowledgeDualWrite');
        for (const item of recorded) {
          const recId = item.recommendationId || item.id;
          if (!recId) continue;
          const outcomeType =
            item.outcome === 'successful'
              ? OPERATIONAL_EVENTS.OUTCOME_SUCCESS
              : item.outcome === 'unsuccessful'
                ? OPERATIONAL_EVENTS.OUTCOME_FAILURE
                : OPERATIONAL_EVENTS.OUTCOME_INCONCLUSIVE;
          safeWriteOperational({
            id: `outcome:${clientId}:${recId}`,
            tenantId: String(clientId),
            entityId: String(recId),
            entityType: 'recommendation',
            eventType: outcomeType,
            source: 'outcome_intelligence',
            payload: {
              recommendationId: recId,
              outcome: item.outcome,
              lifecycle: item.lifecycle,
            },
            evidence: {
              summary: `Outcome ${item.outcome || item.lifecycle} for ${recId}`,
            },
          });
          safeRecordFlightStage({
            flightId: `flight:${clientId}:recommendation:${recId}`,
            tenantId: String(clientId),
            entityId: String(recId),
            entityType: 'recommendation',
            stage: FLIGHT_STAGES.OUTCOME_RECORDED,
            metadata: { outcome: item.outcome || null, lifecycle: item.lifecycle },
          });
        }
      } catch {
        // never block outcome path
      }

      res.set('Cache-Control', 'no-store');
      return res.status(201).json({
        recorded: recorded.length,
        records: recorded,
      });
    } catch (err) {
      console.error('[outcome-records]', err);
      const status = /requires|Unknown|Invalid lifecycle/i.test(
        err && err.message ? err.message : ''
      )
        ? 400
        : 500;
      return res.status(status).json({
        error: 'outcome_records_failed',
        message: err && err.message ? String(err.message) : 'failed',
      });
    }
  }
);

/**
 * POST /api/v1/outcome/lifecycle
 * Body: { recommendationId, lifecycle, notes?, confidenceAtOutcome? }
 */
router.post(
  '/api/v1/outcome/lifecycle',
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
      const lifecycle = String((req.body && req.body.lifecycle) || '')
        .trim()
        .toLowerCase();
      if (!recommendationId || !lifecycle) {
        return res.status(400).json({
          error: 'recommendationId and lifecycle required',
        });
      }

      const max = await getMaxRuntime();
      const result = max.outcomeLifecycle({
        tenantId: String(clientId),
        recommendationId,
        lifecycle,
        notes: req.body && req.body.notes,
        confidenceAtOutcome: req.body && req.body.confidenceAtOutcome,
        force: req.body && req.body.force === true,
      });
      res.set('Cache-Control', 'no-store');
      return res.json(result);
    } catch (err) {
      console.error('[outcome-lifecycle]', err);
      const status = /Invalid lifecycle|Unknown lifecycle|not registered|requires/i.test(
        err && err.message ? err.message : ''
      )
        ? 400
        : 500;
      return res.status(status).json({
        error: 'outcome_lifecycle_failed',
        message: err && err.message ? String(err.message) : 'failed',
      });
    }
  }
);

/**
 * GET /api/v1/outcome/calibration
 * Internal confidence calibration report.
 */
router.get(
  '/api/v1/outcome/calibration',
  requireInternal,
  async (req, res) => {
    try {
      const clientId = resolveTenantId(req);
      if (clientId == null) {
        return res.status(400).json({ error: 'client_id required' });
      }
      const max = await getMaxRuntime();
      const report = max.outcomeCalibration(String(clientId));
      res.set('Cache-Control', 'no-store');
      return res.json({ ...report, internal: true });
    } catch (err) {
      console.error('[outcome-calibration]', err);
      return res.status(500).json({
        error: 'outcome_calibration_failed',
        message: err && err.message ? String(err.message) : 'failed',
      });
    }
  }
);

/**
 * GET /api/v1/outcome/strategies
 */
router.get(
  '/api/v1/outcome/strategies',
  requireInternal,
  async (req, res) => {
    try {
      const clientId = resolveTenantId(req);
      if (clientId == null) {
        return res.status(400).json({ error: 'client_id required' });
      }
      const max = await getMaxRuntime();
      const report = max.outcomeStrategies(String(clientId));
      res.set('Cache-Control', 'no-store');
      return res.json({ ...report, internal: true });
    } catch (err) {
      console.error('[outcome-strategies]', err);
      return res.status(500).json({
        error: 'outcome_strategies_failed',
        message: err && err.message ? String(err.message) : 'failed',
      });
    }
  }
);

/**
 * GET /api/v1/outcome/drift
 */
router.get('/api/v1/outcome/drift', requireInternal, async (req, res) => {
  try {
    const clientId = resolveTenantId(req);
    if (clientId == null) {
      return res.status(400).json({ error: 'client_id required' });
    }
    const max = await getMaxRuntime();
    const report = max.outcomeDrift(String(clientId));
    res.set('Cache-Control', 'no-store');
    return res.json({ ...report, internal: true });
  } catch (err) {
    console.error('[outcome-drift]', err);
    return res.status(500).json({
      error: 'outcome_drift_failed',
      message: err && err.message ? String(err.message) : 'failed',
    });
  }
});

/**
 * GET /api/v1/outcome/review
 * Internal Intelligence Review dashboard.
 */
router.get('/api/v1/outcome/review', requireInternal, async (req, res) => {
  try {
    const clientId = resolveTenantId(req);
    if (clientId == null) {
      return res.status(400).json({ error: 'client_id required' });
    }
    const max = await getMaxRuntime();
    const dashboard = max.outcomeReview(String(clientId));
    res.set('Cache-Control', 'no-store');
    return res.json(dashboard);
  } catch (err) {
    console.error('[outcome-review]', err);
    return res.status(500).json({
      error: 'outcome_review_failed',
      message: err && err.message ? String(err.message) : 'failed',
    });
  }
});

/**
 * GET /api/v1/outcome/:recommendationId
 * Must be registered after static paths above.
 */
router.get(
  '/api/v1/outcome/:recommendationId',
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
      const reserved = new Set([
        'calibration',
        'strategies',
        'drift',
        'review',
        'records',
        'lifecycle',
      ]);
      if (reserved.has(recommendationId)) {
        return res.status(404).json({ error: 'not_found' });
      }

      const max = await getMaxRuntime();
      const record = max.outcomeGet(String(clientId), recommendationId);
      if (!record) {
        return res.status(404).json({ error: 'outcome_not_found' });
      }
      res.set('Cache-Control', 'no-store');
      return res.json(record);
    } catch (err) {
      console.error('[outcome-get]', err);
      return res.status(500).json({
        error: 'outcome_get_failed',
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
