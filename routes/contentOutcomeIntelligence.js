'use strict';

/**
 * SPEC-092 — Content Outcome Intelligence APIs
 *
 * POST /api/content-publications
 * GET  /api/content-publications/:id
 * POST /api/content-publications/:id/performance
 * POST /api/content-publications/:id/outcomes
 * POST /api/content-publications/:id/signals
 * GET  /api/content-publications/:id/outcomes
 * GET  /api/content-outcomes
 * GET  /api/content-outcomes/compare
 * GET  /api/v1/content-outcomes/intelligence
 * GET  /content-outcomes  (minimal operator capture UI)
 */

const path = require('path');
const express = require('express');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const { getRequestClientId } = require('../utils/clientContext');
const {
  ContentOutcomeError,
  createContentPublication,
  addPerformanceSnapshot,
  addBusinessOutcome,
  addQualitativeSignal,
  getPublicationOutcome,
  listContentOutcomes,
  compareContentOutcomes,
  getContentOutcomesForIntelligence,
  ensureContentOutcomeSchema,
} = require('../services/contentOutcomeIntelligence');

const requireDashboardRead = [
  requireAuth,
  requireRole('admin', 'manager', 'viewer', 'client'),
];

const requireOperator = [requireAuth, requireRole('admin', 'manager')];

function noStore(res) {
  res.set('Cache-Control', 'no-store');
}

function tenantFromReq(req) {
  return String(getRequestClientId(req));
}

function handleError(res, err, label) {
  if (err instanceof ContentOutcomeError) {
    noStore(res);
    return res.status(err.status || 400).json({
      error: err.code,
      message: err.message,
    });
  }
  console.error(`[content-outcome:${label}]`, err);
  return res.status(500).json({
    error: 'content_outcome_failed',
    message: err && err.message ? String(err.message) : 'failed',
  });
}

// Minimal operator capture UI
router.get('/content-outcomes', requireOperator, (_req, res) => {
  res.sendFile(path.join(__dirname, '..', 'public', 'content-outcomes.html'));
});

router.post('/api/content-publications', requireOperator, async (req, res) => {
  try {
    await ensureContentOutcomeSchema();
    const tenantId = tenantFromReq(req);
    const row = await createContentPublication({
      ...(req.body || {}),
      tenant_id: tenantId,
    });
    noStore(res);
    return res.status(201).json(row);
  } catch (err) {
    return handleError(res, err, 'create-publication');
  }
});

router.get(
  '/api/content-publications/:id',
  requireDashboardRead,
  async (req, res) => {
    try {
      const payload = await getPublicationOutcome(req.params.id, {
        tenantId: tenantFromReq(req),
      });
      noStore(res);
      return res.json(payload);
    } catch (err) {
      return handleError(res, err, 'get-publication');
    }
  }
);

router.post(
  '/api/content-publications/:id/performance',
  requireOperator,
  async (req, res) => {
    try {
      const row = await addPerformanceSnapshot(req.params.id, {
        ...(req.body || {}),
        tenant_id: tenantFromReq(req),
      });
      noStore(res);
      return res.status(201).json(row);
    } catch (err) {
      return handleError(res, err, 'add-performance');
    }
  }
);

router.post(
  '/api/content-publications/:id/outcomes',
  requireOperator,
  async (req, res) => {
    try {
      let outcomeEngine = null;
      try {
        const { getMaxRuntime } = require('../utils/maxRuntime');
        const max = await getMaxRuntime();
                outcomeEngine = max && max.outcome ? max.outcome : null;
                if (!outcomeEngine && max && typeof max.recordOutcome === 'function') {
                  outcomeEngine = { record: (input) => max.recordOutcome(input) };
                }
      } catch {
        outcomeEngine = null;
      }

      const row = await addBusinessOutcome(
        req.params.id,
        {
          ...(req.body || {}),
          tenant_id: tenantFromReq(req),
        },
        { outcomeEngine }
      );
      noStore(res);
      return res.status(201).json(row);
    } catch (err) {
      return handleError(res, err, 'add-outcome');
    }
  }
);

router.post(
  '/api/content-publications/:id/signals',
  requireOperator,
  async (req, res) => {
    try {
      const row = await addQualitativeSignal(req.params.id, {
        ...(req.body || {}),
        tenant_id: tenantFromReq(req),
      });
      noStore(res);
      return res.status(201).json(row);
    } catch (err) {
      return handleError(res, err, 'add-signal');
    }
  }
);

router.get(
  '/api/content-publications/:id/outcomes',
  requireDashboardRead,
  async (req, res) => {
    try {
      const payload = await getPublicationOutcome(req.params.id, {
        tenantId: tenantFromReq(req),
      });
      noStore(res);
      return res.json(payload);
    } catch (err) {
      return handleError(res, err, 'get-outcomes');
    }
  }
);

router.get('/api/content-outcomes', requireDashboardRead, async (req, res) => {
  try {
    const items = await listContentOutcomes({
      tenantId: tenantFromReq(req),
      channel: req.query.channel,
      objective: req.query.objective,
      topic: req.query.topic,
      from: req.query.from,
      to: req.query.to,
      limit: req.query.limit,
    });
    noStore(res);
    return res.json({ items, count: items.length });
  } catch (err) {
    return handleError(res, err, 'list');
  }
});

router.get(
  '/api/content-outcomes/compare',
  requireDashboardRead,
  async (req, res) => {
    try {
      const comparison = await compareContentOutcomes({
        tenantId: tenantFromReq(req),
        channel: req.query.channel,
        objective: req.query.objective,
        topic: req.query.topic,
        from: req.query.from,
        to: req.query.to,
        limit: req.query.limit,
      });
      noStore(res);
      return res.json(comparison);
    } catch (err) {
      return handleError(res, err, 'compare');
    }
  }
);

/**
 * Max / intelligence consumer surface — same architecture, no parallel store.
 */
router.get(
  '/api/v1/content-outcomes/intelligence',
  requireDashboardRead,
  async (req, res) => {
    try {
      const payload = await getContentOutcomesForIntelligence(
        tenantFromReq(req),
        { limit: req.query.limit }
      );
      noStore(res);
      return res.json(payload);
    } catch (err) {
      return handleError(res, err, 'intelligence');
    }
  }
);

module.exports = router;
