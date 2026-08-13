'use strict';

/**
 * SPEC-092 — Content Outcome Intelligence APIs.
 * Product brief used "SPEC-085"; repository SPEC-085 is Executive Business Brief.
 *
 * POST /api/content-publications
 * GET  /api/content-publications/:id
 * POST /api/content-publications/:id/performance
 * POST /api/content-publications/:id/outcomes
 * POST /api/content-publications/:id/signals
 * GET  /api/content-publications/:id/outcomes
 * GET  /api/content-publications/:id/timeline
 * GET  /api/content-outcomes
 * GET  /api/content-outcomes/compare
 * GET  /api/content-outcomes/recent
 * GET  /content-outcomes  (operator capture UI)
 */

const express = require('express');
const path = require('path');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const {
  getRequestClientId,
  normalizeClientId,
} = require('../utils/clientContext');
const {
  ContentOutcomeError,
  CHANNELS,
  OBJECTIVES,
  BUSINESS_OUTCOME_TYPES,
  ATTRIBUTION_LEVELS,
  SIGNAL_TYPES,
  createContentPublication,
  addPerformanceSnapshot,
  addBusinessOutcome,
  addQualitativeSignal,
  getPublicationOutcome,
  getContentOutcomeTimeline,
  listContentOutcomes,
  getRecentContentOutcomes,
  compareContentOutcomes,
  toIntelligencePayload,
} = require('../services/contentOutcomeIntelligence');

const requireOperator = [
  requireAuth,
  requireRole('admin', 'manager'),
];

const requireDashboardRead = [
  requireAuth,
  requireRole('admin', 'manager', 'viewer'),
];

function noStore(res) {
  res.set('Cache-Control', 'no-store');
}

function resolveClientId(req, body = {}) {
  const fromBody = normalizeClientId(body.clientId ?? body.client_id ?? body.tenantId);
  if (fromBody != null) return fromBody;
  const fromQuery = normalizeClientId(req.query.client_id ?? req.query.tenantId);
  if (fromQuery != null) return fromQuery;
  return getRequestClientId(req);
}

function sendError(res, err) {
  if (err instanceof ContentOutcomeError) {
    return res.status(err.status || 400).json({
      error: err.code,
      message: err.message,
    });
  }
  console.error('[content-outcome]', err);
  return res.status(500).json({
    error: 'content_outcome_failed',
    message: err && err.message ? String(err.message) : 'failed',
  });
}

/** Operator capture UI */
router.get('/content-outcomes', requireOperator, (_req, res) => {
  res.set('Cache-Control', 'no-store');
  return res.sendFile(path.join(__dirname, '..', 'public', 'content-outcomes.html'));
});

router.get('/api/content-outcomes/meta', requireDashboardRead, (_req, res) => {
  noStore(res);
  return res.json({
    channels: CHANNELS,
    objectives: OBJECTIVES,
    businessOutcomeTypes: BUSINESS_OUTCOME_TYPES,
    attributionLevels: ATTRIBUTION_LEVELS,
    signalTypes: SIGNAL_TYPES,
  });
});

router.post('/api/content-publications', requireOperator, async (req, res) => {
  try {
    const clientId = resolveClientId(req, req.body || {});
    if (clientId == null) {
      return res.status(400).json({ error: 'client_id required' });
    }
    const created = await createContentPublication({
      ...(req.body || {}),
      clientId,
    });
    noStore(res);
    return res.status(201).json(created);
  } catch (err) {
    return sendError(res, err);
  }
});

router.get('/api/content-publications/:id', requireDashboardRead, async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    const full = await getPublicationOutcome(req.params.id, { clientId });
    noStore(res);
    return res.json(full);
  } catch (err) {
    return sendError(res, err);
  }
});

router.post(
  '/api/content-publications/:id/performance',
  requireOperator,
  async (req, res) => {
    try {
      const clientId = resolveClientId(req, req.body || {});
      const snap = await addPerformanceSnapshot(req.params.id, {
        ...(req.body || {}),
        clientId,
      });
      noStore(res);
      return res.status(201).json(snap);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.post(
  '/api/content-publications/:id/outcomes',
  requireOperator,
  async (req, res) => {
    try {
      const clientId = resolveClientId(req, req.body || {});
      const outcome = await addBusinessOutcome(req.params.id, {
        ...(req.body || {}),
        clientId,
      });
      noStore(res);
      return res.status(201).json(outcome);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.post(
  '/api/content-publications/:id/signals',
  requireOperator,
  async (req, res) => {
    try {
      const clientId = resolveClientId(req, req.body || {});
      const signal = await addQualitativeSignal(req.params.id, {
        ...(req.body || {}),
        clientId,
      });
      noStore(res);
      return res.status(201).json(signal);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.get(
  '/api/content-publications/:id/outcomes',
  requireDashboardRead,
  async (req, res) => {
    try {
      const clientId = resolveClientId(req);
      const full = await getPublicationOutcome(req.params.id, { clientId });
      noStore(res);
      return res.json({
        publicationId: full.publication.id,
        businessOutcomes: full.businessOutcomes,
        qualitativeSignals: full.qualitativeSignals,
        performanceSnapshots: full.performanceSnapshots,
        evidenceReferences: full.evidenceReferences,
        intelligence: toIntelligencePayload(full),
      });
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.get(
  '/api/content-publications/:id/timeline',
  requireDashboardRead,
  async (req, res) => {
    try {
      const clientId = resolveClientId(req);
      const timeline = await getContentOutcomeTimeline(req.params.id, { clientId });
      noStore(res);
      return res.json(timeline);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.get('/api/content-outcomes', requireDashboardRead, async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    if (clientId == null) {
      return res.status(400).json({ error: 'client_id required' });
    }
    const items = await listContentOutcomes({
      clientId,
      channel: req.query.channel,
      objective: req.query.objective,
      topic: req.query.topic,
      format: req.query.format,
      intendedAudience: req.query.intended_audience || req.query.intendedAudience,
      from: req.query.from || req.query.date_from,
      to: req.query.to || req.query.date_to,
      limit: req.query.limit,
    });
    noStore(res);
    return res.json({
      clientId,
      tenantId: String(clientId),
      count: items.length,
      items: items.map((item) => ({
        ...item,
        intelligence: toIntelligencePayload(item),
      })),
    });
  } catch (err) {
    return sendError(res, err);
  }
});

router.get('/api/content-outcomes/recent', requireDashboardRead, async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    if (clientId == null) {
      return res.status(400).json({ error: 'client_id required' });
    }
    const limit = req.query.limit != null ? Number(req.query.limit) : 5;
    const items = await getRecentContentOutcomes(clientId, limit);
    noStore(res);
    return res.json({
      clientId,
      tenantId: String(clientId),
      count: items.length,
      items: items.map((item) => toIntelligencePayload(item)),
    });
  } catch (err) {
    return sendError(res, err);
  }
});

router.get('/api/content-outcomes/compare', requireDashboardRead, async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    if (clientId == null) {
      return res.status(400).json({ error: 'client_id required' });
    }
    const comparison = await compareContentOutcomes({
      clientId,
      channel: req.query.channel,
      objective: req.query.objective,
      topic: req.query.topic,
      format: req.query.format,
      from: req.query.from || req.query.date_from,
      to: req.query.to || req.query.date_to,
      groupBy: req.query.group_by || req.query.groupBy || 'objective',
    });
    noStore(res);
    return res.json(comparison);
  } catch (err) {
    return sendError(res, err);
  }
});

module.exports = router;
