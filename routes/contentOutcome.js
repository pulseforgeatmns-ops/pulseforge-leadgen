'use strict';

/**
 * Content Outcome Intelligence APIs (SPEC-092 / planning draft SPEC-085).
 *
 * POST /api/content-publications
 * GET  /api/content-publications/:id
 * POST /api/content-publications/:id/performance
 * POST /api/content-publications/:id/outcomes
 * POST /api/content-publications/:id/signals
 * GET  /api/content-publications/:id/outcomes
 * GET  /api/content-outcomes
 * GET  /api/v1/content-outcomes/recent  (Max / intelligence consumers)
 * GET  /content-outcome               (operator UI)
 */

const path = require('path');
const express = require('express');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const {
  getRequestClientId,
  normalizeClientId,
} = require('../utils/clientContext');
const {
  getContentOutcomeService,
  ContentOutcomeError,
} = require('../services/contentOutcome');

const requireOperator = [
  requireAuth,
  requireRole('admin', 'manager', 'viewer', 'client'),
];

const requireWrite = [requireAuth, requireRole('admin', 'manager', 'client')];

function resolveClientId(req) {
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

function handleError(res, err, label) {
  if (err instanceof ContentOutcomeError) {
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

function getService(req) {
  // Prefer shared Postgres service; OutcomeEngine hooked lazily when Max is available.
  const service = getContentOutcomeService();
  return service;
}

router.get('/content-outcome', requireOperator, (req, res) => {
  res.set('Cache-Control', 'no-store');
  res.sendFile(path.join(__dirname, '..', 'public', 'content-outcome.html'));
});

/**
 * POST /api/content-publications
 */
router.post('/api/content-publications', requireWrite, async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    if (clientId == null) {
      return res.status(400).json({ error: 'client_id required' });
    }
    const service = getService(req);
    const result = await service.createPublication({
      ...(req.body || {}),
      client_id: clientId,
      tenant_id: String(clientId),
    });
    res.set('Cache-Control', 'no-store');
    return res.status(201).json(result);
  } catch (err) {
    return handleError(res, err, 'create-publication');
  }
});

/**
 * GET /api/content-publications/:id
 */
router.get('/api/content-publications/:id', requireOperator, async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    if (clientId == null) {
      return res.status(400).json({ error: 'client_id required' });
    }
    const service = getService(req);
    const full = await service.getPublicationOutcome(String(clientId), req.params.id);
    res.set('Cache-Control', 'no-store');
    return res.json(full);
  } catch (err) {
    return handleError(res, err, 'get-publication');
  }
});

/**
 * POST /api/content-publications/:id/performance
 */
router.post(
  '/api/content-publications/:id/performance',
  requireWrite,
  async (req, res) => {
    try {
      const clientId = resolveClientId(req);
      if (clientId == null) {
        return res.status(400).json({ error: 'client_id required' });
      }
      const service = getService(req);
      const snapshot = await service.addPerformanceSnapshot(
        String(clientId),
        req.params.id,
        req.body || {}
      );
      res.set('Cache-Control', 'no-store');
      return res.status(201).json(snapshot);
    } catch (err) {
      return handleError(res, err, 'add-performance');
    }
  }
);

/**
 * POST /api/content-publications/:id/outcomes
 */
router.post(
  '/api/content-publications/:id/outcomes',
  requireWrite,
  async (req, res) => {
    try {
      const clientId = resolveClientId(req);
      if (clientId == null) {
        return res.status(400).json({ error: 'client_id required' });
      }
      const service = getService(req);
      const outcome = await service.addBusinessOutcome(
        String(clientId),
        req.params.id,
        req.body || {}
      );
      res.set('Cache-Control', 'no-store');
      return res.status(201).json(outcome);
    } catch (err) {
      return handleError(res, err, 'add-outcome');
    }
  }
);

/**
 * POST /api/content-publications/:id/signals
 */
router.post(
  '/api/content-publications/:id/signals',
  requireWrite,
  async (req, res) => {
    try {
      const clientId = resolveClientId(req);
      if (clientId == null) {
        return res.status(400).json({ error: 'client_id required' });
      }
      const service = getService(req);
      const signal = await service.addQualitativeSignal(
        String(clientId),
        req.params.id,
        req.body || {}
      );
      res.set('Cache-Control', 'no-store');
      return res.status(201).json(signal);
    } catch (err) {
      return handleError(res, err, 'add-signal');
    }
  }
);

/**
 * GET /api/content-publications/:id/outcomes
 * Complete outcome history (alias of GET publication detail).
 */
router.get(
  '/api/content-publications/:id/outcomes',
  requireOperator,
  async (req, res) => {
    try {
      const clientId = resolveClientId(req);
      if (clientId == null) {
        return res.status(400).json({ error: 'client_id required' });
      }
      const service = getService(req);
      const full = await service.getPublicationOutcome(String(clientId), req.params.id);
      res.set('Cache-Control', 'no-store');
      return res.json(full);
    } catch (err) {
      return handleError(res, err, 'get-outcomes');
    }
  }
);

/**
 * GET /api/content-outcomes
 * List + deterministic comparison.
 */
router.get('/api/content-outcomes', requireOperator, async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    if (clientId == null) {
      return res.status(400).json({ error: 'client_id required' });
    }
    const service = getService(req);
    const result = await service.listContentOutcomes({
      tenantId: String(clientId),
      clientId,
      channel: req.query.channel,
      objective: req.query.objective,
      topic: req.query.topic,
      format: req.query.format,
      dateFrom: req.query.date_from || req.query.from,
      dateTo: req.query.date_to || req.query.to,
      limit: req.query.limit,
    });
    res.set('Cache-Control', 'no-store');
    return res.json(result);
  } catch (err) {
    return handleError(res, err, 'list-outcomes');
  }
});

/**
 * GET /api/v1/content-outcomes/recent
 * Max / intelligence consumer surface — same store, no parallel Max store.
 */
router.get(
  '/api/v1/content-outcomes/recent',
  requireOperator,
  async (req, res) => {
    try {
      const clientId = resolveClientId(req);
      if (clientId == null) {
        return res.status(400).json({ error: 'client_id required' });
      }
      const limit = Math.min(parseInt(req.query.limit, 10) || 5, 50);
      const service = getService(req);
      const result = await service.getRecentContentOutcomes(String(clientId), limit);
      res.set('Cache-Control', 'no-store');
      return res.json(result);
    } catch (err) {
      return handleError(res, err, 'recent');
    }
  }
);

/**
 * GET /api/v1/content-outcomes/meta
 * Enumerations for fast-capture UI.
 */
router.get('/api/v1/content-outcomes/meta', requireOperator, async (req, res) => {
  const service = getService(req);
  res.set('Cache-Control', 'no-store');
  return res.json(service.constants);
});

module.exports = router;
