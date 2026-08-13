'use strict';

/**
 * SPEC-093 — Paige Outcome Learning Loop APIs.
 *
 * POST /api/content-learning/evaluate/:publicationId
 * GET  /api/content-learnings
 * GET  /api/content-learnings/:id
 * POST /api/content-learnings/recompute
 * POST /api/paige/content-recommendation
 */

const express = require('express');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const {
  getRequestClientId,
  normalizeClientId,
} = require('../utils/clientContext');
const {
  LEARNING_TYPES,
  LEARNING_STATUSES,
  AUDIENCE_CLASSES,
  ContentLearningError,
  evaluateContentPublication,
  listContentLearnings,
  getContentLearning,
  getRelevantContentLearnings,
  generateContentRecommendation,
  recomputeContentLearnings,
} = require('../services/contentLearning');

const requireOperator = [requireAuth, requireRole('admin', 'manager')];
const requireDashboardRead = [
  requireAuth,
  requireRole('admin', 'manager', 'viewer'),
];

function noStore(res) {
  res.set('Cache-Control', 'no-store');
}

function resolveClientId(req, body = {}) {
  const fromBody = normalizeClientId(
    body.clientId ?? body.client_id ?? body.tenantId
  );
  if (fromBody != null) return fromBody;
  const fromQuery = normalizeClientId(
    req.query.client_id ?? req.query.tenantId
  );
  if (fromQuery != null) return fromQuery;
  return getRequestClientId(req);
}

function sendError(res, err) {
  if (err instanceof ContentLearningError) {
    return res.status(err.status || 400).json({
      error: err.code,
      message: err.message,
    });
  }
  console.error('[content-learning]', err);
  return res.status(500).json({
    error: 'content_learning_failed',
    message: err && err.message ? String(err.message) : 'failed',
  });
}

router.get('/api/content-learnings/meta', requireDashboardRead, (_req, res) => {
  noStore(res);
  return res.json({
    learningTypes: LEARNING_TYPES,
    learningStatuses: LEARNING_STATUSES,
    audienceClasses: AUDIENCE_CLASSES,
  });
});

router.post(
  '/api/content-learning/evaluate/:publicationId',
  requireOperator,
  async (req, res) => {
    try {
      const clientId = resolveClientId(req, req.body || {});
      const result = await evaluateContentPublication(req.params.publicationId, {
        clientId,
      });
      noStore(res);
      return res.json(result);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.get('/api/content-learnings', requireDashboardRead, async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    if (clientId == null) {
      return res.status(400).json({ error: 'client_id required' });
    }
    const items = await listContentLearnings({
      clientId,
      status: req.query.status,
      learningType: req.query.learning_type || req.query.learningType,
      objective: req.query.objective,
      topic: req.query.topic,
      audienceType: req.query.audience || req.query.audience_type,
      channel: req.query.channel,
      limit: req.query.limit,
    });
    noStore(res);
    return res.json({ items, count: items.length });
  } catch (err) {
    return sendError(res, err);
  }
});

router.get('/api/content-learnings/relevant', requireDashboardRead, async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    if (clientId == null) {
      return res.status(400).json({ error: 'client_id required' });
    }
    const items = await getRelevantContentLearnings({
      tenantId: clientId,
      objective: req.query.objective,
      topic: req.query.topic,
      audience: req.query.audience,
      channel: req.query.channel,
      campaignId: req.query.campaign_id,
      limit: req.query.limit,
    });
    noStore(res);
    return res.json({ items, count: items.length });
  } catch (err) {
    return sendError(res, err);
  }
});

router.get('/api/content-learnings/:id', requireDashboardRead, async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    const item = await getContentLearning(req.params.id, { clientId });
    noStore(res);
    return res.json(item);
  } catch (err) {
    return sendError(res, err);
  }
});

router.post('/api/content-learnings/recompute', requireOperator, async (req, res) => {
  try {
    const clientId = resolveClientId(req, req.body || {});
    if (clientId == null) {
      return res.status(400).json({ error: 'client_id required' });
    }
    const result = await recomputeContentLearnings({
      clientId,
      limit: req.body?.limit,
    });
    noStore(res);
    return res.json(result);
  } catch (err) {
    return sendError(res, err);
  }
});

router.post('/api/paige/content-recommendation', requireOperator, async (req, res) => {
  try {
    const clientId = resolveClientId(req, req.body || {});
    if (clientId == null) {
      return res.status(400).json({ error: 'client_id required' });
    }
    const recommendation = await generateContentRecommendation({
      ...(req.body || {}),
      clientId,
      tenantId: clientId,
    });
    noStore(res);
    return res.json(recommendation);
  } catch (err) {
    return sendError(res, err);
  }
});

module.exports = router;
