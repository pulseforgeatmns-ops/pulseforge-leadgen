'use strict';

/**
 * SPEC-116 — Operator Scorecard Intelligence APIs.
 *
 * POST /api/v1/osi/scorecards/draft
 * GET  /api/v1/osi/scorecards
 * GET  /api/v1/osi/runtime
 * POST /api/v1/osi/scorecards/:id/metrics/:metricId/review
 * POST /api/v1/osi/scorecards/:id/metrics
 * POST /api/v1/osi/scorecards/:id/reorder
 * POST /api/v1/osi/scorecards/:id/feedback
 * POST /api/v1/osi/scorecards/:id/approve
 * POST /api/v1/osi/scorecards/:id/evolve
 * GET  /operator-scorecard
 */

const express = require('express');
const path = require('path');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const {
  resolveActiveTenantId,
} = require('../packages/max/workspace/TenantContextResolver');
const {
  generateDraft,
  getOrCreateDraft,
  getApproved,
  getRuntime,
  review,
  addMetric,
  reorder,
  provideRemovalReason,
  approve,
  evolve,
  briefSectionsFor,
} = require('../services/operatorScorecard');

const requireActor = [requireAuth, requireRole('admin', 'manager', 'client')];

function noStore(res) {
  res.set('Cache-Control', 'no-store');
}

function actorTenantId(req) {
  const user = req.user || (req.session && req.session.user);
  if (user && user.role === 'client') {
    const id = Number(user.client_id);
    return Number.isInteger(id) && id > 0 ? String(id) : null;
  }
  const id = resolveActiveTenantId(req);
  return id != null ? String(id) : null;
}

function fail(res, err, fallbackCode, fallbackStatus = 500) {
  const code = (err && err.code) || fallbackCode;
  const status =
    code === 'osi_not_found' || code === 'osi_metric_not_found'
      ? 404
      : code === 'osi_approved_immutable' ||
          code === 'osi_unreviewed_metrics' ||
          code === 'osi_nothing_to_approve' ||
          code === 'osi_unknown_review_action' ||
          code === 'osi_insufficient_understanding' ||
          code === 'osi_metric_name_required' ||
          code === 'osi_reorder_required' ||
          code === 'osi_superseded'
        ? 400
        : fallbackStatus;
  return res.status(status).json({
    error: code,
    message: String((err && err.message) || err),
  });
}

router.get('/operator-scorecard', requireActor, (_req, res) => {
  res.sendFile(path.join(__dirname, '..', 'public', 'operator-scorecard.html'));
});

router.get('/api/v1/osi/runtime', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({
        error: 'no_tenant',
        message: 'No active client selected.',
      });
    }
    const runtime = await getRuntime(tenantId);
    noStore(res);
    return res.json({
      kind: 'operator_scorecard_runtime',
      spec: 'SPEC-116',
      tenantId,
      runtime,
    });
  } catch (err) {
    console.error('[osi] runtime', err);
    return fail(res, err, 'osi_runtime_failed');
  }
});

router.get('/api/v1/osi/scorecards', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const draft = await getOrCreateDraft(
      {
        tenantId,
        clientId: Number(tenantId) || null,
        businessGoal: req.query.goal || req.query.businessGoal || 'Establish a repeatable acquisition process.',
        objectives: req.query.objective ? [req.query.objective] : ['Establish a repeatable acquisition process.'],
      }
    );
    const approved = await getApproved(tenantId);
    noStore(res);
    return res.json({
      kind: 'operator_scorecard',
      spec: 'SPEC-116',
      isRuntime: false,
      tenantId,
      draft,
      approved,
      brief: briefSectionsFor(approved || draft),
    });
  } catch (err) {
    console.error('[osi] get', err);
    return fail(res, err, 'osi_get_failed');
  }
});

router.post('/api/v1/osi/scorecards/draft', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const body = req.body || {};
    const draft = await generateDraft({
      ...body,
      tenantId,
      clientId: body.clientId || Number(tenantId) || null,
    });
    noStore(res);
    return res.status(201).json({
      kind: 'operator_scorecard_draft',
      spec: 'SPEC-116',
      isRuntime: false,
      draft,
      brief: briefSectionsFor(draft),
    });
  } catch (err) {
    console.error('[osi] draft', err);
    return fail(res, err, 'osi_draft_failed');
  }
});

router.post('/api/v1/osi/scorecards/:id/metrics/:metricId/review', requireActor, async (req, res) => {
  try {
    const user = req.user || (req.session && req.session.user);
    const result = await review(
      req.params.id,
      req.params.metricId,
      req.body || {},
      { operator: (user && (user.email || user.name)) || 'operator' }
    );
    noStore(res);
    return res.json({
      kind: 'operator_scorecard_review',
      spec: 'SPEC-116',
      prompt: result.prompt || null,
      metric: result.metric,
      scorecard: result.scorecard,
    });
  } catch (err) {
    console.error('[osi] review', err);
    return fail(res, err, 'osi_review_failed');
  }
});

router.post('/api/v1/osi/scorecards/:id/metrics', requireActor, async (req, res) => {
  try {
    const user = req.user || (req.session && req.session.user);
    const result = await addMetric(
      req.params.id,
      req.body || {},
      { operator: (user && (user.email || user.name)) || 'operator' }
    );
    noStore(res);
    return res.status(201).json({
      kind: 'operator_scorecard_metric',
      spec: 'SPEC-116',
      metric: result.metric,
      scorecard: result.scorecard,
    });
  } catch (err) {
    console.error('[osi] add', err);
    return fail(res, err, 'osi_add_failed');
  }
});

router.post('/api/v1/osi/scorecards/:id/reorder', requireActor, async (req, res) => {
  try {
    const user = req.user || (req.session && req.session.user);
    const result = await reorder(
      req.params.id,
      (req.body && (req.body.orderedIds || req.body.order)) || [],
      { operator: (user && (user.email || user.name)) || 'operator' }
    );
    noStore(res);
    return res.json({
      kind: 'operator_scorecard',
      spec: 'SPEC-116',
      scorecard: result.scorecard,
    });
  } catch (err) {
    console.error('[osi] reorder', err);
    return fail(res, err, 'osi_reorder_failed');
  }
});

router.post('/api/v1/osi/scorecards/:id/feedback', requireActor, async (req, res) => {
  try {
    const result = await provideRemovalReason(
      req.params.id,
      req.body && (req.body.metricId || req.body.metric_id),
      req.body && req.body.reason
    );
    noStore(res);
    return res.json({
      kind: 'operator_scorecard_feedback',
      spec: 'SPEC-116',
      metric: result.metric,
      scorecard: result.scorecard,
    });
  } catch (err) {
    console.error('[osi] feedback', err);
    return fail(res, err, 'osi_feedback_failed');
  }
});

router.post('/api/v1/osi/scorecards/:id/approve', requireActor, async (req, res) => {
  try {
    const user = req.user || (req.session && req.session.user);
    const approved = await approve(req.params.id, {
      operator: (user && (user.email || user.name)) || 'operator',
      acceptRemaining: req.body && req.body.acceptRemaining,
    });
    noStore(res);
    return res.json({
      kind: 'operator_scorecard_approved',
      spec: 'SPEC-116',
      isRuntime: true,
      scorecard: approved,
      brief: briefSectionsFor(approved),
    });
  } catch (err) {
    console.error('[osi] approve', err);
    return fail(res, err, 'osi_approve_failed');
  }
});

router.post('/api/v1/osi/scorecards/:id/evolve', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    const result = await evolve(tenantId, req.body || {});
    noStore(res);
    return res.json({
      kind: 'operator_scorecard_evolution',
      spec: 'SPEC-116',
      autoApplied: false,
      ...result,
    });
  } catch (err) {
    console.error('[osi] evolve', err);
    return fail(res, err, 'osi_evolve_failed');
  }
});

module.exports = router;
