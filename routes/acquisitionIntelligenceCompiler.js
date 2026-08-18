'use strict';

/**
 * SPEC-113 — Acquisition Intelligence Compiler APIs.
 *
 * POST /api/v1/aic/workspaces
 * POST /api/v1/aic/workspaces/:id/documents
 * POST /api/v1/aic/workspaces/:id/compile
 * GET  /api/v1/aic/workspaces/:id
 * GET  /api/v1/aic/workspaces
 * POST /api/v1/aic/concepts/:id/review
 * POST /api/v1/aic/workspaces/:id/approve
 * POST /api/v1/aic/workspaces/:id/publish
 * GET  /api/v1/aic/workspaces/:id/aim
 */

const express = require('express');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const {
  createWorkspace,
  addDocuments,
  compileWorkspace,
  reviewConcept,
  approveWorkspace,
  publishWorkspace,
  getWorkspace,
  listWorkspaces,
} = require('../services/acquisitionIntelligenceCompiler');

const requireAdmin = [requireAuth, requireRole('admin', 'manager')];

function noStore(res) {
  res.set('Cache-Control', 'no-store');
}

function fail(res, err, fallbackCode, fallbackStatus = 500) {
  const code = (err && err.code) || fallbackCode;
  const status =
    code === 'aic_not_found' || code === 'aic_concept_not_found'
      ? 404
      : code === 'aic_not_approved' ||
          code === 'aic_not_ready_for_review' ||
          code === 'aic_unreviewed_concepts' ||
          code === 'aic_mission_required' ||
          code === 'aic_no_documents' ||
          code === 'aic_empty_document' ||
          code === 'aic_published_immutable' ||
          code === 'aic_unknown_review_action' ||
          code === 'aic_no_outreach' ||
          code === 'aic_nothing_to_publish' ||
          code === 'aic_merge_ids_required'
        ? 400
        : fallbackStatus;
  return res.status(status).json({
    error: code,
    message: String((err && err.message) || err),
  });
}

router.post('/api/v1/aic/workspaces', requireAdmin, (req, res) => {
  try {
    const workspace = createWorkspace(req.body || {});
    noStore(res);
    return res.status(201).json({
      kind: 'aic_workspace',
      spec: 'SPEC-113',
      isOperatingFact: false,
      workspace,
    });
  } catch (err) {
    console.error('[aic] create', err);
    return fail(res, err, 'aic_create_failed');
  }
});

router.get('/api/v1/aic/workspaces', requireAdmin, (req, res) => {
  try {
    noStore(res);
    return res.json({
      kind: 'aic_workspace_list',
      spec: 'SPEC-113',
      workspaces: listWorkspaces(req.query.clientKey || req.query.client_key).map((w) => ({
        id: w.id,
        clientKey: w.clientKey,
        clientName: w.clientName,
        status: w.status,
        documentCount: (w.documents || []).length,
        conceptCount: (w.concepts || []).length,
        aimId: w.aimId,
      })),
    });
  } catch (err) {
    console.error('[aic] list', err);
    return fail(res, err, 'aic_list_failed');
  }
});

router.get('/api/v1/aic/workspaces/:id', requireAdmin, (req, res) => {
  try {
    const workspace = getWorkspace(req.params.id);
    if (!workspace) return res.status(404).json({ error: 'aic_not_found' });
    noStore(res);
    return res.json({
      kind: 'aic_workspace',
      spec: 'SPEC-113',
      isOperatingFact: false,
      workspace,
    });
  } catch (err) {
    console.error('[aic] get', err);
    return fail(res, err, 'aic_get_failed');
  }
});

router.post('/api/v1/aic/workspaces/:id/documents', requireAdmin, (req, res) => {
  try {
    const docs = req.body && req.body.documents ? req.body.documents : [req.body];
    const workspace = addDocuments(req.params.id, docs);
    noStore(res);
    return res.json({
      kind: 'aic_workspace',
      spec: 'SPEC-113',
      workspace,
    });
  } catch (err) {
    console.error('[aic] ingest', err);
    return fail(res, err, 'aic_ingest_failed');
  }
});

router.post('/api/v1/aic/workspaces/:id/compile', requireAdmin, (req, res) => {
  try {
    const workspace = compileWorkspace(req.params.id);
    noStore(res);
    return res.json({
      kind: 'aic_workspace',
      spec: 'SPEC-113',
      workspace,
    });
  } catch (err) {
    console.error('[aic] compile', err);
    return fail(res, err, 'aic_compile_failed');
  }
});

router.post('/api/v1/aic/concepts/:id/review', requireAdmin, (req, res) => {
  try {
    const conceptId = req.params.id;
    const workspaceId = req.body && req.body.workspaceId;
    if (!workspaceId) {
      return res.status(400).json({ error: 'aic_workspace_required' });
    }
    const workspace = reviewConcept(workspaceId, conceptId, req.body || {}, {
      operator: req.session && req.session.user && req.session.user.email,
    });
    noStore(res);
    return res.json({
      kind: 'aic_workspace',
      spec: 'SPEC-113',
      workspace,
    });
  } catch (err) {
    console.error('[aic] review', err);
    return fail(res, err, 'aic_review_failed');
  }
});

router.post('/api/v1/aic/workspaces/:id/approve', requireAdmin, (req, res) => {
  try {
    const workspace = approveWorkspace(req.params.id, {
      operator: (req.body && req.body.operator) ||
        (req.session && req.session.user && req.session.user.email),
      acceptRemaining: req.body && req.body.acceptRemaining,
    });
    noStore(res);
    return res.json({
      kind: 'aic_workspace',
      spec: 'SPEC-113',
      workspace,
    });
  } catch (err) {
    console.error('[aic] approve', err);
    return fail(res, err, 'aic_approve_failed');
  }
});

router.post('/api/v1/aic/workspaces/:id/publish', requireAdmin, (req, res) => {
  try {
    const result = publishWorkspace(req.params.id);
    noStore(res);
    return res.json({
      kind: 'aic_published_aim',
      spec: 'SPEC-113',
      isOperatingFact: false,
      workspace: result.workspace,
      aim: result.aim,
    });
  } catch (err) {
    console.error('[aic] publish', err);
    return fail(res, err, 'aic_publish_failed');
  }
});

router.get('/api/v1/aic/workspaces/:id/aim', requireAdmin, (req, res) => {
  try {
    const workspace = getWorkspace(req.params.id);
    if (!workspace) return res.status(404).json({ error: 'aic_not_found' });
    if (!workspace.publishedAim) {
      return res.status(404).json({ error: 'aic_aim_not_published' });
    }
    noStore(res);
    return res.json({
      kind: 'acquisition_intelligence_model',
      spec: 'SPEC-113',
      isOperatingFact: false,
      model: workspace.publishedAim,
    });
  } catch (err) {
    console.error('[aic] aim', err);
    return fail(res, err, 'aic_aim_failed');
  }
});

module.exports = router;
