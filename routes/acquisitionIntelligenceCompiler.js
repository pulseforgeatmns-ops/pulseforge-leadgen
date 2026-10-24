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
  publishAndPersist,
  getWorkspace,
  listWorkspaces,
  rememberWorkspace,
  hydrateWorkspace,
  hydrateClientWorkspaces,
} = require('../services/acquisitionIntelligenceCompiler');
const {
  resolveActiveTenantId,
} = require('../packages/max/workspace/TenantContextResolver');
const { deriveAimStatus } = require('../services/pilotOnboarding');

const requireActor = [requireAuth, requireRole('admin', 'manager', 'client')];

function noStore(res) {
  res.set('Cache-Control', 'no-store');
}

function actorClientId(req) {
  const user = req.user || (req.session && req.session.user);
  if (user && user.role === 'client') {
    const id = Number(user.client_id);
    return Number.isInteger(id) && id > 0 ? id : null;
  }
  return resolveActiveTenantId(req);
}

function assertWorkspaceTenant(req, workspace) {
  const user = req.user || (req.session && req.session.user);
  if (!user || user.role !== 'client' || !workspace) return;
  const owner = Number(workspace.clientId || workspace.client_id);
  if (owner !== Number(user.client_id)) {
    const err = new Error('AIC workspace belongs to another tenant.');
    err.code = 'aic_forbidden';
    throw err;
  }
}

async function persist(workspace) {
  if (!workspace) return workspace;
  await rememberWorkspace(workspace);
  return workspace;
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
          code === 'aic_merge_ids_required' ||
          code === 'aic_forbidden'
        ? code === 'aic_forbidden' ? 403 : 400
        : fallbackStatus;
  return res.status(status).json({
    error: code,
    message: String((err && err.message) || err),
  });
}

router.post('/api/v1/aic/workspaces', requireActor, async (req, res) => {
  try {
    const clientId = actorClientId(req);
    const body = { ...(req.body || {}), clientId: req.body?.clientId || clientId };
    if (req.user && req.user.role === 'client') {
      body.clientId = clientId;
    }
    const workspace = createWorkspace(body);
    await persist(workspace);
    noStore(res);
    return res.status(201).json({
      kind: 'aic_workspace',
      spec: 'SPEC-113',
      isOperatingFact: false,
      aimStatus: publicAimStatus(workspace),
      workspace,
    });
  } catch (err) {
    console.error('[aic] create', err);
    return fail(res, err, 'aic_create_failed');
  }
});

router.get('/api/v1/aic/me', requireActor, async (req, res) => {
  try {
    const clientId = actorClientId(req);
    if (clientId == null) {
      return res.status(400).json({
        error: 'no_tenant',
        message: 'No active workspace.\n\nSelect or activate\na tenant.',
      });
    }
    const hydrated = await hydrateClientWorkspaces(clientId);
    let workspace = hydrated[0] || listWorkspaces().find((w) => Number(w.clientId || w.client_id) === Number(clientId));
    if (!workspace) {
      const user = req.user || (req.session && req.session.user);
      workspace = createWorkspace({
        clientId,
        clientKey: `tenant-${clientId}`,
        clientName: (req.body && req.body.clientName) || `tenant-${clientId}`,
      });
      workspace.clientId = clientId;
      await persist(workspace);
    }
    noStore(res);
    const user = req.user || (req.session && req.session.user);
    return res.json({
      kind: 'aic_workspace',
      spec: 'SPEC-115',
      aimStatus: publicAimStatus(workspace),
      workspace,
      user: user ? { id: user.id, role: user.role, client_id: user.client_id } : null,
    });
  } catch (err) {
    console.error('[aic] me', err);
    return fail(res, err, 'aic_me_failed');
  }
});

router.get('/api/v1/aic/workspaces', requireActor, async (req, res) => {
  try {
    const clientId = actorClientId(req);
    if (clientId != null) await hydrateClientWorkspaces(clientId);
    let rows = listWorkspaces(req.query.clientKey || req.query.client_key);
    if (req.user && req.user.role === 'client') {
      rows = rows.filter((w) => Number(w.clientId || w.client_id) === Number(req.user.client_id));
    }
    noStore(res);
    return res.json({
      kind: 'aic_workspace_list',
      spec: 'SPEC-113',
      workspaces: rows.map((w) => ({
        id: w.id,
        clientKey: w.clientKey,
        clientName: w.clientName,
        clientId: w.clientId || w.client_id,
        status: w.status,
        documentCount: (w.documents || []).length,
        conceptCount: (w.concepts || []).length,
        aimId: w.aimId,
        aimStatus: publicAimStatus(w),
      })),
    });
  } catch (err) {
    console.error('[aic] list', err);
    return fail(res, err, 'aic_list_failed');
  }
});

router.get('/api/v1/aic/workspaces/:id', requireActor, async (req, res) => {
  try {
    const workspace = (await hydrateWorkspace(req.params.id)) || getWorkspace(req.params.id);
    if (!workspace) return res.status(404).json({ error: 'aic_not_found' });
    assertWorkspaceTenant(req, workspace);
    noStore(res);
    return res.json({
      kind: 'aic_workspace',
      spec: 'SPEC-113',
      isOperatingFact: false,
      aimStatus: publicAimStatus(workspace),
      workspace,
    });
  } catch (err) {
    console.error('[aic] get', err);
    return fail(res, err, 'aic_get_failed');
  }
});

router.post('/api/v1/aic/workspaces/:id/documents', requireActor, async (req, res) => {
  try {
    await hydrateWorkspace(req.params.id);
    const existing = getWorkspace(req.params.id);
    assertWorkspaceTenant(req, existing);
    const docs = req.body && req.body.documents ? req.body.documents : [req.body];
    const workspace = addDocuments(req.params.id, docs);
    await persist(workspace);
    noStore(res);
    return res.json({
      kind: 'aic_workspace',
      spec: 'SPEC-113',
      aimStatus: publicAimStatus(workspace),
      workspace,
    });
  } catch (err) {
    console.error('[aic] ingest', err);
    return fail(res, err, 'aic_ingest_failed');
  }
});

router.post('/api/v1/aic/workspaces/:id/compile', requireActor, async (req, res) => {
  try {
    await hydrateWorkspace(req.params.id);
    assertWorkspaceTenant(req, getWorkspace(req.params.id));
    const workspace = compileWorkspace(req.params.id);
    await persist(workspace);
    noStore(res);
    return res.json({
      kind: 'aic_workspace',
      spec: 'SPEC-113',
      aimStatus: publicAimStatus(workspace),
      workspace,
    });
  } catch (err) {
    console.error('[aic] compile', err);
    return fail(res, err, 'aic_compile_failed');
  }
});

router.post('/api/v1/aic/concepts/:id/review', requireActor, async (req, res) => {
  try {
    const conceptId = req.params.id;
    const workspaceId = req.body && req.body.workspaceId;
    if (!workspaceId) {
      return res.status(400).json({ error: 'aic_workspace_required' });
    }
    await hydrateWorkspace(workspaceId);
    assertWorkspaceTenant(req, getWorkspace(workspaceId));
    const workspace = reviewConcept(workspaceId, conceptId, req.body || {}, {
      operator: req.session && req.session.user && req.session.user.email,
    });
    await persist(workspace);
    noStore(res);
    return res.json({
      kind: 'aic_workspace',
      spec: 'SPEC-113',
      aimStatus: publicAimStatus(workspace),
      workspace,
    });
  } catch (err) {
    console.error('[aic] review', err);
    return fail(res, err, 'aic_review_failed');
  }
});

router.post('/api/v1/aic/workspaces/:id/approve', requireActor, async (req, res) => {
  try {
    await hydrateWorkspace(req.params.id);
    assertWorkspaceTenant(req, getWorkspace(req.params.id));
    const workspace = approveWorkspace(req.params.id, {
      operator: (req.body && req.body.operator) ||
        (req.session && req.session.user && req.session.user.email),
      acceptRemaining: req.body && req.body.acceptRemaining,
    });
    await persist(workspace);
    noStore(res);
    return res.json({
      kind: 'aic_workspace',
      spec: 'SPEC-113',
      aimStatus: publicAimStatus(workspace),
      workspace,
    });
  } catch (err) {
    console.error('[aic] approve', err);
    return fail(res, err, 'aic_approve_failed');
  }
});

router.post('/api/v1/aic/workspaces/:id/publish', requireActor, async (req, res) => {
  try {
    await hydrateWorkspace(req.params.id);
    const existing = getWorkspace(req.params.id);
    assertWorkspaceTenant(req, existing);
    const result = await publishAndPersist(req.params.id, {
      clientId: existing && (existing.clientId || existing.client_id || actorClientId(req)),
    });
    noStore(res);
    return res.json({
      kind: 'aic_published_aim',
      spec: 'SPEC-113',
      isOperatingFact: false,
      aimStatus: publicAimStatus(result.workspace),
      workspace: result.workspace,
      aim: result.aim,
    });
  } catch (err) {
    console.error('[aic] publish', err);
    return fail(res, err, 'aic_publish_failed');
  }
});

router.get('/api/v1/aic/workspaces/:id/aim', requireActor, async (req, res) => {
  try {
    const workspace = (await hydrateWorkspace(req.params.id)) || getWorkspace(req.params.id);
    if (!workspace) return res.status(404).json({ error: 'aic_not_found' });
    assertWorkspaceTenant(req, workspace);
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

function publicAimStatus(workspace) {
  return deriveAimStatus({
    published: workspace && workspace.status === 'published',
    status: workspace && workspace.status,
    documentCount: workspace && workspace.documents ? workspace.documents.length : 0,
    compiled: workspace && ['in_review', 'approved', 'published'].includes(workspace.status),
  });
}

module.exports = router;
