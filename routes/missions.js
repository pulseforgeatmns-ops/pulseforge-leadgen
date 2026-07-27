'use strict';

/**
 * Mission API — SPEC-022.
 *
 * POST /api/v1/missions
 * GET  /api/v1/missions
 * GET  /api/v1/missions/:id
 * POST /api/v1/missions/:id/review
 */

const express = require('express');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const {
  getRequestClientId,
  normalizeClientId,
} = require('../utils/clientContext');
const { getMissionEngine, missionEnabled } = require('../utils/missionRuntime');
const { REVIEW_ACTIONS } = require('../packages/mission-engine');

const requireDashboardRead = [
  requireAuth,
  requireRole('admin', 'manager', 'viewer', 'client'),
];
const requireDashboardWrite = [
  requireAuth,
  requireRole('admin', 'manager'),
];

function resolveTenantId(req) {
  const fromQuery =
    req.query && req.query.client_id != null
      ? normalizeClientId(req.query.client_id)
      : null;
  if (fromQuery != null) return fromQuery;
  return getRequestClientId(req);
}

/**
 * POST /api/v1/missions
 * Body: { objective, constraints?, execute? }
 */
router.post('/api/v1/missions', requireDashboardWrite, async (req, res) => {
  try {
    if (!missionEnabled()) {
      return res.status(503).json({
        error: 'mission_engine_disabled',
        message: 'Set MISSION_ENGINE=1 (default) to enable',
      });
    }
    const tenantId = resolveTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'client_id required' });
    }
    const objective = String(req.body?.objective || '').trim();
    if (!objective) {
      return res.status(400).json({ error: 'objective is required' });
    }

    const engine = await getMissionEngine();
    const mission = await engine.createFromObjective({
      objective,
      tenantId: String(tenantId),
      clientId: tenantId,
      constraints: req.body?.constraints || { targetCount: 50 },
      createdBy:
        (req.session && req.session.user && req.session.user.email) || null,
      execute: req.body?.execute !== false,
    });

    res.set('Cache-Control', 'no-store');
    return res.status(201).json({
      mission,
      card: engine.toCard(mission),
    });
  } catch (err) {
    console.error('[missions] create:', err);
    return res.status(500).json({
      error: 'mission_create_failed',
      message: err && err.message ? String(err.message) : 'create failed',
    });
  }
});

/**
 * GET /api/v1/missions
 */
router.get('/api/v1/missions', requireDashboardRead, async (req, res) => {
  try {
    const tenantId = resolveTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'client_id required' });
    }
    const engine = await getMissionEngine();
    const missions = await engine.list({
      tenantId: String(tenantId),
      clientId: tenantId,
      limit: Number(req.query.limit) || 50,
    });
    res.set('Cache-Control', 'no-store');
    return res.json({
      missions,
      cards: missions.map((m) => engine.toCard(m)),
    });
  } catch (err) {
    console.error('[missions] list:', err);
    return res.status(500).json({
      error: 'mission_list_failed',
      message: err && err.message ? String(err.message) : 'list failed',
    });
  }
});

/**
 * GET /api/v1/missions/:id
 */
router.get('/api/v1/missions/:id', requireDashboardRead, async (req, res) => {
  try {
    const engine = await getMissionEngine();
    const workspace = await engine.getWorkspace(req.params.id);
    if (!workspace) {
      return res.status(404).json({ error: 'mission_not_found' });
    }
    const tenantId = resolveTenantId(req);
    if (
      tenantId != null &&
      String(workspace.mission.tenantId) !== String(tenantId) &&
      String(workspace.mission.clientId) !== String(tenantId)
    ) {
      return res.status(403).json({ error: 'tenant_mismatch' });
    }
    res.set('Cache-Control', 'no-store');
    return res.json(workspace);
  } catch (err) {
    console.error('[missions] get:', err);
    return res.status(500).json({
      error: 'mission_get_failed',
      message: err && err.message ? String(err.message) : 'get failed',
    });
  }
});

/**
 * POST /api/v1/missions/:id/review
 * Body: { action: approve|reject|edit|run_again, notes?, edits? }
 * Approve does NOT send outreach (ADR-003).
 */
router.post(
  '/api/v1/missions/:id/review',
  requireDashboardWrite,
  async (req, res) => {
    try {
      const action = String(req.body?.action || '').toLowerCase();
      if (!Object.values(REVIEW_ACTIONS).includes(action)) {
        return res.status(400).json({
          error: 'invalid_action',
          message: `action must be one of: ${Object.values(REVIEW_ACTIONS).join(', ')}`,
        });
      }
      const engine = await getMissionEngine();
      const existing = await engine.get(req.params.id);
      if (!existing) {
        return res.status(404).json({ error: 'mission_not_found' });
      }
      const mission = await engine.review({
        missionId: req.params.id,
        action,
        notes: req.body?.notes || null,
        edits: req.body?.edits || null,
        actor:
          (req.session && req.session.user && req.session.user.email) || null,
      });
      res.set('Cache-Control', 'no-store');
      return res.json({
        mission,
        card: engine.toCard(mission),
        outboundSent: false,
      });
    } catch (err) {
      console.error('[missions] review:', err);
      return res.status(500).json({
        error: 'mission_review_failed',
        message: err && err.message ? String(err.message) : 'review failed',
      });
    }
  }
);

module.exports = router;
