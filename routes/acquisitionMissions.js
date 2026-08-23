'use strict';

/**
 * SPEC-118 — Acquisition Mission Orchestration APIs.
 *
 * GET  /acquisition-missions
 * GET  /api/v1/amo/missions
 * POST /api/v1/amo/missions
 * GET  /api/v1/amo/missions/:id
 * POST /api/v1/amo/missions/:id/contribute
 * POST /api/v1/amo/missions/:id/progress
 * GET  /api/v1/amo/missions/:id/context
 * GET  /api/v1/amo/missions/:id/explain
 * GET  /api/v1/amo/learning
 * POST /api/v1/amo/ask
 */

const express = require('express');
const path = require('path');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const { resolveActiveTenantId } = require('../packages/max/workspace/TenantContextResolver');
const pool = require('../db');
const {
  createMission,
  inspectMission,
  listMissions,
  contribute,
  progressMission,
  answerOperator,
} = require('../services/acquisitionMission');

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

function actorFrom(req) {
  const user = req.user || (req.session && req.session.user) || {};
  return {
    id: user.id || user.email || 'operator',
    name: user.name || user.email || 'operator',
    role: user.role === 'max' ? 'max' : 'operator',
  };
}

function fail(res, err, fallbackCode, fallbackStatus = 500) {
  const code = (err && err.code) || fallbackCode;
  const status =
    code === 'amo_mission_not_found' ? 404
      : code === 'MISSION_STATE_INCONSISTENT' ? 409
      : code === 'amo_tenant_required'
        || code === 'amo_tenant_mismatch'
        || code === 'amo_objective_required'
        || code === 'amo_contract_violation'
        || code === 'amo_contract_empty'
        || code === 'amo_stage_blocked'
        || code === 'amo_max_orchestrates'
        || code === 'amo_already_at_stage'
        || code === 'no_tenant'
        ? 400
        : fallbackStatus;
  return res.status(status).json({
    error: code,
    message: String((err && err.message) || err),
  });
}

router.get('/acquisition-missions', requireActor, (_req, res) => {
  res.sendFile(path.join(__dirname, '..', 'public', 'acquisition-missions.html'));
});

router.get('/api/v1/amo/missions', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const missions = await listMissions(tenantId, { pool });
    noStore(res);
    return res.json({ kind: 'acquisition_missions', spec: 'SPEC-118', tenantId, missions });
  } catch (err) {
    console.error('[amo] list', err);
    return fail(res, err, 'amo_list_failed');
  }
});

router.post('/api/v1/amo/missions', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const mission = await createMission({
      ...req.body,
      tenantId,
      clientId: Number(tenantId),
      createdBy: req.body?.createdBy || 'max',
      owner: req.body?.owner || 'Operator',
    }, { pool });
    noStore(res);
    return res.status(201).json({ spec: 'SPEC-118', mission });
  } catch (err) {
    console.error('[amo] create', err);
    return fail(res, err, 'amo_create_failed');
  }
});

router.get('/api/v1/amo/missions/:id', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const snapshot = await inspectMission(req.params.id, { tenantId, pool });
    noStore(res);
    return res.json({ spec: 'SPEC-118', tenantId, ...snapshot });
  } catch (err) {
    console.error('[amo] inspect', err);
    return fail(res, err, 'amo_inspect_failed');
  }
});

router.post('/api/v1/amo/missions/:id/contribute', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const result = await contribute(req.params.id, req.body || {}, { tenantId, pool });
    noStore(res);
    return res.json({ spec: 'SPEC-118', ...result });
  } catch (err) {
    console.error('[amo] contribute', err);
    return fail(res, err, 'amo_contribute_failed');
  }
});

router.post('/api/v1/amo/missions/:id/progress', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const actor = actorFrom(req);
    if (req.body?.asMax === true) actor.role = 'max';
    const mission = await progressMission(
      req.params.id,
      actor,
      { stage: req.body?.stage },
      { tenantId, pool }
    );
    noStore(res);
    return res.json({ spec: 'SPEC-118', mission });
  } catch (err) {
    console.error('[amo] progress', err);
    return fail(res, err, 'amo_progress_failed');
  }
});

router.get('/api/v1/amo/missions/:id/context', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const snapshot = await inspectMission(req.params.id, { tenantId, pool });
    noStore(res);
    return res.json({ spec: 'SPEC-118', context: snapshot.context });
  } catch (err) {
    console.error('[amo] context', err);
    return fail(res, err, 'amo_context_failed');
  }
});

router.get('/api/v1/amo/missions/:id/explain', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const snapshot = await inspectMission(req.params.id, { tenantId, pool });
    noStore(res);
    return res.json({ spec: 'SPEC-118', why: snapshot.why });
  } catch (err) {
    console.error('[amo] explain', err);
    return fail(res, err, 'amo_explain_failed');
  }
});

router.get('/api/v1/amo/learning', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const missions = await listMissions(tenantId, { pool });
    const { getAcquisitionMissionRuntime } = require('../services/acquisitionMissionRuntime');
    noStore(res);
    return res.json({
      spec: 'SPEC-118',
      tenantId,
      learning: getAcquisitionMissionRuntime().engine().learning(tenantId),
      missionCount: missions.length,
    });
  } catch (err) {
    console.error('[amo] learning', err);
    return fail(res, err, 'amo_learning_failed');
  }
});

router.post('/api/v1/amo/ask', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const answered = await answerOperator(req.body?.question, {
      tenantId,
      missionId: req.body?.missionId,
      previousReplyRate: req.body?.previousReplyRate,
    }, { pool });
    noStore(res);
    return res.json({ spec: 'SPEC-118', ...answered });
  } catch (err) {
    console.error('[amo] ask', err);
    return fail(res, err, 'amo_ask_failed');
  }
});

module.exports = router;
