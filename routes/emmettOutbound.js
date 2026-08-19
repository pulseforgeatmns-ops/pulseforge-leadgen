'use strict';

/**
 * SPEC-117 — Emmett Outbound Infrastructure Intelligence APIs.
 *
 * GET  /emmett-outbound
 * GET  /api/v1/eoi/dashboard
 * POST /api/v1/eoi/plan
 * POST /api/v1/eoi/plans/:id/approve
 * POST /api/v1/eoi/plans/:id/acknowledge
 * POST /api/v1/eoi/outcomes
 */

const express = require('express');
const path = require('path');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const { resolveActiveTenantId } = require('../packages/max/workspace/TenantContextResolver');
const pool = require('../db');
const {
  planFromClient,
  approvePlan,
  acknowledgeHalt,
  ingestOutcome,
} = require('../services/emmettOutbound');

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
    code === 'eoi_plan_not_found' ? 404
      : code === 'eoi_operator_acknowledgement_required'
        || code === 'eoi_halt_blocks_approval'
        || code === 'eoi_no_halt'
        || code === 'eoi_tenant_required'
        || code === 'no_tenant'
        ? 400
        : fallbackStatus;
  return res.status(status).json({
    error: code,
    message: String((err && err.message) || err),
  });
}

router.get('/emmett-outbound', requireActor, (_req, res) => {
  res.sendFile(path.join(__dirname, '..', 'public', 'emmett-outbound.html'));
});

router.get('/api/v1/eoi/dashboard', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const day = await planFromClient(Number(tenantId), { pool });
    noStore(res);
    return res.json({
      kind: 'emmett_outbound_dashboard',
      spec: 'SPEC-117',
      tenantId,
      dashboard: day.dashboard,
      plan: day.plan,
      health: day.health,
      capacity: day.capacity,
      governor: day.governor,
      queue: day.queue,
    });
  } catch (err) {
    console.error('[eoi] dashboard', err);
    return fail(res, err, 'eoi_dashboard_failed');
  }
});

router.post('/api/v1/eoi/plan', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const day = await planFromClient(Number(tenantId), {
      pool,
      snapshot: req.body?.snapshot,
      prospects: req.body?.prospects,
    });
    noStore(res);
    return res.json({ spec: 'SPEC-117', tenantId, ...day });
  } catch (err) {
    console.error('[eoi] plan', err);
    return fail(res, err, 'eoi_plan_failed');
  }
});

router.post('/api/v1/eoi/plans/:id/approve', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const approved = await approvePlan(
      req.params.id,
      actorFrom(req),
      {
        approvedCapacity: req.body?.approvedCapacity,
        allowLegacySequences: req.body?.allowLegacySequences === true,
        ack: req.body?.ack,
      },
      { tenantId, pool }
    );
    if (String(approved.tenantId) !== String(tenantId)) {
      return res.status(404).json({ error: 'eoi_plan_not_found', message: 'Send plan not found.' });
    }
    noStore(res);
    return res.json({ spec: 'SPEC-117', plan: approved });
  } catch (err) {
    console.error('[eoi] approve', err);
    return fail(res, err, 'eoi_approve_failed');
  }
});

router.post('/api/v1/eoi/plans/:id/acknowledge', requireActor, async (req, res) => {
  try {
    const ack = await acknowledgeHalt(req.params.id, actorFrom(req), req.body?.note, {
      tenantId: actorTenantId(req),
      pool,
    });
    noStore(res);
    return res.json({ spec: 'SPEC-117', ack });
  } catch (err) {
    console.error('[eoi] acknowledge', err);
    return fail(res, err, 'eoi_ack_failed');
  }
});

router.post('/api/v1/eoi/outcomes', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const result = await ingestOutcome({
      ...req.body,
      tenantId,
      clientId: Number(tenantId),
    });
    noStore(res);
    return res.json({ spec: 'SPEC-117', ...result });
  } catch (err) {
    console.error('[eoi] outcome', err);
    return fail(res, err, 'eoi_outcome_failed');
  }
});

module.exports = router;
