'use strict';

/**
 * SPEC-247 — Canonical Acquisition Knowledge APIs.
 */

const express = require('express');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const { resolveActiveTenantId } = require('../packages/max/workspace/TenantContextResolver');
const pool = require('../db');
const acquisitionKnowledge = require('../services/acquisitionKnowledge');

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

function parseTags(value) {
  if (Array.isArray(value)) return value.map(String).filter(Boolean);
  if (typeof value === 'string') return value.split(',').map((part) => part.trim()).filter(Boolean);
  return [];
}

function fail(res, err, fallbackCode, fallbackStatus = 500) {
  const code = (err && err.code) || fallbackCode;
  const status =
    code === 'ak_not_found' ? 404
      : code === 'ak_role_forbidden'
        || code === 'ak_promotion_forbidden' ? 403
        : code === 'ak_tenant_required'
          || code === 'ak_invalid_scope'
          || code === 'ak_invalid_object_type'
          || code === 'ak_invalid_lifecycle_state'
          || code === 'ak_lifecycle_transition_invalid'
          || code === 'ak_lifecycle_evidence_required'
          || code === 'ak_lifecycle_evidence_insufficient'
          || code === 'ak_mission_scope_requires_mission'
          || code === 'ak_title_required'
          || code === 'no_tenant'
          ? 400
          : fallbackStatus;
  return res.status(status).json({
    error: code,
    message: String((err && err.message) || err),
  });
}

router.get('/api/v1/acquisition-knowledge', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (!tenantId) return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    const knowledge = await acquisitionKnowledge.retrieveKnowledge({
      tenantId,
      objectType: req.query.objectType || req.query.type,
      state: req.query.state,
      status: req.query.status,
      scope: req.query.scope,
      missionId: req.query.missionId,
      channel: req.query.channel,
      tags: parseTags(req.query.tags),
      q: req.query.q,
      limit: req.query.limit,
    }, { pool });
    noStore(res);
    return res.json({ spec: acquisitionKnowledge.SPEC, tenantId, knowledge });
  } catch (err) {
    console.error('[acquisition-knowledge] retrieve', err);
    return fail(res, err, 'ak_retrieve_failed');
  }
});

router.post('/api/v1/acquisition-knowledge', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (!tenantId) return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    const actor = actorFrom(req);
    const knowledge = await acquisitionKnowledge.createKnowledge({
      ...(req.body || {}),
      tenantId,
      clientId: Number(tenantId),
    }, { pool, actor });
    noStore(res);
    return res.status(201).json({ spec: acquisitionKnowledge.SPEC, knowledge });
  } catch (err) {
    console.error('[acquisition-knowledge] create', err);
    return fail(res, err, 'ak_create_failed');
  }
});

router.post('/api/v1/acquisition-knowledge/:id/promote', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (!tenantId) return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    const actor = actorFrom(req);
    const knowledge = await acquisitionKnowledge.promoteKnowledge(req.params.id, {
      ...(req.body || {}),
      tenantId,
      approvedBy: (req.body && req.body.approvedBy) || actor.id,
    }, { pool, actor, tenantId });
    noStore(res);
    return res.json({ spec: acquisitionKnowledge.SPEC, knowledge });
  } catch (err) {
    console.error('[acquisition-knowledge] promote', err);
    return fail(res, err, 'ak_promote_failed');
  }
});

router.post('/api/v1/acquisition-knowledge/import', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (!tenantId) return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    const actor = actorFrom(req);
    const result = await acquisitionKnowledge.importKnowledge({
      ...(req.body || {}),
      tenantId,
    }, { pool, actor, tenantId });
    noStore(res);
    return res.status(result.dryRun ? 200 : 201).json(result);
  } catch (err) {
    console.error('[acquisition-knowledge] import', err);
    return fail(res, err, 'ak_import_failed');
  }
});

router.post('/api/v1/acquisition-knowledge/explain', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (!tenantId) return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    const actor = actorFrom(req);
    const explanation = await acquisitionKnowledge.explainRecommendation({
      ...(req.body || {}),
      tenantId,
    }, { pool, actor, tenantId });
    noStore(res);
    return res.json(explanation);
  } catch (err) {
    console.error('[acquisition-knowledge] explain', err);
    return fail(res, err, 'ak_explain_failed');
  }
});

module.exports = router;
