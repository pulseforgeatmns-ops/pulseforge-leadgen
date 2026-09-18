'use strict';

/**
 * SPEC-252 — Tenant outreach scheduling APIs (authorization + observability).
 */

const express = require('express');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const { resolveActiveTenantId } = require('../packages/max/workspace/TenantContextResolver');
const pool = require('../db');
const {
  authorizeScheduledOutreachSend,
  cancelScheduledOutreachSend,
  listScheduledOutreachSends,
  getScheduledOutreachSend,
} = require('../services/tenantOutreachScheduler');

const requireOperator = [requireAuth, requireRole('admin', 'manager', 'client')];

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
    role: user.role || 'operator',
  };
}

function fail(res, err, fallbackCode, fallbackStatus = 500) {
  const code = (err && err.code) || fallbackCode;
  const status = code === 'schedule_not_found' ? 404
    : [
      'tenant_required',
      'prospect_required',
      'outreach_asset_required',
      'sending_identity_required',
      'recipient_required',
      'scheduled_for_required',
      'authorized_by_required',
      'authorization_source_required',
      'subject_required',
      'body_required',
      'no_tenant',
      'schedule_already_sent',
    ].includes(code) ? 400
      : fallbackStatus;
  return res.status(status).json({
    error: code,
    message: String((err && err.message) || err),
  });
}

router.get('/api/v1/tenant-outreach/schedules', requireOperator, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const schedules = await listScheduledOutreachSends(tenantId, {
      status: req.query.status || undefined,
      prospectId: req.query.prospect_id || req.query.prospectId || undefined,
      limit: req.query.limit ? Number(req.query.limit) : 100,
    }, { pool });
    noStore(res);
    return res.json({ tenantId, schedules });
  } catch (err) {
    console.error('[tenant-outreach] list schedules', err);
    return fail(res, err, 'tenant_outreach_list_failed');
  }
});

router.get('/api/v1/tenant-outreach/schedules/:id', requireOperator, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const schedule = await getScheduledOutreachSend(tenantId, req.params.id, { pool });
    if (!schedule) {
      return res.status(404).json({ error: 'schedule_not_found', message: 'Scheduled send not found.' });
    }
    noStore(res);
    return res.json({ schedule });
  } catch (err) {
    console.error('[tenant-outreach] get schedule', err);
    return fail(res, err, 'tenant_outreach_get_failed');
  }
});

router.post('/api/v1/tenant-outreach/schedules/authorize', requireOperator, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const actor = actorFrom(req);
    const body = req.body || {};
    const result = await authorizeScheduledOutreachSend({
      tenantId,
      prospectId: body.prospect_id || body.prospectId,
      acquisitionKnowledgeObjectId: body.acquisition_knowledge_object_id || body.acquisitionKnowledgeObjectId,
      outreachAssetId: body.outreach_asset_id || body.outreachAssetId,
      outreachAssetVersion: body.outreach_asset_version || body.outreachAssetVersion,
      sendingIdentityId: body.sending_identity_id || body.sendingIdentityId,
      recipientEmail: body.recipient_email || body.recipientEmail,
      missionId: body.mission_id || body.missionId,
      threadId: body.thread_id || body.threadId,
      sequenceStep: body.sequence_step || body.sequenceStep || 1,
      scheduledFor: body.scheduled_for || body.scheduledFor,
      timezone: body.timezone,
      subject: body.subject,
      body: body.body,
      authorizationSource: body.authorization_source || body.authorizationSource || 'operator_api',
      authorizedBy: body.authorized_by || body.authorizedBy || String(actor.id),
      pastDuePolicy: body.past_due_policy || body.pastDuePolicy,
      maxLatenessMinutes: body.max_lateness_minutes || body.maxLatenessMinutes,
      idempotencyKey: body.idempotency_key || body.idempotencyKey,
    }, { pool });
    noStore(res);
    return res.status(result.created ? 201 : 200).json(result);
  } catch (err) {
    console.error('[tenant-outreach] authorize schedule', err);
    return fail(res, err, 'tenant_outreach_authorize_failed');
  }
});

router.post('/api/v1/tenant-outreach/schedules/:id/cancel', requireOperator, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const result = await cancelScheduledOutreachSend({
      tenantId,
      scheduleId: req.params.id,
    }, { pool });
    noStore(res);
    return res.json(result);
  } catch (err) {
    console.error('[tenant-outreach] cancel schedule', err);
    return fail(res, err, 'tenant_outreach_cancel_failed');
  }
});

const requireAnchorOperator = [requireAuth, requireRole('admin', 'manager')];
function anchorOperation(operation) {
  return async (req, res) => {
    if (actorTenantId(req) !== '10') return res.status(403).json({ error: 'anchor_tenant_required' });
    try {
      const service = require('../services/governedOutbound').productionService(pool);
      const result = await operation(service, req, actorFrom(req));
      noStore(res);
      return res.json(result);
    } catch (e) { return res.status(409).json({ error: e.code || 'governed_outbound_failed' }); }
  };
}
router.get('/api/v1/tenant-outreach/anchor-program', requireAnchorOperator,
  anchorOperation(service => service.status()));
router.post('/api/v1/tenant-outreach/anchor-program/authorize', requireAnchorOperator,
  anchorOperation((service, req, actor) => service.authorize(req.body || {}, actor)));
router.post('/api/v1/tenant-outreach/anchor-program/:id/mode', requireAnchorOperator,
  anchorOperation((service, req, actor) => service.setMode(req.params.id, req.body?.mode, req.body?.policyHash, actor)));
module.exports = router;
