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
const {
  loadGovernedMissionExecutionStatus,
} = require('../services/governedMissionExecutionStatus');

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

router.get('/api/v1/tenant-outreach/governed-missions/:id', requireOperator, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const governedExecution = await loadGovernedMissionExecutionStatus(pool, {
      tenantId,
      missionId: req.params.id,
    });
    noStore(res);
    return res.json({ tenantId, missionId: req.params.id, governedExecution });
  } catch (err) {
    console.error('[tenant-outreach] governed mission status', err);
    return fail(res, err, 'governed_mission_status_failed');
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
// Preparation only: no send, approval, freeze, activation, or recovery mutation.
router.post('/api/v1/tenant-outreach/anchor-program/prepare-drafts',
  (req, res, next) => {
    noStore(res);
    // Validate before generic auth can normalize or save a stale snapshot.
    // Retain ordinary unauthenticated status, and detach on every early denial.
    try { require('../services/anchorCrmPreparationAction').actorFromRequest(req); }
    catch {
      const status = req.session?.user || req.session?.authenticated === true ? 403 : 401;
      req.session = null;
      return res.status(status).json({ error: 'anchor_crm_action_forbidden', retryAllowed: false });
    }
    next();
  },
  requireAuth, requireRole('admin'), async (req, res) => {
    noStore(res);
    const action = require('../services/anchorCrmPreparationAction');
    let actor;
    try {
      actor = action.actorFromRequest(req);
      // Do not let Express save a stale session snapshot over revocation.
      req.session = null;
      const expected = new URL(process.env.APP_URL);
      if (!['http:', 'https:'].includes(expected.protocol) || req.get('origin') !== expected.origin
        || req.get('sec-fetch-site') === 'cross-site' || !req.is('application/json')) throw new Error('forbidden');
    } catch { req.session = null; return res.status(403).json({ error: 'anchor_crm_action_forbidden', retryAllowed: false }); }
    try { return res.json(await action.prepareDrafts(pool, req.body, actor)); }
    catch (error) { return res.status(409).json({ error: 'anchor_crm_action_rejected',
      status: error.status || 'rejected', writesMayHaveOccurred: error.writesMayHaveOccurred === true, retryAllowed: false }); }
  });
// Explicit same-origin approval controls. These never invoke preparation.
for (const [operation, method] of Object.entries({ preview: 'previewPreparationApproval', install: 'installPreparationApproval', revoke: 'revokePreparationApproval' })) {
  router.post(`/api/v1/tenant-outreach/anchor-program/preparation-approval/${operation}`,
    (req, res, next) => {
      noStore(res);
      try { require('../services/anchorCrmPreparationAction').actorFromRequest(req); }
      catch {
        const status = req.session?.user || req.session?.authenticated === true ? 403 : 401;
        req.session = null;
        return res.status(status).json({ error: 'anchor_crm_approval_forbidden', retryAllowed: false });
      }
      next();
    }, requireAuth, requireRole('admin'), async (req, res) => {
      noStore(res);
      let actor;
      try {
        actor = require('../services/anchorCrmPreparationAction').actorFromRequest(req);
        req.session = null;
        const expected = new URL(process.env.APP_URL);
        if (!['http:', 'https:'].includes(expected.protocol) || req.get('origin') !== expected.origin
          || req.get('sec-fetch-site') === 'cross-site' || !req.is('application/json')) throw new Error('forbidden');
      } catch { req.session = null; return res.status(403).json({ error: 'anchor_crm_approval_forbidden', retryAllowed: false }); }
      try { return res.json(await require('../services/anchorCrmPreparationApproval')[method](pool, req.body, actor)); }
      catch { return res.status(409).json({ error: 'anchor_crm_approval_rejected', status: operation === 'preview' ? 'rejected' : 'outcome_requires_readback',
        writesMayHaveOccurred: operation !== 'preview', retryAllowed: false }); }
    });
}
// SPEC-SCOUT-P0-003 research-only grant. Same admin-session, same-origin gate as
// preparation approval; these never run discovery, preparation or sends.
// The research mission is approved separately and first; it is discovery scope
// only and is never written to acquisition_missions or the outbound program.
for (const [resource, operation, service, method] of [
  ['research-mission', 'preview', 'scoutResearchMission', 'previewResearchMission'],
  ['research-mission', 'install', 'scoutResearchMission', 'installResearchMission'],
  ['research-only-grant', 'preview', 'scoutResearchOnlyGrant', 'previewResearchOnlyGrant'],
  ['research-only-grant', 'install', 'scoutResearchOnlyGrant', 'installResearchOnlyGrant'],
  ['research-only-grant', 'revoke', 'scoutResearchOnlyGrant', 'revokeResearchOnlyGrant'],
  ['research-only-grant', 'inspect', 'scoutResearchOnlyGrant', 'inspectResearchOnlyGrant'],
  ['research-only-grant', 'export', 'scoutResearchRetrospective', 'exportForAdmin'],
  ['research-cycle-release', 'preview', 'scoutResearchOnlyGrant', 'previewResearchCycleRelease'],
  ['research-cycle-release', 'release', 'scoutResearchOnlyGrant', 'releaseResearchCycle']]) {
  router.post(`/api/v1/tenant-outreach/anchor-program/${resource}/${operation}`,
    (req, res, next) => {
      noStore(res);
      try { require('../services/anchorCrmPreparationAction').actorFromRequest(req); }
      catch {
        const status = req.session?.user || req.session?.authenticated === true ? 403 : 401;
        return res.status(status).json({ error: 'research_only_grant_forbidden', retryAllowed: false });
      }
      next();
    }, requireAuth, requireRole('admin'), async (req, res) => {
      noStore(res);
      let actor;
      try {
        actor = require('../services/anchorCrmPreparationAction').actorFromRequest(req);
        const expected = new URL(process.env.APP_URL);
        if (!['http:', 'https:'].includes(expected.protocol) || req.get('origin') !== expected.origin
          || req.get('sec-fetch-site') === 'cross-site' || !req.is('application/json')) throw new Error('forbidden');
      } catch { return res.status(403).json({ error: 'research_only_grant_forbidden', retryAllowed: false }); }
      // Installing commits Google spend authority: require a fresh, single-use
      // password confirmation on this exact session, however long-lived the
      // login is. Nothing is written when it is missing. Revoke never needs it.
      if (operation === 'install') {
        const confirmed = await require('../utils/freshConfirmation').consume(pool, { sessionId: req.sessionID,
          userId: actor.id, purpose: 'scout_research_approval' }).catch(() => false);
        if (!confirmed) return res.status(403).json({ error: 'fresh_confirmation_required', code: 'fresh_confirmation_required',
          status: 'rejected', writesMayHaveOccurred: false, retryAllowed: true });
        // The marker was consumed in SQL; never re-save this request's stale snapshot.
        req.session = null;
      }
      const handler = require(`../services/${service}`);
      // Release consumes its distinct password purpose inside the same durable
      // transaction as the allowance audit. Do not save a stale session copy.
      if (operation === 'release') req.session = null;
      try {
        if (operation === 'inspect') return res.json(await handler.inspectResearchOnlyGrant(pool, req.body?.grantId));
        if (operation === 'export') {
          const cohort = await handler.exportForAdmin(pool, req.body);
          res.set('X-Content-Type-Options', 'nosniff');
          res.attachment(`scout-research-cohort-${cohort.grantId.slice(0, 12)}.json`);
          return res.json(cohort);
        }
        return res.json(await handler[method](pool, req.body, actor));
      } catch (error) {
        if (error.code === 'fresh_confirmation_required') return res.status(403).json({ error: error.code, code: error.code,
          status: 'rejected', writesMayHaveOccurred: false, retryAllowed: true });
        return res.status(409).json({ error: 'research_only_grant_rejected', code: /^research_(only|mission)_[a-z_]+$/.test(error.code || '') ? error.code : null,
          status: ['preview', 'inspect', 'export'].includes(operation) ? 'rejected' : 'outcome_requires_readback',
          writesMayHaveOccurred: !['preview', 'inspect', 'export'].includes(operation), retryAllowed: false });
      }
    });
}
module.exports = router;
