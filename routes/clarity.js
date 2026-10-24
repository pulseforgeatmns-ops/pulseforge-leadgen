'use strict';
const path = require('node:path');
const crypto = require('node:crypto');
const express = require('express');
const { requireAuth, requireRole } = require('../middleware/auth');
const { getAuthenticatedActor, getEffectiveActor, isImpersonating } = require('../utils/requestIdentity');
const { tenant, failure, enabled, id } = require('../packages/max/clarity/common');
const { ClarityStore } = require('../packages/max/clarity/store');
const { createExecutionRequest, EXECUTION_INTENTS } = require('../packages/acquisition-mission/ExecutionRequest');
const { authorizeClarity } = require('../services/clarityAuthorization');
const { routeExecutionRequest } = require('../packages/acquisition-mission/ExecutionRouter');
function scope(req, write = false) {
  const actor = getEffectiveActor(req), auth = getAuthenticatedActor(req);
  if (!actor || actor.active === false) throw failure('clarity_unauthorized',401);
  const selected = req.query?.client_id ?? req.body?.client_id ?? req.session?.active_client_id;
  const scopedRole = actor.role === 'client' || actor.role === 'manager';
  const cid = tenant(scopedRole ? actor.client_id : selected);
  if (selected != null && tenant(selected) !== cid) throw failure('clarity_tenant_mismatch',403);
  if (cid !== 10) throw failure('clarity_not_found',404);
  const canDecide = !isImpersonating(req) && ['admin','manager'].includes(auth?.role) && !!auth?.id
    && (auth.role !== 'manager' || Number(auth.client_id) === cid);
  if (write && !canDecide) throw failure('clarity_approval_forbidden',403);
  return { clientId: cid, actorId: auth?.id, role: auth?.role, canDecide };
}
function csrf(req) {
  if (!req.session.clarity_csrf) req.session.clarity_csrf = crypto.randomBytes(32).toString('hex');
  return req.session.clarity_csrf;
}
function expectedOrigin(req, env = process.env) {
  if (env.APP_URL) {
    try { const url = new URL(env.APP_URL); return ['http:','https:'].includes(url.protocol) && !url.username && !url.password ? url.origin : null; }
    catch (_) { return null; }
  }
  const host = req.get('host');
  return /^(127\.0\.0\.1|localhost|\[::1\])(?::[0-9]+)?$/.test(host || '') ? `${req.protocol}://${host}` : null;
}
function checkCsrf(req, env = process.env) {
  const expected = Buffer.from(req.session?.clarity_csrf || ''), actual = Buffer.from(req.get('X-CSRF-Token') || '');
  if (!expected.length || expected.length !== actual.length || !crypto.timingSafeEqual(expected,actual)) throw failure('clarity_csrf_required',403);
  const origin = req.get('origin');
  if (origin && origin !== expectedOrigin(req,env)) throw failure('clarity_origin_forbidden',403);
}
function publicError(res,error) {
  const code = /^clarity_[a-z_]+$/.test(error.code || '') ? error.code : 'clarity_unavailable';
  return res.status(error.statusCode || 503).json({ error: code });
}
function createClarityRouter({ pool = require('../db'), env = process.env, store = new ClarityStore(pool), route = routeExecutionRequest } = {}) {
  const router = express.Router(), read = [requireAuth,requireRole('admin','manager','viewer','client')];
  async function assertAvailable(clientId) {
    if (!enabled(env)) throw failure('clarity_disabled',404);
    const integration = await store.getIntegration({ clientId });
    if (!integration?.enabled || integration.consent_status !== 'authorized') throw failure('clarity_disabled',404);
  }
  router.get('/clarity-review',read,async(req,res) => {
    try { await authorizeClarity(req,{pool}); return res.set('Cache-Control','no-store').sendFile(path.join(__dirname,'../public/clarity-review.html')); }
    catch (error) { return publicError(res,error); }
  });
  router.get('/api/v1/clarity/recommendations/:id',read,async(req,res) => {
    try {
      const current = await authorizeClarity(req,{pool}); await assertAvailable(current.clientId);
      const recommendation = await store.getRecommendation({ clientId: current.clientId, recommendationId: id(req.params.id) });
      if (!recommendation) throw failure('clarity_not_found',404);
      const report = await store.getSnapshot({ clientId: current.clientId, snapshotId: recommendation.snapshot_id });
      return res.set('Cache-Control','no-store').json({ recommendation,report,
        permissions: { can_decide: current.canDecide }, csrf_token: csrf(req) });
    } catch(error) { return publicError(res,error); }
  });
  router.post('/api/v1/clarity/recommendations/:id/decision',read,async(req,res) => {
    try {
      const current = await authorizeClarity(req,{pool,write:true}); checkCsrf(req,env); await assertAvailable(current.clientId);
      if (!req.is('application/json')) throw failure('clarity_json_required',415);
      const allowed = ['client_id','decision','expected_digest','idempotency_key'];
      if (Object.keys(req.body || {}).some(k => !allowed.includes(k))) throw failure('clarity_unexpected_payload');
      const request = createExecutionRequest({ source: 'approval_button', intent: EXECUTION_INTENTS.REVIEW_CLARITY_RECOMMENDATION,
        runtimeOwner: 'clarity_review', executionMode: 'manual', operatorId: current.actorId,
        permissions: { canExecute: true, role: current.role }, payload: { client_id: current.clientId,
          recommendation_id: id(req.params.id), decision: req.body.decision,
          expected_digest: req.body.expected_digest,idempotency_key: req.body.idempotency_key } });
      const result = await route(request,{ tenantId: current.clientId, pool, clarityStore: store, env });
      return res.set('Cache-Control','no-store').json({ ...result.executionResult,routed: result.routed });
    } catch(error) { return publicError(res,error); }
  });
  return router;
}
module.exports = createClarityRouter();
module.exports.createClarityRouter = createClarityRouter;
module.exports.scope = scope;
module.exports.checkCsrf = checkCsrf;
