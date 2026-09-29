'use strict';
const express = require('express');
const path = require('path');
const { requireAuth, requireRole } = require('../middleware/auth');
const { getRequestClientId } = require('../utils/clientContext');
const { assertAuthorizedClientSwitch } = require('../utils/tenantAuthorization');

function createPaigeSocialRouter(deps = {}) {
  const router = express.Router();
  const store = deps.store || require('../packages/capabilities/contentGeneration').createPostgresSocialContentStore(require('../db'));
  const approval = deps.approval || require('../services/paigeSocialContentApproval').getApprovalService();
  const publish = deps.publish || require('../services/paigeSocialContentPublication').routePaigeSocialContentPublication;
  const accounts = deps.accounts || require('../packages/capabilities/contentPublication/socialAccounts').listSocialAccounts;
  const delegate = deps.delegate || require('../services/specialistDelegation');
  const authorize = deps.authorize || [requireAuth, requireRole('admin', 'manager')];
  router.use(['/paige-social', '/api/paige/social'], ...authorize, (req, res, next) => {
    const raw = req.query.client_id ?? req.body?.client_id ?? getRequestClientId(req);
    const clientId = Number(raw);
    const access = assertAuthorizedClientSwitch(req.user, clientId);
    if (!Number.isInteger(clientId) || clientId < 1) return res.status(400).json({ error: 'client_id_required' });
    if (!access.ok) return res.status(access.status).json({ error: access.error });
    req.paigeScope = { clientId, tenantId: String(clientId) };
    res.set('Cache-Control', 'no-store');
    next();
  });
  function handler(fn) {
    return async (req, res) => {
      try { await store.ensureSchema(); await fn(req, res); }
      catch (err) { res.status(err.message === 'artifact_not_found' ? 404 : 409).json({ error: err.message }); }
    };
  }
  router.get('/paige-social', (_req, res) => res.sendFile(path.join(__dirname, '../public/paige-social.html')));
  router.get('/api/paige/social', handler(async (req, res) => {
    res.json({ ...req.paigeScope, accounts: accounts(req.paigeScope.clientId), artifacts: await store.listByTenant(req.paigeScope.tenantId, req.paigeScope.clientId) });
  }));
  router.post('/api/paige/social/request', handler(async (req, res) => {
    const { channel, objective, campaignId } = req.body || {};
    if (!channel || !String(objective || '').trim()) return res.status(400).json({ error: 'channel_and_objective_required' });
    const delegation = await delegate.createDelegation({ authorizedTenantId: req.paigeScope.tenantId, tenantId: req.paigeScope.tenantId,
      specialist: 'paige', capability: 'social_content', authority: 'draft', objective: String(objective), reason: 'Operator requested social content through Max.',
      constraints: { allowedChannels: [channel] }, targetContext: { entities: campaignId ? [{ kind: 'campaign', id: String(campaignId) }] : [] },
      requestedBy: `operator:${req.user.id || req.user.email}` });
    const result = await delegate.executeDelegation({ authorizedTenantId: req.paigeScope.tenantId, tenantId: req.paigeScope.tenantId, delegationId: delegation.id });
    res.json({ delegation, result });
  }));
  router.get('/api/paige/social/:id/preview', handler(async (req, res) => {
    res.json(await approval.preview({ ...req.paigeScope, artifactId: req.params.id, accountId: req.query.account_id }));
  }));
  router.post('/api/paige/social/:id/decision', handler(async (req, res) => {
    res.json(await approval.recordDecision({ ...req.paigeScope, artifactId: req.params.id, decision: req.body.decision,
      accountId: req.body.accountId, expectedApprovalHash: req.body.expectedApprovalHash,
      approvedBy: `operator:${req.user.id || req.user.email}`, rejectionReason: req.body.rejectionReason }));
  }));
  router.post('/api/paige/social/:id/publish', handler(async (req, res) => {
    const result = await publish({ ...req.paigeScope, artifactId: req.params.id, dryRun: req.body.dryRun === true, invocationSource: 'operator_social_review' });
    res.status(result.success ? 200 : 409).json(result);
  }));
  router.post('/api/paige/social/:id/reconcile', handler(async (req, res) => {
    const result = await publish({ ...req.paigeScope, artifactId: req.params.id, reconcilePostId: req.body.providerPostId,
      reconciliationReason: req.body.reason, reconciledBy: `operator:${req.user.id || req.user.email}`, invocationSource: 'operator_social_reconciliation' });
    res.status(result.success ? 200 : 409).json(result);
  }));
  return router;
}
module.exports = { createPaigeSocialRouter };
