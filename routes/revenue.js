const express = require('express');
const { randomUUID } = require('crypto');
const pool = require('../db');
const revenue = require('../services/revenueService');
const operations = require('../services/revenueOperations');
const { assertRevenueFlag, loadRevenueFlags } = require('../utils/revenueFlags');

const router = express.Router();

function requireRevenueActor(req, res, next) {
  const clientId = String(req.params.clientId || '');
  if (!/^\d+$/.test(clientId)) return res.status(404).json({ error: 'Not found' });
  const user = req.session?.user;
  const legacyAdmin = req.session?.authenticated;
  const pinOperator = req.session?.clients?.[clientId];
  const privileged = user && ['admin', 'manager'].includes(user.role);
  const scopedUser = user && String(user.client_id || '') === clientId && ['client', 'manager'].includes(user.role);
  if (!legacyAdmin && !pinOperator && !privileged && !scopedUser) return res.status(404).json({ error: 'Not found' });
  req.revenueActor = user
    ? { actorType: 'user', actorId: user.id || user.email }
    : legacyAdmin ? { actorType: 'user', actorId: 'legacy-admin' }
      : { actorType: 'client_operator', actorId: `client:${clientId}` };
  next();
}

function context(req) {
  return {
    ...req.revenueActor,
    idempotencyKey: req.get('Idempotency-Key'),
    correlationId: req.get('X-Correlation-ID') || randomUUID(),
    sourceSystem: req.get('X-Source-System') || 'pulseforge_manual',
    sourceEventId: req.get('X-Source-Event-ID') || null,
    followupRecommendationsEnabled: req.revenueFlags.revenue_followup_recommendations_enabled,
  };
}

function requireFlag(name) {
  return (req, res, next) => {
    try { assertRevenueFlag(req.revenueFlags, name); next(); } catch (error) { next(error); }
  };
}

function mutation(handler) {
  return async (req, res, next) => {
    const startedAt = Date.now();
    try {
      const result = await handler(Number(req.params.clientId), req.body || {}, context(req), req);
      console.info(JSON.stringify({
        event: 'revenue_mutation', client_id: Number(req.params.clientId),
        actor_type: req.revenueActor.actorType, correlation_id: context(req).correlationId,
        success: true, latency_ms: Date.now() - startedAt,
      }));
      res.status(result.idempotentReplay ? 200 : 201).json(result);
    } catch (error) {
      error.revenueTelemetry = { clientId: Number(req.params.clientId), actorType: req.revenueActor.actorType, latency: Date.now() - startedAt };
      await revenue.recordOperationalFailure(Number(req.params.clientId), error.code).catch(() => {});
      next(error);
    }
  };
}

router.use('/:clientId', requireRevenueActor, async (req, res, next) => {
  try {
    req.revenueFlags = await loadRevenueFlags(pool, Number(req.params.clientId));
    assertRevenueFlag(req.revenueFlags, 'revenue_schema_enabled');
    next();
  } catch (error) { next(error); }
});

router.post('/:clientId/customers', requireFlag('revenue_operator_writes_enabled'), mutation((clientId, body, ctx) => revenue.createCustomer(clientId, body, ctx)));
router.post('/:clientId/opportunities', requireFlag('revenue_operator_writes_enabled'), mutation((clientId, body, ctx) => revenue.createOpportunity(clientId, body, ctx)));
router.patch('/:clientId/opportunities/:opportunityId', requireFlag('revenue_operator_writes_enabled'), mutation((clientId, body, ctx, req) => revenue.updateOpportunity(clientId, req.params.opportunityId, body, ctx)));
router.post('/:clientId/jobs', requireFlag('revenue_operator_writes_enabled'), mutation((clientId, body, ctx) => revenue.createJob(clientId, body, ctx)));
router.post('/:clientId/jobs/:jobId/start', requireFlag('revenue_operator_writes_enabled'), mutation((clientId, body, ctx, req) => revenue.startJob(clientId, req.params.jobId, body, ctx)));
router.post('/:clientId/jobs/:jobId/complete', requireFlag('revenue_operator_writes_enabled'), mutation((clientId, body, ctx, req) => revenue.completeJob(clientId, req.params.jobId, body, ctx)));
router.post('/:clientId/payments', requireFlag('revenue_operator_writes_enabled'), mutation((clientId, body, ctx) => revenue.recordPayment(clientId, body, ctx)));
router.post('/:clientId/refunds', requireFlag('revenue_operator_writes_enabled'), mutation((clientId, body, ctx) => revenue.recordRefund(clientId, body, ctx)));

router.get('/:clientId/revenue-outcomes', requireFlag('revenue_operator_reads_enabled'), async (req, res, next) => {
  try { res.json(await revenue.listRevenueOutcomes(Number(req.params.clientId))); } catch (error) { next(error); }
});
router.get('/:clientId/operator/revenue-context', requireFlag('revenue_operator_reads_enabled'), async (req, res, next) => {
  try { res.json(await revenue.getMaxRevenueContext(Number(req.params.clientId))); } catch (error) { next(error); }
});
router.get('/:clientId/max/revenue-context', requireFlag('revenue_max_reads_enabled'), async (req, res, next) => {
  try { res.json(await revenue.getMaxRevenueContext(Number(req.params.clientId))); } catch (error) { next(error); }
});
router.get('/:clientId/revenue-health', requireFlag('revenue_operator_reads_enabled'), async (req, res, next) => {
  try { res.json(await operations.getRevenueHealth(pool, Number(req.params.clientId))); } catch (error) { next(error); }
});
router.get('/:clientId/revenue-audit', requireFlag('revenue_operator_reads_enabled'), async (req, res, next) => {
  try { res.json(await operations.listOperatorAudit(pool, Number(req.params.clientId), req.query.limit)); } catch (error) { next(error); }
});

router.use((error, req, res, next) => {
  if (!error) return next();
  const telemetry = error.revenueTelemetry || {};
  console.error(JSON.stringify({
    event: 'revenue_mutation', client_id: telemetry.clientId,
    actor_type: telemetry.actorType, success: false, failure_reason: error.code || error.message,
    latency_ms: telemetry.latency,
  }));
  const status = error.status || 500;
  res.status(status).json({ error: status === 500 ? 'Revenue operation failed' : error.message, code: error.code || 'INTERNAL_ERROR' });
});

module.exports = router;
