'use strict';

/**
 * SPEC-014 — Admin Validation Dashboard + Flight Recorder.
 * Admin/manager only. Not customer-facing.
 *
 * GET  /admin/knowledge-health          → Validation Dashboard HTML
 * GET  /admin/flight-recorder           → Flight Recorder HTML
 * GET  /api/v1/admin/knowledge/health   → health JSON
 * GET  /api/v1/admin/knowledge/flights  → recent flights
 * GET  /api/v1/admin/knowledge/flights/:flightId → journey
 * POST /api/v1/admin/knowledge/outbox/drain → process outbox
 * POST /cron/knowledge-outbox           → CRON_SECRET-gated drain
 */

const path = require('path');
const express = require('express');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const {
  getRequestClientId,
  normalizeClientId,
} = require('../utils/clientContext');
const {
  getDualWriteHealth,
  safeProcessOutbox,
  dualWriteEnabled,
} = require('../utils/knowledgeDualWrite');
const { getKnowledgeBoot } = require('../utils/knowledgeRuntime');
const {
  getFlightJourney,
  listRecentFlights,
} = require('../packages/knowledge/dualWrite');

const requireAdmin = [requireAuth, requireRole('admin', 'manager')];

router.get('/admin/knowledge-health', requireAdmin, (req, res) => {
  res.sendFile(path.join(__dirname, '..', 'public', 'knowledge-health.html'));
});

router.get('/admin/flight-recorder', requireAdmin, (req, res) => {
  res.sendFile(path.join(__dirname, '..', 'public', 'flight-recorder.html'));
});

router.use(
  '/admin/flight-recorder',
  express.static(path.join(__dirname, '..', 'public', 'flight-recorder'), {
    index: false,
    fallthrough: true,
  })
);

router.get('/api/v1/admin/knowledge/health', requireAdmin, async (req, res) => {
  try {
    const tenantId = resolveTenantId(req);
    const health = await getDualWriteHealth({
      tenantId: tenantId != null ? String(tenantId) : undefined,
    });
    res.set('Cache-Control', 'no-store');
    return res.json({
      ...health,
      dualWriteFlag: dualWriteEnabled(),
      internal: true,
      customerFacing: false,
    });
  } catch (err) {
    console.error('[knowledge-health]', err);
    return res.status(500).json({
      error: 'knowledge_health_failed',
      message: err && err.message ? String(err.message) : 'failed',
    });
  }
});

router.get('/api/v1/admin/knowledge/flights', requireAdmin, async (req, res) => {
  try {
    const boot = await getKnowledgeBoot();
    if (!boot.pool) {
      return res.json({ flights: [], enabled: false });
    }
    const tenantId = resolveTenantId(req);
    const flights = await listRecentFlights(boot.pool, {
      tenantId: tenantId != null ? String(tenantId) : undefined,
      limit: Math.min(Number(req.query.limit) || 25, 100),
    });
    res.set('Cache-Control', 'no-store');
    return res.json({ flights, enabled: true });
  } catch (err) {
    console.error('[knowledge-flights]', err);
    return res.status(500).json({
      error: 'flights_list_failed',
      message: err && err.message ? String(err.message) : 'failed',
    });
  }
});

router.get(
  '/api/v1/admin/knowledge/flights/:flightId',
  requireAdmin,
  async (req, res) => {
    try {
      const boot = await getKnowledgeBoot();
      if (!boot.pool) {
        return res.status(503).json({ error: 'dual_write_disabled' });
      }
      const tenantId = resolveTenantId(req);
      const journey = await getFlightJourney(boot.pool, {
        flightId: req.params.flightId,
        tenantId: tenantId != null ? String(tenantId) : undefined,
      });
      res.set('Cache-Control', 'no-store');
      return res.json(journey);
    } catch (err) {
      console.error('[knowledge-flight]', err);
      return res.status(500).json({
        error: 'flight_journey_failed',
        message: err && err.message ? String(err.message) : 'failed',
      });
    }
  }
);

router.post(
  '/api/v1/admin/knowledge/outbox/drain',
  requireAdmin,
  async (req, res) => {
    try {
      const result = await safeProcessOutbox({
        limit: Math.min(Number(req.body?.limit) || 50, 200),
      });
      res.set('Cache-Control', 'no-store');
      return res.json(result);
    } catch (err) {
      console.error('[knowledge-outbox-drain]', err);
      return res.status(500).json({
        error: 'outbox_drain_failed',
        message: err && err.message ? String(err.message) : 'failed',
      });
    }
  }
);

async function handleKnowledgeOutboxCron(req, res) {
  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (process.env.CRON_SECRET && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const result = await safeProcessOutbox({
    limit: Math.min(Number(req.query.limit) || 100, 500),
  });
  return res.json({ success: true, ...result });
}

router.post('/cron/knowledge-outbox', handleKnowledgeOutboxCron);
router.get('/cron/knowledge-outbox', handleKnowledgeOutboxCron);

function resolveTenantId(req) {
  if (req.query.client_id != null || req.query.clientId != null) {
    return normalizeClientId(req.query.client_id || req.query.clientId);
  }
  return getRequestClientId(req);
}

module.exports = router;
