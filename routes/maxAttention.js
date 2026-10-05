'use strict';

const express = require('express');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const { normalizeClientId } = require('../utils/clientContext');
const {
  runAttentionScheduler,
  listOperatorAttention,
  getAttentionHealth,
} = require('../services/maxAttentionService');

const requireAttentionWrite = [
  requireAuth,
  requireRole('admin', 'manager'),
];

function resolveClientId(req) {
  const fromBody = normalizeClientId(req.body?.client_id ?? req.body?.clientId);
  if (fromBody != null) return fromBody;
  const fromQuery = normalizeClientId(req.query?.client_id);
  if (fromQuery != null) return fromQuery;
  return normalizeClientId(req.session?.active_client_id);
}

router.post('/api/v1/max/attention/cycle', requireAttentionWrite, async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    if (clientId == null) {
      return res.status(400).json({ error: 'client_id_required' });
    }
    const result = await runAttentionScheduler(clientId, {
      now: req.body?.now ? new Date(req.body.now) : new Date(),
      limit: req.body?.limit ? Number(req.body.limit) : 20,
    });
    return res.json({ ok: true, ...result });
  } catch (error) {
    console.error('[max-attention-cycle]', error);
    return res.status(500).json({ error: 'attention_cycle_failed', message: error.message });
  }
});

router.get('/api/v1/max/attention/operator-queue', requireAttentionWrite, async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    if (clientId == null) {
      return res.status(400).json({ error: 'client_id_required' });
    }
    const items = await listOperatorAttention(clientId, {
      limit: req.query.limit ? Number(req.query.limit) : 10,
    });
    return res.json({ ok: true, count: items.length, items });
  } catch (error) {
    return res.status(500).json({ error: 'operator_queue_failed', message: error.message });
  }
});

router.get('/api/v1/max/attention/health', requireAttentionWrite, async (req, res) => {
  try {
    const heartbeat = await getAttentionHealth();
    return res.json({ ok: true, heartbeat });
  } catch (error) {
    return res.status(500).json({ error: 'attention_health_failed', message: error.message });
  }
});

module.exports = router;
