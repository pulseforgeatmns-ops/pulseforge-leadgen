'use strict';

const express = require('express');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const pool = require('../db');
const { resolveAttentionScope } = require('../utils/maxAttentionAuthorization');
const {
  runAttentionScheduler,
  listOperatorAttention,
  getAttentionHealth,
} = require('../services/maxAttentionService');

const requireAttentionWrite = [
  requireAuth,
  requireRole('admin', 'manager'),
];

router.post('/api/v1/max/attention/cycle', requireAttentionWrite, async (req, res) => {
  try {
    const clientId = await resolveAttentionScope(req, pool);
    if (clientId == null) {
      return res.status(400).json({ error: 'client_id_required' });
    }
    const result = await runAttentionScheduler(clientId, {
      now: req.body?.now ? new Date(req.body.now) : new Date(),
      limit: req.body?.limit ? Number(req.body.limit) : 20,
    });
    return res.json({ ok: true, ...result });
  } catch (error) {
    if (error.statusCode) return res.status(error.statusCode).json({ error: error.code });
    console.error('[max-attention-cycle]', error);
    return res.status(500).json({ error: 'attention_cycle_failed', message: error.message });
  }
});

router.get('/api/v1/max/attention/operator-queue', requireAttentionWrite, async (req, res) => {
  try {
    const clientId = await resolveAttentionScope(req, pool);
    if (clientId == null) {
      return res.status(400).json({ error: 'client_id_required' });
    }
    const items = await listOperatorAttention(clientId, {
      limit: req.query.limit ? Number(req.query.limit) : 10,
    });
    return res.json({ ok: true, count: items.length, items });
  } catch (error) {
    if (error.statusCode) return res.status(error.statusCode).json({ error: error.code });
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
