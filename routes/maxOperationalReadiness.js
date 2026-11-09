'use strict';

const express = require('express');
const pool = require('../db');
const { requireAuth, requireRole } = require('../middleware/auth');
const { getMaxOperationalReadiness } = require('../services/maxOperationalReadiness');

function createMaxOperationalReadinessRouter({ db = pool, readiness = getMaxOperationalReadiness } = {}) {
  const router = express.Router();
  router.get('/api/v1/max/operational-readiness', requireAuth, requireRole('admin', 'manager'), async (_req, res) => {
    res.set('Cache-Control', 'no-store');
    try { return res.json(await readiness({ db })); }
    catch (_) { return res.status(503).json({ error: 'max_operational_readiness_unavailable', readOnly: true }); }
  });
  return router;
}

module.exports = createMaxOperationalReadinessRouter();
module.exports.createMaxOperationalReadinessRouter = createMaxOperationalReadinessRouter;
