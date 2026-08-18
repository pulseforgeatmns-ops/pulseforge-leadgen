'use strict';

/**
 * SPEC-112 — Acquisition Intelligence Model APIs.
 *
 * GET  /api/v1/aim
 * GET  /api/v1/aim/:clientKey
 * POST /api/v1/aim/:clientKey/qualify
 * GET  /api/v1/aim/:clientKey/pilot
 */

const express = require('express');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const {
  listAims,
  getAim,
  qualify,
  pilotStatus,
} = require('../services/acquisitionIntelligenceModel');

const requireAdmin = [requireAuth, requireRole('admin', 'manager')];

function noStore(res) {
  res.set('Cache-Control', 'no-store');
}

router.get('/api/v1/aim', requireAdmin, (req, res) => {
  try {
    noStore(res);
    return res.json({
      kind: 'acquisition_intelligence_model_list',
      spec: 'SPEC-112',
      isOperatingFact: false,
      models: listAims().map((m) => ({
        id: m.id,
        clientKey: m.clientKey,
        clientName: m.clientName,
        status: m.status,
        mission: m.mission && m.mission.transformation,
      })),
    });
  } catch (err) {
    console.error('[aim] list', err);
    return res.status(500).json({ error: 'aim_list_failed', message: String(err.message || err) });
  }
});

router.get('/api/v1/aim/:clientKey', requireAdmin, (req, res) => {
  try {
    const model = getAim(req.params.clientKey);
    if (!model) return res.status(404).json({ error: 'aim_not_found' });
    noStore(res);
    return res.json({
      kind: 'acquisition_intelligence_model',
      spec: 'SPEC-112',
      isOperatingFact: false,
      model,
    });
  } catch (err) {
    console.error('[aim] get', err);
    return res.status(500).json({ error: 'aim_get_failed', message: String(err.message || err) });
  }
});

router.post('/api/v1/aim/:clientKey/qualify', requireAdmin, (req, res) => {
  try {
    const result = qualify(req.params.clientKey, req.body && req.body.prospect ? req.body.prospect : req.body);
    noStore(res);
    return res.json({
      kind: 'aim_qualification',
      spec: 'SPEC-112',
      isOperatingFact: false,
      qualification: result.qualification,
      briefing: result.briefing,
    });
  } catch (err) {
    if (err && err.code === 'aim_not_found') {
      return res.status(404).json({ error: 'aim_not_found' });
    }
    console.error('[aim] qualify', err);
    return res.status(500).json({ error: 'aim_qualify_failed', message: String(err.message || err) });
  }
});

router.get('/api/v1/aim/:clientKey/pilot', requireAdmin, (req, res) => {
  try {
    const status = pilotStatus(req.params.clientKey);
    noStore(res);
    return res.json(status);
  } catch (err) {
    if (err && err.code === 'aim_not_found') {
      return res.status(404).json({ error: 'aim_not_found' });
    }
    console.error('[aim] pilot', err);
    return res.status(500).json({ error: 'aim_pilot_failed', message: String(err.message || err) });
  }
});

module.exports = router;
