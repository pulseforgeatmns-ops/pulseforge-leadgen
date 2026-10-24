'use strict';

/**
 * SPEC-075 — Service Mode Operator Loop API (GET-only, admin/manager).
 *
 * GET /api/v1/operator/service-loop
 *   ?days=&limit=&relationshipInteractionId=&companyId=&prospectId=&opportunityId=
 *
 * Read-only manual action queue. No writes, no autonomous execution.
 */

const express = require('express');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const {
  ServiceModeOperatorLoopError,
  getServiceModeOperatorLoop,
} = require('../services/serviceModeOperatorLoop');

const requireAdmin = [requireAuth, requireRole('admin', 'manager')];

function noStore(res) {
  res.set('Cache-Control', 'no-store');
}

router.get('/api/v1/operator/service-loop', requireAdmin, async (req, res) => {
  try {
    const loop = await getServiceModeOperatorLoop({
      days: req.query.days,
      limit: req.query.limit,
      relationshipInteractionId:
        req.query.relationshipInteractionId ||
        req.query.interactionId ||
        undefined,
      companyId: req.query.companyId || undefined,
      prospectId: req.query.prospectId || undefined,
      opportunityId: req.query.opportunityId || undefined,
      includeMarketContext: req.query.includeMarketContext,
      clientId: req.query.clientId,
    });
    noStore(res);
    return res.json(loop);
  } catch (err) {
    if (err instanceof ServiceModeOperatorLoopError) {
      noStore(res);
      return res.status(err.status || 400).json({
        error: err.code,
        message: err.message,
        ok: false,
        kind: 'service_mode_operator_loop',
        isEvidence: false,
      });
    }
    console.error('[service-mode-operator-loop]', err);
    return res.status(500).json({
      error: 'service_mode_operator_loop_failed',
      message: err && err.message ? String(err.message) : 'failed',
    });
  }
});

module.exports = router;
