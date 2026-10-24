'use strict';

/**
 * SPEC-074 — Prospect Operating Brief API (GET-only, admin/manager).
 *
 * GET /api/v1/prospects/operating-brief
 *   ?companyId=&prospectId=&opportunityId=&contactId=
 *   &relationshipInteractionId=&days=
 *
 * Read-only synthesis. No writes, no autonomous execution.
 */

const express = require('express');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const {
  ProspectOperatingBriefError,
  getProspectOperatingBrief,
} = require('../services/prospectOperatingBrief');

const requireAdmin = [requireAuth, requireRole('admin', 'manager')];

function noStore(res) {
  res.set('Cache-Control', 'no-store');
}

router.get('/api/v1/prospects/operating-brief', requireAdmin, async (req, res) => {
  try {
    const brief = await getProspectOperatingBrief({
      companyId: req.query.companyId || undefined,
      prospectId: req.query.prospectId || undefined,
      opportunityId: req.query.opportunityId || undefined,
      contactId: req.query.contactId || undefined,
      relationshipInteractionId:
        req.query.relationshipInteractionId ||
        req.query.interactionId ||
        undefined,
      days: req.query.days,
      includeMarketContext: req.query.includeMarketContext,
      includeRelationshipContext: req.query.includeRelationshipContext,
      clientId: req.query.clientId,
    });
    noStore(res);
    return res.json(brief);
  } catch (err) {
    if (err instanceof ProspectOperatingBriefError) {
      noStore(res);
      return res.status(err.status || 400).json({
        error: err.code,
        message: err.message,
        ok: false,
        kind: 'prospect_operating_brief',
        isEvidence: false,
      });
    }
    console.error('[prospect-operating-brief]', err);
    return res.status(500).json({
      error: 'prospect_operating_brief_failed',
      message: err && err.message ? String(err.message) : 'failed',
    });
  }
});

module.exports = router;
