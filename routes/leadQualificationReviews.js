/**
 * SPEC-258 — Operator qualification review decisions for website-origin leads.
 */

const express = require('express');
const pool = require('../db');
const { requireAuth, requireRole } = require('../middleware/auth');
const { getRequestClientId } = require('../utils/clientContext');
const {
  applyQualificationDecision,
  DECISION_ACTIONS,
  ERROR,
} = require('../lib/leadQualificationReview');

const router = express.Router();
const requireOperator = [requireAuth, requireRole('admin', 'manager')];

function operatorFromSession(req) {
  if (!req.user?.id) return null;
  return {
    id: req.user.id,
    name: req.user.name,
    role: req.user.role,
    email: req.user.email,
  };
}

function mapErrorToStatus(error) {
  switch (error) {
    case ERROR.UNSUPPORTED_ACTION:
    case ERROR.OPERATOR_REQUIRED:
      return 400;
    case ERROR.REVIEW_NOT_FOUND:
    case ERROR.PROSPECT_NOT_FOUND:
    case ERROR.ORIGINATING_ACTION_NOT_FOUND:
      return 404;
    case ERROR.PROSPECT_CLIENT_MISMATCH:
    case ERROR.ORIGINATING_ACTION_CLIENT_MISMATCH:
    case ERROR.REVIEW_ALREADY_TERMINAL:
    case ERROR.CONFLICTING_DECISION:
      return 409;
    default:
      return 500;
  }
}

/**
 * POST /api/v1/lead-qualification-reviews/:id/decision
 * Body: { action: 'QUALIFY' | 'NURTURE' | 'DISQUALIFY', note?: string }
 */
router.post('/api/v1/lead-qualification-reviews/:id/decision', requireOperator, async (req, res) => {
  try {
    const operator = operatorFromSession(req);
    if (!operator) {
      return res.status(401).json({ ok: false, error: ERROR.OPERATOR_REQUIRED });
    }

    const action = String(req.body?.action || '').trim().toUpperCase();
    const note = req.body?.note != null ? String(req.body.note).trim().slice(0, 2000) : null;

    if (req.body?.operator_id || req.body?.operatorId || req.body?.client_id || req.body?.clientId) {
      return res.status(400).json({
        ok: false,
        error: 'CLIENT_AND_OPERATOR_CONTEXT_MUST_COME_FROM_SESSION',
      });
    }

    if (!DECISION_ACTIONS.includes(action)) {
      return res.status(400).json({ ok: false, error: ERROR.UNSUPPORTED_ACTION });
    }

    const clientId = getRequestClientId(req);
    const result = await applyQualificationDecision(pool, {
      reviewId: req.params.id,
      action,
      operator,
      note,
      clientId,
    });

    if (!result.ok) {
      const status = mapErrorToStatus(result.error);
      return res.status(status).json({ ok: false, error: result.error });
    }

    return res.json({
      ok: true,
      idempotent: result.idempotent === true,
      review_id: result.reviewId,
      qualification_status: result.qualificationStatus,
      opportunityCreationReady: result.opportunityCreationReady,
      payload: result.payload,
    });
  } catch (err) {
    console.error('[lead-qualification] decision failed:', err.message);
    return res.status(500).json({ ok: false, error: 'DECISION_FAILED' });
  }
});

module.exports = router;
