'use strict';

const express = require('express');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const { normalizeClientId } = require('../utils/clientContext');
const {
  evaluateDecision,
  runExpectationDecisionScan,
} = require('../services/maxDecisionExecutionService');

const requireDecisionWrite = [
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

router.post('/api/v1/max/decisions/evaluate', requireDecisionWrite, async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    if (clientId == null) {
      return res.status(400).json({ error: 'client_id_required' });
    }
    const result = await evaluateDecision(clientId, req.body || {});
    return res.json({
      ok: true,
      duplicate: Boolean(result.duplicate),
      decision_id: result.decision?.id,
      execution_status: result.decision?.execution_status,
      verification_status: result.decision?.verification_status,
      receipt: result.receipt,
      telemetry: result.telemetry,
    });
  } catch (error) {
    console.error('[max-decision-execution]', error);
    return res.status(500).json({ error: 'decision_evaluation_failed', message: error.message });
  }
});

router.post('/api/v1/max/decisions/scan-expectations', requireDecisionWrite, async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    if (clientId == null) {
      return res.status(400).json({ error: 'client_id_required' });
    }
    const results = await runExpectationDecisionScan(clientId, {
      now: req.body?.now ? new Date(req.body.now) : new Date(),
    });
    return res.json({
      ok: true,
      count: results.length,
      decisions: results.map(r => ({
        decision_id: r.decision?.id,
        receipt: r.receipt,
        execution_status: r.decision?.execution_status,
      })),
    });
  } catch (error) {
    return res.status(500).json({ error: 'expectation_scan_failed', message: error.message });
  }
});

router.get('/api/v1/max/decisions/:id/receipt', requireDecisionWrite, async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    if (clientId == null) {
      return res.status(400).json({ error: 'client_id_required' });
    }
    const pool = require('../db');
    const { rows } = await pool.query(
      `SELECT id, rationale, execution_status, verification_status, receipt_summary, owner, reevaluate_after, created_at
       FROM max_operational_decisions WHERE id = $1 AND client_id = $2`,
      [req.params.id, clientId]
    );
    if (!rows[0]) {
      return res.status(404).json({ error: 'decision_not_found' });
    }
    return res.json({ ok: true, decision: rows[0] });
  } catch (error) {
    return res.status(500).json({ error: 'receipt_failed', message: error.message });
  }
});

module.exports = router;
