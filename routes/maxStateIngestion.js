'use strict';

const express = require('express');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const { normalizeClientId } = require('../utils/clientContext');
const {
  ingestOperationalEvidence,
  ingestSpreadsheetEvidence,
  listOverdueExpectationPrompts,
} = require('../services/maxStateIngestionService');
const { afterIngestionDecisions } = require('../services/maxDecisionExecutionService');

const requireIngestWrite = [
  requireAuth,
  requireRole('admin', 'manager'),
];

function resolveClientId(req) {
  const fromBody = normalizeClientId(req.body?.client_id ?? req.body?.clientId);
  if (fromBody != null) return fromBody;
  return normalizeClientId(req.session?.active_client_id);
}

router.post('/api/v1/max/ingest', requireIngestWrite, async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    if (clientId == null) {
      return res.status(400).json({ error: 'client_id_required' });
    }
    const result = await ingestOperationalEvidence(clientId, req.body || {});
    let decisionFollowUp = null;
    try {
      decisionFollowUp = await afterIngestionDecisions(clientId, result);
    } catch (decisionErr) {
      console.warn('[max-decision-execution] post-ingest evaluate skipped:', decisionErr.message);
    }
    return res.json({
      ok: true,
      ingestion_id: result.ingestion_id,
      receipt: result.receipt,
      telemetry: result.telemetry,
      unresolved: result.unresolved,
      conflicts: result.conflicts,
      downstream_effects: result.downstream_effects,
      decision_follow_up: decisionFollowUp
        ? {
          decision_id: decisionFollowUp.decision?.id,
          receipt: decisionFollowUp.receipt,
          execution_status: decisionFollowUp.decision?.execution_status,
        }
        : null,
    });
  } catch (error) {
    console.error('[max-state-ingestion]', error);
    return res.status(500).json({ error: 'ingestion_failed', message: error.message });
  }
});

router.post('/api/v1/max/ingest/spreadsheet', requireIngestWrite, async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    if (clientId == null) {
      return res.status(400).json({ error: 'client_id_required' });
    }
    const batch = await ingestSpreadsheetEvidence(clientId, req.body || {});
    const held = batch.recordResults.filter(r => (r.unresolved?.length || r.conflicts?.length)).length;
    return res.json({
      ok: true,
      records_examined: batch.recordsExamined,
      summary: batch.summary,
      held_for_review: held,
      records: batch.recordResults.map(r => ({
        ingestion_id: r.ingestion_id,
        receipt: r.receipt,
        telemetry: r.telemetry,
      })),
    });
  } catch (error) {
    console.error('[max-state-ingestion/spreadsheet]', error);
    return res.status(500).json({ error: 'spreadsheet_ingestion_failed', message: error.message });
  }
});

router.get('/api/v1/max/ingest/expectations/overdue', requireIngestWrite, async (req, res) => {
  try {
    const clientId = normalizeClientId(req.query.client_id) ?? resolveClientId(req);
    if (clientId == null) {
      return res.status(400).json({ error: 'client_id_required' });
    }
    const prompts = await listOverdueExpectationPrompts(clientId);
    return res.json({ ok: true, prompts });
  } catch (error) {
    return res.status(500).json({ error: 'expectation_scan_failed', message: error.message });
  }
});

module.exports = router;
