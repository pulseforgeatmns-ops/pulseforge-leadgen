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
const {
  interpretConversationalInput,
  interpretWithDurableConversationContext,
  PostgresConversationMemoryRepository,
} = require('../packages/max/understanding');
const pool = require('../db');

const requireIngestWrite = [
  requireAuth,
  requireRole('admin', 'manager'),
];

function resolveClientId(req) {
  const fromBody = normalizeClientId(req.body?.client_id ?? req.body?.clientId);
  if (fromBody != null) return fromBody;
  return normalizeClientId(req.session?.active_client_id);
}

router.post('/api/v1/max/understand', requireIngestWrite, async (req, res) => {
  try {
    const text = req.body?.text || req.body?.message;
    if (!text || !String(text).trim()) {
      return res.status(400).json({ error: 'text_required' });
    }
    const clientId = resolveClientId(req);
    const conversationId = req.body?.conversation_id || req.body?.conversationId || null;
    const actor = { userId: req.session?.user?.id, role: req.session?.user?.role };
    const baseInput = {
      text,
      conversationId,
      actor,
      now: req.body?.now,
      conversationMemory: req.body?.conversation_memory || req.body?.conversationMemory,
      contextAccounts: req.body?.context_accounts || req.body?.contextAccounts,
    };
    let interpreted;
    if (conversationId && clientId != null) {
      const memoryRepository = new PostgresConversationMemoryRepository(pool);
      await memoryRepository.init();
      interpreted = await interpretWithDurableConversationContext({
        ...baseInput,
        tenantId: clientId,
        clientId,
        memoryRepository,
      });
    } else {
      interpreted = interpretConversationalInput(baseInput);
    }
    return res.json({
      ok: true,
      situation_model: interpreted.situationModel,
      preview: interpreted.preview,
      validation: interpreted.validation,
      diagnostics: interpreted.diagnostics || interpreted.situationModel?.diagnostics,
      understanding_telemetry: interpreted.understandingTelemetry || null,
      conversation_memory_telemetry: interpreted.conversationMemoryTelemetry || null,
    });
  } catch (error) {
    console.error('[max-understanding]', error);
    return res.status(500).json({ error: 'understanding_failed', message: error.message });
  }
});

router.post('/api/v1/max/ingest', requireIngestWrite, async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    if (clientId == null) {
      return res.status(400).json({ error: 'client_id_required' });
    }
    const result = await ingestOperationalEvidence(clientId, req.body || {});
    let decisionFollowUp = null;
    if (!result.commit_blocked) {
      try {
        decisionFollowUp = await afterIngestionDecisions(clientId, result);
      } catch (decisionErr) {
        console.warn('[max-decision-execution] post-ingest evaluate skipped:', decisionErr.message);
      }
    }
    return res.json({
      ok: true,
      ingestion_id: result.ingestion_id,
      receipt: result.receipt,
      telemetry: result.telemetry,
      unresolved: result.unresolved,
      conflicts: result.conflicts,
      downstream_effects: result.downstream_effects,
      situation_model: result.situation_model || null,
      understanding_preview: result.understanding_preview || null,
      clarification_required: result.clarification_required || null,
      commit_blocked: Boolean(result.commit_blocked),
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
