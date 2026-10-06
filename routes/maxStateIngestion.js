'use strict';

const express = require('express');
const multer = require('multer');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const { normalizeClientId } = require('../utils/clientContext');
const {
  ingestOperationalEvidence,
  ingestSpreadsheetEvidence,
  listOverdueExpectationPrompts,
} = require('../services/maxStateIngestionService');
const { submitMaxComposerTurn } = require('../services/maxComposerService');
const { afterIngestionDecisions } = require('../services/maxDecisionExecutionService');
const {
  interpretConversationalInput,
  interpretWithDurableConversationContext,
  PostgresConversationMemoryRepository,
} = require('../packages/max/understanding');
const { createMaxAttachment } = require('../packages/max/composer');
const { LIMITS } = require('../packages/max/composer/limits');
const pool = require('../db');
const { getEffectiveActor, impersonationProvenance } = require('../utils/requestIdentity');
const { logImpersonationAction } = require('../services/aoImpersonationService');
const {
  uploadAndTranscribeVoice,
  retryTranscription,
  createVoiceTranscriptionAdapter,
} = require('../services/maxVoiceService');

const composerUpload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: LIMITS.maxFileBytes,
    files: LIMITS.maxAttachmentsPerTurn,
  },
});

const requireIngestWrite = [
  requireAuth,
  requireRole('admin', 'manager'),
];

const requireComposerWrite = [
  requireAuth,
  requireRole('admin', 'manager', 'ao'),
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
    const actor = actorFromSession(req);
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

function actorFromSession(req) {
  const effective = getEffectiveActor(req);
  const provenance = impersonationProvenance(req);
  return {
    userId: effective?.id,
    role: effective?.role,
    aoId: effective?.role === 'ao' ? effective.id : undefined,
    authenticatedUserId: provenance?.authenticated_user_id ?? undefined,
    impersonated: Boolean(provenance),
    impersonation: provenance || undefined,
  };
}

function parseComposerJsonBody(body = {}) {
  const attachments = [];
  const attachmentInputs = [];
  for (const raw of body.attachments || []) {
    const att = createMaxAttachment({
      id: raw.id,
      type: raw.type,
      filename: raw.filename,
      mimeType: raw.mimeType || raw.mime_type,
      transcription: raw.transcription,
    });
    attachments.push(att);
    if (raw.content_base64 || raw.contentBase64) {
      attachmentInputs.push({
        id: att.id,
        content_base64: raw.content_base64 || raw.contentBase64,
        transcription: raw.transcription,
      });
    }
  }
  return {
    text: body.text || body.message,
    conversation_id: body.conversation_id || body.conversationId,
    confirm: body.confirm,
    conversation_memory: body.conversation_memory || body.conversationMemory,
    attachments,
    attachment_inputs: attachmentInputs,
    envelope_id: body.envelope_id || body.envelopeId,
    metadata: body.metadata,
  };
}

async function handleComposerSubmit(req, res) {
  try {
    const clientId = resolveClientId(req);
    if (clientId == null) {
      return res.status(400).json({ error: 'client_id_required' });
    }
    let payload;
    if (req.is('multipart/form-data')) {
      const attachments = [];
      const attachmentInputs = [];
      for (const file of req.files || []) {
        const att = createMaxAttachment({
          type: file.fieldname === 'voice' ? 'voice' : inferAttachmentType(file),
          filename: file.originalname,
          mimeType: file.mimetype,
          transcription: req.body?.transcription,
        });
        attachments.push(att);
        const durationMs = Number(req.body?.duration_ms || req.body?.durationMs || 0) || undefined;
        attachmentInputs.push({
          id: att.id,
          buffer: file.buffer,
          transcription: req.body?.transcription,
          durationMs,
        });
      }
      payload = {
        text: req.body?.text || req.body?.message,
        conversation_id: req.body?.conversation_id || req.body?.conversationId,
        confirm: req.body?.confirm === 'true' || req.body?.confirm === true,
        conversation_memory: req.body?.conversation_memory
          ? JSON.parse(req.body.conversation_memory)
          : req.body?.conversationMemory,
        attachments,
        attachment_inputs: attachmentInputs,
        envelope_id: req.body?.envelope_id || req.body?.envelopeId,
        metadata: req.body?.metadata ? JSON.parse(req.body.metadata) : undefined,
        actor: actorFromSession(req),
      };
    } else {
      payload = parseComposerJsonBody(req.body || {});
      payload.actor = actorFromSession(req);
    }

    const result = await submitMaxComposerTurn(clientId, payload);
    if (impersonationProvenance(req)) {
      await logImpersonationAction(req, { action: 'max_composer_submit', route: req.originalUrl });
    }
    if (!result.ok) {
      let status = 400;
      if (result.error === 'extraction_failed') status = 422;
      if (result.error === 'transcription_pending') status = 202;
      if (result.error === 'empty_turn') status = 422;
      return res.status(status).json(result);
    }

    let decisionFollowUp = null;
    if (!result.preview_only && !result.commit_blocked && !result.duplicate_envelope) {
      try {
        const primary = result.results?.[0] || result;
        decisionFollowUp = await afterIngestionDecisions(clientId, primary);
      } catch (decisionErr) {
        console.warn('[max-composer] post-ingest evaluate skipped:', decisionErr.message);
      }
    }

    return res.json({
      ok: true,
      ...result,
      decision_follow_up: decisionFollowUp
        ? {
          decision_id: decisionFollowUp.decision?.id,
          receipt: decisionFollowUp.receipt,
          execution_status: decisionFollowUp.decision?.execution_status,
        }
        : null,
    });
  } catch (error) {
    console.error('[max-composer]', error);
    return res.status(500).json({ error: 'composer_failed', message: error.message });
  }
}

function inferAttachmentType(file) {
  const mime = String(file.mimetype || '').toLowerCase();
  const name = String(file.originalname || '').toLowerCase();
  if (/spreadsheet|excel|csv/.test(mime) || /\.(xlsx|xls|csv)$/.test(name)) return 'spreadsheet';
  if (/^image\//.test(mime) || /\.(png|jpe?g|webp)$/.test(name)) return 'image';
  if (/^audio\//.test(mime)) return 'voice';
  return 'document';
}

router.post(
  '/api/v1/max/composer',
  requireComposerWrite,
  composerUpload.any(),
  handleComposerSubmit,
);

async function handleVoiceUpload(req, res) {
  try {
    const clientId = resolveClientId(req);
    if (clientId == null) {
      return res.status(400).json({ error: 'client_id_required' });
    }
    const file = (req.files || []).find(f => f.fieldname === 'voice' || f.fieldname === 'audio') || req.file;
    if (!file?.buffer?.length) {
      return res.status(400).json({ error: 'audio_required' });
    }
    const durationMs = Number(req.body?.duration_ms || req.body?.durationMs || 0) || null;
    if (durationMs && durationMs > LIMITS.maxVoiceDurationMs) {
      return res.status(400).json({ error: 'duration_exceeded', max_ms: LIMITS.maxVoiceDurationMs });
    }
    const actor = actorFromSession(req);
    const result = await uploadAndTranscribeVoice(clientId, {
      buffer: file.buffer,
      mimeType: file.mimetype,
      durationMs,
      conversationId: req.body?.conversation_id || req.body?.conversationId,
      actorId: actor.userId,
      attachmentId: req.body?.attachment_id || req.body?.attachmentId,
      adapter: createVoiceTranscriptionAdapter(),
    });
    return res.json({
      ok: true,
      recording_id: result.recording.id,
      attachment_id: result.recording.attachment_id || result.recording.id,
      transcription: result.transcription,
      telemetry: result.telemetry,
    });
  } catch (error) {
    const code = error.code || 'voice_upload_failed';
    const status = code === 'unsupported_audio_type' ? 415
      : code === 'empty_transcript' ? 422
        : code === 'recording_not_found' ? 404
          : 500;
    if (code === 'empty_transcript' || code === 'transcription_provider_error') {
      return res.status(status).json({
        ok: false,
        error: code,
        message: error.message,
        transcription_status: code === 'empty_transcript' ? 'empty' : 'failed',
      });
    }
    console.error('[max-voice-upload]', error);
    return res.status(status).json({ error: code, message: error.message });
  }
}

router.post(
  '/api/v1/max/voice/transcribe',
  requireComposerWrite,
  composerUpload.fields([{ name: 'voice', maxCount: 1 }, { name: 'audio', maxCount: 1 }]),
  handleVoiceUpload,
);

router.post('/api/v1/max/voice/recordings/:id/transcribe', requireComposerWrite, async (req, res) => {
  try {
    const clientId = resolveClientId(req);
    if (clientId == null) {
      return res.status(400).json({ error: 'client_id_required' });
    }
    const result = await retryTranscription(clientId, req.params.id, {
      adapter: createVoiceTranscriptionAdapter(),
      force: req.body?.force !== false,
    });
    return res.json({
      ok: true,
      recording_id: result.recording.id,
      transcription: {
        text: result.text,
        confidence: result.confidence,
        segments: result.segments,
        providerMetadata: result.providerMetadata,
      },
      from_cache: Boolean(result.fromCache),
      telemetry: result.telemetry,
    });
  } catch (error) {
    const code = error.code || 'transcription_failed';
    const status = code === 'recording_not_found' ? 404 : code === 'empty_transcript' ? 422 : 500;
    return res.status(status).json({ error: code, message: error.message });
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
    const held = batch.recordResults.filter(r => r.skipped || (r.unresolved?.length || r.conflicts?.length)).length;
    return res.json({
      ok: true,
      records_examined: batch.recordsExamined,
      workbook_summary: batch.workbookSummary || null,
      reconciliation_summary: batch.reconciliationPlan?.summary || null,
      summary: batch.summary,
      held_for_review: held,
      telemetry: batch.telemetry || null,
      records: batch.recordResults.map(r => ({
        ingestion_id: r.ingestion_id,
        receipt: r.receipt,
        skipped: r.skipped || false,
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
