'use strict';

const { createMaxIngestionEnvelope } = require('./types');
const { LIMITS } = require('./limits');
const { emptyComposerTelemetry, bump, noteDimension } = require('./telemetry');
const { extractAttachment } = require('./adapters');
const voiceAdapter = require('./adapters/voice');
const { putAttachmentBuffer, getAttachmentBuffer } = require('./attachmentStore');
const { synthesizeRowUnderstandingText } = require('./rowText');
const { buildSpreadsheetPreview } = require('./preview');
const { interpretConversationalInput, ConversationMemory } = require('../understanding');
const { ingestOperationalUpdate } = require('../stateIngestion/pipeline');
const {
  buildSpreadsheetReconciliationPlan,
  commitSpreadsheetReconciliationPlan,
} = require('../stateIngestion/spreadsheetReconciliation');
const { stableHash } = require('../stateIngestion/fingerprints');
const { SOURCE_TYPES } = require('../stateIngestion/types');
const { bumpVoice, noteVoiceDimension } = require('../voice/telemetry');
const { persistVoiceRecording, transcribeVoiceRecording } = require('../voice/voiceIngestion');
const { createVoiceTranscriptionAdapter } = require('../voice/transcriptionAdapter');
const { formatUnderstandingPreview } = require('../understanding/preview');

const appliedEnvelopeKeys = new Set();

function envelopeKey(tenantId, envelopeId) {
  return `${tenantId}:${envelopeId}`;
}

function flattenSpreadsheetRows(attachments = []) {
  const rows = [];
  for (const att of attachments) {
    const data = att.structuredData;
    if (!data?.sheets) continue;
    for (const sheet of data.sheets) {
      for (const row of sheet.rows || []) {
        rows.push({
          ...row,
          sheet: sheet.sheet,
          filename: data.filename,
          attachmentId: att.id,
        });
      }
    }
  }
  return rows;
}

function augmentTextWithExtractions(envelope, attachments) {
  const chunks = [];
  if (envelope.text && String(envelope.text).trim()) {
    chunks.push(String(envelope.text).trim());
  }
  for (const att of attachments) {
    if (att.extractionStatus === 'failed') continue;
    if (att.transcription) chunks.push(att.transcription);
    if (att.extractedText) chunks.push(att.extractedText);
  }
  return chunks.join('\n\n').trim();
}

function envelopeHasVoiceAttachments(envelope) {
  return envelope.attachments.some(a => a.type === 'voice' || a.mimeType?.startsWith('audio/'));
}

async function extractEnvelopeAttachments(envelope, attachmentInputs = [], telemetry, voiceCtx = {}) {
  const extracted = [];
  for (const input of attachmentInputs) {
    const att = envelope.attachments.find(a => a.id === input.id);
    if (!att) continue;
    let buffer = input.buffer;
    if (!buffer && att.storageRef) {
      const stored = getAttachmentBuffer(att.storageRef, envelope.tenantId);
      buffer = stored?.buffer;
    }
    const isVoice = att.type === 'voice' || supportsVoiceAttachment(att);
    let transcribeVoice;
    if (isVoice && buffer && voiceCtx.recordingStore) {
      transcribeVoice = async ({ attachment, buffer: audioBuffer, mimeType, durationMs }) => {
        bumpVoice(telemetry, 'max_voice_upload_success_count');
        const record = await persistVoiceRecording({
          store: voiceCtx.recordingStore,
          clientId: voiceCtx.clientId,
          attachmentId: attachment.id,
          buffer: audioBuffer,
          mimeType: mimeType || attachment.mimeType,
          durationMs: durationMs ?? input.durationMs,
          conversationId: envelope.conversationId,
          actorId: envelope.actor?.userId,
          envelopeId: envelope.id,
        });
        const transcribed = await transcribeVoiceRecording({
          store: voiceCtx.recordingStore,
          clientId: voiceCtx.clientId,
          recordingId: record.id,
          adapter: voiceCtx.transcriptionAdapter,
          telemetry,
        });
        bump(telemetry, 'max_voice_transcription_success_count', transcribed.fromCache ? 0 : 1);
        return {
          text: transcribed.text,
          confidence: transcribed.confidence,
          segments: transcribed.segments,
          providerMetadata: transcribed.providerMetadata,
          recordingId: record.id,
        };
      };
    }
    const result = await extractAttachment(att, {
      buffer,
      filename: att.filename,
      mimeType: att.mimeType,
      durationMs: input.durationMs,
      transcription: input.transcription || att.transcription,
      segments: input.segments || att.extractionEvidence?.segments,
      confidence: input.confidence,
      transcribeVoice,
    });
    Object.assign(att, result);
    if (att.extractionStatus === 'ready') bump(telemetry, 'max_attachment_extraction_success_count');
    if (att.extractionStatus === 'failed') {
      bump(telemetry, 'max_attachment_extraction_failure_count');
      if (isVoice) bump(telemetry, 'max_voice_transcription_failure_count');
    }
    extracted.push(att);
  }
  return extracted;
}

function supportsVoiceAttachment(att) {
  return voiceAdapter.supports(att);
}

async function buildVoiceReviewPreview({
  envelope,
  memory,
  store,
  now,
  telemetry,
}) {
  const combinedText = augmentTextWithExtractions(envelope, envelope.attachments);
  if (!combinedText) {
    return { error: 'empty_turn', message: 'Transcript empty — nothing to interpret.' };
  }
  let contextAccounts;
  if (store?.snapshotContext) {
    const ctx = await store.snapshotContext();
    contextAccounts = ctx.companies?.map(c => c.name).filter(Boolean);
  }
  const interpreted = interpretConversationalInput({
    text: combinedText,
    conversationId: envelope.conversationId,
    memory,
    actor: envelope.actor,
    now,
    contextAccounts,
  });
  memory.recordTurn({
    inputId: envelope.id,
    text: combinedText,
    situationModel: interpreted.situationModel,
  });
  if (interpreted.validation?.blockCommit) {
    bump(telemetry, 'max_voice_commit_blocked_count');
    bump(telemetry, 'max_voice_clarification_required_count');
    noteVoiceDimension(telemetry, 'blocked_reason', 'material_ambiguity');
  }
  return {
    situationModel: interpreted.situationModel,
    understandingPreview: interpreted.preview || formatUnderstandingPreview(interpreted.situationModel),
    validation: interpreted.validation,
    combinedText,
  };
}

async function submitComposerTurn({
  clientId,
  envelope: envelopeInput,
  attachmentInputs = [],
  store,
  confirm = null,
  conversationMemory = null,
  sourceType = SOURCE_TYPES.OPERATOR_REPORTED,
  now = new Date(),
  voiceRecordingStore = null,
  transcriptionAdapter = null,
}) {
  const telemetry = emptyComposerTelemetry();
  bump(telemetry, 'max_composer_submission_count');
  noteDimension(telemetry, 'source_type', envelopeInput.sourceType);

  const envelope = createMaxIngestionEnvelope({
    ...envelopeInput,
    tenantId: String(clientId),
  });
  bump(telemetry, 'max_composer_attachment_count', envelope.attachments.length);

  if (envelope.attachments.length > LIMITS.maxAttachmentsPerTurn) {
    return {
      ok: false,
      error: 'too_many_attachments',
      message: `At most ${LIMITS.maxAttachmentsPerTurn} attachments per turn.`,
      telemetry,
    };
  }

  for (const input of attachmentInputs) {
    if (input.buffer && input.buffer.length > LIMITS.maxFileBytes) {
      return {
        ok: false,
        error: 'file_too_large',
        message: `Attachments must be under ${LIMITS.maxFileBytes} bytes.`,
        telemetry,
      };
    }
    const att = envelope.attachments.find(a => a.id === input.id);
    if (att && input.buffer) {
      att.storageRef = putAttachmentBuffer(clientId, att.id, input.buffer, {
        filename: att.filename,
        mimeType: att.mimeType,
      });
    }
  }

  const voiceCtx = {
    recordingStore: voiceRecordingStore,
    clientId,
    transcriptionAdapter: transcriptionAdapter || createVoiceTranscriptionAdapter({ stub: !process.env.OPENAI_API_KEY }),
  };
  await extractEnvelopeAttachments(envelope, attachmentInputs, telemetry, voiceCtx);

  const spreadsheetRows = flattenSpreadsheetRows(envelope.attachments);
  const hasVoice = envelopeHasVoiceAttachments(envelope);
  const extractionFailures = envelope.attachments.filter(a => a.extractionStatus === 'failed');
  const spreadsheetAttachments = envelope.attachments.filter(a => a.type === 'spreadsheet');
  const spreadsheetOnly = spreadsheetAttachments.length > 0
    && envelope.attachments.every(a => a.type === 'spreadsheet' || a.extractionStatus === 'failed');
  if (
    spreadsheetOnly
    && spreadsheetRows.length === 0
    && (extractionFailures.length || spreadsheetAttachments.some(a => a.extractionStatus !== 'ready'))
  ) {
    return {
      ok: false,
      error: 'extraction_failed',
      message: extractionFailures[0]?.extractionEvidence?.message
        || 'Could not read spreadsheet contents.',
      extraction_failures: extractionFailures.length
        ? extractionFailures.map(a => ({ id: a.id, filename: a.filename, evidence: a.extractionEvidence }))
        : spreadsheetAttachments.map(a => ({
          id: a.id,
          filename: a.filename,
          evidence: a.extractionEvidence || { error: 'empty_spreadsheet' },
        })),
      telemetry,
      envelope,
    };
  }

  if (extractionFailures.length && envelope.attachments.every(a => a.extractionStatus === 'failed')) {
    return {
      ok: false,
      error: 'extraction_failed',
      message: extractionFailures[0].extractionEvidence?.message
        || extractionFailures[0].extractionEvidence?.error
        || 'Could not extract attachment contents.',
      extraction_failures: extractionFailures.map(a => ({
        id: a.id,
        filename: a.filename,
        evidence: a.extractionEvidence,
      })),
      telemetry,
      envelope,
    };
  }

  if (envelope.sourceType === 'mixed') bump(telemetry, 'max_mixed_input_count');

  const voiceAttachments = envelope.attachments.filter(a => a.type === 'voice');
  const voiceOnlyPending = voiceAttachments.length > 0
    && voiceAttachments.every(a => a.extractionStatus === 'pending');
  if (voiceOnlyPending) {
    return {
      ok: false,
      error: 'transcription_pending',
      message: 'Voice note is stored but transcription is not available yet.',
      telemetry,
      envelope,
    };
  }

  const memory = conversationMemory instanceof ConversationMemory
    ? conversationMemory
    : ConversationMemory.fromSeed(conversationMemory || {});

  if (!memory.conversationId && envelope.conversationId) {
    memory.conversationId = envelope.conversationId;
  }

  if (spreadsheetRows.length > LIMITS.maxSpreadsheetRows) {
    return {
      ok: false,
      error: 'too_many_rows',
      message: `Spreadsheet exceeds ${LIMITS.maxSpreadsheetRows} row limit.`,
      telemetry,
    };
  }

  const instruction = envelope.text && String(envelope.text).trim() ? String(envelope.text).trim() : null;
  const requiresBatchReview = spreadsheetRows.length > 1;
  const requiresVoiceReview = hasVoice && confirm !== true;
  const shouldCommit = confirm === true || (!requiresBatchReview && !requiresVoiceReview && confirm !== false);

  if (requiresVoiceReview && !spreadsheetRows.length) {
    const review = await buildVoiceReviewPreview({ envelope, memory, store, now, telemetry });
    if (review.error) {
      return { ok: false, error: review.error, message: review.message, telemetry, envelope };
    }
    return {
      ok: true,
      preview_only: true,
      voice_review_required: true,
      understanding_preview: review.understandingPreview,
      situation_model: review.situationModel,
      validation: review.validation,
      clarification_required: review.validation?.narrowestClarification || null,
      commit_blocked: Boolean(review.validation?.blockCommit),
      transcript: voiceAttachments[0]?.transcription || voiceAttachments[0]?.extractedText || null,
      conversation_memory: memory,
      extraction_failures: extractionFailures.map(a => ({
        id: a.id,
        filename: a.filename,
        evidence: a.extractionEvidence,
      })),
      telemetry,
      envelope,
    };
  }

  if (requiresBatchReview && confirm !== true) {
    const preview = buildSpreadsheetPreview({
      rows: spreadsheetRows,
      instruction,
      store,
      memory,
      conversationId: envelope.conversationId,
      filename: spreadsheetRows[0]?.filename,
      sheetName: spreadsheetRows[0]?.sheet,
    });
    bump(telemetry, 'max_spreadsheet_upload_count');
    bump(telemetry, 'max_spreadsheet_row_interpreted_count', spreadsheetRows.length);
    bump(telemetry, 'max_spreadsheet_row_blocked_count', preview.needs_clarification);
    if (preview.reconciliation_plan?.summary?.conflicts) {
      bump(telemetry, 'max_spreadsheet_conflict_count', preview.reconciliation_plan.summary.conflicts);
    }
    if (preview.reconciliation_plan?.summary?.ambiguous) {
      bump(telemetry, 'max_spreadsheet_ambiguity_count', preview.reconciliation_plan.summary.ambiguous);
    }

    const combinedText = augmentTextWithExtractions(envelope, envelope.attachments);
    let situationModel = null;
    let understandingPreview = null;
    if (combinedText) {
      const interpreted = interpretConversationalInput({
        text: combinedText,
        conversationId: envelope.conversationId,
        memory,
        actor: envelope.actor,
        now,
      });
      situationModel = interpreted.situationModel;
      understandingPreview = interpreted.preview;
      memory.recordTurn({
        inputId: envelope.id,
        text: combinedText,
        situationModel,
      });
    }

    return {
      ok: true,
      preview_only: true,
      review_required: true,
      batch_preview: preview,
      understanding_preview: understandingPreview,
      situation_model: situationModel,
      conversation_memory: memory,
      extraction_failures: extractionFailures.map(a => ({
        id: a.id,
        filename: a.filename,
        evidence: a.extractionEvidence,
      })),
      telemetry,
      envelope,
    };
  }

  const idKey = envelopeKey(clientId, envelope.id);
  if (appliedEnvelopeKeys.has(idKey)) {
    return {
      ok: true,
      duplicate_envelope: true,
      telemetry,
      envelope,
    };
  }

  const results = [];
  let primary = null;

  if (spreadsheetRows.length) {
    const sheetMap = new Map();
    for (const row of spreadsheetRows) {
      if (!sheetMap.has(row.sheet)) sheetMap.set(row.sheet, []);
      sheetMap.get(row.sheet).push(row);
    }
    const structuredData = {
      filename: spreadsheetRows[0]?.filename || 'spreadsheet',
      sheets: [...sheetMap.entries()].map(([sheet, sheetRows]) => ({ sheet, rows: sheetRows })),
    };
    const fileHash = stableHash([JSON.stringify(structuredData)]);
    const plan = buildSpreadsheetReconciliationPlan({
      structuredData,
      store,
      instruction,
      fileId: envelope.id,
      fileHash,
      memory,
      conversationId: envelope.conversationId,
    });
    bump(telemetry, 'max_spreadsheet_upload_count');
    bump(telemetry, 'max_spreadsheet_rows_processed_count', plan.rows.length);
    const batch = await commitSpreadsheetReconciliationPlan({
      plan,
      clientId,
      store,
      sourceActor: envelope.actor?.userId || envelope.actor?.aoId || null,
      instruction,
      now,
      commitMode: shouldCommit ? 'safe_only' : 'none',
    });
    for (const [key, value] of Object.entries(batch.telemetry || {})) {
      if (key.startsWith('max_spreadsheet_')) bump(telemetry, key, value);
    }
    for (const rowResult of batch.results) {
      bump(telemetry, 'max_spreadsheet_row_interpreted_count');
      if (rowResult.skipped || rowResult.unresolved?.length || rowResult.commit_blocked) {
        bump(telemetry, 'max_spreadsheet_row_blocked_count');
      }
      results.push(rowResult);
      if (!primary && !rowResult.skipped) primary = rowResult;
    }
    if (!primary && results.length) primary = results[0];
  } else {
    const text = augmentTextWithExtractions(envelope, envelope.attachments);
    if (!text) {
      return { ok: false, error: 'empty_turn', message: 'Text or extractable attachment required.', telemetry };
    }
    primary = await ingestOperationalUpdate({
      clientId,
      text,
      sourceType,
      sourceActor: envelope.actor?.userId || envelope.actor?.aoId || null,
      actor: envelope.actor,
      conversationId: envelope.conversationId,
      conversationMemory: memory,
      operatorCorrection: Boolean(envelope.metadata?.operatorCorrection),
      store,
      now,
      artifact: envelope.attachments[0]
        ? {
          artifact_type: envelope.attachments[0].type || 'attachment',
          filename: envelope.attachments[0].filename,
          metadata: {
            attachment_id: envelope.attachments[0].id,
            envelope_id: envelope.id,
            extraction_status: envelope.attachments[0].extractionStatus,
          },
        }
        : null,
    });
    results.push(primary);
  }

  appliedEnvelopeKeys.add(idKey);

  const understandingPreview = primary?.understanding_preview
    || (primary?.situation_model ? formatUnderstandingPreview(primary.situation_model) : null)
    || (results[0]?.understanding_preview
      || (results[0]?.situation_model ? formatUnderstandingPreview(results[0].situation_model) : null));

  return {
    ok: true,
    committed: shouldCommit,
    ingestion_id: primary?.ingestion_id,
    receipt: primary?.receipt,
    results,
    situation_model: primary?.situation_model || results[0]?.situation_model,
    understanding_preview: understandingPreview,
    clarification_required: primary?.clarification_required,
    commit_blocked: Boolean(primary?.commit_blocked),
    conversation_memory: primary?.conversation_memory || memory,
    extraction_failures: extractionFailures.map(a => ({
      id: a.id,
      filename: a.filename,
      evidence: a.extractionEvidence,
    })),
    telemetry,
    envelope,
  };
}

function resetComposerIdempotencyForTests() {
  appliedEnvelopeKeys.clear();
}

module.exports = {
  submitComposerTurn,
  flattenSpreadsheetRows,
  augmentTextWithExtractions,
  resetComposerIdempotencyForTests,
};
