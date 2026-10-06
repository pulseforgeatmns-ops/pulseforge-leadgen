'use strict';

const { createMaxIngestionEnvelope } = require('./types');
const { LIMITS } = require('./limits');
const { emptyComposerTelemetry, bump, noteDimension } = require('./telemetry');
const { extractAttachment } = require('./adapters');
const { putAttachmentBuffer, getAttachmentBuffer } = require('./attachmentStore');
const { synthesizeRowUnderstandingText } = require('./rowText');
const { buildSpreadsheetPreview } = require('./preview');
const { interpretConversationalInput, ConversationMemory } = require('../understanding');
const { ingestOperationalUpdate } = require('../stateIngestion/pipeline');
const { SOURCE_TYPES } = require('../stateIngestion/types');

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

async function extractEnvelopeAttachments(envelope, attachmentInputs = [], telemetry) {
  const extracted = [];
  for (const input of attachmentInputs) {
    const att = envelope.attachments.find(a => a.id === input.id);
    if (!att) continue;
    let buffer = input.buffer;
    if (!buffer && att.storageRef) {
      const stored = getAttachmentBuffer(att.storageRef, envelope.tenantId);
      buffer = stored?.buffer;
    }
    const result = await extractAttachment(att, {
      buffer,
      filename: att.filename,
      transcription: input.transcription || att.transcription,
    });
    Object.assign(att, result);
    if (att.extractionStatus === 'ready') bump(telemetry, 'max_attachment_extraction_success_count');
    if (att.extractionStatus === 'failed') bump(telemetry, 'max_attachment_extraction_failure_count');
    extracted.push(att);
  }
  return extracted;
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

  await extractEnvelopeAttachments(envelope, attachmentInputs, telemetry);

  const spreadsheetRows = flattenSpreadsheetRows(envelope.attachments);
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
  const shouldCommit = confirm === true || (!requiresBatchReview && confirm !== false);

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
    bump(telemetry, 'max_spreadsheet_row_interpreted_count', spreadsheetRows.length);
    bump(telemetry, 'max_spreadsheet_row_blocked_count', preview.needs_clarification);

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
    for (const row of spreadsheetRows) {
      const text = synthesizeRowUnderstandingText({
        instruction,
        rowValues: row.values,
        sheetName: row.sheet,
        rowNumber: row.rowNumber,
        filename: row.filename,
      });
      bump(telemetry, 'max_spreadsheet_row_interpreted_count');
      const result = await ingestOperationalUpdate({
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
        artifact: {
          artifact_type: 'spreadsheet',
          filename: row.filename,
          metadata: {
            attachment_id: row.attachmentId,
            sheet: row.sheet,
            row: row.rowNumber,
            envelope_id: envelope.id,
            values: row.values,
          },
          raw_content: row.raw,
        },
      });
      if (result.unresolved?.length || result.commit_blocked) {
        bump(telemetry, 'max_spreadsheet_row_blocked_count');
      }
      results.push({ rowNumber: row.rowNumber, sheet: row.sheet, ...result });
      if (!primary) primary = result;
    }
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

  return {
    ok: true,
    committed: shouldCommit,
    ingestion_id: primary?.ingestion_id,
    receipt: primary?.receipt,
    results,
    situation_model: primary?.situation_model,
    understanding_preview: primary?.understanding_preview,
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
