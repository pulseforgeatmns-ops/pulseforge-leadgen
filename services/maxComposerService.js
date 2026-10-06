'use strict';

const pool = require('../db');
const { PostgresStateStore } = require('../packages/max/stateIngestion');
const { submitComposerTurn, createMaxIngestionEnvelope } = require('../packages/max/composer');
const { SOURCE_TYPES } = require('../packages/max/stateIngestion/types');
const { createVoiceStore, createVoiceTranscriptionAdapter } = require('./maxVoiceService');

async function createStore(clientId, db = pool) {
  const store = new PostgresStateStore(db, { clientId });
  await store.init();
  return store;
}

async function submitMaxComposerTurn(clientId, body = {}, { db = pool } = {}) {
  const store = await createStore(clientId, db);
  const envelope = createMaxIngestionEnvelope({
    id: body.envelope_id || body.envelopeId,
    tenantId: clientId,
    conversationId: body.conversation_id || body.conversationId,
    text: body.text || body.message,
    attachments: body.attachments || [],
    sourceType: body.source_type || body.sourceType,
    actor: body.actor || {},
    metadata: body.metadata || {},
  });

  const attachmentInputs = (body.attachment_inputs || body.attachmentInputs || []).map(a => ({
    id: a.id,
    buffer: a.buffer || (a.content_base64 || a.contentBase64
      ? Buffer.from(a.content_base64 || a.contentBase64, 'base64')
      : null),
    transcription: a.transcription,
  }));

  const sourceType = body.ingest_source_type || body.ingestSourceType
    || (body.actor?.role === 'ao' ? SOURCE_TYPES.AO_REPORTED : SOURCE_TYPES.OPERATOR_REPORTED);

  const voiceRecordingStore = await createVoiceStore(db);

  return submitComposerTurn({
    clientId,
    envelope,
    attachmentInputs,
    store,
    confirm: body.confirm,
    conversationMemory: body.conversation_memory || body.conversationMemory,
    sourceType,
    now: body.now ? new Date(body.now) : new Date(),
    voiceRecordingStore,
    transcriptionAdapter: createVoiceTranscriptionAdapter(),
  });
}

module.exports = {
  submitMaxComposerTurn,
};
