'use strict';

const pool = require('../db');
const { ensureMaxVoiceSchema } = require('../utils/maxVoiceSchema');
const {
  PostgresVoiceRecordingStore,
  MemoryVoiceRecordingStore,
  persistVoiceRecording,
  transcribeVoiceRecording,
  createVoiceTranscriptionAdapter,
  emptyVoiceTelemetry,
  bumpVoice,
} = require('../packages/max/voice');

async function createVoiceStore(db = pool) {
  if (db !== pool) {
    return new MemoryVoiceRecordingStore();
  }
  await ensureMaxVoiceSchema(db);
  return new PostgresVoiceRecordingStore(db);
}

async function uploadAndTranscribeVoice(clientId, {
  buffer,
  mimeType,
  durationMs,
  conversationId,
  actorId,
  attachmentId,
  envelopeId,
  adapter,
  db = pool,
} = {}) {
  const telemetry = emptyVoiceTelemetry();
  bumpVoice(telemetry, 'max_voice_recording_completed_count');
  const store = await createVoiceStore(db);

  let record;
  try {
    record = await persistVoiceRecording({
      store,
      clientId,
      attachmentId,
      buffer,
      mimeType,
      durationMs,
      conversationId,
      actorId,
      envelopeId,
    });
    bumpVoice(telemetry, 'max_voice_upload_success_count');
  } catch (err) {
    bumpVoice(telemetry, 'max_voice_upload_failure_count');
    throw err;
  }

  const transcription = await transcribeVoiceRecording({
    store,
    clientId,
    recordingId: record.id,
    adapter: adapter || createVoiceTranscriptionAdapter(),
    telemetry,
  });

  return {
    recording: transcription.recording,
    transcription: {
      text: transcription.text,
      confidence: transcription.confidence,
      segments: transcription.segments,
      providerMetadata: transcription.providerMetadata,
    },
    telemetry,
  };
}

async function retryTranscription(clientId, recordingId, { adapter, db = pool, force = true } = {}) {
  const telemetry = emptyVoiceTelemetry();
  const store = await createVoiceStore(db);
  const transcription = await transcribeVoiceRecording({
    store,
    clientId,
    recordingId,
    adapter: adapter || createVoiceTranscriptionAdapter(),
    force,
    telemetry,
  });
  return { ...transcription, telemetry };
}

module.exports = {
  createVoiceStore,
  MemoryVoiceRecordingStore,
  uploadAndTranscribeVoice,
  retryTranscription,
  persistVoiceRecording,
  transcribeVoiceRecording,
  createVoiceTranscriptionAdapter,
};
