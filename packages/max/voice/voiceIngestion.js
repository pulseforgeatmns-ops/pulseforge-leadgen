'use strict';

const { putAttachmentBuffer, getAttachmentBuffer } = require('../composer/attachmentStore');
const { isSupportedAudioMime, normalizeMimeType } = require('./types');
const { createVoiceTranscriptionAdapter } = require('./transcriptionAdapter');
const { newVoiceRecordingId } = require('./recordingStore');
const { emptyVoiceTelemetry, bumpVoice, noteVoiceDimension, durationBucket } = require('./telemetry');

const transcriptionCache = new Map();

function transcriptionCacheKey(clientId, attachmentId) {
  return `${clientId}:${attachmentId}`;
}

function resetVoiceTranscriptionCacheForTests() {
  transcriptionCache.clear();
}

async function persistVoiceRecording({
  store,
  clientId,
  attachmentId,
  buffer,
  mimeType,
  durationMs,
  conversationId,
  actorId,
  envelopeId,
  storageRef,
}) {
  const id = attachmentId || newVoiceRecordingId();
  const mime = normalizeMimeType(mimeType);
  if (!isSupportedAudioMime(mime)) {
    const err = new Error('unsupported_audio_type');
    err.code = 'unsupported_audio_type';
    throw err;
  }
  const ref = storageRef || putAttachmentBuffer(clientId, id, buffer, { filename: `${id}.audio`, mimeType: mime });
  const record = await store.create({
    id,
    client_id: clientId,
    conversation_id: conversationId || null,
    actor_id: actorId || null,
    mime_type: mime,
    duration_ms: durationMs ?? null,
    storage_ref: ref,
    audio_bytea: buffer,
    transcription_status: 'pending',
    transcript_segments: [],
    envelope_id: envelopeId || null,
    attachment_id: attachmentId || id,
  });
  return record;
}

async function loadVoiceBuffer(record, clientId) {
  if (record.audio_bytea) return Buffer.from(record.audio_bytea);
  const stored = getAttachmentBuffer(record.storage_ref, clientId);
  return stored?.buffer || null;
}

async function transcribeVoiceRecording({
  store,
  clientId,
  recordingId,
  adapter,
  force = false,
  telemetry = emptyVoiceTelemetry(),
}) {
  const record = await store.get(recordingId, clientId);
  if (!record) {
    const err = new Error('recording_not_found');
    err.code = 'recording_not_found';
    throw err;
  }

  const cacheKey = transcriptionCacheKey(clientId, record.id);
  if (!force && transcriptionCache.has(cacheKey)) {
    return transcriptionCache.get(cacheKey);
  }

  if (!force && record.transcription_status === 'completed' && record.transcript_text) {
    const cached = {
      recording: record,
      text: record.transcript_text,
      confidence: record.transcription_confidence,
      segments: record.transcript_segments || [],
      fromCache: true,
    };
    transcriptionCache.set(cacheKey, cached);
    return cached;
  }

  await store.update(record.id, clientId, { transcription_status: 'processing' });
  const buffer = await loadVoiceBuffer(record, clientId);
  if (!buffer?.length) {
    await store.update(record.id, clientId, { transcription_status: 'failed' });
    bumpVoice(telemetry, 'max_voice_transcription_failure_count');
    noteVoiceDimension(telemetry, 'transcription_status', 'failed');
    const err = new Error('audio_not_found');
    err.code = 'audio_not_found';
    throw err;
  }

  const transcribe = adapter || createVoiceTranscriptionAdapter();
  try {
    const result = await transcribe.transcribe({
      buffer,
      mimeType: record.mime_type,
      durationMs: record.duration_ms,
    });
    const text = String(result.text || '').trim();
    if (!text) {
      await store.update(record.id, clientId, {
        transcription_status: 'empty',
        transcript_text: null,
        transcript_segments: [],
        transcription_confidence: result.confidence ?? null,
        transcription_provider: transcribe.name,
        transcription_metadata: result.providerMetadata || {},
      });
      bumpVoice(telemetry, 'max_voice_transcription_failure_count');
      noteVoiceDimension(telemetry, 'transcription_status', 'empty');
      const err = new Error('empty_transcript');
      err.code = 'empty_transcript';
      throw err;
    }

    const updated = await store.update(record.id, clientId, {
      transcription_status: 'completed',
      transcript_text: text,
      transcript_segments: result.segments || [],
      transcription_confidence: result.confidence ?? null,
      transcription_provider: transcribe.name,
      transcription_metadata: result.providerMetadata || {},
    });
    bumpVoice(telemetry, 'max_voice_transcription_success_count');
    noteVoiceDimension(telemetry, 'transcription_status', 'completed');
    noteVoiceDimension(telemetry, 'duration_bucket', durationBucket(record.duration_ms));

    const payload = {
      recording: updated,
      text,
      confidence: result.confidence,
      segments: result.segments || [],
      providerMetadata: result.providerMetadata,
      fromCache: false,
    };
    transcriptionCache.set(cacheKey, payload);
    return payload;
  } catch (err) {
    if (err.code !== 'empty_transcript') {
      await store.update(record.id, clientId, { transcription_status: 'failed' });
      bumpVoice(telemetry, 'max_voice_transcription_failure_count');
      noteVoiceDimension(telemetry, 'transcription_status', 'failed');
    }
    throw err;
  }
}

module.exports = {
  persistVoiceRecording,
  transcribeVoiceRecording,
  loadVoiceBuffer,
  resetVoiceTranscriptionCacheForTests,
};
