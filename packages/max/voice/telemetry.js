'use strict';

function emptyVoiceTelemetry() {
  return {
    max_voice_recording_started_count: 0,
    max_voice_recording_completed_count: 0,
    max_voice_upload_success_count: 0,
    max_voice_upload_failure_count: 0,
    max_voice_transcription_success_count: 0,
    max_voice_transcription_failure_count: 0,
    max_voice_commit_blocked_count: 0,
    max_voice_clarification_required_count: 0,
    dimensions: {},
  };
}

function bumpVoice(telemetry, key, by = 1) {
  telemetry[key] = (telemetry[key] || 0) + by;
}

function noteVoiceDimension(telemetry, key, value) {
  if (!telemetry.dimensions) telemetry.dimensions = {};
  telemetry.dimensions[key] = value;
}

function durationBucket(durationMs) {
  const sec = Number(durationMs || 0) / 1000;
  if (sec <= 30) return '0-30s';
  if (sec <= 120) return '30-120s';
  if (sec <= 300) return '120-300s';
  return '300s+';
}

function mergeVoiceTelemetry(into, voice) {
  if (!voice) return into;
  for (const [k, v] of Object.entries(voice)) {
    if (k === 'dimensions') {
      into.dimensions = { ...(into.dimensions || {}), ...(v || {}) };
    } else if (typeof v === 'number') {
      into[k] = (into[k] || 0) + v;
    }
  }
  return into;
}

module.exports = {
  emptyVoiceTelemetry,
  bumpVoice,
  noteVoiceDimension,
  durationBucket,
  mergeVoiceTelemetry,
};
