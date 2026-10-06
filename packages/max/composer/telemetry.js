'use strict';

function emptyComposerTelemetry() {
  return {
    max_composer_submission_count: 0,
    max_composer_attachment_count: 0,
    max_attachment_extraction_success_count: 0,
    max_attachment_extraction_failure_count: 0,
    max_spreadsheet_row_interpreted_count: 0,
    max_spreadsheet_row_blocked_count: 0,
    max_mixed_input_count: 0,
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

function bump(telemetry, key, by = 1) {
  telemetry[key] = (telemetry[key] || 0) + by;
}

function noteDimension(telemetry, key, value) {
  if (!telemetry.dimensions) telemetry.dimensions = {};
  telemetry.dimensions[key] = value;
}

module.exports = {
  emptyComposerTelemetry,
  bump,
  noteDimension,
};
