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
    max_attachment_intent_detected_count: 0,
    max_spreadsheet_reconcile_intent_count: 0,
    max_spreadsheet_preview_intent_count: 0,
    max_spreadsheet_commit_intent_count: 0,
    max_attachment_intent_fallback_error_count: 0,
    max_attachment_command_without_plan_count: 0,
    max_spreadsheet_terminal_turn_count: 0,
    max_spreadsheet_noop_row_count: 0,
    max_spreadsheet_unmapped_field_count: 0,
    max_spreadsheet_fields_compared_count: 0,
    max_spreadsheet_zero_change_workbook_count: 0,
    max_spreadsheet_generic_fallback_leak_count: 0,
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
