'use strict';

const LIMITS = Object.freeze({
  maxAttachmentsPerTurn: 5,
  maxFileBytes: 5 * 1024 * 1024,
  maxSpreadsheetRows: 500,
  maxExtractedTextChars: 120_000,
  /** MAX-VOICE-001 V1 hard cutoff */
  maxVoiceDurationMs: 5 * 60 * 1000,
  maxVoiceWarnDurationMs: 4 * 60 * 1000,
});

module.exports = {
  LIMITS,
};
