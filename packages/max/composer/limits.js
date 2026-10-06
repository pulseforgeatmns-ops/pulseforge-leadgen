'use strict';

const LIMITS = Object.freeze({
  maxAttachmentsPerTurn: 5,
  maxFileBytes: 5 * 1024 * 1024,
  maxSpreadsheetRows: 500,
  maxExtractedTextChars: 120_000,
});

module.exports = {
  LIMITS,
};
