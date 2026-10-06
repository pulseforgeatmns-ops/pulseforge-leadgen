'use strict';

const { normalizeText } = require('./claimParser');

/**
 * Maps spreadsheet status labels to internal semantics.
 * "Hot" is sales priority — never disposition lifecycle.
 */
function interpretSpreadsheetStatus(raw) {
  const label = normalizeText(raw).toLowerCase();
  if (!label) return { kind: 'empty' };

  if (/^(dead|closed|lost|no longer|not interested|pass)$/i.test(label) || /\bdead\b/i.test(label)) {
    return {
      kind: 'disposition',
      disposition_status: 'dead',
      confidence: 'high',
      sourceLabel: raw,
    };
  }

  if (/^(hot|priority|high priority)$/i.test(label) || /\bhot\b/i.test(label)) {
    return {
      kind: 'sales_priority',
      sales_priority: 'hot',
      confidence: 'high',
      sourceLabel: raw,
      dispositionBlocked: true,
    };
  }

  if (/^(warm|interested|waiting|follow up|follow-up)$/i.test(label)) {
    return {
      kind: 'source_label_only',
      sourceLabel: raw,
      confidence: 'medium',
    };
  }

  return {
    kind: 'ambiguous_status',
    sourceLabel: raw,
    confidence: 'low',
  };
}

module.exports = {
  interpretSpreadsheetStatus,
};
