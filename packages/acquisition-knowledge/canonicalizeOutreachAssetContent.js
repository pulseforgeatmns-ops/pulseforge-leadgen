'use strict';

const { parseOutreachSourceText } = require('./parseOutreachSourceText');

function asText(value) {
  return value == null ? '' : String(value).trim();
}

function clone(value) {
  return JSON.parse(JSON.stringify(value == null ? null : value));
}

function hasStructuredExecutableCopy(content = {}) {
  return Boolean(asText(content.subject) && asText(content.statement));
}

/**
 * Deterministic, idempotent canonicalization of outreach_asset content.
 *
 * - Preserves sourceText as provenance evidence.
 * - Populates content.subject + content.statement when unambiguous.
 * - Skips assets that already have structured executable copy.
 * - Fails closed on ambiguous partial state or unparseable sourceText.
 *
 * @returns {{
 *   content: object,
 *   changed: boolean,
 *   skipped: boolean,
 *   reason?: string,
 *   subject?: string,
 *   statement?: string,
 * }}
 */
function canonicalizeOutreachAssetContent(content = {}, opts = {}) {
  const input = content && typeof content === 'object' ? content : {};
  const next = clone(input);
  const hasSubject = asText(next.subject);
  const hasStatement = asText(next.statement);

  if (hasSubject && hasStatement) {
    return {
      content: next,
      changed: false,
      skipped: true,
      reason: 'already_structured',
      subject: next.subject,
      statement: next.statement,
    };
  }

  if (hasSubject || hasStatement) {
    const err = new Error('Partial structured copy: both subject and statement are required.');
    err.code = 'outreach_asset_copy_partial';
    throw err;
  }

  const sourceText = asText(next.sourceText);
  if (!sourceText) {
    const err = new Error('No sourceText available for canonicalization.');
    err.code = 'source_text_missing';
    throw err;
  }

  const parsed = parseOutreachSourceText(sourceText);
  next.subject = parsed.subject;
  next.statement = parsed.statement;

  return {
    content: next,
    changed: true,
    skipped: false,
    reason: 'canonicalized_from_source_text',
    subject: parsed.subject,
    statement: parsed.statement,
  };
}

/**
 * Audit helper — inspect whether an outreach asset needs repair.
 */
function auditOutreachAssetContent(content = {}) {
  const input = content && typeof content === 'object' ? content : {};
  const hasSubject = Boolean(asText(input.subject));
  const hasStatement = Boolean(asText(input.statement));
  const hasSourceText = Boolean(asText(input.sourceText));
  const structured = hasSubject && hasStatement;
  const partial = (hasSubject && !hasStatement) || (!hasSubject && hasStatement);

  let parseable = null;
  let parseError = null;
  if (hasSourceText && !structured) {
    try {
      parseOutreachSourceText(input.sourceText);
      parseable = true;
    } catch (err) {
      parseable = false;
      parseError = { code: err.code, message: err.message };
    }
  }

  const needsRepair = !structured && hasSourceText && parseable === true && !partial;

  return {
    contentKeys: Object.keys(input),
    hasStructuredSubject: hasSubject,
    hasStructuredStatement: hasStatement,
    hasStructuredExecutableCopy: structured,
    hasPartialStructuredCopy: partial,
    hasSourceText,
    sourceTextParseable: parseable,
    parseError,
    needsRepair,
    requiresManualReview: partial || (hasSourceText && parseable === false && !structured),
  };
}

module.exports = {
  hasStructuredExecutableCopy,
  canonicalizeOutreachAssetContent,
  auditOutreachAssetContent,
};
