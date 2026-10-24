'use strict';

/**
 * SPEC-247B — Deterministic extraction of executable email copy from outreach sourceText.
 *
 * Parses Babrun-style Markdown packages:
 *   **N. Contact — Company**
 *   **Subject: ...**
 *   <body>
 *   ---
 *
 * sourceText is provenance; structured subject/statement are the runtime contract.
 */

function asText(value) {
  return value == null ? '' : String(value).trim();
}

function knowledgeError(code, message, extras = {}) {
  const err = new Error(message || code);
  err.code = code;
  Object.assign(err, extras);
  return err;
}

const SUBJECT_LINE_RE = /^\*\*Subject:\s*(.+?)\s*\*\*\s*$/im;

function findSubjectMatches(sourceText) {
  const matches = [];
  const re = /^\*\*Subject:\s*(.+?)\s*\*\*\s*$/gim;
  let match;
  while ((match = re.exec(sourceText)) !== null) {
    matches.push({
      fullMatch: match[0],
      subject: asText(match[1]),
      index: match.index,
      endIndex: match.index + match[0].length,
    });
  }
  return matches;
}

function stripTerminalSeparator(body) {
  let text = String(body);
  // Remove trailing horizontal rule separator (Babrun package delimiter).
  text = text.replace(/\r\n/g, '\n');
  text = text.replace(/\n---\s*$/, '');
  return text.replace(/\s+$/, '');
}

/**
 * @param {string} sourceText
 * @returns {{ subject: string, statement: string }}
 */
function parseOutreachSourceText(sourceText) {
  const raw = sourceText == null ? '' : String(sourceText);
  if (!raw.trim()) {
    throw knowledgeError('source_text_empty', 'sourceText is empty.');
  }

  const subjectMatches = findSubjectMatches(raw);
  if (subjectMatches.length === 0) {
    throw knowledgeError('source_text_subject_missing', 'No **Subject: ...** line found in sourceText.');
  }
  if (subjectMatches.length > 1) {
    throw knowledgeError('source_text_subject_ambiguous', 'Multiple **Subject: ...** lines found in sourceText.', {
      subjectLineCount: subjectMatches.length,
    });
  }

  const { subject, endIndex } = subjectMatches[0];
  if (!subject) {
    throw knowledgeError('source_text_subject_empty', 'Subject line is present but empty.');
  }

  let body = raw.slice(endIndex);
  body = body.replace(/^\s*\n/, '');
  body = stripTerminalSeparator(body);
  if (!body.trim()) {
    throw knowledgeError('source_text_body_missing', 'Message body missing after subject line.');
  }

  return { subject, statement: body };
}

/**
 * Non-throwing parse for import paths that should fail closed only when explicitly required.
 * @returns {{ ok: true, subject: string, statement: string } | { ok: false, code: string, message: string }}
 */
function tryParseOutreachSourceText(sourceText) {
  try {
    const parsed = parseOutreachSourceText(sourceText);
    return { ok: true, ...parsed };
  } catch (err) {
    return {
      ok: false,
      code: err.code || 'source_text_parse_failed',
      message: err.message,
    };
  }
}

module.exports = {
  SUBJECT_LINE_RE,
  parseOutreachSourceText,
  tryParseOutreachSourceText,
  stripTerminalSeparator,
};
