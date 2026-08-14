'use strict';

/**
 * SPEC-099 — Client-facing presentation boundary.
 * Suppress unnecessary internal architecture terminology from Max prose
 * for client-role users. Does not alter structured metadata / evidence.
 */

const INTERNAL_TERM_REPLACEMENTS = [
  { pattern: /\bSPEC-\d+\b/gi, replacement: '' },
  { pattern: /\bContextEnvelope\b/gi, replacement: 'context' },
  { pattern: /\bMission Plan IR\b/gi, replacement: 'mission plan' },
  { pattern: /\bExecution Domain\b/gi, replacement: 'work area' },
  { pattern: /\bexecution domain\b/gi, replacement: 'work area' },
  { pattern: /\bDiscovery Profile(?:\s+version)?\b/gi, replacement: 'discovery profile' },
  { pattern: /\bCIE\b/g, replacement: 'business understanding' },
  { pattern: /\bClient Intelligence Engine\b/gi, replacement: 'business understanding' },
  { pattern: /\bWorkspaceEngine\b/gi, replacement: 'Max' },
  { pattern: /\bOperatorObjective(?:Context)?\b/gi, replacement: 'objective' },
  { pattern: /\bPublic Max Launch\b/gi, replacement: 'campaign' },
];

/**
 * Soften architectural jargon in client-visible prose.
 * @param {string} text
 * @returns {string}
 */
function softenClientFacingProse(text) {
  if (text == null) return text;
  let out = String(text);
  for (const { pattern, replacement } of INTERNAL_TERM_REPLACEMENTS) {
    out = out.replace(pattern, replacement);
  }
  // Collapse whitespace left by removals without erasing intentional newlines.
  out = out
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .replace(/[ \t]{2,}/g, ' ')
    .replace(/ +\./g, '.')
    .replace(/ +,/g, ',')
    .trim();
  return out;
}

/**
 * Apply client-facing softening to a Max workspace response payload.
 * Preserves evidence / confidence / uncertainty / sources fields.
 * @param {object} result
 * @returns {object}
 */
function presentMaxResultForClient(result) {
  if (!result || typeof result !== 'object') return result;
  const next = { ...result };
  if (typeof next.prose === 'string') {
    next.prose = softenClientFacingProse(next.prose);
  }
  if (typeof next.text === 'string') {
    next.text = softenClientFacingProse(next.text);
  }
  if (next.message && typeof next.message.text === 'string') {
    next.message = {
      ...next.message,
      text: softenClientFacingProse(next.message.text),
    };
  }
  if (next.structured && typeof next.structured.answer === 'string') {
    next.structured = {
      ...next.structured,
      answer: softenClientFacingProse(next.structured.answer),
    };
  }
  return next;
}

/**
 * Workspace display name for shell identity (company, not person).
 * @param {{ name?: string, business_name?: string }|null} client
 * @returns {string|null}
 */
function workspaceDisplayName(client) {
  if (!client) return null;
  const raw = String(client.business_name || client.name || '').trim();
  if (!raw) return null;
  // Strip common legal suffixes so "AS Cleaning Co." → "AS Cleaning"
  // (shell CSS uppercases → "AS CLEANING").
  return raw
    .replace(
      /(?:[\s,]+(?:p\.?\s*l\.?\s*l\.?\s*c\.?|l\.?\s*l\.?\s*c\.?|l\.?\s*l\.?\s*p\.?|inc\.?|co\.?|corp\.?))+$/i,
      ''
    )
    .replace(/[\s,]+$/g, '')
    .trim() || raw;
}

module.exports = {
  softenClientFacingProse,
  presentMaxResultForClient,
  workspaceDisplayName,
  INTERNAL_TERM_REPLACEMENTS,
};
