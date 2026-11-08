'use strict';

/**
 * P0 Max reliability — negated directives are constraints, not requests.
 *
 * "Do not create a mission." / "No outreach, calls or campaign activation." /
 * "Don't launch anything" describe what the operator FORBIDS. Classifiers that
 * look for request language (create mission, launch, start campaign) must not
 * match inside such clauses, otherwise a prohibition is routed as the very
 * action it forbids.
 *
 * Tenant-independent and purely lexical. A clause runs from a negation token to
 * the next clause boundary (sentence punctuation, semicolon, newline, dash).
 */

const CLAUSE_BOUNDARY = '[.!?;\\n\\u2014\\u2013]|\\s-\\s|$';
const NEGATED_CLAUSE_RE = new RegExp(
  `\\b(?:do\\s+not|don['’]?t|dont|never|must\\s+not|should\\s+not|without|no)\\b[^]*?(?=${CLAUSE_BOUNDARY})`,
  'gi'
);

/**
 * Remove negated clauses so request detectors only see affirmative language.
 * @param {string} text
 * @returns {string}
 */
function stripNegatedDirectives(text) {
  return String(text || '').replace(NEGATED_CLAUSE_RE, ' ').replace(/\s+/g, ' ').trim();
}

/**
 * The negated clauses themselves ("do not launch, execute, or mail anything").
 * Used to derive durable authorization constraints from operator prohibitions.
 * @param {string} text
 * @returns {string[]}
 */
function extractNegatedClauses(text) {
  const raw = String(text || '');
  const re = new RegExp(NEGATED_CLAUSE_RE.source, 'gi');
  const out = [];
  let match = re.exec(raw);
  while (match) {
    const clause = match[0].replace(/\s+/g, ' ').trim();
    if (clause) out.push(clause);
    if (match[0].length === 0) re.lastIndex += 1;
    match = re.exec(raw);
  }
  return out;
}

/**
 * True when `pattern` matches only inside negated clauses of `text`.
 * @param {string} text
 * @param {RegExp} pattern
 */
function matchesOnlyWhenNegated(text, pattern) {
  const raw = String(text || '');
  const re = new RegExp(pattern.source, pattern.flags.replace('g', ''));
  return re.test(raw) && !re.test(stripNegatedDirectives(raw));
}

module.exports = { stripNegatedDirectives, extractNegatedClauses, matchesOnlyWhenNegated };
