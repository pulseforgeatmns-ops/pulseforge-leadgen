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

const DECISION_VOCABULARY =
  /\b(?:approv(?:e|ed|al)|authori[sz]e|reject|deny|cancel|abort|go\s+ahead|proceed|yes|yep|yeah|ok(?:ay)?|send|launch|execute|continue|resume|hold|stop|revise|rewrite|regenerate|redo)\b/i;
const INFORMATIONAL_REQUEST =
  /\b(?:explain|identify|distinguish|differentiate|separate|summari[sz]e|recommend|assess|analy[sz]e|describe|list|cite|compare|evaluate|interpret|report|tell\s+me|show\s+me|why|what|which|how)\b/i;

/**
 * P0 #1089 — a message whose prohibitions qualify a separate informational
 * request ("Explain the outbound pause. Do not approve or resume missions.")
 * is not a decision response: the negated clauses are constraints on the
 * request, not an answer to a pending approve/reject decision. A message that
 * is only a prohibition ("Don't send it.") or that also uses decision
 * vocabulary affirmatively keeps its normal decision handling.
 * @param {string} text
 * @returns {boolean}
 */
function isConstraintQualifiedRequest(text) {
  const raw = String(text || '');
  if (!extractNegatedClauses(raw).length) return false;
  const affirmative = stripNegatedDirectives(raw);
  if (affirmative.split(/\s+/).filter((w) => /[a-z]/i.test(w)).length < 3) return false;
  if (DECISION_VOCABULARY.test(affirmative)) return false;
  return INFORMATIONAL_REQUEST.test(affirmative);
}

module.exports = {
  stripNegatedDirectives,
  extractNegatedClauses,
  matchesOnlyWhenNegated,
  isConstraintQualifiedRequest,
};
