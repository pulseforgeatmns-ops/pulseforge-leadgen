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

/**
 * P0 authorization polarity — vocabulary that, said affirmatively, grants or
 * advances an operator decision. Inside a negated clause ("Don't proceed.",
 * "Do not send it", "Never launch this") it is a prohibition and must never be
 * read as authorization.
 */
const AUTHORIZATION_VERB =
  /\b(?:approv(?:e|ed|es|ing|al)|authori[sz](?:e|ed|es|ing|ation)|proceed(?:s|ing)?|go(?:ing)?\s+ahead|send(?:s|ing)?|execut(?:e|ed|es|ing|ion)|launch(?:es|ed|ing)?|continu(?:e|es|ing)|resum(?:e|es|ing)|run(?:s|ning)?|start(?:s|ing)?|begin(?:s|ning)?|move\s+forward|do\s+it|kick(?:ing)?\s+(?:it\s+)?off|green[\s-]?light|sign\s+off|mail(?:s|ing)?|publish(?:es|ing)?|dispatch(?:es|ing)?|yes)\b/i;
/** Vocabulary that ends a mission. Only an affirmative use is a cancellation. */
const CANCEL_VERB =
  /\b(?:cancel(?:s|led|ling)?|abort(?:s|ed|ing)?|stop\s+(?:the\s+|this\s+)?mission|kill\s+(?:the\s+|this\s+)?mission|terminate\s+(?:the\s+|this\s+)?mission)\b/i;

/**
 * Classify the authorization polarity of an operator message.
 *
 * - negatedAuthorization: approval / proceed / send / execute … appears inside
 *   a negated clause ("Don't proceed.", "No, do not approve.").
 * - negatedCancel: cancellation vocabulary appears only inside a negated clause
 *   ("Don't cancel the mission.").
 * - affirmativeAuthorization / affirmativeCancel: the same vocabulary outside
 *   any negated clause.
 *
 * Purely lexical and conservative: an ambiguous mixture never authorizes.
 * @param {string} text
 */
function classifyAuthorizationPolarity(text) {
  const raw = String(text || '');
  const clauses = extractNegatedClauses(raw);
  const affirmative = stripNegatedDirectives(raw);
  return {
    negatedAuthorization: clauses.some((c) => AUTHORIZATION_VERB.test(c)),
    negatedCancel: clauses.some((c) => CANCEL_VERB.test(c)),
    affirmativeAuthorization: AUTHORIZATION_VERB.test(affirmative),
    affirmativeCancel: CANCEL_VERB.test(affirmative),
    negatedClauses: clauses,
    affirmative,
  };
}

/**
 * True when the message prohibits authorization or cancellation and does not
 * also cancel affirmatively: such a message may never dispatch an approval,
 * continuation, send, or cancellation.
 * @param {string} text
 */
function prohibitsAuthorization(text) {
  const p = classifyAuthorizationPolarity(text);
  return (p.negatedAuthorization || p.negatedCancel) && !p.affirmativeCancel;
}

module.exports = {
  AUTHORIZATION_VERB,
  CANCEL_VERB,
  classifyAuthorizationPolarity,
  prohibitsAuthorization,
  stripNegatedDirectives,
  extractNegatedClauses,
  matchesOnlyWhenNegated,
  isConstraintQualifiedRequest,
};
