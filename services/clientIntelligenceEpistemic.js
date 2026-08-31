'use strict';

/**
 * SPEC-221 — Durable Epistemic State for Business Understanding
 *
 * Preserves not only what the operator said, but what status the operator
 * assigned to that knowledge (KNOWN, HYPOTHESIS, UNKNOWN, UNRESOLVED, NOT_APPLICABLE).
 * Separates value from epistemic state as independent canonical concerns.
 */

const EPISTEMIC_STATES = Object.freeze({
  KNOWN: 'KNOWN',
  HYPOTHESIS: 'HYPOTHESIS',
  UNKNOWN: 'UNKNOWN',
  UNRESOLVED: 'UNRESOLVED',
  NOT_APPLICABLE: 'NOT_APPLICABLE',
});

/**
 * Classify the epistemic state of a natural language statement based on its
 * semantic meaning in the context of business understanding.
 *
 * @param {string} text - Raw or cleaned answer statement
 * @param {object} [opts] - Context options
 * @returns {string} One of EPISTEMIC_STATES
 */
function classifyEpistemicState(text, opts = {}) {
  const raw = String(text || '').trim();
  if (!raw) return EPISTEMIC_STATES.UNRESOLVED;

  const s = raw.toLowerCase().replace(/['’]/g, "'").replace(/[.!?]+$/g, '').replace(/\s+/g, ' ').trim();

  // 1. Explicit NOT_APPLICABLE
  if (
    /^(n\/?a|not applicable|none|nil|-)$/i.test(s) ||
    /\b(not applicable|doesn'?t apply|does not apply|don'?t apply|isn'?t applicable|is not applicable|not relevant|doesn'?t pertain|not something we do)\b/i.test(s) ||
    /\b(don'?t|do not|no)\s+(have|plan|sell|operate|want|use)\s+(any\s+|to\s+)?(employees|locations|offices|stores|consumers|physical|clients|customers|staff|workers|hiring)\b/i.test(s) ||
    /\bno\s+(employees|locations|offices|stores|consumers|staff|workers)\b/i.test(s) ||
    /\bdon'?t\s+plan\s+to\s+hire\b/i.test(s)
  ) {
    return EPISTEMIC_STATES.NOT_APPLICABLE;
  }

  // 2. HYPOTHESIS (evaluated before UNKNOWN so "X seems likely but we haven't validated it" -> HYPOTHESIS)
  if (
    /\b(working assumption|our assumption|my assumption|assum(e|ing)|working hypothesis|our hypothesis|working theory|only a theory|theory right now|speculat(e|ion)|working model)\b/i.test(s) ||
    /\b(my|our)\s+guess\b/i.test(s) ||
    /\b(we|i)\s+think\b/i.test(s) ||
    /\b(we|i)\s+believe\b/i.test(s) ||
    /\b(we|i)\s+suspect\b/i.test(s) ||
    /\b(hunch|unvalidated|haven'?t validated|don'?t have evidence|no evidence yet)\b/i.test(s) ||
    /\b(seems likely|appears to be|might be|could be|probably|maybe)\b/i.test(s)
  ) {
    return EPISTEMIC_STATES.HYPOTHESIS;
  }

  // 3. Explicit UNKNOWN
  if (
    /\b(haven'?t|have not|never|hadn'?t|had not|yet to)\s+(really\s+)?(defined|established|decided|figured|worked out|tested|investigated|settled|formalized|determined|measured|tracked|thought about|created|built)\b/i.test(s) ||
    /\b(haven'?t|have not)\s+(gotten|got)\s+around\s+to\b/i.test(s) ||
    /\b(don'?t|do not|didn'?t|did not)\s+(really\s+)?(know|have|see|possess)\b/i.test(s) ||
    /\b(not\s+sure|unsure|no\s+idea|tbd|unknown|idk|unclear|hard to say|hard to tell)\b/i.test(s) ||
    /\b(couldn'?t|could not|can'?t|cannot)\s+tell\s+you\b/i.test(s) ||
    /\b(still|currently)\s+(figuring|working|deciding|determining|looking into|exploring|trying to figure)\b/i.test(s) ||
    /\b(isn'?t|is not|there is no|there isn'?t|there'?s no)\s+(enough\s+)?(evidence|data|formal|defined|established|information|answer)\b/i.test(s) ||
    /\bno\s+(formal|defined|established|set)\s+(brand\s+voice|strategy|icp|positioning|differentiation|process|guideline)\b/i.test(s) ||
    /\bthat'?s\s+still\s+something\s+we'?re\s+(figuring|working|deciding|defining)\b/i.test(s) ||
    /\bwe\s+haven'?t\s+defined\s+a\s+formal\s+brand\s+voice\s+yet\b/i.test(s)
  ) {
    if (/\b(but|except|however)\b/i.test(s)) {
      const parts = s.split(/\b(but|except|however)\b/i);
      const clausePart = parts.slice(2).join(' ').trim();
      if (clausePart && classifyEpistemicState(clausePart) === EPISTEMIC_STATES.KNOWN) {
        return EPISTEMIC_STATES.KNOWN;
      }
    }
    return EPISTEMIC_STATES.UNKNOWN;
  }

  // 4. Default for direct affirmative propositions
  return EPISTEMIC_STATES.KNOWN;
}

/**
 * Creates a canonical BusinessFact representation per SPEC-221.
 * Ensures value and epistemic state are separate canonical concerns.
 */
function createBusinessFact({
  subject,
  value = null,
  epistemicState = EPISTEMIC_STATES.UNRESOLVED,
  confidence = 0.5,
  evidence = null,
  provenance = null,
}) {
  const isAffirmative = epistemicState === EPISTEMIC_STATES.KNOWN;
  const isHypothesis = epistemicState === EPISTEMIC_STATES.HYPOTHESIS;

  return {
    subject,
    value: isAffirmative ? value : null,
    hypothesis_value: isHypothesis ? (value || evidence) : null,
    epistemic_state: epistemicState,
    confidence: Number(confidence) || 0.5,
    evidence: evidence ? String(evidence) : null,
    provenance: provenance ? String(provenance) : null,
  };
}

/**
 * Enforces persistence invariant: no stage may silently promote
 * UNKNOWN/UNRESOLVED/HYPOTHESIS/NOT_APPLICABLE into KNOWN.
 */
function preserveEpistemicState(currentFact, nextFact) {
  if (!currentFact) return nextFact;
  const currState = currentFact.epistemic_state || EPISTEMIC_STATES.UNRESOLVED;
  const nextState = nextFact.epistemic_state || EPISTEMIC_STATES.UNRESOLVED;

  if (currState !== EPISTEMIC_STATES.KNOWN && nextState === EPISTEMIC_STATES.KNOWN && !nextFact.explicitOperatorConfirmation) {
    return {
      ...nextFact,
      epistemic_state: currState,
      value: currState === EPISTEMIC_STATES.KNOWN ? nextFact.value : null,
    };
  }
  return nextFact;
}

module.exports = {
  EPISTEMIC_STATES,
  classifyEpistemicState,
  createBusinessFact,
  preserveEpistemicState,
};
