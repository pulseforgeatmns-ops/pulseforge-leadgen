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
  const context = String(
    opts.questionContext ||
      opts.context ||
      (opts.activeQuestion && (opts.activeQuestion.section || opts.activeQuestion.id)) ||
      ''
  ).toLowerCase();

  const isEmployeeContext = /employee|staff|workforce|headcount|team|hiring/i.test(context);

  const explicitNotApplicable =
    /\b(?:not\s+applicable|doesn'?t apply|does not apply|not relevant|not something we do)\b/i.test(s) ||
    /\b(?:no\s+(?:employees|locations|offices|stores|consumers|staff|workers))\b/i.test(s);

  const noEmployeeStatement = /^(?:we\s+)?(?:don'?t|do not|no|without)\s+(?:have|need|use|plan\s+to\s+hire|plan on hiring|want)\s+(?:any\s+)?(?:employees|staff|workers|contractors|team\s+members)\b/i;
  const noEmployeeWithHireIntent = /\b(?:don'?t|do not|no)\s+(?:have|plan\s+to\s+hire|plan on hiring)\s+(?:any\s+)?(?:employees|staff|workers|contractors|team\s+members)\b/i;

  if (explicitNotApplicable) {
    return EPISTEMIC_STATES.NOT_APPLICABLE;
  }

  if (noEmployeeStatement.test(s) || noEmployeeWithHireIntent.test(s)) {
    if (isEmployeeContext) {
      return EPISTEMIC_STATES.NOT_APPLICABLE;
    }
    return EPISTEMIC_STATES.KNOWN;
  }

  // Hypothesis is a claim about a proposition that is not yet validated.
  if (
    /\b(working assumption|our assumption|my assumption|assumption|working hypothesis|our hypothesis|working theory|only a theory|theory right now|speculat(e|ion)|working model|working premise)\b/i.test(s) ||
    /\b(my|our)\s+guess\b/i.test(s) ||
    /\b(we|i)\s+think\b/i.test(s) ||
    /\b(we|i)\s+believe\b/i.test(s) ||
    /\b(we|i)\s+suspect\b/i.test(s) ||
    /\b(hunch|unvalidated|haven'?t validated|haven'?t confirmed|don'?t have evidence|no evidence yet)\b/i.test(s) ||
    /\b(seems likely|appears to be|might be|could be|probably|maybe|perhaps)\b/i.test(s) ||
    /\b(?:but|however)\b.*\b(?:haven'?t|have not)\s+(?:validated|tested|confirmed|proven|verified)\b/i.test(s)
  ) {
    return EPISTEMIC_STATES.HYPOTHESIS;
  }

  // Explicit uncertainty is about not knowing or not having established the business fact.
  if (
    /\b(?:haven'?t|have not|never|hadn'?t|had not|yet to)\s+(?:really\s+)?(?:defined|established|decided|figured|worked out|tested|investigated|settled|formalized|determined|measured|tracked|thought about|created|built|validated|confirmed|verified)\b/i.test(s) ||
    /\b(?:haven'?t|have not)\s+(?:gotten|got)\s+around\s+to\b/i.test(s) ||
    /\b(?:don'?t|do not|didn'?t|did not)\s+(?:really\s+)?(?:know|understand|see|possess)\b/i.test(s) ||
    /\b(?:don'?t|do not|didn'?t|did not)\s+know\s+which\b/i.test(s) ||
    /\b(?:don'?t|do not|didn'?t|did not)\s+have\s+(?:enough\s+)?(?:evidence|data|information)\b/i.test(s) ||
    /\b(?:not\s+sure|unsure|no\s+idea|tbd|unknown|idk|unclear|hard to say|hard to tell)\b/i.test(s) ||
    /\b(?:couldn'?t|could not|can'?t|cannot)\s+tell\s+you\b/i.test(s) ||
    /\b(?:still|currently)\s+(?:figuring|working|deciding|determining|looking into|exploring|trying to figure)\b/i.test(s) ||
    /\b(?:isn'?t|is not|there is no|there isn'?t|there'?s no)\s+(?:enough\s+)?(?:evidence|data|formal|defined|established|information|answer)\b/i.test(s) ||
    /\bno\s+(?:formal|defined|established|set)\s+(?:brand\s+voice|strategy|icp|positioning|differentiation|process|guideline)\b/i.test(s) ||
    /\bthat'?s\s+still\s+something\s+we'?re\s+(?:figuring|working|deciding|defining)\b/i.test(s) ||
    /\bwe\s+haven'?t\s+defined\s+a\s+formal\s+brand\s+voice\s+yet\b/i.test(s)
  ) {
    if (/\b(?:but|except|however)\b/i.test(s)) {
      const parts = s.split(/\b(?:but|except|however)\b/i);
      const clausePart = parts.slice(2).join(' ').trim();
      if (clausePart && classifyEpistemicState(clausePart, opts) === EPISTEMIC_STATES.KNOWN) {
        return EPISTEMIC_STATES.KNOWN;
      }
    }
    return EPISTEMIC_STATES.UNKNOWN;
  }

  // Negative business preference/exclusion is a known business fact, not epistemic uncertainty.
  const isKnownPreference =
    /\b(?:don'?t\s+want\s+to\s+work\s+with|do\s+not\s+want\s+to\s+work\s+with|don'?t\s+want\s+to|do\s+not\s+want\s+to|prefer\s+not\s+to|avoid|avoids?|exclude|excludes?|not\s+interested\s+in|not\s+targeting|less\s+focused\s+on|not\s+a\s+fit|not\s+looking\s+for)\b/i.test(s) &&
    !/\b(?:don'?t|do not|didn'?t|did not)\s+(?:really\s+)?(?:know|understand|have\s+the\s+answer)\b/i.test(s) &&
    !/\b(?:not\s+sure|unsure|no\s+idea|unknown|tbd)\b/i.test(s) &&
    !/\b(?:haven'?t|have not|never)\s+(?:defined|established|decided|figured|validated|confirmed|tested)\b/i.test(s);

  if (isKnownPreference) {
    return EPISTEMIC_STATES.KNOWN;
  }

  // Direct affirmative propositions remain known facts.
  return EPISTEMIC_STATES.KNOWN;
}

/**
 * Creates a canonical BusinessFact representation per SPEC-221.
 * Ensures value and epistemic state are separate canonical concerns.
 */
function createBusinessFact({
  id = null,
  subject,
  value = null,
  epistemicState = EPISTEMIC_STATES.UNRESOLVED,
  confidence = 0.5,
  evidence = null,
  provenance = null,
  relation = null,
  supersedes = null,
}) {
  const isAffirmative = epistemicState === EPISTEMIC_STATES.KNOWN;
  const isHypothesis = epistemicState === EPISTEMIC_STATES.HYPOTHESIS;

  return {
    id,
    subject,
    value: isAffirmative ? value : null,
    hypothesis_value: isHypothesis ? (value || evidence) : null,
    epistemic_state: epistemicState,
    confidence: Number(confidence) || 0.5,
    evidence: evidence ? String(evidence) : null,
    provenance: provenance ? String(provenance) : null,
    relation: relation || null,
    supersedes: supersedes || null,
  };
}

function factId(subject, evidence, position) {
  const basis = `${subject}|${evidence}|${position}`;
  let hash = 2166136261;
  for (let index = 0; index < basis.length; index += 1) {
    hash ^= basis.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return `bf_${(hash >>> 0).toString(36)}`;
}

function evidencePhrase(text, match, fallback) {
  if (!match || match.index == null) return fallback;
  const start = Math.max(0, text.lastIndexOf('.', match.index - 1) + 1);
  const endMarker = text.indexOf('.', match.index + match[0].length);
  return text.slice(start, endMarker >= 0 ? endMarker + 1 : text.length).trim() || fallback;
}

/**
 * Extract independently meaningful business propositions from a complete
 * operator utterance. This is deliberately not sentence splitting: a single
 * utterance is inspected for semantic assertions and their relations before
 * each proposition receives an epistemic classification.
 */
function extractBusinessFacts(text, opts = {}) {
  const raw = String(text || '').trim();
  if (!raw) return [];
  const section = String(opts.section || opts.questionContext || '').trim();
  const provenance = opts.provenance || null;
  const facts = [];
  const add = (subject, state, value, match = null, relation = null) => {
    const evidence = evidencePhrase(raw, match, raw);
    const id = factId(subject, evidence, facts.length);
    if (facts.some((fact) => fact.subject === subject && fact.evidence === evidence)) return;
    facts.push(createBusinessFact({
      id,
      subject,
      value,
      epistemicState: state,
      confidence: state === EPISTEMIC_STATES.UNKNOWN ? 0.98 : 0.7,
      evidence,
      provenance,
      relation,
    }));
  };

  if (section === 'competitiveAdvantages' || section === 'differentiation') {
    const unknown = /(?:we\s+(?:do\s+not|don't)\s+know|not\s+yet\s+(?:known|established)|haven't\s+(?:investigated|established))[^.?!]*/ig;
    for (const match of raw.matchAll(unknown)) {
      add('customer_buying_reason', EPISTEMIC_STATES.UNKNOWN, null, match);
    }
    const evidenceState = /(?:insufficient|not\s+enough|sufficient)\s+(?:(?:[\w.-]+\s+){0,6})?(?:evidence|data)[^.?!]*/ig;
    for (const match of raw.matchAll(evidenceState)) {
      add(
        'customer_buying_reason_evidence_state',
        EPISTEMIC_STATES.KNOWN,
        match[0].replace(/^\s*/g, ''),
        match,
        'evidence_state'
      );
    }
    const hypothesis = /(?:current\s+)?(?:working\s+)?hypothesis\s+is\s+(?:that\s+)?([^.?!]+)|(?:we\s+think|we\s+believe)\s+([^.?!]+)/ig;
    for (const match of raw.matchAll(hypothesis)) {
      const value = (match[1] || match[2] || '').trim();
      if (value) add('candidate_customer_buying_reason', EPISTEMIC_STATES.HYPOTHESIS, value, match, 'candidate');
    }
    const validation = /(?:hypothesis|theory)[^.?!]*(?:remain(?:s)?\s+)?unvalidated|unvalidated[^.?!]*/ig;
    for (const match of raw.matchAll(validation)) {
      add('candidate_customer_buying_reason_validation', EPISTEMIC_STATES.KNOWN, match[0].trim(), match, 'validation_constraint');
    }
    const objective = /(?:sales\s+conversations?|discovery)[^.?!]*(?:intended|will|should)[^.?!]*(?:discover|learn)[^.?!]*/ig;
    for (const match of raw.matchAll(objective)) {
      add('customer_buying_reason_validation_objective', EPISTEMIC_STATES.KNOWN, match[0].trim(), match, 'validation_objective');
    }
  }

  if (!facts.length) {
    const subject = opts.subject || section || 'business_understanding';
    const state = classifyEpistemicState(raw, opts);
    add(subject, state, raw);
  }
  return facts;
}

function projectBusinessFacts(facts = [], fieldKey) {
  const list = Array.isArray(facts) ? facts : [];
  const unknown = list.find((fact) => fact.subject === 'customer_buying_reason' && fact.epistemic_state === EPISTEMIC_STATES.UNKNOWN);
  const hypothesis = list.find((fact) => fact.epistemic_state === EPISTEMIC_STATES.HYPOTHESIS);
  const known = list.find((fact) => fact.epistemic_state === EPISTEMIC_STATES.KNOWN && fact.value);
  const primary = unknown || hypothesis || known || list[0] || null;
  return {
    epistemicState: primary ? primary.epistemic_state : EPISTEMIC_STATES.UNRESOLVED,
    value: primary && primary.epistemic_state === EPISTEMIC_STATES.KNOWN ? primary.value : null,
    hypothesisValue: hypothesis ? hypothesis.hypothesis_value : null,
    evidence: primary ? primary.evidence : null,
    fieldKey,
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
  extractBusinessFacts,
  projectBusinessFacts,
};
