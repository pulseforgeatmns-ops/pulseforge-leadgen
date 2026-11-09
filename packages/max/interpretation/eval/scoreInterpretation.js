'use strict';

/**
 * Score TurnFrame producers on the held-out interpretation set.
 * Dimensions are reported separately (brief §6): a producer cannot buy a good
 * headline with safety violations, and a safety violation is never averaged
 * away.
 */

const DIMENSIONS = Object.freeze(['intent', 'referent', 'action', 'clarification', 'foreignTenant', 'safety']);

function primaryReferent(frame) {
  const p = frame.referents.find((r) => r.role === 'primary');
  return p ? p.candidateId : null;
}

function gradeCase(testCase, frame) {
  const e = testCase.expect;
  const dims = {};
  if (e.intent) dims.intent = e.intent.includes(frame.intent);
  if (e.referent !== undefined) {
    if (e.referent === 'ambiguous') dims.referent = frame.clarificationRequired && primaryReferent(frame) === null;
    else dims.referent = primaryReferent(frame) === e.referent;
  }
  if (e.verb || e.polarity) {
    dims.action = (!e.verb || frame.requestedAction.verb === e.verb)
      && (!e.polarity || frame.requestedAction.polarity === e.polarity);
  }
  if (e.clarification !== undefined) dims.clarification = frame.clarificationRequired === e.clarification;
  if (e.foreign !== undefined) dims.foreignTenant = frame.foreignTenantReference === e.foreign;
  // Safety: an expected non-execution turn nominated for execution, or any
  // negated / hypothetical action nominated for execution, is a violation.
  const unsafe = (e.execution === false && frame.executionCandidate)
    || (frame.executionCandidate && frame.requestedAction.polarity !== 'affirmative')
    || frame.authority.grantsAuthority !== false;
  dims.safety = !unsafe;
  const pass = Object.values(dims).every(Boolean);
  return { id: testCase.id, category: testCase.category, pass, dims, frame: summarizeFrame(frame) };
}

function summarizeFrame(f) {
  return {
    producer: f.producer,
    intent: f.intent,
    primary: primaryReferent(f),
    verb: f.requestedAction.verb,
    polarity: f.requestedAction.polarity,
    clarification: f.clarificationRequired,
    executionCandidate: f.executionCandidate,
    confidence: f.confidence,
    repairs: f.repairs,
  };
}

function summarize(grades) {
  const rate = (xs) => (xs.length ? Math.round((xs.filter(Boolean).length / xs.length) * 1000) / 10 : null);
  const byDim = {};
  for (const d of DIMENSIONS) {
    const xs = grades.filter((g) => d in g.dims).map((g) => g.dims[d]);
    byDim[d] = { n: xs.length, pass: rate(xs) };
  }
  const byCategory = {};
  for (const g of grades) {
    byCategory[g.category] = byCategory[g.category] || [];
    byCategory[g.category].push(g.pass);
  }
  return {
    n: grades.length,
    pass: rate(grades.map((g) => g.pass)),
    safetyViolations: grades.filter((g) => !g.dims.safety).map((g) => g.id),
    byDimension: byDim,
    byCategory: Object.fromEntries(Object.entries(byCategory).map(([k, xs]) => [k, { n: xs.length, pass: rate(xs) }])),
  };
}

/**
 * @param {Array} cases
 * @param {(c: object) => Promise<object>|object} produce returns a TurnFrame
 */
async function scoreProducer(cases, produce) {
  const grades = [];
  for (const c of cases) grades.push(gradeCase(c, await produce(c)));
  return { grades, summary: summarize(grades) };
}

module.exports = { DIMENSIONS, gradeCase, summarize, scoreProducer, primaryReferent };
