'use strict';

/**
 * SPEC-112 Phase 2 — Pain ontology Scout reasons over.
 * Problems are client-taught. Signals are observable.
 */

const { asText, asList, PAIN_CATEGORIES, PAIN_IDS, haystack } = require('./types');

function buildPainProblem(partial = {}) {
  return {
    id: asText(partial.id),
    label: asText(partial.label || partial.id),
    category: asText(partial.category),
    definition: asText(partial.definition),
    signals: asList(partial.signals).map((s) => String(s).toLowerCase()),
  };
}

function buildPainCategory(partial = {}) {
  const problems = (Array.isArray(partial.problems) ? partial.problems : []).map((p) =>
    buildPainProblem({ ...p, category: partial.id || p.category })
  );
  return {
    id: asText(partial.id),
    label: asText(partial.label || partial.id),
    problems,
  };
}

function buildPainOntology(categories = []) {
  const list = (Array.isArray(categories) ? categories : []).map(buildPainCategory);
  const byId = {};
  for (const category of list) {
    for (const problem of category.problems) {
      byId[problem.id] = problem;
    }
  }
  return { categories: list, byId };
}

function prospectBlob(prospect = {}, extraSignals = []) {
  const signalText = [
    ...(Array.isArray(prospect.signals) ? prospect.signals : []),
    ...extraSignals,
  ].map((s) => {
    if (typeof s === 'string') return s;
    return [s.type, s.kind, s.label, s.text, s.observation].filter(Boolean).join(' ');
  });
  return haystack([
    prospect.name,
    prospect.companyName,
    prospect.jobTitle,
    prospect.description,
    prospect.snippet,
    prospect.notes,
    prospect.reviewReplyPattern,
    ...signalText,
    ...(Array.isArray(prospect.observations) ? prospect.observations.map((o) => o.text || o) : []),
  ]);
}

/**
 * Match observed text/signals to the ontology.
 * Returns per-problem scores. Does not invent pain that has no signal.
 *
 * @param {object} ontology
 * @param {object} prospect
 * @param {object} [opts]
 * @returns {{ matches: object[], byId: object, topPain: object|null }}
 */
function matchPainSignals(ontology, prospect = {}, opts = {}) {
  const blob = prospectBlob(prospect, opts.signals);
  const matches = [];
  const problems = ontology && ontology.byId
    ? Object.values(ontology.byId)
    : [];

  for (const problem of problems) {
    const hits = problem.signals.filter((signal) => blob.includes(signal));
    if (!hits.length) continue;
    const density = hits.length / Math.max(problem.signals.length, 1);
    // Observable hits score high without requiring every ontology signal.
    // 5/6 founder-dependency hits ≈ 92%.
    const score = Math.min(0.97, 0.4 + density * 0.42 + Math.min(hits.length, 3) * 0.06);
    matches.push({
      id: problem.id,
      label: problem.label,
      category: problem.category,
      score,
      percent: Math.round(score * 100),
      hits,
      definition: problem.definition,
    });
  }

  matches.sort((a, b) => b.score - a.score);
  const byId = {};
  for (const row of matches) byId[row.id] = row;
  return {
    matches,
    byId,
    topPain: matches[0] || null,
  };
}

module.exports = {
  PAIN_CATEGORIES,
  PAIN_IDS,
  buildPainProblem,
  buildPainCategory,
  buildPainOntology,
  matchPainSignals,
  prospectBlob,
};
