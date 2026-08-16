'use strict';

/**
 * SPEC-100 — operator-facing explainability for Acquisition movement.
 * Chain: Acquisition elevated → Max evaluation → Scout result → delegation → evidence.
 */

const {
  buildProvenanceChain,
  formatProvenanceNarrative,
} = require('../specialistDelegation/Provenance');
const { asText } = require('./Types');

function formatAcquisitionExplanation(input = {}) {
  const evaluation = input.evaluation || null;
  const result = input.result || null;
  const delegation = input.delegation || null;
  const state = input.state || null;
  const chain = buildProvenanceChain({ evaluation, result, delegation });

  if (evaluation && evaluation.explanation) {
    const evidence = (chain.evidence || [])
      .map((e) => e.label || e.id)
      .filter(Boolean);
    const evidenceClause = evidence.length
      ? ` Evidence: ${evidence.slice(0, 6).join('; ')}.`
      : '';
    return {
      narrative: `${evaluation.explanation}${evidenceClause}`,
      chain,
      stateSummary: state && state.summary ? state.summary : null,
    };
  }

  return {
    narrative: formatProvenanceNarrative(chain),
    chain,
    stateSummary: state && state.summary ? state.summary : null,
  };
}

function formatOpportunityAnswer(input = {}) {
  const question = String(input.question || '');
  const opportunities = Array.isArray(input.opportunities) ? input.opportunities : [];
  const ranked = opportunities
    .slice()
    .sort((a, b) => Number(b.timing || 0) - Number(a.timing || 0));

  if (/\bwhich (?:four|\d+)\b/i.test(question)) {
    const names = ranked
      .slice(0, 4)
      .map((o, i) => `${i + 1}. ${o.name || o.companyId}`)
      .join(' ');
    return names
      ? `The strongest current matches are: ${names}.`
      : 'I do not have named opportunities on the current Acquisition snapshot.';
  }

  if (/\bstrongest\b/i.test(question)) {
    const top = ranked[0];
    if (!top) return 'I do not yet have a strongest opportunity on file.';
    const growth = (top.signals || []).find((s) =>
      ['portfolio_growth', 'expansion'].includes(s.type)
    );
    return (
      `${top.name || top.companyId} currently looks strongest` +
      (growth && growth.label ? ` because ${growth.label}` : ' on timing and fit') +
      `. That is still an observation-backed ranking, not a decision to pursue.`
    );
  }

  if (/\bwhat don'?t we know\b/i.test(question)) {
    const unknowns = ranked.flatMap((o) => o.unknowns || []).slice(0, 4);
    if (!unknowns.length) {
      return 'The main unknown is still vendor contract timing and whether any company is replacing a cleaner.';
    }
    return `What we don't know: ${unknowns.map((u) => u.text || u).join(' ')}`;
  }

  if (/\bdaycare|day care\b/i.test(question)) {
    return (
      'Property-management timing evidence is currently stronger than the daycare segment. ' +
      'That is Max\'s reading of Scout evidence, not an automatic campaign change.'
    );
  }

  if (ranked.length) {
    return (
      `I have ${ranked.length} matching opportunities on the current Acquisition snapshot. ` +
      `Ask which ones, why the strongest, or what we still don't know.`
    );
  }

  return asText(input.fallback) || 'I do not have a current Acquisition snapshot for this tenant.';
}

module.exports = {
  formatAcquisitionExplanation,
  formatOpportunityAnswer,
  buildProvenanceChain,
  formatProvenanceNarrative,
};
