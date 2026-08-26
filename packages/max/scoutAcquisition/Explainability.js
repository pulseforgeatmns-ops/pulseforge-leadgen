'use strict';

/**
 * SPEC-100 — operator-facing explainability for Acquisition movement.
 * Chain: Acquisition elevated → Max evaluation → Scout result → delegation → evidence.
 */

const {
  buildProvenanceChain,
  formatProvenanceNarrative,
} = require('../specialistDelegation/Provenance');
const { asText, COVERAGE_BANDS } = require('./Types');
const { investigationFromResult } = require('./InvestigationProvenance');
const {
  answerOperatorQuestion,
  deserializeGraph,
  serializeForOperator,
} = require('../../scout/explainability/ExplainabilityGraph');

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

function resolveInvestigation(input = {}) {
  return (
    input.investigation ||
    (input.state && input.state.investigation) ||
    investigationFromResult(input.result) ||
    investigationFromResult(input.reuse) ||
    null
  );
}

function resolveExplainabilityGraph(input = {}) {
  const serialized =
    input.explainabilityGraph ||
    input.result?.pipeline?.explainabilityGraph ||
    input.result?.explainabilityGraph ||
    input.state?.explainabilityGraph ||
    null;
  if (!serialized || !Array.isArray(serialized.nodes)) return null;
  return deserializeGraph(serialized);
}

function formatCognitiveExplainabilityAnswer(input = {}) {
  const question = String(input.question || '');
  const graph = resolveExplainabilityGraph(input);
  if (!graph) return null;

  const answer = answerOperatorQuestion(graph, question);
  if (answer) {
    return {
      narrative: answer,
      chain: serializeForOperator(graph),
      spec: 'SPEC-183',
    };
  }

  return {
    narrative: serializeForOperator(graph).join('\n'),
    chain: serializeForOperator(graph),
    spec: 'SPEC-183',
  };
}

function formatInvestigationAnswer(input = {}) {
  const cognitive = formatCognitiveExplainabilityAnswer(input);
  if (cognitive && /why.*(?:linkedin|provider|recommend|reject|terminology|evidence|change)/i.test(input.question || '')) {
    return cognitive.narrative;
  }

  const question = String(input.question || '');
  const investigation = resolveInvestigation(input);
  const evaluation = input.evaluation || (input.state && {
    coverageBand: input.state.coverageBand,
    coverageConfidence: input.state.coverageConfidence,
    conclusionTrust: input.state.conclusionTrust,
    marketAbsenceJustified: input.state.marketAbsenceJustified,
  }) || {};
  const coverage = investigation && investigation.coverage;
  const sources = investigation && investigation.sources;
  const band =
    (investigation && investigation.coverageBand) ||
    evaluation.coverageBand ||
    COVERAGE_BANDS.WEAK;
  const coverageConfidence =
    investigation && investigation.coverageConfidence != null
      ? investigation.coverageConfidence
      : evaluation.coverageConfidence;

  if (!investigation) {
    return 'I do not yet have durable investigation provenance for this Scout run.';
  }

  if (/\bwhat did scout(?: actually)? investigate\b/i.test(question)) {
    const geo =
      investigation.scope.geography ||
      (investigation.scope.investigatedGeography || []).join(', ') ||
      'no geography Scout successfully covered';
    const segments = (investigation.scope.segments || []).join(', ') || 'the delegated segments';
    const signals = (investigation.scope.desiredSignals || []).join(', ');
    return (
      `Scout actually investigated ${geo} for ${segments}` +
      (signals ? `, looking for ${signals.replace(/_/g, ' ')} signals` : '') +
      `. That is the scope he covered, not merely the scope that was requested` +
      (investigation.scope.requestedGeography &&
      investigation.scope.geography &&
      investigation.scope.requestedGeography !== investigation.scope.geography
        ? ` (requested ${investigation.scope.requestedGeography}).`
        : '.')
    );
  }

  if (/\bhow thorough|how (?:complete|deep) was\b/i.test(question)) {
    return (
      `Coverage was ${band}` +
      (coverageConfidence != null ? ` (${coverageConfidence})` : '') +
      `. Scout discovered ${coverage.candidatesDiscovered} and evaluated ${coverage.candidatesEvaluated} companies. ` +
      `${coverage.basicFitCount} had basic fit; ${coverage.signalBearingCount} carried a relevant signal; ` +
      `${coverage.supportedOpportunityCount} met the supported-opportunity threshold.`
    );
  }

  if (/\bwhy did (?:he|scout) find nothing\b/i.test(question)) {
    const reasons = (investigation.rejectionSummary || [])
      .map((row) => `${row.count} ${String(row.reason || '').replace(/_/g, ' ')}`)
      .join('; ');
    if (coverage && coverage.supportedOpportunityCount > 0) {
      return `Scout did find ${coverage.supportedOpportunityCount} supported opportunit${coverage.supportedOpportunityCount === 1 ? 'y' : 'ies'}.`;
    }
    return (
      `He found nothing that cleared the supported-opportunity threshold` +
      (reasons ? ` because: ${reasons}.` : '.') +
      (band === COVERAGE_BANDS.WEAK
        ? ' That is not the same as proving the market is empty.'
        : ' The search was thorough enough that I treat this as meaningful negative intelligence.')
    );
  }

  if (/\bhow many compan(?:y|ies)\b/i.test(question)) {
    return (
      `Scout discovered ${coverage.candidatesDiscovered} companies and evaluated ${coverage.candidatesEvaluated}. ` +
      `${coverage.basicFitCount} had reasonable business fit and ${coverage.supportedOpportunityCount} were supported opportunities.`
    );
  }

  if (/\bwhat eliminated\b/i.test(question)) {
    const reasons = (investigation.rejectionSummary || [])
      .map((row) => `${row.reason.replace(/_/g, ' ')}: ${row.count}`)
      .join('; ');
    const near = (investigation.nearThreshold || [])
      .slice(0, 3)
      .map((row) => `${row.company}: ${row.rejectedBecause}`)
      .join(' ');
    return (
      (reasons ? `What eliminated candidates: ${reasons}.` : 'No aggregate rejection categories were recorded.') +
      (near ? ` Near-threshold: ${near}` : '')
    );
  }

  if (/\bcoverage weak|where was\b/i.test(question)) {
    const unavailable = ((sources && sources.sourceTypesUnavailable) || []).join(', ');
    const limits = (investigation.limitations || []).slice(0, 3).join(' ');
    return (
      `Coverage was weakest where Scout lacked perception` +
      (unavailable ? ` (${unavailable.replace(/_/g, ' ')})` : '') +
      `. ${limits}`
    );
  }

  if (/\bdo you trust\b/i.test(question)) {
    if (band === COVERAGE_BANDS.STRONG) {
      return (
        `I trust Scout's conclusion about the companies he evaluated. ` +
        `Coverage was strong enough that a zero is meaningful negative intelligence, not just a missing search. ` +
        `Result confidence and coverage confidence stay separate` +
        (coverageConfidence != null ? ` — coverageConfidence is ${coverageConfidence}.` : '.')
      );
    }
    return (
      `I don't treat Scout's conclusion as a reliable market-absence claim. ` +
      `Coverage was ${band}, so a zero is an incomplete investigation rather than evidence that no opportunities exist. ` +
      `I can still trust what he reported about the companies he did evaluate.`
    );
  }

  if (/\binvestigate next\b/i.test(question)) {
    const next = [];
    const unavailable = (sources && sources.sourceTypesUnavailable) || [];
    if (unavailable.includes('linkedin_social_intelligence')) {
      next.push('LinkedIn social timing once Link is available');
    }
    if (unavailable.includes('facebook_social_intelligence')) {
      next.push('Facebook social timing once Faye is available');
    }
    if ((investigation.scope.requestedGeography || '') !== (investigation.scope.geography || '')) {
      next.push('the requested geographies Scout did not successfully cover');
    }
    if (coverage && coverage.unresolvedCount > 0) {
      next.push(`${coverage.unresolvedCount} unresolved candidates after provider failure`);
    }
    if (!next.length) {
      next.push('current vendor-need and contract-timing evidence in the same segment');
    }
    return `Next I would investigate ${next.join('; ')}.`;
  }

  return (
    `Scout evaluated ${coverage.candidatesEvaluated} of ${coverage.candidatesDiscovered} discovered companies ` +
    `with ${band} coverage. ${coverage.supportedOpportunityCount} were supported opportunities.`
  );
}

module.exports = {
  formatAcquisitionExplanation,
  formatOpportunityAnswer,
  formatInvestigationAnswer,
  formatCognitiveExplainabilityAnswer,
  resolveExplainabilityGraph,
  buildProvenanceChain,
  formatProvenanceNarrative,
};
