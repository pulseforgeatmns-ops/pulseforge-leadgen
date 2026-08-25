'use strict';

/**
 * SPEC-158 — Hypothesis-Driven Discovery orchestrator.
 * Mission → Market Definition → Hypotheses → Investigation Branches → Evidence
 */

const { expandCitiesFromSearchDefinition } = require('./DiscoveryCoverageEngine');
const { asText, SOURCE_TYPES } = require('../../max/scoutAcquisition/Types');
const {
  generateInitialSearchHypotheses,
  evaluateHypothesisBranch,
  generateFollowUpHypotheses,
  shouldContinueHypothesisInvestigation,
  inferTerminologyRevision,
  DEFAULT_RESULT_THRESHOLD,
} = require('../investigation/SearchHypothesisEngine');
const {
  createInvestigationTree,
  addHypothesisBranch,
  recordBranchEvidence,
  spawnBranchFromFailure,
  setFinalUnderstanding,
  serializeInvestigationTree,
} = require('../investigation/InvestigationTree');
const { reviseMarketDefinition } = require('../intelligence/MarketDefinition');
const {
  recordTerminologyPerformance,
  exportTerminologyLearning,
} = require('../memory/TerminologyLearning');

function defaultEnabledSources(adapters = []) {
  const sources = new Set();
  for (const adapter of adapters || []) {
    if (!adapter) continue;
    if (typeof adapter.available === 'function' && !adapter.available()) continue;
    if (adapter.sourceType) sources.add(adapter.sourceType);
  }
  if (!sources.size) sources.add(SOURCE_TYPES.PUBLIC_BUSINESS_DATA);
  return [...sources];
}

function adapterForSource(adapters, sourceType) {
  return (adapters || []).find((row) => row && row.sourceType === sourceType) || null;
}

function scopedSearchDefinition(searchDefinition, workload) {
  const cityToken = asText(workload.city).split(/\s+/)[0];
  return {
    ...searchDefinition,
    geography: {
      ...(searchDefinition.geography || {}),
      label: workload.city,
      cities: [cityToken],
    },
    segments: [workload.concept],
    _coverageWorkload: workload,
    _hypothesisId: workload.hypothesisId,
    _branchId: workload.branchId,
  };
}

function buildWorkloadsForHypothesis(hypothesis, cities, sources, branchId) {
  const workloads = [];
  for (const city of cities) {
    for (const term of hypothesis.searchTerms || []) {
      for (const source of sources) {
        workloads.push({
          id: `${branchId}|${city}|${term}|${source}`,
          city,
          concept: term,
          source,
          hypothesisId: hypothesis.id,
          branchId,
        });
      }
    }
  }
  return workloads;
}

async function executeWorkloads(workloads, searchDefinition, adapters) {
  const executed = [];
  const candidates = [];
  const errors = [];
  const marketAdapters = (adapters || []).filter((row) => row && row.id !== 'existing_pf');

  for (const workload of workloads) {
    const adapter = adapterForSource(marketAdapters, workload.source);
    if (!adapter || (typeof adapter.available === 'function' && !adapter.available())) {
      executed.push({ ...workload, status: 'skipped', reason: 'adapter_unavailable', resultCount: 0 });
      continue;
    }

    const scoped = scopedSearchDefinition(searchDefinition, workload);
    try {
      const report = await adapter.discover(scoped);
      const rows = report.candidates || [];
      executed.push({ ...workload, status: 'executed', resultCount: rows.length });
      for (const row of rows) {
        candidates.push({
          ...row,
          _coverageWorkload: workload,
          discoveryConcept: workload.concept,
          discoveryCity: workload.city,
          discoverySource: workload.source,
          hypothesisId: workload.hypothesisId,
          branchId: workload.branchId,
        });
      }
      if (report.errors && report.errors.length) {
        errors.push(...report.errors.map((err) => ({ ...err, workload })));
      }
    } catch (err) {
      executed.push({
        ...workload,
        status: 'failed',
        reason: err.message || String(err),
        resultCount: 0,
      });
      errors.push({ code: 'provider_error', message: err.message || String(err), workload });
    }
  }

  return { executed, candidates, errors };
}

function dominantConceptFromCandidates(candidates = []) {
  const counts = new Map();
  for (const row of candidates) {
    const concept = asText(row.discoveryConcept || row._coverageWorkload?.concept);
    if (!concept) continue;
    counts.set(concept, (counts.get(concept) || 0) + 1);
  }
  let best = null;
  let bestCount = 0;
  for (const [concept, count] of counts.entries()) {
    if (count > bestCount) {
      best = concept;
      bestCount = count;
    }
  }
  return best;
}

/**
 * Execute hypothesis-driven discovery investigation.
 * @param {object} input
 * @returns {Promise<object>}
 */
async function executeHypothesisDrivenCoverage(input = {}) {
  const marketDefinition = input.marketDefinition || {};
  const searchDefinition = input.searchDefinition || {};
  const adapters = input.adapters || [];
  const opts = input.opts || {};
  const threshold = opts.resultThreshold != null ? opts.resultThreshold : DEFAULT_RESULT_THRESHOLD;

  const cities = expandCitiesFromSearchDefinition(searchDefinition);
  const sources = opts.enabledSources || defaultEnabledSources(adapters);
  const externalSources = sources.filter((src) => src !== SOURCE_TYPES.EXISTING_PF);

  let learningStore = input.terminologyLearningStore || new Map();
  const terminologyLearning = opts.terminologyLearning || [];

  const initialHypotheses = generateInitialSearchHypotheses(marketDefinition, {
    terminologyLearning,
  });
  const tree = createInvestigationTree(marketDefinition, initialHypotheses);

  const allCandidates = [];
  const allExecuted = [];
  const allErrors = [];
  const evaluatedHypotheses = [];
  let pendingHypotheses = initialHypotheses.slice();
  let lastBranchId = null;

  while (pendingHypotheses.length) {
    const hypothesis = pendingHypotheses.shift();
    const branch = addHypothesisBranch(tree, hypothesis, { parentBranchId: lastBranchId });
    branch.status = 'executing';
    branch.startedAt = new Date().toISOString();

    const workloads = buildWorkloadsForHypothesis(hypothesis, cities, externalSources, branch.id);
    const { executed, candidates, errors } = await executeWorkloads(
      workloads,
      searchDefinition,
      adapters
    );

    allExecuted.push(...executed);
    allErrors.push(...errors);

    const branchCandidates = [];
    const seen = new Set(allCandidates.map((c) => asText(c.id || c.name).toLowerCase()));
    for (const row of candidates) {
      const key = asText(row.id || row.name).toLowerCase();
      branchCandidates.push(row);
      if (!seen.has(key)) {
        seen.add(key);
        allCandidates.push(row);
      }
    }

    const resultCount = branchCandidates.length;
    const uniqueCandidates = new Set(branchCandidates.map((c) => asText(c.id || c.name))).size;
    const dominantConcept = dominantConceptFromCandidates(branchCandidates);

    const evaluated = evaluateHypothesisBranch(hypothesis, {
      resultCount,
      uniqueCandidates,
      dominantConcept,
    }, { resultThreshold: threshold });

    evaluatedHypotheses.push(evaluated);
    const hypIdx = tree.hypotheses.findIndex((h) => h.id === hypothesis.id);
    if (hypIdx >= 0) tree.hypotheses[hypIdx] = evaluated;

    recordBranchEvidence(tree, branch.id, {
      resultCount,
      uniqueCandidates,
      dominantConcept,
      confidence: evaluated.confidence,
      hypothesisStatus: evaluated.status,
    });

    for (const term of hypothesis.searchTerms || []) {
      learningStore = recordTerminologyPerformance(learningStore, {
        geography: marketDefinition.geography,
        terminology: term,
        resultCount,
        confidence: evaluated.confidence,
        reason: `"${term}" returned ${resultCount} results in ${marketDefinition.geography || 'market'}`,
      });
    }

    lastBranchId = branch.id;

    if (!shouldContinueHypothesisInvestigation(evaluatedHypotheses, allCandidates.length, opts)) {
      break;
    }

    if (evaluated.status !== 'confirmed') {
      const followUps = generateFollowUpHypotheses(marketDefinition, evaluatedHypotheses, opts);
      for (const followUp of followUps) {
        spawnBranchFromFailure(tree, branch.id, followUp);
        pendingHypotheses.push(followUp);
      }
    }
  }

  let revisedMarketDefinition = marketDefinition;
  const revision = inferTerminologyRevision(evaluatedHypotheses);
  if (revision) {
    revisedMarketDefinition = reviseMarketDefinition(marketDefinition, revision);
  }

  const confirmed = evaluatedHypotheses.filter((h) => h.status === 'confirmed');
  const best = confirmed.sort((a, b) => (b.confidence || 0) - (a.confidence || 0))[0];

  setFinalUnderstanding(tree, {
    market: revisedMarketDefinition.market,
    geography: revisedMarketDefinition.geography,
    dominantTerminology: best?.searchTerms?.[0] || revision?.dominantTerminology || null,
    totalCandidates: allCandidates.length,
    revisedMarketDefinition,
    summary: best
      ? `Market best described through: ${best.text} (confidence ${best.confidence}).`
      : `Investigation explored ${evaluatedHypotheses.length} terminology hypotheses; ${allCandidates.length} candidates found.`,
  });

  const conceptsSearched = new Set(allExecuted.filter((e) => e.status === 'executed').map((e) => e.concept));
  const citiesSearched = new Set(allExecuted.filter((e) => e.status === 'executed').map((e) => e.city));
  const sourcesSearched = new Set(allExecuted.filter((e) => e.status === 'executed').map((e) => e.source));

  const discoveryPlan = {
    missionGeography: searchDefinition.geography && searchDefinition.geography.label,
    cities,
    concepts: [...conceptsSearched],
    sources: externalSources,
    workloads: allExecuted,
    totals: {
      cities: cities.length,
      concepts: conceptsSearched.size,
      sources: externalSources.length,
      searches: allExecuted.length,
    },
    hypothesisDriven: true,
  };

  const coverage = {
    cities: { searched: citiesSearched.size, planned: cities.length, ratio: cities.length ? citiesSearched.size / cities.length : 1 },
    concepts: { searched: conceptsSearched.size, planned: conceptsSearched.size || 1, ratio: 1 },
    sources: { searched: sourcesSearched.size, planned: externalSources.length, ratio: externalSources.length ? sourcesSearched.size / externalSources.length : 1 },
    searches: { executed: allExecuted.filter((e) => e.status === 'executed').length, addressed: allExecuted.length, planned: allExecuted.length, ratio: 1 },
    complete: true,
    warnings: [],
  };

  return {
    candidates: allCandidates,
    executed: allExecuted,
    coverage,
    errors: allErrors,
    discoveryPlan,
    investigationTree: tree,
    investigationReport: serializeInvestigationTree(tree),
    searchHypotheses: evaluatedHypotheses,
    revisedMarketDefinition,
    terminologyLearning: exportTerminologyLearning(learningStore),
  };
}

module.exports = {
  executeHypothesisDrivenCoverage,
  buildWorkloadsForHypothesis,
};
