'use strict';

/**
 * SPEC-153 — Discovery Coverage Engine (ADR-076: Coverage Before Conclusion).
 * Scout proves the market was investigated before concluding what exists within it.
 *
 * Mission → Discovery Strategy → Coverage Plan → Source Execution → Candidate Universe
 */

const { expandGeography } = require('../../acquisition-mission/MissionPlanner');
const { MANCHESTER_GEO } = require('../../capabilities/discovery/seedProfiles');
const { expandConcepts } = require('./ConceptLibrary');
const { asText, nowIso, SOURCE_TYPES } = require('../../max/scoutAcquisition/Types');
const { discoverCandidates } = require('../../max/scoutAcquisition/DiscoveryAdapters');
const {
  computeCoverageFromEstimate,
  normalizeCandidateUniverseEstimate,
  extractExpectedValue,
} = require('../universe/CandidateUniverseEstimate');

const ORIGINS = Object.freeze({
  EXISTING_INTELLIGENCE: 'existing_intelligence',
  EXTERNAL_DISCOVERY: 'external_discovery',
  MISSION_MEMORY: 'mission_memory',
  IMPORTED: 'imported',
});

function clamp01(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.min(1, n));
}

/**
 * Expand mission geography into per-city search workloads. Geography is never executed literally.
 * @param {object} searchDefinition
 * @returns {string[]}
 */
function expandCitiesFromSearchDefinition(searchDefinition = {}) {
  const geo = searchDefinition.geography || {};
  const label = asText(geo.label);
  if (!label) return [];

  if (/greater\s+manchester/i.test(label)) {
    const state = geo.state || 'NH';
    return MANCHESTER_GEO.cities.map((city) => formatCityState(city, state));
  }

  // Single-city missions execute literally; cluster expansion requires "Greater Manchester".
  if (!/greater/i.test(label)) {
    if (Array.isArray(geo.cities) && geo.cities.length === 1) {
      return [formatCityState(geo.cities[0], geo.state || inferStateFromLabel(label))];
    }
    return [label];
  }

  const expanded = expandGeography(label, label);
  if (/greater\s+manchester/i.test(expanded.region || '')) {
    const state = geo.state || 'NH';
    return MANCHESTER_GEO.cities.map((city) => formatCityState(city, state));
  }
  if (expanded.cities && expanded.cities.length > 1) {
    const state = geo.state || inferStateFromLabel(label) || 'NH';
    return dedupeCities(expanded.cities.map((city) => formatCityState(city, state)));
  }

  const baseCities = Array.isArray(geo.cities) && geo.cities.length ? geo.cities.slice() : [];
  const nearby = Array.isArray(geo.permittedNearby) ? geo.permittedNearby.slice() : [];
  const merged = [...new Set([...baseCities, ...nearby])];
  if (merged.length > 1) {
    const state = geo.state || inferStateFromLabel(label);
    return dedupeCities(merged.map((city) => formatCityState(city, state)));
  }

  if (merged.length === 1) {
    const state = geo.state || inferStateFromLabel(label);
    return [formatCityState(merged[0], state)];
  }

  return [label];
}

function inferStateFromLabel(label) {
  const text = asText(label);
  if (/\bNH\b|New Hampshire/i.test(text)) return 'NH';
  if (/\bTN\b|Tennessee/i.test(text)) return 'TN';
  if (/\bWV\b|West Virginia/i.test(text)) return 'WV';
  if (/\bRI\b|Rhode Island/i.test(text)) return 'RI';
  return null;
}

function formatCityState(city, state) {
  const name = asText(city);
  if (!name) return '';
  if (/\b[A-Z]{2}\b/.test(name)) return name;
  return state ? `${name} ${state}` : name;
}

function dedupeCities(cities) {
  const seen = new Set();
  const out = [];
  for (const city of cities) {
    const key = city.toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(city);
  }
  return out;
}

function defaultEnabledSources(adapters = []) {
  const sources = new Set();
  for (const adapter of adapters || []) {
    if (!adapter) continue;
    if (typeof adapter.available === 'function' && !adapter.available()) continue;
    if (adapter.sourceType) sources.add(adapter.sourceType);
  }
  if (!sources.size) {
    sources.add(SOURCE_TYPES.PUBLIC_BUSINESS_DATA);
  }
  return [...sources];
}

function adapterForSource(adapters, sourceType) {
  return (adapters || []).find((row) => row && row.sourceType === sourceType) || null;
}

/**
 * Build a structured coverage plan: City × Concept × Source workloads.
 * @param {object} searchDefinition
 * @param {object} [opts]
 * @returns {object}
 */
function buildDiscoveryPlan(searchDefinition = {}, opts = {}) {
  const cities = expandCitiesFromSearchDefinition(searchDefinition);
  const concepts = expandConcepts(searchDefinition, opts.marketDefinition);
  const adapters = opts.adapters || [];
  const enabledSources = opts.enabledSources || defaultEnabledSources(adapters);
  const externalSources = enabledSources.filter((src) => src !== SOURCE_TYPES.EXISTING_PF);

  const workloads = [];
  for (const city of cities) {
    for (const concept of concepts) {
      for (const source of externalSources) {
        workloads.push({
          id: `${city}|${concept}|${source}`,
          city,
          concept,
          source,
        });
      }
    }
  }

  return {
    missionGeography: searchDefinition.geography && searchDefinition.geography.label,
    cities,
    concepts,
    sources: externalSources,
    workloads,
    totals: {
      cities: cities.length,
      concepts: concepts.length,
      sources: externalSources.length,
      searches: workloads.length,
    },
  };
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
  };
}

/**
 * Execute every workload in the coverage plan. One source returning zero does not stop others.
 * @returns {Promise<object>}
 */
async function executeCoveragePlan(plan, searchDefinition, adapters = [], opts = {}) {
  const executed = [];
  const errors = [];
  const candidates = [];
  const sourceTypesChecked = [];
  const sourceTypesUnavailable = [];

  const marketAdapters = (adapters || []).filter((row) => row && row.id !== 'existing_pf');

  for (const workload of plan.workloads || []) {
    const adapter = adapterForSource(marketAdapters, workload.source);
    if (!adapter || (typeof adapter.available === 'function' && !adapter.available())) {
      sourceTypesUnavailable.push(workload.source);
      executed.push({
        ...workload,
        status: 'skipped',
        reason: 'adapter_unavailable',
        resultCount: 0,
      });
      continue;
    }

    const scoped = scopedSearchDefinition(searchDefinition, workload);
    try {
      const report = await adapter.discover(scoped);
      const rows = report.candidates || [];
      executed.push({
        ...workload,
        status: 'executed',
        resultCount: rows.length,
      });
      if (!sourceTypesChecked.includes(workload.source)) {
        sourceTypesChecked.push(workload.source);
      }
      for (const row of rows) {
        candidates.push({
          ...row,
          _coverageWorkload: workload,
          discoveryConcept: workload.concept,
          discoveryCity: workload.city,
          discoverySource: workload.source,
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
      errors.push({
        code: 'provider_error',
        message: err.message || String(err),
        workload,
      });
    }
  }

  const coverage = computeCoverageMetrics(plan, executed);
  return {
    candidates,
    executed,
    coverage,
    errors,
    sourceTypesChecked: [...new Set(sourceTypesChecked)],
    sourceTypesUnavailable: [...new Set(sourceTypesUnavailable)],
    discoveryPlan: plan,
  };
}

/**
 * Fallback single-pass discovery when coverage engine is disabled.
 */
async function executeLegacyDiscovery(searchDefinition, adapters) {
  return discoverCandidates(searchDefinition, adapters);
}

function computeCoverageMetrics(plan, executed = []) {
  const planned = plan.totals || { cities: 0, concepts: 0, sources: 0, searches: 0 };
  const addressed = executed.filter((row) => row.status === 'executed' || row.status === 'skipped');
  const citiesSearched = new Set(
    addressed.filter((row) => row.status === 'executed').map((row) => row.city)
  );
  const conceptsSearched = new Set(
    addressed.filter((row) => row.status === 'executed').map((row) => row.concept)
  );
  const sourcesSearched = new Set(
    addressed.filter((row) => row.status === 'executed').map((row) => row.source)
  );
  const searchesExecuted = executed.filter((row) => row.status === 'executed').length;

  const ratio = (searched, total) => (total > 0 ? searched / total : 1);
  const cityRatio = ratio(citiesSearched.size, planned.cities);
  const conceptRatio = ratio(conceptsSearched.size, planned.concepts);
  const sourceRatio = ratio(sourcesSearched.size, planned.sources);
  const searchRatio = ratio(addressed.length, planned.searches);

  const complete =
    planned.searches === 0 ||
    (addressed.length >= planned.searches &&
      citiesSearched.size >= planned.cities &&
      conceptsSearched.size >= planned.concepts);

  return {
    cities: { searched: citiesSearched.size, planned: planned.cities, ratio: cityRatio },
    concepts: { searched: conceptsSearched.size, planned: planned.concepts, ratio: conceptRatio },
    sources: { searched: sourcesSearched.size, planned: planned.sources, ratio: sourceRatio },
    searches: { executed: searchesExecuted, addressed: addressed.length, planned: planned.searches, ratio: searchRatio },
    complete,
    warnings: buildCoverageWarnings({
      cities: { searched: citiesSearched.size, planned: planned.cities },
      concepts: { searched: conceptsSearched.size, planned: planned.concepts },
      sources: { searched: sourcesSearched.size, planned: planned.sources },
      searches: { addressed: addressed.length, planned: planned.searches },
      complete,
    }),
  };
}

function buildCoverageWarnings(metrics) {
  const warnings = [];
  if (metrics.complete) return warnings;
  if (metrics.cities.searched < metrics.cities.planned) {
    warnings.push(
      `Only ${metrics.cities.searched} / ${metrics.cities.planned} cities searched.`
    );
  }
  if (metrics.concepts.searched < metrics.concepts.planned) {
    warnings.push(
      `Only ${metrics.concepts.searched} / ${metrics.concepts.planned} concepts searched.`
    );
  }
  if (metrics.sources.searched < metrics.sources.planned) {
    warnings.push(
      `Only ${metrics.sources.searched} / ${metrics.sources.planned} sources executed.`
    );
  }
  if (metrics.searches.addressed < metrics.searches.planned) {
    warnings.push(
      `Only ${metrics.searches.addressed} / ${metrics.searches.planned} planned searches addressed.`
    );
  }
  warnings.push('Discovery incomplete.');
  warnings.push('Recommendation: Continue investigation.');
  return warnings;
}

function buildCandidateUniverseRecords(candidates = [], opts = {}) {
  const seeded = opts.seeded || [];
  const records = [];
  const seen = new Set();

  for (const row of seeded) {
    const id = asText(row.id || row.companyId || row.candidate_id);
    if (!id || seen.has(id)) continue;
    seen.add(id);
    records.push({
      candidate_id: id,
      origin: row.origin || ORIGINS.EXISTING_INTELLIGENCE,
      sources: [row.source || row.discoverySource || SOURCE_TYPES.EXISTING_PF],
      cities: [row.location || row.discoveryCity].filter(Boolean),
      confidence: row.confidence != null ? Number(row.confidence) : 0.75,
      dedupeStatus: 'primary',
      name: row.name || null,
    });
  }

  for (const row of candidates) {
    const id =
      asText(row.id || row.companyId || row.placeId) ||
      `cand-${asText(row.name).toLowerCase().replace(/[^a-z0-9]+/g, '-')}`;
    const dedupeStatus = seen.has(id) ? 'duplicate' : 'primary';
    if (!seen.has(id)) seen.add(id);
    const workload = row._coverageWorkload || {};
    records.push({
      candidate_id: id,
      origin: ORIGINS.EXTERNAL_DISCOVERY,
      sources: [row.discoverySource || row.source || SOURCE_TYPES.PUBLIC_BUSINESS_DATA],
      cities: [row.discoveryCity || row.location].filter(Boolean),
      confidence: row.icpScore != null ? clamp01(Number(row.icpScore) / 100) : 0.55,
      dedupeStatus,
      name: row.name || null,
      concept: workload.concept || row.discoveryConcept || null,
    });
  }

  return records;
}

function computeDiscoveryConfidence(input = {}) {
  const coverage = input.coverage || {};
  const universe = input.candidateUniverse || [];
  const searchSuccess = input.searchSuccess != null ? input.searchSuccess : 0.5;
  const evidenceQuality = input.evidenceQuality != null ? input.evidenceQuality : 0.5;

  const coverageScore = clamp01(
    ((coverage.cities && coverage.cities.ratio) || 0) * 0.25 +
      ((coverage.concepts && coverage.concepts.ratio) || 0) * 0.25 +
      ((coverage.sources && coverage.sources.ratio) || 0) * 0.2 +
      ((coverage.searches && coverage.searches.ratio) || 0) * 0.3
  );

  const uniqueSources = new Set(universe.flatMap((row) => row.sources || [])).size;
  const uniqueCities = new Set(universe.flatMap((row) => row.cities || [])).size;
  const diversityScore = clamp01(
    Math.min(1, uniqueSources / 3) * 0.5 + Math.min(1, uniqueCities / 4) * 0.5
  );

  const overall = clamp01(
    coverageScore * 0.45 + searchSuccess * 0.2 + diversityScore * 0.2 + evidenceQuality * 0.15
  );

  return {
    overall: Number(overall.toFixed(2)),
    coverage: Number(coverageScore.toFixed(2)),
    searchSuccess: Number(clamp01(searchSuccess).toFixed(2)),
    candidateDiversity: Number(diversityScore.toFixed(2)),
    evidenceQuality: Number(clamp01(evidenceQuality).toFixed(2)),
    investigationComplete: Boolean(coverage.complete),
  };
}

function buildDiscoveryReport(input = {}) {
  const coverage = input.coverage || {};
  const universe = input.candidateUniverse || [];
  const qualified = input.qualifiedCount != null ? Number(input.qualifiedCount) : 0;
  const confidence = input.discoveryConfidence || computeDiscoveryConfidence(input);
  const universeEstimate = normalizeCandidateUniverseEstimate(input.universeEstimate);
  const investigated =
    input.investigated != null
      ? Number(input.investigated)
      : universe.filter((row) => row.dedupeStatus !== 'duplicate').length;
  const marketCoveragePct =
    input.coveragePct != null
      ? input.coveragePct
      : computeCoverageFromEstimate(investigated, universeEstimate);

  const report = {
    coverage: {
      cities: `${coverage.cities && coverage.cities.searched}/${coverage.cities && coverage.cities.planned}`,
      concepts: `${coverage.concepts && coverage.concepts.searched}/${coverage.concepts && coverage.concepts.planned}`,
      sources: `${coverage.sources && coverage.sources.searched}/${coverage.sources && coverage.sources.planned}`,
      searches: `${coverage.searches && coverage.searches.addressed}/${coverage.searches && coverage.searches.planned}`,
    },
    candidateUniverse: investigated,
    qualified,
    confidence: confidence.overall,
    discoveryConfidence: confidence,
    status: coverage.complete ? 'complete' : 'incomplete',
    warnings: coverage.warnings || [],
    recommendation: coverage.complete
      ? qualified > 0
        ? 'Review qualified candidates.'
        : 'Investigation complete. No qualified candidates remain — proceed to operator review.'
      : 'Continue investigation.',
  };

  if (input.marketDefinition) {
    report.marketDefinition = {
      market: input.marketDefinition.market,
      geography: input.marketDefinition.geography,
      customerTypes: input.marketDefinition.customerTypes,
      decisionMakers: input.marketDefinition.decisionMakers,
      businessModels: input.marketDefinition.businessModels,
      terminology: input.marketDefinition.terminology,
      adjacentMarkets: input.marketDefinition.adjacentMarkets,
      exclusions: input.marketDefinition.exclusions,
      buyingSignals: input.marketDefinition.buyingSignals,
      expectedEvidence: input.marketDefinition.expectedEvidence,
      operatorSegment: input.marketDefinition.operatorSegment,
    };
  }

  if (input.investigationReport) {
    report.investigationHypotheses = input.investigationReport.hypotheses;
    report.investigationBranches = input.investigationReport.branches;
    report.finalUnderstanding = input.investigationReport.finalUnderstanding;
  }

  if (input.revisedMarketDefinition) {
    report.revisedMarketDefinition = {
      market: input.revisedMarketDefinition.market,
      terminology: input.revisedMarketDefinition.terminology,
      customerTypes: input.revisedMarketDefinition.customerTypes,
      revisionHistory: input.revisedMarketDefinition.revisionHistory,
    };
  }

  if (universeEstimate) {
    report.estimatedMarket = {
      minimum: universeEstimate.minimum,
      expected: universeEstimate.expected,
      maximum: universeEstimate.maximum,
      confidence: universeEstimate.confidence,
      reasoning: universeEstimate.reasoning,
      revisionHistory: universeEstimate.revisionHistory || [],
    };
    report.investigated = investigated;
    if (marketCoveragePct != null) {
      report.marketCoveragePct = marketCoveragePct;
      report.marketCoverage = `${Math.round(marketCoveragePct * 100)}%`;
    }
  }

  return report;
}

function canConcludeEmptyUniverse(coverage, qualifiedCount) {
  return Boolean(coverage && coverage.complete) && Number(qualifiedCount) === 0;
}

function discoveryStatusFromCoverage(coverage) {
  if (!coverage || coverage.complete !== true) return 'incomplete';
  return 'complete';
}

module.exports = {
  ORIGINS,
  expandCitiesFromSearchDefinition,
  buildDiscoveryPlan,
  executeCoveragePlan,
  executeLegacyDiscovery,
  computeCoverageMetrics,
  buildCandidateUniverseRecords,
  computeDiscoveryConfidence,
  buildDiscoveryReport,
  canConcludeEmptyUniverse,
  discoveryStatusFromCoverage,
};
