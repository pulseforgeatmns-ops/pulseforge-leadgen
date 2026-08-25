'use strict';

/**
 * SPEC-100A — construct the acquisition candidate universe.
 * Retrieve existing tenant intelligence before discovering the open market.
 * Persist useful discovery so Scout does not rediscover the same companies.
 */

const { asText, clone, nowIso, ageMs, DEFAULT_FRESHNESS_MS, REFRESH_MS, SOURCE_TYPES } = require('./Types');
const { retrieveExistingIntelligence } = require('./ExistingIntelligence');
const { discoverCandidates, defaultDiscoveryAdapters } = require('./DiscoveryAdapters');
const { resolveCandidateUniverse } = require('./EntityResolution');
const { searchDefinitionFingerprint } = require('./SearchDefinition');
const {
  buildDiscoveryPlan,
  executeCoveragePlan,
  buildCandidateUniverseRecords,
  buildDiscoveryReport,
  computeDiscoveryConfidence,
  discoveryStatusFromCoverage,
} = require('../../scout/coverage/DiscoveryCoverageEngine');
const {
  evaluateDiscoveryCapability,
  buildCapabilityBlockedResult,
} = require('../../scout/coverage/DiscoveryCapabilityGate');
const { executeHypothesisDrivenCoverage } = require('../../scout/coverage/HypothesisDrivenDiscovery');
const { runHypothesisDrivenDiscovery } = require('../../scout/coverage/HypothesisDrivenDiscoveryEngine');

function createMemoryDiscoveryStore(snapshot = null) {
  /** @type {Map<string, object[]>} */
  const rows = new Map();
  if (snapshot && Array.isArray(snapshot.rows)) {
    for (const row of snapshot.rows) {
      rows.set(String(row.tenantId), clone(row.companies || []));
    }
  }
  return {
    kind: 'memory',
    async list(tenantId) {
      return clone(rows.get(String(tenantId)) || []);
    },
    async upsert(tenantId, companies) {
      const current = rows.get(String(tenantId)) || [];
      const byId = new Map(current.map((c) => [String(c.id), c]));
      for (const company of companies || []) {
        if (!company || !company.id) continue;
        byId.set(String(company.id), {
          ...byId.get(String(company.id)),
          ...company,
          lastEvaluatedAt: company.lastEvaluatedAt || nowIso(),
        });
      }
      const next = [...byId.values()];
      rows.set(String(tenantId), next);
      return clone(next);
    },
    serialize() {
      return {
        rows: [...rows.entries()].map(([tenantId, companies]) => ({
          tenantId,
          companies: clone(companies),
        })),
      };
    },
  };
}

function isFresh(iso, now, windowMs) {
  if (!iso) return false;
  return ageMs(iso, now) <= windowMs;
}

function assessExistingSufficiency(existing, searchDefinition, opts = {}) {
  const now = opts.now != null ? Number(opts.now) : Date.now();
  const freshnessMs = opts.freshnessMs != null ? Number(opts.freshnessMs) : DEFAULT_FRESHNESS_MS;
  const companies = (existing && existing.companies) || [];
  const fresh = companies.filter(
    (c) =>
      isFresh(c.lastEvaluatedAt || c.updatedAt, now, freshnessMs) ||
      isFresh(c.discoveredAt, now, freshnessMs)
  );
  const stale = companies.filter((c) => !fresh.includes(c));
  const relevant = companies.length;
  const fingerprint = searchDefinitionFingerprint(searchDefinition);
  return {
    sufficient: fresh.length > 0 && relevant > 0,
    shouldDiscoverGap: true,
    reuse: fresh,
    refresh: stale.filter((c) => !isFresh(c.lastEvaluatedAt, now, REFRESH_MS)),
    relevantCount: relevant,
    freshCount: fresh.length,
    fingerprint,
  };
}

function alreadyKnown(existingCompanies, discovered) {
  const { recordsMatch } = require('./EntityResolution');
  return (existingCompanies || []).some((row) => recordsMatch(row, discovered));
}

/**
 * Retrieve → (optional) discover gap → resolve/dedup.
 *
 * @returns {Promise<object>}
 */
async function constructCandidateUniverse(input = {}) {
  const searchDefinition = input.searchDefinition;
  const tenantId = asText(searchDefinition && searchDefinition.tenantId);
  const existing = input.existing ||
    retrieveExistingIntelligence({
      authorizedTenantId: tenantId,
      tenantId,
      targetContext: {
        geography: searchDefinition.geography && searchDefinition.geography.label,
        segments: searchDefinition.segments,
      },
      businessContext: {
        exclusions: searchDefinition.exclusions,
        preferredSegments: searchDefinition.segments,
        serviceGeography: searchDefinition.geography && searchDefinition.geography.label,
      },
      companies: input.companies || [],
      people: input.people || [],
    });

  const store = input.discoveryStore || null;
  let persisted = [];
  if (store && typeof store.list === 'function') {
    persisted = (await store.list(tenantId)) || [];
  }

  const persistedScoped = retrieveExistingIntelligence({
    authorizedTenantId: tenantId,
    tenantId,
    targetContext: {
      geography: searchDefinition.geography && searchDefinition.geography.label,
      segments: searchDefinition.segments,
    },
    businessContext: {
      exclusions: searchDefinition.exclusions,
      preferredSegments: searchDefinition.segments,
      serviceGeography: searchDefinition.geography && searchDefinition.geography.label,
    },
    companies: persisted,
    people: [],
  });

  const existingCompanies = [
    ...(existing.companies || []),
    ...persistedScoped.companies.filter(
      (row) => !(existing.companies || []).some((c) => String(c.id) === String(row.id))
    ),
  ];
  const rejectedFromRetrieve = [
    ...(existing.rejectedCandidates || []),
    ...(persistedScoped.rejectedCandidates || []),
  ];
  const retrievedCount =
    (existing.discoveredCompanies || existing.companies || []).length +
    (persistedScoped.discoveredCompanies || []).length;

  const sufficiency = assessExistingSufficiency(
    { companies: existingCompanies },
    searchDefinition,
    { now: input.now, freshnessMs: input.freshnessMs }
  );

  const sourceTypesChecked = [SOURCE_TYPES.EXISTING_PF];
  const sourceTypesUnavailable = [];
  const discoveryErrors = [];
  const actionsTaken = [
    { text: 'Retrieved existing tenant-scoped company, prospect, and discovery intelligence.' },
  ];
  let discoveredRaw = [];
  let discoveryRan = false;
  let discoveryFailedCompletely = false;
  let capabilityBlocked = false;
  let capabilityEvaluation = null;

  const adapters = Array.isArray(input.adapters)
    ? input.adapters
    : defaultDiscoveryAdapters(input.adapterOpts || {});
  const marketAdapters = adapters.filter((a) => a && a.id !== 'existing_pf');
  const hasUsableMarketAdapter = marketAdapters.some(
    (a) => typeof a.available !== 'function' || a.available()
  );

  let discoveryPlan = null;
  let coverageMetrics = null;
  let candidateUniverseRecords = buildCandidateUniverseRecords([], {
    seeded: existingCompanies.map((row) => ({ ...row, origin: 'existing_intelligence' })),
  });
  let discoveryReport = null;
  let investigationReport = null;
  let investigationState = null;
  let investigationPlan = null;
  let revisedMarketDefinition = input.marketDefinition || null;

  const useCoverageEngine = input.useCoverageEngine !== false;
  if (input.useCoverageEngine === false) {
    throw new Error(
      'CoverageEngine bypass is not permitted (SPEC-154). All discovery must execute DiscoveryPipeline.'
    );
  }

  if (input.forceDiscover === true || sufficiency.shouldDiscoverGap) {
    discoveryPlan = buildDiscoveryPlan(searchDefinition, {
      adapters: marketAdapters,
      marketDefinition: input.marketDefinition,
    });

    capabilityEvaluation = evaluateDiscoveryCapability({
      adapters: marketAdapters,
      coveragePlan: discoveryPlan,
      requireExternalDiscovery: true,
      discover: input.adapterOpts && input.adapterOpts.discover,
      enablePlaces: input.adapterOpts && input.adapterOpts.enablePlaces,
      placesProvider: input.adapterOpts && input.adapterOpts.placesProvider,
    });

    if (!capabilityEvaluation.canExecute) {
      const blocked = buildCapabilityBlockedResult(capabilityEvaluation);
      capabilityBlocked = true;
      sourceTypesUnavailable.push(SOURCE_TYPES.PUBLIC_BUSINESS_DATA);
      actionsTaken.push({
        text: blocked.blockReason,
      });
      actionsTaken.push({
        text: blocked.explanation,
      });
      return {
        searchDefinition,
        companies: existingCompanies.map((c) => ({
          ...c,
          tenantId: c.tenantId || tenantId,
          discoveredAt: c.discoveredAt || nowIso(),
        })),
        people: existing.people || input.people || [],
        rejectedFromRetrieve,
        sufficiency,
        discoveryRan: false,
        discoveryFailedCompletely: false,
        capabilityBlocked: true,
        capabilityEvaluation: blocked.capabilityEvaluation,
        blockReason: blocked.blockReason,
        blockerCode: blocked.blockerCode,
        candidatesDiscovered: retrievedCount,
        candidatesResolved: 0,
        duplicatesRemoved: 0,
        resolvedToExisting: 0,
        sourceTypesChecked,
        sourceTypesUnavailable,
        discoveryErrors: [],
        actionsTaken,
        retrievedBeforeDiscover: true,
        broadened: false,
        discoveryPlan,
        coverage: null,
        candidateUniverse: candidateUniverseRecords,
        discoveryReport: buildDiscoveryReport({
          coverage: {},
          candidateUniverse: candidateUniverseRecords,
          qualifiedCount: 0,
        }),
        discoveryStatus: 'blocked',
        investigationReport,
        revisedMarketDefinition,
      };
    }

    discoveryRan = true;
    let result;
    if (useCoverageEngine) {
      if (input.marketDefinition && input.useHypothesisDiscoveryEngine !== false) {
        const engineResult = await runHypothesisDrivenDiscovery({
          mission: input.mission || {},
          marketDefinition: input.marketDefinition,
          searchDefinition,
          adapters: marketAdapters,
          opts: input.hypothesisOpts || {},
        });
        result = engineResult;
        discoveryPlan = engineResult.discoveryPlan;
        coverageMetrics = engineResult.coverage;
        investigationReport = engineResult.investigationPlan;
        investigationState = engineResult.investigationState;
        investigationPlan = engineResult.investigationPlan;
        revisedMarketDefinition = input.marketDefinition;
        actionsTaken.push({
          text: `Hypothesis-driven discovery engine (SPEC-177): ${(engineResult.executedTasks || []).length} investigation tasks executed; identity ${engineResult.identityComplete ? 'complete' : 'pending'}.`,
        });
      } else if (input.marketDefinition && input.useHypothesisEngine !== false) {
        const hypothesisResult = await executeHypothesisDrivenCoverage({
          marketDefinition: input.marketDefinition,
          searchDefinition,
          adapters: marketAdapters,
          opts: input.hypothesisOpts || {},
          terminologyLearningStore: input.terminologyLearningStore,
        });
        result = hypothesisResult;
        discoveryPlan = hypothesisResult.discoveryPlan;
        coverageMetrics = hypothesisResult.coverage;
        investigationReport = hypothesisResult.investigationReport;
        revisedMarketDefinition = hypothesisResult.revisedMarketDefinition || input.marketDefinition;
        actionsTaken.push({
          text: `Hypothesis-driven investigation: ${(hypothesisResult.searchHypotheses || []).length} terminology hypotheses evaluated.`,
        });
      } else {
        result = await executeCoveragePlan(discoveryPlan, searchDefinition, marketAdapters);
        coverageMetrics = result.coverage;
        actionsTaken.push({
          text: `Executed coverage plan: ${coverageMetrics.searches.addressed}/${coverageMetrics.searches.planned} searches across ${coverageMetrics.cities.planned} cities and ${coverageMetrics.concepts.planned} concepts.`,
        });
      }
    } else {
      result = await discoverCandidates(searchDefinition, marketAdapters);
    }
    discoveredRaw = (result.candidates || []).filter((row) => {
      if (row.tenantId && tenantId && String(row.tenantId) !== String(tenantId)) return false;
      return true;
    });
    discoveryErrors.push(...(result.errors || []));
    for (const src of result.sourceTypesChecked || []) {
      if (!sourceTypesChecked.includes(src)) sourceTypesChecked.push(src);
    }
    for (const src of result.sourceTypesUnavailable || []) {
      if (!sourceTypesUnavailable.includes(src)) sourceTypesUnavailable.push(src);
    }
    const newOnly = discoveredRaw.filter((row) => !alreadyKnown(existingCompanies, row));
    discoveredRaw = newOnly;
    candidateUniverseRecords = buildCandidateUniverseRecords(discoveredRaw, {
      seeded: existingCompanies.map((row) => ({ ...row, origin: 'existing_intelligence' })),
    });
    const discoveryConfidence = computeDiscoveryConfidence({
      coverage: coverageMetrics,
      candidateUniverse: candidateUniverseRecords,
      searchSuccess:
        coverageMetrics && coverageMetrics.searches.planned
          ? coverageMetrics.searches.executed / coverageMetrics.searches.planned
          : discoveredRaw.length > 0
            ? 0.6
            : 0.3,
      evidenceQuality: existingCompanies.length ? 0.65 : 0.35,
    });
    discoveryReport = buildDiscoveryReport({
      coverage: coverageMetrics,
      candidateUniverse: candidateUniverseRecords,
      qualifiedCount: 0,
      discoveryConfidence,
      marketDefinition: revisedMarketDefinition,
      investigationReport,
      revisedMarketDefinition,
    });
    if (discoveredRaw.length) {
      actionsTaken.push({
        text: `Discovered ${discoveredRaw.length} additional candidate${discoveredRaw.length === 1 ? '' : 's'} for remaining coverage gaps.`,
      });
    } else if (candidateUniverseRecords.length) {
      actionsTaken.push({
        text: `Candidate universe seeded from existing intelligence (${candidateUniverseRecords.length} record${candidateUniverseRecords.length === 1 ? '' : 's'}) before external discovery.`,
      });
    } else {
      actionsTaken.push({
        text: coverageMetrics && !coverageMetrics.complete
          ? 'Discovery incomplete — coverage plan not fully executed.'
          : 'Ran bounded discovery for remaining coverage gaps; no new unique companies were added.',
      });
    }
    const marketChecked = (result.sourceTypesChecked || []).filter(
      (s) => s === SOURCE_TYPES.PUBLIC_BUSINESS_DATA
    );
    if (!marketChecked.length && (result.errors || []).some((e) => e.code === 'provider_error')) {
      discoveryFailedCompletely = existingCompanies.length === 0;
    }
  } else if (!hasUsableMarketAdapter) {
    sourceTypesUnavailable.push(SOURCE_TYPES.PUBLIC_BUSINESS_DATA);
    actionsTaken.push({
      text: 'No external discovery adapter was available; evaluated existing intelligence only.',
    });
  }

  const resolved = resolveCandidateUniverse(existingCompanies, discoveredRaw);
  const companies = resolved.companies.map((c) => ({
    ...c,
    tenantId: c.tenantId || tenantId,
    discoveredAt: c.discoveredAt || nowIso(),
  }));

  if (store && typeof store.upsert === 'function' && companies.length) {
    await store.upsert(tenantId, companies);
  }
  if (typeof input.persistCompanies === 'function' && companies.length) {
    try {
      await input.persistCompanies({ tenantId, companies, searchDefinition });
    } catch {
      discoveryErrors.push({
        code: 'persist_failed',
        message: 'Discovery persistence failed; in-memory universe was preserved.',
      });
    }
  }

  const candidatesDiscovered = retrievedCount + discoveredRaw.length;
  if (!candidateUniverseRecords.length) {
    candidateUniverseRecords = buildCandidateUniverseRecords(discoveredRaw, {
      seeded: existingCompanies.map((row) => ({ ...row, origin: 'existing_intelligence' })),
    });
  }
  const discoveryStatus = coverageMetrics
    ? discoveryStatusFromCoverage(coverageMetrics)
    : existingCompanies.length && !hasUsableMarketAdapter
      ? 'complete'
      : discoveryRan
        ? discoveryStatusFromCoverage(coverageMetrics || { complete: true })
        : existingCompanies.length
          ? 'complete'
          : 'incomplete';
  return {
    searchDefinition,
    companies,
    people: existing.people || input.people || [],
    rejectedFromRetrieve,
    sufficiency,
    discoveryRan,
    discoveryFailedCompletely,
    candidatesDiscovered,
    candidatesResolved: resolved.candidatesResolved,
    duplicatesRemoved: resolved.duplicatesRemoved,
    resolvedToExisting: resolved.resolvedToExisting,
    sourceTypesChecked,
    sourceTypesUnavailable,
    discoveryErrors,
    actionsTaken,
    retrievedBeforeDiscover: true,
    broadened: false,
    discoveryPlan,
    coverage: coverageMetrics,
    candidateUniverse: candidateUniverseRecords,
    discoveryReport,
    discoveryStatus,
    capabilityBlocked,
    capabilityEvaluation,
    blockReason: capabilityBlocked ? capabilityEvaluation && capabilityEvaluation.blockReason : null,
    investigationReport,
    investigationState,
    investigationPlan,
    revisedMarketDefinition,
  };
}

module.exports = {
  createMemoryDiscoveryStore,
  assessExistingSufficiency,
  constructCandidateUniverse,
  isFresh,
};
