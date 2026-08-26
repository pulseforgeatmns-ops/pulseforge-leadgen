'use strict';

/**
 * SPEC-177 — Hypothesis-Driven Discovery Engine.
 *
 * Replaces provider-first discovery with hypothesis-driven investigation.
 * Business hypotheses → Investigation Plan → Evidence Requirements → Provider Assignment
 * → Evidence Collection → Identity Resolution → Evidence Synthesis.
 *
 * Invariants:
 * - Business hypotheses own investigations.
 * - Providers collect evidence — they do not define search strategy.
 * - Investigations are planned from uncertainty, not keywords.
 */

const { asText, SOURCE_TYPES } = require('../../max/scoutAcquisition/Types');
const { scopeSearchDefinitionForTask } = require('./EvidenceRequest');
const { generateCanonicalHypotheses, businessHypothesesForPlanner } = require('../hypothesis/CanonicalHypothesisEngine');
const {
  createInvestigationState,
  addEvidenceToGraph,
  updateHypothesisBuckets,
} = require('../investigation/InvestigationState');
const {
  buildIdentityKey,
  establishBusinessIdentity,
  leadHasEstablishedIdentity,
} = require('../identity/BusinessIdentity');
const { INVESTIGATIVE_EVIDENCE } = require('./EvidenceRequirements');
const {
  createHypothesisInvestigationPlan,
  updatePlanAfterEvidence,
  getNextInvestigationTasks,
  markInvestigationComplete,
  buildOperatorExplanations,
  revisePlanForUnavailableProviders,
} = require('./HypothesisInvestigationPlanner');
const {
  normalizeProviderReport,
  mergeEvidenceReports,
  buildProviderEvidenceReport,
} = require('./ProviderEvidenceContract');
const { explainProviderForOperator } = require('./EvidenceProviderAssignment');

/** Provider ID → adapter sourceType mapping. */
const PROVIDER_TO_SOURCE_TYPE = Object.freeze({
  google_maps: SOURCE_TYPES.PUBLIC_BUSINESS_DATA,
  county_records: SOURCE_TYPES.PUBLIC_BUSINESS_DATA,
  existing_pf: SOURCE_TYPES.EXISTING_PF,
  website: SOURCE_TYPES.COMPANY_WEBSITES,
  linkedin: SOURCE_TYPES.LINKEDIN,
  facebook: SOURCE_TYPES.FACEBOOK,
  instagram: SOURCE_TYPES.INSTAGRAM,
  prospeo: SOURCE_TYPES.ENRICHMENT_PROVIDER,
  hunter: SOURCE_TYPES.ENRICHMENT_PROVIDER,
  news: SOURCE_TYPES.ENRICHMENT_PROVIDER,
});

function adapterForProvider(adapters = [], providerId) {
  const sourceType = PROVIDER_TO_SOURCE_TYPE[providerId];
  if (providerId === 'google_maps') {
    return (
      adapters.find((a) => a && a.sourceType === SOURCE_TYPES.PUBLIC_BUSINESS_DATA) || null
    );
  }
  if (!sourceType) {
    return adapters.find((a) => a && a.id === providerId) || null;
  }
  return (
    adapters.find((a) => a && a.sourceType === sourceType && a.id !== 'existing_pf') ||
    adapters.find((a) => a && a.sourceType === sourceType) ||
    null
  );
}

function scopedSearchForTask(searchDefinition, task, marketDefinition) {
  return scopeSearchDefinitionForTask(searchDefinition, task, marketDefinition);
}

function normalizeCandidateRow(row) {
  return {
    ...row,
    place_id: row.place_id || row.placeId,
    company: row.company || row.name,
    name: row.name || row.company,
    formatted_address: row.formatted_address || row.address,
    formatted_phone_number: row.formatted_phone_number || row.phone,
    url: row.url || row.website,
    website: row.website || row.url,
    government_registry: row.government_registry || row.registry_id,
  };
}

function candidateMatchKey(row) {
  const normalized = normalizeCandidateRow(row);
  const identity = establishBusinessIdentity(normalized);
  if (identity.identityKey) return identity.identityKey;

  const name = asText(normalized.name || normalized.company).toLowerCase();
  const address = asText(normalized.address || normalized.formatted_address).toLowerCase();
  if (name && address) return `nameaddr:${name}|${address}`;
  return asText(normalized.id || name).toLowerCase();
}

/**
 * Merge overlapping business identities from multiple providers (Scenario 5).
 * @param {object[]} candidates
 * @returns {object[]}
 */
function mergeIdentities(candidates = []) {
  const byKey = new Map();
  const merged = [];

  for (const raw of candidates) {
    const row = normalizeCandidateRow(raw);
    const identity = establishBusinessIdentity(row);
    if (!identity.established && !row.name && !row.company) continue;

    const key = candidateMatchKey(row);

    const existing = byKey.get(key);
    if (existing) {
      byKey.set(key, fuseCandidateRecords(existing, row, identity));
    } else {
      // Secondary match: try name+address against existing entries
      const nameAddrKey = `nameaddr:${asText(row.name || row.company).toLowerCase()}|${asText(row.address || row.formatted_address).toLowerCase()}`;
      const byNameAddr = [...byKey.entries()].find(([k, v]) => {
        const vKey = `nameaddr:${asText(v.name || v.company).toLowerCase()}|${asText(v.address || v.formatted_address).toLowerCase()}`;
        return vKey === nameAddrKey && nameAddrKey !== 'nameaddr:|';
      });

      if (byNameAddr) {
        const [existingKey, existingRow] = byNameAddr;
        const fused = fuseCandidateRecords(existingRow, row, identity);
        byKey.delete(existingKey);
        byKey.set(key, fused);
        const idx = merged.indexOf(existingRow);
        if (idx >= 0) merged[idx] = fused;
        else merged.push(fused);
      } else {
        const canonical = {
          ...row,
          _identityKey: key,
          _identityConfidence: identity.confidence,
          _identitySignals: identity.signals,
          _mergedFrom: [row.discoverySource || row.source].filter(Boolean),
        };
        byKey.set(key, canonical);
        merged.push(canonical);
      }
    }
  }

  return merged;
}

function fuseCandidateRecords(existing, incoming, identity) {
  const sources = new Set([
    ...(existing._mergedFrom || []),
    incoming.discoverySource || incoming.source,
  ].filter(Boolean));

  return {
    ...existing,
    ...incoming,
    name: existing.name || incoming.name || incoming.company,
    phone: existing.phone || incoming.phone,
    address: existing.address || incoming.address,
    website: existing.website || incoming.website || incoming.url,
    placeId: existing.placeId || incoming.placeId || incoming.place_id,
    people: [...(existing.people || []), ...(incoming.people || [])],
    signals: [...(existing.signals || []), ...(incoming.signals || [])],
    _identityKey: existing._identityKey || identity.identityKey,
    _identityConfidence: Math.max(existing._identityConfidence || 0, identity.confidence),
    _mergedFrom: [...sources],
  };
}

async function executeProviderAssignment(assignment, searchDefinition, adapters, marketDefinition) {
  const adapter = adapterForProvider(adapters, assignment.providerId);
  const task = {
    id: `task:${assignment.evidenceType}`,
    evidenceType: assignment.evidenceType,
    providers: [assignment],
  };
  const scoped = scopedSearchForTask(searchDefinition, task, marketDefinition);

  const adapterAvailable =
    adapter && (typeof adapter.available !== 'function' || adapter.available());

  if (!adapterAvailable) {
    return normalizeProviderReport(
      { candidates: [], errors: [{ message: `${assignment.providerId} unavailable` }], available: false },
      assignment
    );
  }

  try {
    const report = await adapter.discover(scoped);
    return normalizeProviderReport(report, assignment);
  } catch (err) {
    return normalizeProviderReport(
      { candidates: [], errors: [{ message: err.message || String(err) }] },
      assignment
    );
  }
}

async function executeInvestigationTask(task, searchDefinition, adapters, marketDefinition, opts = {}) {
  const reports = [];
  const errors = [];

  for (const assignment of task.providers || []) {
    if (assignment.status === 'skipped') continue;

    const adapter = adapterForProvider(adapters, assignment.providerId);
    const adapterAvailable =
      adapter && (typeof adapter.available !== 'function' || adapter.available());
    if (!adapterAvailable && assignment.status === 'unavailable') continue;
    const report = await executeProviderAssignment(
      assignment,
      searchDefinition,
      adapters,
      marketDefinition
    );
    reports.push(report);
    if (report.status === 'failed') {
      errors.push({ providerId: assignment.providerId, error: report.error });
    }
  }

  const merged = mergeEvidenceReports(reports);
  const candidates = mergeIdentities(merged.candidates || []);

  return {
    taskId: task.id,
    evidenceType: task.evidenceType,
    reports,
    mergedReport: merged,
    candidates,
    errors,
    status: candidates.length ? 'completed' : reports.some((r) => r.status === 'completed') ? 'partial' : 'failed',
  };
}

/**
 * Run hypothesis-driven discovery (SPEC-177).
 * @param {object} input
 * @returns {Promise<object>}
 */
async function runHypothesisDrivenDiscovery(input = {}) {
  const mission = input.mission || {};
  const marketDefinition = input.marketDefinition || {};
  const searchDefinition = input.searchDefinition || marketDefinition.searchDefinition || {};
  const adapters = input.adapters || [];
  const opts = input.opts || {};

  const canonical =
    input.canonicalHypotheses ||
    generateCanonicalHypotheses(marketDefinition, mission, opts);
  const hypotheses =
    input.hypotheses || businessHypothesesForPlanner(canonical.hypotheses || canonical.business);
  let plan = input.investigationPlan || createHypothesisInvestigationPlan({
    mission,
    marketDefinition,
    hypotheses,
    canonicalHypotheses: canonical,
    opts,
  });

  let investigationState =
    input.investigationState ||
    createInvestigationState({
      mission,
      marketDefinition,
      hypotheses,
      coverage: null,
    });

  investigationState = {
    ...investigationState,
    investigationPlan: plan,
    questions: plan.questions,
    evidenceRequirements: plan.evidenceRequirements,
    assignedProviders: plan.assignedProviders,
    satisfiedEvidence: plan.satisfiedEvidence,
    outstandingEvidence: plan.outstandingEvidence,
  };

  const allCandidates = [];
  const allReports = [];
  const allErrors = [];
  const executedTasks = [];
  const collectedEvidence = [];
  let iteration = 0;
  const maxIterations = opts.maxIterations != null ? opts.maxIterations : 10;

  while (iteration < maxIterations) {
    iteration += 1;
    const nextTasks = getNextInvestigationTasks(plan, opts);
    if (!nextTasks.length) break;

    for (const task of nextTasks) {
      if (plan.sufficientlyInvestigated) break;

      const result = await executeInvestigationTask(
        task,
        searchDefinition,
        adapters,
        marketDefinition,
        opts
      );

      executedTasks.push(result);
      allReports.push(...result.reports);
      allErrors.push(...result.errors);

      for (const report of result.reports) {
        collectedEvidence.push({
          evidenceType: report.evidenceType,
          type: report.evidenceType,
          providerId: report.providerId,
          evidenceProduced: report.evidenceProduced,
          confidence: report.confidence,
          coverage: report.coverage,
          limitations: report.limitations,
        });
      }

      const newCandidates = result.candidates || [];
      const seen = new Set(allCandidates.map((c) => c._identityKey || asText(c.id || c.name).toLowerCase()));
      for (const row of newCandidates) {
        const key = row._identityKey || asText(row.id || row.name).toLowerCase();
        if (!seen.has(key)) {
          seen.add(key);
          allCandidates.push(row);
        }
      }

      task.status = result.status === 'failed' ? 'failed' : 'completed';
      plan = updatePlanAfterEvidence(plan, collectedEvidence);

      investigationState = addEvidenceToGraph(investigationState, collectedEvidence.slice(-result.reports.length));
      investigationState = {
        ...investigationState,
        investigationPlan: plan,
        satisfiedEvidence: plan.satisfiedEvidence,
        outstandingEvidence: plan.outstandingEvidence,
        questions: plan.questions,
      };

      if (plan.sufficientlyInvestigated) {
        plan = markInvestigationComplete(plan);
        investigationState = {
          ...investigationState,
          investigationPlan: plan,
          phase: 'complete',
        };
        break;
      }
    }

    if (plan.sufficientlyInvestigated) break;

    // No progress — try revising for unavailable providers (Scenario 4).
    const failedProviders = allReports
      .filter((r) => r.status === 'failed')
      .map((r) => r.providerId);
    if (failedProviders.length && iteration < maxIterations) {
      plan = revisePlanForUnavailableProviders(plan, failedProviders);
      investigationState = { ...investigationState, investigationPlan: plan };
    } else {
      break;
    }
  }

  const identityComplete = (plan.satisfiedEvidence || []).some(
    (s) => s.evidenceType === INVESTIGATIVE_EVIDENCE.IDENTITY
  );

  const qualifiedCandidates = allCandidates.filter((row) => {
    if (opts.requireEstablishedIdentity === false) return true;
    return leadHasEstablishedIdentity(row);
  });

  investigationState = updateHypothesisBuckets(
    investigationState,
    plan.hypotheses.map((h) => ({
      ...h,
      lifecycle: plan.sufficientlyInvestigated ? 'confirmed' : 'testing',
      confidence: plan.sufficientlyInvestigated ? 0.85 : h.confidence,
    }))
  );

  const operatorExplanations = buildOperatorExplanations(plan);

  return {
    candidates: qualifiedCandidates,
    allCandidates,
    investigationPlan: plan,
    investigationState,
    executedTasks,
    providerReports: allReports,
    collectedEvidence,
    errors: allErrors,
    identityComplete,
    sufficientlyInvestigated: plan.sufficientlyInvestigated,
    operatorExplanations,
    discoveryPlan: {
      hypothesisDriven: true,
      spec: 'SPEC-177',
      tasksExecuted: executedTasks.length,
      providersUsed: [...new Set(allReports.map((r) => r.providerId))],
      identityComplete,
      sufficientlyInvestigated: plan.sufficientlyInvestigated,
    },
    coverage: buildCoverageMetrics(plan, executedTasks),
  };
}

function buildCoverageMetrics(plan, executedTasks = []) {
  const totalReqs = (plan.evidenceRequirements || []).length;
  const satisfied = (plan.satisfiedEvidence || []).length;
  return {
    evidenceRequirements: { satisfied, planned: totalReqs, ratio: totalReqs ? satisfied / totalReqs : 1 },
    tasks: { executed: executedTasks.length, planned: (plan.tasks || []).length },
    complete: plan.sufficientlyInvestigated === true,
    warnings: [],
  };
}

/**
 * Explain why a provider was selected (operator explainability for Max).
 * @param {object} plan
 * @param {string} providerId
 * @returns {string|null}
 */
function explainProviderUsage(plan, providerId) {
  const assignment = (plan.assignedProviders || []).find((a) => a.providerId === providerId);
  if (!assignment) return null;
  return explainProviderForOperator(assignment, plan);
}

module.exports = {
  runHypothesisDrivenDiscovery,
  mergeIdentities,
  executeInvestigationTask,
  executeProviderAssignment,
  scopedSearchForTask,
  explainProviderUsage,
  PROVIDER_TO_SOURCE_TYPE,
  buildProviderEvidenceReport,
};
