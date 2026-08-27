'use strict';

/**
 * SPEC-172 — Canonical Scout Evidence Handoff.
 * SPEC-174 — Canonical Evidence Coverage (registry-driven adapter completeness).
 * Normalizes all Scout-internal evidence representations exactly once
 * before Acquisition Mission execution consumes them.
 */

const {
  collectExportableEvidenceEntries,
  isExportableGraphNode,
  EVIDENCE_CLASSIFICATION,
  SCOUT_EVIDENCE_SOURCES,
  GRAPH_NODE_EVIDENCE_CLASSIFICATION,
} = require('./EvidenceCoverageRegistry');
const {
  serializeForAmo,
  deserializeGraph,
} = require('../explainability/ExplainabilityGraph');

const SCOUT_EVIDENCE_HANDOFF_VIOLATION = 'SCOUT_EVIDENCE_HANDOFF_VIOLATION';
const SCOUT_EVIDENCE_COVERAGE_VIOLATION = 'SCOUT_EVIDENCE_COVERAGE_VIOLATION';
const CANONICAL_EVIDENCE_NORMALIZATION_PATH = 'ScoutDiscoveryArtifact.normalizeCanonicalEvidenceItem';

function scoutEvidenceHandoffError(message, extras = {}) {
  const err = new Error(message);
  err.name = 'ValidationError';
  err.code = SCOUT_EVIDENCE_HANDOFF_VIOLATION;
  err.tmeClass = 'validation';
  err.spec = 'SPEC-172';
  err.rollback = true;
  err.commitStatus = 'rolled_back';
  if (extras.details) err.details = extras.details;
  return err;
}

function scoutEvidenceCoverageError(message, extras = {}) {
  const err = new Error(message);
  err.name = 'ValidationError';
  err.code = SCOUT_EVIDENCE_COVERAGE_VIOLATION;
  err.tmeClass = 'validation';
  err.spec = 'SPEC-174';
  err.rollback = true;
  err.commitStatus = 'rolled_back';
  if (extras.details) err.details = extras.details;
  return err;
}

function isPlainObject(value) {
  return value != null && typeof value === 'object' && !Array.isArray(value);
}

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

function round2(n) {
  return Number(Number(n).toFixed(2));
}

function clamp01(n) {
  const v = Number(n);
  if (!Number.isFinite(v)) return 0.5;
  return Math.max(0, Math.min(1, v));
}

function resolveScoutExecutionResult(raw = {}) {
  if (raw.discoveryReport && !raw.intelligenceResult && !raw.payload) {
    return raw;
  }

  const intelligence = raw.intelligenceResult || raw;
  const payload = intelligence.payload || {};
  const pipeline = raw.pipeline || intelligence.pipeline || {};

  return {
    ...intelligence,
    payload,
    pipeline,
    missionIntelligenceReport:
      intelligence.missionIntelligenceReport ||
      pipeline.missionIntelligenceReport ||
      raw.missionIntelligenceReport ||
      null,
    investigationState:
      pipeline.investigationState ||
      intelligence.investigationState ||
      raw.investigationState ||
      null,
    explainabilityGraph:
      pipeline.explainabilityGraph ||
      intelligence.explainabilityGraph ||
      raw.explainabilityGraph ||
      null,
    investigationPlan:
      pipeline.investigationPlan ||
      intelligence.investigationPlan ||
      raw.investigationPlan ||
      null,
    intelligenceReport:
      raw.intelligenceReport ||
      pipeline.intelligenceReport ||
      intelligence.intelligenceReport ||
      payload.intelligenceReport ||
      null,
  };
}

function resolveEvidenceGraph(scoutResult = {}) {
  const resolved = resolveScoutExecutionResult(scoutResult);
  const mir = resolved.missionIntelligenceReport || {};
  const investigationState =
    resolved.investigationState ||
    mir.investigationState ||
    null;

  return (
    investigationState?.evidenceGraph ||
    resolved.payload?.investigation?.evidenceGraph ||
    { nodes: [], edges: [] }
  );
}

function graphNodeToEvidenceRef(node = {}) {
  const data = node.data || {};
  return {
    id: node.id,
    label: data.label || node.label || data.source || 'Evidence',
    source: data.source || data.kind || 'investigation_graph',
    sourceKind: data.kind || 'observed',
    observedAt: data.observedAt || null,
    confidence: data.weight != null ? Number(data.weight) : undefined,
    snapshot: {
      source: data.source || data.kind,
      companyName: data.companyName || null,
      evidenceType: data.kind || null,
      observedAt: data.observedAt || null,
    },
    provenance: {
      kind: 'evidence_graph',
      source: node.id,
      graphNodeType: node.type,
    },
  };
}

function normalizeCanonicalEvidenceItem(raw, context = {}) {
  if (raw == null) return null;
  if (typeof raw === 'string') {
    const text = raw.trim();
    if (!text) return null;
    const originalLocation = context.sourcePath || null;
    return {
      id: context.fallbackId || `ev_${context.index || 0}`,
      source: text,
      sourceType: 'legacy_string',
      claim: text,
      observedAt: null,
      entityId: context.entityId || null,
      originalLocation,
      provenance: {
        kind: 'legacy_string',
        source: originalLocation || 'unknown',
        originalLocation,
        normalizationPath: CANONICAL_EVIDENCE_NORMALIZATION_PATH,
      },
      confidence: 0.5,
      originalRef: null,
    };
  }
  if (!isPlainObject(raw)) return null;

  const snapshot = isPlainObject(raw.snapshot) ? raw.snapshot : {};
  const source =
    asText(raw.source)
    || asText(snapshot.source)
    || asText(raw.sourceKind)
    || asText(raw.kind)
    || 'unknown';
  const label = asText(raw.label) || asText(raw.text) || asText(raw.claim) || source;
  const observedAt =
    asText(raw.observedAt)
    || asText(raw.timestamp)
    || asText(raw.observed_at)
    || asText(snapshot.observedAt)
    || null;
  const entityId =
    asText(raw.entityId)
    || asText(snapshot.companyId)
    || asText(context.entityId)
    || null;

  const originalLocation = context.sourcePath || null;
  const provenance = isPlainObject(raw.provenance)
    ? {
      ...raw.provenance,
      ...(originalLocation ? { originalLocation } : {}),
      normalizationPath: CANONICAL_EVIDENCE_NORMALIZATION_PATH,
    }
    : {
      kind: asText(raw.sourceKind) || asText(raw.kind) || 'observed',
      source,
      ...(entityId ? { entityId } : {}),
      ...(originalLocation ? { collectedFrom: originalLocation, originalLocation } : {}),
      normalizationPath: CANONICAL_EVIDENCE_NORMALIZATION_PATH,
    };

  return {
    id: asText(raw.id) || context.fallbackId || `ev_${context.index || 0}`,
    source,
    sourceType: asText(raw.sourceType || raw.sourceKind || raw.kind) || null,
    observedAt,
    claim: label,
    value: raw.value != null ? raw.value : undefined,
    entityId,
    provenance,
    originalLocation,
    confidence: round2(clamp01(raw.confidence != null ? raw.confidence : snapshot.weight)),
    originalRef: asText(raw.id) || null,
  };
}

function evidenceIdentity(item) {
  if (!item) return null;
  if (item.id && !/^ev_\d+$/.test(item.id)) return `id:${item.id}`;
  if (item.originalRef) return `ref:${item.originalRef}`;

  const source = asText(item.source).toLowerCase();
  const claim = asText(item.claim).toLowerCase();
  const entityId = asText(item.entityId).toLowerCase();
  const observedAt = asText(item.observedAt);
  return `fp:${entityId}|${source}|${claim}|${observedAt}`;
}

function richnessScore(item) {
  if (!item) return 0;
  let score = 0;
  if (item.provenance && Object.keys(item.provenance).length > 1) score += 2;
  if (item.observedAt) score += 1;
  if (item.entityId) score += 1;
  if (item.confidence != null && item.confidence !== 0.5) score += 1;
  if (item.originalRef) score += 1;
  if (item.value != null) score += 1;
  if (item.sourceType) score += 1;
  return score;
}

function deduplicateCanonicalEvidence(items = []) {
  const byIdentity = new Map();
  for (const item of items) {
    if (!item) continue;
    const key = evidenceIdentity(item);
    if (!key) continue;
    const existing = byIdentity.get(key);
    if (!existing || richnessScore(item) > richnessScore(existing)) {
      byIdentity.set(key, item);
    }
  }
  return [...byIdentity.values()];
}

function collectGraphEvidenceEntries(scoutResult = {}) {
  const graph = resolveEvidenceGraph(scoutResult);
  const entries = [];
  for (const node of graph.nodes || []) {
    const nodeType = node.type || node.nodeType;
    if (!isExportableGraphNode(nodeType)) continue;
    entries.push({
      raw: graphNodeToEvidenceRef(node),
      context: {
        sourcePath: 'investigationGraph.nodes[EVIDENCE]',
        entityId: node.data?.entityId || null,
        fallbackId: node.id,
        graphNodeType: nodeType,
      },
    });
  }
  return entries;
}

/**
 * Gather evidence from every exportable Scout source via the coverage registry,
 * normalize, deduplicate, and preserve provenance.
 * @param {object} scoutResult
 * @returns {object[]}
 */
function collectCanonicalScoutEvidence(scoutResult = {}) {
  const resolved = resolveScoutExecutionResult(scoutResult);
  const payload = resolved.payload || {};
  const graphEvidenceEntries = collectGraphEvidenceEntries(scoutResult);
  const exportableEntries = collectExportableEvidenceEntries(
    scoutResult,
    resolved,
    payload,
    graphEvidenceEntries
  );
  const collected = [];

  for (const entry of exportableEntries) {
    const normalized = normalizeCanonicalEvidenceItem(entry.raw, entry.context);
    if (normalized) collected.push(normalized);
  }

  return deduplicateCanonicalEvidence(collected);
}

/**
 * Count exportable evidence identities before the export boundary deduplicates them.
 * @param {object} scoutResult
 * @returns {object[]}
 */
function collectExportableEvidenceIdentities(scoutResult = {}) {
  const resolved = resolveScoutExecutionResult(scoutResult);
  const payload = resolved.payload || {};
  const graphEvidenceEntries = collectGraphEvidenceEntries(scoutResult);
  const exportableEntries = collectExportableEvidenceEntries(
    scoutResult,
    resolved,
    payload,
    graphEvidenceEntries
  );
  const normalized = exportableEntries
    .map((entry) => normalizeCanonicalEvidenceItem(entry.raw, entry.context))
    .filter(Boolean);
  return deduplicateCanonicalEvidence(normalized);
}

/**
 * Development invariant: every exportable Scout evidence item must appear in canonical evidence.
 * @param {object} scoutResult
 * @param {object[]} canonicalEvidence
 */
function assertScoutEvidenceCoverage(scoutResult = {}, canonicalEvidence = []) {
  const exportable = collectExportableEvidenceIdentities(scoutResult);
  const canonical = deduplicateCanonicalEvidence(canonicalEvidence);

  if (exportable.length !== canonical.length) {
    const exportableKeys = new Set(exportable.map((item) => evidenceIdentity(item)));
    const canonicalKeys = new Set(canonical.map((item) => evidenceIdentity(item)));
    const missingFromCanonical = [...exportableKeys].filter((key) => !canonicalKeys.has(key));
    const orphanCanonical = [...canonicalKeys].filter((key) => !exportableKeys.has(key));

    throw scoutEvidenceCoverageError(
      'Exportable Scout evidence count does not match canonical evidence after normalization.',
      {
        details: {
          exportableCount: exportable.length,
          canonicalCount: canonical.length,
          missingFromCanonical,
          orphanCanonical,
        },
      }
    );
  }
}

function collectBuyingSignals(payload = {}) {
  const signals = [];
  const seen = new Set();

  for (const bucket of ['opportunities', 'acquisitionOpportunities', 'fitCandidates', 'watchCandidates']) {
    for (const candidate of payload[bucket] || []) {
      for (const sig of candidate.signals || []) {
        const label = typeof sig === 'string' ? sig : sig?.label || sig?.type;
        const key = `${candidate.name || ''}|${label}`;
        if (!label || seen.has(key)) continue;
        seen.add(key);
        signals.push(typeof sig === 'object' ? { ...sig, company: candidate.name || sig.company } : sig);
      }
    }
  }

  for (const sig of payload.buyingSignals || payload.signals || []) {
    const label = typeof sig === 'string' ? sig : sig?.label || sig?.type;
    if (!label || seen.has(label)) continue;
    seen.add(label);
    signals.push(sig);
  }

  return signals;
}

function buildCompanies(payload = {}) {
  const opportunities = payload.opportunities || payload.acquisitionOpportunities || [];
  const fitCandidates = payload.fitCandidates || [];
  if (Array.isArray(payload.companies) && payload.companies.length) {
    return payload.companies;
  }
  const source = opportunities.length ? opportunities : fitCandidates;
  return source
    .map((row) => ({
      id: row.companyId || row.id,
      name: row.name,
      fit: row.fit,
      timing: row.timing,
      confidence: row.confidence,
    }))
    .filter((row) => row.id || row.name);
}

function resolveExplainabilityProjection(scoutResult = {}, opts = {}) {
  const resolved = resolveScoutExecutionResult(scoutResult);
  const serialized = resolved.explainabilityGraph || null;

  if (serialized && Array.isArray(serialized.nodes) && serialized.nodes.length) {
    const graph = deserializeGraph(serialized);
    return serializeForAmo(graph);
  }

  const mir = resolved.missionIntelligenceReport || null;
  const investigationState = resolved.investigationState || null;
  if (!mir && !investigationState) return null;

  const { buildExplainabilityGraph } = require('../explainability/ExplainabilityGraph');
  const graph = buildExplainabilityGraph({
    mission: opts.mission || {},
    investigationState,
    plan: resolved.investigationPlan || investigationState?.investigationPlan || null,
    missionIntelligenceReport: mir,
  });
  return serializeForAmo(graph);
}

function resolveBlocked(resolved = {}, payload = {}) {
  const qualifiedCount =
    payload.qualifiedCount != null
      ? Number(payload.qualifiedCount)
      : opportunities.length + (payload.fitCandidates || []).length;

  if (
    payload.capabilityBlocked === true ||
    payload.blockerCode === 'external_discovery_capability_unavailable'
  ) {
    return true;
  }

  return (
    resolved.status === 'blocked'
    || payload.outcome === 'blocked'
    || (qualifiedCount <= 0 && payload.discoveryStatus === 'incomplete')
  );
}

/**
 * Build the canonical Scout discovery artifact consumed by AMO execution.
 * @param {object} scoutResult
 * @param {object} [opts]
 * @returns {object}
 */
function buildScoutDiscoveryArtifact(scoutResult = {}, opts = {}) {
  const resolved = resolveScoutExecutionResult(scoutResult);
  const payload = resolved.payload || {};
  const opportunities = payload.opportunities || payload.acquisitionOpportunities || [];
  const evidence = collectCanonicalScoutEvidence(scoutResult);
  assertScoutEvidenceCoverage(scoutResult, evidence);
  const blocked = resolveBlocked(resolved, payload);
  const mir = resolved.missionIntelligenceReport || null;
  const cognitiveTrace = resolveExplainabilityProjection(scoutResult, opts);

  return {
    spec: 'SPEC-174',
    evidenceSpec: 'SPEC-172',
    explainabilitySpec: 'SPEC-183',
    companies: buildCompanies(payload),
    prospects: payload.prospects || payload.people || [],
    buyingSignals: collectBuyingSignals(payload),
    evidence,
    confidence:
      payload.confidence != null
        ? Number(payload.confidence)
        : resolved.confidence != null
          ? Number(resolved.confidence)
          : null,
    confidenceBreakdown: payload.confidenceBreakdown || null,
    businessUnderstanding: mir?.businessUnderstanding || null,
    businessJudgment: mir?.businessJudgment || null,
    missionIntelligenceReport: mir,
    coverage: payload.coverage || payload.discoveryReport?.coverage || null,
    fitCandidates: payload.fitCandidates || [],
    watchCandidates: payload.watchCandidates || [],
    opportunities,
    outcome: resolved.status || (blocked ? 'blocked' : 'completed'),
    blocked,
    summary:
      asText(resolved.summary)
      || asText(payload.summary)
      || (blocked ? 'Discovery blocked under current criteria.' : 'Scout discovery completed.'),
    missionObjective: opts.missionObjective || payload.missionObjective || null,
    discoveryReport: payload.discoveryReport || null,
    discoveryStatus: payload.discoveryStatus || payload.discoveryReport?.status || null,
    discoveryConfidence: payload.discoveryConfidence || null,
    discoveryPlan: payload.discoveryPlan || null,
    intelligenceReport: resolved.intelligenceReport || payload.intelligenceReport || null,
    investigationState: resolved.investigationState || null,
    explainabilityGraph: resolved.explainabilityGraph || null,
    cognitiveTrace,
    qualifiedCount:
      payload.qualifiedCount != null
        ? Number(payload.qualifiedCount)
        : opportunities.length + (payload.fitCandidates || []).length,
    candidateUniverseCount: payload.candidateUniverse?.length || null,
    estimatedMarket: payload.universeEstimate || payload.discoveryReport?.estimatedMarket || null,
    providerExecution: payload.providerExecution || payload.providerReports || null,
    marketCoveragePct:
      payload.coveragePct != null
        ? payload.coveragePct
        : payload.discoveryReport?.marketCoveragePct ?? null,
    approvalConsumed: Boolean(opts.approvalConsumed),
    sourceResult: scoutResult,
  };
}

function mapCanonicalEvidenceToContribution(evidence = []) {
  return evidence.map((item) => ({
    id: item.id,
    label: item.claim || item.source,
    source: item.source,
    company: item.entityId || null,
    observedAt: item.observedAt || null,
    evidenceType: item.sourceType || null,
    provenance: item.provenance || null,
    confidence: item.confidence,
    originalRef: item.originalRef || null,
  }));
}

/**
 * Fail fast when canonical evidence existed before normalization but contribution is empty.
 * @param {object} artifact
 * @param {object} contributionPayload
 */
function assertScoutEvidenceHandoff(artifact = {}, contributionPayload = {}) {
  if (artifact.blocked === true || contributionPayload.blocked === true) return;

  const canonicalCount = Array.isArray(artifact.evidence) ? artifact.evidence.length : 0;
  if (canonicalCount === 0) return;

  const contributionEvidence = contributionPayload.evidence || contributionPayload.evidenceRefs || [];
  const contributionSignals = contributionPayload.buyingSignals || contributionPayload.signals || [];
  const hasContributionEvidence = Array.isArray(contributionEvidence) && contributionEvidence.length > 0;
  const hasContributionSignals = Array.isArray(contributionSignals) && contributionSignals.length > 0;

  if (!hasContributionEvidence && !hasContributionSignals) {
    throw scoutEvidenceHandoffError(
      'Scout evidence was present in the canonical discovery artifact but lost during contribution normalization.',
      {
        details: {
          canonicalEvidenceCount: canonicalCount,
          contributionEvidenceCount: 0,
        },
      }
    );
  }
}

/**
 * Resolve internal Scout reasoning artifacts for replay/debug/learning.
 * Never attach this object to AMO discovery contributions (SPEC-173).
 * @param {object} scoutResult
 * @returns {object}
 */
function resolveScoutInternalReasoning(scoutResult = {}) {
  const artifact = buildScoutDiscoveryArtifact(scoutResult);
  return {
    spec: 'SPEC-173',
    investigationState: artifact.investigationState || null,
    missionIntelligenceReport: artifact.missionIntelligenceReport || null,
    intelligenceReport: artifact.intelligenceReport || null,
    explainabilityGraph: artifact.explainabilityGraph || null,
    cognitiveTrace: artifact.cognitiveTrace || null,
    sourceResult: artifact.sourceResult || scoutResult,
  };
}

module.exports = {
  SCOUT_EVIDENCE_HANDOFF_VIOLATION,
  SCOUT_EVIDENCE_COVERAGE_VIOLATION,
  EVIDENCE_CLASSIFICATION,
  SCOUT_EVIDENCE_SOURCES,
  GRAPH_NODE_EVIDENCE_CLASSIFICATION,
  CANONICAL_EVIDENCE_NORMALIZATION_PATH,
  resolveScoutExecutionResult,
  resolveExplainabilityProjection,
  collectGraphEvidenceEntries,
  collectExportableEvidenceIdentities,
  collectCanonicalScoutEvidence,
  buildScoutDiscoveryArtifact,
  resolveScoutInternalReasoning,
  assertScoutEvidenceCoverage,
  assertScoutEvidenceHandoff,
  mapCanonicalEvidenceToContribution,
  normalizeCanonicalEvidenceItem,
  deduplicateCanonicalEvidence,
  evidenceIdentity,
};
