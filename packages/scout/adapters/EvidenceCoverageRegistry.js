'use strict';

/**
 * SPEC-174 — Canonical Evidence Coverage Registry.
 * Declarative map of every Scout evidence location and its export classification.
 * The Canonical Evidence Adapter iterates EXPORTABLE entries only.
 */

const { GRAPH_NODE_TYPES } = require('../investigation/types');

const EVIDENCE_CLASSIFICATION = Object.freeze({
  EXPORTABLE: 'EXPORTABLE',
  INTERNAL: 'INTERNAL',
  DERIVED: 'DERIVED',
  TRANSIENT: 'TRANSIENT',
  INVALID: 'INVALID',
});

const GRAPH_NODE_EVIDENCE_CLASSIFICATION = Object.freeze({
  [GRAPH_NODE_TYPES.EVIDENCE]: EVIDENCE_CLASSIFICATION.EXPORTABLE,
  [GRAPH_NODE_TYPES.SOURCE]: EVIDENCE_CLASSIFICATION.INTERNAL,
  [GRAPH_NODE_TYPES.CLAIM]: EVIDENCE_CLASSIFICATION.DERIVED,
  [GRAPH_NODE_TYPES.HYPOTHESIS]: EVIDENCE_CLASSIFICATION.INTERNAL,
  [GRAPH_NODE_TYPES.MISSION]: EVIDENCE_CLASSIFICATION.INTERNAL,
  [GRAPH_NODE_TYPES.MARKET]: EVIDENCE_CLASSIFICATION.INTERNAL,
  [GRAPH_NODE_TYPES.CANDIDATE]: EVIDENCE_CLASSIFICATION.INTERNAL,
  [GRAPH_NODE_TYPES.DECISION_MAKER]: EVIDENCE_CLASSIFICATION.INTERNAL,
  [GRAPH_NODE_TYPES.CONFIDENCE]: EVIDENCE_CLASSIFICATION.INTERNAL,
});

const NON_EXPORTABLE_SCOUT_LOCATIONS = Object.freeze([
  { path: 'investigationGraph.hypothesis', classification: EVIDENCE_CLASSIFICATION.INTERNAL },
  { path: 'investigationGraph.claim', classification: EVIDENCE_CLASSIFICATION.DERIVED },
  { path: 'investigationGraph.mission', classification: EVIDENCE_CLASSIFICATION.INTERNAL },
  { path: 'investigationGraph.market', classification: EVIDENCE_CLASSIFICATION.INTERNAL },
  { path: 'investigationGraph.candidate', classification: EVIDENCE_CLASSIFICATION.INTERNAL },
  { path: 'investigationGraph.decision_maker', classification: EVIDENCE_CLASSIFICATION.INTERNAL },
  { path: 'investigationGraph.confidence', classification: EVIDENCE_CLASSIFICATION.INTERNAL },
  { path: 'investigationGraph.source', classification: EVIDENCE_CLASSIFICATION.INTERNAL },
  { path: 'heuristicActivation', classification: EVIDENCE_CLASSIFICATION.DERIVED },
  { path: 'searchPlanner', classification: EVIDENCE_CLASSIFICATION.INTERNAL },
  { path: 'investigationState.hypotheses', classification: EVIDENCE_CLASSIFICATION.INTERNAL },
  { path: 'missionIntelligenceReport.recommendation', classification: EVIDENCE_CLASSIFICATION.INTERNAL },
]);

const CANDIDATE_BUCKETS = Object.freeze([
  'opportunities',
  'acquisitionOpportunities',
  'fitCandidates',
  'watchCandidates',
  'evaluatedCandidates',
]);

function entityIdFromCandidate(candidate = {}) {
  return candidate.companyId || candidate.id || candidate.candidate_id || candidate.name || null;
}

function collectDiscoveryReportEvidenceSources(resolved, payload, target) {
  const report =
    payload.discoveryReport ||
    resolved.discoveryReport ||
    null;
  if (!report || !Array.isArray(report.evidenceSources)) return;

  for (const [index, row] of report.evidenceSources.entries()) {
    if (!row || row.succeeded !== true) continue;
    const sourceName = row.source || row.id || 'discovery_source';
    target.push({
      raw: {
        id: `ev-discovery-source-${index}`,
        label: sourceName,
        source: sourceName,
        sourceKind: 'discovery_source',
        provenance: {
          kind: 'discovery_report',
          source: sourceName,
          attempted: row.attempted,
          succeeded: row.succeeded,
        },
      },
      context: {
        sourcePath: 'discoveryReport.evidenceSources',
        index,
        fallbackId: `ev-discovery-source-${index}`,
      },
    });
  }
}

/**
 * Declarative registry of Scout evidence sources.
 * Each EXPORTABLE entry exposes a collector returning { raw, context } pairs.
 */
const SCOUT_EVIDENCE_SOURCES = Object.freeze([
  {
    id: 'result.evidenceRefs',
    path: 'result.evidenceRefs',
    classification: EVIDENCE_CLASSIFICATION.EXPORTABLE,
    collect(ctx) {
      const refs = ctx.resolved.evidenceRefs;
      if (!Array.isArray(refs)) return [];
      return refs.map((raw, index) => ({
        raw,
        context: {
          sourcePath: 'result.evidenceRefs',
          index,
          fallbackId: raw?.id || `result_evidenceRefs_${index}`,
        },
      }));
    },
  },
  {
    id: 'payload.evidence',
    path: 'payload.evidence',
    classification: EVIDENCE_CLASSIFICATION.EXPORTABLE,
    collect(ctx) {
      const refs = ctx.payload.evidence;
      if (!Array.isArray(refs)) return [];
      return refs.map((raw, index) => ({
        raw,
        context: {
          sourcePath: 'payload.evidence',
          index,
          fallbackId: raw?.id || `payload_evidence_${index}`,
        },
      }));
    },
  },
  {
    id: 'payload.evidenceRefs',
    path: 'payload.evidenceRefs',
    classification: EVIDENCE_CLASSIFICATION.EXPORTABLE,
    collect(ctx) {
      const refs = ctx.payload.evidenceRefs;
      if (!Array.isArray(refs)) return [];
      return refs.map((raw, index) => ({
        raw,
        context: {
          sourcePath: 'payload.evidenceRefs',
          index,
          fallbackId: raw?.id || `payload_evidenceRefs_${index}`,
        },
      }));
    },
  },
  ...CANDIDATE_BUCKETS.flatMap((bucket) => [
    {
      id: `payload.${bucket}.evidenceRefs`,
      path: `payload.${bucket}[].evidenceRefs`,
      classification: EVIDENCE_CLASSIFICATION.EXPORTABLE,
      collect(ctx) {
        const entries = [];
        for (const candidate of ctx.payload[bucket] || []) {
          const entityId = entityIdFromCandidate(candidate);
          for (const [index, raw] of (candidate.evidenceRefs || []).entries()) {
            entries.push({
              raw,
              context: {
                sourcePath: `payload.${bucket}[].evidenceRefs`,
                entityId,
                index,
                fallbackId: raw?.id || `${bucket}_evidenceref_${index}`,
              },
            });
          }
        }
        return entries;
      },
    },
    {
      id: `payload.${bucket}.evidence`,
      path: `payload.${bucket}[].evidence`,
      classification: EVIDENCE_CLASSIFICATION.EXPORTABLE,
      collect(ctx) {
        const entries = [];
        for (const candidate of ctx.payload[bucket] || []) {
          const entityId = entityIdFromCandidate(candidate);
          for (const [index, raw] of (candidate.evidence || []).entries()) {
            entries.push({
              raw,
              context: {
                sourcePath: `payload.${bucket}[].evidence`,
                entityId,
                index,
                fallbackId: raw?.id || `${bucket}_evidence_${index}`,
              },
            });
          }
        }
        return entries;
      },
    },
  ]),
  {
    id: 'payload.candidateUniverse.evidence',
    path: 'payload.candidateUniverse[].evidence',
    classification: EVIDENCE_CLASSIFICATION.EXPORTABLE,
    collect(ctx) {
      const entries = [];
      for (const [index, candidate] of (ctx.payload.candidateUniverse || []).entries()) {
        const entityId = entityIdFromCandidate(candidate);
        for (const [evIndex, raw] of (candidate.evidence || []).entries()) {
          entries.push({
            raw,
            context: {
              sourcePath: 'payload.candidateUniverse[].evidence',
              entityId,
              index: evIndex,
              fallbackId: raw?.id || `candidateUniverse_evidence_${index}_${evIndex}`,
            },
          });
        }
        for (const [evIndex, raw] of (candidate.evidenceRefs || []).entries()) {
          entries.push({
            raw,
            context: {
              sourcePath: 'payload.candidateUniverse[].evidenceRefs',
              entityId,
              index: evIndex,
              fallbackId: raw?.id || `candidateUniverse_evidenceref_${index}_${evIndex}`,
            },
          });
        }
      }
      return entries;
    },
  },
  {
    id: 'investigationGraph.evidence',
    path: 'investigationGraph.nodes[EVIDENCE]',
    classification: EVIDENCE_CLASSIFICATION.EXPORTABLE,
    collect(ctx) {
      return ctx.graphEvidenceEntries || [];
    },
  },
  {
    id: 'discoveryReport.evidenceSources',
    path: 'discoveryReport.evidenceSources',
    classification: EVIDENCE_CLASSIFICATION.EXPORTABLE,
    collect(ctx) {
      const entries = [];
      collectDiscoveryReportEvidenceSources(ctx.resolved, ctx.payload, entries);
      return entries;
    },
  },
]);

function getExportableEvidenceSources() {
  return SCOUT_EVIDENCE_SOURCES.filter(
    (source) => source.classification === EVIDENCE_CLASSIFICATION.EXPORTABLE
  );
}

function buildEvidenceCollectionContext(scoutResult, resolved, payload, graphEvidenceEntries) {
  return {
    scoutResult,
    resolved,
    payload,
    graphEvidenceEntries,
  };
}

function collectExportableEvidenceEntries(scoutResult, resolved, payload, graphEvidenceEntries) {
  const ctx = buildEvidenceCollectionContext(scoutResult, resolved, payload, graphEvidenceEntries);
  const entries = [];
  for (const source of getExportableEvidenceSources()) {
    const collected = source.collect(ctx);
    if (Array.isArray(collected) && collected.length) {
      entries.push(...collected);
    }
  }
  return entries;
}

function isExportableGraphNode(nodeType) {
  const normalized = String(nodeType || '').toLowerCase();
  return GRAPH_NODE_EVIDENCE_CLASSIFICATION[normalized] === EVIDENCE_CLASSIFICATION.EXPORTABLE
    || GRAPH_NODE_EVIDENCE_CLASSIFICATION[nodeType] === EVIDENCE_CLASSIFICATION.EXPORTABLE;
}

module.exports = {
  EVIDENCE_CLASSIFICATION,
  GRAPH_NODE_EVIDENCE_CLASSIFICATION,
  NON_EXPORTABLE_SCOUT_LOCATIONS,
  SCOUT_EVIDENCE_SOURCES,
  CANDIDATE_BUCKETS,
  getExportableEvidenceSources,
  collectExportableEvidenceEntries,
  collectDiscoveryReportEvidenceSources,
  isExportableGraphNode,
};
