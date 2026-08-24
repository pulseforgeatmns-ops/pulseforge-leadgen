'use strict';

/**
 * SPEC-146 — Evidence Conflict Resolution Engine (ECRE).
 * ADR-065 — Conflicting Evidence Is Intelligence.
 *
 * Detect → Explain → Resolve → Expose remaining uncertainty.
 */

const { detectEvidenceConflicts, detectAllEvidenceConflicts } = require('./ConflictDetection');
const {
  resolveEvidenceConflict,
  resolveAllConflicts,
  applyConflictConfidenceAdjustment,
} = require('./ConflictResolution');
const { buildConflictReport, buildEvidenceConflictsSection } = require('./ConflictReport');
const {
  createProviderConflictLearningStore,
  loadConflictLearningFromMemory,
  exportConflictLearningForMemory,
} = require('./ProviderConflictLearning');
const {
  CONFLICT_CATEGORIES,
  CONFLICT_SEVERITY,
  RESOLUTION_STRATEGIES,
  CONFLICT_SUBJECTS,
  buildEvidenceConflict,
  buildConflictResolutionResult,
} = require('./types');

/**
 * Run the full ECRE pipeline for one candidate.
 * @param {object} candidate
 * @param {object[]} [evidence]
 * @param {object} [opts]
 * @returns {object}
 */
function resolveEvidenceConflicts(candidate, evidence, opts = {}) {
  const mergedEvidence = evidence || [
    ...(candidate.evidence || []),
    ...(candidate.signals || []).map((s) => ({
      ...s,
      source: s.source || 'signal',
      label: s.label || s.text,
    })),
  ];

  const detected = detectEvidenceConflicts(candidate, mergedEvidence);
  const resolved = resolveAllConflicts(detected, opts);
  const baseConfidence = opts.baseConfidence != null ? opts.baseConfidence : 0.85;
  const adjustedConfidence = applyConflictConfidenceAdjustment(baseConfidence, resolved);

  const learning = opts.learning || createProviderConflictLearningStore();
  for (const conflict of resolved) {
    if (conflict.resolution?.resolved && conflict.conflictingClaims?.length) {
      const best = conflict.conflictingClaims[0];
      learning.recordResolution(conflict, best?.source);
    } else {
      for (const claim of conflict.conflictingClaims || []) {
        learning.recordConflictOutcome(claim.source, {
          resolved: false,
          subject: conflict.subject,
          observedAt: claim.observedAt,
        });
      }
    }
  }

  return buildConflictReport({
    entityId: candidate.id,
    conflicts: resolved,
    baseConfidence,
    adjustedConfidence,
    learning: learning.summarize(),
  });
}

/**
 * Run ECRE across a candidate universe (pipeline stage).
 * @param {object} input
 * @returns {object}
 */
function runEvidenceConflictResolution(input = {}) {
  const candidates = input.candidates || input.candidateUniverse?.candidates || [];
  const evidenceByCandidate = input.evidenceByCandidate || input.evidenceCollection?.evidenceByCandidate || [];
  const opts = input.opts || {};
  const learning = opts.learning || loadConflictLearningFromMemory(opts.memory || {});

  const evidenceMap = new Map();
  for (const entry of evidenceByCandidate) {
    evidenceMap.set(String(entry.candidateId || entry.id), entry.evidence || entry.items || []);
  }

  const allConflicts = [];
  const byCandidate = [];
  let totalDetected = 0;
  let totalResolved = 0;
  let totalOutstanding = 0;

  for (const candidate of candidates) {
    const evidence = evidenceMap.get(String(candidate.id)) || candidate.evidence || [];
    const result = resolveEvidenceConflicts(candidate, evidence, {
      ...opts,
      learning,
      baseConfidence: candidate.confidence || input.baseConfidence || 0.85,
    });

    allConflicts.push(...result.conflicts);
    totalDetected += result.detected;
    totalResolved += result.resolved;
    totalOutstanding += result.outstanding;

    byCandidate.push({
      candidateId: candidate.id,
      candidateName: candidate.name,
      ...result,
    });
  }

  const baseConfidence = input.baseConfidence != null ? input.baseConfidence : 0.91;
  const section = buildEvidenceConflictsSection(allConflicts, { baseConfidence });

  return {
    version: 'SPEC-146',
    byCandidate,
    conflicts: allConflicts,
    detected: totalDetected,
    resolved: totalResolved,
    outstanding: totalOutstanding,
    evidenceConflicts: section,
    providerLearning: learning.summarize(),
    providerConflictLearning: exportConflictLearningForMemory(learning),
    adjustedConfidence: section.summary.recommendationConfidenceAfter,
    investigationTasks: allConflicts
      .filter((c) => !c.resolution?.resolved)
      .map((c) => c.investigationTask)
      .filter(Boolean),
  };
}

/**
 * Convert ECRE conflicts to legacy contradiction format for backward compatibility.
 * @param {object[]} conflicts
 * @returns {object[]}
 */
function conflictsToLegacyFormat(conflicts = []) {
  return conflicts.map((c) => ({
    id: c.id,
    entityId: c.entityId,
    description: c.description || c.resolution?.reason || c.unresolvedReason,
    field: c.subject,
    resolved: c.resolution?.resolved === true,
    confidencePenalty: c.confidencePenalty,
    workingEstimate: c.resolution?.workingEstimate,
    strategy: c.resolution?.strategy,
    severity: c.severity,
    conflictingClaims: c.conflictingClaims,
  }));
}

module.exports = {
  CONFLICT_CATEGORIES,
  CONFLICT_SEVERITY,
  RESOLUTION_STRATEGIES,
  CONFLICT_SUBJECTS,
  buildEvidenceConflict,
  buildConflictResolutionResult,
  detectEvidenceConflicts,
  detectAllEvidenceConflicts,
  resolveEvidenceConflict,
  resolveAllConflicts,
  applyConflictConfidenceAdjustment,
  resolveEvidenceConflicts,
  runEvidenceConflictResolution,
  buildEvidenceConflictsSection,
  buildConflictReport,
  conflictsToLegacyFormat,
  createProviderConflictLearningStore,
  loadConflictLearningFromMemory,
  exportConflictLearningForMemory,
};
