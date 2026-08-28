'use strict';

/**
 * SPEC-198 — Investigation Execution Trace.
 * Every candidate-scoped investigation task persists a compact canonical trace.
 */

const { asText } = require('../../max/scoutAcquisition/Types');

function hypothesisConfidence(hypothesisState = {}, hypothesisId) {
  const row = hypothesisState && hypothesisId ? hypothesisState[hypothesisId] : null;
  if (!row || row.confidence == null) return 0;
  const n = Number(row.confidence);
  return Number.isFinite(n) ? Number(n.toFixed(2)) : 0;
}

/**
 * Snapshot qualification, readiness, hypothesis confidence, and rank for a candidate.
 *
 * @param {object} candidate
 * @returns {object}
 */
function snapshotCandidateMetrics(candidate = {}, hypothesisId = null) {
  const evaluation = candidate.evaluation || {};
  const qualification = evaluation.qualification || {};
  const readiness = evaluation.readiness || {};
  const hypothesisState = candidate.hypothesisState || {};

  return {
    qualification: asText(qualification.status) || null,
    readiness: asText(readiness.status || evaluation.readinessState) || null,
    confidence: hypothesisConfidence(hypothesisState, hypothesisId || null),
    rank: candidate.rank != null ? Number(candidate.rank) : candidate.prospectRank != null ? Number(candidate.prospectRank) : null,
  };
}

/**
 * Resolve the provider id used for an investigation task execution.
 *
 * @param {object} result
 * @param {object} task
 * @returns {string|null}
 */
function resolveProviderFromResult(result = {}, task = {}) {
  const reportProvider =
    result.reports &&
    result.reports[0] &&
    (result.reports[0].providerId || result.reports[0].provider);
  if (reportProvider) return asText(reportProvider);

  const taskProvider =
    (task.providers && task.providers[0] && (task.providers[0].providerId || task.providers[0].availableProvider)) ||
    null;
  return asText(taskProvider) || null;
}

/**
 * Build the compact canonical execution trace for one candidate investigation task.
 *
 * @param {object} input
 * @returns {object}
 */
function buildInvestigationExecutionTrace(input = {}) {
  const task = input.task || {};
  const before = input.before || {};
  const after = input.after || {};
  const result = input.result || {};

  const evidenceProduced =
    input.evidenceProduced ||
    (result.mergedReport && result.mergedReport.evidenceProduced) ||
    task.evidenceProduced ||
    [];

  return {
    candidateId: asText(task.candidateId || task.entityId) || null,
    candidateName: asText(task.entityName || task.candidateContext && task.candidateContext.businessName) || null,
    hypothesisId: asText(task.hypothesisId) || null,
    gap: asText(task.gap) || null,
    provider: resolveProviderFromResult(result, task),
    startedAt: asText(input.startedAt || task.startedAt) || null,
    completedAt: asText(input.completedAt || task.completedAt) || null,
    evidenceProduced: Array.isArray(evidenceProduced) ? evidenceProduced.slice() : [],
    qualificationBefore: before.qualification != null ? before.qualification : null,
    qualificationAfter: after.qualification != null ? after.qualification : null,
    readinessBefore: before.readiness != null ? before.readiness : null,
    readinessAfter: after.readiness != null ? after.readiness : null,
    confidenceBefore: before.confidence != null ? before.confidence : 0,
    confidenceAfter: after.confidence != null ? after.confidence : 0,
    rankBefore: before.rank != null ? before.rank : null,
    rankAfter: after.rank != null ? after.rank : null,
  };
}

module.exports = {
  snapshotCandidateMetrics,
  resolveProviderFromResult,
  buildInvestigationExecutionTrace,
  hypothesisConfidence,
};
