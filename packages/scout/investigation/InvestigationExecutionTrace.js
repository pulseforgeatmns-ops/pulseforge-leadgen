'use strict';

/**
 * SPEC-198 — Investigation Execution Trace (observability-only).
 *
 * Formats compact canonical traces from execution state already produced by
 * the investigation loop. Must not introduce parallel evaluation, selection,
 * or progression logic.
 */

const { asText } = require('../../max/scoutAcquisition/Types');

/**
 * Read trace metric fields from objects already held in canonical execution state.
 * Pure read — no evaluation, ranking, or provider resolution.
 *
 * @param {object|null} evaluation — candidate.evaluation at record time
 * @param {object|null} hypothesisRow — candidate.hypothesisState[hypothesisId] at record time
 * @param {number|null} rank — candidate.rank at record time
 * @returns {object}
 */
function readTraceMetricsFromExecutionState(evaluation = null, hypothesisRow = null, rank = null) {
  const qualification = evaluation && evaluation.qualification ? evaluation.qualification : {};
  const readiness = evaluation && evaluation.readiness ? evaluation.readiness : {};

  let confidence = 0;
  if (hypothesisRow && hypothesisRow.confidence != null) {
    const n = Number(hypothesisRow.confidence);
    confidence = Number.isFinite(n) ? Number(n.toFixed(2)) : 0;
  }

  return {
    qualification: asText(qualification.status) || null,
    readiness: asText(readiness.status || (evaluation && evaluation.readinessState)) || null,
    confidence,
    rank: rank != null ? Number(rank) : null,
  };
}

/**
 * Resolve the provider id recorded on the execution result.
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
    startedAt: asText(input.startedAt) || null,
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
  readTraceMetricsFromExecutionState,
  resolveProviderFromResult,
  buildInvestigationExecutionTrace,
};
