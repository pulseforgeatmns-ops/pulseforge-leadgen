'use strict';

/**
 * SPEC-177 — Provider Evidence Contract.
 * Every provider reports evidence produced, confidence, coverage, and limitations —
 * not merely raw search results.
 */

function buildProviderEvidenceReport(partial = {}) {
  return {
    providerId: partial.providerId || '',
    providerLabel: partial.providerLabel || partial.providerId || '',
    evidenceType: partial.evidenceType || null,
    task: partial.task || '',
    evidenceProduced: Array.isArray(partial.evidenceProduced) ? partial.evidenceProduced : [],
    confidence: partial.confidence != null ? Number(partial.confidence) : 0,
    coverage: partial.coverage != null ? Number(partial.coverage) : 0,
    limitations: Array.isArray(partial.limitations) ? partial.limitations : [],
    candidates: Array.isArray(partial.candidates) ? partial.candidates : [],
    rawResultCount: partial.rawResultCount != null ? Number(partial.rawResultCount) : 0,
    status: partial.status || 'completed',
    error: partial.error || null,
    executedAt: partial.executedAt || new Date().toISOString(),
  };
}

/**
 * Normalize a raw adapter report into the provider evidence contract.
 * @param {object} adapterReport
 * @param {object} assignment
 * @returns {object}
 */
function normalizeProviderReport(adapterReport = {}, assignment = {}) {
  const candidates = adapterReport.candidates || [];
  const errors = adapterReport.errors || [];
  const failed = adapterReport.available === false || errors.length > 0;
  const evidenceType = assignment.evidenceType || null;

  const evidenceProduced = inferEvidenceProduced(candidates, evidenceType, assignment);

  const confidence = computeReportConfidence({
    candidates,
    evidenceProduced,
    assignment,
    failed,
  });

  const coverage = computeReportCoverage({
    candidates,
    evidenceProduced,
    assignment,
    failed,
  });

  const limitations = [];
  if (failed) {
    limitations.push(
      errors[0]?.message ||
        `${assignment.providerId || 'Provider'} could not produce ${evidenceType || 'evidence'}`
    );
  }
  if (candidates.length === 0 && !failed) {
    limitations.push('No candidates returned for this evidence type.');
  }

  return buildProviderEvidenceReport({
    providerId: assignment.providerId,
    providerLabel: assignment.providerLabel,
    evidenceType,
    task: assignment.task,
    evidenceProduced,
    confidence,
    coverage,
    limitations,
    candidates,
    rawResultCount: candidates.length,
    status: failed ? 'failed' : candidates.length ? 'completed' : 'empty',
    error: failed ? errors[0]?.message || 'Provider unavailable' : null,
  });
}

function inferEvidenceProduced(candidates = [], evidenceType, assignment = {}) {
  const produced = new Set();
  if (evidenceType) produced.add(evidenceType);

  for (const row of candidates) {
    if (row.placeId || row.place_id) produced.add('identity');
    if (row.phone) produced.add('identity');
    if (row.address) produced.add('identity');
    if (row.registry_id || row.government_registry) produced.add('licensing');
    if (row.website || row.url) produced.add('contact_path');
    if (Array.isArray(row.people) && row.people.length) produced.add('decision_makers');
    if (Array.isArray(row.signals) && row.signals.length) {
      for (const sig of row.signals) produced.add(String(sig).toLowerCase());
    }
    if (row.property_count != null) produced.add('portfolio_evidence');
  }

  if (assignment.providerId === 'linkedin') produced.add('decision_makers');
  if (assignment.providerId === 'google_maps') {
    produced.add('identity');
    produced.add('reviews');
  }
  if (assignment.providerId === 'county_records') produced.add('licensing');

  return [...produced];
}

function computeReportConfidence({ candidates, evidenceProduced, assignment, failed }) {
  if (failed) return 0;
  const base = assignment.confidence || 0.5;
  if (!candidates.length) return Math.max(0, base * 0.2);
  const signalBoost = Math.min(0.3, evidenceProduced.length * 0.05);
  return Number(Math.min(0.99, base + signalBoost + Math.min(0.2, candidates.length * 0.02)).toFixed(3));
}

function computeReportCoverage({ candidates, evidenceProduced, assignment, failed }) {
  if (failed) return 0;
  const base = assignment.coverage || 0.5;
  if (!candidates.length) return 0;
  return Number(Math.min(1, base * (candidates.length > 0 ? 1 : 0.3)).toFixed(3));
}

/**
 * Merge provider reports for the same evidence type.
 * @param {object[]} reports
 * @returns {object}
 */
function mergeEvidenceReports(reports = []) {
  if (!reports.length) {
    return buildProviderEvidenceReport({ status: 'empty' });
  }

  const allCandidates = [];
  const allEvidence = new Set();
  const allLimitations = [];
  let totalConfidence = 0;
  let totalCoverage = 0;

  for (const report of reports) {
    allCandidates.push(...(report.candidates || []));
    for (const ev of report.evidenceProduced || []) allEvidence.add(ev);
    allLimitations.push(...(report.limitations || []));
    totalConfidence += report.confidence || 0;
    totalCoverage += report.coverage || 0;
  }

  const n = reports.length;
  return buildProviderEvidenceReport({
    providerId: reports.map((r) => r.providerId).join('+'),
    evidenceType: reports[0].evidenceType,
    task: reports[0].task,
    evidenceProduced: [...allEvidence],
    confidence: Number((totalConfidence / n).toFixed(3)),
    coverage: Number((totalCoverage / n).toFixed(3)),
    limitations: [...new Set(allLimitations)],
    candidates: allCandidates,
    rawResultCount: allCandidates.length,
    status: allCandidates.length ? 'completed' : 'empty',
  });
}

module.exports = {
  buildProviderEvidenceReport,
  normalizeProviderReport,
  inferEvidenceProduced,
  mergeEvidenceReports,
};
