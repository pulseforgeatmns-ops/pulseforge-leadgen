'use strict';

/**
 * SPEC-184 — Provider Execution Continuity (ADR-099).
 *
 * Provider execution telemetry is observational — it must never influence
 * business reasoning. It exists for explainability, debugging, operational
 * visibility, and provider health.
 */

const PROVIDER_LABELS = Object.freeze({
  google_places: 'Google Places',
  google_maps: 'Google Maps',
  linkedin: 'LinkedIn',
  county_records: 'County Records',
  apollo: 'Apollo',
});

function providerLabel(providerId) {
  const key = String(providerId || '').toLowerCase();
  return PROVIDER_LABELS[key] || (providerId ? String(providerId) : 'Unknown provider');
}

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

/**
 * Normalize one provider report into canonical ProviderExecution observational shape.
 * @param {object} report
 * @returns {object|null}
 */
function normalizeProviderExecutionRecord(report = {}) {
  if (!report || typeof report !== 'object') return null;

  if (report.observational === true && report.spec === 'SPEC-184') {
    return { ...report };
  }

  const execution = report.execution || null;
  const queries = execution && Array.isArray(execution.queries) ? execution.queries : [];
  const primaryQuery = queries[0] || {};
  const totals = (execution && execution.totals) || {};
  const execErrors = (execution && execution.errors) || [];
  const reportError = report.error || null;

  const httpStatus =
    primaryQuery.httpStatus != null
      ? Number(primaryQuery.httpStatus)
      : execErrors[0] && execErrors[0].httpStatus != null
        ? Number(execErrors[0].httpStatus)
        : null;

  const googleStatus =
    asText(primaryQuery.googleStatus)
    || asText(execErrors[0] && execErrors[0].googleStatus)
    || null;

  const googleError =
    asText(primaryQuery.googleError)
    || asText(execErrors[0] && execErrors[0].message)
    || asText(reportError)
    || null;

  const latencyMs =
    primaryQuery.latencyMs != null
      ? Number(primaryQuery.latencyMs)
      : totals.latencyMs != null
        ? Number(totals.latencyMs)
        : null;

  const rawResults =
    report.results != null
      ? Number(report.results)
      : totals.results != null
        ? Number(totals.results)
        : report.rawResultCount != null
          ? Number(report.rawResultCount)
          : null;

  const mappedCount =
    report.qualified != null
      ? Number(report.qualified)
      : report.mappedCandidateCount != null
        ? Number(report.mappedCandidateCount)
        : Array.isArray(report.candidates)
          ? report.candidates.length
          : null;

  const filtered =
    rawResults != null && mappedCount != null && rawResults > mappedCount
      ? rawResults - mappedCount
      : null;

  let reason = null;
  if (report.status === 'failed') {
    reason = googleError || reportError || 'Provider execution failed.';
  } else if (mappedCount === 0 && rawResults != null && rawResults > 0) {
    reason = 'No businesses met qualification criteria.';
  } else if (mappedCount === 0 && googleStatus === 'ZERO_RESULTS') {
    reason = 'Provider returned zero results for the query.';
  } else if (execution && execution.abortReason) {
    reason = `Provider aborted before HTTP: ${execution.abortReason}.`;
  }

  return {
    providerId: asText(report.providerId) || null,
    provider: providerLabel(report.providerId || report.providerLabel),
    evidenceRequirement: asText(report.evidenceType) || null,
    query: asText(primaryQuery.query) || null,
    httpStatus,
    googleStatus,
    googleError,
    latencyMs,
    results: rawResults,
    qualified: mappedCount,
    filtered,
    reason,
    status: asText(report.status) || null,
    executed: execution ? execution.executed !== false : report.status !== 'failed',
    abortReason: execution ? execution.abortReason || null : null,
    queries,
    execution,
    observational: true,
    spec: 'SPEC-184',
  };
}

/**
 * @param {object[]|null|undefined} records
 * @returns {object[]}
 */
function normalizeProviderExecution(records) {
  if (!Array.isArray(records)) return [];
  return records.map(normalizeProviderExecutionRecord).filter(Boolean);
}

/**
 * Extract provider execution from a discovery stage output envelope.
 * @param {object} output
 * @returns {object[]}
 */
function extractProviderExecutionFromOutput(output = {}) {
  const fromPayload = output.discoveryPayload && output.discoveryPayload.providerExecution;
  if (Array.isArray(fromPayload) && fromPayload.length) {
    return normalizeProviderExecution(fromPayload);
  }

  const scoutPayload =
    output.scoutResult &&
    (output.scoutResult.payload || output.scoutResult.intelligenceResult?.payload);
  if (scoutPayload) {
    const upstream = scoutPayload.providerExecution || scoutPayload.providerReports;
    if (Array.isArray(upstream) && upstream.length) {
      return normalizeProviderExecution(upstream);
    }
  }

  const executionDiscovery =
    output.executionResult &&
    output.executionResult.discovery &&
    output.executionResult.discovery.payload &&
    output.executionResult.discovery.payload.providerExecution;
  if (Array.isArray(executionDiscovery) && executionDiscovery.length) {
    return normalizeProviderExecution(executionDiscovery);
  }

  return [];
}

/**
 * Attach provider diagnostics to a TME validation error without mutating business logic.
 * @param {Error} err
 * @param {object} output
 * @returns {Error}
 */
function attachProviderDiagnostics(err, output = {}) {
  const providerExecution = extractProviderExecutionFromOutput(output);
  if (!providerExecution.length) return err;
  err.details = {
    ...(err.details && typeof err.details === 'object' ? err.details : {}),
    providerExecution,
  };
  return err;
}

/**
 * @param {object} record
 * @returns {string[]}
 */
function formatProviderExecutionRecordLines(record = {}) {
  if (!record || typeof record !== 'object') return [];
  const lines = [];
  if (record.provider) {
    lines.push(`Provider: ${record.provider}`);
  }
  if (record.evidenceRequirement) {
    lines.push(`Evidence Requirement: ${record.evidenceRequirement}`);
  }
  if (record.query) {
    lines.push(`Query: ${record.query}`);
  }
  if (record.httpStatus != null) {
    lines.push(`HTTP: ${record.httpStatus}`);
  }
  if (record.googleStatus) {
    lines.push(`Google Status: ${record.googleStatus}`);
  } else if (record.googleError && !record.httpStatus) {
    lines.push(String(record.googleError));
  }
  if (record.latencyMs != null) {
    lines.push(`Latency: ${record.latencyMs} ms`);
  }
  if (record.results != null) {
    lines.push(`Results: ${record.results}`);
  }
  if (record.qualified != null) {
    lines.push(`Qualified: ${record.qualified}`);
  }
  if (record.filtered != null && record.filtered > 0) {
    lines.push(`Filtered: ${record.filtered}`);
  }
  if (record.reason) {
    lines.push(`Reason: ${record.reason}`);
  }
  return lines;
}

/**
 * @param {object[]} records
 * @returns {string[]}
 */
function formatProviderExecutionLines(records = []) {
  const normalized = normalizeProviderExecution(records);
  if (!normalized.length) return [];

  const lines = ['Provider Execution', ''];
  for (let i = 0; i < normalized.length; i += 1) {
    const block = formatProviderExecutionRecordLines(normalized[i]);
    if (!block.length) continue;
    if (i > 0) lines.push('');
    lines.push(...block);
  }
  return lines;
}

/**
 * @param {object[]} records
 * @returns {string}
 */
function formatProviderExecutionProse(records = []) {
  return formatProviderExecutionLines(records).join('\n').trim();
}

module.exports = {
  PROVIDER_LABELS,
  providerLabel,
  normalizeProviderExecutionRecord,
  normalizeProviderExecution,
  extractProviderExecutionFromOutput,
  attachProviderDiagnostics,
  formatProviderExecutionRecordLines,
  formatProviderExecutionLines,
  formatProviderExecutionProse,
};
