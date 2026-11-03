'use strict';

// Observational only. Summarize the whole research pass independently of the
// 20-row detail sample. Fixed vocabularies and work/output bounds prevent raw
// provider text or arbitrarily large receipts from reaching durable logs.
const LIMITS = Object.freeze({ reports: 1000, observations: 10000, issueGroups: 64 });
const PROVIDERS = ['google_places', 'google_maps', 'county_records', 'linkedin', 'facebook', 'website', 'news', 'prospeo', 'existing_pf', 'apollo'];
const STATUSES = ['completed', 'partial', 'blocked', 'failed', 'skipped', 'success', 'empty'];
const DENIED = ['REQUEST_DENIED', 'google_places_status_REQUEST_DENIED'];
const QUOTA = ['OVER_QUERY_LIMIT', 'RESOURCE_EXHAUSTED', 'google_places_status_OVER_QUERY_LIMIT'];
const UNAVAILABLE = ['provider_unavailable', 'external_discovery_capability_unavailable', 'adapter_unavailable'];
const SCOPE = ['insufficient_coverage', 'invalid_target'];
const CODES = [...DENIED, ...QUOTA, ...UNAVAILABLE, ...SCOPE, 'provider_error', 'provider_failed',
  'discovery_provider_failed', 'INVALID_REQUEST', 'UNKNOWN_ERROR', 'UNKNOWN_STATUS', 'enrichment_unavailable',
  'facility_research_identity_only', 'facility_research_request_scope_mismatch'];
const CAUSES = ['facility_research_identity_only', 'facility_research_request_scope_mismatch'];
const choice = (value, allowed) => allowed.includes(value) ? value : 'unknown';
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);

function summarizeResearchProviderIssues(result = {}, reports) {
  const groups = new Map(), providerCounts = new Map();
  const seen = { denied: false, quota: false, failure: false, unavailable: false, insufficient_coverage: false, invalid_target: false };
  let observations = 0, errorObservations = 0, queryObservations = 0, omittedObservations = 0;
  let malformed = 0, unknownErrorCount = 0, failedWithoutCauseCount = 0, omittedIssueGroups = 0;
  let successfulReports = 0;
  function observe(provider, error) {
    if (!object(error)) malformed++;
    const code = choice(error?.code, CODES), status = error?.googleStatus == null ? null : choice(error.googleStatus, CODES);
    const cause = CAUSES.includes(error?.causeCode) ? error.causeCode : null;
    const codes = [code, status];
    if (code === 'unknown' || status === 'unknown') unknownErrorCount++;
    if (codes.some(x => DENIED.includes(x))) seen.denied = true;
    else if (codes.some(x => QUOTA.includes(x))) seen.quota = true;
    else if (UNAVAILABLE.includes(code) && (status === null || UNAVAILABLE.includes(status))) seen.unavailable = true;
    else if (SCOPE.includes(code) && status === null) seen[code] = true;
    else seen.failure = true; // Unknown and mixed failures are never optional unavailability.
    const key = JSON.stringify([provider, code, status, cause]);
    if (groups.has(key)) groups.get(key).observations++;
    else if (groups.size < LIMITS.issueGroups) groups.set(key, { provider, code, status, cause, observations: 1 });
    else omittedIssueGroups++;
  }
  function scan(list, visit) {
    if (list === undefined) return;
    if (!Array.isArray(list)) { malformed++; return; }
    const available = Math.min(list.length, LIMITS.observations - observations);
    omittedObservations += list.length - available;
    for (let i = 0; i < available; i++) { observations++; visit(list[i]); }
  }
  scan(result.errors, error => { errorObservations++; observe('discovery', error); });
  if (result.payload?.blockerCode != null) observe('discovery', { code: result.payload.blockerCode });
  const reportsTotal = Array.isArray(reports) ? reports.length : null;
  const reportsScanned = Math.min(reportsTotal || 0, LIMITS.reports);
  for (let i = 0; i < reportsScanned; i++) {
    const report = reports[i];
    if (!object(report)) { malformed++; continue; }
    if (report.execution != null && !object(report.execution)) malformed++;
    const provider = choice(report.providerId, PROVIDERS), status = choice(report.status, STATUSES);
    const key = `${provider}:${status}`;
    providerCounts.set(key, { provider, status, reports: (providerCounts.get(key)?.reports || 0) + 1 });
    if (status === 'unknown') malformed++;
    if (['completed', 'success', 'empty'].includes(status)) successfulReports++;
    let causes = 0;
    const error = value => { causes++; errorObservations++; observe(provider, value); };
    scan(report.errors, error);
    scan(report.execution?.errors, error);
    scan(report.execution?.queries, query => {
      queryObservations++;
      if (!object(query)) { malformed++; return; }
      if (query.googleStatus != null && !['OK', 'ZERO_RESULTS'].includes(query.googleStatus)) {
        causes++; observe(provider, { code: query.googleStatus });
      } else if (Number.isInteger(query.httpStatus) && query.httpStatus >= 400) {
        causes++; observe(provider, { code: 'provider_error' });
      }
    });
    if (['failed', 'blocked', 'partial'].includes(status) && !causes) {
      failedWithoutCauseCount++; seen.failure = true;
    }
  }
  const omittedReports = reportsTotal == null ? 0 : reportsTotal - reportsScanned;
  const complete = reportsTotal !== null && !omittedReports && !omittedObservations && !malformed && !omittedIssueGroups;
  const blocker = seen.denied ? 'provider_denied' : seen.quota ? 'provider_quota' : seen.failure ? 'provider_failed'
    : !complete ? 'diagnostic_incomplete' : seen.invalid_target ? 'invalid_target'
      : seen.insufficient_coverage ? 'insufficient_coverage' : seen.unavailable ? 'provider_unavailable' : 'unknown';
  const completed = ['completed', 'success'].includes(result.status || result.payload?.outcome)
    && result.blocked !== true && result.payload?.blocked !== true && !result.payload?.blockerCode;
  const nonBlocking = complete && completed && successfulReports > 0 && blocker === 'provider_unavailable';
  return {
    diagnostic: { blocker: nonBlocking || (complete && completed && successfulReports > 0 && blocker === 'unknown') ? 'none' : blocker,
      nonBlockingProviderIssue: nonBlocking ? 'provider_unavailable' : null, providerIssuesComplete: complete },
    summary: { schemaVersion: 1, scope: 'research_pass_observations', complete, limits: LIMITS,
      reportsTotal, reportsScanned, omittedReports, observationsScanned: observations, omittedObservations,
      malformedObservations: malformed, errorObservations, queryObservations, unknownErrorCount, failedWithoutCauseCount,
      // Counts are observations, not unique HTTP calls, charges or admissions.
      issuesTruncated: omittedIssueGroups > 0, omittedIssueObservations: omittedIssueGroups,
      providerOutcomes: [...providerCounts.values()], issues: [...groups.values()] },
  };
}

module.exports = { summarizeResearchProviderIssues };
