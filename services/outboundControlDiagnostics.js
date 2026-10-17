'use strict';

// Logging-only projections. Never feed these values back into control decisions.
function count(value) {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : null;
}
function correlation(tenantId, cycleId) {
  const tenant = typeof tenantId === 'number' ? String(tenantId) : tenantId;
  return {
    tenantId: typeof tenant === 'string' && /^[1-9][0-9]{0,9}$/.test(tenant) ? tenant : null,
    cycleId: typeof cycleId === 'string' && (/^moc_[1-9][0-9]{0,9}_[a-f0-9]{16}$/.test(cycleId)
      || /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(cycleId)) ? cycleId : null,
  };
}
function choice(value, allowed) { return allowed.includes(value) ? value : 'unknown'; }
// New observational receipts deliberately bypass legacy evidence/control fields.
// Explicit null means unavailable; only older payloads fall back to execution.
function discoveryProviderReceipts(payload = {}) {
  if (!payload || typeof payload !== 'object') return undefined;
  return Object.hasOwn(payload, 'providerReceipts') ? payload.providerReceipts : payload.providerExecution;
}
function discoveryDiagnostic(discovery) {
  const result = discovery?.result || discovery || {};
  const payload = result.payload || {};
  const receipts = discoveryProviderReceipts(payload);
  const reports = Array.isArray(receipts) ? receipts.slice(0, 20) : [];
  // Exact code matching only. Error messages, URLs and arbitrary provider fields never leave here.
  const errors = [...(Array.isArray(result.errors) ? result.errors.slice(0, 100) : []),
    ...reports.flatMap(row => [
      ...(Array.isArray(row?.errors) ? row.errors.slice(0, 100) : []),
      ...(Array.isArray(row?.execution?.errors) ? row.execution.errors.slice(0, 100) : []),
    ])].slice(0, 100);
  const codes = [payload.blockerCode, ...errors.flatMap(row => [row?.code, row?.googleStatus])];
  let blocker = 'unknown';
  if (discovery?.kind === 'backoff') blocker = 'backoff';
  else if (codes.some(code => ['REQUEST_DENIED', 'google_places_status_REQUEST_DENIED'].includes(code))) blocker = 'provider_denied';
  else if (codes.some(code => ['OVER_QUERY_LIMIT', 'RESOURCE_EXHAUSTED', 'google_places_status_OVER_QUERY_LIMIT'].includes(code))) blocker = 'provider_quota';
  else if (codes.some(code => ['provider_unavailable', 'external_discovery_capability_unavailable', 'adapter_unavailable'].includes(code))) blocker = 'provider_unavailable';
  else if (codes.some(code => ['provider_error', 'discovery_provider_failed'].includes(code))) blocker = 'provider_failed';
  else if (codes.includes('insufficient_coverage')) blocker = 'insufficient_coverage';
  else if (codes.includes('invalid_target')) blocker = 'invalid_target';
  return {
    kind: choice(discovery?.kind, ['backoff', 'investigated', 'reused', 'skipped']),
    outcome: choice(result.status || payload.outcome, ['completed', 'partial', 'blocked', 'failed', 'success']),
    blocker,
  };
}
// Defaults allocated before admission runs are not a measured zero.
function queueMatchesDiagnostic(value) {
  const available = value != null && typeof value === 'object' && !Array.isArray(value);
  return {
    available, scope: 'distinct_matched_queue_rows', partition: 'vertical_then_attempts_then_cooldown',
    ...Object.fromEntries(['duplicateCandidateMatches', 'unobservedCandidateMatches', 'distinctMatchedRows']
      .map(key => [key, available ? count(value[key]) : null])),
    firstFailureCounts: Object.fromEntries(require('./scoutQueueAccounting').BUCKETS
      .map(key => [key, available ? count(value.firstFailureCounts?.[key]) : null])),
  };
}
function admissionReceiptDiagnostic(tenantId, cycleId, admission, measured = false) {
  const keys = ['outside_geography', 'segment_mismatch', 'contradictory_business_type',
    'unclassifiable_vertical', 'suppressed', 'owned_elsewhere', 'valid_ownership_collision',
    'stale_ownership', 'same_company_different_contact', 'insufficient_business_fit',
    'restaurant_boh_only', 'service_area_not_confirmed', 'mission_segment_mismatch',
    'public_website_required', 'pulseforge_reviewed_offer_required', 'pulseforge_prior_manual_first_touch'];
  const available = measured === true && admission != null && typeof admission === 'object';
  const rejected = available && admission.rejected && typeof admission.rejected === 'object'
    && !Array.isArray(admission.rejected) ? admission.rejected : null;
  const values = rejected ? Object.values(rejected) : [];
  const total = rejected && values.every(v => count(v) !== null)
    ? count(values.reduce((sum, v) => sum + v, 0)) : null;
  return {
    ...correlation(tenantId, cycleId), measured: available,
    // Fit is qualification, not new stock. Existing matches remain distinct.
    netNewAdmitted: available ? count(admission.admittedToEnrichment) : null,
    existingQueueCandidateMatches: available ? count(admission.alreadyQueued) : null,
    queueMatches: queueMatchesDiagnostic(available ? admission.queueMatches : null),
    counts: Object.fromEntries(['discovered', 'evaluated', 'fit', 'admittedToEnrichment',
      'alreadyUsable', 'alreadyQueued', 'recoveryBackoff', 'recovered'].map(k => [k, available ? count(admission[k]) : null])),
    rejected: Object.fromEntries(keys.map(k => [k, count(rejected?.[k])])),
    recordedRejectedTotal: total,
    // Preserve the sum without exposing arbitrary reason text or inventing a cause.
    unrecognizedReasonCount: rejected && values.every(v => count(v) !== null)
      ? count(Object.entries(rejected).reduce((sum, [k, v]) => sum + (keys.includes(k) ? 0 : v), 0)) : null,
  };
}
// Durable, bounded observations only; never infer network spend from executed/status.
function providerReceiptDiagnostic(tenantId, cycleId, discovery) {
  const result = discovery?.result || discovery || {};
  const reports = discoveryProviderReceipts(result.payload);
  const statuses = ['completed', 'partial', 'blocked', 'failed', 'skipped', 'success', 'empty'];
  const providers = ['google_places', 'google_maps', 'county_records', 'linkedin', 'facebook', 'website', 'news', 'prospeo', 'existing_pf', 'apollo'];
  const codes = ['OK', 'ZERO_RESULTS', 'REQUEST_DENIED', 'OVER_QUERY_LIMIT', 'RESOURCE_EXHAUSTED', 'INVALID_REQUEST', 'UNKNOWN_ERROR', 'UNKNOWN_STATUS', 'insufficient_coverage', 'provider_error', 'provider_unavailable', 'external_discovery_capability_unavailable', 'adapter_unavailable', 'discovery_provider_failed', 'invalid_target', 'enrichment_unavailable', 'google_places_status_REQUEST_DENIED', 'google_places_status_OVER_QUERY_LIMIT'];
  const boolean = v => typeof v === 'boolean' ? v : null;
  const time = v => typeof v === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z$/.test(v) && Number.isFinite(Date.parse(v)) ? v : null;
  const array = (v, limit) => Array.isArray(v) ? v.slice(0, limit) : [];
  const errors = v => array(v, 20).map(e => ({ code: choice(e?.code, codes), status: choice(e?.googleStatus, codes) }));
  const providerErrors = report => {
    const normalized = errors([...array(report?.execution?.errors, 20), ...array(report?.errors, 20)]);
    return normalized.filter((error, i) => normalized.findIndex(other =>
      other.code === error.code && other.status === error.status) === i);
  };
  return {
    ...correlation(tenantId, cycleId), schemaVersion: 1,
    discovery: discoveryDiagnostic(discovery),
    reportsAvailable: Array.isArray(reports),
    reportsTruncated: Array.isArray(reports) && reports.length > 20,
    startedAt: time(result.startedAt), completedAt: time(result.completedAt),
    errors: errors(result.errors),
    providers: array(reports, 20).map(report => {
      const execution = report?.execution || {};
      return {
        provider: choice(report?.providerId, providers), status: choice(report?.status, statuses),
        executed: boolean(execution.executed), executedAt: time(report?.executedAt),
        rawResultCount: count(report?.results ?? report?.rawResultCount), mappedCandidateCount: count(report?.qualified ?? report?.mappedCandidateCount),
        queryCount: count(execution.totals?.queries), resultCount: count(execution.totals?.results),
        errors: providerErrors(report),
        queriesTruncated: Array.isArray(execution.queries) && execution.queries.length > 20,
        queries: array(execution.queries, 20).map(query => ({
          status: choice(query?.googleStatus, codes),
          httpStatus: Number.isInteger(query?.httpStatus) && query.httpStatus >= 100 && query.httpStatus <= 599 ? query.httpStatus : null,
          resultCount: count(query?.resultCount),
          // Older attempts omit cache metadata. Unknown stays unknown.
          cacheHit: boolean(query?.cacheHit),
          cacheStrategy: choice(query?.cacheStrategy, ['memory_ttl', 'memory_inflight']),
          cacheAgeMs: count(query?.cacheAgeMs), cacheExpiresInMs: count(query?.cacheExpiresInMs),
          origin: query?.cacheHit === true ? 'cache' : query?.cacheHit === false ? 'uncached' : 'unknown',
        })),
      };
    }),
  };
}
function scoutRampDiagnostic({ tenantId, cycleId, requested, promoted, recoveredExisting, discoveredQueued, admission, enrichment, discovery }) {
  return {
    ...correlation(tenantId, cycleId), phase: 'scout_ramp',
    requested: count(requested), promoted: count(promoted), recoveredExisting: count(recoveredExisting),
    discoveredQueued: count(discoveredQueued),
    admission: Object.fromEntries(['discovered', 'evaluated', 'fit', 'admittedToEnrichment'].map(key => [key, count(admission?.[key])])),
    enrichment: Object.fromEntries(['considered', 'emailResolved', 'emailVerified'].map(key => [key, count(enrichment?.[key])])),
    inventoryMeasured: false,
    preCycleCleanInventory: null, postCycleCleanInventory: null, netCleanInventoryDelta: null,
    discovery: discoveryDiagnostic(discovery),
  };
}
// First-failure counts across the loaded inventory, not the newly promoted cohort.
// SQL-prefiltered rows (missing email, DNC, outreach review) are outside this scope.
function cleanInventoryExclusionDiagnostic(value) {
  const keys = ['business_fit', 'geography', 'contact_not_verified', 'dnc',
    'duplicate_company_or_email', 'missing_row', 'do_not_contact', 'outreach_review_required',
    'invalid_outreach_email', 'email_not_verified', 'email_status_not_verified',
    'contaminated_email_domain', 'inferred_pattern_provenance', 'unobserved_provenance',
    'contact_tenant_mismatch', 'synthetic_contact', 'missing_email_provenance',
    'contact_classification_not_sendable', 'contact_classification_address_missing',
    'contact_classification_email_changed', 'contact_classification_not_authorized',
    'missing_company_domain', 'shared_profile_company_domain', 'recipient_company_domain_mismatch',
    'prior_contact_or_human_owned', 'ao_owned_alias', 'ao_owned', 'already_attempted',
    'prior_canonical_execution', 'prior_reply_or_booked', 'reply_received', 'engaged',
    'quote_requested', 'incumbent_vendor', 'wrong_contact', 'closed', 'nurture', 'paused',
    'service_area_not_confirmed',
    'mission_segment_mismatch', 'public_website_required', 'pulseforge_reviewed_offer_required',
    'pulseforge_prior_manual_first_touch'];
  const available = value != null && typeof value === 'object' && !Array.isArray(value);
  const entries = available ? Object.entries(value) : [];
  const valid = available && entries.every(([, n]) => count(n) !== null);
  return {
    available, scope: 'loaded_inventory_first_failure',
    counts: Object.fromEntries(keys.map(key => [key, available
      ? (Object.hasOwn(value, key) ? count(value[key]) : 0) : null])),
    recordedExcludedTotal: valid ? count(entries.reduce((sum, [, n]) => sum + n, 0)) : null,
    unrecognizedReasonCount: valid
      ? count(entries.reduce((sum, [key, n]) => sum + (keys.includes(key) ? 0 : n), 0)) : null,
  };
}
function promotionCleanDiagnostic(value) {
  return {
    available: value != null && typeof value === 'object' && !Array.isArray(value),
    scope: 'enrichment_promoted_or_recovered',
    ...Object.fromEntries(['outcomes', 'identifiedProspects', 'unidentifiedOutcomes',
      'duplicateOutcomeReferences', 'clean', 'unmatched'].map(key => [key, count(value?.[key])])),
    exclusions: { ...cleanInventoryExclusionDiagnostic(value?.exclusionCounts),
      scope: 'enrichment_cohort_first_failure' },
  };
}
function controlCycleDiagnostic(tenantId, event) {
  return {
    ...correlation(tenantId, event.cycleId), phase: 'control_cycle', eventWriteCompleted: true,
    state: choice(event.state, ['healthy', 'critical', 'delivery_halted', 'replenish']),
    scoutPromotionCleanCohort: promotionCleanDiagnostic(event.scoutPromotionCleanCohort),
    cleanInventoryExclusions: cleanInventoryExclusionDiagnostic(event.cleanInventoryExclusions),
    cleanInventoryBefore: count(event.cleanInventoryBefore), cleanInventoryAfter: count(event.cleanInventoryAfter),
    pendingPrepared: event.pendingPreparedMeasured === true ? count(event.pendingPrepared) : null, preparedAdded: count(event.preparedAdded),
    prepareRequested: count(event.prepareRequested), sentToday: event.sentTodayMeasured === true ? count(event.sentToday) : null,
    dispatchCapacityNow: count(event.dispatchCapacityNow),
    prepareSkippedReason: choice(event.prepareSkippedReason, ['no_clean_inventory', 'preparation_backoff', 'discovery_evidence', 'verified_inventory_shortfall', 'no_remaining_capacity', 'preparation_disabled', 'no_deficit', 'control_execute_disabled', 'send_lock_overlap', 'preparation_not_executed', 'governor_halt', 'grant_inactive', 'daily_authorization_exhausted', 'total_authorization_exhausted', 'no_remaining_slots', 'pending_covers_capacity', 'batch_limit_reached']),
  };
}
function emitDiagnostic(logger, label, value) {
  try { logger.log?.(label, JSON.stringify(value)); } catch (_) { /* Diagnostics must not change execution. */ }
}
module.exports = { admissionReceiptDiagnostic, providerReceiptDiagnostic, scoutRampDiagnostic, controlCycleDiagnostic, discoveryDiagnostic, emitDiagnostic };
