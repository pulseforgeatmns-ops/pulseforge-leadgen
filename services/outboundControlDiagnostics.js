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
function discoveryDiagnostic(discovery) {
  const result = discovery?.result || discovery || {};
  const payload = result.payload || {};
  const reports = Array.isArray(payload.providerExecution) ? payload.providerExecution.slice(0, 20) : [];
  // Exact code matching only. Error messages, URLs and arbitrary provider fields never leave here.
  const errors = [...(Array.isArray(result.errors) ? result.errors.slice(0, 100) : []),
    ...reports.flatMap(row => Array.isArray(row?.execution?.errors) ? row.execution.errors.slice(0, 100) : [])].slice(0, 100);
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
function controlCycleDiagnostic(tenantId, event) {
  return {
    ...correlation(tenantId, event.cycleId), phase: 'control_cycle', eventWriteCompleted: true,
    state: choice(event.state, ['healthy', 'critical', 'delivery_halted', 'replenish']),
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
module.exports = { scoutRampDiagnostic, controlCycleDiagnostic, discoveryDiagnostic, emitDiagnostic };
