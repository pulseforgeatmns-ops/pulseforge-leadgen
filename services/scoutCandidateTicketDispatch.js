'use strict';

const { createHash } = require('node:crypto');
const { toDiscoveredCompany } = require('../packages/max/scoutAcquisition/DiscoveryAdapters');
const { resolveCandidateUniverse, mergeResolved } = require('../packages/max/scoutAcquisition/EntityResolution');
const { ID_FIELDS, mergeTicketRefs } = require('../utils/scoutTicketMembership');

const ENABLED_TENANTS = new Set(['10', '13']);
const refKey = ref => JSON.stringify(ID_FIELDS.map(field => ref[field]));

function candidateTicketDependencyKey({ tenantId, program, source }) {
  const evidenceVersion = require('./scoutWebsiteBusinessEvidence').BUSINESS_EVIDENCE_VERSION || 'scout-website-business-evidence-v1';
  return createHash('sha256').update(JSON.stringify({ version: 'candidate-dependencies-v1',
    tenantId: String(tenantId), policy: program?.policy || {}, source: source?.payload || {}, evidenceVersion,
    recoveryProviderAvailability: {
      hunter: Boolean(process.env.HUNTER_API_KEY), prospeo: Boolean(process.env.PROSPEO_API_KEY),
    } })).digest('hex');
}

function claimedCompanies(claims, tenantId, observed = []) {
  const { normalizeDomain } = require('../utils/canonicalEmailEligibility');
  const complete = claims.map(work => {
    const rows = work.members.map(raw => {
      // Historical Places checkpoints stored the search string as industry.
      // Reconstruct that known provider provenance without changing saved raw;
      // absent query metadata must never promote it into observed business fit.
      const observed = raw.source === 'google_places' && raw.industry && !raw.discoveryQuery
        ? { ...raw, discoveryQuery: raw.industry } : raw;
      return toDiscoveredCompany(observed, { tenantId }, raw.source || 'public_business_places');
    });
    const valid = rows.filter(Boolean).sort((a,b) => Number(Boolean(b.location && b.website))-Number(Boolean(a.location && a.website)));
    let company = valid.length ? valid.reduce((prior, row) => mergeResolved(prior, row)) : { name: '', tenantId };
    // A durable obligation cannot be split by mutable raw.id/name/domain. Carry
    // every actual member together; conflicting domains require identity repair.
    const domains = new Set(work.members.map(raw => normalizeDomain(raw.domain || raw.website || raw.url)).filter(Boolean));
    company = { ...company, _scoutTicketRefs: mergeTicketRefs(...work.members) };
    if (domains.size > 1) company.name = '';
    return company;
  });
  const invalid = complete.filter(row => !row.name);
  const companies = [...resolveCandidateUniverse([], complete.filter(row => row.name)).companies, ...invalid];
  for (let index = 0; index < companies.length; index++) {
    const candidate = companies[index];
    const claimed = new Map(mergeTicketRefs(candidate).map(ref => [refKey(ref), ref]));
    for (const observation of observed) {
      const matching = mergeTicketRefs(observation).filter(ref => claimed.has(refKey(ref)));
      if (!matching.length) continue;
      // Reuse actual discovery evidence, but processing authority comes only
      // from the independent durable claim, never from a pipeline merge.
      companies[index] = mergeResolved(companies[index], { ...observation,
        _scoutTicketRefs: matching.map(ref => claimed.get(refKey(ref))) });
    }
  }
  for (const company of companies) {
    const observations = [company, ...(company.classificationObservations || [])];
    const locations = new Set(observations.map(row => String(row.location || row.address || '').toLowerCase()
      .replace(/[^a-z0-9]/g, '')).filter(Boolean));
    const domains = new Set(observations.map(row => normalizeDomain(row.website || row.domain)).filter(Boolean));
    if (locations.size > 1 || domains.size > 1) company.name = '';
  }
  return companies;
}

function createCandidateTicketDispatcher({ ticketStore, tenantId, dependencyKey, persist }) {
  if (!ENABLED_TENANTS.has(String(tenantId))) throw Error('Scout durable candidate dispatch is not enabled for this tenant');
  let attempted = false;
  let result = null;
  let failure = null;
  const pass = {
    async renew(refs) {
      try { await ticketStore.renew(refs); return true; }
      catch (error) {
        if (['claim_stale', 'lease_expired', 'membership_changed', 'stale_claim'].includes(error.ticketCode)) return false;
        throw error;
      }
    },
    finish(refs, outcome, write) {
      return ticketStore.finish(refs, { ...outcome, dependencyKey }, write);
    },
  };
  return {
    async drain(input = {}) {
      if (attempted) return result;
      // One attempt per control cycle, including failure. Never repeat provider
      // work in a fallback path after an ambiguous persistence/commit result.
      attempted = true;
      try {
        const claims = await ticketStore.claimDue({ dependencyKey, limit: 100 });
        const companies = claimedCompanies(claims, String(tenantId), input.companies || []);
        result = await persist({ ...input, tenantId: String(tenantId), companies }, pass);
        return result;
      } catch (error) { failure = 'candidate_processing_failed'; throw error; }
    },
    get attempted() { return attempted; },
    async snapshot() { return candidateTicketDiagnostic({ ...(await ticketStore.snapshot({ dependencyKey })), failure }); },
  };
}

function candidateTicketDiagnostic(value) {
  if (!value) return null;
  const count = n => Number.isSafeInteger(n) && n >= 0 ? n : null;
  const states = ['pending', 'deferred', 'blocked', 'handled'];
  const reasons = Object.values(require('./scoutCandidateTickets').REASONS).flat();
  return { status: ['pending_candidate_work', 'observed_candidate_set_handled', 'not_observed'].includes(value.status)
    ? value.status : 'candidate_work_unavailable',
  total: count(value.total), due: count(value.due), claimed: count(value.claimed),
  byState: Object.fromEntries(states.map(key => [key, count(value.byState?.[key] || 0)])),
  byReason: Object.fromEntries(reasons.filter(key => value.byReason?.[key]).map(key => [key, count(value.byReason[key])])),
  failure: value.failure === 'candidate_processing_failed' ? value.failure : null };
}

function queryProgressionDiagnostic(value) {
  if (!value) return null;
  const result = { status: ['observed_query_set_handled', 'pending_query_work', 'not_observed'].includes(value.status)
    ? value.status : 'not_observed',
  ...Object.fromEntries(['observedQueries', 'handledQueries', 'fetchedQueries', 'replayedQueries',
    'pendingAcknowledgements', 'unresolvedQueries'].map(key => [key,
    Number.isSafeInteger(value[key]) && value[key] >= 0 ? value[key] : null])) };
  if (value.continuation) {
    const statuses = ['finite_query_exhausted', 'provider_page_limit_reached', 'legacy_continuation_unknown',
      'page_token_not_ready', 'page_fetch_in_progress', 'page_fetch_outcome_unknown',
      'continuation_token_invalid', 'page_provider_failure', 'page_provider_denied', 'cycle_page_budget_reached',
      'coverage_epoch_permission_required', 'coverage_epoch_not_started', 'coverage_epoch_expired',
      'coverage_epoch_manifest_changed', 'coverage_epoch_legacy_required'];
    statuses.push('tenant_query_previously_searched','tenant_query_outcome_unconfirmed','tenant_query_low_novelty');
    result.continuation = { maxNewPages: value.continuation.maxNewPages === 1 ? 1 : null,
      newPagesReserved: Number.isSafeInteger(value.continuation.newPagesReserved) && value.continuation.newPagesReserved >= 0
        && value.continuation.newPagesReserved <= 1 ? value.continuation.newPagesReserved : null, statuses: Object.fromEntries(statuses
      .filter(key => Number.isSafeInteger(value.continuation.statuses?.[key]) && value.continuation.statuses[key] >= 0)
      .map(key => [key, value.continuation.statuses[key]])) };
  }
  if (value.tenantMemory) result.tenantMemory=Object.fromEntries(
    ['queriesSkipped','queriesUnconfirmed','lowNoveltyQueriesDeferred','detailsReused','detailsReserved'].map(key=>[key,
      Number.isSafeInteger(value.tenantMemory[key]) && value.tenantMemory[key]>=0 ? value.tenantMemory[key] : null]));
  if (value.coverageEpochStatus === 'configuration_invalid') result.coverageEpochStatus = 'configuration_invalid';
  if (value.coverageApprovalManifest) {
    const manifest = require('./scoutCoverageEpoch').projectCoverageApprovalManifest(value.coverageApprovalManifest);
    if (manifest) result.coverageApprovalManifest = manifest;
  }
  return result;
}

function observedQueryDependency(value) {
  const p=queryProgressionDiagnostic(value); const statuses=p?.continuation?.statuses;
  if (!statuses || !p.observedQueries || p.fetchedQueries || p.replayedQueries) return null;
  const stopped = Object.entries(statuses).filter(([key]) => !['cycle_page_budget_reached','page_token_not_ready','page_fetch_in_progress'].includes(key))
    .reduce((sum,[,n])=>sum+n,0);
  if (stopped < p.observedQueries) return null;
  return { scope:'observed_query_set', reason:'observed_queries_require_dependency',
    action:'review_remaining_approved_query_plan_or_recorded_provider_dependency', statuses };
}

module.exports = { observedQueryDependency, queryProgressionDiagnostic, ENABLED_TENANTS, candidateTicketDependencyKey, claimedCompanies, createCandidateTicketDispatcher, candidateTicketDiagnostic };
